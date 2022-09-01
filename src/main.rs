#![allow(unused)]
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::*;

type TransactionID = u32;
type ClientID = u16;
type Amount = f64;

#[derive(Clone, Debug)]
pub enum Transaction {
    Deposit(ClientID, TransactionID, Amount),
    Withdraw(ClientID, TransactionID, Amount),
    Dispute(ClientID, TransactionID),
    Resolve(ClientID, TransactionID),
    ChargeBack(ClientID, TransactionID),
}

impl Transaction {
    fn tx(&self) -> TransactionID {
        *match self {
            Transaction::Deposit(_, tx, _)
            | Transaction::Withdraw(_, tx, _)
            | Transaction::Dispute(_, tx)
            | Transaction::Resolve(_, tx)
            | Transaction::ChargeBack(_, tx) => tx,
        }
    }

    fn client_id(&self) -> ClientID {
        *match self {
            Transaction::Deposit(client_id, _, _)
            | Transaction::Withdraw(client_id, _, _)
            | Transaction::Dispute(client_id, _)
            | Transaction::Resolve(client_id, _)
            | Transaction::ChargeBack(client_id, _) => client_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionInner {
    client: ClientID,
    tx: TransactionID,
    amount: Amount, // we defined that positive amount means 'deposit', and negative amount means 'withdraw'
    status: DisputeStatus,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DisputeStatus {
    Normal,
    Disputed,
    Resolved,
    ChargeBacked,
}

#[derive(Error, Debug)]
pub enum PaymentError {
    #[error("Duplicate transaction id, {0:?}")]
    DuplicateTransaction(Transaction),

    #[error("Insuffiecient balance, tx = {0:?}, availabe = {1}")]
    InsuffiecientBalance(Transaction, Amount),

    #[error("No such transaction id, txn = {0:?}")]
    NoSuchTransactionID(Transaction),

    #[error("Invalid disputed status, txn = {0:?}, current status = {1:?}")]
    InvalidDisputeStatus(Transaction, DisputeStatus),

    #[error("Account is locked after tx = {1}, txn={0:?}")]
    Locked(Transaction, TransactionID), //

    #[error("No endpoint to send({0:?})")]
    NoEndpointToSend(Transaction),

    #[error("Unknown error")]
    Unknown(Transaction, Option<TransactionInner>),
}

#[derive(Debug, Clone)]
struct PaymentSummary {
    asset_book: AssetBook,
    ledger: Vec<Transaction>,
    //disputed_records: std::collections::HashSet<TransactionID>,
}

#[derive(Debug, Clone, Default)]
struct Asset {
    total: f64,
    available: f64,
    hold: f64,
    is_locked: Option<TransactionID>,
}

impl Asset {
    fn new() -> Self {
        Asset {
            total: 0.0,
            available: 0.0,
            hold: 0.0,
            is_locked: None,
        }
    }
}

type ServiceEndpoint = tokio::sync::mpsc::UnboundedSender<Transaction>;

struct PaymentEngine {
    endpoint: Vec<ServiceEndpoint>,
    finance_queue: (
        tokio::sync::mpsc::UnboundedSender<PaymentService>,
        tokio::sync::mpsc::UnboundedReceiver<PaymentService>,
    ),
    shutdown: Option<tokio::task::JoinHandle<()>>,
}

impl PaymentEngine {
    fn new() -> Self {
        Self {
            endpoint: vec![],
            finance_queue: tokio::sync::mpsc::unbounded_channel(),
            shutdown: None,
        }
    }

    // start the enginge with a number of services
    fn start(
        &mut self,
        size: std::num::NonZeroU8,
        dead_letter_queue: mpsc::UnboundedSender<PaymentError>,
    ) {
        let sz = size.get() as usize;
        let mut wait_group = vec![];
        wait_group.reserve(sz);
        for _ in 0..sz {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let dead_que = dead_letter_queue.clone();
            // update wait group
            wait_group.push(tokio::spawn(async move {
                PaymentService::new().run(rx, dead_que).await
            }));
            // update endpoint
            self.endpoint.push(tx);
        }

        // when service ended, we get summary
        self.shutdown = Some(tokio::spawn(async move {
            for wait in wait_group {
                let summary = wait.await;
            }
        }));
    }

    fn execute(&mut self, txn: Transaction) -> Result<(), anyhow::Error> {
        // Engine should start before execute any transaction
        if self.shutdown.is_none() {
            panic!("Engine not started yet");
        }

        // simple dispatcher with hashing
        let client_id = txn.client_id();
        self.endpoint[txn.client_id() as usize % self.endpoint.len()].send(txn)?;

        Ok(())
    }

    async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown {
            // close each service by dropping each corresponding endpoint
            self.endpoint.into_iter().for_each(|ep| {
                drop(ep);
            });

            // wait for all services to finish
            shutdown.await;
        }
    }
}

#[derive(Default)]
struct PaymentService {
    running: std::collections::HashMap<TransactionID, TransactionInner>,
    ledger: Vec<Transaction>, // FIFO
}

type AssetBook = std::collections::HashMap<ClientID, Asset>;

impl PaymentService {
    fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    async fn run(
        mut self,
        mut receiver: mpsc::UnboundedReceiver<Transaction>,
        dead_letter_queue: tokio::sync::mpsc::UnboundedSender<PaymentError>,
    ) -> Result<PaymentSummary, anyhow::Error> {
        let mut asset_book = AssetBook::new();

        while let Some(txn) = receiver.recv().await {
            // make new asset for new client
            let mut asset = asset_book
                .entry(txn.client_id())
                .or_insert_with(|| Asset::new());

            // The instruction seemed not mention what to do for the following transactions once it's locked
            // Here I choose to simply skip transaction and publish an error
            if let Some(tx) = asset.is_locked {
                let _ = dead_letter_queue.send(PaymentError::Locked(txn, tx));
                continue;
            }

            // for tracking
            self.ledger.push(txn.clone());

            let tx = txn.tx();
            match (txn, self.running.get_mut(&tx)) {
                // Case 1. Deposit
                (Transaction::Deposit(client, tx, amount), None) => {
                    let txn = TransactionInner {
                        client,
                        tx,
                        amount,
                        status: DisputeStatus::Normal,
                    };

                    self.running.insert(tx, txn);

                    asset.total += amount;
                    asset.available += amount;
                }
                // Case 2. Error for deposit if found duplicate transaction id
                (txn @ Transaction::Deposit(..), Some(_)) => {
                    dead_letter_queue.send(PaymentError::DuplicateTransaction(txn));
                }

                // Case 3. Withdraw
                (Transaction::Withdraw(client, tx, amount), None) => {
                    // check if insuffiecient balance
                    if asset.available - amount < 0.0 {
                        dead_letter_queue.send(PaymentError::InsuffiecientBalance(
                            Transaction::Withdraw(client, tx, amount),
                            asset.available,
                        ));
                        continue;
                    }

                    // happy path
                    let txn = TransactionInner {
                        client,
                        tx,
                        amount: -amount, // negate to indicate it's a withdraw move
                        status: DisputeStatus::Normal,
                    };

                    self.running.insert(tx, txn);

                    asset.total -= amount;
                    asset.available -= amount;
                }

                // Case 4. Error for withdraw if found duplicated tx
                (txn @ Transaction::Withdraw(..), Some(_)) => {
                    dead_letter_queue.send(PaymentError::DuplicateTransaction(txn));
                }

                // Case 5. Dispute from normal status
                (
                    txn @ Transaction::Dispute { .. },
                    Some(TransactionInner {
                        ref amount,
                        status: ref mut st @ DisputeStatus::Normal,
                        ..
                    }),
                ) => {
                    // Error if inner is a withdraw transaction
                    if amount < &0.0 {
                        dead_letter_queue
                            .send(PaymentError::InsuffiecientBalance(txn, asset.available));
                        continue;
                    }
                    // Error if available fund is smaller than std::abs(amount)
                    if asset.available - amount < 0.0 {
                        dead_letter_queue
                            .send(PaymentError::InsuffiecientBalance(txn, asset.available));
                        continue;
                    }
                    // Happy path
                    asset.available -= amount;
                    asset.hold += amount;
                    // update status because it's disputed
                    *st = DisputeStatus::Disputed;
                }

                // Case 6. Resolve from disputed status
                (
                    Transaction::Resolve { .. },
                    Some(TransactionInner {
                        amount,
                        status: ref mut st @ DisputeStatus::Disputed,
                        ..
                    }),
                ) => {
                    asset.available += *amount;
                    asset.hold -= *amount;
                    // update status because it's resolved and no loger disputed
                    *st = DisputeStatus::Normal;
                }

                // Case 7. Chargeback from disputed status
                (
                    Transaction::ChargeBack { .. },
                    Some(TransactionInner {
                        tx,
                        ref amount,
                        status: ref mut st @ DisputeStatus::Disputed,
                        ..
                    }),
                ) => {
                    asset.total -= amount;
                    asset.hold -= amount;
                    // instruction said it's locked when chargeback
                    asset.is_locked = Some(*tx);

                    // update status since it's charged back
                    *st = DisputeStatus::ChargeBacked;
                }

                // Case 8. Error for Resolve | ChargeBack if any but disputed status
                (
                    txn @ (Transaction::ChargeBack { .. } | Transaction::Resolve { .. }),
                    Some(TransactionInner { status, .. }),
                ) => {
                    dead_letter_queue.send(PaymentError::InvalidDisputeStatus(txn, *status));
                }

                // Case 8. Error for no such tx
                (txn, None) => {
                    dead_letter_queue.send(PaymentError::NoSuchTransactionID(txn));
                }

                // Case 9. Error for the remain combinations
                (txn @ _, txn_inner @ _) => {
                    dead_letter_queue.send(PaymentError::Unknown(txn, txn_inner.cloned()));
                }
            }
        }

        let summary = PaymentSummary {
            asset_book,
            ledger: self.ledger,
        };

        Ok::<PaymentSummary, anyhow::Error>(summary)
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    println!("Hello, world!");
    Ok(())
}
