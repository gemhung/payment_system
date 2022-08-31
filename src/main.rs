#![allow(unused)]
use thiserror::Error;
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
    DuplicateTX(Transaction),

    #[error("Insuffiecient balance, tx = {0:?}, availabe = {1}")]
    InsuffiecientBalance(Transaction, Amount),

    #[error("No such transaction id, txn = {0:?}")]
    NoSuchTX(Transaction),

    #[error("Invalid disputed status, txn = {0:?}, current status = {1:?}")]
    InvalidDisputeStatus(Transaction, DisputeStatus),

    #[error("Account is locked after tx = {1}, txn={0:?}")]
    Locked(Transaction, TransactionID), //

    #[error("Unknown error")]
    Unknown(Transaction, Option<TransactionInner>),
}

#[derive(Debug, Clone)]
struct FinanceBook {
    client: ClientID,
    asset: Asset,
    ledger: Vec<Transaction>,
    //disputed_records: std::collections::HashSet<TransactionID>,
    is_locked: bool,
}

#[derive(Debug, Clone)]
struct Asset {
    total: f64,
    available: f64,
    hold: f64,
}

impl Asset {
    fn new() -> Self {
        Asset {
            total: 0.0,
            available: 0.0,
            hold: 0.0,
        }
    }
}

type ServiceEndpoint = tokio::sync::mpsc::UnboundedSender<Transaction>;

struct PaymentEngine {
    endpoints: std::collections::HashMap<ClientID, ServiceEndpoint>,
    dead_letter_queue: (
        tokio::sync::mpsc::UnboundedSender<PaymentError>,
        tokio::sync::mpsc::UnboundedReceiver<PaymentError>,
    ),
    finance_queue: (
        tokio::sync::mpsc::UnboundedSender<FinanceBook>,
        tokio::sync::mpsc::UnboundedReceiver<FinanceBook>,
    ),
}

impl PaymentEngine {
    fn new() -> Self {
        Self {
            endpoints: std::collections::HashMap::new(),
            dead_letter_queue: tokio::sync::mpsc::unbounded_channel(),
            finance_queue: tokio::sync::mpsc::unbounded_channel(),
        }
    }

    fn start() {}

    fn execute(
        &mut self,
        txn: Transaction,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<Transaction>> {
        match self.endpoints.get(&txn.client_id()) {
            Some(ep) => {
                ep.send(txn);
            }

            None => {}
        }

        Ok(())
    }
}

#[derive(Default)]
struct PaymentService {
    running: std::collections::HashMap<TransactionID, TransactionInner>,
    ledger: Vec<Transaction>, // FIFO
}

impl PaymentService {
    fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    async fn start(
        mut self,
        client: ClientID,
        dead_letter_queue: tokio::sync::mpsc::Sender<Result<(), PaymentError>>,
    ) -> Result<ServiceEndpoint, anyhow::Error> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let async_work = async move {
            let mut asset = Asset::new();
            let mut is_locked = None;

            while let Some(txn) = rx.recv().await {
                // The instruction seemed not mention what to do for the following transactions once it's locked
                // Here I choose to simply skip such transaction and send an error back
                if let Some(tx) = is_locked {
                    dead_letter_queue
                        .send(Err(PaymentError::Locked(txn, tx)))
                        .await?;
                    continue;
                }

                // insert new record to ledger
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
                    // Case 2. Error for deposit if found duplicate tx
                    (txn @ Transaction::Deposit(..), Some(_)) => {
                        dead_letter_queue
                            .send(Err(PaymentError::DuplicateTX(txn)))
                            .await?;
                    }

                    // Case 3. Withdraw
                    (Transaction::Withdraw(client, tx, amount), None) => {
                        // check if insuffiecient balance
                        if asset.available - amount < 0.0 {
                            dead_letter_queue
                                .send(Err(PaymentError::InsuffiecientBalance(
                                    Transaction::Withdraw(client, tx, amount),
                                    asset.available,
                                )))
                                .await;
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
                        dead_letter_queue
                            .send(Err(PaymentError::DuplicateTX(txn)))
                            .await?;
                    }

                    // Case 5. Dispute from normal status
                    (
                        txn @ Transaction::Dispute { .. },
                        Some(TransactionInner {
                            amount,
                            status: ref mut st @ DisputeStatus::Normal,
                            ..
                        }),
                    ) => {
                        if asset.available - *amount < 0.0 {
                            dead_letter_queue
                                .send(Err(PaymentError::InsuffiecientBalance(
                                    txn,
                                    asset.available,
                                )))
                                .await?;
                            continue;
                        }
                        asset.available -= *amount;
                        asset.hold += *amount;
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
                            amount,
                            status: ref mut st @ DisputeStatus::Disputed,
                            ..
                        }),
                    ) => {
                        asset.total -= *amount;
                        asset.hold -= *amount;
                        // instruction said it's locked when chargeback
                        is_locked = Some(*tx);

                        // update status since it's charged back
                        *st = DisputeStatus::ChargeBacked;
                    }

                    // Case 8. Error for Resolve | ChargeBack if any but disputed status
                    (
                        txn @ (Transaction::ChargeBack { .. } | Transaction::Resolve { .. }),
                        Some(TransactionInner { status, .. }),
                    ) => {
                        dead_letter_queue
                            .send(Err(PaymentError::InvalidDisputeStatus(txn, *status)))
                            .await?;
                    }

                    // Case 8. Error for no such tx
                    (txn, None) => {
                        dead_letter_queue
                            .send(Err(PaymentError::NoSuchTX(txn)))
                            .await?;
                    }

                    // Case 9. Error for the remain combinations
                    (txn @ _, txn_inner @ _) => {
                        dead_letter_queue
                            .send(Err(PaymentError::Unknown(txn, txn_inner.cloned())))
                            .await?;
                    }
                }
            }

            let finalize = FinanceBook {
                client,
                asset,
                is_locked: is_locked.is_some(),
                ledger: self.ledger,
                //disputed_records: self.disputed_records,
            };

            Ok::<FinanceBook, anyhow::Error>(finalize)
        };

        tokio::spawn(async_work);

        Ok(tx)
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    println!("Hello, world!");
    Ok(())
}
