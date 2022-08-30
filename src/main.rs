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

#[derive(Default)]
struct PaymentEngine {
    running: std::collections::HashMap<TransactionID, TransactionInner>,
    history: Vec<Transaction>, // FIFO
}

#[derive(Debug, Clone)]
struct AccountBook {
    client: ClientID,
    total: f64,
    available: f64,
    hold: f64,
    history: Vec<Transaction>,
    //disputed_records: std::collections::HashSet<TransactionID>,
    is_locked: bool,
}

impl PaymentEngine {
    fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    async fn run(
        mut self,
        client: ClientID,
        err_queue: tokio::sync::mpsc::Sender<Result<(), PaymentError>>,
        finalize_queue: tokio::sync::mpsc::Sender<AccountBook>,
        mut receiver: tokio::sync::mpsc::Receiver<Transaction>,
    ) -> Result<(), anyhow::Error> {
        let mut total: f64 = 0.0;
        let mut available: f64 = 0.0;
        let mut hold: f64 = 0.0;
        let mut is_locked = None;

        while let Some(txn) = receiver.recv().await {
            // The instruction seemed not mention what to do for the following transactions once it's locked
            // Here I choose to simply skip such transaction and send an error back
            if let Some(tx) = is_locked {
                err_queue.send(Err(PaymentError::Locked(txn, tx))).await?;
                continue;
            }

            // insert new record to history
            self.history.push(txn.clone());

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
                    total += amount;
                    available += amount;
                }
                // Case 2. Error for deposit if found duplicate tx
                (txn @ Transaction::Deposit(..), Some(_)) => {
                    err_queue.send(Err(PaymentError::DuplicateTX(txn))).await?;
                }

                // Case 3. Withdraw
                (Transaction::Withdraw(client, tx, amount), None) => {
                    // check if insuffiecient balance
                    if available - amount < 0.0 {
                        err_queue
                            .send(Err(PaymentError::InsuffiecientBalance(
                                Transaction::Withdraw(client, tx, amount),
                                available,
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

                    total -= amount;
                    available -= amount;
                }

                // Case 4. Error for withdraw if found duplicated tx
                (txn @ Transaction::Withdraw(..), Some(_)) => {
                    err_queue.send(Err(PaymentError::DuplicateTX(txn))).await?;
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
                    if available - *amount < 0.0 {
                        err_queue
                            .send(Err(PaymentError::InsuffiecientBalance(txn, available)))
                            .await?;
                        continue;
                    }
                    available -= *amount;
                    hold += *amount;
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
                    available += *amount;
                    hold -= *amount;
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
                    total -= *amount;
                    hold -= *amount;
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
                    err_queue
                        .send(Err(PaymentError::InvalidDisputeStatus(txn, *status)))
                        .await?;
                }

                // Case 8. Error for no such tx
                (txn, None) => {
                    err_queue.send(Err(PaymentError::NoSuchTX(txn))).await?;
                }

                // Case 9. Unknown error
                (txn @ _, txn_inner @ _) => {
                    err_queue
                        .send(Err(PaymentError::Unknown(txn, txn_inner.cloned())))
                        .await?;
                }
            }
        }

        finalize_queue
            .send(AccountBook {
                client,
                total,
                available,
                hold,
                is_locked: is_locked.is_some(),
                history: self.history,
                //disputed_records: self.disputed_records,
            })
            .await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    println!("Hello, world!");
    Ok(())
}
