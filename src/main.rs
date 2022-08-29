#![allow(unused)]
use tracing::*;
/// TransactionID is a valid u32
type TransactionID = u32;
type ClientID = u16;
type Amount = f64;

enum Transaction {
    Deposit(ClientID, TransactionID, Amount),
    Withdraw(ClientID, TransactionID, Amount),
    Dispute(ClientID, TransactionID),
    Resolve(ClientID, TransactionID),
    ChargeBack(ClientID, TransactionID),
}

impl Transaction {
    fn tx(&self) -> TransactionID {
        *match self {
            Transaction::Deposit(_, tx, _) => tx,
            Transaction::Withdraw(_, tx, _) => tx,
            Transaction::Dispute(_, tx) => tx,
            Transaction::Resolve(_, tx) => tx,
            Transaction::ChargeBack(_, tx) => tx,
        }
    }
}

struct TransactionInner {
    client: ClientID,
    tx: TransactionID,
    amount: Amount, // positive means deposit, and negative means withdraw
    status: DisputeStatus,
}

enum DisputeStatus {
    Normal,
    Disputed,
    Resolved,
    ChargeBacked,
}

#[derive(Default)]
struct PaymentEngine {
    m: std::collections::HashMap<TransactionID, TransactionInner>,
    is_locked: bool,
}

impl PaymentEngine {
    fn new() -> Self {
        Self {
            is_locked: false,
            ..Default::default()
        }
    }

    async fn run(
        mut self,
        client: ClientID,
        mut receiver: tokio::sync::mpsc::Receiver<Transaction>,
    ) {
        let mut total: f64 = 0.0;
        let mut available: f64 = 0.0;
        let mut hold: f64 = 0.0;

        while let Some(txn) = receiver.recv().await {
            if self.is_locked {
                warn!("client's payment is locked. No transaction will proceed");
                continue;
            }

            let tx = txn.tx();
            match (txn, self.m.get(&tx)) {
                // Deposit
                (Transaction::Deposit(client, tx, amount), None) => {
                    let txn = TransactionInner {
                        client,
                        tx,
                        amount,
                        status: DisputeStatus::Normal,
                    };

                    self.m.insert(tx, txn);
                }
                // Duplicate Deposit
                (Transaction::Deposit(client, tx, amount), Some(_)) => {}

                // Withdraw
                (Transaction::Withdraw(client, tx, amount), None) => {
                    let txn = TransactionInner {
                        client,
                        tx,
                        amount: -amount, // negative to indicate it's withdraw move
                        status: DisputeStatus::Normal,
                    };

                    self.m.insert(tx, txn);
                }
                // Duplicate Withdraw
                (Transaction::Withdraw(client, tx, amount), Some(_)) => {}

                // Dispute from Normal
                (
                    Transaction::Dispute { .. },
                    Some(TransactionInner {
                        client,
                        tx,
                        amount,
                        status: DisputeStatus::Normal,
                    }),
                ) => {}

                // Dispute error if any but 'Normal',
                (
                    Transaction::Dispute { .. },
                    Some(TransactionInner {
                        status: DisputeStatus::Resolved | DisputeStatus::ChargeBacked,
                        ..
                    }),
                ) => {}

                // Resolve from disputed
                (
                    Transaction::Resolve { .. },
                    Some(TransactionInner {
                        client,
                        tx,
                        amount,
                        status: DisputeStatus::Disputed,
                    }),
                ) => {}
                // Resolve error if any but disputed
                (Transaction::Resolve { .. }, Some(TransactionInner { status, .. })) => {}

                // Chargeback from disputed
                (
                    Transaction::ChargeBack { .. },
                    Some(TransactionInner {
                        client,
                        tx,
                        amount,
                        status: DisputeStatus::Normal,
                    }),
                ) => {}

                // ChargeBack error if any but disputed
                (Transaction::ChargeBack { .. }, Some(TransactionInner { status, .. })) => {}

                // error, no such record
                (txn, None) => {}

                // unknown error
                _ => {}
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    println!("Hello, world!");
    Ok(())
}
