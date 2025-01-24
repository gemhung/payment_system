use super::asset::AssetBook;
use super::engine::Amount;
use super::engine::ClientID;
use super::engine::PaymentSummary;
use super::engine::Transaction;
use super::engine::TransactionID;
use super::error::PaymentError;
use tokio::sync::mpsc;
use tracing::*;

#[derive(Default)]
pub(crate) struct PaymentService {
    clients_book: std::collections::HashMap<(ClientID, TransactionID), TransactionInner>,
    pub history: Vec<Transaction>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisputeStatus {
    Normal,
    Disputed,
    ChargeBacked,
}

#[derive(Debug, Clone)]
pub struct TransactionInner {
    #[allow(dead_code)]
    client: ClientID,
    tx: TransactionID,
    amount: Amount, // We defined that positive amount means 'deposit', and negative amount means 'withdrawal'
    status: DisputeStatus,
}

impl PaymentService {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub(super) async fn run(
        mut self,
        mut receiver: mpsc::UnboundedReceiver<Transaction>,
        dead_letter_queue: tokio::sync::mpsc::UnboundedSender<PaymentError>,
    ) -> Result<PaymentSummary, anyhow::Error> {
        let mut asset_book = AssetBook::new();

        while let Some(txn) = receiver.recv().await {
            // Make new asset for new client
            let client_id = txn.client_id();
            let asset = asset_book
                .entry(txn.client_id())
                .or_insert_with(crate::payment::asset::Asset::new);

            // The instruction seemed not mention what to do for the following transactions once it's locked
            // Here I choose to simply skip transaction and publish an error
            if let Some(tx) = asset.is_locked {
                let _ = dead_letter_queue.send(PaymentError::Locked(txn, tx));
                continue;
            }

            // For tracking
            self.history.push(txn.clone());

            let tx = txn.tx();
            match (txn, self.clients_book.get_mut(&(client_id, tx))) {
                // Case 1. Deposit
                (Transaction::Deposit(client, tx, amount), None) => {
                    let txn = TransactionInner {
                        client,
                        tx,
                        amount,
                        status: DisputeStatus::Normal,
                    };

                    self.clients_book.insert((client_id, tx), txn);

                    asset.total += amount;
                    asset.available += amount;
                    debug!(?asset, "Deposit,");
                }
                // Case 2. Error for deposit if found duplicate transaction id
                (txn @ Transaction::Deposit(..), Some(_)) => {
                    let _ = dead_letter_queue.send(PaymentError::DuplicateTransaction(txn));
                }
                // Case 3. Withdraw
                (Transaction::Withdrawal(client, tx, amount), None) => {
                    // check if insuffiecient balance
                    if asset.available - amount < 0.0 {
                        let _ = dead_letter_queue.send(PaymentError::InsufficientBalance(
                            Transaction::Withdrawal(client, tx, amount),
                            asset.available,
                        ));
                        continue;
                    }

                    // Happy path
                    let txn = TransactionInner {
                        client,
                        tx,
                        amount: -amount, // negate to indicate it's a withdrawal move
                        status: DisputeStatus::Normal,
                    };

                    self.clients_book.insert((client_id, tx), txn);

                    asset.total -= amount;
                    asset.available -= amount;
                    debug!(?asset, "Withdrawal, ");
                }
                // Case 4. Error for withdrawal if found duplicated transaction id
                (txn @ Transaction::Withdrawal(..), Some(_)) => {
                    let _ = dead_letter_queue.send(PaymentError::DuplicateTransaction(txn));
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
                    // Error if inner is a withdrawal transaction
                    if amount < &0.0 {
                        let _ = dead_letter_queue.send(PaymentError::CanNotDisputeWithdrawal(txn));
                        continue;
                    }
                    // Error if available fund is smaller than std::abs(amount)
                    if asset.available - amount < 0.0 {
                        let _ = dead_letter_queue
                            .send(PaymentError::InsufficientBalance(txn, asset.available));
                        continue;
                    }
                    // Happy path
                    asset.available -= amount;
                    asset.hold += amount;
                    // Update status because it's disputed
                    *st = DisputeStatus::Disputed;
                    debug!(?asset, "Dispute,");
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
                    // Update status because it's resolved and no loger disputed
                    *st = DisputeStatus::Normal;
                    debug!(?asset, "Resolve,");
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
                    // Instruction said it's locked when chargeback
                    asset.is_locked = Some(*tx);

                    // Update status since it's charged back
                    *st = DisputeStatus::ChargeBacked;
                    debug!(?asset, "Chargeback, ");
                }
                // Case 8.
                // * Error when resolving or chargeBacking any status but disputed status
                // * Error when disputing any status but normal status
                (
                    txn @ (Transaction::Dispute { .. }
                    | Transaction::ChargeBack { .. }
                    | Transaction::Resolve { .. }),
                    Some(TransactionInner { status, .. }),
                ) => {
                    let _ =
                        dead_letter_queue.send(PaymentError::InvalidDisputeStatus(txn, *status));
                }
                // Case 8. Error for no such tx
                (txn, None) => {
                    let _ = dead_letter_queue.send(PaymentError::NoSuchTransactionID(txn));
                }
            }
        }

        let summary = PaymentSummary {
            asset_book,
            history: self.history,
        };

        Ok::<PaymentSummary, anyhow::Error>(summary)
    }
}
