use super::engine::Amount;
use super::engine::Transaction;
use super::engine::TransactionID;
use super::service::DisputeStatus;
use super::service::TransactionInner;
use thiserror::Error;

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

    #[error("Unknown error")]
    Unknown(Transaction, Option<TransactionInner>),
}
