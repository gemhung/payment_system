/************************************
*                                   *
*        Unit tests                 *
*                                   *
*************************************
*/

use super::engine::PaymentEngine;
use super::engine::Transaction;
use super::error::PaymentError;
use std::num::NonZeroUsize;

#[tokio::test]
async fn start_shutdown_engine() -> Result<(), anyhow::Error> {
    let mut engine = PaymentEngine::new();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    engine.start(std::num::NonZeroUsize::new(10).unwrap(), tx);

    drop(rx);
    engine.shutdown().await?;

    Ok(())
}

#[tokio::test]
async fn duplicate_start_engine() -> Result<(), anyhow::Error> {
    let mut engine = PaymentEngine::new();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    engine.start(std::num::NonZeroUsize::new(10).unwrap(), tx.clone());
    engine.start(std::num::NonZeroUsize::new(10).unwrap(), tx);

    drop(rx);
    engine.shutdown().await?;

    Ok(())
}

#[tokio::test]
async fn shutdown_without_start() -> Result<(), anyhow::Error> {
    let engine = PaymentEngine::new();
    engine.shutdown().await?;

    Ok(())
}

#[tokio::test]
async fn execute_without_start() -> Result<(), anyhow::Error> {
    let engine = PaymentEngine::new();

    match engine.execute(Transaction::Deposit(1, 1, 3.0)) {
        Err(err) => {
            let found = err.to_string().find("Engine not started yet");
            if found.is_none() {
                panic!("Can't find expected erorr string")
            }
        }
        Ok(_) => {
            panic!("Suppose to return error")
        }
    }

    engine.shutdown().await?;

    Ok(())
}

#[tokio::test]
async fn deposit() -> Result<(), anyhow::Error> {
    let mut engine = PaymentEngine::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    engine.start(NonZeroUsize::new(1).unwrap(), tx);

    let handle = tokio::spawn(async move {
        let r = rx.recv().await;
        if r.is_some() {
            panic!("Unexpected error, r={:?}", r);
        }
    });

    engine.execute(Transaction::Deposit(1, 1, 3.0))?;

    engine.shutdown().await?;
    handle.await?;

    Ok(())
}
#[tokio::test]
async fn deposit_duplicate_transaction() -> Result<(), anyhow::Error> {
    let mut engine = PaymentEngine::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    engine.start(NonZeroUsize::new(1).unwrap(), tx);

    let handle = tokio::spawn(async move {
        let r = rx.recv().await;
        if r.is_none() {
            panic!("Shouldn't receive end of stream before payment error");
        }

        if !matches!(r, Some(PaymentError::DuplicateTransaction { .. })) {
            panic!("Expect `DuplicateTransaction` error but get err = {:?}", r);
        }
    });

    engine.execute(Transaction::Deposit(1, 1, 3.0))?;
    engine.execute(Transaction::Deposit(1, 1, 3.0))?;

    engine.shutdown().await?;
    handle.await?;

    Ok(())
}

#[tokio::test]
async fn withdrawal() -> Result<(), anyhow::Error> {
    let mut engine = PaymentEngine::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    engine.start(NonZeroUsize::new(1).unwrap(), tx);

    let handle = tokio::spawn(async move {
        let r = rx.recv().await;
        if r.is_some() {
            panic!("Unexpected error");
        }
    });

    engine.execute(Transaction::Deposit(1, 1, 3.0))?;
    engine.execute(Transaction::Withdrawal(1, 2, 3.0))?;

    engine.shutdown().await?;
    handle.await?;

    Ok(())
}

#[tokio::test]
async fn withdrawal_duplicate_transaction() -> Result<(), anyhow::Error> {
    let mut engine = PaymentEngine::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    engine.start(NonZeroUsize::new(1).unwrap(), tx);

    let handle = tokio::spawn(async move {
        let r = rx.recv().await;
        if r.is_none() {
            panic!("Shouldn't receive end of stream before payment error");
        }

        if !matches!(r, Some(PaymentError::DuplicateTransaction { .. })) {
            panic!("Expect `DuplicateTransaction` error but get err = {:?}", r);
        }
    });

    engine.execute(Transaction::Deposit(1, 1, 3.0))?;
    engine.execute(Transaction::Withdrawal(1, 2, 3.0))?;
    engine.execute(Transaction::Withdrawal(1, 2, 3.0))?;

    engine.shutdown().await?;
    handle.await?;

    Ok(())
}

#[tokio::test]
async fn withdrawal_insufficient_balance() -> Result<(), anyhow::Error> {
    let mut engine = PaymentEngine::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    engine.start(NonZeroUsize::new(1).unwrap(), tx);

    let handle = tokio::spawn(async move {
        let r = rx.recv().await;
        if r.is_none() {
            panic!("Shouldn't receive end of stream before payment error");
        }

        if !matches!(r, Some(PaymentError::InsufficientBalance { .. })) {
            panic!("Expect `insufficient` error but get err = {:?}", r);
        }
    });

    engine.execute(Transaction::Deposit(1, 1, 3.0))?;
    engine.execute(Transaction::Withdrawal(1, 2, 4.0))?;

    engine.shutdown().await?;
    handle.await?;

    Ok(())
}
#[tokio::test]
async fn dispute_a_normal_txn() -> Result<(), anyhow::Error> {
    let mut engine = PaymentEngine::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    engine.start(NonZeroUsize::new(10).unwrap(), tx);

    let handle = tokio::spawn(async move {
        let r = rx.recv().await;
        if r.is_some() {
            panic!("Suppose not to get any error ");
        }
    });

    engine.execute(Transaction::Deposit(1, 2, 3.0))?;

    engine.shutdown().await?;
    handle.await?;

    Ok(())
}
#[tokio::test]
async fn dispute_no_such_transaction() -> Result<(), anyhow::Error> {
    let mut engine = PaymentEngine::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    engine.start(NonZeroUsize::new(10).unwrap(), tx);

    let handle = tokio::spawn(async move {
        let r = rx.recv().await;
        if r.is_none() {
            panic!("Shouldn't receive end of stream before payment error");
        }

        if !matches!(r, Some(PaymentError::NoSuchTransactionID { .. })) {
            panic!("Expect `NoSuchTransaction` error but get err = {:?}", r);
        }
    });

    engine.execute(Transaction::Deposit(1, 1, 3.0))?;
    engine.execute(Transaction::Withdrawal(1, 2, 1.0))?;
    engine.execute(Transaction::Dispute(1, 3))?;

    engine.shutdown().await?;
    handle.await?;

    Ok(())
}

#[tokio::test]
async fn dispute_withdrawal_txn() -> Result<(), anyhow::Error> {
    let mut engine = PaymentEngine::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    engine.start(NonZeroUsize::new(10).unwrap(), tx);

    let handle = tokio::spawn(async move {
        let r = rx.recv().await;
        if r.is_none() {
            panic!("Shouldn't receive end of stream before payment error");
        }

        if !matches!(r, Some(PaymentError::CanNotDisputeWithdrawal { .. })) {
            panic!(
                "Expect `CanNotDisputeWithdrawal` error but get err = {:?}",
                r
            );
        }
    });

    engine.execute(Transaction::Deposit(1, 1, 3.0))?;
    engine.execute(Transaction::Withdrawal(1, 2, 1.0))?;
    engine.execute(Transaction::Dispute(1, 2))?;

    engine.shutdown().await?;
    handle.await?;

    Ok(())
}

#[tokio::test]
async fn dispute_a_disputed_txn() -> Result<(), anyhow::Error> {
    let mut engine = PaymentEngine::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    engine.start(NonZeroUsize::new(10).unwrap(), tx);

    let handle = tokio::spawn(async move {
        let r = rx.recv().await;
        if r.is_none() {
            panic!("Shouldn't receive end of stream before payment error");
        }

        if !matches!(r, Some(PaymentError::InvalidTransactionStatus { .. })) {
            panic!("Expect `InvalidDisputeStatus` error but get err = {:?}", r);
        }
    });

    engine.execute(Transaction::Deposit(1, 1, 3.0))?;
    engine.execute(Transaction::Dispute(1, 1))?;
    engine.execute(Transaction::Dispute(1, 1))?;

    engine.shutdown().await?;
    handle.await?;

    Ok(())
}

#[tokio::test]
async fn chargeback() -> Result<(), anyhow::Error> {
    let mut engine = PaymentEngine::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    engine.start(NonZeroUsize::new(1).unwrap(), tx);

    let handle = tokio::spawn(async move {
        let r = rx.recv().await;
        if r.is_some() {
            panic!("Unexpected error, r = {:?}", r);
        }
    });

    engine.execute(Transaction::Deposit(1, 1, 2.0))?;
    engine.execute(Transaction::Deposit(1, 2, 3.0))?;
    engine.execute(Transaction::Dispute(1, 2))?;
    engine.execute(Transaction::ChargeBack(1, 2))?;

    engine.shutdown().await?;
    handle.await?;

    Ok(())
}

#[tokio::test]
async fn chargeback_no_such_transaction() -> Result<(), anyhow::Error> {
    let mut engine = PaymentEngine::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    engine.start(NonZeroUsize::new(1).unwrap(), tx);

    let handle = tokio::spawn(async move {
        let r = rx.recv().await;
        if r.is_none() {
            panic!("Shouldn't receive end of stream before payment error");
        }

        if !matches!(r, Some(PaymentError::NoSuchTransactionID { .. })) {
            panic!("Expect `NoSuchTransactionID` error but get err = {:?}", r);
        }
    });

    engine.execute(Transaction::Deposit(1, 1, 2.0))?;
    engine.execute(Transaction::Dispute(1, 1))?;
    engine.execute(Transaction::ChargeBack(1, 3))?;

    engine.shutdown().await?;
    handle.await?;

    Ok(())
}
#[tokio::test]
async fn chargeback_and_locked() -> Result<(), anyhow::Error> {
    let mut engine = PaymentEngine::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    engine.start(NonZeroUsize::new(1).unwrap(), tx);

    let handle = tokio::spawn(async move {
        let r = rx.recv().await;
        if r.is_none() {
            panic!("Shouldn't receive end of stream before payment error");
        }

        if !matches!(r, Some(PaymentError::Locked { .. })) {
            panic!("Expect `Locked` error but get err = {:?}", r);
        }
    });

    engine.execute(Transaction::Deposit(1, 1, 2.0))?;
    engine.execute(Transaction::Deposit(1, 2, 3.0))?;
    engine.execute(Transaction::Dispute(1, 2))?;
    engine.execute(Transaction::ChargeBack(1, 2))?;
    engine.execute(Transaction::Deposit(1, 3, 3.0))?;

    engine.shutdown().await?;
    handle.await?;

    Ok(())
}
