use super::asset::Asset;
use super::asset::AssetBook;
use super::error::PaymentError;
use super::service;
use super::service::PaymentService;
use tokio::sync::mpsc;
use tracing::*;

pub type TransactionID = u32;
pub type ClientID = u16;
pub type Amount = f64;

type ServiceEndpoint = tokio::sync::mpsc::UnboundedSender<Transaction>;

pub struct PaymentEngine {
    endpoint: Vec<ServiceEndpoint>,
    shutdown: Option<tokio::task::JoinHandle<()>>,
}

impl PaymentEngine {
    pub fn new() -> Self {
        Self {
            endpoint: vec![],
            shutdown: None,
        }
    }

    // start the enginge with a number of services
    pub fn start(
        &mut self,
        size: std::num::NonZeroU8,
        dead_letter_queue: mpsc::UnboundedSender<PaymentError>,
    ) {
        let sz = size.get() as usize;
        let mut wait_group = vec![];
        wait_group.reserve(sz);
        self.endpoint.reserve(sz);
        // Start services
        for _ in 0..sz {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let dead_que = dead_letter_queue.clone();
            // update wait group
            wait_group.push(tokio::spawn(async move {
                PaymentService::new().run(rx, dead_que).await
            }));
            // update endpoint for later dispatch
            self.endpoint.push(tx);
        }

        // Handle returned summary when services ended
        self.shutdown = Some(tokio::spawn(async move {
            for wait in wait_group {
                let summary = wait.await;
                match summary {
                    Ok(Ok(PaymentSummary { asset_book, .. })) => {
                        asset_book.into_iter().for_each(|(client_id, asset)| {
                            // As intructions required, we output a precision of up to four places past the decimal
                            let total = format!("{:.4}", asset.total);
                            let available = format!("{:.4}", asset.available);
                            let hold = format!("{:.4}", asset.hold);
                            let locked = asset.is_locked.is_some();
                            println!(
                                "{},{},{},{},{}",
                                client_id, total, available, hold, locked
                            );
                        })
                    }

                    err => {
                        error!(?err);
                    }
                }
            }
        }));
    }

    pub fn execute(&mut self, txn: Transaction) -> Result<(), anyhow::Error> {
        // Engine should start before execute any transaction
        if self.shutdown.is_none() {
            panic!("Engine not started yet");
        }

        // simple dispatcher using hashing
        let client_id = txn.client_id();
        self.endpoint[txn.client_id() as usize % self.endpoint.len()].send(txn)?;

        Ok(())
    }

    pub async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown {
            // close each service by dropping corresponding endpoint
            drop(self.endpoint);
            // wait for all services to exit
            shutdown.await;
        }
    }
}

#[derive(Clone, Debug)]
pub enum Transaction {
    Deposit(ClientID, TransactionID, Amount),
    Withdrawal(ClientID, TransactionID, Amount),
    Dispute(ClientID, TransactionID),
    Resolve(ClientID, TransactionID),
    ChargeBack(ClientID, TransactionID),
}

impl Transaction {
    pub fn tx(&self) -> TransactionID {
        *match self {
            Transaction::Deposit(_, tx, _)
            | Transaction::Withdrawal(_, tx, _)
            | Transaction::Dispute(_, tx)
            | Transaction::Resolve(_, tx)
            | Transaction::ChargeBack(_, tx) => tx,
        }
    }

    pub fn client_id(&self) -> ClientID {
        *match self {
            Transaction::Deposit(client_id, _, _)
            | Transaction::Withdrawal(client_id, _, _)
            | Transaction::Dispute(client_id, _)
            | Transaction::Resolve(client_id, _)
            | Transaction::ChargeBack(client_id, _) => client_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PaymentSummary {
    pub asset_book: AssetBook,
    pub history: Vec<Transaction>,
}
