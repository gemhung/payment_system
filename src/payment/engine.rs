use super::asset::AssetBook;
use super::error::PaymentError;
use super::service::PaymentService;
use tokio::sync::mpsc;

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

    // Start the enginge with a number of services
    pub fn start(
        &mut self,
        size: std::num::NonZeroUsize,
        dead_letter_queue: mpsc::UnboundedSender<PaymentError>,
    ) {
        // Starting engine again is a safe ops
        if self.shutdown.is_some() {
            return;
        }
        let sz = size.get();

        self.endpoint.reserve(sz);
        // Start services one by one
        let mut join_set = tokio::task::JoinSet::new();
        for _ in 0..sz {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let dead_que = dead_letter_queue.clone();
            // Spawn service and add handle to wait group
            join_set.spawn(PaymentService::new().run(rx, dead_que));
            // Update endpoint for later dispatch
            self.endpoint.push(tx);
        }
        // Handle returned summary when services ended
        self.shutdown = Some(tokio::spawn(async move {
            // Wait until all services ended
            let write_csv_header = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
            while let Some(res) = join_set.join_next().await {
                match res {
                    Ok(Ok(PaymentSummary { asset_book, .. })) => {
                        // Csv header
                        if write_csv_header.swap(false, std::sync::atomic::Ordering::SeqCst) {
                            println!("client,available,held,total,locked");
                        }
                        asset_book.print_to_stdout();
                    }
                    err => {
                        tracing::error!(?err);
                    }
                }
            }
        }));
    }

    pub fn execute(&self, txn: Transaction) -> Result<(), anyhow::Error> {
        // Engine should start before execute any transaction
        if self.shutdown.is_none() {
            return Err(anyhow::Error::msg("Engine not started yet"));
        }
        // Simple dispatcher using hashing
        self.endpoint[txn.client_id() as usize % self.endpoint.len()].send(txn)?;

        Ok(())
    }

    pub async fn shutdown(self) -> Result<(), anyhow::Error> {
        if let Some(shutdown) = self.shutdown {
            // Close each service by dropping corresponding endpoint
            drop(self.endpoint);
            // Wait for all services to exit
            shutdown.await?;
        }

        Ok(())
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
    #[allow(dead_code)]
    pub history: Vec<Transaction>,
}
