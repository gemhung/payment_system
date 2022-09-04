mod csv;
mod payment;

use payment::engine::PaymentEngine;
use tracing::*;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_line_number(true)
        .with_file(true)
        .init();

    // Get arguments
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 2 {
        panic!(
            "Invalid arguments len. It accept only one argumetn, args = {:?}",
            args
        );
    }

    // Open csv file
    let file = std::fs::File::open(&args[1])?;

    // Init csv parser to create a stream of transaction
    let mut csv_stream = csv::TransactionReader { source: file }.into_iter();

    // Init error handling(dead_letter_queue)
    // dead_letter_queue is a special queue to handle all payment errors. Ex: we can alert or send an email
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let dead_letter_wait = tokio::spawn(async move {
        while let Some(err) = rx.recv().await {
            // For now , we simply print error with debug level cause the instruction didn't mention how to deal with error
            error!(?err);
        }
    });

    // Init payment engine
    let mut engine = PaymentEngine::new();
    // Payment services number is same as available_parallelism
    let sz = std::thread::available_parallelism().unwrap_or_else(|err| {
        error!(?err);
        std::num::NonZeroUsize::new(10).expect("should be positive number")
    });
    debug!(?sz);
    // Start engine to run multiple identical payment services concurrently
    engine.start(sz, tx);

    // Feed all transactions to engine
    loop {
        match csv_stream.next() {
            // End of stream
            None => {
                break;
            }
            // CSV parsing error
            Some(Err(err)) => {
                error!(?err);
            }
            // Happy path
            Some(Ok(transaction)) => {
                if let Err(err) = engine.execute(transaction) {
                    error!(?err);
                }
            }
        }
    }

    // Graceful shutdown
    engine.shutdown().await?;
    dead_letter_wait.await?;

    Ok(())
}
