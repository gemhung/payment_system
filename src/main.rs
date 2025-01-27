mod csv;
mod payment;

use payment::engine::PaymentEngine;

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
    let (dead_letter_tx, mut dead_letter_rx) = tokio::sync::mpsc::unbounded_channel();
    let dead_letter_wait = tokio::spawn(async move {
        while let Some(err) = dead_letter_rx.recv().await {
            // For now , we simply print out error cause the instruction seems not to mention how to deal with error
            tracing::error!(?err);
        }
    });

    // Init payment engine
    let mut engine = PaymentEngine::new();
    // Payment services number is same as available_parallelism
    let sz = std::thread::available_parallelism().unwrap_or_else(|err| {
        tracing::error!(?err);
        std::num::NonZeroUsize::new(10).expect("should be positive number")
    });
    tracing::debug!(?sz);
    // Non-blocking. Start engine to run multiple identical payment services concurrently
    engine.start(sz, dead_letter_tx);

    // Feed all transactions to engine
    loop {
        match csv_stream.next() {
            // End of stream
            None => {
                break;
            }
            // CSV parsing error
            Some(Err(err)) => {
                tracing::error!(?err);
            }
            // Happy path
            Some(Ok(transaction)) => {
                if let Err(err) = engine.execute(transaction) {
                    tracing::error!(?err);
                }
            }
        }
    }

    // Graceful shutdown
    engine.shutdown().await?;
    dead_letter_wait.await?;

    Ok(())
}
