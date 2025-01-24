use crate::payment::engine::Transaction;

pub struct TransactionReader<T> {
    pub source: T,
}

impl<T: std::io::Read> TransactionReader<T> {
    pub fn into_iter(self) -> IntoIter<T> {
        let records = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(self.source)
            .into_records();
        IntoIter { records }
    }
}

pub struct IntoIter<T> {
    records: csv::StringRecordsIntoIter<T>,
}

impl<T: std::io::Read> Iterator for IntoIter<T> {
    type Item = Result<Transaction, anyhow::Error>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.records.next() {
            // End of stream
            None => None,
            Some(Ok(mut row)) => {
                if !matches!(row.len(), 4) {
                    return Some(Err(anyhow::anyhow!(
                        "invalid csv elements number, len = {}",
                        row.len()
                    )));
                }
                // Remove space to avoid parsing error later
                row.trim();
                // Convert csv line into transcation
                match (
                    row.get(0).inspect(|inner| {
                        inner.to_string().make_ascii_lowercase();
                    }),
                    row.get(1).map(|inner| inner.parse()),
                    row.get(2).map(|inner| inner.parse()),
                    row.get(3).map(|inner| inner.parse()),
                ) {
                    (
                        Some("deposit"),
                        Some(Ok(client_id)),
                        Some(Ok(transaction_id)),
                        Some(Ok(amount)),
                    ) => Some(Ok(Transaction::Deposit(client_id, transaction_id, amount))),
                    (
                        Some("withdrawal"),
                        Some(Ok(client_id)),
                        Some(Ok(transaction_id)),
                        Some(Ok(amount)),
                    ) => Some(Ok(Transaction::Withdrawal(
                        client_id,
                        transaction_id,
                        amount,
                    ))),
                    (Some("dispute"), Some(Ok(client_id)), Some(Ok(transaction_id)), _) => {
                        Some(Ok(Transaction::Dispute(client_id, transaction_id)))
                    }
                    (Some("resolve"), Some(Ok(client_id)), Some(Ok(transaction_id)), _) => {
                        Some(Ok(Transaction::Resolve(client_id, transaction_id)))
                    }
                    (Some("chargeback"), Some(Ok(client_id)), Some(Ok(transaction_id)), _) => {
                        Some(Ok(Transaction::ChargeBack(client_id, transaction_id)))
                    }

                    err => Some(Err(anyhow::anyhow!("invalid inputs, err = {:?}", err))),
                }
            }

            err => Some(Err(anyhow::anyhow!("Invalid csv line, err = {:?}", err))),
        }
    }
}
