# payment_system
![image](https://github.com/user-attachments/assets/43188abd-a4b7-4a51-8ea7-af73a7c6b2e8)

## Transaction Status
![image](https://github.com/user-attachments/assets/e1d83c4c-310c-488c-80c0-adfcaf925064)

## Assumations
* For now, we can only dispute `deposit` transaction. Technically it's okay to dipsute both deposit and withdrawl transaction but the guidline description seemed to be only for deposit transactoin. Hence I choose to dispute only deposit transaction to be clear and minimum
* New transaction is added and marked as `normal` status
* Disputing any transaction other than normal status is an error
* After `resolving` a `disputed` transaction, it became `normal` status and we can `dispute` it again later
* After `charging back` a `disputed` transaction, any further transaction will be skipped and then pop an error
* Using `f64` for `amount` type but it migh have `overflow` and `round` problem. In reality, we should use `string` to be safe
* When printing out to stdout, we use `format` macro to get a precision up to 4 places past to decimal

# Run
Running result will output to stdout
```cmd
cargo run -- transactions.csv > accounts.csv
```


# Payment possible errors
```rust
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
```

