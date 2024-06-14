use everscale_types::models::Transaction;

pub trait TransactionExt {
    fn account_lt(&self) -> u64;
}

impl TransactionExt for Transaction {
    fn account_lt(&self) -> u64 {
        self.lt + self.out_msg_count.into_inner() as u64 + 1
    }
}