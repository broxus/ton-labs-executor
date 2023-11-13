use everscale_types::cell::CellBuilder;
use everscale_types::models::{Account, AccountState, CurrencyCollection, IntAddr, StdAddr, StorageInfo, StorageUsed};
use everscale_types::num::VarUint56;
use everscale_vm::types::Result;

use crate::blockchain_config::MAX_ACCOUNT_CELLS;
use crate::utils::storage_stats;

pub trait AccountExt {
    fn uninit(
        acc_addr: &StdAddr,
        balance: &CurrencyCollection,
    ) -> Result<Account>;

    fn update_storage_stat(&mut self) -> Result<()>;
    fn update_storage_stat_fast(&mut self) -> Result<()>;
}

impl AccountExt for Account {
    fn uninit(
        acc_addr: &StdAddr,
        balance: &CurrencyCollection,
    ) -> Result<Account> {
        let acc = Account {
            address: IntAddr::Std(acc_addr.clone()),
            storage_stat: StorageInfo::default(),
            last_trans_lt: 0,
            balance: balance.clone(),
            state: AccountState::Uninit,
            init_code_hash: None,
        };
        // stat will be updated at the end of execution according to global capability
        // acc.update_storage_stat()?;
        Ok(acc)
    }

    fn update_storage_stat(&mut self) -> Result<()> {
        let stats = if let Some(init_code_hash) = self.init_code_hash.as_ref() {
            let data = (self.last_trans_lt, &self.balance, &self.state, init_code_hash);
            storage_stats(data, true, MAX_ACCOUNT_CELLS)?
        } else {
            let data = (self.last_trans_lt, &self.balance, &self.state);
            storage_stats(data, true, MAX_ACCOUNT_CELLS)?
        };
        self.storage_stat.used = StorageUsed {
            cells: VarUint56::new(stats.cell_count),
            bits: VarUint56::new(stats.bit_count),
            public_cells: VarUint56::ZERO,
        };
        Ok(())
    }

    fn update_storage_stat_fast(&mut self) -> Result<()> {
        let stats = if let Some(init_code_hash) = self.init_code_hash.as_ref() {
            let data = (self.last_trans_lt, &self.balance, &self.state, init_code_hash);
            CellBuilder::build_from(data)?.stats()
        } else {
            let data = (self.last_trans_lt, &self.balance, &self.state);
            CellBuilder::build_from(data)?.stats()
        };
        self.storage_stat.used = StorageUsed {
            cells: VarUint56::new(stats.cell_count),
            bits: VarUint56::new(stats.bit_count),
            public_cells: VarUint56::ZERO,
        };
        Ok(())
    }
}
