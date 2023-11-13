use std::result::Result as StdResult;
use std::sync::atomic::Ordering;

use everscale_types::cell::{Cell, CellFamily, CellTreeStats};
use everscale_types::error as types;
use everscale_types::models::{AccountStatus, AccountStatusChange, ActionPhase, ComputePhase, ComputePhaseSkipReason, CurrencyCollection, ExecutedComputePhase, Lazy, MsgInfo, OptionalAccount, OrdinaryTxInfo, SkippedComputePhase, StorageUsedShort, Transaction};
use everscale_types::num::{Tokens, Uint15};
use everscale_types::prelude::{CellBuilder, Dict, HashBytes, Store};

use crate::ExecuteParams;

pub fn storage_stats<S: Store>(
    s: S,
    with_root_cell: bool,
    cell_limit: usize,
) -> StdResult<CellTreeStats, types::Error> {
    let mut builder = CellBuilder::new();
    s.store_into(&mut builder, &mut Cell::empty_context())?;
    // storage stats for slice do not count root cell, but do count its bits
    let mut storage = builder.as_full_slice()
        .compute_unique_stats(cell_limit)
        .ok_or(types::Error::DepthOverflow)?;
    if with_root_cell {
        // root hash cannot appear somewhere in the tree as circular cell dependency is impossible
        storage.cell_count += 1;
    } else {
        storage.bit_count -= builder.bit_len() as u64;
    }
    Ok(storage)
}

pub struct TxTime {
    now: u32,
    tx_lt: u64,
}

impl TxTime {
    pub fn new(
        params: &ExecuteParams,
        account: &OptionalAccount,
        incoming_msg_info: Option<&MsgInfo>,
    ) -> Self {
        let msg_lt = match incoming_msg_info {
            Some(MsgInfo::Int(h)) => h.created_lt,
            Some(MsgInfo::ExtOut(h)) => h.created_lt,
            _ => 0,
        };
        let tx_lt = std::cmp::max(
            account.0.as_ref().map_or(0, |a| a.last_trans_lt),
            std::cmp::max(params.last_tr_lt.load(Ordering::Relaxed), msg_lt + 1),
        );
        Self {
            now: params.block_unixtime,
            tx_lt,
        }
    }
    pub fn now(&self) -> u32 {
        self.now
    }
    pub fn tx_lt(&self) -> u64 {
        self.tx_lt
    }
    pub fn write_msg_time(&self, msg_info: &mut MsgInfo, msg_index: usize) {
        match msg_info {
            MsgInfo::Int(h) => {
                h.created_lt = self.tx_lt + 1 + msg_index as u64;
                h.created_at = self.now;
            }
            MsgInfo::ExtOut(h) => {
                h.created_lt = self.tx_lt + 1 + msg_index as u64;
                h.created_at = self.now;
            }
            MsgInfo::ExtIn(_) => {}
        };
    }
    pub fn non_aborted_account_lt(&self, msgs_count: usize) -> u64 {
        self.tx_lt + 1 + msgs_count as u64
    }
}

pub fn create_tx(
    account_addr: HashBytes,
    orig_status: AccountStatus,
    time: &TxTime,
    in_msg_cell: Option<&Cell>,
) -> Transaction {
    Transaction {
        // filled at the start of execution, immutable afterwards
        account: account_addr,
        lt: time.tx_lt,
        now: time.now,
        orig_status,
        in_msg: in_msg_cell.cloned(),
        // written during the whole tx execution
        total_fees: CurrencyCollection::default(),
        // will be replaced at the very end of full tx execution flow
        out_msg_count: Uint15::default(),
        out_msgs: Dict::default(),
        // will be replaced at any end of tx execution (if tx exists)
        end_status: AccountStatus::Active,
        info: Lazy::from_raw(Cell::empty_cell()),
        state_update: Lazy::from_raw(Cell::empty_cell()),
        // will be replaced outside of executor
        prev_trans_hash: HashBytes::default(),
        prev_trans_lt: 0,
    }
}

pub fn default_tx_info(bounce: bool) -> OrdinaryTxInfo {
    OrdinaryTxInfo {
        credit_first: !bounce,
        storage_phase: None,
        credit_phase: None,
        compute_phase: ComputePhase::Skipped(SkippedComputePhase {
            reason: ComputePhaseSkipReason::NoState,
        }),
        action_phase: None,
        aborted: false,
        bounce_phase: None,
        destroyed: false,
    }
}

pub fn default_vm_phase() -> ExecutedComputePhase {
    ExecutedComputePhase {
        success: false,
        msg_state_used: false,
        account_activated: false,
        gas_fees: Default::default(),
        gas_used: Default::default(),
        gas_limit: Default::default(),
        gas_credit: None,
        mode: 0,
        exit_code: 0,
        exit_arg: None,
        vm_steps: 0,
        vm_init_state_hash: Default::default(),
        vm_final_state_hash: Default::default(),
    }
}

pub fn default_action_phase() -> ActionPhase {
    ActionPhase {
        success: false,
        valid: false,
        no_funds: false,
        status_change: AccountStatusChange::Unchanged,
        total_fwd_fees: None,
        total_action_fees: None,
        result_code: 0,
        result_arg: None,
        total_actions: 0,
        special_actions: 0,
        skipped_actions: 0,
        messages_created: 0,
        action_list_hash: HashBytes::ZERO,
        total_message_size: StorageUsedShort::default(),
    }
}

#[derive(Debug, Clone)]
pub struct CopyleftReward {
    pub reward: Tokens,
    pub address: HashBytes,
}
