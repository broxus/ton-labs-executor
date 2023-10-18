/*
* Copyright (C) 2019-2022 TON Labs. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific TON DEV software governing permissions and
* limitations under the License.
*/

use std::sync::atomic::Ordering;

use everscale_types::cell::Cell;
use everscale_types::models::{Account, AccountState, ComputePhase, ComputePhaseSkipReason, CurrencyCollection, Lazy, OptionalAccount, SkippedComputePhase, StateInit, TickTock, TickTockTxInfo, Transaction, TxInfo};
use everscale_types::num::{Tokens, Uint15};
use everscale_types::prelude::CellFamily;
use everscale_vm::{
    boolean, int,
    SmartContractInfo, stack::{integer::IntegerData, Stack, StackItem},
};
use everscale_vm::{error, fail, OwnedCellSlice, types::Result};

use crate::{ActionPhaseResult, error::ExecutorError, ExecuteParams, InputMessage, TransactionExecutor};
use crate::blockchain_config::PreloadedBlockchainConfig;
use crate::utils::{create_tx, store, TxTime};

pub struct TickTockTransactionExecutor {
    pub config: PreloadedBlockchainConfig,
    pub tt: TickTock,
}

impl TickTockTransactionExecutor {
    pub fn new(config: PreloadedBlockchainConfig, tt: TickTock) -> Self {
        Self {
            config,
            tt,
        }
    }
}

impl TransactionExecutor for TickTockTransactionExecutor {
    ///
    /// Create end execute tick or tock transaction for special account
    fn execute_with_params(
        &self,
        in_msg: Option<&InputMessage>,
        account: &mut OptionalAccount,
        params: &ExecuteParams,
    ) -> Result<Transaction> {
        if in_msg.is_some() {
            fail!("Tick Tock transaction must not have input message")
        }
        let (account_address, account_id, mut acc_balance, due_before_storage) = match &account.0 {
            Some(account) => {
                let Some(account_id) = account.address.as_std() else {
                    fail!("Tick Tock contract should have Standard address")
                };
                match account.state {
                    AccountState::Active(StateInit { special: Some(tt), .. }) =>
                        if tt.tock != (self.tt == TickTock::Tock) && tt.tick != (self.tt == TickTock::Tick) {
                            fail!("wrong type of account's tick tock flag")
                        }
                    _ => fail!("Account {} is not special account for tick tock", account_id)
                }
                log::debug!(target: "executor", "tick tock transaction account {}", account_id);
                let due_before_storage = account.storage_stat.due_payment.map_or(0, |due| due.into_inner());
                (account.address.clone(), account_id.address, account.balance.clone(), due_before_storage)
            },
            None => fail!("Tick Tock transaction requires deployed account")
        };

        let is_masterchain = true;
        let is_special = true;
        let time = TxTime::new(&params, account, None);
        let mut tr = create_tx(account_id, account.status(), &time, None);

        account.0.as_mut().map(|a| a.storage_stat.last_paid = 0);
        let (storage_phase, storage_fee) = match self.storage_phase(
            account,
            &mut acc_balance,
            &mut tr.total_fees,
            time.now(),
            is_masterchain,
            is_special,
        ) {
            Ok(storage_ph) => {
                let mut storage_fee = storage_ph.storage_fees_collected.into_inner();
                if let Some(due) = &storage_ph.storage_fees_due {
                    storage_fee += due.into_inner()
                }
                storage_fee -= due_before_storage;
                (storage_ph, storage_fee)
            }
            Err(e) => fail!(
                ExecutorError::TrExecutorError(
                    format!(
                        "cannot create storage phase of a new transaction for \
                         smart contract for reason {}", e
                    )
                )
            )
        };
        let mut description = TickTockTxInfo {
            kind: self.tt.clone(),
            storage_phase,
            compute_phase: ComputePhase::Skipped(SkippedComputePhase { reason: ComputePhaseSkipReason::NoState }),
            action_phase: None,
            aborted: false,
            destroyed: false,
        };

        let old_account = account.clone();
        let original_acc_balance = acc_balance.clone();

        let mut smc_info = SmartContractInfo {
            capabilities: self.config().global_version().capabilities,
            myself: OwnedCellSlice::new(store(&account_address)?)?,
            block_lt: params.block_lt,
            trans_lt: time.tx_lt(),
            unix_time: params.block_unixtime,
            seq_no: params.seq_no,
            balance: acc_balance.clone(),
            config_params: self.config().raw_config().params.root().clone(),
            ..Default::default()
        };
        smc_info.calc_rand_seed(params.seed_block, account_id.as_slice());
        let mut stack = Stack::new();
        stack
            .push(int!(acc_balance.tokens.into_inner()))
            .push(StackItem::integer(IntegerData::from_unsigned_bytes_be(&account_id.0)))
            .push(boolean!(self.tt == TickTock::Tock))
            .push(int!(-2));
        log::debug!(target: "executor", "compute_phase {}", time.tx_lt());
        let (compute_phase, actions, new_data) = match self.compute_phase(
            None,
            account,
            &mut acc_balance,
            &CurrencyCollection::default(),
            smc_info,
            stack,
            storage_fee,
            is_masterchain,
            is_special,
            &params,
        ) {
            Ok((compute_ph, actions, new_data)) => (compute_ph, actions, new_data),
            Err(e) => {
                log::debug!(target: "executor", "compute_phase error: {}", e);
                match e.downcast_ref::<ExecutorError>() {
                    Some(ExecutorError::NoAcceptError(_, _)) => return Err(e),
                    _ => fail!(ExecutorError::TrExecutorError(e.to_string()))
                }
            }
        };
        let mut out_msgs = vec![];
        description.compute_phase = compute_phase;
        description.action_phase = match description.compute_phase {
            ComputePhase::Executed(ref phase) => {
                tr.total_fees.tokens = tr.total_fees.tokens
                    .checked_add(phase.gas_fees).ok_or_else(|| error!("integer overflow"))?;
                if phase.success {
                    log::debug!(target: "executor", "compute_phase: TrComputePhase::Vm success");
                    log::debug!(target: "executor", "action_phase {}", time.tx_lt());
                    match self.action_phase_with_copyleft(
                        &mut tr.total_fees.tokens,
                        &time,
                        account,
                        &original_acc_balance,
                        &mut acc_balance,
                        &mut CurrencyCollection::default(),
                        &Tokens::ZERO,
                        actions.unwrap_or(Cell::empty_cell()),
                        &account_address,
                        is_special,
                    ) {
                        Ok(ActionPhaseResult { phase, messages, .. }) => {
                            out_msgs = messages;
                            // ignore copyleft reward because account is special
                            Some(phase)
                        }
                        Err(e) => fail!(
                            ExecutorError::TrExecutorError(
                                format!(
                                    "cannot create action phase of a new transaction \
                                     for smart contract for reason {}", e
                                )
                            )
                        )
                    }
                } else {
                    log::debug!(target: "executor", "compute_phase: TrComputePhase::Vm failed");
                    None
                }
            }
            ComputePhase::Skipped(ref skipped) => {
                log::debug!(target: "executor",
                    "compute_phase: skipped: reason {:?}", skipped.reason);
                None
            }
        };
        if description.action_phase.as_ref().map_or(false, |a| a.success) {
            if let Some(new_data) = new_data {
                if let Some(Account { state: AccountState::Active(ref mut state), .. }) = account.0 {
                    state.data = Some(new_data)
                }
            }
        }

        description.aborted = match description.action_phase {
            Some(ref phase) => {
                log::debug!(target: "executor",
                    "action_phase: present: success={}, err_code={}", phase.success, phase.result_code);
                !phase.success
            }
            None => {
                log::debug!(target: "executor", "action_phase: none");
                true
            }
        };

        log::debug!(target: "executor", "Desciption.aborted {}", description.aborted);
        tr.end_status = account.status();
        account.0.as_mut().map(|a| a.balance = acc_balance);
        if description.aborted {
            *account = old_account;
        }
        params.last_tr_lt.store(time.tx_lt(), Ordering::Relaxed);
        account.0.as_mut().map(|a| a.last_trans_lt = time.non_aborted_account_lt(out_msgs.len()));
        tr.out_msg_count = Uint15::new(out_msgs.len() as u16);
        for (msg_index, msg) in out_msgs.into_iter().enumerate() {
            tr.out_msgs.set(Uint15::new(msg_index as u16), msg)?;
        }
        tr.info = Lazy::new(&TxInfo::TickTock(description))?;
        Ok(tr)
    }
    fn ordinary_transaction(&self) -> bool { false }
    fn config(&self) -> &PreloadedBlockchainConfig { &self.config }

    fn build_stack(&self, _in_msg: Option<&InputMessage>, account: &OptionalAccount) -> Result<Stack> {
        let account = account.0.as_ref().ok_or_else(|| error!("Tick Tock transaction requires deployed account"))?;
        let account_balance = account.balance.tokens.into_inner();
        let account_id = account.address.as_std().map(|a| a.address)
            .ok_or_else(|| error!("Tick Tock transaction requires Std address"))?;
        let mut stack = Stack::new();
        stack
            .push(int!(account_balance))
            .push(StackItem::integer(IntegerData::from_unsigned_bytes_be(&account_id.0)))
            .push(boolean!(self.tt == TickTock::Tock))
            .push(int!(-2));
        Ok(stack)
    }
}
