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

use everscale_types::cell::Cell;
use everscale_types::models::{Account, AccountState, ComputePhase, ComputePhaseSkipReason, CurrencyCollection, IntAddr, Lazy, OptionalAccount, SkippedComputePhase, StateInit, TickTock, TickTockTxInfo, Transaction, TxInfo};
use everscale_types::num::{Tokens, Uint15};
use everscale_types::prelude::CellFamily;
use everscale_vm::{
    boolean, int,
    stack::{integer::IntegerData, Stack, StackItem},
};
use everscale_vm::{error, fail, types::Result};

use crate::{ActionPhaseResult, error::ExecutorError, ExecuteParams, TransactionExecutor};
use crate::blockchain_config::PreloadedBlockchainConfig;
use crate::transaction_executor::Common;
use crate::utils::{create_tx, TxTime};

pub struct TickTockTransactionExecutor {
    pub tt: TickTock,
}

impl TickTockTransactionExecutor {
    pub fn new(tt: TickTock) -> Self {
        Self {
            tt,
        }
    }
}

impl TransactionExecutor for TickTockTransactionExecutor {
    ///
    /// Create end execute tick or tock transaction for special account
    fn execute_with_params(
        &self,
        in_msg: Option<&Cell>,
        account: &mut OptionalAccount,
        params: &ExecuteParams,
        config: &PreloadedBlockchainConfig,
    ) -> Result<Transaction> {
        if in_msg.is_some() {
            fail!("Tick Tock transaction must not have input message")
        }
        let (acc_addr, mut acc_balance) = match &account.0 {
            Some(account) => {
                let IntAddr::Std(acc_addr) = &account.address else {
                    fail!("Tick Tock contract should have Standard address")
                };
                match account.state {
                    AccountState::Active(StateInit { special: Some(tt), .. }) =>
                        if tt.tock != (self.tt == TickTock::Tock) && tt.tick != (self.tt == TickTock::Tick) {
                            fail!("wrong type of account's tick tock flag")
                        }
                    _ => fail!("Account {} is not special account for tick tock", acc_addr)
                }
                log::debug!(target: "executor", "tick tock transaction account {}", acc_addr);
                (acc_addr.clone(), account.balance.clone())
            },
            None => fail!("Tick Tock transaction requires deployed account")
        };

        let is_special = true;
        let time = TxTime::new(&params, account, None);
        let mut tr = create_tx(acc_addr.address, account.status(), &time, None);

        let (Some(storage_phase), storage_fee) = Common::storage_phase(
            config,
            account,
            &mut acc_balance,
            &mut tr.total_fees,
            time.now(),
            acc_addr.is_masterchain(),
            is_special,
        ).map_err(|e| error!(
            ExecutorError::TrExecutorError(
                format!(
                    "cannot create storage phase of a new transaction for \
                     smart contract for reason {}", e
                )
            )
        ))? else {
            fail!("Tick Tock transaction storage phase cannot be skipped")
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
        let action_phase_acc_balance = acc_balance.clone();

        let mut stack = Stack::new();
        stack
            .push(int!(acc_balance.tokens.into_inner()))
            .push(StackItem::integer(IntegerData::from_unsigned_bytes_be(&acc_addr.address.0)))
            .push(boolean!(self.tt == TickTock::Tock))
            .push(int!(-2));
        log::debug!(target: "executor", "compute_phase {}", time.tx_lt());
        let (actions, new_data);
        (description.compute_phase, actions, new_data) = Common::compute_phase(
            config,
            false,
            None,
            account,
            &mut acc_balance,
            &CurrencyCollection::default(),
            &time,
            stack,
            storage_fee,
            &acc_addr,
            is_special,
            &params,
        ).map_err(|e| {
            log::error!(target: "executor", "compute_phase error: {}", e);
            match e.downcast_ref::<ExecutorError>() {
                Some(ExecutorError::NoAcceptError(_, _)) => e,
                _ => error!(ExecutorError::TrExecutorError(e.to_string()))
            }
        })?;
        let out_msgs;
        (description.action_phase, out_msgs) = match description.compute_phase {
            ComputePhase::Executed(ref phase) => {
                tr.total_fees.tokens = tr.total_fees.tokens
                    .checked_add(phase.gas_fees).ok_or_else(|| error!("integer overflow"))?;
                if phase.success {
                    log::debug!(target: "executor", "compute_phase: TrComputePhase::Vm success");
                    log::debug!(target: "executor", "action_phase {}", time.tx_lt());
                    match Common::action_phase_with_copyleft(
                        config,
                        &mut tr.total_fees.tokens,
                        &time,
                        account,
                        &action_phase_acc_balance,
                        &mut acc_balance,
                        &mut CurrencyCollection::default(),
                        &Tokens::ZERO,
                        actions.unwrap_or(Cell::empty_cell()),
                        &acc_addr,
                        is_special,
                    ) {
                        Ok(ActionPhaseResult { phase, messages, .. }) => {
                            // ignore copyleft reward because account is special
                            (Some(phase), messages)
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
                    (None, vec![])
                }
            }
            ComputePhase::Skipped(ref skipped) => {
                log::debug!(target: "executor",
                    "compute_phase: skipped: reason {:?}", skipped.reason);
                (None, vec![])
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

        log::debug!(target: "executor", "Description.aborted {}", description.aborted);
        account.0.as_mut().map(|a| a.balance = acc_balance);
        if description.aborted {
            *account = old_account;
        }
        account.0.as_mut().map(|a| a.last_trans_lt = time.non_aborted_account_lt(out_msgs.len()));
        tr.out_msg_count = Uint15::new(out_msgs.len() as u16);
        for (msg_index, msg) in out_msgs.into_iter().enumerate() {
            tr.out_msgs.set(Uint15::new(msg_index as u16), msg)?;
        }
        tr.info = Lazy::new(&TxInfo::TickTock(description))?;
        Ok(tr)
    }
}
