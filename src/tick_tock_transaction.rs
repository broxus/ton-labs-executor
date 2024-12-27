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
use everscale_types::dict::{build_dict_from_sorted_iter, Dict};
use everscale_types::models::{Account, AccountState, ComputePhase, ComputePhaseSkipReason, CurrencyCollection, IntAddr, Lazy, OptionalAccount, SkippedComputePhase, StateInit, TickTock, TickTockTxInfo, Transaction, TxInfo};
use everscale_types::num::{Tokens, Uint15};
use everscale_types::prelude::CellFamily;
use tycho_vm::{tuple, SafeRc, Tuple};
use anyhow::{anyhow, bail, Result};
use num_bigint::{BigInt, Sign};

use crate::{error::ExecutorError, ExecuteParams, TransactionExecutor};
use crate::blockchain_config::PreloadedBlockchainConfig;
use crate::ext::transaction_ext::TransactionExt;
use crate::transaction_executor::{ActionPhaseResult, Common};
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
        min_lt: u64,
        params: &ExecuteParams,
        config: &PreloadedBlockchainConfig,
        unpacked_config: SafeRc<Tuple>,
    ) -> Result<(Transaction, u64)> {
        if in_msg.is_some() {
            bail!("Tick Tock transaction must not have input message")
        }
        let (acc_addr, mut acc_balance) = match &account.0 {
            Some(account) => {
                let IntAddr::Std(acc_addr) = &account.address else {
                    bail!("Tick Tock contract should have Standard address")
                };
                match account.state {
                    AccountState::Active(StateInit { special: Some(tt), .. }) =>
                        if tt.tock != (self.tt == TickTock::Tock) && tt.tick != (self.tt == TickTock::Tick) {
                            bail!("wrong type of account's tick tock flag")
                        }
                    _ => bail!("Account {} is not special account for tick tock", acc_addr)
                }
                tracing::debug!(target: "executor", "tick tock transaction account {}", acc_addr);
                (acc_addr.clone(), account.balance.clone())
            },
            None => bail!("Tick Tock transaction requires deployed account")
        };

        let is_special = true;
        let time = TxTime::new(&params, account, min_lt, None);
        let mut tr = create_tx(acc_addr.address, account.status(), &time, None);

        let (Some(storage_phase), storage_fee) = Common::storage_phase(
            config,
            account,
            &mut acc_balance,
            &mut tr.total_fees,
            time.now(),
            acc_addr.is_masterchain(),
            is_special,
        ).map_err(|e| anyhow!(
            ExecutorError::TrExecutorError(
                format!(
                    "cannot create storage phase of a new transaction for \
                     smart contract for reason {}", e
                )
            )
        ))? else {
            bail!("Tick Tock transaction storage phase cannot be skipped")
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

        let stack = tuple![
            int acc_balance.tokens.into_inner(),
            int BigInt::from_bytes_be(Sign::Plus, acc_addr.address.as_slice()),
            int if self.tt == TickTock::Tock { -1 } else { 0 },
            int -2,
        ];

        tracing::debug!(target: "executor", "compute_phase {}", time.tx_lt());
        let (actions, new_data);
        (description.compute_phase, actions, new_data) = Common::compute_phase(
            config,
            unpacked_config,
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
            tracing::error!(target: "executor", "compute_phase error: {}", e);
            match e.downcast_ref::<ExecutorError>() {
                Some(ExecutorError::NoAcceptError(_)) => e,
                _ => anyhow!(ExecutorError::TrExecutorError(e.to_string()))
            }
        })?;
        let out_msgs;
        (description.action_phase, out_msgs) = match description.compute_phase {
            ComputePhase::Executed(ref phase) => {
                tr.total_fees.tokens = tr.total_fees.tokens
                    .checked_add(phase.gas_fees).ok_or_else(|| anyhow!("integer overflow"))?;
                if phase.success {
                    tracing::debug!(target: "executor", "compute_phase: TrComputePhase::Vm success");
                    tracing::debug!(target: "executor", "action_phase {}", time.tx_lt());
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
                        Err(e) => bail!(
                            ExecutorError::TrExecutorError(
                                format!(
                                    "cannot create action phase of a new transaction \
                                     for smart contract for reason {}", e
                                )
                            )
                        )
                    }
                } else {
                    tracing::debug!(target: "executor", "compute_phase: TrComputePhase::Vm failed");
                    (None, vec![])
                }
            }
            ComputePhase::Skipped(ref skipped) => {
                tracing::debug!(target: "executor",
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
                tracing::debug!(target: "executor",
                    "action_phase: present: success={}, err_code={}", phase.success, phase.result_code);
                !phase.success
            }
            None => {
                tracing::debug!(target: "executor", "action_phase: none");
                true
            }
        };

        tracing::debug!(target: "executor", "Description.aborted {}", description.aborted);
        account.0.as_mut().map(|a| a.balance = acc_balance);
        if description.aborted {
            *account = old_account;
        }
        tr.out_msg_count = Uint15::new(out_msgs.len() as u16);
        tr.out_msgs = Dict::from_raw(build_dict_from_sorted_iter(
            (0..).map(Uint15::new).zip(out_msgs), 15, &mut Cell::empty_context()
        )?);
        account.0.as_mut().map(|a| a.last_trans_lt = tr.account_lt());
        let gas_used = match &description.compute_phase {
            ComputePhase::Skipped(_) => 0,
            ComputePhase::Executed(compute) => compute.gas_used.into_inner(),
        };
        tr.info = Lazy::new(&TxInfo::TickTock(description))?;
        Ok((tr, gas_used))
    }
}
