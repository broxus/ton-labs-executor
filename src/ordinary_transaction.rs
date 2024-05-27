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

use std::cmp::min;
#[cfg(feature = "timings")]
use std::sync::atomic::AtomicU64;
#[cfg(feature = "timings")]
use std::time::Instant;
use anyhow::Context;

use everscale_types::cell::{Cell, CellBuilder, CellSliceRange};
use everscale_types::models::{Account, AccountState, AccountStatus, AccountStatusChange, BouncePhase, ComputePhase, CurrencyCollection, GlobalCapability, IntAddr, Lazy, MsgInfo, OptionalAccount, OwnedMessage, Transaction, TxInfo};
use everscale_types::num::{Tokens, Uint15};
use everscale_types::prelude::{CellFamily, Load};
use everscale_vm::{
    boolean, int,
    stack::{integer::IntegerData, Stack, StackItem},
};
use everscale_vm::{error, fail, OwnedCellSlice, types::Result};

use crate::{ActionPhaseResult, InputMessage};
use crate::blockchain_config::{MAX_MSG_CELLS, PreloadedBlockchainConfig, VERSION_GLOBAL_REVERT_MESSAGES_WITH_ANYCAST_ADDRESSES};
use crate::error::ExecutorError;
use crate::ext::account::AccountExt;
use crate::ext::extra_currency_collection::ExtraCurrencyCollectionExt;
use crate::transaction_executor::{Common, ExecuteParams, TransactionExecutor};
use crate::utils::{create_tx, default_tx_info, storage_stats, TxTime};

pub struct OrdinaryTransactionExecutor {
    #[cfg(feature = "timings")]
    timings: [AtomicU64; 3], // 0 - preparation, 1 - compute, 2 - after compute
}

impl OrdinaryTransactionExecutor {
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "timings")]
            timings: [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)],
        }
    }

    #[cfg(feature = "timings")]
    pub fn timing(&self, kind: usize) -> u64 {
        self.timings[kind].load(Ordering::Relaxed)
    }
}

impl TransactionExecutor for OrdinaryTransactionExecutor {
    ///
    /// Create end execute transaction from message for account
    fn execute_with_params(
        &self,
        in_msg: Option<&Cell>,
        account: &mut OptionalAccount,
        params: &ExecuteParams,
        config: &PreloadedBlockchainConfig,
    ) -> Result<Transaction> {
        #[cfg(feature = "timings")]
            let mut now = Instant::now();

        let in_msg = in_msg.ok_or_else(|| error!("Ordinary transaction must have input message"))?;
        let in_msg = InputMessage {
            cell: in_msg,
            data: &OwnedMessage::load_from(
                &mut in_msg.as_slice().context("input message as slice")?
            ).context("load input message from slice")?
        };

        let (acc_addr, bounce, is_ext_msg, mut msg_balance);
        match &in_msg.data.info {
            MsgInfo::Int(h) => {
                acc_addr = &h.dst;
                bounce = h.bounce;
                is_ext_msg = false;
                msg_balance = h.value.clone();
                let ihr_delivered = false;  // ihr is disabled because it does not work
                if !ihr_delivered {
                    msg_balance.tokens += h.ihr_fee;
                }
            },
            MsgInfo::ExtIn(h) => {
                acc_addr = &h.dst;
                bounce = false;
                is_ext_msg = true;
                msg_balance = CurrencyCollection::default();
            },
            MsgInfo::ExtOut(_) => fail!(ExecutorError::InvalidExtMessage)
        };
        log::debug!(
            target: "executor",
            "Ordinary transaction executing, in message id: {}",
            in_msg.cell.repr_hash()
        );
        if !account.0.as_ref().map_or(true, |a| a.address == *acc_addr) {
            fail!(ExecutorError::InvalidExtMessage);
        }
        let acc_addr = match acc_addr {
            IntAddr::Std(std) => std,
            _ => fail!(ExecutorError::InvalidExtMessage)
        };
        match &account.0 {
            Some(_) => log::debug!(target: "executor", "Account = {}", acc_addr),
            None => log::debug!(target: "executor", "Account = None, address = {}", acc_addr),
        }

        let time = TxTime::new(&params, account, Some(&in_msg.data.info));
        let mut tr = create_tx(acc_addr.address, account.status(), &time, Some(in_msg.cell));

        let mut description = default_tx_info(bounce);

        if acc_addr.anycast.is_some() && config.global_version().version >=
            VERSION_GLOBAL_REVERT_MESSAGES_WITH_ANYCAST_ADDRESSES {
            description.aborted = true;
            account.0.as_mut().map(|a| a.last_trans_lt = time.tx_lt());
            tr.info = Lazy::new(&TxInfo::Ordinary(description))?;
            return Ok(tr);
        }

        // end of validation

        let mut acc_balance = account.balance().clone();

        log::debug!(target: "executor", "acc_balance: {}, msg_balance: {}, credit_first: {}",
            acc_balance.tokens, msg_balance.tokens, !bounce);

        let is_special = config.is_special_account(&acc_addr);

        // first check if contract can pay for importing external message
        if is_ext_msg && !is_special {
            let in_fwd_fee = config.calc_fwd_fee(acc_addr.is_masterchain(), &storage_stats(&in_msg.data, false, MAX_MSG_CELLS)?)?;
            log::debug!(target: "executor", "import message fee: {}, acc_balance: {}", in_fwd_fee, acc_balance.tokens);
            acc_balance.tokens = acc_balance.tokens.checked_sub(in_fwd_fee).ok_or(ExecutorError::NoFundsToImportMsg)?;
            tr.total_fees.tokens = tr.total_fees.tokens.checked_add(in_fwd_fee).ok_or_else(|| error!("integer overflow"))?;
        }

        if description.credit_first && !is_ext_msg {
            description.credit_phase = match Common::credit_phase(account, &mut tr.total_fees.tokens, &mut msg_balance, &mut acc_balance) {
                Ok(credit_ph) => Some(credit_ph),
                Err(e) => fail!(
                    ExecutorError::TrExecutorError(
                        format!("cannot create credit phase of a new transaction for smart contract for reason {}", e)
                    )
                )
            };
        }

        let storage_fee;
        (description.storage_phase, storage_fee) = Common::storage_phase(
            config,
            account,
            &mut acc_balance,
            &mut tr.total_fees,
            time.now(),
            acc_addr.is_masterchain(),
            is_special
        ).map_err(|e| error!(
            ExecutorError::TrExecutorError(
                format!("cannot create storage phase of a new transaction for smart contract for reason {}", e)
            )
        ))?;

        if description.credit_first {
            msg_balance.tokens = min(msg_balance.tokens, acc_balance.tokens);
        }

        log::debug!(target: "executor",
            "storage_phase: {}", if description.storage_phase.is_some() {"present"} else {"none"});
        // pre-compute phase balance, to be used in "reserve" out actions;
        // does not include msg balance from credit phase. may be initially empty.
        let original_acc_balance = {
            let mut original_acc_balance = account.balance().clone();
            if let Some(tokens) = original_acc_balance.tokens.checked_sub(tr.total_fees.tokens) {
                original_acc_balance.tokens = tokens;
            };
            _ = original_acc_balance.other.try_sub_assign(&tr.total_fees.other)?;
            original_acc_balance
        };

        if !description.credit_first && !is_ext_msg {
            description.credit_phase = match Common::credit_phase(account, &mut tr.total_fees.tokens, &mut msg_balance, &mut acc_balance) {
                Ok(credit_ph) => Some(credit_ph),
                Err(e) => fail!(
                    ExecutorError::TrExecutorError(
                        format!("cannot create credit phase of a new transaction for smart contract for reason {}", e)
                    )
                )
            };
        }
        log::debug!(target: "executor",
            "credit_phase: {}", if description.credit_phase.is_some() {"present"} else {"none"});

        #[cfg(feature = "timings")] {
            self.timings[0].fetch_add(now.elapsed().as_micros() as u64, Ordering::SeqCst);
            now = Instant::now();
        }

        let mut stack = Stack::new();

        stack
            .push(int!(acc_balance.tokens.into_inner()))
            .push(int!(msg_balance.tokens.into_inner()))
            .push(StackItem::Cell(in_msg.cell.clone()))
            // .push(StackItem::Slice(OwnedCellSlice::try_from(in_msg.data.body.clone())?))
            // TODO: uncomment the line above and delete the `push()` below -
            //  it's only to reproduce logs from ton_types, though not truly correct
            .push(StackItem::Slice(OwnedCellSlice::try_from(
                if in_msg.data.body.1.is_data_empty() && in_msg.data.body.1.is_refs_empty() {
                    (Cell::empty_cell(), CellSliceRange::empty())
                } else {
                    in_msg.data.body.clone()
                })?))
            .push(boolean!(is_ext_msg));
        log::debug!(target: "executor", "compute_phase");
        let (actions, new_data);
        (description.compute_phase, actions, new_data) = Common::compute_phase(
            config,
            true,
            Some(&in_msg),
            account,
            &mut acc_balance,
            &msg_balance,
            &time,
            stack,
            storage_fee,
            acc_addr,
            is_special,
            &params,
        ).map_err(|e| {
            log::error!(target: "executor", "compute_phase error: {}", e);
            match e.downcast_ref::<ExecutorError>() {
                Some(ExecutorError::NoAcceptError(_, _)) => e,
                _ => error!(ExecutorError::TrExecutorError(e.to_string()))
            }
        })?;
        // let mut copyleft = None;
        let mut action_phase_acc_balance = acc_balance.clone();
        let (compute_phase_gas_fees, mut out_msgs);
        (description.action_phase, out_msgs) = match &description.compute_phase {
            ComputePhase::Executed(phase) => {
                compute_phase_gas_fees = phase.gas_fees;
                tr.total_fees.tokens = tr.total_fees.tokens.checked_add(compute_phase_gas_fees)
                    .ok_or_else(|| error!("integer overflow"))?;
                if phase.success {
                    log::debug!(target: "executor", "compute_phase: success");
                    log::debug!(target: "executor", "action_phase: lt={}", time.tx_lt());
                    // since the balance is not used anywhere else if we have reached this point,
                    // then we can change it here
                    match Common::action_phase_with_copyleft(
                        config,
                        &mut tr.total_fees.tokens,
                        &time,
                        account,
                        &original_acc_balance,
                        &mut action_phase_acc_balance,
                        &mut msg_balance,
                        &compute_phase_gas_fees,
                        actions.unwrap_or(Cell::empty_cell()),
                        &acc_addr,
                        is_special
                    ) {
                        Ok(ActionPhaseResult{phase, messages, copyleft_reward}) => {
                            if let Some(copyleft_reward) = &copyleft_reward {
                                tr.total_fees.tokens = tr.total_fees.tokens.checked_sub(copyleft_reward.reward)
                                    .ok_or_else(|| error!("integer underflow"))?;
                            }
                            // copyleft = copyleft_reward;
                            (Some(phase),  messages)
                        }
                        Err(e) => fail!(
                            ExecutorError::TrExecutorError(
                                format!("cannot create action phase of a new transaction for smart contract for reason {}", e)
                            )
                        )
                    }
                } else {
                    log::debug!(target: "executor", "compute_phase: failed");
                    (None, vec![])
                }
            }
            ComputePhase::Skipped(skipped) => {
                compute_phase_gas_fees = Tokens::ZERO;
                log::debug!(target: "executor", "compute_phase: skipped reason {:?}", skipped.reason);
                if is_ext_msg {
                    fail!(ExecutorError::ExtMsgComputeSkipped(skipped.reason.clone()))
                }
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

        #[cfg(feature="timings")] {
            self.timings[1].fetch_add(now.elapsed().as_micros() as u64, Ordering::SeqCst);
            now = Instant::now();
        }

        description.aborted = match description.action_phase.as_ref() {
            Some(phase) => {
                log::debug!(
                    target: "executor",
                    "action_phase: present: success={}, err_code={}", phase.success, phase.result_code
                );
                if AccountStatusChange::Deleted == phase.status_change {
                    *account = OptionalAccount::EMPTY;
                    description.destroyed = true;
                }
                if phase.success {
                    acc_balance = action_phase_acc_balance;
                }
                !phase.success
            }
            None => {
                log::debug!(target: "executor", "action_phase: none");
                true
            }
        };

        log::debug!(target: "executor", "Description.aborted {}", description.aborted);
        if description.aborted && !is_ext_msg && bounce {
            if description.action_phase.is_none() || config.global_version().capabilities.contains(GlobalCapability::CapBounceAfterFailedAction) {
                log::debug!(target: "executor", "bounce_phase");
                let msg_index = description.action_phase.as_ref().map_or(0, |a| a.messages_created);
                description.bounce_phase = match Common::bounce_phase(
                    config,
                    msg_balance.clone(),
                    &mut acc_balance,
                    &compute_phase_gas_fees,
                    msg_index as usize,
                    &in_msg.data,
                    &mut tr.total_fees.tokens,
                    &time,
                    &acc_addr,
                    params.block_version,
                ) {
                    Ok((bounce_ph, Some(bounce_msg))) => {
                        out_msgs.push(bounce_msg);
                        Some(bounce_ph)
                    }
                    Ok((bounce_ph, None)) => Some(bounce_ph),
                    Err(e) => fail!(
                        ExecutorError::TrExecutorError(
                            format!("cannot create bounce phase of a new transaction for smart contract for reason {}", e)
                        )
                    )
                };
            }
            // if money can be returned to sender
            // restore account balance - storage fee
            if let Some(BouncePhase::Executed(_)) = description.bounce_phase {
                log::debug!(target: "executor", "restore balance {} => {}", acc_balance.tokens, original_acc_balance.tokens);
                acc_balance = original_acc_balance;
            } else if account.0.is_none() && (acc_balance.tokens.is_zero() && acc_balance.other.is_zero()?) {
                let mut acc = Account::uninit(acc_addr, &acc_balance)?;
                if !is_special { acc.storage_stat.last_paid = time.now() };
                *account = OptionalAccount(Some(acc));
            }
        }
        if account.status() == AccountStatus::Uninit && acc_balance.tokens.is_zero() && acc_balance.other.is_zero()? {
            *account = OptionalAccount::EMPTY;
        }
        log::debug!(target: "executor", "set balance {}", acc_balance.tokens);
        account.0.as_mut().map(|a| a.balance = acc_balance);
        log::debug!(target: "executor", "add messages");
        account.0.as_mut().map(|a| a.last_trans_lt = time.non_aborted_account_lt(out_msgs.len()));
        tr.out_msg_count = Uint15::new(out_msgs.len() as u16);
        for (msg_index, msg) in out_msgs.into_iter().enumerate() {
            tr.out_msgs.set(Uint15::new(msg_index as u16), msg)?;
        }
        tr.info = Lazy::from_raw(CellBuilder::build_from(TxInfo::Ordinary(description))?);
        #[cfg(feature="timings")]
        self.timings[2].fetch_add(now.elapsed().as_micros() as u64, Ordering::SeqCst);
        // tr.set_copyleft_reward(copyleft);
        Ok(tr)
    }
}
