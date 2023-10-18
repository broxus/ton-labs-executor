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

#[cfg(feature = "timings")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
#[cfg(feature = "timings")]
use std::time::Instant;

use everscale_types::cell::{Cell, CellSliceRange};
use everscale_types::models::{Account, AccountState, AccountStatus, AccountStatusChange, BouncePhase, ComputePhase, CurrencyCollection, GlobalCapability, Lazy, MsgInfo, OptionalAccount, Transaction, TxInfo};
use everscale_types::num::{Tokens, Uint15};
use everscale_types::prelude::CellFamily;
use everscale_vm::{
    boolean, int,
    SmartContractInfo, stack::{integer::IntegerData, Stack, StackItem},
};
use everscale_vm::{error, fail, OwnedCellSlice, types::Result};
use everscale_vm::executor::BehaviorModifiers;

use crate::{ActionPhaseResult, InputMessage};
use crate::blockchain_config::{MAX_MSG_CELLS, PreloadedBlockchainConfig, VERSION_BLOCK_REVERT_MESSAGES_WITH_ANYCAST_ADDRESSES};
use crate::error::ExecutorError;
use crate::ext::account::AccountExt;
use crate::ext::extra_currency_collection::ExtraCurrencyCollectionExt;
use crate::transaction_executor::{ExecuteParams, TransactionExecutor};
use crate::utils::{create_tx, default_tx_info, storage_stats, store, TxTime};

pub struct OrdinaryTransactionExecutor {
    config: PreloadedBlockchainConfig,
    disable_signature_check: bool,

    #[cfg(feature = "timings")]
    timings: [AtomicU64; 3], // 0 - preparation, 1 - compute, 2 - after compute
}

impl OrdinaryTransactionExecutor {
    pub fn new(config: PreloadedBlockchainConfig) -> Self {
        Self {
            config,
            disable_signature_check: false,
            #[cfg(feature = "timings")]
            timings: [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)],
        }
    }

    pub fn set_signature_check_disabled(&mut self, disabled: bool) {
        self.disable_signature_check = disabled;
    }

    #[cfg(feature = "timings")]
    pub fn timing(&self, kind: usize) -> u64 {
        self.timings[kind].load(Ordering::Relaxed)
    }
}

impl TransactionExecutor for OrdinaryTransactionExecutor {
    fn behavior_modifiers(&self) -> BehaviorModifiers {
        BehaviorModifiers {
            chksig_always_succeed: self.disable_signature_check,
        }
    }

    ///
    /// Create end execute transaction from message for account
    fn execute_with_params(
        &self,
        in_msg: Option<&InputMessage>,
        account: &mut OptionalAccount,
        params: &ExecuteParams,
    ) -> Result<Transaction> {
        #[cfg(feature = "timings")]
            let mut now = Instant::now();

        let revert_anycast =
            self.config.global_version().version >= VERSION_BLOCK_REVERT_MESSAGES_WITH_ANYCAST_ADDRESSES;
        let in_msg = in_msg.ok_or_else(|| error!("Ordinary transaction must have input message"))?;

        let (is_masterchain, account_address, bounce, is_ext_msg, mut msg_balance, ihr_fee) = match &in_msg.data.info {
            MsgInfo::Int(h) => (h.dst.is_masterchain(), h.dst.clone(), h.bounce, false, h.value.clone(), h.ihr_fee),
            MsgInfo::ExtIn(h) => (h.dst.is_masterchain(), h.dst.clone(), false, true, CurrencyCollection::default(), Tokens::ZERO),
            MsgInfo::ExtOut(_) => fail!(ExecutorError::InvalidExtMessage)
        };
        log::debug!(
            target: "executor",
            "Ordinary transaction executing, in message id: {}",
            in_msg.cell.repr_hash()
        );
        let Some(account_id) = match account.0.as_ref().map(|a| &a.address) {
            Some(addr) if addr == &account_address => { &account_address },
            None => &account_address,
            _ => fail!(ExecutorError::InvalidExtMessage)
        }.as_std().map(|a| a.address) else {
            fail!(ExecutorError::InvalidExtMessage)
        };
        match &account.0 {
            Some(_) => log::debug!(target: "executor", "Account = {}", account_id),
            None => log::debug!(target: "executor", "Account = None, address = {}", account_id),
        }

        let mut acc_balance = account.balance().clone();
        let ihr_delivered = false;  // ihr is disabled because it does not work
        if !ihr_delivered {
            msg_balance.tokens += ihr_fee;
        }
        log::debug!(target: "executor", "acc_balance: {}, msg_balance: {}, credit_first: {}",
            acc_balance.tokens, msg_balance.tokens, !bounce);

        let is_special = self.config.is_special_account(&account_address);
        let time = TxTime::new(&params, account, Some(&in_msg.data.info));
        let mut tr = create_tx(account_id, account.status(), &time, Some(in_msg.cell));

        let mut description = default_tx_info(!bounce);

        if revert_anycast && account_address.anycast().is_some() {
            description.aborted = true;
            tr.end_status = account.status();
            params.last_tr_lt.store(time.tx_lt(), Ordering::Relaxed);
            account.0.as_mut().map(|a| a.last_trans_lt = time.tx_lt());
            tr.info = Lazy::new(&TxInfo::Ordinary(description))?;
            return Ok(tr);
        }

        // first check if contract can pay for importing external message
        if is_ext_msg && !is_special {
            // external message comes serialized
            let in_fwd_fee = self.config.calc_fwd_fee(is_masterchain, &storage_stats(&in_msg.data, false, MAX_MSG_CELLS)?)?;
            log::debug!(target: "executor", "import message fee: {}, acc_balance: {}", in_fwd_fee, acc_balance.tokens);
            acc_balance.tokens = acc_balance.tokens.checked_sub(in_fwd_fee).ok_or(ExecutorError::NoFundsToImportMsg)?;
            tr.total_fees.tokens = tr.total_fees.tokens.checked_add(in_fwd_fee).ok_or_else(|| error!("integer overflow"))?;
        }

        if description.credit_first && !is_ext_msg {
            description.credit_phase = match self.credit_phase(account, &mut tr.total_fees.tokens, &mut msg_balance, &mut acc_balance) {
                Ok(credit_ph) => Some(credit_ph),
                Err(e) => fail!(
                    ExecutorError::TrExecutorError(
                        format!("cannot create credit phase of a new transaction for smart contract for reason {}", e)
                    )
                )
            };
        }
        let due_before_storage = account.0.as_ref()
            .map(|a| a.storage_stat.due_payment).flatten().map(|a| a.into_inner());
        let mut storage_fee;
        description.storage_phase = match self.storage_phase(
            account,
            &mut acc_balance,
            &mut tr.total_fees,
            time.now(),
            is_masterchain,
            is_special
        ) {
            Ok(storage_ph) => {
                storage_fee = storage_ph.storage_fees_collected.into_inner();
                if let Some(due) = &storage_ph.storage_fees_due {
                    storage_fee += due.into_inner()
                }
                if let Some(due) = due_before_storage {
                    storage_fee -= due;
                }
                Some(storage_ph)
            },
            Err(e) => fail!(
                ExecutorError::TrExecutorError(
                    format!("cannot create storage phase of a new transaction for smart contract for reason {}", e)
                )
            )
        };

        if description.credit_first && msg_balance.tokens > acc_balance.tokens {
            msg_balance.tokens = acc_balance.tokens;
        }

        log::debug!(target: "executor",
            "storage_phase: {}", if description.storage_phase.is_some() {"present"} else {"none"});
        let mut original_acc_balance = account.balance().clone();
        if let Some(a) = original_acc_balance.tokens.checked_sub(tr.total_fees.tokens) { original_acc_balance.tokens = a };
        _ = original_acc_balance.other.try_sub_assign(&tr.total_fees.other)?;

        if !description.credit_first && !is_ext_msg {
            description.credit_phase = match self.credit_phase(account, &mut tr.total_fees.tokens, &mut msg_balance, &mut acc_balance) {
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

        let last_paid = if !is_special { params.block_unixtime } else { 0 };
        account.0.as_mut().map(|a| a.storage_stat.last_paid = last_paid);
        #[cfg(feature = "timings")] {
            self.timings[0].fetch_add(now.elapsed().as_micros() as u64, Ordering::SeqCst);
            now = Instant::now();
        }

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
        smc_info.calc_rand_seed(params.seed_block, &account_id.0);
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
        let (compute_ph, actions, new_data) = match self.compute_phase(
            Some(&in_msg),
            account,
            &mut acc_balance,
            &msg_balance,
            smc_info,
            stack,
            storage_fee,
            is_masterchain,
            is_special,
            &params,
        ) {
            Ok(result_tuple) => result_tuple,
            Err(e) => {
                log::error!(target: "executor", "compute_phase error: {}", e);
                match e.downcast_ref::<ExecutorError>() {
                    Some(ExecutorError::NoAcceptError(_, _)) => return Err(e),
                    _ => fail!(ExecutorError::TrExecutorError(e.to_string()))
                }
            }
        };
        let mut out_msgs = vec![];
        let mut action_phase_processed = false;
        let mut compute_phase_gas_fees = Tokens::ZERO;
        // let mut copyleft = None;
        description.compute_phase = compute_ph;
        let mut new_acc_balance = acc_balance.clone();
        description.action_phase = match &description.compute_phase {
            ComputePhase::Executed(phase) => {
                compute_phase_gas_fees = phase.gas_fees;
                tr.total_fees.tokens = tr.total_fees.tokens.checked_add(phase.gas_fees)
                    .ok_or_else(|| error!("integer overflow"))?;
                if phase.success {
                    log::debug!(target: "executor", "compute_phase: success");
                    log::debug!(target: "executor", "action_phase: lt={}", time.tx_lt());
                    action_phase_processed = true;
                    // since the balance is not used anywhere else if we have reached this point,
                    // then we can change it here
                    match self.action_phase_with_copyleft(
                        &mut tr.total_fees.tokens,
                        &time,
                        account,
                        &original_acc_balance,
                        &mut new_acc_balance,
                        &mut msg_balance,
                        &compute_phase_gas_fees,
                        actions.unwrap_or(Cell::empty_cell()),
                        &account_address,
                        is_special
                    ) {
                        Ok(ActionPhaseResult{phase, messages, copyleft_reward}) => {
                            out_msgs = messages;
                            if let Some(copyleft_reward) = &copyleft_reward {
                                tr.total_fees.tokens = tr.total_fees.tokens.checked_sub(copyleft_reward.reward)
                                    .ok_or_else(|| error!("integer underflow"))?;
                            }
                            // copyleft = copyleft_reward;
                            Some(phase)
                        }
                        Err(e) => fail!(
                            ExecutorError::TrExecutorError(
                                format!("cannot create action phase of a new transaction for smart contract for reason {}", e)
                            )
                        )
                    }
                } else {
                    log::debug!(target: "executor", "compute_phase: failed");
                    None
                }
            }
            ComputePhase::Skipped(skipped) => {
                log::debug!(target: "executor", "compute_phase: skipped reason {:?}", skipped.reason);
                if is_ext_msg {
                    fail!(ExecutorError::ExtMsgComputeSkipped(skipped.reason.clone()))
                }
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
                    acc_balance = new_acc_balance;
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
            if !action_phase_processed || self.config().global_version().capabilities.contains(GlobalCapability::CapBounceAfterFailedAction) {
                log::debug!(target: "executor", "bounce_phase");
                let msg_index = description.action_phase.as_ref().map_or(0, |a| a.messages_created);
                description.bounce_phase = match self.bounce_phase(
                    msg_balance.clone(),
                    &mut acc_balance,
                    &compute_phase_gas_fees,
                    msg_index as usize,
                    &in_msg.data,
                    &mut tr.total_fees.tokens,
                    &time,
                    &account_address,
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
                *account = OptionalAccount(Some(Account::uninit(
                    account_address.clone(),
                    0,
                    last_paid,
                    acc_balance.clone(),
                )?));
            }
        }
        if account.status() == AccountStatus::Uninit && acc_balance.tokens.is_zero() && acc_balance.other.is_zero()? {
            *account = OptionalAccount::EMPTY;
        }
        tr.end_status = account.status();
        log::debug!(target: "executor", "set balance {}", acc_balance.tokens);
        account.0.as_mut().map(|a| a.balance = acc_balance);
        log::debug!(target: "executor", "add messages");
        params.last_tr_lt.store(time.tx_lt(), Ordering::Relaxed);
        account.0.as_mut().map(|a| a.last_trans_lt = time.non_aborted_account_lt(out_msgs.len()));
        tr.out_msg_count = Uint15::new(out_msgs.len() as u16);
        for (msg_index, msg) in out_msgs.into_iter().enumerate() {
            tr.out_msgs.set(Uint15::new(msg_index as u16), msg)?;
        }
        tr.info = Lazy::from_raw(store(TxInfo::Ordinary(description))?);
        #[cfg(feature="timings")]
        self.timings[2].fetch_add(now.elapsed().as_micros() as u64, Ordering::SeqCst);
        // tr.set_copyleft_reward(copyleft);
        Ok(tr)
    }
    fn ordinary_transaction(&self) -> bool { true }
    fn config(&self) -> &PreloadedBlockchainConfig { &self.config }
    fn build_stack(&self, in_msg: Option<&InputMessage>, account: &OptionalAccount) -> Result<Stack> {
        let mut stack = Stack::new();
        let Some(in_msg) = in_msg else { return Ok(stack) };
        let (msg_tokens, is_inbound_external) = match &in_msg.data.info {
            MsgInfo::Int(h) => (h.value.tokens, false),
            MsgInfo::ExtIn(_) => (Tokens::ZERO, true),
            MsgInfo::ExtOut(_) => (Tokens::ZERO, false),
        };
        let acc_balance = int!(account.balance().tokens.into_inner());
        let msg_balance = int!(msg_tokens.into_inner());
        let function_selector = boolean!(is_inbound_external);
        stack
            .push(acc_balance)
            .push(msg_balance)
            .push(StackItem::Cell(in_msg.cell.clone()))
            .push(StackItem::Slice(OwnedCellSlice::try_from(in_msg.data.body.clone())?))
            .push(function_selector);
        Ok(stack)
    }
}
