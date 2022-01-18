/*
* Copyright (C) 2019-2021 TON Labs. All Rights Reserved.
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

use crate::{
    blockchain_config::{BlockchainConfig, CalcMsgFwdFees},
    error::ExecutorError,
    ExecuteParams, TransactionExecutor,
};
#[cfg(feature = "timings")]
use std::sync::atomic::AtomicU64;
use std::sync::{atomic::Ordering, Arc};
#[cfg(feature = "timings")]
use std::time::Instant;
use ton_block::{
    AccStatusChange, Account, AccountStatus, AddSub, CommonMsgInfo, Grams, Message, Serializable,
    TrBouncePhase, TrComputePhase, Transaction, TransactionDescr, TransactionDescrOrdinary, MASTERCHAIN_ID
};
use ton_types::{error, fail, Result};
use ton_vm::{
    boolean, int,
    stack::{integer::IntegerData, Stack, StackItem},
};
use ton_vm::executor::BehaviorModifiers;






pub struct OrdinaryTransactionExecutor {
    config: BlockchainConfig,
    disable_signature_check: bool,

    #[cfg(feature="timings")]
    timings: [AtomicU64; 3], // 0 - preparation, 1 - compute, 2 - after compute
}

impl OrdinaryTransactionExecutor {
    pub fn new(config: BlockchainConfig) -> Self {
        Self {
            config,
            disable_signature_check: false,
            #[cfg(feature="timings")]
            timings: [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)],
        }
    }

    pub fn set_signature_check_disabled(&mut self, disabled: bool) {
        self.disable_signature_check = disabled;
    }

    #[cfg(feature="timings")]
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
        in_msg: Option<&Message>,
        account: &mut Account,
        params: ExecuteParams,
    ) -> Result<Transaction> {
        #[cfg(feature="timings")]
        let mut now = Instant::now();

        let in_msg = in_msg.ok_or_else(|| error!("Ordinary transaction must have input message"))?;
        let in_msg_cell = in_msg.serialize()?; // TODO: get from outside
        let is_masterchain = in_msg.dst_workchain_id() == Some(MASTERCHAIN_ID);
        log::debug!(target: "executor", "Ordinary transaction executing, in message id: {:x}", in_msg_cell.repr_hash());
        let (bounce, is_ext_msg) = match in_msg.header() {
            CommonMsgInfo::ExtOutMsgInfo(_) => fail!(ExecutorError::InvalidExtMessage),
            CommonMsgInfo::IntMsgInfo(ref hdr) => (hdr.bounce, false),
            CommonMsgInfo::ExtInMsgInfo(_) => (false, true)
        };

        let account_address = in_msg.dst_ref().ok_or_else(|| ExecutorError::TrExecutorError(
            format!("Input message {:x} has no dst address", in_msg_cell.repr_hash())
        ))?;
        let account_id = match account.get_id() {
            Some(account_id) => {
                log::debug!(target: "executor", "Account = {:x}", account_id);
                account_id
            }
            None => {
                log::debug!(target: "executor", "Account = None, address = {:x}", account_address.address());
                account_address.address()
            }
        };

        let mut acc_balance = account.balance().cloned().unwrap_or_default();
        let mut msg_balance = in_msg.get_value().cloned().unwrap_or_default();
        let ihr_delivered = false;  // ihr is disabled because it does not work
        if !ihr_delivered {
            msg_balance.grams.0 += in_msg.int_header().map_or(0, |h| h.ihr_fee.0);
        }
        log::debug!(target: "executor", "acc_balance: {}, msg_balance: {}, credit_first: {}",
            acc_balance.grams, msg_balance.grams, !bounce);

        let is_special = self.config.is_special_account(account_address)?;
        let lt = std::cmp::max(
            account.last_tr_time().unwrap_or(0),
            std::cmp::max(params.last_tr_lt.load(Ordering::Relaxed), in_msg.lt().unwrap_or(0) + 1)
        );
        let mut tr = Transaction::with_address_and_status(account_id, account.status());
        tr.set_logical_time(lt);
        tr.set_now(params.block_unixtime);
        tr.set_in_msg_cell(in_msg_cell.clone());

        let mut description = TransactionDescrOrdinary {
            credit_first: !bounce,
            ..TransactionDescrOrdinary::default()
        };

        // first check if contract can pay for importing external message
        if is_ext_msg && !is_special {
            // extranal message comes serialized
            let in_fwd_fee = self.config.get_fwd_prices(is_masterchain).fwd_fee(&in_msg_cell);
            log::debug!(target: "executor", "import message fee: {}, acc_balance: {}", in_fwd_fee, acc_balance.grams);
            if !acc_balance.grams.sub(&in_fwd_fee)? {
                fail!(ExecutorError::NoFundsToImportMsg)
            }
            tr.add_fee_grams(&in_fwd_fee)?;
        }

        if description.credit_first && !is_ext_msg {
            description.credit_ph = match self.credit_phase(account, &mut tr, &mut msg_balance, &mut acc_balance) {
                Ok(credit_ph) => Some(credit_ph),
                Err(e) => fail!(
                    ExecutorError::TrExecutorError(
                        format!("cannot create credit phase of a new transaction for smart contract for reason {}", e)
                    )
                )
            };
        }
        description.storage_ph = match self.storage_phase(
            account,
            &mut acc_balance,
            &mut tr,
            is_masterchain,
            is_special
        ) {
            Ok(storage_ph) => Some(storage_ph),
            Err(e) => fail!(
                ExecutorError::TrExecutorError(
                    format!("cannot create storage phase of a new transaction for smart contract for reason {}", e)
                )
            )
        };

        if description.credit_first && msg_balance.grams.0 > acc_balance.grams.0{
            msg_balance.grams.0 = acc_balance.grams.0;
        }

        log::debug!(target: "executor",
            "storage_phase: {}", if description.storage_ph.is_some() {"present"} else {"none"});
        let mut original_acc_balance = account.balance().cloned().unwrap_or_default();
        original_acc_balance.sub(tr.total_fees())?;

        if !description.credit_first && !is_ext_msg {
            description.credit_ph = match self.credit_phase(account, &mut tr, &mut msg_balance, &mut acc_balance) {
                Ok(credit_ph) => Some(credit_ph),
                Err(e) => fail!(
                    ExecutorError::TrExecutorError(
                        format!("cannot create credit phase of a new transaction for smart contract for reason {}", e)
                    )
                )
            };
        }
        log::debug!(target: "executor",
            "credit_phase: {}", if description.credit_ph.is_some() {"present"} else {"none"});

        let last_paid = if !is_special {params.block_unixtime} else {0};
        account.set_last_paid(last_paid);
        #[cfg(feature="timings")] {
            self.timings[0].fetch_add(now.elapsed().as_micros() as u64, Ordering::SeqCst);
            now = Instant::now();
        }

        let smci = self.build_contract_info(
            &acc_balance, account_address, params.block_unixtime, params.block_lt, lt, params.seed_block
        );
        let mut stack = Stack::new();
        stack
            .push(int!(acc_balance.grams.0))
            .push(int!(msg_balance.grams.0))
            .push(StackItem::Cell(in_msg_cell))
            .push(StackItem::Slice(in_msg.body().unwrap_or_default()))
            .push(boolean!(is_ext_msg));
        log::debug!(target: "executor", "compute_phase");
        let (compute_ph, actions, new_data) = match self.compute_phase(
            Some(in_msg),
            account,
            &mut acc_balance,
            &msg_balance,
            params.state_libs,
            smci,
            stack,
            is_masterchain,
            is_special,
            params.debug
        ) {
            Ok((compute_ph, actions, new_data)) => (compute_ph, actions, new_data),
            Err(e) =>
                if let Some(e) = e.downcast_ref::<ExecutorError>() {
                    match e {
                        ExecutorError::NoAcceptError(num, stack) => fail!(
                            ExecutorError::NoAcceptError(*num, stack.clone())
                        ),
                        _ => fail!("Unknown error")
                    }
                } else {
                    fail!(ExecutorError::TrExecutorError(e.to_string()))
                }
        };
        let mut out_msgs = vec![];
        let mut action_phase_processed = false;
        let mut compute_phase_gas_fees = Grams(0);
        description.compute_ph = compute_ph;
        description.action = match &description.compute_ph {
            TrComputePhase::Vm(phase) => {
                compute_phase_gas_fees = phase.gas_fees.clone();
                tr.add_fee_grams(&phase.gas_fees)?;
                if phase.success {
                    log::debug!(target: "executor", "compute_phase: success");
                    log::debug!(target: "executor", "action_phase: lt={}", lt);
                    action_phase_processed = true;
                    // since the balance is not used anywhere else if we have reached this point,
                    // then we can change it here
                    match self.action_phase(
                        &mut tr,
                        account,
                        &original_acc_balance,
                        &mut acc_balance,
                        &mut msg_balance,
                        &phase.gas_fees,
                        actions.unwrap_or_default(),
                        new_data,
                        is_special
                    ) {
                        Ok((action_ph, msgs)) => {
                            out_msgs = msgs;
                            Some(action_ph)
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
            TrComputePhase::Skipped(skipped) => {
                log::debug!(target: "executor", "compute_phase: skipped reason {:?}", skipped.reason);
                if is_ext_msg {
                    fail!(ExecutorError::ExtMsgComputeSkipped(skipped.reason.clone()))
                }
                None
            }
        };

        #[cfg(feature="timings")] {
            self.timings[1].fetch_add(now.elapsed().as_micros() as u64, Ordering::SeqCst);
            now = Instant::now();
        }

        description.aborted = match description.action.as_ref() {
            Some(phase) => {
                log::debug!(
                    target: "executor",
                    "action_phase: present: success={}, err_code={}", phase.success, phase.result_code
                );
                match phase.status_change {
                    AccStatusChange::Deleted => {
                        *account = Account::default();
                        description.destroyed = true;
                    },
                    _ => ()
                }
                !phase.success
            }
            None => {
                log::debug!(target: "executor", "action_phase: none");
                true
            }
        };

        log::debug!(target: "executor", "Desciption.aborted {}", description.aborted);
        if description.aborted && !is_ext_msg && bounce {
            if !action_phase_processed {
                log::debug!(target: "executor", "bounce_phase");
                let my_addr = account.get_addr().unwrap_or(&in_msg.dst().ok_or_else(|| ExecutorError::TrExecutorError(
                    "Or account address or in_msg dst address should be present".to_string()
                ))?).clone();
                description.bounce = match self.bounce_phase(
                    msg_balance.clone(),
                    &mut acc_balance,
                    &compute_phase_gas_fees,
                    in_msg,
                    &mut tr,
                    &my_addr
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
            if let Some(TrBouncePhase::Ok(_)) = description.bounce {
                log::debug!(target: "executor", "restore balance {} => {}", acc_balance.grams, original_acc_balance.grams);
                acc_balance = original_acc_balance;
                if (account.status() == AccountStatus::AccStateUninit) && acc_balance.is_zero()? {
                    *account = Account::default();
                }
            } else {
                if account.is_none() && !acc_balance.is_zero()? {
                    *account = Account::uninit(
                        in_msg.dst().ok_or_else(|| ExecutorError::TrExecutorError(
                            "cannot create bounce phase of a new transaction for smart contract".to_string()
                        ))?,
                        0,
                        last_paid,
                        acc_balance.clone()
                    );
                }
            }
        }
        if (account.status() == AccountStatus::AccStateUninit) && acc_balance.is_zero()? {
            *account = Account::default();
        }
        tr.set_end_status(account.status());
        log::debug!(target: "executor", "set balance {}", acc_balance.grams);
        account.set_balance(acc_balance);
        log::debug!(target: "executor", "add messages");
        params.last_tr_lt.store(lt, Ordering::Relaxed);
        let lt = self.add_messages(&mut tr, out_msgs, params.last_tr_lt)?;
        account.set_last_tr_time(lt);
        tr.write_description(&TransactionDescr::Ordinary(description))?;
        #[cfg(feature="timings")]
        self.timings[2].fetch_add(now.elapsed().as_micros() as u64, Ordering::SeqCst);
        Ok(tr)
    }
    fn ordinary_transaction(&self) -> bool { true }
    fn config(&self) -> &BlockchainConfig { &self.config }
    fn build_stack(&self, in_msg: Option<&Message>, account: &Account) -> Stack {
        let mut stack = Stack::new();
        let in_msg = match in_msg {
            Some(in_msg) => in_msg,
            None => return stack
        };
        let acc_balance = int!(account.balance().map(|value| value.grams.0).unwrap_or_default());
        let msg_balance = int!(in_msg.get_value().map(|value| value.grams.0).unwrap_or_default());
        let function_selector = boolean!(in_msg.is_inbound_external());
        let body_slice = in_msg.body().unwrap_or_default();
        let in_msg_cell = in_msg.serialize().unwrap_or_default();
        stack
            .push(acc_balance)
            .push(msg_balance)
            .push(StackItem::Cell(in_msg_cell))
            .push(StackItem::Slice(body_slice))
            .push(function_selector);
        stack
    }
}
