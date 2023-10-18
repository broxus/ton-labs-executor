use std::cmp::min;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use ahash::HashMap;
use everscale_types::cell::{CellBuilder, CellTreeStats, DynCell, EMPTY_CELL_HASH, MAX_BIT_LEN, MAX_REF_COUNT};
use everscale_types::dict::Dict;
use everscale_types::models::{Account, AccountState, AccountStatusChange, ActionPhase, BaseMessage, BouncePhase, ChangeLibraryMode, ComputePhase, ComputePhaseSkipReason, CreditPhase, CurrencyCollection, ExecutedBouncePhase, ExtOutMsgInfo, ExtraCurrencyCollection, GasLimitsPrices, GlobalCapabilities, GlobalCapability, HashUpdate, IntAddr, IntMsgInfo, Lazy, LibDescr, LibRef, MsgInfo, NoFundsBouncePhase, OptionalAccount, OutAction, OutActionsRevIter, OwnedMessage, OwnedRelaxedMessage, RelaxedExtOutMsgInfo, RelaxedIntMsgInfo, RelaxedMsgInfo, ReserveCurrencyFlags, SendMsgFlags, ShardIdent, SimpleLib, SkippedComputePhase, StateInit, StdAddr, StorageInfo, StoragePhase, StorageUsedShort, Transaction, VarAddr, WorkchainDescription, WorkchainFormat};
use everscale_types::num::{Tokens, Uint9, VarUint56};
use everscale_types::prelude::{Cell, CellFamily, CellSliceRange, ExactSize, HashBytes, Load};
use everscale_vm::{Fmt, OwnedCellSlice};
use everscale_vm::{error, fail, types::{ExceptionCode, Result}};
use everscale_vm::error::tvm_exception;
use everscale_vm::executor::{BehaviorModifiers, IndexProvider};
use everscale_vm::executor::gas::gas_state::Gas;
use everscale_vm::SmartContractInfo;
use everscale_vm::stack::Stack;

use crate::blockchain_config::{CalcMsgFwdFees, MAX_ACTIONS, MAX_MSG_BITS, MAX_MSG_CELLS, PreloadedBlockchainConfig, VERSION_BLOCK_NEW_CALCULATION_BOUNCED_STORAGE};
use crate::error::ExecutorError;
use crate::ext::account::AccountExt;
use crate::ext::extra_currency_collection::ExtraCurrencyCollectionExt;
use crate::ext::gas_limit_prices::GasLimitsPricesExt;
use crate::utils::{CopyleftReward, default_action_phase, default_vm_phase, storage_stats, store, TxTime};
use crate::vmsetup::{VMSetup, VMSetupContext};

const RESULT_CODE_ACTIONLIST_INVALID:            i32 = 32;
const RESULT_CODE_TOO_MANY_ACTIONS:              i32 = 33;
const RESULT_CODE_UNKNOWN_OR_INVALID_ACTION:     i32 = 34;
const RESULT_CODE_INCORRECT_SRC_ADDRESS:         i32 = 35;
const RESULT_CODE_INCORRECT_DST_ADDRESS:         i32 = 36;
const RESULT_CODE_NOT_ENOUGH_TOKENS:             i32 = 37;
const RESULT_CODE_NOT_ENOUGH_OTHER_CURRENCIES:   i32 = 38;
const RESULT_CODE_INVALID_BALANCE:               i32 = 40;
const RESULT_CODE_BAD_ACCOUNT_STATE:             i32 = 41;
const RESULT_CODE_ANYCAST:                       i32 = 50;
// const RESULT_CODE_NOT_FOUND_LICENSE:             i32 = 51; copyleft
const RESULT_CODE_UNSUPPORTED:                   i32 = !(ExceptionCode::NormalTermination as i32);

#[derive(Eq, PartialEq, Debug)]
pub enum IncorrectCheckRewrite {
    Anycast,
    Other
}

pub struct InputMessage<'a> {
    pub cell: &'a Cell,
    pub data: &'a OwnedMessage,
}

pub struct ExecuteParams {
    pub state_libs: Dict<HashBytes, LibDescr>,
    pub block_unixtime: u32,
    pub block_lt: u64,
    pub seq_no: u32,
    pub last_tr_lt: Arc<AtomicU64>,
    pub seed_block: HashBytes,
    pub debug: bool,
    pub trace_callback: Option<Arc<everscale_vm::executor::TraceCallback>>,
    pub index_provider: Option<Arc<dyn IndexProvider>>,
    pub behavior_modifiers: Option<BehaviorModifiers>,
    pub block_version: u32,
}

pub struct ActionPhaseResult {
    pub phase: ActionPhase,
    pub messages: Vec<Cell>,
    pub copyleft_reward: Option<CopyleftReward>,
}

impl Default for ExecuteParams {
    fn default() -> Self {
        Self {
            state_libs: Dict::new(),
            block_unixtime: 0,
            block_lt: 0,
            seq_no: 0,
            last_tr_lt: Arc::new(AtomicU64::new(0)),
            seed_block: HashBytes::default(),
            debug: false,
            trace_callback: None,
            index_provider: None,
            behavior_modifiers: None,
            block_version: 0,
        }
    }
}

pub trait TransactionExecutor {
    fn behavior_modifiers(&self) -> BehaviorModifiers {
        BehaviorModifiers::default()
    }

    fn execute_with_params(
        &self,
        in_msg: Option<&InputMessage>,
        account: &mut OptionalAccount,
        params: &ExecuteParams,
    ) -> Result<Transaction>;
    fn execute_with_libs_and_params(
        &self,
        in_msg: Option<&InputMessage>,
        account_root: &mut Cell,
        params: &ExecuteParams,
    ) -> Result<Transaction> {
        let old_hash = account_root.repr_hash().clone();
        let mut account = OptionalAccount::load_from(account_root.as_slice()?.as_mut())?;
        let mut transaction = self.execute_with_params(
            in_msg,
            &mut account,
            params,
        )?;
        if let Some(ref mut account) = account.0 {
            if self.config().global_version().capabilities.contains(GlobalCapability::CapFastStorageStat) {
                account.update_storage_stat_fast()?;
            } else {
                account.update_storage_stat()?;
            }
        }
        let new_account_cell = store(&account)?;
        let new_hash = new_account_cell.repr_hash();
        transaction.state_update = Lazy::new(&HashUpdate { old: old_hash, new: *new_hash })?;
        *account_root = new_account_cell;
        Ok(transaction)
    }
/*
    #[deprecated]
    fn build_contract_info(
        &self,
        acc_balance: &CurrencyCollection,
        acc_address: &MsgAddressInt,
        unix_time: u32,
        block_lt: u64,
        trans_lt: u64,
        seed_block: UInt256
    ) -> SmartContractInfo {
        let config_params = self.config().raw_config().config_params.data().cloned();
        let mut smci = SmartContractInfo {
            capabilities: self.config().raw_config().capabilities(),
            myself: SliceData::load_builder(acc_address.write_to_new_cell().unwrap_or_default()).unwrap(),
            block_lt,
            trans_lt,
            unix_time,
            balance: acc_balance.clone(),
            config_params,
            ..Default::default()
        };
        smci.calc_rand_seed(seed_block, &acc_address.address().get_bytestring(0));
        smci
    }
*/
    fn ordinary_transaction(&self) -> bool;
    fn config(&self) -> &PreloadedBlockchainConfig;

    #[deprecated]
    fn build_stack(&self, in_msg: Option<&InputMessage>, account: &OptionalAccount) -> Result<Stack>;

    /// Implementation of transaction's storage phase.
    /// If account does not exist - phase skipped.
    /// Calculates storage fees and substracts them from account balance.
    /// If account balance becomes negative after that, then account is frozen.
    fn storage_phase(
        &self,
        acc_opt: &mut OptionalAccount,
        acc_balance: &mut CurrencyCollection,
        tr_total_fees: &mut CurrencyCollection,
        now: u32,
        is_masterchain: bool,
        is_special: bool
    ) -> Result<StoragePhase> {
        log::debug!(target: "executor", "storage_phase");
        let Some(ref mut acc) = acc_opt.0 else {
            log::debug!(target: "executor", "Account::None");
            return Ok(StoragePhase{
                storage_fees_collected: Tokens::ZERO,
                storage_fees_due: None,
                status_change: AccountStatusChange::Unchanged,
            })
        };
        if now < acc.storage_stat.last_paid {
            fail!("transaction timestamp must be greater then account timestamp")
        }
        if is_special {
            log::debug!(target: "executor", "Special account: AccStatusChange::Unchanged");
            return Ok(StoragePhase {
                storage_fees_collected: Tokens::ZERO,
                storage_fees_due: acc.storage_stat.due_payment,
                status_change: AccountStatusChange::Unchanged,
            })
        }
        let mut fee = self.config().calc_storage_fee(
            &acc.storage_stat,
            is_masterchain,
            now,
        )?;

        if let Some(due_payment) = acc.storage_stat.due_payment {
            fee = fee.checked_add(due_payment).ok_or_else(|| error!("integer overflow"))?;
            acc.storage_stat.due_payment = None;
        }

        if acc_balance.tokens >= fee {
            log::debug!(target: "executor", "acc_balance: {}, storage fee: {}", acc_balance.tokens, fee);
            acc_balance.tokens = acc_balance.tokens.checked_sub(fee).ok_or_else(|| error!("integer underflow"))?;
            tr_total_fees.tokens = tr_total_fees.tokens.checked_add(fee).ok_or_else(|| error!("integer overflow"))?;
            Ok(StoragePhase {
                storage_fees_collected: fee,
                storage_fees_due: None,
                status_change: AccountStatusChange::Unchanged,
            })
        } else {
            log::debug!(target: "executor", "acc_balance: {} is storage fee from total: {}", acc_balance.tokens, fee);
            let storage_fees_collected = std::mem::take(&mut acc_balance.tokens);
            tr_total_fees.tokens = tr_total_fees.tokens.checked_add(storage_fees_collected)
                .ok_or_else(|| error!("integer overflow"))?;
            fee = fee.checked_sub(storage_fees_collected).ok_or_else(|| error!("integer underflow"))?;
            let unchanged = StoragePhase {
                storage_fees_collected,
                storage_fees_due: Some(fee),
                status_change: AccountStatusChange::Unchanged,
            };

            match &acc.state {
                AccountState::Uninit | AccountState::Frozen(_) // need delete
                if fee > Tokens::new(self.config().get_gas_config(is_masterchain).delete_due_limit.into()) => {
                    tr_total_fees.other.try_add_assign(&acc_balance.other)?;
                    *acc_opt = OptionalAccount::EMPTY;
                    acc_balance.other = ExtraCurrencyCollection::new();
                    Ok(StoragePhase { status_change: AccountStatusChange::Deleted, ..unchanged })
                },
                AccountState::Active(state) // need freeze
                if fee > Tokens::new(self.config().get_gas_config(is_masterchain).freeze_due_limit.into()) => {
                    acc.storage_stat.due_payment = Some(fee);
                    let hash = *store(state)?.repr_hash();
                    acc.state = AccountState::Frozen(hash);
                    Ok(StoragePhase { status_change: AccountStatusChange::Frozen, ..unchanged })
                }
                _ => {
                    acc.storage_stat.due_payment = Some(fee);
                    Ok(unchanged)
                }
            }
        }
    }

    /// Implementation of transaction's credit phase.
    /// Increases account balance by the amount that appears in the internal message header.
    /// If account does not exist - phase skipped.
    /// If message is not internal - phase skipped.
    fn credit_phase(
        &self,
        acc: &mut OptionalAccount,
        tr_total_fees_tokens: &mut Tokens,
        msg_balance: &mut CurrencyCollection,
        acc_balance: &mut CurrencyCollection,
    ) -> Result<CreditPhase> {
        let collected = if let Some((Some(due_payment), acc)) = match &mut acc.0 {
            Some(acc) => Some((acc.storage_stat.due_payment, acc)),
            _ => None,
        } {
            let collected = min(msg_balance.tokens, due_payment);
            msg_balance.tokens = msg_balance.tokens.checked_sub(collected)
                .ok_or_else(|| error!("integer underflow"))?;
            acc.storage_stat.due_payment = Some(due_payment.checked_sub(collected)
                .ok_or_else(|| error!("integer underflow"))?)
                .filter(|a| a > &Tokens::ZERO);
            *tr_total_fees_tokens = tr_total_fees_tokens.checked_add(collected)
                .ok_or_else(|| error!("integer overflow"))?;

            Some(collected).filter(|a| a > &Tokens::ZERO)
        } else { None };

        log::debug!(
            target: "executor",
            "credit_phase: add funds {} to {}",
            msg_balance.tokens, acc_balance.tokens
        );
        acc_balance.tokens = acc_balance.tokens.checked_add(msg_balance.tokens)
            .ok_or_else(|| error!("integer overflow"))?;
        acc_balance.other.try_add_assign(&msg_balance.other)?;

        Ok(CreditPhase {due_fees_collected: collected, credit: msg_balance.clone() })
        //TODO: Is it need to credit with ihr_fee value in internal messages?
    }

    /// Implementation of transaction's computing phase.
    /// Evaluates new account state and invokes TVM if account has contract code.
    fn compute_phase(
        &self,
        msg: Option<&InputMessage>,
        acc: &mut OptionalAccount,
        acc_balance: &mut CurrencyCollection,
        msg_balance: &CurrencyCollection,
        mut smc_info: SmartContractInfo,
        stack: Stack,
        storage_fee: u128,
        is_masterchain: bool,
        is_special: bool,
        params: &ExecuteParams,
    ) -> Result<(ComputePhase, Option<Cell>, Option<Cell>)> {
        let mut result_acc = acc.clone();
        let mut vm_phase = default_vm_phase();
        let init_code_hash = self.config().global_version().capabilities.contains(GlobalCapability::CapInitCodeHash);
        let libs_disabled = !self.config().global_version().capabilities.contains(GlobalCapability::CapSetLibCode);
        let is_external = if let Some(msg) = msg {
            if let MsgInfo::Int(header) = &msg.data.info {
                log::debug!(target: "executor", "msg internal, bounce: {}", header.bounce);
                if result_acc.0.is_none() {
                    if let Some(mut new_acc) = account_from_message(msg, msg_balance, true, init_code_hash, libs_disabled).ok().flatten() {
                        if !is_special { new_acc.storage_stat.last_paid = smc_info.unix_time() };
                        result_acc = OptionalAccount(Some(new_acc.clone()));
                        // if there was a balance in message (not bounce), then account state at least become uninit
                        if let AccountState::Active(_) = new_acc.state {
                            new_acc.state = AccountState::Uninit
                            // storage stats will be updated at the end of execution
                        };
                        *acc = OptionalAccount(Some(new_acc));
                    }
                }
                false
            } else {
                log::debug!(target: "executor", "msg external");
                true
            }
        } else {
            debug_assert!(result_acc.0.is_some());
            false
        };
        log::debug!(target: "executor", "acc balance: {}", acc_balance.tokens);
        log::debug!(target: "executor", "msg balance: {}", msg_balance.tokens);
        let is_ordinary = self.ordinary_transaction();
        if acc_balance.tokens.is_zero() {
            log::debug!(target: "executor", "skip computing phase no gas");
            return Ok((ComputePhase::Skipped(SkippedComputePhase { reason: ComputePhaseSkipReason::NoGas }), None, None))
        }
        let gas_config = self.config().get_gas_config(is_masterchain);
        let gas = init_gas(acc_balance.tokens.into_inner(), msg_balance.tokens.into_inner(), is_external, is_special, is_ordinary, gas_config);
        if gas.limit() == 0 && gas.credit() == 0 {
            log::debug!(target: "executor", "skip computing phase no gas");
            return Ok((ComputePhase::Skipped(SkippedComputePhase { reason: ComputePhaseSkipReason::NoGas }), None, None))
        }

        let mut acc_libs;
        if let Some(msg) = msg {
            if let Some(state_init) = &msg.data.init {
                // FIXME must be used only if account is frozen or uninit
                acc_libs = &state_init.libraries;
            }
            if let Some(reason) = compute_new_state(&mut result_acc, acc_balance, &msg, init_code_hash, libs_disabled) {
                if !init_code_hash {
                    *acc = result_acc;
                }
                return Ok((ComputePhase::Skipped(SkippedComputePhase { reason }), None, None))
            }
        };

        vm_phase.gas_credit = match gas.credit() as u32 {
            0 => None,
            value => Some(value.try_into()?)
        };
        vm_phase.gas_limit = (gas.limit() as u64).try_into()?;

        let (code, data) = match result_acc.state() {
            Some(AccountState::Active(state @ StateInit { code: Some(code), .. })) => {
                acc_libs = &state.libraries;
                (code, state.data.clone().unwrap_or(Cell::empty_cell()))
            },
            _ => {
                vm_phase.exit_code = !(ExceptionCode::FatalError as i32);
                if is_external {
                    fail!(ExecutorError::NoAcceptError(vm_phase.exit_code, None))
                } else {
                    vm_phase.exit_arg = None;
                    vm_phase.success = false;
                    vm_phase.gas_fees = Tokens::try_from(if is_special { 0 } else { gas_config.calc_gas_fee(0) })?;
                    acc_balance.tokens = acc_balance.tokens.checked_sub(vm_phase.gas_fees).ok_or_else(|| {
                        log::debug!(target: "executor", "can't sub funds: {} from acc_balance: {}", vm_phase.gas_fees, acc_balance.tokens);
                        error!("can't sub funds: from acc_balance")
                    })?;
                    *acc = result_acc;
                    return Ok((ComputePhase::Executed(vm_phase), None, None));
                }
            }
        };
        smc_info.set_mycode(code.clone());
        smc_info.set_storage_fee(storage_fee);
        if let Some(init_code_hash) = (&result_acc).0.as_ref().map(|a| a.init_code_hash).flatten() {
            smc_info.set_init_code_hash(init_code_hash);
        }
        let mut vm = VMSetup::with_context(
            OwnedCellSlice::new(code.clone())?,
            gas,
            VMSetupContext {
                capabilities: self.config().global_version().capabilities,
                block_version: params.block_version,
                signature_id: self.config().global_id(),
            },
        )
            .set_smart_contract_info(smc_info)?
            .set_stack(stack)
            .set_data(data)?
            .set_libraries(acc_libs, &params.state_libs)?
            .set_debug(params.debug)
            .modify_behavior(params.behavior_modifiers.as_ref())
            .create()?;

        //TODO: set vm_init_state_hash

        if let Some(trace_callback) = params.trace_callback.clone() {
            vm.set_trace_callback(move |engine, info| trace_callback(engine, info));
        }

        let result = vm.execute();
        log::trace!(target: "executor", "execute result: {:?}", result);
        let mut raw_exit_arg = None;
        match result {
            Err(err) => {
                log::debug!(target: "executor", "VM terminated with exception: {}", err);
                let exception = tvm_exception(err)?;
                vm_phase.exit_code = if let Some(code) = exception.custom_code() {
                    code
                } else {
                    match exception.exception_code() {
                        Some(ExceptionCode::OutOfGas) => !(ExceptionCode::OutOfGas as i32), // correct error code according cpp code
                        Some(error_code) => error_code as i32,
                        None => ExceptionCode::UnknownError as i32
                    }
                };
                vm_phase.exit_arg = match exception.value.as_integer().and_then(|value| value.into(std::i32::MIN..=std::i32::MAX)) {
                    Err(_) | Ok(0) => None,
                    Ok(exit_arg) => Some(exit_arg)
                };
                raw_exit_arg = Some(exception.value);
            }
            Ok(exit_code) => vm_phase.exit_code = exit_code
        };
        vm_phase.success = vm.get_committed_state().is_committed();
        log::debug!(target: "executor", "VM terminated with exit code {}", vm_phase.exit_code);

        // calc gas fees
        let gas = vm.gas();
        let credit = gas.credit() as u32;
        //for external messages gas will not be exacted if VM throws the exception and gas_credit != 0
        let used = gas.vm_total_used() as u64;
        vm_phase.gas_used = used.try_into()?;
        if credit != 0 {
            if is_external {
                fail!(ExecutorError::NoAcceptError(vm_phase.exit_code, raw_exit_arg))
            }
            vm_phase.gas_fees = Tokens::ZERO;
        } else { // credit == 0 means contract accepted
            let gas_fees = if is_special { 0 } else { gas_config.calc_gas_fee(used) };
            vm_phase.gas_fees = gas_fees.try_into()?;
        };

        log::debug!(
            target: "executor",
            "gas after: gl: {}, gc: {}, gu: {}, fees: {}",
            gas.limit() as u64, credit, used, vm_phase.gas_fees
        );

        //set mode
        vm_phase.mode = 0;
        vm_phase.vm_steps = vm.steps();
        //TODO: vm_final_state_hash
        log::debug!(target: "executor", "acc_balance: {}, gas fees: {}", acc_balance.tokens, vm_phase.gas_fees);
        acc_balance.tokens = acc_balance.tokens.checked_sub(vm_phase.gas_fees).ok_or_else(|| {
            log::error!(target: "executor", "This situation is unreachable: can't sub funds: {} from acc_balance: {}", vm_phase.gas_fees, acc_balance.tokens);
            error!("can't sub funds: from acc_balance")
        })?;

        let new_data = if let Ok(cell) = vm.get_committed_state().get_root().as_cell() {
            Some(cell.clone())
        } else {
            log::debug!(target: "executor", "invalid contract, it must be cell in c4 register");
            vm_phase.success = false;
            None
        };

        let out_actions = if let Ok(root_cell) = vm.get_committed_state().get_actions().as_cell() {
            Some(root_cell.clone())
        } else {
            log::debug!(target: "executor", "invalid contract, it must be cell in c5 register");
            vm_phase.success = false;
            None
        };

        *acc = result_acc;
        Ok((ComputePhase::Executed(vm_phase), out_actions, new_data))
    }
    /// Implementation of transaction's action phase.
    /// If computing phase is successful then action phase is started.
    /// If TVM invoked in computing phase returned some output actions,
    /// then they will be added to transaction's output message list.
    /// Total value from all outbound internal messages will be collected and
    /// subtracted from account balance. If account has enough funds this
    /// will be succeeded, otherwise action phase is failed, transaction will be
    /// marked as aborted, account changes will be rolled back.
/*
    #[deprecated]
    fn action_phase(
        &self,
        tr: &mut Transaction,
        acc: &mut Account,
        original_acc_balance: &CurrencyCollection,
        acc_balance: &mut CurrencyCollection,
        msg_remaining_balance: &mut CurrencyCollection,
        compute_phase_fees: &Grams,
        actions_cell: Cell,
        new_data: Option<Cell>,
        my_addr: &MsgAddressInt,
        is_special: bool,
    ) -> Result<(TrActionPhase, Vec<Message>)> {
        let result = self.action_phase_with_copyleft(
            tr, acc, original_acc_balance, acc_balance, msg_remaining_balance,
            compute_phase_fees, actions_cell, new_data, my_addr, is_special)?;
        Ok((result.phase, result.messages))
    }
*/
    fn action_phase_with_copyleft(
        &self,
        tr_total_fees_tokens: &mut Tokens,
        time: &TxTime,
        acc: &mut OptionalAccount,
        original_acc_balance: &CurrencyCollection,
        acc_balance: &mut CurrencyCollection,
        msg_remaining_balance: &mut CurrencyCollection,
        compute_phase_fees: &Tokens,
        actions_cell: Cell,
        my_addr: &IntAddr,
        is_special: bool,
    ) -> Result<ActionPhaseResult> {
        let mut acc_copy = acc.clone();
        let mut acc_remaining_balance = acc_balance.clone();
        let mut phase = default_action_phase();
        let mut total_reserved_value = CurrencyCollection::default();
        phase.action_list_hash = *actions_cell.repr_hash();

        let process_err_code = |mut err_code: i32, i: usize, phase: &mut ActionPhase| -> Result<bool> {
            if err_code == RESULT_CODE_UNSUPPORTED {
                err_code = RESULT_CODE_UNKNOWN_OR_INVALID_ACTION;
            }
            if err_code != 0 {
                log::debug!(target: "executor", "action failed: error_code={}", err_code);
                phase.valid = true;
                phase.result_code = err_code;
                if i != 0 {
                    phase.result_arg = Some(i as i32);
                }
                if err_code == RESULT_CODE_NOT_ENOUGH_TOKENS || err_code == RESULT_CODE_NOT_ENOUGH_OTHER_CURRENCIES {
                    phase.no_funds = true;
                }
                Ok(true)
            } else {
                Ok(false)
            }
        };

        let mut reversed_actions = vec![];
        for action in OutActionsRevIter::new(actions_cell.as_slice()?) {
            phase.total_actions += 1;
            if phase.total_actions > MAX_ACTIONS {
                log::debug!(target: "executor", "too many actions, more than {MAX_ACTIONS}");
                phase.result_code = RESULT_CODE_TOO_MANY_ACTIONS;
                return Ok(ActionPhaseResult { phase, messages: vec![], copyleft_reward: None })
            }
            reversed_actions.push(action?);
        }

        let mut account_deleted = false;

        let copyleft_reward = None;
        // Todo copyleft
        // self.copyleft_action_handler(compute_phase_fees, &mut phase, &actions)?;
        // if phase.result_code != 0 {
        //     return Ok(ActionPhaseResult::from_phase(phase));
        // }

        enum EitherMsg<P, R> {
            Postponed(P),
            Ready(R),
        }
        let mut out_msgs_temp = vec![];
        for (action_index, action) in reversed_actions.into_iter().rev().enumerate() {
            log::debug!(target: "executor", "\nAction #{}\nType: {}\nInitial balance: {}",
                action_index,
                action_type(&action),
                balance_to_string(&acc_remaining_balance)
            );
            let mut init_balance = acc_remaining_balance.clone();
            let err_code = match action {
                OutAction::SendMsg{ mode, out_msg } => {
                    let out_msg = out_msg.load()?;
                    if mode.contains(SendMsgFlags::ALL_BALANCE) {
                        out_msgs_temp.push(EitherMsg::Postponed((action_index, mode, out_msg)));
                        log::debug!(target: "executor", "Message with flag `send ALL_BALANCE` will be sent last. Skip it for now.");
                        continue
                    }
                    let result = outmsg_action_handler(
                        &mut phase,
                        time,
                        out_msgs_temp.len(),
                        mode,
                        out_msg,
                        &mut acc_remaining_balance,
                        msg_remaining_balance,
                        compute_phase_fees,
                        self.config(),
                        &self.config().global_version().capabilities,
                        &self.config().workchains(),
                        is_special,
                        my_addr,
                        &total_reserved_value,
                        &mut account_deleted
                    );
                    match result {
                        Ok(out_msg) => {
                            out_msgs_temp.push(EitherMsg::Ready(out_msg));
                            0
                        }
                        Err(code) => code
                    }
                }
                OutAction::ReserveCurrency{ mode, value } => {
                    match reserve_action_handler(mode, &value, original_acc_balance, &mut acc_remaining_balance) {
                        Ok(reserved_value) => {
                            phase.special_actions += 1;
                            match (total_reserved_value.tokens.checked_add(reserved_value.tokens),
                                   total_reserved_value.other.try_add_assign(&reserved_value.other)) {
                                (Some(tokens), Ok(_)) => {
                                    total_reserved_value.tokens = tokens;
                                    0
                                }
                                _ => RESULT_CODE_INVALID_BALANCE
                            }
                        }
                        Err(code) => code
                    }
                }
                OutAction::SetCode{ new_code } => {
                    match setcode_action_handler(&mut acc_copy, new_code) {
                        None => {
                            phase.special_actions += 1;
                            0
                        }
                        Some(code) => code
                    }
                }
                OutAction::ChangeLibrary{ mode, lib}  => {
                    match change_library_action_handler(&mut acc_copy, mode, lib) {
                        None => {
                            phase.special_actions += 1;
                            0
                        }
                        Some(code) => code
                    }
                }
                OutAction::CopyLeft { .. } => RESULT_CODE_UNKNOWN_OR_INVALID_ACTION // { 0 }
            };
            if let Some(a) = init_balance.tokens.checked_sub(acc_remaining_balance.tokens) { init_balance.tokens = a };
            _ = init_balance.other.try_sub_assign(&acc_remaining_balance.other)?;
            log::debug!(target: "executor", "Final balance:   {}\nDelta:           {}",
                balance_to_string(&acc_remaining_balance),
                balance_to_string(&init_balance)
            );
            if process_err_code(err_code, action_index, &mut phase)? {
                return Ok(ActionPhaseResult { phase, messages: vec![], copyleft_reward })
            }
        }

        let mut out_msgs = Vec::with_capacity(out_msgs_temp.len());
        for (msg_index, element) in out_msgs_temp.into_iter().enumerate() {
            match element {
                EitherMsg::Postponed((action_index, mode, out_msg)) => {
                    log::debug!(target: "executor", "\nSend message with all balance:\nInitial balance: {}",
                balance_to_string(&acc_remaining_balance));
                    let result = outmsg_action_handler(
                        &mut phase,
                        time,
                        msg_index,
                        mode,
                        out_msg,
                        &mut acc_remaining_balance,
                        msg_remaining_balance,
                        compute_phase_fees,
                        self.config(),
                        &self.config().global_version().capabilities,
                        &self.config().workchains(),
                        is_special,
                        my_addr,
                        &total_reserved_value,
                        &mut account_deleted,
                    );
                    log::debug!(target: "executor", "Final balance:   {}", balance_to_string(&acc_remaining_balance));
                    let err_code = match result {
                        Ok(out_msg_cell) => {
                            out_msgs.push(out_msg_cell);
                            0
                        }
                        Err(code) => code
                    };
                    if process_err_code(err_code, action_index, &mut phase)? {
                        return Ok(ActionPhaseResult { phase, messages: vec![], copyleft_reward });
                    }
                }
                EitherMsg::Ready(out_msg_cell) => {
                    out_msgs.push(out_msg_cell);
                }
            }
        }

        //calc new account balance
        log::debug!(target: "executor", "\nReturn reserved balance:\nInitial:  {}\nReserved: {}",
            balance_to_string(&acc_remaining_balance),
            balance_to_string(&total_reserved_value)
        );
        match (acc_remaining_balance.tokens.checked_add(total_reserved_value.tokens)
                   .ok_or_else(|| error!("integer overflow")),
               (acc_remaining_balance.other.try_add_assign(&total_reserved_value.other))) {
            (Ok(tokens), Ok(_)) => { acc_remaining_balance.tokens = tokens },
            (Err(err), _) | (_, Err(err)) => {
                log::debug!(target: "executor", "failed to add account balance with reserved value {}", err);
                fail!("failed to add account balance with reserved value {}", err)
            }
        }

        log::debug!(target: "executor", "Final:    {}", balance_to_string(&acc_remaining_balance));

        let fee = phase.total_action_fees.unwrap_or(Tokens::ZERO);
        log::debug!(target: "executor", "Total action fees: {}", fee);
        *tr_total_fees_tokens = tr_total_fees_tokens.checked_add(fee)
            .ok_or_else(|| error!("integer overflow"))?;

        if account_deleted {
            log::debug!(target: "executor", "\nAccount deleted");
            phase.status_change = AccountStatusChange::Deleted;
        }
        phase.valid = true;
        phase.success = true;
        phase.messages_created = out_msgs.len() as u16;
        *acc_balance = acc_remaining_balance;
        *acc = acc_copy;
        Ok(ActionPhaseResult { phase, messages: out_msgs, copyleft_reward })
    }

    /// Implementation of transaction's bounce phase.
    /// Bounce phase occurs only if transaction 'aborted' flag is set and
    /// if inbound message is internal message with field 'bounce=true'.
    /// Generates outbound internal message for original message sender, with value equal
    /// to value of original message minus gas payments and forwarding fees
    /// and empty body. Generated message is added to transaction's output message list.
    fn bounce_phase(
        &self,
        mut remaining_msg_balance: CurrencyCollection,
        acc_balance: &mut CurrencyCollection,
        compute_phase_fees: &Tokens,
        msg_index: usize,
        msg: &OwnedMessage,
        tr_total_fees_tokens: &mut Tokens,
        time: &TxTime,
        my_addr: &IntAddr,
        block_version: u32,
    ) -> Result<(BouncePhase, Option<Cell>)> {
        let MsgInfo::Int(header) = &msg.info else {
            fail!("Not found msg internal header")
        };
        if !header.bounce {
            fail!("Bounce flag not set")
        }
        // create bounced message and swap src and dst addresses
        let mut header = header.clone();
        header.src = std::mem::replace(&mut header.dst, header.src.clone());
        match check_rewrite_dest_addr(&header.dst,
                                      &self.config().global_version().capabilities,
                                      &self.config().workchains(),
                                      my_addr) {
            Ok(new_dst) => {header.dst = new_dst}
            Err(_) => {
                log::warn!(target: "executor", "Incorrect destination address in a bounced message {}", header.dst);
                fail!("Incorrect destination address in a bounced message {}", header.dst)
            }
        }

        let is_masterchain = header.src.is_masterchain() || header.dst.is_masterchain();

        // create header for new bounced message and swap src and dst addresses
        header.ihr_disabled = true;
        header.bounce = false;
        header.bounced = true;
        header.ihr_fee = Tokens::ZERO;

        let mut bounce_msg_body = CellBuilder::new();
        let mut bounce_state_init = None;
        if self.config().global_version().capabilities.contains(GlobalCapability::CapBounceMsgBody) {
            bounce_msg_body.store_u32(-1_i32 as u32)?;
            if let Some(body) = Some(&msg.body.1)
                .filter(|r| !(r.is_data_empty() && r.is_refs_empty()))
                .map(|r| r.apply(&msg.body.0)).transpose()? {
                let mut body_copy = body.clone();
                body_copy.shrink(Some(min(256, body.remaining_bits())), Some(0))?;
                bounce_msg_body.store_slice_data(body_copy)?;
                if self.config().global_version().capabilities.contains(GlobalCapability::CapFullBodyInBounced) {
                    bounce_msg_body.store_reference(store(body)?)?;
                }
            }
            if self.config().global_version().capabilities.contains(GlobalCapability::CapFullBodyInBounced) {
                if let Some(init) = &msg.init {
                    bounce_state_init = Some(init.clone());
                }
            }
        }

        let mut bounce_msg = OwnedMessage {
            info: MsgInfo::Int(header),
            init: bounce_state_init,
            body: if bounce_msg_body.has_capacity(MAX_BIT_LEN, MAX_REF_COUNT as u8) {
                (Cell::empty_cell(), CellSliceRange::empty())
            } else {
                let cell = bounce_msg_body.build()?;
                let range = CellSliceRange::full(cell.as_ref());
                (cell, range)
            },
            layout: None,
        };

        // calculated storage for bounced message is empty
        let (storage, fwd_full_fees) = if block_version >= VERSION_BLOCK_NEW_CALCULATION_BOUNCED_STORAGE {
            let storage = storage_stats(&bounce_msg, false, MAX_MSG_CELLS)?;
            let fwd_full_fees = self.config().calc_fwd_fee(is_masterchain, &storage)?;
            (StorageUsedShort{ cells: VarUint56::new(storage.cell_count), bits: VarUint56::new(storage.bit_count) }, fwd_full_fees)
        } else {
            let fwd_full_fees = self.config().calc_fwd_fee(is_masterchain, &CellTreeStats::ZERO)?;
            (StorageUsedShort { cells: VarUint56::ZERO, bits: VarUint56::ZERO }, fwd_full_fees)
        };
        let fwd_prices = self.config().get_fwd_prices(is_masterchain);
        let fwd_mine_fees = fwd_prices.mine_fee(&fwd_full_fees).ok_or_else(|| error!("integer overflow"))?;
        let fwd_fees = fwd_full_fees - fwd_mine_fees;

        log::debug!(target: "executor", "get fee {} from bounce msg {}", fwd_full_fees, remaining_msg_balance.tokens.into_inner());

        if remaining_msg_balance.tokens < fwd_full_fees + *compute_phase_fees {
            log::debug!(
                target: "executor", "bounce phase - not enough tokens {} to get fwd fee {}",
                remaining_msg_balance.tokens, fwd_full_fees
            );
            return Ok((BouncePhase::NoFunds(NoFundsBouncePhase { msg_size: storage, req_fwd_fees: fwd_full_fees }), None))
        }

        if let Some(a) = acc_balance.tokens.checked_sub(remaining_msg_balance.tokens) { acc_balance.tokens = a };
        _ = acc_balance.other.try_sub_assign(&remaining_msg_balance.other)?;
        remaining_msg_balance.tokens = remaining_msg_balance.tokens.checked_sub(fwd_full_fees)
            .ok_or_else(|| error!("integer underflow"))?;
        remaining_msg_balance.tokens = remaining_msg_balance.tokens.checked_sub(*compute_phase_fees)
            .ok_or_else(|| error!("integer underflow"))?;

        if let MsgInfo::Int(ref mut header) = bounce_msg.info {
            header.value = remaining_msg_balance;
            header.fwd_fee = fwd_fees;
            log::debug!(
                target: "executor",
                "bounce fees: {} bounce value: {}",
                fwd_mine_fees, header.value.tokens
            );
        } else {
            unreachable!("bounce phase creates only one internal message")
        }
        time.set_msg_time(&mut bounce_msg.info, msg_index);
        *tr_total_fees_tokens = tr_total_fees_tokens.checked_add(fwd_mine_fees)
            .ok_or_else(|| error!("integer overflow"))?;

        Ok((BouncePhase::Executed(ExecutedBouncePhase {
            msg_size: storage,
            msg_fees: fwd_mine_fees,
            fwd_fees,
        }), Some(store(&bounce_msg)?)))
    }
/*
    fn copyleft_action_handler(
        &self,
        compute_phase_fees: &Grams,
        mut phase: &mut TrActionPhase,
        actions: &LinkedList<OutAction>,
    ) -> Result<Option<CopyleftReward>> {
        let mut copyleft_reward = Grams::zero();
        let mut copyleft_address = AccountId::default();
        let mut was_copyleft_instruction = false;

        for (i, action) in actions.iter().enumerate() {
            if let OutAction::CopyLeft { license, address } = action {
                if was_copyleft_instruction {
                    fail!("Duplicated copyleft action")
                }
                let copyleft_config = self.config().raw_config().copyleft_config()?;
                phase.spec_actions += 1;
                if let Some(copyleft_percent) = copyleft_config.license_rates.get(license)? {
                    if copyleft_percent >= 100 {
                        fail!(
                            "copyleft percent on license {} is too big {}",
                            license, copyleft_percent
                        )
                    }
                    log::debug!(
                        target: "executor",
                        "Found copyleft action: license: {}, address: {}",
                        license, address
                    );
                    copyleft_reward = (*compute_phase_fees * copyleft_percent as u128) / 100;
                    copyleft_address = address.clone();
                    was_copyleft_instruction = true;
                } else {
                    log::debug!(target: "executor", "Not found license {} in config", license);
                    phase.result_code = RESULT_CODE_NOT_FOUND_LICENSE;
                    phase.result_arg = Some(i as i32);
                    return Ok(None)
                }
            }
        }

        if was_copyleft_instruction && !copyleft_reward.is_zero() {
            Ok(Some(CopyleftReward{reward: copyleft_reward, address: copyleft_address}))
        } else {
            Ok(None)
        }
    }
*/
}

/// Calculate new account state according to inbound message and current account state.
/// If account does not exist - it can be created with uninitialized state.
/// If account is uninitialized - it can be created with active state.
/// If account exists - it can be frozen.
/// Returns computed initial phase.
fn compute_new_state(
    acc: &mut OptionalAccount,
    acc_balance: &CurrencyCollection,
    in_msg: &InputMessage,
    init_code_hash: bool,
    disable_set_lib: bool,
) -> Option<ComputePhaseSkipReason> {
    log::debug!(target: "executor", "compute_account_state");
    let Some(ref mut acc) = acc.0 else {
        log::error!(target: "executor", "account must exist");
        return Some(if in_msg.data.init.is_none() { ComputePhaseSkipReason::NoState } else { ComputePhaseSkipReason::BadState });
    };
    match acc.state {
        //Account exists, but can be in different states.
        AccountState::Active(_) => {
            //account is active, just return it
            log::debug!(target: "executor", "account state: AccountActive");
            None
        }
        AccountState::Uninit => {
            log::debug!(target: "executor", "AccountUninit");
            if let Some(state_init) = &in_msg.data.init {
                // if msg is a constructor message then
                // borrow code and data from it and switch account state to 'active'.
                log::debug!(target: "executor", "message for uninitialized: activated");
                let text = "Cannot construct account from message with hash";
                if !check_libraries(state_init, disable_set_lib, text, in_msg.cell.as_ref()) {
                    return Some(ComputePhaseSkipReason::BadState);
                }
                match try_activate_by_init_code_hash(acc, state_init, init_code_hash) {
                    Err(err) => {
                        log::debug!(target: "executor", "reason: {}", err);
                        Some(ComputePhaseSkipReason::BadState)
                    }
                    Ok(_) => None
                }
            } else {
                log::debug!(target: "executor", "message for uninitialized: skip computing phase");
                Some(ComputePhaseSkipReason::NoState)
            }
        }
        AccountState::Frozen(_) => {
            log::debug!(target: "executor", "AccountFrozen");
            //account balance was credited and if it positive after that
            //and inbound message bear code and data then make some check and unfreeze account
            if !acc_balance.tokens.is_zero() { // This check is redundant
                if let Some(state_init) = &in_msg.data.init {
                    let text = "Cannot unfreeze account from message with hash";
                    if !check_libraries(state_init, disable_set_lib, text, in_msg.cell.as_ref()) {
                        return Some(ComputePhaseSkipReason::BadState);
                    }
                    log::debug!(target: "executor", "message for frozen: activated");
                    return match try_activate_by_init_code_hash(acc, state_init, init_code_hash) {
                        Err(err) => {
                            log::debug!(target: "executor", "reason: {}", err);
                            Some(ComputePhaseSkipReason::BadState)
                        }
                        Ok(_) => None
                    }
                }
            }
            //skip computing phase, because account is frozen (bad state)
            log::debug!(target: "executor", "account is frozen (bad state): skip computing phase");
            Some(ComputePhaseSkipReason::NoState)
        }
    }
}

/// Try to activate account with new StateInit
pub fn try_activate_by_init_code_hash(
    stuff: &mut Account,
    state_init: &StateInit,
    init_code_hash: bool
) -> Result<()> {
    let (activate, init_code_hash) = match &stuff.state {
        AccountState::Uninit => {
            if store(state_init).ok().map(|a| *a.repr_hash()) == stuff.address.as_std().map(|a| a.address) {
                (true, init_code_hash)
            } else {
                fail!("StateInit doesn't correspond to uninit account address")
            }
        },
        AccountState::Frozen(state_init_hash) => {
            if store(state_init).ok().map(|a| *a.repr_hash()) == Some(*state_init_hash){
                (true, false)
            } else {
                fail!("StateInit doesn't correspond to frozen hash")
            }
        },
        _ => (false, false)
    };
    if activate { stuff.state = AccountState::Active(state_init.clone()); }
    if init_code_hash { stuff.init_code_hash = state_init.code.as_ref().map(|a| *a.repr_hash()); }
    Ok(())
}

fn check_replace_src_addr(info: RelaxedMsgInfo, acc_addr: &IntAddr) -> std::result::Result<MsgInfo, IntAddr> {
    match info {
        RelaxedMsgInfo::Int(RelaxedIntMsgInfo {
                                ihr_disabled,
                                bounce,
                                bounced,
                                src,
                                dst,
                                value,
                                ihr_fee,
                                fwd_fee,
                                created_lt,
                                created_at
                            }) => {
            let src = match src {
                None => acc_addr.clone(),
                Some(src) if &src == acc_addr => src,
                Some(unknown) => return Err(unknown)
            };
            Ok(MsgInfo::Int(IntMsgInfo {
                ihr_disabled,
                bounce,
                bounced,
                src,
                dst,
                value,
                ihr_fee,
                fwd_fee,
                created_lt,
                created_at,
            }))
        },
        RelaxedMsgInfo::ExtOut(RelaxedExtOutMsgInfo {
                                   src,
                                   dst,
                                   created_lt,
                                   created_at
                               }) => {
            let src = match src {
                None => acc_addr.clone(),
                Some(src) if &src == acc_addr => src,
                Some(unknown) => return Err(unknown)
            };
            Ok(MsgInfo::ExtOut(ExtOutMsgInfo {
                src,
                dst,
                created_lt,
                created_at,
            }))
        }
    }
}

fn is_masterchain(msg_info: &MsgInfo) -> bool {
    match msg_info {
        MsgInfo::Int(h) => h.src.is_masterchain() || h.dst.is_masterchain(),
        MsgInfo::ExtIn(h) => h.dst.is_masterchain(),
        MsgInfo::ExtOut(h) => h.src.is_masterchain(),
    }
}

fn is_valid_addr_len(addr_len: u16, min_addr_len: u16, max_addr_len: u16, addr_len_step: u16) -> bool {
    (addr_len >= min_addr_len) && (addr_len <= max_addr_len) &&
    ((addr_len == min_addr_len) || (addr_len == max_addr_len) ||
    ((addr_len_step != 0) && ((addr_len - min_addr_len) % addr_len_step == 0)))
}

fn check_rewrite_dest_addr(
    dst: &IntAddr,
    capabilities: &GlobalCapabilities,
    workchains: &HashMap<i32, WorkchainDescription>,
    my_addr: &IntAddr
) -> std::result::Result<IntAddr, IncorrectCheckRewrite> {
    let (anycast, addr_len, workchain, address, repack);
    match dst {
        IntAddr::Var(dst) => {
            repack = dst.address_len.into_inner() == 256 && (-128..128).contains(&dst.workchain);
            anycast = dst.anycast.clone();
            addr_len = dst.address_len.into_inner();
            workchain = dst.workchain;
            address = dst.address.clone();
        }
        IntAddr::Std(dst) => {
            repack = false;
            anycast = dst.anycast.clone();
            addr_len = 256;
            workchain = dst.workchain as i32;
            address = dst.address.0.to_vec();
        }
    }

    let cap_workchains = capabilities.contains(GlobalCapability::CapWorkchains);
    let is_masterchain = workchain == ShardIdent::MASTERCHAIN.workchain();
    if !is_masterchain {
        if !cap_workchains &&
           my_addr.workchain() != workchain && my_addr.is_masterchain() == false
        {
            log::debug!(
                target: "executor",
                "cannot send message from {} to {} it doesn't allow yet",
                my_addr, dst
            );
            return Err(IncorrectCheckRewrite::Other);
        }
        if let Some(wc) = workchains.get(&workchain) {
            if !wc.accept_msgs {
                log::debug!(
                    target: "executor",
                    "destination address belongs to workchain {} not accepting new messages",
                    workchain
                );
                return Err(IncorrectCheckRewrite::Other);
            }
            let (min_addr_len, max_addr_len, addr_len_step) = match wc.format {
                WorkchainFormat::Extended(wf) =>
                    (wf.min_addr_len.into_inner(), wf.max_addr_len.into_inner(), wf.addr_len_step.into_inner()),
                WorkchainFormat::Basic(_) => (256, 256, 0)
            };
            if !is_valid_addr_len(addr_len, min_addr_len, max_addr_len, addr_len_step) {
                log::debug!(
                    target: "executor",
                    "destination address has length {} invalid for destination workchain {}",
                    addr_len, workchain
                );
                return Err(IncorrectCheckRewrite::Other);
            }
        } else {
            log::debug!(
                target: "executor",
                "destination address contains unknown workchain_id {}",
                workchain
            );
            return Err(IncorrectCheckRewrite::Other);
        }
    } else {
        if !cap_workchains &&
           my_addr.is_masterchain() == false && my_addr.workchain() != ShardIdent::BASECHAIN.workchain()
        {
            log::debug!(
                target: "executor",
                "masterchain cannot accept from {} workchain",
                my_addr.workchain()
            );
            return Err(IncorrectCheckRewrite::Other);
        }
        if addr_len != 256 {
            log::debug!(
                target: "executor",
                "destination address has length {} invalid for destination workchain {}",
                addr_len, workchain
            );
            return Err(IncorrectCheckRewrite::Other);
        }
    }

    if anycast.is_some() {
        log::debug!(target: "executor", "address cannot be anycast");
        return Err(IncorrectCheckRewrite::Anycast);
        /*
        if is_masterchain {
            log::debug!(target: "executor", "masterchain address cannot be anycast");
            return None
        }
        match src.address().get_slice(0, anycast.depth.as_usize()) {
            Ok(pfx) => {
                if pfx != anycast.rewrite_pfx {
                    match AnycastInfo::with_rewrite_pfx(pfx) {
                        Ok(anycast) => {
                            repack = true;
                            anycast_opt = Some(anycast)
                        }
                        Err(err) => {
                            log::debug!(target: "executor", "Incorrect anycast prefix {}", err);
                            return None
                        }
                    }
                }
            }
            Err(err) => {
                log::debug!(target: "executor", "Incorrect src address {}", err);
                return None
            }
        }
         */
    }

    if !repack {
        Ok(dst.clone())
    } else if addr_len == 256 && (-128..128).contains(&workchain) {
        // repack as an addr_std
        let address = HashBytes::from_slice(&address);
        Ok(IntAddr::Std(StdAddr { anycast, workchain: workchain as i8, address }))
    } else {
        // repack as an addr_var
        let address_len = Uint9::try_from(addr_len).map_err(|_| {
            IncorrectCheckRewrite::Other
        })?;
        Ok(IntAddr::Var(VarAddr { anycast, address_len, workchain, address }))
    }
}

fn outmsg_action_handler(
    phase: &mut ActionPhase,
    time: &TxTime,
    msg_index: usize,
    mut mode: SendMsgFlags,
    BaseMessage { info, init, body, layout }: OwnedRelaxedMessage,
    acc_balance: &mut CurrencyCollection,
    msg_balance: &mut CurrencyCollection,
    compute_phase_fees: &Tokens,
    config: &PreloadedBlockchainConfig,
    capabilities: &GlobalCapabilities,
    workchains: &HashMap<i32, WorkchainDescription>,
    is_special: bool,
    my_addr: &IntAddr,
    reserved_value: &CurrencyCollection,
    account_deleted: &mut bool
) -> std::result::Result<Cell, i32> {
    if mode.intersects(!SendMsgFlags::all()) ||
        // we cannot send all balance from account and from message simultaneously ?
        mode.contains(SendMsgFlags::WITH_REMAINING_BALANCE | SendMsgFlags::ALL_BALANCE) ||
        (mode.contains(SendMsgFlags::DELETE_IF_EMPTY) && !mode.contains(SendMsgFlags::ALL_BALANCE))
    {
        log::error!(target: "executor", "outmsg mode has unsupported flags");
        return Err(RESULT_CODE_UNSUPPORTED);
    }
    let skip = if mode.contains(SendMsgFlags::IGNORE_ERROR) {
        None
    } else {
        Some(())
    };
    let fwd_mine_fee: Tokens;
    let total_fwd_fees: Tokens;
    let mut send_value; // to sub from acc_balance

    let mut msg = match check_replace_src_addr(info, my_addr) {
        Ok(info) => {
            // Drop old flags if it will be impossible to serialize with them.
            // Thus computing layout every time will sometimes reduce msg size.
            let layout = layout.filter(|layout| {
                let size = layout.compute_full_size(info.exact_size(), init.as_ref(), body.exact_size());
                size.bits <= MAX_BIT_LEN && size.refs <= MAX_REF_COUNT as u8
            });
            OwnedMessage { info, init, body, layout }
        }
        Err(addr) => {
            log::warn!(target: "executor", "Incorrect source address {:?}", addr);
            return Err(RESULT_CODE_INCORRECT_SRC_ADDRESS);
        }
    };

    let is_masterchain = is_masterchain(&msg.info);

    let fwd_prices = config.get_fwd_prices(is_masterchain);
    let compute_fwd_fee = if is_special {
        Tokens::ZERO
    } else {
        storage_stats(&msg, false, MAX_MSG_CELLS).map_err(|e| e.into())
            .and_then(|a| config.calc_fwd_fee(is_masterchain, &a))
            .map_err(|err| {
                log::error!(target: "executor", "cannot serialize message in action phase : {}", err);
                RESULT_CODE_ACTIONLIST_INVALID
            })?
    };
    if let MsgInfo::Int(ref mut int_header) = &mut msg.info {
        match check_rewrite_dest_addr(&int_header.dst, capabilities, workchains, my_addr) {
            Ok(new_dst) => { int_header.dst = new_dst }
            Err(type_error) => {
                return if type_error == IncorrectCheckRewrite::Anycast {
                    log::warn!(target: "executor", "Incorrect destination anycast address {}", int_header.dst);
                    Err(skip.map(|_| RESULT_CODE_ANYCAST).unwrap_or_default())
                } else {
                    log::warn!(target: "executor", "Incorrect destination address {}", int_header.dst);
                    Err(skip.map(|_| RESULT_CODE_INCORRECT_DST_ADDRESS).unwrap_or_default())
                }
            }
        }

        int_header.bounced = false;
        send_value = int_header.value.clone();

        if cfg!(feature = "ihr_disabled") {
            int_header.ihr_disabled = true;
        }
        if !int_header.ihr_disabled {
            let compute_ihr_fee = fwd_prices.ihr_fee(&compute_fwd_fee).ok_or(RESULT_CODE_UNSUPPORTED)?;
            if int_header.ihr_fee < compute_ihr_fee {
                int_header.ihr_fee = compute_ihr_fee
            }
        } else {
            int_header.ihr_fee = Tokens::ZERO;
        }
        let fwd_fee = *std::cmp::max(&int_header.fwd_fee, &compute_fwd_fee);
        fwd_mine_fee = fwd_prices.mine_fee(&fwd_fee).ok_or(RESULT_CODE_UNSUPPORTED)?;
        total_fwd_fees = fwd_fee + int_header.ihr_fee;

        let fwd_remain_fee = fwd_fee - fwd_mine_fee;
        if mode.contains(SendMsgFlags::ALL_BALANCE) {
            //send all remaining account balance
            send_value = acc_balance.clone();
            int_header.value = acc_balance.clone();

            mode &= !SendMsgFlags::PAY_FEE_SEPARATELY;
        }
        if mode.contains(SendMsgFlags::WITH_REMAINING_BALANCE) {
            //send all remainig balance of inbound message
            if let Some(tokens) = send_value.tokens.checked_add(msg_balance.tokens) {
                send_value.tokens = tokens;
            }
            send_value.other.try_add_assign(&msg_balance.other).ok();
            if mode.contains(SendMsgFlags::PAY_FEE_SEPARATELY) == false {
                if &send_value.tokens < compute_phase_fees {
                    return Err(skip.map(|_| RESULT_CODE_NOT_ENOUGH_TOKENS).unwrap_or_default())
                }
                send_value.tokens = send_value.tokens.checked_sub(*compute_phase_fees).ok_or_else(|| {
                    log::error!(target: "executor", "cannot subtract msg balance: integer underflow");
                    RESULT_CODE_ACTIONLIST_INVALID
                })?;
            }
            int_header.value = send_value.clone();
        }
        if mode.contains(SendMsgFlags::PAY_FEE_SEPARATELY) {
            //we must pay the fees, sum them with msg value
            send_value.tokens += total_fwd_fees;
        } else if int_header.value.tokens < total_fwd_fees {
            //msg value is too small, reciever cannot pay the fees
            log::warn!(
                target: "executor",
                "msg balance {} is too small, cannot pay fwd+ihr fees: {}",
                int_header.value.tokens, total_fwd_fees
            );
            return Err(skip.map(|_| RESULT_CODE_NOT_ENOUGH_TOKENS).unwrap_or_default())
        } else {
            //receiver will pay the fees
            int_header.value.tokens -= total_fwd_fees;
        }

        //set evaluated fees and value back to msg
        int_header.fwd_fee = fwd_remain_fee;
    } else if let MsgInfo::ExtOut(_) = &msg.info {
        fwd_mine_fee = compute_fwd_fee;
        total_fwd_fees = compute_fwd_fee;
        send_value = CurrencyCollection::new(compute_fwd_fee.into_inner());
    } else {
        return Err(RESULT_CODE_UNSUPPORTED)
    }

    if let Some(tokens) = acc_balance.tokens.checked_sub(send_value.tokens) {
        acc_balance.tokens = tokens;
    } else {
        log::warn!(
            target: "executor",
            "account balance {} is too small, cannot send {}", acc_balance.tokens, send_value.tokens
        );
        return Err(skip.map(|_| RESULT_CODE_NOT_ENOUGH_TOKENS).unwrap_or_default());
    };

    if let Err(_) | Ok(false) = acc_balance.other.try_sub_assign(&send_value.other) {
        log::warn!(
            target: "executor",
            "account balance {:?} is too small, cannot send {:?}", acc_balance.other, send_value.other
        );
        return Err(skip.map(|_| RESULT_CODE_NOT_ENOUGH_OTHER_CURRENCIES).unwrap_or_default())
    }

    /* Note: with move to new cells balance cannot have zero value in dict of other tokens
    let mut acc_balance_copy = ExtraCurrencyCollection::default();
    match acc_balance.other.iterate_with_keys(|key: u32, b| -> Result<bool> {
        if !b.is_zero() {
            acc_balance_copy.set(&key, &b)?;
        }
        Ok(true)
    }) {
        Ok(false) | Err(_) => {
            log::warn!(target: "executor", "Cannot reduce account extra balance");
            return Err(skip.map(|_| RESULT_CODE_INVALID_BALANCE).unwrap_or_default())
        }
        _ => ()
    }
    std::mem::swap(&mut acc_balance.other, &mut acc_balance_copy);
    */

    if mode.contains(SendMsgFlags::DELETE_IF_EMPTY)
        && mode.contains(SendMsgFlags::ALL_BALANCE)
        && acc_balance.tokens.is_zero()
        && reserved_value.tokens.is_zero() {
        *account_deleted = true;
    }

    // total fwd fees is sum of messages full fwd and ihr fees
    phase.total_fwd_fees = phase.total_fwd_fees
        .map_or(Some(total_fwd_fees), |a| a.checked_add(total_fwd_fees))
        .filter(|a| !a.is_zero() && a.is_valid());

    // total action fees is sum of messages fwd mine fees
    phase.total_action_fees = phase.total_action_fees
        .map_or(Some(fwd_mine_fee), |a| a.checked_add(fwd_mine_fee))
        .filter(|a| !a.is_zero() && a.is_valid());

    time.set_msg_time(&mut msg.info, msg_index);

    // FIXME this is actually not correct, but otherwise will sometimes CellOverflow in builder
    msg.layout = (&msg).layout.filter(|layout| {
        let size = layout.compute_full_size((&msg).info.exact_size(), (&msg).init.as_ref(), (&msg).body.exact_size());
        size.bits <= MAX_BIT_LEN && size.refs <= MAX_REF_COUNT as u8
    }).clone();

    let (msg_cell, stats) = store(&msg)
        .and_then(|cell|
            cell.compute_unique_stats(MAX_MSG_CELLS)
                .ok_or(everscale_types::error::Error::CellOverflow)
                .map(|stats| (cell, stats))
        )
        .map_err(|err| {
            log::error!(target: "executor", "cannot serialize message in action phase : {}", err);
            RESULT_CODE_ACTIONLIST_INVALID
        })?;
    phase.total_message_size = StorageUsedShort {
        cells: phase.total_message_size.cells + stats.cell_count,
        bits: phase.total_message_size.bits + stats.bit_count,
    };

    if phase.total_message_size.bits.into_inner() as usize > MAX_MSG_BITS ||
        phase.total_message_size.cells.into_inner() as usize > MAX_MSG_CELLS {
        log::warn!(target: "executor", "total messages too large: bits: {}, cells: {}",
            phase.total_message_size.bits, phase.total_message_size.cells);
        return Err(RESULT_CODE_ACTIONLIST_INVALID);
    }

    if mode.contains(SendMsgFlags::ALL_BALANCE | SendMsgFlags::WITH_REMAINING_BALANCE) {
        *msg_balance = CurrencyCollection::default();
    }

    if log::log_enabled!(target: "executor", log::Level::Debug) {
        let (src, dst) = match &msg.info {
            MsgInfo::Int(h) => (h.src.to_string(), h.dst.to_string()),
            MsgInfo::ExtIn(_) => ("None".to_string(), "None".to_string()),
            MsgInfo::ExtOut(h) => (h.src.to_string(), "None".to_string()),
        };
        let body = if msg.body.1.is_data_empty() && msg.body.1.is_refs_empty() {
            "None".to_string()
        } else {
            format!("{}", Fmt(&msg.body))
        };
        log::debug!(target: "executor", "Message details:\n\tFlag: {}\n\tValue: {}\n\tSource: {}\n\tDestination: {}\n\tBody: {}\n\tStateInit: {}",
            mode.bits(),
            balance_to_string(&send_value),
            src,
            dst,
            body,
            msg.init.as_ref().map_or("None".to_string(), |_| "Present".to_string())
        );
    }

    Ok(msg_cell)
}

/// Reserves some grams from accout balance.
/// Returns calculated reserved value. its calculation depends on mode.
/// Reduces balance by the amount of the reserved value.
fn reserve_action_handler(
    mode: ReserveCurrencyFlags,
    val: &CurrencyCollection,
    original_acc_balance: &CurrencyCollection,
    acc_remaining_balance: &mut CurrencyCollection,
) -> std::result::Result<CurrencyCollection, i32> {
    if mode.intersects(!ReserveCurrencyFlags::all()) {
        return Err(RESULT_CODE_UNKNOWN_OR_INVALID_ACTION);
    }
    log::debug!(target: "executor", "Reserve with mode = {} value = {}", mode.bits(), balance_to_string(val));

    let mut reserved;
    if mode.contains(ReserveCurrencyFlags::WITH_ORIGINAL_BALANCE) {
        // Append all currencies
        if mode.contains(ReserveCurrencyFlags::REVERSE) {
            reserved = original_acc_balance.clone();
            if let Some(a) = reserved.tokens.checked_sub(val.tokens) { reserved.tokens = a };
            _ = reserved.other.try_sub_assign(&val.other).or(Err(RESULT_CODE_UNSUPPORTED))?;
        } else {
            reserved = val.clone();
            reserved.tokens = reserved.tokens.checked_add(original_acc_balance.tokens).ok_or(RESULT_CODE_INVALID_BALANCE)?;
            reserved.other.try_add_assign(&original_acc_balance.other).or(Err(RESULT_CODE_INVALID_BALANCE))?;
        }
    } else {
        if mode.contains(ReserveCurrencyFlags::REVERSE) { // flag 8 without flag 4 unacceptable
            return Err(RESULT_CODE_UNKNOWN_OR_INVALID_ACTION);
        }
        reserved = val.clone();
    }
    if mode.contains(ReserveCurrencyFlags::IGNORE_ERROR) {
        // Only grams
        reserved.tokens = min(reserved.tokens, acc_remaining_balance.tokens);
    }

    let mut remaining = acc_remaining_balance.clone();
    if remaining.tokens < reserved.tokens {
        return Err(RESULT_CODE_NOT_ENOUGH_TOKENS)
    }
    remaining.tokens = remaining.tokens.checked_sub(reserved.tokens).ok_or(RESULT_CODE_INVALID_BALANCE)?;
    remaining.other.try_sub_assign(&reserved.other).ok().filter(|a| *a).ok_or(RESULT_CODE_NOT_ENOUGH_OTHER_CURRENCIES)?;
    std::mem::swap(&mut remaining, acc_remaining_balance);

    if mode.contains(ReserveCurrencyFlags::ALL_BUT) {
        // swap all currencies
        std::mem::swap(&mut reserved, acc_remaining_balance);
    }

    Ok(reserved)
}

fn setcode_action_handler(acc: &mut OptionalAccount, code: Cell) -> Option<i32> {
    log::debug!(target: "executor", "OutAction::SetCode\nPrevious code hash: {}\nNew code hash:      {}",
        &acc.0.as_ref()
            .map(|a| match &a.state { AccountState::Active( state ) => state.code.as_ref(), _ => None } )
            .flatten().map_or(*EMPTY_CELL_HASH, |code| *code.repr_hash()),
        code.repr_hash(),
    );
    match &mut acc.0 {
        Some(Account { state: AccountState::Active(state), .. }) => {
            state.code = Some(code);
            None
        }
        _ => Some(RESULT_CODE_BAD_ACCOUNT_STATE)
    }
}

fn change_library_action_handler(acc: &mut OptionalAccount, mode: ChangeLibraryMode, lib_ref: LibRef) -> Option<i32> {
    let Some(Account { state: AccountState::Active(ref mut state), .. }) = &mut acc.0 else {
        return Some(RESULT_CODE_BAD_ACCOUNT_STATE);
    };
    let result = match (mode, lib_ref) {
        (ChangeLibraryMode::Remove, lib_ref) => {
            let hash = match lib_ref {
                LibRef::Hash(hash) => hash,
                LibRef::Cell(root) => *root.repr_hash(),
            };
            state.libraries.remove(hash).is_ok()
        }
        (mode, LibRef::Cell(root)) => {
            let public = mode == ChangeLibraryMode::AddPublic;
            state.libraries.add(root.repr_hash().0, SimpleLib { public, root }).is_ok()
        }
        (mode, LibRef::Hash(hash)) => {
            let public = mode == ChangeLibraryMode::AddPublic;
            match state.libraries.get(&hash) {
                Ok(Some(lib)) if lib.public == public => true,
                Ok(Some(lib)) =>
                    state.libraries.set(hash, SimpleLib { root: lib.root, public }).is_ok(),
                _ => false
            }
        }
    };
    if result { None } else { Some(RESULT_CODE_BAD_ACCOUNT_STATE) }
}

pub fn init_gas(
    acc_balance: u128,
    msg_balance: u128,
    is_external: bool,
    is_special: bool,
    is_ordinary: bool,
    gas_info: &GasLimitsPrices,
) -> Gas {
    let gas_max = if is_special {
        gas_info.special_gas_limit
    } else {
        min(
            (1 << (7 * 8)) - 1, // because gas_limit is stored as VarUInteger7
            min(gas_info.gas_limit, gas_info.calc_gas(acc_balance)),
        )
    };
    let mut gas_credit = 0;
    let gas_limit = if !is_ordinary {
        gas_max
    } else {
        if is_external {
            gas_credit = min(
                (1 << (3 * 8)) - 1, // because gas_credit is stored as VarUInteger3
                min(gas_info.gas_credit, gas_max),
            );
        }
        min(gas_max, gas_info.calc_gas(msg_balance))
    };
    log::debug!(
        target: "executor",
        "gas before: gm: {}, gl: {}, gc: {}, price: {}",
        gas_max, gas_limit, gas_credit, gas_info.get_real_gas_price()
    );
    Gas::new(gas_limit as i64, gas_credit as i64, gas_max as i64, gas_info.get_real_gas_price() as i64)
}

fn check_libraries(init: &StateInit, disable_set_lib: bool, text: &str, msg: &DynCell) -> bool {
    let mut len = 0;
    for a in init.libraries.iter() {
        match a {
            Ok(_) => { len += 1; },
            Err(err) => {
                log::trace!(
                    target: "executor",
                    "{} {} because libraries are broken {}",
                        text, msg.repr_hash(), err
                );
                return false;
            },
        }
    };
    if !disable_set_lib || len == 0 {
        true
    } else {
        log::trace!(
            target: "executor",
            "{} {} because libraries are disabled",
                text, msg.repr_hash()
        );
        false
    }
}

/// Calculate new account according to inbound message.
/// If message has no value, account will not created.
/// If hash of state_init is equal to account address (or flag check address is false), account will be active.
/// Otherwise, account will be nonexist or uninit according bounce flag: if bounce, account will be uninit that save money.
/// NOTE: originally this method ignored exceptions and returned None
fn account_from_message(
    msg: &InputMessage,
    msg_remaining_balance: &CurrencyCollection,
    check_std_address: bool,
    init_code_hash: bool,
    disable_set_lib: bool,
) -> Result<Option<Account>> {
    let MsgInfo::Int(hdr) = &msg.data.info else {
        return Ok(None);
    };
    if let Some(init) = &msg.data.init {
        match &init.code {
            Some(init_code) if !check_std_address ||
                (store(init).ok().map(|a| *a.repr_hash()) == hdr.dst.as_std().map(|a| a.address)) => {
                let text = "Cannot construct account from message with hash";
                if check_libraries(init, disable_set_lib, text, msg.cell.as_ref()) {
                    let account = Account {
                        address: hdr.dst.clone(),
                        storage_stat: StorageInfo::default(),
                        last_trans_lt: 0,
                        balance: msg_remaining_balance.clone(),
                        state: AccountState::Active(init.clone()),
                        init_code_hash: if init_code_hash { Some(*init_code.repr_hash()) } else { None },
                    };
                    return Ok(Some(account));
                }
            }
            Some(_) if check_std_address => {
                log::trace!(
                    target: "executor",
                    "Cannot construct account from message with hash {} \
                        because the destination address does not math with hash message code",
                        msg.cell.repr_hash()
                );
            }
            _ => {}
        }
    }
    if hdr.bounce {
        log::trace!(
            target: "executor",
            "Account will not be created. Value of {} message will be returned",
            msg.cell.repr_hash()
        );
        Ok(None)
    } else {
        let mut account = Account::uninit(hdr.dst.clone(), 0, 0, msg_remaining_balance.clone())?;
        account.update_storage_stat()?;
        Ok(Some(account))
    }
}

fn balance_to_string(balance: &CurrencyCollection) -> String {
    let value = balance.tokens.into_inner();
    format!("{}.{:03} {:03} {:03}      ({})",
            value / 1e9 as u128,
            (value % 1e9 as u128) / 1e6 as u128,
            (value % 1e6 as u128) / 1e3 as u128,
            value % 1e3 as u128,
            value,
    )
}

fn action_type(action: &OutAction) -> &'static str {
    match action {
        OutAction::SendMsg { .. } => "SendMsg",
        OutAction::SetCode { .. } => "SetCode",
        OutAction::ReserveCurrency { .. } => "ReserveCurrency",
        OutAction::ChangeLibrary { .. } => "ChangeLibrary",
        OutAction::CopyLeft { .. } => "CopyLeft",
    }
}