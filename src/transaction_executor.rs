use std::cmp::min;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use everscale_types::cell::{CellBuilder, CellTreeStats, DynCell, EMPTY_CELL_HASH, MAX_BIT_LEN, MAX_REF_COUNT};
use everscale_types::dict::Dict;
use everscale_types::models::{Account, AccountState, AccountStatusChange, ActionPhase, BaseMessage, BouncePhase, ChangeLibraryMode, ComputePhase, ComputePhaseSkipReason, CreditPhase, CurrencyCollection, ExecutedBouncePhase, ExtOutMsgInfo, ExtraCurrencyCollection, GlobalCapability, HashUpdate, IntAddr, IntMsgInfo, Lazy, LibDescr, LibRef, MsgInfo, NoFundsBouncePhase, OptionalAccount, OutAction, OutActionsRevIter, OwnedMessage, OwnedRelaxedMessage, RelaxedExtOutMsgInfo, RelaxedIntMsgInfo, RelaxedMsgInfo, ReserveCurrencyFlags, SendMsgFlags, ShardIdent, SimpleLib, SkippedComputePhase, StateInit, StdAddr, StorageInfo, StoragePhase, StorageUsedShort, Transaction, VarAddr, WorkchainFormat, WorkchainFormatExtended};
use everscale_types::num::{Tokens, Uint9, VarUint56};
use everscale_types::prelude::{Cell, CellFamily, CellSliceRange, ExactSize, HashBytes};
use everscale_vm::{Fmt, OwnedCellSlice};
use everscale_vm::{error, fail, types::{ExceptionCode, Result}};
use everscale_vm::error::tvm_exception;
use everscale_vm::executor::{BehaviorModifiers, VmBuilder};
use everscale_vm::SmartContractInfo;
use everscale_vm::stack::Stack;

use crate::blockchain_config::{MsgForwardPricesExt, MAX_ACTIONS, MAX_MSG_BITS, MAX_MSG_CELLS, PreloadedBlockchainConfig, VERSION_BLOCK_NEW_CALCULATION_BOUNCED_STORAGE};
use crate::error::ExecutorError;
use crate::ext::account::AccountExt;
use crate::ext::extra_currency_collection::ExtraCurrencyCollectionExt;
use crate::ext::gas_limit_prices::GasLimitsPricesExt;
use crate::utils::{CopyleftReward, default_action_phase, default_vm_phase, storage_stats, TxTime};

pub struct InputMessage<'a> {
    pub cell: &'a Cell,
    pub data: &'a OwnedMessage,
}

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(i32)]
enum ActionError {
    ActionListInvalid = 32,
    TooManyActions = 33,
    UnknownOrInvalidAction = 34,
    IncorrectSrcAddress = 35,
    IncorrectDstAddress = 36,
    NotEnoughTokens = 37,
    NotEnoughOtherCurrencies = 38,
    InvalidBalance = 40,
    BadAccountState = 41,
    Anycast = 50,
    // LicenceNotFound = 51; copyleft
    Unsupported = -1,
}

#[derive(Eq, PartialEq, Debug)]
pub enum IncorrectCheckRewrite {
    Anycast,
    Other
}

pub struct ActionPhaseResult {
    pub phase: ActionPhase,
    pub messages: Vec<Cell>,
    pub copyleft_reward: Option<CopyleftReward>,
}

pub struct ExecuteParams {
    pub state_libs: Dict<HashBytes, LibDescr>,
    /// block's start unix time, `block.timestamp` and `now()` in tsol
    pub block_unixtime: u32,
    /// block's start logical time, `block.logicaltime` in tsol
    pub block_lt: u64,
    /// last used lt, to be replaced with lt of newly created tx
    pub last_tr_lt: Arc<AtomicU64>,
    pub seed_block: HashBytes,
    pub debug: bool,
    pub trace_callback: Option<Arc<everscale_vm::executor::TraceCallback>>,
    pub behavior_modifiers: Option<BehaviorModifiers>,
    pub block_version: u32,
}

impl Default for ExecuteParams {
    fn default() -> Self {
        Self {
            state_libs: Dict::new(),
            block_unixtime: 0,
            block_lt: 0,
            last_tr_lt: Arc::new(AtomicU64::new(0)),
            seed_block: HashBytes::default(),
            debug: false,
            trace_callback: None,
            behavior_modifiers: None,
            block_version: 0,
        }
    }
}

// Canary so trait is object safe
const _: Option<&dyn TransactionExecutor> = None;
pub trait TransactionExecutor {
    fn execute_with_params(
        &self,
        in_msg: Option<&Cell>,
        account: &mut OptionalAccount,
        params: &ExecuteParams,
        config: &PreloadedBlockchainConfig,
    ) -> Result<Transaction>;
    fn execute_with_libs_and_params(
        &self,
        in_msg: Option<&Cell>,
        account_root: &mut Lazy<OptionalAccount>,
        params: &ExecuteParams,
        config: &PreloadedBlockchainConfig,
    ) -> Result<Transaction> {
        let old_hash = account_root.inner().repr_hash();
        let mut account = account_root.load()?;
        let mut transaction = self.execute_with_params(
            in_msg,
            &mut account,
            params,
            config,
        )?;
        if let Some(ref mut account) = account.0 {
            if config.global_version().capabilities.contains(GlobalCapability::CapFastStorageStat) {
                account.update_storage_stat_fast()?;
            } else {
                account.update_storage_stat()?;
            }
        }
        let new_account_root = Lazy::new(&account)?;
        let new_hash = new_account_root.inner().repr_hash();
        transaction.state_update = Lazy::new(&HashUpdate { old: *old_hash, new: *new_hash })?;
        transaction.end_status = account.status();
        // outputs below: no errors possible
        params.last_tr_lt.store(transaction.lt, Ordering::Relaxed);
        *account_root = new_account_root;
        Ok(transaction)
    }
}
pub(crate) struct Common;
impl Common {
    /// Implementation of transaction's storage phase.
    /// If account does not exist - phase skipped.
    /// Calculates storage fees and substracts them from account balance.
    /// If account balance becomes negative after that, then account is frozen.
    pub fn storage_phase(
        config: &PreloadedBlockchainConfig,
        acc_opt: &mut OptionalAccount,
        acc_balance: &mut CurrencyCollection,
        tr_total_fees: &mut CurrencyCollection,
        now: u32,
        is_masterchain: bool,
        is_special: bool
    ) -> Result<(Option<StoragePhase>, Tokens)> {
        tracing::debug!(target: "executor", "storage_phase");
        let Some(ref mut acc) = acc_opt.0 else {
            tracing::debug!(target: "executor", "Account::None");
            // FIXME must be skipped (None), left for compatibility
            return Ok((Some(StoragePhase{
                storage_fees_collected: Tokens::ZERO,
                storage_fees_due: None,
                status_change: AccountStatusChange::Unchanged,
            }), Tokens::ZERO))
        };
        if is_special {
            tracing::debug!(target: "executor", "Special account: AccStatusChange::Unchanged");
            acc.storage_stat.last_paid = 0;
            return Ok((Some(StoragePhase {
                storage_fees_collected: Tokens::ZERO,
                storage_fees_due: acc.storage_stat.due_payment,
                status_change: AccountStatusChange::Unchanged,
            }), Tokens::ZERO))
        } else if now < acc.storage_stat.last_paid {
            fail!("transaction timestamp must be greater than account timestamp")
        }

        let calculated_fee = config.calc_storage_fee(&acc.storage_stat, is_masterchain, now)?;
        acc.storage_stat.last_paid = now;
        let mut fee = calculated_fee;

        if let Some(due_payment) = acc.storage_stat.due_payment {
            fee = fee.checked_add(due_payment).ok_or_else(|| error!("integer overflow"))?;
            acc.storage_stat.due_payment = None;
        }

        if acc_balance.tokens >= fee {
            tracing::debug!(target: "executor", "acc_balance: {}, storage fee: {}", acc_balance.tokens, fee);
            acc_balance.tokens = acc_balance.tokens.checked_sub(fee).ok_or_else(|| error!("integer underflow"))?;
            tr_total_fees.tokens = tr_total_fees.tokens.checked_add(fee).ok_or_else(|| error!("integer overflow"))?;
            Ok((Some(StoragePhase {
                storage_fees_collected: fee,
                storage_fees_due: None,
                status_change: AccountStatusChange::Unchanged,
            }), calculated_fee))
        } else {
            tracing::debug!(target: "executor", "acc_balance: {} is storage fee from total: {}", acc_balance.tokens, fee);
            let storage_fees_collected = std::mem::take(&mut acc_balance.tokens);
            tr_total_fees.tokens = tr_total_fees.tokens.checked_add(storage_fees_collected)
                .ok_or_else(|| error!("integer overflow"))?;
            let fee = fee.checked_sub(storage_fees_collected).ok_or_else(|| error!("integer underflow"))?;

            let status_change = match &acc.state {
                AccountState::Uninit | AccountState::Frozen(_) // need delete
                if fee > Tokens::new(config.get_gas_config(is_masterchain).delete_due_limit.into()) => {
                    // yet no recoverable way to collect other tokens
                    tr_total_fees.other.try_add_assign(&acc_balance.other)?;
                    acc_balance.other = ExtraCurrencyCollection::new();
                    *acc_opt = OptionalAccount::EMPTY;
                    AccountStatusChange::Deleted
                },
                AccountState::Active(state) // need freeze
                if fee > Tokens::new(config.get_gas_config(is_masterchain).freeze_due_limit.into()) => {
                    acc.storage_stat.due_payment = Some(fee);
                    let hash = *CellBuilder::build_from(state)?.repr_hash();
                    acc.state = AccountState::Frozen(hash);
                    AccountStatusChange::Frozen
                }
                _ => {
                    acc.storage_stat.due_payment = Some(fee);
                    AccountStatusChange::Unchanged
                }
            };
            Ok((Some(StoragePhase {
                storage_fees_collected,
                storage_fees_due: Some(fee),
                status_change,
            }), calculated_fee))
        }
    }

    /// Implementation of transaction's credit phase.
    /// Increases account balance by the amount that appears in the internal message header.
    /// If account does not exist - phase skipped.
    /// If message is not internal - phase skipped.
    pub fn credit_phase(
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
                .filter(|a| !a.is_zero());
            *tr_total_fees_tokens = tr_total_fees_tokens.checked_add(collected)
                .ok_or_else(|| error!("integer overflow"))?;

            Some(collected).filter(|a| !a.is_zero())
        } else { None };

        tracing::debug!(
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
    pub fn compute_phase(
        config: &PreloadedBlockchainConfig,
        is_ordinary: bool,
        msg: Option<&InputMessage>,
        acc: &mut OptionalAccount,
        acc_balance: &mut CurrencyCollection,
        msg_balance: &CurrencyCollection,
        time: &TxTime,
        stack: Stack,
        storage_fee: Tokens,
        acc_addr: &StdAddr,
        is_special: bool,
        params: &ExecuteParams,
    ) -> Result<(ComputePhase, Option<Cell>, Option<Cell>)> {
        let mut result_acc = acc.clone();
        let mut vm_phase = default_vm_phase();
        let init_code_hash = config.global_version().capabilities.contains(GlobalCapability::CapInitCodeHash);
        let libs_disabled = !config.global_version().capabilities.contains(GlobalCapability::CapSetLibCode);
        let is_external = if let Some(msg) = msg {
            if let MsgInfo::Int(header) = &msg.data.info {
                tracing::debug!(target: "executor", "msg internal, bounce: {}", header.bounce);
                if result_acc.0.is_none() {
                    if let Some(mut new_acc) = account_from_message(msg, msg_balance, init_code_hash, libs_disabled).ok().flatten() {
                        if !is_special { new_acc.storage_stat.last_paid = time.now() };
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
                tracing::debug!(target: "executor", "msg external");
                true
            }
        } else {
            debug_assert!(result_acc.0.is_some());
            false
        };
        tracing::debug!(target: "executor", "acc balance: {}", acc_balance.tokens);
        tracing::debug!(target: "executor", "msg balance: {}", msg_balance.tokens);
        if acc_balance.tokens.is_zero() {
            tracing::debug!(target: "executor", "skip computing phase no gas");
            return Ok((ComputePhase::Skipped(SkippedComputePhase { reason: ComputePhaseSkipReason::NoGas }), None, None))
        }
        let gas_config = config.get_gas_config(acc_addr.is_masterchain());
        let gas = gas_config.init_gas(&acc_balance.tokens, &msg_balance.tokens, is_external, is_special, is_ordinary)?;
        if gas.limit() == 0 && gas.credit() == 0 {
            tracing::debug!(target: "executor", "skip computing phase no gas");
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

        vm_phase.gas_credit = match gas.credit() {
            0 => None,
            value => Some(value.try_into()?)
        };
        vm_phase.gas_limit = gas.limit().try_into()?;

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
                    vm_phase.gas_fees = if is_special { Tokens::ZERO } else { gas_config.calc_gas_fee(0) };
                    acc_balance.tokens = acc_balance.tokens.checked_sub(vm_phase.gas_fees).ok_or_else(|| {
                        tracing::debug!(target: "executor", "can't sub funds: {} from acc_balance: {}", vm_phase.gas_fees, acc_balance.tokens);
                        error!("can't sub funds: from acc_balance")
                    })?;
                    *acc = result_acc;
                    return Ok((ComputePhase::Executed(vm_phase), None, None));
                }
            }
        };
        let smc_info = SmartContractInfo {
            myself: OwnedCellSlice::new(CellBuilder::build_from(&IntAddr::Std(acc_addr.clone()))?)?,
            block_lt: params.block_lt,
            trans_lt: time.tx_lt(),
            unix_time: time.now(),
            balance: acc_balance.clone(),
            config_params: config.raw_config().params.as_dict().root().clone(),
            mycode: code.clone(),
            rand_seed: SmartContractInfo::calc_rand_seed(&params.seed_block, &acc_addr.address),
            actions: 0,
            init_code_hash: result_acc.0.as_ref().map(|a| a.init_code_hash).flatten().unwrap_or_default(),
            msgs_sent: 0,
            storage_fee_collected: storage_fee.into_inner(),
        };
        let mut vm = VmBuilder::new(
            config.global_version().capabilities,
            smc_info,
            gas,
        )
            .set_stack(stack)
            .set_data(data)?
            .set_libraries(acc_libs, &params.state_libs)?
            .set_debug(params.debug)
            .modify_behavior(params.behavior_modifiers.as_ref())
            .set_trace_callback(params.trace_callback.clone())
            .set_block_version(params.block_version)
            .set_signature_id(config.global_id())
            .build()?;

        let result = vm.execute();
        tracing::trace!(target: "executor", "execute result: {:?}", result);
        let mut raw_exit_arg = None;
        match result {
            Err(err) => {
                tracing::debug!(target: "executor", "VM terminated with exception: {}", err);
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
        tracing::debug!(target: "executor", "VM terminated with exit code {}", vm_phase.exit_code);

        //for external messages gas will not be exacted if VM throws the exception and gas_credit != 0
        let used = vm.gas().vm_total_used();
        vm_phase.gas_used = used.try_into()?;
        if vm.gas().credit() != 0 {
            if is_external {
                fail!(ExecutorError::NoAcceptError(vm_phase.exit_code, raw_exit_arg))
            }
            vm_phase.gas_fees = Tokens::ZERO;
        } else { // credit == 0 means contract accepted
            vm_phase.gas_fees = if is_special { Tokens::ZERO } else { gas_config.calc_gas_fee(used) };
        };

        tracing::debug!(
            target: "executor",
            "gas after: gl: {}, gc: {}, gu: {}, fees: {}",
            vm.gas().limit(), vm.gas().credit(), used, vm_phase.gas_fees
        );

        //set mode
        vm_phase.mode = 0;
        vm_phase.vm_steps = vm.steps();
        //TODO: vm_final_state_hash
        tracing::debug!(target: "executor", "acc_balance: {}, gas fees: {}", acc_balance.tokens, vm_phase.gas_fees);
        acc_balance.tokens = acc_balance.tokens.checked_sub(vm_phase.gas_fees).ok_or_else(|| {
            tracing::error!(target: "executor", "This situation is unreachable: can't sub funds: {} from acc_balance: {}", vm_phase.gas_fees, acc_balance.tokens);
            error!("can't sub funds: from acc_balance")
        })?;

        let new_data = if let Ok(cell) = vm.get_committed_state().get_root().as_cell() {
            Some(cell.clone())
        } else {
            tracing::debug!(target: "executor", "invalid contract, it must be cell in c4 register");
            vm_phase.success = false;
            None
        };

        let out_actions = if let Ok(root_cell) = vm.get_committed_state().get_actions().as_cell() {
            Some(root_cell.clone())
        } else {
            tracing::debug!(target: "executor", "invalid contract, it must be cell in c5 register");
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
        config: &PreloadedBlockchainConfig,
        tr: &mut Transaction,
        acc: &mut Account,
        original_acc_balance: &CurrencyCollection,
        acc_balance: &mut CurrencyCollection,
        msg_remaining_balance: &mut CurrencyCollection,
        compute_phase_fees: &Grams,
        actions_cell: Cell,
        new_data: Option<Cell>,
        acc_addr: &StdAddr,
        is_special: bool,
    ) -> Result<(TrActionPhase, Vec<Message>)> {
        let result = Self::action_phase_with_copyleft(
            config, tr, acc, original_acc_balance, acc_balance, msg_remaining_balance,
            compute_phase_fees, actions_cell, new_data, acc_addr, is_special)?;
        Ok((result.phase, result.messages))
    }
*/
    pub fn action_phase_with_copyleft(
        config: &PreloadedBlockchainConfig,
        tr_total_fees_tokens: &mut Tokens,
        time: &TxTime,
        acc: &mut OptionalAccount,
        original_acc_balance: &CurrencyCollection,
        acc_balance: &mut CurrencyCollection,
        msg_remaining_balance: &mut CurrencyCollection,
        compute_phase_fees: &Tokens,
        actions_cell: Cell,
        acc_addr: &StdAddr,
        is_special: bool,
    ) -> Result<ActionPhaseResult> {
        let mut acc_copy = acc.clone();
        let mut acc_remaining_balance = acc_balance.clone();
        let mut phase = default_action_phase();
        let mut total_reserved_value = CurrencyCollection::default();
        phase.action_list_hash = *actions_cell.repr_hash();

        fn process_err_code(mut err_code: ActionError, action_index: usize, phase: &mut ActionPhase) {
            if err_code == ActionError::Unsupported {
                err_code = ActionError::UnknownOrInvalidAction;
            }
            tracing::debug!(target: "executor", "action failed: error_code={}", err_code as i32);
            phase.valid = true;
            phase.result_code = err_code as i32;
            if action_index != 0 {
                phase.result_arg = Some(action_index as i32);
            }
            if err_code == ActionError::NotEnoughTokens || err_code == ActionError::NotEnoughOtherCurrencies {
                phase.no_funds = true;
            }
        }

        let mut reversed_actions = vec![];
        for action in OutActionsRevIter::new(actions_cell.as_slice()?) {
            phase.total_actions += 1;
            if phase.total_actions > MAX_ACTIONS {
                tracing::debug!(target: "executor", "too many actions, more than {MAX_ACTIONS}");
                phase.result_code = ActionError::TooManyActions as i32;
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
            tracing::debug!(target: "executor", "\nAction #{}\nType: {}\nInitial balance: {}",
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
                        tracing::debug!(target: "executor", "Message with flag `send ALL_BALANCE` will be sent last. Skip it for now.");
                        continue
                    }
                    outmsg_action_handler(
                        &mut phase,
                        time,
                        out_msgs_temp.len(),
                        mode,
                        out_msg,
                        &mut acc_remaining_balance,
                        msg_remaining_balance,
                        compute_phase_fees,
                        config,
                        is_special,
                        acc_addr,
                        &total_reserved_value,
                        &mut account_deleted
                    ).map(|out_msg| out_msg.map(|a| {
                        phase.messages_created += 1;
                        out_msgs_temp.push(EitherMsg::Ready(a))
                    })).err()
                }
                OutAction::ReserveCurrency { mode, value } => {
                    reserve_action_handler(mode, &value, original_acc_balance, &mut acc_remaining_balance)
                        .and_then(|reserved_value| {
                            phase.special_actions += 1;
                            total_reserved_value.tokens.checked_add(reserved_value.tokens)
                                .map(|tokens| total_reserved_value.tokens = tokens)
                                .zip(total_reserved_value.other.try_add_assign(&reserved_value.other).ok())
                                .ok_or(ActionError::InvalidBalance)
                        }).err()
                }
                OutAction::SetCode{ new_code } => {
                    setcode_action_handler(&mut acc_copy, new_code)
                        .map(|_| phase.special_actions += 1)
                        .err()
                }
                OutAction::ChangeLibrary { mode, lib } => {
                    change_library_action_handler(&mut acc_copy, mode, lib)
                        .map(|_| phase.special_actions += 1)
                        .err()
                }
                OutAction::CopyLeft { .. } => Some(ActionError::UnknownOrInvalidAction) // { 0 }
            };
            if let Some(a) = init_balance.tokens.checked_sub(acc_remaining_balance.tokens) { init_balance.tokens = a };
            _ = init_balance.other.try_sub_assign(&acc_remaining_balance.other)?;
            tracing::debug!(target: "executor", "Final balance:   {}\nDelta:           {}",
                balance_to_string(&acc_remaining_balance),
                balance_to_string(&init_balance)
            );
            if let Some(err_code) = err_code {
                process_err_code(err_code, action_index, &mut phase);
                return Ok(ActionPhaseResult { phase, messages: vec![], copyleft_reward })
            }
        }

        let mut out_msgs = Vec::with_capacity(out_msgs_temp.len());
        for (msg_index, element) in out_msgs_temp.into_iter().enumerate() {
            match element {
                EitherMsg::Postponed((action_index, mode, out_msg)) => {
                    tracing::debug!(target: "executor", "\nSend message with all balance:\nInitial balance: {}",
                balance_to_string(&acc_remaining_balance));
                    let err_code = outmsg_action_handler(
                        &mut phase,
                        time,
                        msg_index,
                        mode,
                        out_msg,
                        &mut acc_remaining_balance,
                        msg_remaining_balance,
                        compute_phase_fees,
                        config,
                        is_special,
                        acc_addr,
                        &total_reserved_value,
                        &mut account_deleted,
                    ).map(|out_msg| out_msg.map(|a| {
                        phase.messages_created += 1;
                        out_msgs.push(a)
                    })).err();
                    tracing::debug!(target: "executor", "Final balance:   {}", balance_to_string(&acc_remaining_balance));
                    if let Some(err_code) = err_code {
                        process_err_code(err_code, action_index, &mut phase);
                        return Ok(ActionPhaseResult { phase, messages: vec![], copyleft_reward })
                    }
                }
                EitherMsg::Ready(out_msg_cell) => {
                    out_msgs.push(out_msg_cell);
                }
            }
        }

        //calc new account balance
        tracing::debug!(target: "executor", "\nReturn reserved balance:\nInitial:  {}\nReserved: {}",
            balance_to_string(&acc_remaining_balance),
            balance_to_string(&total_reserved_value)
        );
        if let Err(err) = acc_remaining_balance.tokens.checked_add(total_reserved_value.tokens)
            .map(|tokens| acc_remaining_balance.tokens = tokens)
            .map(|_| acc_remaining_balance.other.try_add_assign(&total_reserved_value.other))
            .ok_or_else(|| anyhow::Error::msg("integer overflow")) {
            tracing::debug!(target: "executor", "failed to add account balance with reserved value {}", err);
            fail!("failed to add account balance with reserved value {}", err)
        }
        tracing::debug!(target: "executor", "Final:    {}", balance_to_string(&acc_remaining_balance));

        let fee = phase.total_action_fees.unwrap_or(Tokens::ZERO);
        tracing::debug!(target: "executor", "Total action fees: {}", fee);
        *tr_total_fees_tokens = tr_total_fees_tokens.checked_add(fee)
            .ok_or_else(|| error!("integer overflow"))?;

        if account_deleted {
            tracing::debug!(target: "executor", "\nAccount deleted");
            phase.status_change = AccountStatusChange::Deleted;
        }
        phase.valid = true;
        phase.success = true;
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
    pub fn bounce_phase(
        config: &PreloadedBlockchainConfig,
        mut remaining_msg_balance: CurrencyCollection,
        acc_balance: &mut CurrencyCollection,
        compute_phase_fees: &Tokens,
        msg_index: usize,
        msg: &OwnedMessage,
        tr_total_fees_tokens: &mut Tokens,
        time: &TxTime,
        acc_addr: &StdAddr,
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
        match check_rewrite_dest_addr(&header.dst, config, acc_addr) {
            Ok(new_dst) => {header.dst = new_dst}
            Err(_) => {
                tracing::warn!(target: "executor", "Incorrect destination address in a bounced message {}", header.dst);
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
        if config.global_version().capabilities.contains(GlobalCapability::CapBounceMsgBody) {
            bounce_msg_body.store_u32(-1_i32 as u32)?;
            if let Some(body) = Some(&msg.body.1)
                .filter(|r| !(r.is_data_empty() && r.is_refs_empty()))
                .map(|r| r.apply(&msg.body.0)).transpose()? {
                let mut body_copy = body.clone();
                body_copy.shrink(Some(min(256, body.remaining_bits())), Some(0))?;
                bounce_msg_body.store_slice_data(body_copy)?;
                if config.global_version().capabilities.contains(GlobalCapability::CapFullBodyInBounced) {
                    bounce_msg_body.store_reference(CellBuilder::build_from(&body)?)?;
                }
            }
            if config.global_version().capabilities.contains(GlobalCapability::CapFullBodyInBounced) {
                if let Some(init) = &msg.init {
                    bounce_state_init = Some(init.clone());
                }
            }
        }

        let mut bounce_msg = OwnedMessage {
            info: MsgInfo::Int(header),
            init: bounce_state_init,
            body: if bounce_msg_body.bit_len() == 0 && bounce_msg_body.reference_count() == 0 {
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
            let fwd_full_fees = config.calc_fwd_fee(is_masterchain, &storage)?;
            (StorageUsedShort{ cells: VarUint56::new(storage.cell_count), bits: VarUint56::new(storage.bit_count) }, fwd_full_fees)
        } else {
            let fwd_full_fees = config.calc_fwd_fee(is_masterchain, &CellTreeStats::ZERO)?;
            (StorageUsedShort { cells: VarUint56::ZERO, bits: VarUint56::ZERO }, fwd_full_fees)
        };
        let fwd_prices = config.get_fwd_prices(is_masterchain);
        let fwd_mine_fees = fwd_prices.mine_fee(&fwd_full_fees).ok_or_else(|| error!("integer overflow"))?;
        let fwd_fees = fwd_full_fees - fwd_mine_fees;

        tracing::debug!(target: "executor", "get fee {} from bounce msg {}", fwd_full_fees, remaining_msg_balance.tokens.into_inner());

        if remaining_msg_balance.tokens < fwd_full_fees + *compute_phase_fees {
            tracing::debug!(
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
            tracing::debug!(
                target: "executor",
                "bounce fees: {} bounce value: {}",
                fwd_mine_fees, header.value.tokens
            );
        } else {
            unreachable!("bounce phase creates only one internal message")
        }
        time.write_msg_time(&mut bounce_msg.info, msg_index);
        *tr_total_fees_tokens = tr_total_fees_tokens.checked_add(fwd_mine_fees)
            .ok_or_else(|| error!("integer overflow"))?;

        Ok((BouncePhase::Executed(ExecutedBouncePhase {
            msg_size: storage,
            msg_fees: fwd_mine_fees,
            fwd_fees,
        }), Some(CellBuilder::build_from(&bounce_msg)?)))
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
    tracing::debug!(target: "executor", "compute_account_state");
    let Some(ref mut acc) = acc.0 else {
        tracing::error!(target: "executor", "account must exist");
        return Some(if in_msg.data.init.is_none() { ComputePhaseSkipReason::NoState } else { ComputePhaseSkipReason::BadState });
    };
    match acc.state {
        //Account exists, but can be in different states.
        AccountState::Active(_) => {
            //account is active, just return it
            tracing::debug!(target: "executor", "account state: AccountActive");
            None
        }
        AccountState::Uninit => {
            tracing::debug!(target: "executor", "AccountUninit");
            if let Some(state_init) = &in_msg.data.init {
                // if msg is a constructor message then
                // borrow code and data from it and switch account state to 'active'.
                tracing::debug!(target: "executor", "message for uninitialized: activated");
                let text = "Cannot construct account from message with hash";
                if !check_libraries(state_init, disable_set_lib, text, in_msg.cell.as_ref()) {
                    return Some(ComputePhaseSkipReason::BadState);
                }
                match try_activate_by_init_code_hash(acc, state_init, init_code_hash) {
                    Err(err) => {
                        tracing::debug!(target: "executor", "reason: {}", err);
                        Some(ComputePhaseSkipReason::BadState)
                    }
                    Ok(_) => None
                }
            } else {
                tracing::debug!(target: "executor", "message for uninitialized: skip computing phase");
                Some(ComputePhaseSkipReason::NoState)
            }
        }
        AccountState::Frozen(_) => {
            tracing::debug!(target: "executor", "AccountFrozen");
            //account balance was credited and if it positive after that
            //and inbound message bear code and data then make some check and unfreeze account
            if !acc_balance.tokens.is_zero() { // This check is redundant
                if let Some(state_init) = &in_msg.data.init {
                    let text = "Cannot unfreeze account from message with hash";
                    if !check_libraries(state_init, disable_set_lib, text, in_msg.cell.as_ref()) {
                        return Some(ComputePhaseSkipReason::BadState);
                    }
                    tracing::debug!(target: "executor", "message for frozen: activated");
                    return match try_activate_by_init_code_hash(acc, state_init, init_code_hash) {
                        Err(err) => {
                            tracing::debug!(target: "executor", "reason: {}", err);
                            Some(ComputePhaseSkipReason::BadState)
                        }
                        Ok(_) => None
                    }
                }
            }
            //skip computing phase, because account is frozen (bad state)
            tracing::debug!(target: "executor", "account is frozen (bad state): skip computing phase");
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
            if Some(*CellBuilder::build_from(state_init)?.repr_hash()) ==
                stuff.address.as_std().map(|a| a.address) {
                (true, init_code_hash)
            } else {
                fail!("StateInit doesn't correspond to uninit account address")
            }
        },
        AccountState::Frozen(state_init_hash) => {
            if CellBuilder::build_from(state_init)?.repr_hash() == state_init_hash {
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

fn check_replace_src_addr(info: RelaxedMsgInfo, acc_addr: &StdAddr) -> std::result::Result<MsgInfo, IntAddr> {
    match info {
        RelaxedMsgInfo::Int(RelaxedIntMsgInfo {
                                ihr_disabled,
                                bounce,
                                bounced,
                                src,
                                dst,
                                value,
                                // FIXME: are values from VM execution usable?
                                ihr_fee: _,
                                fwd_fee: _,
                                created_lt,
                                created_at
                            }) => {
            let src = match src {
                None => acc_addr.clone(),
                Some(IntAddr::Std(src)) if src == *acc_addr => src,
                Some(unknown) => return Err(unknown)
            };
            Ok(MsgInfo::Int(IntMsgInfo {
                ihr_disabled,
                bounce,
                bounced,
                src: IntAddr::Std(src),
                dst,
                value,
                ihr_fee: Tokens::ZERO,
                fwd_fee: Tokens::ZERO,
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
                Some(IntAddr::Std(src)) if src == *acc_addr => src,
                Some(unknown) => return Err(unknown)
            };
            Ok(MsgInfo::ExtOut(ExtOutMsgInfo {
                src: IntAddr::Std(src),
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

fn is_valid_addr_len(addr_len: Uint9, wf: &WorkchainFormatExtended) -> bool {
    let len = addr_len.into_inner();
    let min = wf.min_addr_len.into_inner();
    let max = wf.max_addr_len.into_inner();
    let step = wf.addr_len_step.into_inner();
    (len == min) || (len == max) || (
        (len > min) && (len < max) && (step != 0) && (len - min) % step == 0
    )
}

fn check_rewrite_dest_addr(
    dst: &IntAddr,
    config: &PreloadedBlockchainConfig,
    acc_addr: &StdAddr,
) -> std::result::Result<IntAddr, IncorrectCheckRewrite> {
    let (repack, anycast, addr_len, workchain, address);
    match dst {
        IntAddr::Var(dst) => {
            repack = dst.address_len == 256 && i8::try_from(dst.workchain).is_ok();
            anycast = dst.anycast.clone();
            addr_len = dst.address_len;
            workchain = dst.workchain;
            address = dst.address.as_slice();
        }
        IntAddr::Std(dst) => {
            repack = false;
            anycast = dst.anycast.clone();
            addr_len = Uint9::new(256);
            workchain = dst.workchain as i32;
            address = dst.address.0.as_slice();
        }
    }

    let cap_workchains = config.global_version().capabilities.contains(GlobalCapability::CapWorkchains);
    if workchain != ShardIdent::MASTERCHAIN.workchain() {
        if !cap_workchains && !acc_addr.is_masterchain() && acc_addr.workchain as i32 != workchain {
            tracing::debug!(
                target: "executor",
                "cannot send message from {} to {} it doesn't allow yet",
                acc_addr, dst
            );
            return Err(IncorrectCheckRewrite::Other);
        }
        if let Some(wc) = config.workchains().get(&workchain) {
            if !wc.accept_msgs {
                tracing::debug!(
                    target: "executor",
                    "destination address belongs to workchain {} not accepting new messages",
                    workchain
                );
                return Err(IncorrectCheckRewrite::Other);
            }
            if let WorkchainFormat::Extended(wf) = &wc.format {
                if !is_valid_addr_len(addr_len, wf) {
                    tracing::debug!(
                        target: "executor",
                        "destination address has length {} invalid for destination workchain {}",
                        addr_len, workchain
                    );
                    return Err(IncorrectCheckRewrite::Other);
                }
            }
        } else {
            tracing::debug!(
                target: "executor",
                "destination address contains unknown workchain_id {}",
                workchain
            );
            return Err(IncorrectCheckRewrite::Other);
        }
    } else {
        if !cap_workchains && !acc_addr.is_masterchain() &&
            acc_addr.workchain as i32 != ShardIdent::BASECHAIN.workchain() {
            tracing::debug!(
                target: "executor",
                "masterchain cannot accept from {} workchain",
                acc_addr.workchain
            );
            return Err(IncorrectCheckRewrite::Other);
        }
        if addr_len != 256 {
            tracing::debug!(
                target: "executor",
                "destination address has length {} invalid for destination workchain {}",
                addr_len, workchain
            );
            return Err(IncorrectCheckRewrite::Other);
        }
    }

    if anycast.is_some() {
        tracing::debug!(target: "executor", "address cannot be anycast");
        return Err(IncorrectCheckRewrite::Anycast);
    }
    /* anycast not supported
    if let Some(ref mut anycast) = anycast {
        if workchain == ShardIdent::MASTERCHAIN.workchain() {
            log::debug!(target: "executor", "masterchain address cannot be anycast");
            return Err(IncorrectCheckRewrite::Anycast);
        }
        let mut pfx = acc_addr.address.0[..(anycast.depth.into_bit_len() as usize + 7) / 8].to_vec();
        let last_bits = anycast.depth.into_bit_len() % 8;
        if last_bits != 0 {
            let to_drop = 8 - last_bits as usize;
            pfx.last_mut().map(|a| (*a >> to_drop) << to_drop);
        }
        if pfx != anycast.rewrite_prefix {
            repack = true;
            *anycast = Box::new(everscale_types::models::Anycast {
                depth: anycast.depth,
                rewrite_prefix: pfx
            });
        }
    }
    */

    if !repack {
        Ok(dst.clone())
    } else if addr_len == 256 && i8::try_from(workchain).is_ok() {
        // repack as an addr_std
        let address = HashBytes::from_slice(address);
        Ok(IntAddr::Std(StdAddr { anycast, workchain: workchain as i8, address }))
    } else {
        // repack as an addr_var
        let address = address.to_vec();
        Ok(IntAddr::Var(VarAddr { anycast, address_len: addr_len, workchain, address }))
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
    is_special: bool,
    acc_addr: &StdAddr,
    reserved_value: &CurrencyCollection,
    account_deleted: &mut bool
) -> std::result::Result<Option<Cell>, ActionError> {
    if mode.intersects(!SendMsgFlags::all()) ||
        // we cannot send all balance from account and from message simultaneously ?
        mode.contains(SendMsgFlags::WITH_REMAINING_BALANCE | SendMsgFlags::ALL_BALANCE) ||
        (mode.contains(SendMsgFlags::DELETE_IF_EMPTY) && !mode.contains(SendMsgFlags::ALL_BALANCE))
    {
        tracing::error!(target: "executor", "outmsg mode has unsupported flags");
        return Err(ActionError::Unsupported);
    }
    let skip = if mode.contains(SendMsgFlags::IGNORE_ERROR) {
        Ok(None)
    } else {
        Err(())
    };
    let fwd_mine_fee: Tokens;
    let total_fwd_fees: Tokens;
    let mut send_value; // to sub from acc_balance

    let mut msg = match check_replace_src_addr(info, acc_addr) {
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
            tracing::warn!(target: "executor", "Incorrect source address {:?}", addr);
            return Err(ActionError::IncorrectSrcAddress);
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
                tracing::error!(target: "executor", "cannot serialize message in action phase : {}", err);
                ActionError::ActionListInvalid
            })?
    };
    if let MsgInfo::Int(ref mut int_header) = &mut msg.info {
        match check_rewrite_dest_addr(&int_header.dst, config, acc_addr) {
            Ok(new_dst) => { int_header.dst = new_dst }
            Err(IncorrectCheckRewrite::Anycast) => {
                tracing::warn!(target: "executor", "Incorrect destination anycast address {}", int_header.dst);
                return skip.map_err(|_| ActionError::Anycast)
            }
            Err(IncorrectCheckRewrite::Other) => {
                tracing::warn!(target: "executor", "Incorrect destination address {}", int_header.dst);
                return skip.map_err(|_| ActionError::IncorrectDstAddress)
            }
        }

        int_header.bounced = false;
        send_value = int_header.value.clone();

        if cfg!(feature = "ihr_disabled") {
            int_header.ihr_disabled = true;
        }
        if !int_header.ihr_disabled {
            let compute_ihr_fee = fwd_prices.ihr_fee(&compute_fwd_fee).ok_or(ActionError::Unsupported)?;
            if int_header.ihr_fee < compute_ihr_fee {
                int_header.ihr_fee = compute_ihr_fee
            }
        } else {
            int_header.ihr_fee = Tokens::ZERO;
        }
        let fwd_fee = *std::cmp::max(&int_header.fwd_fee, &compute_fwd_fee);
        fwd_mine_fee = fwd_prices.mine_fee(&fwd_fee).ok_or(ActionError::Unsupported)?;
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
                    return skip.map_err(|_| ActionError::NotEnoughTokens)
                }
                send_value.tokens = send_value.tokens.checked_sub(*compute_phase_fees).ok_or_else(|| {
                    tracing::error!(target: "executor", "cannot subtract msg balance: integer underflow");
                    ActionError::ActionListInvalid
                })?;
            }
            int_header.value = send_value.clone();
        }
        if mode.contains(SendMsgFlags::PAY_FEE_SEPARATELY) {
            //we must pay the fees, sum them with msg value
            send_value.tokens += total_fwd_fees;
        } else if int_header.value.tokens < total_fwd_fees {
            //msg value is too small, reciever cannot pay the fees
            tracing::warn!(
                target: "executor",
                "msg balance {} is too small, cannot pay fwd+ihr fees: {}",
                int_header.value.tokens, total_fwd_fees
            );
            return skip.map_err(|_| ActionError::NotEnoughTokens)
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
        return Err(ActionError::Unsupported)
    }

    if let Some(tokens) = acc_balance.tokens.checked_sub(send_value.tokens) {
        acc_balance.tokens = tokens;
    } else {
        tracing::warn!(
            target: "executor",
            "account balance {} is too small, cannot send {}", acc_balance.tokens, send_value.tokens
        );
        return skip.map_err(|_| ActionError::NotEnoughTokens);
    };

    if let Err(_) | Ok(false) = acc_balance.other.try_sub_assign(&send_value.other) {
        tracing::warn!(
            target: "executor",
            "account balance {:?} is too small, cannot send {:?}", acc_balance.other, send_value.other
        );
        return skip.map_err(|_| ActionError::NotEnoughOtherCurrencies)
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

    time.write_msg_time(&mut msg.info, msg_index);

    // FIXME this is actually not correct, but otherwise will sometimes CellOverflow in builder
    msg.layout = (&msg).layout.filter(|layout| {
        let size = layout.compute_full_size((&msg).info.exact_size(), (&msg).init.as_ref(), (&msg).body.exact_size());
        size.bits <= MAX_BIT_LEN && size.refs <= MAX_REF_COUNT as u8
    }).clone();

    let (msg_cell, stats) = CellBuilder::build_from(&msg)
        .and_then(|cell|
            cell.compute_unique_stats(MAX_MSG_CELLS)
                .ok_or(everscale_types::error::Error::CellOverflow)
                .map(|stats| (cell, stats))
        )
        .map_err(|err| {
            tracing::error!(target: "executor", "cannot serialize message in action phase : {}", err);
            ActionError::ActionListInvalid
        })?;
    phase.total_message_size = StorageUsedShort {
        cells: phase.total_message_size.cells + stats.cell_count,
        bits: phase.total_message_size.bits + stats.bit_count,
    };

    if phase.total_message_size.bits.into_inner() as usize > MAX_MSG_BITS ||
        phase.total_message_size.cells.into_inner() as usize > MAX_MSG_CELLS {
        tracing::warn!(target: "executor", "total messages too large: bits: {}, cells: {}",
            phase.total_message_size.bits, phase.total_message_size.cells);
        return Err(ActionError::ActionListInvalid);
    }

    if mode.contains(SendMsgFlags::ALL_BALANCE | SendMsgFlags::WITH_REMAINING_BALANCE) {
        *msg_balance = CurrencyCollection::default();
    }

    if tracing::enabled!(target: "executor", tracing::Level::DEBUG) {
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
        tracing::debug!(target: "executor", "Message details:\n\tFlag: {}\n\tValue: {}\n\tSource: {}\n\tDestination: {}\n\tBody: {}\n\tStateInit: {}",
            mode.bits(),
            balance_to_string(&send_value),
            src,
            dst,
            body,
            msg.init.as_ref().map_or("None".to_string(), |_| "Present".to_string())
        );
    }

    Ok(Some(msg_cell))
}

/// Reserves some grams from accout balance.
/// Returns calculated reserved value. its calculation depends on mode.
/// Reduces balance by the amount of the reserved value.
fn reserve_action_handler(
    mode: ReserveCurrencyFlags,
    val: &CurrencyCollection,
    original_acc_balance: &CurrencyCollection,
    acc_remaining_balance: &mut CurrencyCollection,
) -> std::result::Result<CurrencyCollection, ActionError> {
    if mode.intersects(!ReserveCurrencyFlags::all()) {
        return Err(ActionError::UnknownOrInvalidAction);
    }
    tracing::debug!(target: "executor", "Reserve with mode = {} value = {}", mode.bits(), balance_to_string(val));

    let mut reserved;
    if mode.contains(ReserveCurrencyFlags::WITH_ORIGINAL_BALANCE) {
        // Append all currencies
        if mode.contains(ReserveCurrencyFlags::REVERSE) {
            reserved = original_acc_balance.clone();
            if let Some(a) = reserved.tokens.checked_sub(val.tokens) { reserved.tokens = a };
            _ = reserved.other.try_sub_assign(&val.other).or(Err(ActionError::Unsupported))?;
        } else {
            reserved = val.clone();
            reserved.tokens = reserved.tokens.checked_add(original_acc_balance.tokens).ok_or(ActionError::InvalidBalance)?;
            reserved.other.try_add_assign(&original_acc_balance.other).or(Err(ActionError::InvalidBalance))?;
        }
    } else {
        if mode.contains(ReserveCurrencyFlags::REVERSE) { // flag 8 without flag 4 unacceptable
            return Err(ActionError::UnknownOrInvalidAction);
        }
        reserved = val.clone();
    }
    if mode.contains(ReserveCurrencyFlags::IGNORE_ERROR) {
        // Only grams
        reserved.tokens = min(reserved.tokens, acc_remaining_balance.tokens);
    }

    let mut remaining = acc_remaining_balance.clone();
    if remaining.tokens < reserved.tokens {
        return Err(ActionError::NotEnoughTokens)
    }
    remaining.tokens = remaining.tokens.checked_sub(reserved.tokens).ok_or(ActionError::InvalidBalance)?;
    remaining.other.try_sub_assign(&reserved.other).ok().filter(|a| *a).ok_or(ActionError::NotEnoughOtherCurrencies)?;
    std::mem::swap(&mut remaining, acc_remaining_balance);

    if mode.contains(ReserveCurrencyFlags::ALL_BUT) {
        // swap all currencies
        std::mem::swap(&mut reserved, acc_remaining_balance);
    }

    Ok(reserved)
}

fn setcode_action_handler(acc: &mut OptionalAccount, code: Cell) -> std::result::Result<(), ActionError> {
    tracing::debug!(target: "executor", "OutAction::SetCode\nPrevious code hash: {}\nNew code hash:      {}",
        &acc.0.as_ref()
            .map(|a| match &a.state { AccountState::Active( state ) => state.code.as_ref(), _ => None } )
            .flatten().map_or(*EMPTY_CELL_HASH, |code| *code.repr_hash()),
        code.repr_hash(),
    );
    if let Some(Account { state: AccountState::Active(state), .. }) = &mut acc.0 {
        state.code = Some(code);
        Ok(())
    } else {
        Err(ActionError::BadAccountState)
    }
}

fn change_library_action_handler(acc: &mut OptionalAccount, mode: ChangeLibraryMode, lib_ref: LibRef) -> std::result::Result<(), ActionError> {
    let Some(Account { state: AccountState::Active(ref mut state), .. }) = &mut acc.0 else {
        return Err(ActionError::BadAccountState);
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
    if result { Ok(()) } else { Err(ActionError::BadAccountState) }
}

fn check_libraries(init: &StateInit, disable_set_lib: bool, text: &str, msg: &DynCell) -> bool {
    let mut len = 0;
    for a in init.libraries.iter() {
        match a {
            Ok(_) => { len += 1; },
            Err(err) => {
                tracing::trace!(
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
        tracing::trace!(
            target: "executor",
            "{} {} because libraries are disabled",
                text, msg.repr_hash()
        );
        false
    }
}

/// Calculate new account according to inbound message.
/// If message has no value, account will not created.
/// If hash of state_init is equal to account address, account will be active.
/// Otherwise, account will be nonexist or uninit according bounce flag: if bounce, account will be uninit that save money.
/// NOTE: originally this method ignored exceptions and returned None
fn account_from_message(
    msg: &InputMessage,
    msg_remaining_balance: &CurrencyCollection,
    init_code_hash: bool,
    disable_set_lib: bool,
) -> Result<Option<Account>> {
    let MsgInfo::Int(hdr) = &msg.data.info else {
        return Ok(None);
    };
    let IntAddr::Std(address_std) = &hdr.dst else {
        fail!("cannot create account from message to unsupported non-std address")
    };
    if let Some(init) = &msg.data.init {
        match &init.code {
            Some(init_code) if *CellBuilder::build_from(&init)?.repr_hash() == address_std.address => {
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
            Some(_) => {
                tracing::trace!(
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
        tracing::trace!(
            target: "executor",
            "Account will not be created. Value of {} message will be returned",
            msg.cell.repr_hash()
        );
        Ok(None)
    } else {
        let account = Account::uninit(address_std, msg_remaining_balance)?;
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