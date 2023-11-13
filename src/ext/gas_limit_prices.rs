use std::cmp::min;

use everscale_types::models::GasLimitsPrices;
use everscale_types::num::{Tokens, VarUint24, VarUint56};
use everscale_vm::executor::gas::gas_state::Gas;
use everscale_vm::fail;
use everscale_vm::types::Result;

pub trait GasLimitsPricesExt {
    fn calc_gas_fee(&self, gas_used: u64) -> Tokens;
    fn init_gas(
        &self,
        acc_balance: &Tokens,
        msg_balance: &Tokens,
        is_external: bool,
        is_special: bool,
        is_ordinary: bool,
    ) -> Result<Gas>;
}

impl GasLimitsPricesExt for GasLimitsPrices {
    /// Calculate gas fee by gas used value
    fn calc_gas_fee(&self, gas_used: u64) -> Tokens {
        // There is a flat_gas_limit value which is the minimum gas value possible and has fixed price.
        // If actual gas value is less then flat_gas_limit then flat_gas_price paid.
        // If actual gas value is bigger then flat_gas_limit then flat_gas_price paid for first
        // flat_gas_limit gas and remaining value costs gas_price
        if gas_used <= self.flat_gas_limit {
            Tokens::new(self.flat_gas_price as u128)
        } else {
            // gas_price is pseudo value (shifted by 16 as forward and storage price)
            // after calculation divide by 0xffff with ceil rounding
            Tokens::new(self.flat_gas_price as u128 + (
                ((gas_used - self.flat_gas_limit) as u128 * self.gas_price as u128 + 0xffff) >> 16
            ))
        }
    }

    fn init_gas(
        &self,
        acc_balance: &Tokens,
        msg_balance: &Tokens,
        is_external: bool,
        is_special: bool,
        is_ordinary: bool,
    ) -> Result<Gas> {
        if self.gas_limit > VarUint56::MAX.into_inner() {
            fail!("config error: gas_limit exceeds 56 bits and cannot be stored in tx.info.compute_phase")
        }
        if self.special_gas_limit > VarUint56::MAX.into_inner() {
            fail!("config error: special_gas_limit exceeds 56 bits and cannot be stored in tx.info.compute_phase")
        }
        if self.gas_credit > VarUint24::MAX.into_inner() as u64 {
            fail!("config error: gas_credit exceeds 24 bits and cannot be stored in tx.info.compute_phase")
        }
        let gas_max = if is_special {
            self.special_gas_limit
        } else {
            calc_gas(self, acc_balance).map_or(self.gas_limit, |a| min(self.gas_limit, a))
        };
        let mut gas_credit = 0;
        let gas_limit = if !is_ordinary {
            gas_max
        } else {
            if is_external {
                gas_credit = min(self.gas_credit, gas_max) as u32;
            }
            calc_gas(self, msg_balance).map_or(gas_max, |a| min(gas_max, a))
        };
        let gas_price_nano = self.gas_price >> 16;
        if gas_price_nano << 16 != self.gas_price || gas_price_nano == 0 {
            // forbid division by zero and imprecise computation
            fail!("config error: gas_price must have at least 16 trailing zero bits")
        }
        log::debug!(
            target: "executor",
            "gas before: gm: {}, gl: {}, gc: {}, price: {}",
            gas_max, gas_limit, gas_credit, gas_price_nano
        );

        Ok(Gas::new(gas_limit, gas_credit, gas_max, gas_price_nano))
    }
}
/// Calculate gas by token balance
fn calc_gas(gas_config: &GasLimitsPrices, value: &Tokens) -> Option<u64> {
    match value.into_inner() {
        value if value <= gas_config.flat_gas_price as u128 => Some(0),
        value => {
            let res = ((value - gas_config.flat_gas_price as u128) << 16) / (gas_config.gas_price as u128);
            u64::try_from(gas_config.flat_gas_limit as u128 + res).ok()
        }
    }
}
