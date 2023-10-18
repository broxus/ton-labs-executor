use everscale_types::models::GasLimitsPrices;

pub trait GasLimitsPricesExt {
    fn calc_gas_fee(&self, gas_used: u64) -> u128;
    fn calc_gas(&self, value: u128) -> u64;
    fn get_real_gas_price(&self) -> u64;
}

impl GasLimitsPricesExt for GasLimitsPrices {
    /// Calculate gas fee by gas used value
    fn calc_gas_fee(&self, gas_used: u64) -> u128 {
        // There is a flat_gas_limit value which is the minimum gas value possible and has fixed price.
        // If actual gas value is less then flat_gas_limit then flat_gas_price paid.
        // If actual gas value is bigger then flat_gas_limit then flat_gas_price paid for first
        // flat_gas_limit gas and remaining value costs gas_price
        if gas_used <= self.flat_gas_limit {
            self.flat_gas_price as u128
        } else {
            // gas_price is pseudo value (shifted by 16 as forward and storage price)
            // after calculation divide by 0xffff with ceil rounding
            self.flat_gas_price as u128
                + (((gas_used - self.flat_gas_limit) as u128 * self.gas_price as u128 + 0xffff)
                    >> 16)
        }
    }
    /// Calculate gas by grams balance
    fn calc_gas(&self, value: u128) -> u64 {
        // if value >= self.max_gas_threshold {
        //     return self.gas_limit
        // }
        if value < self.flat_gas_price as u128 {
            return 0;
        }
        let res = ((value - self.flat_gas_price as u128) << 16) / (self.gas_price as u128);
        self.flat_gas_limit + res as u64
    }

    /// Get gas price in nanograms
    fn get_real_gas_price(&self) -> u64 {
        self.gas_price >> 16
    }
}
