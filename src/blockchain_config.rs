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


use ahash::{HashMap, HashSet};
use everscale_types::cell::{CellTreeStats, HashBytes};
use everscale_types::dict::DictKey;
use everscale_types::models::{BlockchainConfig, GasLimitsPrices, GlobalCapabilities, GlobalCapability, GlobalVersion, IntAddr, MsgForwardPrices, StorageInfo, StoragePrices, WorkchainDescription, WorkchainFormat, WorkchainFormatBasic};
use everscale_types::num::Tokens;
use everscale_types::prelude::{Cell, CellBuilder, CellFamily, Dict, Store};
use everscale_vm::fail;
use everscale_vm::types::Result;

use crate::ext::gas_limit_prices::GasLimitsPricesExt;

pub const VERSION_BLOCK_REVERT_MESSAGES_WITH_ANYCAST_ADDRESSES: u32 = 8;
pub const VERSION_BLOCK_NEW_CALCULATION_BOUNCED_STORAGE: u32 = 30;

pub const MAX_ACTIONS: u16 = 255;

pub const MAX_MSG_BITS: usize = 1 << 21;

pub const MAX_MSG_CELLS: usize = 1 << 13;
/// Limited only by storage stats data type. Account size should be regulated by storage fee.
pub const MAX_ACCOUNT_CELLS: usize = 1 << 45;

pub(crate) trait TONDefaultConfig {
    /// Get default value for masterchain
    fn default_mc() -> Self;
    /// Get default value for workchains
    fn default_wc() -> Self;
}

impl TONDefaultConfig for MsgForwardPrices {
    fn default_mc() -> Self {
        MsgForwardPrices {
            lump_price: 10000000,
            bit_price: 655360000,
            cell_price: 65536000000,
            ihr_price_factor: 98304,
            first_frac: 21845,
            next_frac: 21845,
        }
    }

    fn default_wc() -> Self {
        MsgForwardPrices {
            lump_price: 1000000,
            bit_price: 65536000,
            cell_price: 6553600000,
            ihr_price_factor: 98304,
            first_frac: 21845,
            next_frac: 21845,
        }
    }
}

pub trait CalcMsgFwdFees {
    fn fwd_fee(&self, msg_cell: &CellTreeStats) -> u128;
    fn ihr_fee(&self, fwd_fee: &Tokens) -> Option<Tokens>;
    fn mine_fee(&self, fwd_fee: &Tokens) -> Option<Tokens>;
    fn next_fee(&self, fwd_fee: &Tokens) -> Option<Tokens>;
}

impl CalcMsgFwdFees for MsgForwardPrices {
    /// Calculate message forward fee
    /// Forward fee is calculated according to the following formula:
    /// `fwd_fee = (lump_price + ceil((bit_price * msg.bits + cell_price * msg.cells)/2^16))`.
    /// `msg.bits` and `msg.cells` are calculated from message represented as tree of cells. Root cell is not counted.
    fn fwd_fee(&self, msg_stats_no_root: &CellTreeStats) -> u128 {
        let bits = msg_stats_no_root.bit_count as u128;
        let cells = msg_stats_no_root.cell_count as u128;

        // All prices except `lump_price` are presented in `0xffff * price` form.
        // It is needed because `ihr_factor`, `first_frac` and `next_frac` are not integer values
        // but calculations are performed in integers, so prices are multiplied to some big
        // number (0xffff) and fee calculation uses such values. At the end result is divided by
        // 0xffff with ceil rounding to obtain nanoTokens (add 0xffff and then `>> 16`)
        self.lump_price as u128 + ((cells * self.cell_price as u128 + bits * self.bit_price as u128 + 0xffff) >> 16)
    }

    /// Calculate message IHR fee
    /// IHR fee is calculated as `(msg_forward_fee * ihr_factor) >> 16`
    fn ihr_fee(&self, fwd_fee: &Tokens) -> Option<Tokens> {
        Tokens::try_from((fwd_fee.into_inner() * self.ihr_price_factor as u128) >> 16).ok()
    }

    /// Calculate mine part of forward fee
    /// Forward fee for internal message is splited to `int_msg_mine_fee` and `int_msg_remain_fee`:
    /// `msg_forward_fee = int_msg_mine_fee + int_msg_remain_fee`
    /// `int_msg_mine_fee` is a part of transaction `total_fees` and will go validators of account's shard
    /// `int_msg_remain_fee` is placed in header of internal message and will go to validators
    /// of shard to which message destination address is belong.
    fn mine_fee(&self, fwd_fee: &Tokens) -> Option<Tokens> {
        Tokens::try_from((fwd_fee.into_inner() * self.first_frac as u128) >> 16).ok()
    }
    fn next_fee(&self, fwd_fee: &Tokens) -> Option<Tokens> {
        Tokens::try_from((fwd_fee.into_inner() * self.next_frac as u128) >> 16).ok()
    }
}

#[derive(Clone)]
pub struct AccStoragePrices {
    prices: Vec<StoragePrices>,
}

impl Default for AccStoragePrices {
    fn default() -> Self {
        AccStoragePrices {
            prices: vec![
                StoragePrices {
                    utime_since: 0,
                    bit_price_ps: 1,
                    cell_price_ps: 500,
                    mc_bit_price_ps: 1000,
                    mc_cell_price_ps: 500000,
                }
            ]
        }
    }
}

impl AccStoragePrices {
    /// Calculate storage fee for provided data
    pub fn calc_storage_fee(&self, cells: u64, bits: u64, mut last_paid: u32, now: u32, is_masterchain: bool) -> u128 {
        if now <= last_paid || last_paid == 0 || self.prices.is_empty() || now <= self.prices[0].utime_since {
            return 0;
        }
        let mut fee = 0u128;
        // storage prices config contains prices array for some time intervals
        // to calculate account storage fee we need to sum fees for all intervals since last
        // storage fee pay calculated by formula `(cells * cell_price + bits * bits_price) * interval`
        for i in 0..self.prices.len() {
            let prices = &self.prices[i];
            let end = if i < self.prices.len() - 1 {
                self.prices[i + 1].utime_since
            } else {
                now
            };

            if end >= last_paid {
                let delta = end - std::cmp::max(prices.utime_since, last_paid);
                fee += if is_masterchain {
                    (cells as u128 * prices.mc_cell_price_ps as u128 + bits as u128 * prices.mc_bit_price_ps as u128) * delta as u128
                } else {
                    (cells as u128 * prices.cell_price_ps as u128 + bits as u128 * prices.bit_price_ps as u128) * delta as u128
                };
                last_paid = end;
            }
        }

        // stirage fee is calculated in pseudo values (like forward fee and gas fee) - multiplied
        // to 0xffff, so divide by this value with ceil rounding
        (fee + 0xffff) >> 16
    }

    pub fn with_config(config18: Dict<u32, StoragePrices>) -> Result<Self> {
        let mut prices = vec![];
        for (i, kv) in config18.iter().enumerate() {
            let (k, v) = kv?;
            if i as u32 != k {
                fail!(format!("config18 is sparse: got key {k} for element {i}"));
            }
            prices.push(v);
        }
        prices.shrink_to_fit();
        Ok(AccStoragePrices { prices })
    }
}

impl TONDefaultConfig for GasLimitsPrices {
    fn default_mc() -> Self {
        GasLimitsPrices {
            gas_price: 655360000,
            flat_gas_limit: 100,
            flat_gas_price: 1000000,
            gas_limit: 1000000,
            special_gas_limit: 10000000,
            gas_credit: 10000,
            block_gas_limit: 10000000,
            freeze_due_limit: 100000000,
            delete_due_limit: 1000000000,
        }
    }

    fn default_wc() -> Self {
        GasLimitsPrices {
            gas_price: 65536000,
            flat_gas_limit: 100,
            flat_gas_price: 100000,
            gas_limit: 1000000,
            special_gas_limit: 1000000,
            gas_credit: 10000,
            block_gas_limit: 10000000,
            freeze_due_limit: 100000000,
            delete_due_limit: 1000000000,
        }
    }
}

/// Blockchain configuration parameters
#[derive(Clone)]
pub struct PreloadedBlockchainConfig {
    gas_prices_mc: GasLimitsPrices,
    gas_prices_wc: GasLimitsPrices,
    fwd_prices_mc: MsgForwardPrices,
    fwd_prices_wc: MsgForwardPrices,
    storage_prices: AccStoragePrices,
    special_contracts: HashSet<HashBytes>,
    workchains: HashMap<i32, WorkchainDescription>,
    global_version: GlobalVersion,
    global_id: i32,
    raw_config: BlockchainConfig,
}

impl Default for PreloadedBlockchainConfig {
    fn default() -> Self {
        Self {
            gas_prices_mc: GasLimitsPrices::default_mc(),
            gas_prices_wc: GasLimitsPrices::default_wc(),
            fwd_prices_mc: MsgForwardPrices::default_mc(),
            fwd_prices_wc: MsgForwardPrices::default_wc(),
            storage_prices: AccStoragePrices::default(),
            special_contracts: Self::default_special_contracts().keys()
                .collect::<std::result::Result<_, _>>().expect("shouldn't fail"),
            workchains: Self::default_workchains().iter()
                .collect::<std::result::Result<_, _>>().expect("shouldn't fail"),
            raw_config: Self::default_raw_config(),
            global_version: Self::default_global_version(),
            global_id: 42,
        }
    }
}

impl PreloadedBlockchainConfig {

    fn default_special_contracts() -> Dict<HashBytes, ()> {
        let mut fundamental_contracts = Dict::<HashBytes, ()>::new();
        for special_contract in vec![
            HashBytes::from([0x33u8; 32]),
            HashBytes::from([0x66u8; 32]),
            "34517C7BDF5187C55AF4F8B61FDC321588C7AB768DEE24B006DF29106458D7CF".parse::<HashBytes>().unwrap(),
        ] {
            fundamental_contracts.add(special_contract, ()).expect("Shouldn't fail");
        }
        fundamental_contracts
    }

    fn default_global_version() -> GlobalVersion {
        GlobalVersion {
            version: 0,
            capabilities: GlobalCapabilities::new(0x52e),
        }
    }

    fn default_workchains() -> Dict<i32, WorkchainDescription> {
        let mut workchains = Dict::<i32, WorkchainDescription>::new();
        workchains.add(0, WorkchainDescription {
            enabled_since: 0,
            actual_min_split: 0,
            min_split: 0,
            max_split: 0,
            active: true,
            accept_msgs: true,
            zerostate_root_hash: HashBytes::default(),
            zerostate_file_hash: HashBytes::default(),
            version: 0,
            format: WorkchainFormat::Basic(WorkchainFormatBasic {
                vm_version: 0,
                vm_mode: 0,
            }),
        }).expect("Shouldn't fail");
        workchains
    }

    fn default_raw_config() -> BlockchainConfig {
        fn store<K: Store + DictKey, V: Store>(dict: &mut Dict::<K, Cell>, key: K, value: V) -> Result<()> {
            let mut builder = CellBuilder::new();
            value.store_into(&mut builder, &mut Cell::empty_context())?;
            dict.add(key, builder.build()?)?;
            Ok(())
        }

        let mut dict = Dict::<u32, Cell>::new();

        // store(&mut dict, 20, GasLimitsPrices::default_mc()).expect("Shouldn't fail");
        // store(&mut dict, 21, GasLimitsPrices::default_wc()).expect("Shouldn't fail");
        // store(&mut dict, 24, MsgForwardPrices::default_mc()).expect("Shouldn't fail");
        // store(&mut dict, 25, MsgForwardPrices::default_wc()).expect("Shouldn't fail");
        // store(&mut dict, 18, AccStoragePrices::default()).expect("Shouldn't fail");
        // store(&mut dict, 31, Self::default_special_contracts()).expect("Shouldn't fail");
        store(&mut dict, 12, Self::default_workchains()).expect("Shouldn't fail");
        store(&mut dict, 8, Self::default_global_version()).expect("Shouldn't fail");

        BlockchainConfig {
            address: [0x55; 32].into(),
            params: dict,
        }
    }

    /// Create `BlockchainConfig` struct with `ConfigParams` taken from blockchain
    pub fn with_config(config: BlockchainConfig, global_id: i32) -> Result<Self> {
        Ok(PreloadedBlockchainConfig {
            gas_prices_mc: config.get_gas_prices(true)?,
            gas_prices_wc: config.get_gas_prices(false)?,
            fwd_prices_mc: config.get_msg_forward_prices(true)?,
            fwd_prices_wc: config.get_msg_forward_prices(false)?,
            storage_prices: AccStoragePrices::with_config(config.get_storage_prices()?)?,
            special_contracts: config.get_fundamental_addresses()?.keys()
                .collect::<std::result::Result<_, _>>()?,
            workchains: config.get_workchains()?.iter()
                .collect::<std::result::Result<_, _>>()?,
            global_version: config.get_global_version()?,
            global_id,
            raw_config: config,
        })
    }

    pub fn global_id(&self) -> i32 {
        self.global_id
    }

    /// Get `MsgForwardPrices` for message forward fee calculation
    pub fn get_fwd_prices(&self, is_masterchain: bool) -> &MsgForwardPrices {
        if is_masterchain {
            &self.fwd_prices_mc
        } else {
            &self.fwd_prices_wc
        }
    }

    /// Calculate gas fee for account
    pub fn calc_gas_fee(&self, gas_used: u64, address: &IntAddr) -> u128 {
        self.get_gas_config(address.is_masterchain()).calc_gas_fee(gas_used)
    }

    /// Get `GasLimitsPrices` for account gas fee calculation
    pub fn get_gas_config(&self, is_masterchain: bool) -> &GasLimitsPrices {
        if is_masterchain {
            &self.gas_prices_mc
        } else {
            &self.gas_prices_wc
        }
    }

    /// Calculate forward fee. Root cell must not be accounted is stats.
    pub fn calc_fwd_fee(&self, is_masterchain: bool, msg_stats_no_root: &CellTreeStats) -> Result<Tokens> {
        let mut in_fwd_fee = self.get_fwd_prices(is_masterchain).fwd_fee(msg_stats_no_root);
        if self.global_version.capabilities.contains(GlobalCapability::CapFeeInGasUnits) {
            in_fwd_fee = self.get_gas_config(is_masterchain).calc_gas_fee(in_fwd_fee.try_into()?);
        }
        Ok(Tokens::try_from(in_fwd_fee)?)
    }

    /// Calculate account storage fee
    pub fn calc_storage_fee(&self, storage: &StorageInfo, is_masterchain: bool, now: u32) -> Result<Tokens> {
        let mut storage_fee = self.storage_prices.calc_storage_fee(
            storage.used.cells.into_inner(),
            storage.used.bits.into_inner(),
            storage.last_paid,
            now,
            is_masterchain,
        );
        if self.global_version.capabilities.contains(GlobalCapability::CapFeeInGasUnits) {
            storage_fee = self.get_gas_config(is_masterchain).calc_gas_fee(storage_fee.try_into()?);
        }
        Ok(Tokens::try_from(storage_fee)?)
    }

    /// Check if account is special TON account
    pub fn is_special_account(&self, address: &IntAddr) -> bool {
        if address.is_masterchain() {
            if let Some(account_id) = address.as_std().map(|a| a.address) {
                // special account adresses are stored in hashmap
                // config account is special too
                return self.raw_config.address == account_id ||
                    self.special_contracts.contains(&account_id.0)
            }
        }
        false
    }

    pub fn global_version(&self) -> GlobalVersion {
        self.global_version
    }

    pub fn storage_prices(&self) -> &AccStoragePrices {
        &self.storage_prices
    }

    pub fn workchains(&self) -> &HashMap<i32, WorkchainDescription> {
        &self.workchains
    }

    pub fn raw_config(&self) -> &BlockchainConfig {
        &self.raw_config
    }
}
