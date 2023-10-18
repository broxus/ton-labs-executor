/*
* Copyright (C) 2019-2023 TON Labs. All Rights Reserved.
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

use everscale_types::models::{GlobalCapabilities, LibDescr, SimpleLib};
use everscale_types::prelude::{Cell, HashBytes, Dict};
use everscale_vm::{
    executor::{Engine, BehaviorModifiers, gas::gas_state::Gas}, smart_contract_info::SmartContractInfo,
    stack::{Stack, StackItem, savelist::SaveList}
};
use everscale_vm::{OwnedCellSlice, types::Result};

pub struct VMSetupContext {
    pub capabilities: GlobalCapabilities,
    pub block_version: u32,
    pub signature_id: i32,
}

/// Builder for virtual machine engine. Initialises registers,
/// stack and code of VM engine. Returns initialized instance of TVM.
pub struct VMSetup {
    vm: Engine,
    code: OwnedCellSlice,
    ctrls: SaveList,
    stack: Option<Stack>,
    gas: Gas,
    ctx: VMSetupContext,
}

impl VMSetup {

    /// Creates new instance of VMSetup with contract code.
    /// Initializes some registers of TVM with predefined values.
    pub fn with_context(code: OwnedCellSlice, gas: Gas, ctx: VMSetupContext) -> Self {
        VMSetup {
            vm: Engine::with_capabilities(ctx.capabilities),
            code,
            ctrls: SaveList::new(),
            stack: None,
            gas,
            ctx,
        }
    }

    pub fn set_smart_contract_info(mut self, sci: SmartContractInfo) -> Result<VMSetup> {
        debug_assert_ne!(sci.capabilities, 0);
        let mut sci = sci.into_temp_data_item();
        self.ctrls.put(7, &mut sci)?;
        Ok(self)
    }
/*
    /// Sets SmartContractInfo for TVM register c7
    #[deprecated]
    pub fn set_contract_info_with_config(
        self,
        mut sci: SmartContractInfo,
        config: &PreloadedBlockchainConfig
    ) -> Result<VMSetup> {
        sci.capabilities |= config.global_version().capabilities.into_inner();
        self.set_smart_contract_info(sci)
    }

    /// Sets SmartContractInfo for TVM register c7
    #[deprecated]
    pub fn set_contract_info(
        self,
        mut sci: SmartContractInfo,
        with_init_code_hash: bool
    ) -> Result<VMSetup> {
        if with_init_code_hash {
            sci.capabilities |= GlobalCapability::CapInitCodeHash;
        }
        self.set_smart_contract_info(sci)
    }
*/
    /// Sets persistent data for contract in register c4
    pub fn set_data(mut self, data: Cell) -> Result<VMSetup> {
        self.ctrls.put(4, &mut StackItem::Cell(data))?;
        Ok(self)
    }

    /// Sets initial stack for TVM
    pub fn set_stack(mut self, stack: Stack) -> VMSetup {
        self.stack = Some(stack);
        self
    }

    /// Sets libraries for TVM
    pub fn set_libraries(
        mut self,
        account_libs: &Dict<HashBytes, SimpleLib>,
        shared_libs: &Dict<HashBytes, LibDescr>
    ) -> Result<VMSetup> {
        self.vm.set_libraries(account_libs, shared_libs)?;
        Ok(self)
    }

    /// Sets trace flag to TVM for printing stack and commands
    pub fn set_debug(mut self, enable: bool) -> VMSetup {
        if enable {
            self.vm.set_trace(Engine::TRACE_ALL);
        } else {
            self.vm.set_trace(0);
        }
        self
    }

    /// Disables signature check
    pub fn modify_behavior(mut self, modifiers: Option<&BehaviorModifiers>) -> VMSetup {
        if let Some(modifiers) = modifiers {
            self.vm.modify_behavior(modifiers);
        }
        self
    }

    /// Creates new instance of TVM with defined stack, registers and code.
    pub fn create(self) -> Result<Engine> {
        if cfg!(debug_assertions) {
            // account balance is duplicated in stack and in c7 - so check
            let balance_in_smc = self
                .ctrls
                .get(7)
                .unwrap()
                .as_tuple()
                .unwrap()[0]
                .as_tuple()
                .unwrap()[7]
                .as_tuple()
                .unwrap()[0]
                .as_integer()
                .unwrap();
            let stack_depth = self.stack.as_ref().unwrap().depth();
            let balance_in_stack = self
                .stack
                .as_ref()
                .unwrap()
                .get(stack_depth - 1)
                .as_integer()
                .unwrap();
            assert_eq!(balance_in_smc, balance_in_stack);
        }
        let mut vm = self.vm.setup(
            self.code,
            Some(self.ctrls),
            self.stack,
            self.gas,
        )?;
        vm.set_block_version(self.ctx.block_version);
        vm.set_signature_id(self.ctx.signature_id);
        Ok(vm)
    }
}
