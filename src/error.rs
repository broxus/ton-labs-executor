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

use everscale_types::models::ComputePhaseSkipReason;
use tycho_vm::VmException;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ExecutorError {
    #[error("Invalid external message")]
    InvalidExtMessage,
    #[error("Transaction executor internal error: {0}")]
    TrExecutorError(String),
    #[error("VM Exception, code: {0}")]
    TvmExceptionCode(VmException),
    #[error("Contract did not accept message, exit code: {0}")]
    NoAcceptError(i32),
    #[error("Cannot pay for importing this external message")]
    NoFundsToImportMsg,
    #[error("Compute phase skipped while processing external inbound message with reason {0:?}")]
    ExtMsgComputeSkipped(ComputePhaseSkipReason)
}
