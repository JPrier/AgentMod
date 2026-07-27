//! Deterministic, dependency-light primitives for intercepting proposals and
//! dispatching committed events.
//!
//! The crate deliberately knows nothing about sessions, providers, tools,
//! persistence, or canonical state. Runtime code supplies typed proposals,
//! handlers, capabilities, and observer events.

mod compiler;
mod decision;
mod execution;
mod observer;

pub use compiler::{
    CompileDiagnostic, CompileError, CompiledOrder, HandlerId, OrderingSpec, PluginId,
    compile_order,
};
pub use decision::{
    ActionCapabilities, ApprovalRequest, ContinuationId, Decision, DecisionCapability,
    DecisionCapabilityError, JoinPolicy, WakeCondition,
};
pub use execution::{
    BlockingInterceptor, BlockingPipeline, BlockingPipelineBuilder, ExecutionOutcome,
    ExecutionReport, ExecutionStep, ExecutionStepResult, FailurePolicy, HandlerFailure,
    HandlerFailureKind, InterceptorError, InterceptorRegistration,
};
pub use observer::{
    AsyncObserver, BackpressurePolicy, DispatchOutcome, ObserverConfigError, ObserverDispatcher,
    ObserverError, ObserverStats,
};
