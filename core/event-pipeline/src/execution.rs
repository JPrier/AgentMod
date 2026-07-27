use crate::{ActionCapabilities, CompileError, Decision, HandlerId, OrderingSpec, compile_order};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, timeout};

/// Handler-reported failure without dependency- or runtime-specific error types.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InterceptorError {
    message: String,
}

impl InterceptorError {
    /// Creates an interceptor error.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Returns the readable failure message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for InterceptorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(formatter)
    }
}

impl std::error::Error for InterceptorError {}

/// Asynchronous blocking proposal interceptor.
#[async_trait]
pub trait BlockingInterceptor<T>: Send + Sync {
    /// Evaluates a proposal and returns a typed decision.
    async fn intercept(&self, proposal: T) -> Result<Decision<T>, InterceptorError>;
}

/// Explicit behavior when an interceptor errors, times out, or returns an
/// unsupported decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailurePolicy {
    /// Stop with an aborted execution outcome.
    Abort,
    /// Fail closed with a rejection decision.
    Reject,
    /// Stop with a cancellation decision.
    Cancel,
    /// Record the failure and continue with the unchanged proposal.
    ContinueUnchanged,
}

/// One blocking interceptor and its execution controls.
pub struct InterceptorRegistration<T> {
    /// Deterministic ordering declaration.
    pub ordering: OrderingSpec,
    /// Maximum execution time for one invocation.
    pub timeout: Duration,
    /// Explicit failure behavior.
    pub failure_policy: FailurePolicy,
    /// Handler implementation.
    pub handler: Arc<dyn BlockingInterceptor<T>>,
}

impl<T> InterceptorRegistration<T> {
    /// Creates a registration.
    #[must_use]
    pub fn new(
        ordering: OrderingSpec,
        timeout: Duration,
        failure_policy: FailurePolicy,
        handler: Arc<dyn BlockingInterceptor<T>>,
    ) -> Self {
        Self {
            ordering,
            timeout,
            failure_policy,
            handler,
        }
    }
}

/// Mutable registration builder compiled into an immutable pipeline.
pub struct BlockingPipelineBuilder<T> {
    registrations: Vec<InterceptorRegistration<T>>,
}

impl<T> Default for BlockingPipelineBuilder<T> {
    fn default() -> Self {
        Self {
            registrations: Vec::new(),
        }
    }
}

impl<T> BlockingPipelineBuilder<T> {
    /// Creates an empty pipeline builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one interceptor.
    pub fn register(&mut self, registration: InterceptorRegistration<T>) {
        self.registrations.push(registration);
    }

    /// Compiles deterministic order and freezes the pipeline.
    ///
    /// # Errors
    ///
    /// Returns ordering diagnostics for duplicate handlers, missing
    /// dependencies, or cycles.
    pub fn compile(self) -> Result<BlockingPipeline<T>, CompileError> {
        let specifications: Vec<_> = self
            .registrations
            .iter()
            .map(|registration| registration.ordering.clone())
            .collect();
        let order = compile_order(&specifications)?;
        let mut by_id: BTreeMap<_, _> = self
            .registrations
            .into_iter()
            .map(|registration| (registration.ordering.handler.clone(), registration))
            .collect();
        let registrations = order
            .handlers()
            .iter()
            .filter_map(|handler| by_id.remove(handler))
            .collect();
        Ok(BlockingPipeline { registrations })
    }
}

/// Immutable compiled blocking pipeline.
pub struct BlockingPipeline<T> {
    registrations: Vec<InterceptorRegistration<T>>,
}

impl<T> BlockingPipeline<T>
where
    T: Clone + Send + 'static,
{
    /// Executes handlers in compiled order with per-handler timeout and
    /// capability validation.
    pub async fn execute(
        &self,
        proposal: T,
        capabilities: ActionCapabilities,
    ) -> ExecutionReport<T> {
        let pipeline_started = Instant::now();
        let mut current = proposal;
        let mut steps = Vec::with_capacity(self.registrations.len());

        for registration in &self.registrations {
            let input = current.clone();
            let started = Instant::now();
            let invocation = timeout(
                registration.timeout,
                registration.handler.intercept(input.clone()),
            )
            .await;
            let duration = started.elapsed();

            let decision = match invocation {
                Ok(Ok(decision)) => match capabilities.validate(&decision) {
                    Ok(()) => {
                        steps.push(ExecutionStep {
                            handler: registration.ordering.handler.clone(),
                            input,
                            result: ExecutionStepResult::Decision(decision.clone()),
                            duration,
                        });
                        match decision {
                            Decision::Continue(value) | Decision::Replace(value) => {
                                current = value;
                                continue;
                            }
                            terminal => {
                                return ExecutionReport {
                                    steps,
                                    outcome: ExecutionOutcome::Decision(terminal),
                                    duration: pipeline_started.elapsed(),
                                };
                            }
                        }
                    }
                    Err(error) => HandlerFailure {
                        kind: HandlerFailureKind::UnsupportedDecision,
                        message: error.to_string(),
                    },
                },
                Ok(Err(error)) => HandlerFailure {
                    kind: HandlerFailureKind::HandlerError,
                    message: error.to_string(),
                },
                Err(_) => HandlerFailure {
                    kind: HandlerFailureKind::Timeout,
                    message: format!(
                        "handler `{}` exceeded timeout of {} ms",
                        registration.ordering.handler,
                        registration.timeout.as_millis()
                    ),
                },
            };

            steps.push(ExecutionStep {
                handler: registration.ordering.handler.clone(),
                input,
                result: ExecutionStepResult::Failure(decision.clone()),
                duration,
            });
            let reason = format!(
                "interceptor `{}` failed: {}",
                registration.ordering.handler, decision.message
            );
            match registration.failure_policy {
                FailurePolicy::ContinueUnchanged => {}
                FailurePolicy::Reject => {
                    return ExecutionReport {
                        steps,
                        outcome: ExecutionOutcome::Decision(Decision::Reject { reason }),
                        duration: pipeline_started.elapsed(),
                    };
                }
                FailurePolicy::Cancel => {
                    return ExecutionReport {
                        steps,
                        outcome: ExecutionOutcome::Decision(Decision::Cancel { reason }),
                        duration: pipeline_started.elapsed(),
                    };
                }
                FailurePolicy::Abort => {
                    return ExecutionReport {
                        steps,
                        outcome: ExecutionOutcome::Aborted {
                            handler: registration.ordering.handler.clone(),
                            failure: decision,
                        },
                        duration: pipeline_started.elapsed(),
                    };
                }
            }
        }

        ExecutionReport {
            steps,
            outcome: ExecutionOutcome::Decision(Decision::Continue(current)),
            duration: pipeline_started.elapsed(),
        }
    }
}

/// Classified handler failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandlerFailureKind {
    /// Handler returned an error.
    HandlerError,
    /// Handler exceeded its configured deadline.
    Timeout,
    /// Handler returned a decision unsupported by the action.
    UnsupportedDecision,
}

/// Failure recorded in an execution step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandlerFailure {
    /// Failure classification.
    pub kind: HandlerFailureKind,
    /// Readable failure detail.
    pub message: String,
}

/// Recorded result of invoking one handler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionStepResult<T> {
    /// Handler returned a valid typed decision.
    Decision(Decision<T>),
    /// Handler failed or selected an unsupported decision.
    Failure(HandlerFailure),
}

/// Auditable execution information for one handler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionStep<T> {
    /// Invoked handler.
    pub handler: HandlerId,
    /// Exact proposal supplied to the handler.
    pub input: T,
    /// Decision or classified failure.
    pub result: ExecutionStepResult<T>,
    /// Handler execution duration.
    pub duration: Duration,
}

/// Terminal pipeline result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionOutcome<T> {
    /// A normal typed decision.
    Decision(Decision<T>),
    /// Failure policy required the pipeline to abort outside decision semantics.
    Aborted {
        /// Handler causing the abort.
        handler: HandlerId,
        /// Classified failure.
        failure: HandlerFailure,
    },
}

/// Complete auditable report for one pipeline execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionReport<T> {
    /// Ordered handler steps.
    pub steps: Vec<ExecutionStep<T>>,
    /// Terminal outcome.
    pub outcome: ExecutionOutcome<T>,
    /// Total execution duration.
    pub duration: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ApprovalRequest, ContinuationId};
    use std::future::pending;

    struct FunctionInterceptor<F>(F);

    #[async_trait]
    impl<T, F, Fut> BlockingInterceptor<T> for FunctionInterceptor<F>
    where
        T: Send + 'static,
        F: Fn(T) -> Fut + Send + Sync,
        Fut: Future<Output = Result<Decision<T>, InterceptorError>> + Send,
    {
        async fn intercept(&self, proposal: T) -> Result<Decision<T>, InterceptorError> {
            (self.0)(proposal).await
        }
    }

    #[tokio::test]
    async fn records_replacement_inputs_and_terminal_decision() {
        let mut builder = BlockingPipelineBuilder::new();
        builder.register(registration("replace", |value| async move {
            Ok(Decision::Replace(value + 1))
        }));
        builder.register(InterceptorRegistration::new(
            OrderingSpec::new("approve", "test").after("replace"),
            Duration::from_secs(1),
            FailurePolicy::Abort,
            Arc::new(FunctionInterceptor(|value| async move {
                Ok(Decision::RequireApproval {
                    request: ApprovalRequest {
                        summary: format!("approve {value}"),
                    },
                    continuation: ContinuationId("next".into()),
                })
            })),
        ));
        let report = builder
            .compile()
            .expect("valid pipeline")
            .execute(10, ActionCapabilities::all())
            .await;

        assert_eq!(report.steps.len(), 2);
        assert_eq!(report.steps[0].input, 10);
        assert_eq!(report.steps[1].input, 11);
        assert!(matches!(
            report.outcome,
            ExecutionOutcome::Decision(Decision::RequireApproval { .. })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_uses_explicit_reject_policy() {
        let mut builder = BlockingPipelineBuilder::new();
        builder.register(InterceptorRegistration::new(
            OrderingSpec::new("slow", "test"),
            Duration::from_secs(5),
            FailurePolicy::Reject,
            Arc::new(FunctionInterceptor(|_: u8| async {
                pending::<Result<Decision<u8>, InterceptorError>>().await
            })),
        ));
        let pipeline = builder.compile().expect("valid pipeline");
        let execution = pipeline.execute(1, ActionCapabilities::all());
        tokio::pin!(execution);
        tokio::time::advance(Duration::from_secs(5)).await;
        let report = execution.await;

        assert!(matches!(
            report.steps[0].result,
            ExecutionStepResult::Failure(HandlerFailure {
                kind: HandlerFailureKind::Timeout,
                ..
            })
        ));
        assert!(matches!(
            report.outcome,
            ExecutionOutcome::Decision(Decision::Reject { .. })
        ));
    }

    #[tokio::test]
    async fn unsupported_decision_obeys_continue_policy() {
        let mut builder = BlockingPipelineBuilder::new();
        builder.register(InterceptorRegistration::new(
            OrderingSpec::new("replace", "test"),
            Duration::from_secs(1),
            FailurePolicy::ContinueUnchanged,
            Arc::new(FunctionInterceptor(|value| async move {
                Ok(Decision::Replace(value + 100))
            })),
        ));
        builder.register(InterceptorRegistration::new(
            OrderingSpec::new("continue", "test").after("replace"),
            Duration::from_secs(1),
            FailurePolicy::Abort,
            Arc::new(FunctionInterceptor(|value| async move {
                Ok(Decision::Continue(value + 1))
            })),
        ));
        let report = builder
            .compile()
            .expect("valid pipeline")
            .execute(4, ActionCapabilities::minimal())
            .await;

        assert!(matches!(
            report.steps[0].result,
            ExecutionStepResult::Failure(HandlerFailure {
                kind: HandlerFailureKind::UnsupportedDecision,
                ..
            })
        ));
        assert_eq!(
            report.outcome,
            ExecutionOutcome::Decision(Decision::Continue(5))
        );
    }

    fn registration<F, Fut>(name: &str, function: F) -> InterceptorRegistration<i32>
    where
        F: Fn(i32) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Decision<i32>, InterceptorError>> + Send + 'static,
    {
        InterceptorRegistration::new(
            OrderingSpec::new(name, "test"),
            Duration::from_secs(1),
            FailurePolicy::Abort,
            Arc::new(FunctionInterceptor(function)),
        )
    }
}
