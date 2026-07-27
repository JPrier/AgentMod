use async_trait::async_trait;
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as SyncMutex};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;

/// Observer-reported failure. Failures are counted and do not alter committed
/// events or canonical state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObserverError {
    message: String,
}

impl ObserverError {
    /// Creates an observer error.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Returns the readable error.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(formatter)
    }
}

impl std::error::Error for ObserverError {}

/// Asynchronous receiver of already committed events.
///
/// The interface exposes only event delivery. It intentionally provides no
/// canonical state write capability.
#[async_trait]
pub trait AsyncObserver<E>: Send + Sync {
    /// Observes one committed event.
    async fn observe(&self, event: E) -> Result<(), ObserverError>;
}

/// Behavior when an observer queue reaches its configured capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackpressurePolicy {
    /// Wait for capacity.
    Block,
    /// Drop the event currently being dispatched.
    DropNewest,
    /// Remove the oldest queued event and enqueue the new event.
    DropOldest,
}

/// Result of one dispatch attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatchOutcome {
    /// Event entered the bounded queue.
    Enqueued,
    /// New event was dropped.
    DroppedNewest,
    /// Oldest queued event was dropped and the new event was enqueued.
    DroppedOldest,
    /// Dispatcher had already begun shutdown.
    Closed,
}

/// Invalid observer dispatcher configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObserverConfigError;

impl fmt::Display for ObserverConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("observer queue capacity must be greater than zero")
    }
}

impl std::error::Error for ObserverConfigError {}

/// Snapshot of observer queue and delivery counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObserverStats {
    /// Events accepted into the queue.
    pub enqueued: u64,
    /// New events dropped due to pressure.
    pub dropped_newest: u64,
    /// Old queued events dropped due to pressure.
    pub dropped_oldest: u64,
    /// Events successfully delivered.
    pub delivered: u64,
    /// Observer calls returning errors.
    pub failed: u64,
}

struct QueueState<E> {
    events: VecDeque<E>,
    closed: bool,
}

struct SharedQueue<E> {
    state: Mutex<QueueState<E>>,
    not_empty: Notify,
    not_full: Notify,
    capacity: usize,
    policy: BackpressurePolicy,
    enqueued: AtomicU64,
    dropped_newest: AtomicU64,
    dropped_oldest: AtomicU64,
    delivered: AtomicU64,
    failed: AtomicU64,
}

impl<E> SharedQueue<E> {
    fn stats(&self) -> ObserverStats {
        ObserverStats {
            enqueued: self.enqueued.load(Ordering::Relaxed),
            dropped_newest: self.dropped_newest.load(Ordering::Relaxed),
            dropped_oldest: self.dropped_oldest.load(Ordering::Relaxed),
            delivered: self.delivered.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }
}

/// Bounded single-observer asynchronous dispatcher.
///
/// Each observer receives events sequentially, preserving accepted queue order.
/// Dropping a dispatcher aborts its worker; call [`Self::shutdown`] to drain.
pub struct ObserverDispatcher<E> {
    shared: Arc<SharedQueue<E>>,
    worker: SyncMutex<Option<JoinHandle<()>>>,
}

impl<E> ObserverDispatcher<E>
where
    E: Send + 'static,
{
    /// Starts a dispatcher and its bounded worker.
    ///
    /// # Errors
    ///
    /// Returns an error when `capacity` is zero.
    pub fn new(
        observer: Arc<dyn AsyncObserver<E>>,
        capacity: usize,
        policy: BackpressurePolicy,
    ) -> Result<Self, ObserverConfigError> {
        if capacity == 0 {
            return Err(ObserverConfigError);
        }
        let shared = Arc::new(SharedQueue {
            state: Mutex::new(QueueState {
                events: VecDeque::with_capacity(capacity),
                closed: false,
            }),
            not_empty: Notify::new(),
            not_full: Notify::new(),
            capacity,
            policy,
            enqueued: AtomicU64::new(0),
            dropped_newest: AtomicU64::new(0),
            dropped_oldest: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            failed: AtomicU64::new(0),
        });
        let worker_shared = Arc::clone(&shared);
        let worker = tokio::spawn(async move {
            observer_worker(worker_shared, observer).await;
        });
        Ok(Self {
            shared,
            worker: SyncMutex::new(Some(worker)),
        })
    }

    /// Dispatches an event under the configured pressure policy.
    pub async fn dispatch(&self, event: E) -> DispatchOutcome {
        let mut pending = Some(event);
        loop {
            let notification = self.shared.not_full.notified();
            let mut state = self.shared.state.lock().await;
            if state.closed {
                return DispatchOutcome::Closed;
            }
            if state.events.len() < self.shared.capacity {
                let Some(event) = pending.take() else {
                    return DispatchOutcome::Closed;
                };
                state.events.push_back(event);
                self.shared.enqueued.fetch_add(1, Ordering::Relaxed);
                drop(state);
                self.shared.not_empty.notify_one();
                return DispatchOutcome::Enqueued;
            }
            match self.shared.policy {
                BackpressurePolicy::Block => {
                    drop(state);
                    notification.await;
                }
                BackpressurePolicy::DropNewest => {
                    self.shared.dropped_newest.fetch_add(1, Ordering::Relaxed);
                    return DispatchOutcome::DroppedNewest;
                }
                BackpressurePolicy::DropOldest => {
                    state.events.pop_front();
                    let Some(event) = pending.take() else {
                        return DispatchOutcome::Closed;
                    };
                    state.events.push_back(event);
                    self.shared.dropped_oldest.fetch_add(1, Ordering::Relaxed);
                    self.shared.enqueued.fetch_add(1, Ordering::Relaxed);
                    drop(state);
                    self.shared.not_empty.notify_one();
                    return DispatchOutcome::DroppedOldest;
                }
            }
        }
    }

    /// Returns a lock-free snapshot of dispatcher counters.
    #[must_use]
    pub fn stats(&self) -> ObserverStats {
        self.shared.stats()
    }

    /// Stops accepting events, drains accepted events, and returns final stats.
    pub async fn shutdown(self) -> ObserverStats {
        {
            let mut state = self.shared.state.lock().await;
            state.closed = true;
        }
        self.shared.not_empty.notify_waiters();
        self.shared.not_full.notify_waiters();
        let worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker {
            let _ = worker.await;
        }
        self.shared.stats()
    }
}

impl<E> Drop for ObserverDispatcher<E> {
    fn drop(&mut self) {
        if let Ok(worker) = self.worker.get_mut()
            && let Some(worker) = worker.take()
        {
            worker.abort();
        }
    }
}

async fn observer_worker<E>(shared: Arc<SharedQueue<E>>, observer: Arc<dyn AsyncObserver<E>>)
where
    E: Send + 'static,
{
    loop {
        let event = loop {
            let notification = shared.not_empty.notified();
            let mut state = shared.state.lock().await;
            if let Some(event) = state.events.pop_front() {
                drop(state);
                shared.not_full.notify_one();
                break Some(event);
            }
            if state.closed {
                break None;
            }
            drop(state);
            notification.await;
        };
        let Some(event) = event else {
            return;
        };
        if observer.observe(event).await.is_ok() {
            shared.delivered.fetch_add(1, Ordering::Relaxed);
        } else {
            shared.failed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::{Notify, Semaphore};

    struct GatedObserver {
        started: Notify,
        permits: Semaphore,
        received: Mutex<Vec<u8>>,
    }

    impl GatedObserver {
        fn new() -> Self {
            Self {
                started: Notify::new(),
                permits: Semaphore::new(0),
                received: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl AsyncObserver<u8> for GatedObserver {
        async fn observe(&self, event: u8) -> Result<(), ObserverError> {
            self.started.notify_one();
            let permit = self
                .permits
                .acquire()
                .await
                .expect("test semaphore remains open");
            permit.forget();
            self.received.lock().await.push(event);
            Ok(())
        }
    }

    #[tokio::test]
    async fn drop_newest_preserves_queued_event() {
        let observer = Arc::new(GatedObserver::new());
        let dispatcher = ObserverDispatcher::new(
            Arc::clone(&observer) as Arc<dyn AsyncObserver<u8>>,
            1,
            BackpressurePolicy::DropNewest,
        )
        .expect("positive capacity");
        assert_eq!(dispatcher.dispatch(1).await, DispatchOutcome::Enqueued);
        observer.started.notified().await;
        assert_eq!(dispatcher.dispatch(2).await, DispatchOutcome::Enqueued);
        assert_eq!(dispatcher.dispatch(3).await, DispatchOutcome::DroppedNewest);
        observer.permits.add_permits(2);
        let stats = dispatcher.shutdown().await;
        assert_eq!(*observer.received.lock().await, [1, 2]);
        assert_eq!(stats.dropped_newest, 1);
        assert_eq!(stats.delivered, 2);
    }

    #[tokio::test]
    async fn drop_oldest_replaces_queued_event() {
        let observer = Arc::new(GatedObserver::new());
        let dispatcher = ObserverDispatcher::new(
            Arc::clone(&observer) as Arc<dyn AsyncObserver<u8>>,
            1,
            BackpressurePolicy::DropOldest,
        )
        .expect("positive capacity");
        assert_eq!(dispatcher.dispatch(1).await, DispatchOutcome::Enqueued);
        observer.started.notified().await;
        assert_eq!(dispatcher.dispatch(2).await, DispatchOutcome::Enqueued);
        assert_eq!(dispatcher.dispatch(3).await, DispatchOutcome::DroppedOldest);
        observer.permits.add_permits(2);
        let stats = dispatcher.shutdown().await;
        assert_eq!(*observer.received.lock().await, [1, 3]);
        assert_eq!(stats.dropped_oldest, 1);
        assert_eq!(stats.delivered, 2);
    }

    #[tokio::test]
    async fn block_policy_waits_for_capacity() {
        let observer = Arc::new(GatedObserver::new());
        let dispatcher = Arc::new(
            ObserverDispatcher::new(
                Arc::clone(&observer) as Arc<dyn AsyncObserver<u8>>,
                1,
                BackpressurePolicy::Block,
            )
            .expect("positive capacity"),
        );
        assert_eq!(dispatcher.dispatch(1).await, DispatchOutcome::Enqueued);
        observer.started.notified().await;
        assert_eq!(dispatcher.dispatch(2).await, DispatchOutcome::Enqueued);

        let dispatching = {
            let dispatcher = Arc::clone(&dispatcher);
            tokio::spawn(async move { dispatcher.dispatch(3).await })
        };
        tokio::task::yield_now().await;
        assert!(!dispatching.is_finished());
        observer.permits.add_permits(1);
        assert_eq!(
            dispatching.await.expect("dispatch task succeeds"),
            DispatchOutcome::Enqueued
        );
        observer.permits.add_permits(2);

        let dispatcher = Arc::try_unwrap(dispatcher)
            .unwrap_or_else(|_| panic!("dispatch task released its dispatcher reference"));
        let stats = dispatcher.shutdown().await;
        assert_eq!(*observer.received.lock().await, [1, 2, 3]);
        assert_eq!(stats.delivered, 3);
    }
}
