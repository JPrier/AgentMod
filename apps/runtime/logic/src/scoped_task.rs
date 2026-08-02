//! Cancellation-safe fresh Tokio task execution.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::task::{JoinError, JoinHandle};

/// Spawns a future on a fresh Tokio task without allowing it to detach from
/// the caller that awaits it.
///
/// This constructor is deliberately synchronous: the returned future stores
/// only a [`JoinHandle`], never the potentially deep child future. Dropping it
/// aborts pending child work while preserving the fresh worker-stack boundary.
pub(crate) fn scoped_task<F, T>(future: F) -> ScopedTask<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    ScopedTask {
        handle: tokio::spawn(future),
        completed: false,
    }
}

/// A fresh task that aborts pending work when its awaiting owner is dropped.
pub(crate) struct ScopedTask<T> {
    handle: JoinHandle<T>,
    completed: bool,
}

impl<T> Future for ScopedTask<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let task = self.get_mut();
        match Pin::new(&mut task.handle).poll(context) {
            Poll::Ready(result) => {
                task.completed = true;
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Drop for ScopedTask<T> {
    fn drop(&mut self) {
        if !self.completed {
            self.handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::mem::size_of_val;
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::scoped_task;

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn caller_cancellation_aborts_pending_child_task() {
        let (started_sender, started_receiver) = oneshot::channel();
        let (dropped_sender, dropped_receiver) = oneshot::channel();
        let caller = tokio::spawn(async move {
            scoped_task(async move {
                let _drop_signal = DropSignal(Some(dropped_sender));
                let _ = started_sender.send(());
                pending::<()>().await;
            })
            .await
        });

        started_receiver.await.expect("child task started");
        caller.abort();
        assert!(
            caller
                .await
                .expect_err("caller task must be cancelled")
                .is_cancelled()
        );
        timeout(Duration::from_secs(1), dropped_receiver)
            .await
            .expect("child task must be aborted promptly")
            .expect("child task drop signal");
    }

    #[tokio::test]
    #[allow(
        clippy::large_stack_arrays,
        reason = "the regression deliberately models the oversized child future that the scoped wrapper must not retain"
    )]
    async fn awaiting_frame_does_not_retain_the_child_future() {
        let payload = [0_u8; 64 * 1024];
        let child = async move { payload.len() };
        assert!(size_of_val(&child) >= 64 * 1024);

        let task = scoped_task(child);
        assert!(size_of_val(&task) < 256);
        assert_eq!(task.await.expect("child task"), 64 * 1024);
    }

    #[tokio::test]
    async fn child_panic_uses_the_supplied_stable_error_mapping() {
        let error = scoped_task(async {
            panic!("scoped child panic");
        })
        .await
        .map_err(|error| {
            if error.is_panic() {
                "task_panicked"
            } else {
                "task_cancelled"
            }
        })
        .expect_err("panic must be mapped");

        assert_eq!(error, "task_panicked");
    }
}
