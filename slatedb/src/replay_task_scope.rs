use std::fmt;
use std::future::Future;

use tokio::task::JoinHandle;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

/// Owns every spawned task for one reader replay operation.
///
/// Cancellation stops the operation. `shutdown` additionally waits until all
/// tracked descendants have exited, which is the boundary required before a
/// reader may report that its database runtime is closed.
#[derive(Clone)]
pub(crate) struct ReplayTaskScope {
    cancellation: CancellationToken,
    tasks: TaskTracker,
}

impl ReplayTaskScope {
    pub(crate) fn new() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            tasks: TaskTracker::new(),
        }
    }

    pub(crate) fn child(&self) -> Self {
        Self {
            cancellation: self.cancellation.child_token(),
            tasks: TaskTracker::new(),
        }
    }

    pub(crate) fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub(crate) async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }

    pub(crate) fn spawn<F>(&self, task: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tasks.spawn(task)
    }

    pub(crate) async fn shutdown(&self) {
        self.cancel();
        self.tasks.close();
        self.tasks.wait().await;
    }

    #[cfg(test)]
    pub(crate) fn active_tasks(&self) -> usize {
        self.tasks.len()
    }
}

impl fmt::Debug for ReplayTaskScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplayTaskScope")
            .field("cancelled", &self.cancellation.is_cancelled())
            .field("active_tasks", &self.tasks.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::ReplayTaskScope;

    #[tokio::test]
    async fn shutdown_waits_until_tracked_tasks_exit() {
        let scope = ReplayTaskScope::new();
        let cancellation = scope.clone();
        let task = scope.spawn(async move {
            cancellation.cancelled().await;
        });
        assert_eq!(scope.active_tasks(), 1);

        scope.shutdown().await;

        task.await.unwrap();
        assert_eq!(scope.active_tasks(), 0);
    }
}
