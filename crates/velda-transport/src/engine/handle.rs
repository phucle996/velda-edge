//! Engine handle for submitting declarative reconciliations.

use tokio::sync::mpsc;

use crate::error::{Result, TransportError};
use crate::ingress::listener::IngressBinding;

/// Controller handle allowing supervisors to submit declarative listener reconciliations
/// to the running [`super::runner::TrafficEngine`].
#[derive(Clone, Debug)]
pub struct EngineHandle {
    pub(crate) reconcile_tx: mpsc::Sender<Vec<IngressBinding>>,
}

impl EngineHandle {
    /// Submits a declarative list of desired ingress bindings to the running traffic engine.
    ///
    /// The engine automatically compares desired bindings against live sockets, binding new
    /// ports, gracefully closing removed ports, and leaving identical listeners untouched.
    pub async fn reconcile(&self, desired: Vec<IngressBinding>) -> Result<()> {
        self.reconcile_tx.send(desired).await.map_err(|_| {
            TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "TrafficEngine has terminated",
            ))
        })
    }
}
