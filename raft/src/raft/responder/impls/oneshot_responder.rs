use std::marker::PhantomData;

use crate::OptionalSend;
use crate::RaftTypeConfig;
use crate::raft::responder::Responder;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::OneshotReceiverOf;
use crate::type_config::alias::OneshotSenderOf;

/// A [`Responder`] implementation that sends the response via a oneshot channel.
///
/// This could be used when the [`Raft::client_write`] caller wants to wait for the response.
///
/// [`Raft::client_write`]: `crate::raft::Raft::client_write`
pub struct OneshotResponder<C, T>
where
    C: RaftTypeConfig,
    T: OptionalSend + 'static,
{
    tx: OneshotSenderOf<C, T>,
    _p: PhantomData<C>,
}

impl<C, T> OneshotResponder<C, T>
where
    C: RaftTypeConfig,
    T: OptionalSend + 'static,
{
    /// Create a new instance from a [`AsyncRuntime::Oneshot::Sender`].
    pub fn new(tx: OneshotSenderOf<C, T>) -> Self {
        Self {
            tx,
            _p: PhantomData,
        }
    }

    /// Create a new responder and receiver pair.
    ///
    /// This is a convenience method that creates a oneshot channel and returns
    /// a [`OneshotResponder`] wrapping the sender and the receiver.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - The [`OneshotResponder`] that can be used to send a response
    /// - The receiver that can be used to wait for the response
    pub fn new_pair() -> (Self, OneshotReceiverOf<C, T>) {
        let (tx, rx) = C::oneshot();
        (Self::new(tx), rx)
    }
}

impl<C, T> Responder<C, T> for OneshotResponder<C, T>
where
    C: RaftTypeConfig,
    T: OptionalSend + 'static,
{
    fn on_complete(self, res: T) {
        self.tx.send(res);
        log::debug!("OneshotConsumer.tx.send: done");
    }
}
