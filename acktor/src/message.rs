//! Message passing between actors.
//!
//! A [`Message`] is a type that can be sent between actors. It is a trait that can be derived
//! for custom types. A specific message type can be sent to an actor only if the actor implements
//! the corresponding [`Handler`] trait for the message type, which describes the action of the
//! actor when it receives the message.
//!

use std::future::{self, Future};
use std::sync::Arc;

use tracing::debug;

use crate::actor::Actor;
use crate::channel::oneshot;
use crate::errors::{BoxError, ErrorReport, SendError};

mod result;
pub use result::MessageResult;

mod future_result;
pub use future_result::FutureMessageResult;

#[cfg(feature = "identifier")]
mod index;
#[cfg(feature = "identifier")]
#[cfg_attr(docsrs, doc(cfg(feature = "identifier")))]
pub use index::MessageId;

/// Types that can be sent between actors.
pub trait Message: Send + 'static {
    /// The type of the response produced when this message is handled.
    type Result: Send + 'static;
}

/// Types that can be sent as a response to a message.
pub trait MessageResponse<A, M>: Send
where
    A: Actor,
    M: Message,
{
    /// Handles the response.
    fn handle(
        self,
        ctx: &mut A::Context,
        tx: Option<oneshot::Sender<M::Result>>,
    ) -> impl Future<Output = ()> + Send;
}

/// Describes how an actor handles a specific message type.
pub trait Handler<M>: Actor
where
    M: Message,
{
    /// The return type of the handler, which must implement [`MessageResponse`].
    type Result: MessageResponse<Self, M>;

    /// Handles a message.
    fn handle(
        &mut self,
        msg: M,
        ctx: &mut Self::Context,
    ) -> impl Future<Output = Self::Result> + Send;
}

impl Message for () {
    type Result = ();
}

// implement Message trait for a few common wrapper types

impl<M> Message for Box<M>
where
    M: Message,
{
    type Result = M::Result;
}

impl<M> Message for Arc<M>
where
    M: Message + Sync,
{
    type Result = M::Result;
}

// implement MessageResponse trait for a few common wrapper types

impl<A, M, T, E> MessageResponse<A, M> for Result<T, E>
where
    A: Actor,
    M: Message<Result = Self>,
    T: Send,
    E: Into<BoxError> + Send,
{
    fn handle(
        self,
        _ctx: &mut A::Context,
        tx: Option<oneshot::Sender<M::Result>>,
    ) -> impl Future<Output = ()> + Send {
        if let Some(tx) = tx {
            if let Err(SendError::Closed(Err(e))) = tx.send(self) {
                debug!(
                    "Could not send the result back to the sender since the channel is closed, \
                    log the dropped error: {}",
                    e.into().report()
                );
            }
        }
        // tx is None means the sender does not care about the result, so we simply drop it
        future::ready(())
    }
}

impl<A, M, T> MessageResponse<A, M> for Option<T>
where
    A: Actor,
    M: Message<Result = Self>,
    T: Send,
{
    fn handle(
        self,
        _ctx: &mut A::Context,
        tx: Option<oneshot::Sender<M::Result>>,
    ) -> impl Future<Output = ()> + Send {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
        future::ready(())
    }
}

impl<A, M, T> MessageResponse<A, M> for Box<T>
where
    A: Actor,
    M: Message<Result = Self>,
    T: Send,
{
    fn handle(
        self,
        _ctx: &mut A::Context,
        tx: Option<oneshot::Sender<M::Result>>,
    ) -> impl Future<Output = ()> + Send {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
        future::ready(())
    }
}

impl<A, M, T> MessageResponse<A, M> for Arc<T>
where
    A: Actor,
    M: Message<Result = Self>,
    T: Send + Sync,
{
    fn handle(
        self,
        _ctx: &mut A::Context,
        tx: Option<oneshot::Sender<M::Result>>,
    ) -> impl Future<Output = ()> + Send {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
        future::ready(())
    }
}

impl<A, M, T> MessageResponse<A, M> for Vec<T>
where
    A: Actor,
    M: Message<Result = Self>,
    T: Send,
{
    fn handle(
        self,
        _ctx: &mut A::Context,
        tx: Option<oneshot::Sender<M::Result>>,
    ) -> impl Future<Output = ()> + Send {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
        future::ready(())
    }
}

macro_rules! impl_message_response_for {
    ($type:ty) => {
        impl<A, M> MessageResponse<A, M> for $type
        where
            A: Actor,
            M: Message<Result = Self>,
        {
            fn handle(
                self,
                _ctx: &mut A::Context,
                tx: Option<oneshot::Sender<M::Result>>,
            ) -> impl Future<Output = ()> + Send {
                if let Some(tx) = tx {
                    let _ = tx.send(self);
                }
                future::ready(())
            }
        }
    };
}

impl_message_response_for!(());
impl_message_response_for!(bool);
impl_message_response_for!(i8);
impl_message_response_for!(i16);
impl_message_response_for!(i32);
impl_message_response_for!(i64);
impl_message_response_for!(isize);
impl_message_response_for!(u8);
impl_message_response_for!(u16);
impl_message_response_for!(u32);
impl_message_response_for!(u64);
impl_message_response_for!(usize);
impl_message_response_for!(f32);
impl_message_response_for!(f64);
impl_message_response_for!(char);
impl_message_response_for!(String);

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use anyhow::Result;
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::context::Context;

    #[derive(Debug)]
    struct A;

    impl Actor for A {
        type Context = Context<Self>;
        type Error = anyhow::Error;
    }

    /// Synthetic message whose `Result` is `T`. Used to drive `MessageResponse` tests
    /// without coupling to a concrete message type per scalar.
    struct M<T>(PhantomData<T>);

    impl<T> Message for M<T>
    where
        T: Send + 'static,
    {
        type Result = T;
    }

    async fn roundtrip<R>(value: R) -> Result<R>
    where
        R: MessageResponse<A, M<R>> + Send + 'static,
    {
        let mut ctx = Context::<A>::with_capacity("test".into(), 1);
        let (tx, rx) = oneshot::channel::<R>();
        value.handle(&mut ctx, Some(tx)).await;
        Ok(rx.await?)
    }

    #[tokio::test]
    async fn test_message_response() -> Result<()> {
        assert_eq!(roundtrip(()).await?, ());
        assert_eq!(roundtrip(true).await?, true);
        assert_eq!(roundtrip(-1_i32).await?, -1);
        assert_eq!(roundtrip(42_u64).await?, 42);
        assert_eq!(roundtrip('x').await?, 'x');
        assert_eq!(roundtrip(String::from("hello")).await?, "hello");

        // result
        assert_eq!(
            roundtrip::<std::result::Result<i32, String>>(Ok(10)).await?,
            Ok(10)
        );
        assert_eq!(
            roundtrip::<std::result::Result<i32, String>>(Err(String::from("err"))).await?,
            Err(String::from("err"))
        );

        // option
        assert_eq!(roundtrip(Some(42_u32)).await?, Some(42));
        assert_eq!(roundtrip(None::<u32>).await?, None);

        // box and arc
        assert_eq!(*roundtrip(Box::new(7_u64)).await?, 7);
        assert_eq!(*roundtrip(Arc::new(String::from("hi"))).await?, "hi");

        // vec
        assert_eq!(roundtrip(vec![1_u8, 2, 3]).await?, vec![1, 2, 3]);

        Ok(())
    }

    #[test]
    fn test_debug_fmt() {
        let result = MessageResult::<M<i32>>(42);
        assert_eq!(format!("{result:?}"), "MessageResult<M<i32>>");

        let future_result = FutureMessageResult::<M<i32>>::new(async { 42 });
        assert_eq!(format!("{future_result:?}"), "FutureMessageResult<M<i32>>");
    }
}
