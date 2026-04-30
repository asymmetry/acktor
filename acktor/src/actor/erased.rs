use std::any::{Any, type_name};

use super::Actor;
use crate::address::{Address, Sender, Recipient};
use crate::message::Message;
use crate::utils::ShortName;

/// Return type of [`AddressToTypeErasedRecipientFn`].
///
/// It wraps a type-erased trait object with its original type name for better error messages when
/// downcasting fails.
#[cfg_attr(docsrs, doc(cfg(feature = "type-erased-recipient-hook")))]
pub struct TypeErasedRecipient {
    inner: Box<dyn Any + Send + Sync>,
    type_name: &'static str,
}

impl TypeErasedRecipient {
    /// Constructs a new [`TypeErasedRecipient`] from a [`Recipient`].
    pub fn new<M>(recipient: Recipient<M>) -> Self
    where
        M: Message,
    {
        Self {
            inner: Box::new(recipient),
            type_name: type_name::<Recipient<M>>(),
        }
    }

    /// Attempts to downcast the [`TypeErasedRecipient`] to a concrete [`Recipient`].
    pub fn downcast<M>(self) -> Result<Box<Recipient<M>>, (Self, String)>
    where
        M: Message,
    {
        self.inner.downcast::<Recipient<M>>().map_err(|inner| {
            let error_msg = format!(
                "Could not downcast TypeErasedRecipient: expected type {}, actual type {}",
                crate::utils::ShortName::of::<Recipient<M>>(),
                crate::utils::ShortName(self.type_name),
            );
            (
                Self {
                    inner,
                    type_name: self.type_name,
                },
                error_msg,
            )
        })
    }
}

/// Function-pointer type returned by
/// [`Actor::type_erased_recipient_hook`][super::Actor::type_erased_recipient_hook],
/// which converts an [`Address`] into a [`TypeErasedRecipient`] that can be downcast to a
/// concrete [`Recipient`].
#[cfg_attr(docsrs, doc(cfg(feature = "type-erased-recipient-hook")))]
pub type AddressToTypeErasedRecipientFn<A> = fn(&Address<A>) -> TypeErasedRecipient;

/// Types which can be converted into a [`TypeErasedRecipient`].
pub trait ToTypeErasedRecipient {
    fn to_type_erased_recipient(&self) -> Result<TypeErasedRecipient, String>;
}

impl<A> ToTypeErasedRecipient for Address<A>
where
    A: Actor,
{
    fn to_type_erased_recipient(&self) -> Result<TypeErasedRecipient, String> {
        self.type_erased_recipient().ok_or_else(|| {
            format!(
                "{} could not be converted to a `TypeErasedRecipient`",
                ShortName::of::<Self>(),
            )
        })
    }
}

impl<M> ToTypeErasedRecipient for Recipient<M>
where
    M: Message,
{
    fn to_type_erased_recipient(&self) -> Result<TypeErasedRecipient, String> {
        self.type_erased_recipient().ok_or_else(|| {
            format!(
                "{} could not be converted to a `TypeErasedRecipient`",
                ShortName::of::<Self>(),
            )
        })
    }
}
