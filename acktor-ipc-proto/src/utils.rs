use bytes::Bytes;

pub use crate::proto::utils::actor_ref::Ref as ActorRefType;
pub use crate::proto::utils::result::Result as ProtoResultType;
pub use crate::proto::utils::result_address::Result as ResultAddressType;
pub use crate::proto::utils::{
    ActorRef, Option as ProtoOption, Result as ProtoResult, ResultAddress, Tuple, VecBool,
    VecDouble, VecFloat, VecInt32, VecInt64, VecUint32, VecUint64,
};

impl ProtoResult {
    #[inline]
    pub fn ok(ok: Bytes) -> Self {
        Self {
            result: Some(ProtoResultType::Ok(ok)),
        }
    }

    #[inline]
    pub fn err(err: String) -> Self {
        Self {
            result: Some(ProtoResultType::Err(err)),
        }
    }
}

impl ProtoOption {
    #[inline]
    pub fn some(some: Bytes) -> Self {
        Self { option: Some(some) }
    }

    #[inline]
    pub fn none() -> Self {
        Self { option: None }
    }
}

macro_rules! impl_vec_new {
    ($msg:ident, $type:ty) => {
        impl $msg {
            #[inline]
            pub fn new(values: Vec<$type>) -> Self {
                Self { values }
            }
        }
    };
}

impl_vec_new!(VecBool, bool);
impl_vec_new!(VecInt32, i32);
impl_vec_new!(VecInt64, i64);
impl_vec_new!(VecUint32, u32);
impl_vec_new!(VecUint64, u64);
impl_vec_new!(VecFloat, f32);
impl_vec_new!(VecDouble, f64);

macro_rules! impl_tuple_ctor {
    ($name:ident, [$($field:ident),+], [$($none:ident),*]) => {
        #[inline]
        #[allow(clippy::too_many_arguments)]
        pub fn $name($($field: Bytes),+) -> Self {
            Self {
                $($field: Some($field),)+
                $($none: None,)*
            }
        }
    };
}

impl Tuple {
    impl_tuple_ctor!(tuple2, [t0, t1], [t2, t3, t4, t5, t6, t7, t8, t9]);
    impl_tuple_ctor!(tuple3, [t0, t1, t2], [t3, t4, t5, t6, t7, t8, t9]);
    impl_tuple_ctor!(tuple4, [t0, t1, t2, t3], [t4, t5, t6, t7, t8, t9]);
    impl_tuple_ctor!(tuple5, [t0, t1, t2, t3, t4], [t5, t6, t7, t8, t9]);
    impl_tuple_ctor!(tuple6, [t0, t1, t2, t3, t4, t5], [t6, t7, t8, t9]);
    impl_tuple_ctor!(tuple7, [t0, t1, t2, t3, t4, t5, t6], [t7, t8, t9]);
    impl_tuple_ctor!(tuple8, [t0, t1, t2, t3, t4, t5, t6, t7], [t8, t9]);
    impl_tuple_ctor!(tuple9, [t0, t1, t2, t3, t4, t5, t6, t7, t8], [t9]);
    impl_tuple_ctor!(tuple10, [t0, t1, t2, t3, t4, t5, t6, t7, t8, t9], []);
}

impl ActorRef {
    #[inline]
    pub fn index(actor_id: u64) -> Self {
        Self {
            r#ref: Some(ActorRefType::Index(actor_id)),
        }
    }

    #[inline]
    pub fn label(label: String) -> Self {
        Self {
            r#ref: Some(ActorRefType::Label(label)),
        }
    }
}

impl ResultAddress {
    #[inline]
    pub fn ok(actor_id: u64) -> Self {
        Self {
            result: Some(ResultAddressType::Ok(actor_id)),
        }
    }

    #[inline]
    pub fn err(err: String) -> Self {
        Self {
            result: Some(ResultAddressType::Err(err)),
        }
    }
}
