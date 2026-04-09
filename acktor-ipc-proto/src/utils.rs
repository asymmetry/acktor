use bytes::Bytes;

pub use crate::proto::utils::result_message::Result as ResultType;
pub use crate::proto::utils::{
    BoolVec, DoubleVec, FloatVec, Int32Vec, Int64Vec, OptionMessage, ResultMessage, TupleMessage,
    Uint32Vec, Uint64Vec,
};

impl ResultMessage {
    #[inline]
    pub fn ok(ok: Bytes) -> Self {
        Self {
            result: Some(ResultType::Ok(ok)),
        }
    }

    #[inline]
    pub fn err(err: String) -> Self {
        Self {
            result: Some(ResultType::Err(err)),
        }
    }
}

impl OptionMessage {
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

impl_vec_new!(BoolVec, bool);
impl_vec_new!(Int32Vec, i32);
impl_vec_new!(Int64Vec, i64);
impl_vec_new!(Uint32Vec, u32);
impl_vec_new!(Uint64Vec, u64);
impl_vec_new!(FloatVec, f32);
impl_vec_new!(DoubleVec, f64);

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

impl TupleMessage {
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
