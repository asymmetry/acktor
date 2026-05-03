use std::marker::PhantomData;

use pretty_assertions::{assert_eq, assert_ne};

use acktor::{Message, MessageId, Signal, StableId};

fn first_16(arr: [u8; 32]) -> [u8; 16] {
    [
        arr[0], arr[1], arr[2], arr[3], arr[4], arr[5], arr[6], arr[7], arr[8], arr[9], arr[10],
        arr[11], arr[12], arr[13], arr[14], arr[15],
    ]
}

#[derive(Message, MessageId)]
#[result_type(())]
struct Ping;

#[derive(Message, MessageId)]
#[result_type(())]
struct Pong;

#[derive(StableId)]
struct Wrap<T>(PhantomData<T>);

#[derive(Message, MessageId)]
#[result_type(())]
struct Bound<T>(PhantomData<T>)
where
    T: Send + 'static;

#[derive(Message, MessageId)]
#[result_type(())]
struct BoundDup<T1, T2>
where
    T1: Send + 'static,
    T2: Send + 'static,
{
    _1: PhantomData<T1>,
    _2: PhantomData<T2>,
}

#[derive(Message, MessageId)]
#[result_type(())]
struct BoundLifetime<'a>(PhantomData<&'a ()>)
where
    'a: 'static;

#[derive(StableId)]
struct A;

#[derive(StableId)]
struct B;

#[derive(StableId)]
struct Usize<const N: usize>;

#[derive(StableId)]
struct I32<const I: i32>;

#[derive(StableId)]
struct Bool<const F: bool>;

#[derive(StableId)]
struct Char<const C: char>;

#[derive(StableId)]
struct Mixed<T, const N: usize>(PhantomData<T>)
where
    T: Send + 'static;

mod inner_a {
    #[derive(acktor::StableId)]
    pub struct SameName;
}

mod inner_b {
    #[derive(acktor::StableId)]
    pub struct SameName;
}

#[test]
fn test_no_generics() {
    assert_ne!(Ping::TYPE_ID, Pong::TYPE_ID);
    assert_ne!(Ping::ID, Pong::ID);
    assert_eq!(Ping::ID, Ping::TYPE_ID.as_u64());

    // stable type name: `module_path!() + "::" + ident` as bytes.
    let expected = first_16(
        sha2_const::Sha256::new()
            .update(b"test_identifier")
            .update(b"::")
            .update(b"Ping")
            .finalize(),
    );
    assert_eq!(Ping::TYPE_ID.as_bytes(), &expected);
}

#[test]
fn test_type_generics() {
    assert_ne!(<Wrap<A>>::TYPE_ID, <Wrap<B>>::TYPE_ID);

    let wrap = first_16(
        sha2_const::Sha256::new()
            .update(b"test_identifier")
            .update(b"::")
            .update(b"Wrap")
            .finalize(),
    );
    let a = first_16(
        sha2_const::Sha256::new()
            .update(b"test_identifier")
            .update(b"::")
            .update(b"A")
            .finalize(),
    );
    let expected = first_16(
        sha2_const::Sha256::new()
            .update(&wrap)
            .update(&a)
            .finalize(),
    );
    assert_eq!(<Wrap<A>>::TYPE_ID.as_bytes(), &expected);

    assert!(<Bound<A>>::TYPE_ID.as_u64() != 0);
    assert!(<BoundDup<A, B>>::TYPE_ID.as_u64() != 0);
    assert!(<BoundLifetime<'_>>::TYPE_ID.as_u64() != 0);
}

#[test]
fn test_const_generics() {
    assert_ne!(<Usize<3>>::TYPE_ID, <Usize<4>>::TYPE_ID);

    let base = first_16(
        sha2_const::Sha256::new()
            .update(b"test_identifier")
            .update(b"::")
            .update(b"Usize")
            .finalize(),
    );
    let seven = first_16(
        sha2_const::Sha256::new()
            .update(&7u64.to_be_bytes())
            .finalize(),
    );
    let expected = first_16(
        sha2_const::Sha256::new()
            .update(&base)
            .update(&seven)
            .finalize(),
    );
    assert_eq!(<Usize<7>>::TYPE_ID.as_bytes(), &expected);

    assert_ne!(<Bool<true>>::TYPE_ID, <Bool<false>>::TYPE_ID);
    assert_ne!(<Char<'a'>>::TYPE_ID, <Char<'b'>>::TYPE_ID);
    assert_ne!(<I32<-1>>::TYPE_ID, <I32<1>>::TYPE_ID);
}

#[test]
fn test_mixed_generics() {
    assert_ne!(<Mixed<A, 3>>::TYPE_ID, <Mixed<B, 3>>::TYPE_ID);
    assert_ne!(<Mixed<A, 3>>::TYPE_ID, <Mixed<A, 4>>::TYPE_ID);

    let mixed = first_16(
        sha2_const::Sha256::new()
            .update(b"test_identifier")
            .update(b"::")
            .update(b"Mixed")
            .finalize(),
    );
    let a = first_16(
        sha2_const::Sha256::new()
            .update(b"test_identifier")
            .update(b"::")
            .update(b"A")
            .finalize(),
    );
    let seven = first_16(
        sha2_const::Sha256::new()
            .update(&7u64.to_be_bytes())
            .finalize(),
    );
    let mixed_a = first_16(
        sha2_const::Sha256::new()
            .update(&mixed)
            .update(&a)
            .finalize(),
    );
    let expected = first_16(
        sha2_const::Sha256::new()
            .update(&mixed_a)
            .update(&seven)
            .finalize(),
    );
    assert_eq!(<Mixed<A, 7>>::TYPE_ID.as_bytes(), &expected);
}

#[test]
fn test_stable_type_name() {
    // same type name, different modules
    assert_ne!(inner_a::SameName::TYPE_ID, inner_b::SameName::TYPE_ID,);

    // use statement should not affect the stable type id
    let expected = first_16(
        sha2_const::Sha256::new()
            .update(b"acktor::signal") // module_path!()
            .update(b"::")
            .update(b"Signal") // ident
            .finalize(),
    );
    assert_eq!(Signal::TYPE_ID.as_bytes(), &expected);
}
