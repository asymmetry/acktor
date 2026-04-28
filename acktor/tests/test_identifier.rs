use std::marker::PhantomData;

use pretty_assertions::{assert_eq, assert_ne};

use acktor::{HasStableTypeId, Message, MessageId, Signal};

#[derive(Message, MessageId)]
#[result_type(())]
struct Ping;

#[derive(Message, MessageId)]
#[result_type(())]
struct Pong;

#[derive(HasStableTypeId)]
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

#[derive(HasStableTypeId)]
struct A;

#[derive(HasStableTypeId)]
struct B;

#[derive(HasStableTypeId)]
struct Usize<const N: usize>;

#[derive(HasStableTypeId)]
struct I32<const I: i32>;

#[derive(HasStableTypeId)]
struct Bool<const F: bool>;

#[derive(HasStableTypeId)]
struct Char<const C: char>;

#[derive(HasStableTypeId)]
struct Mixed<T, const N: usize>(PhantomData<T>)
where
    T: Send + 'static;

mod inner_a {
    #[derive(acktor::HasStableTypeId)]
    pub struct SameName;
}

mod inner_b {
    #[derive(acktor::HasStableTypeId)]
    pub struct SameName;
}

#[test]
fn test_no_generics() {
    assert_ne!(Ping::STABLE_TYPE_ID, Pong::STABLE_TYPE_ID);
    assert_ne!(Ping::ID, Pong::ID);
    assert_eq!(Ping::ID, Ping::STABLE_TYPE_ID.as_u64());

    // stable type name: `module_path!() + "::" + ident` as bytes.
    let expected = sha2_const::Sha256::new()
        .update(b"test_identifier")
        .update(b"::")
        .update(b"Ping")
        .finalize();
    assert_eq!(Ping::STABLE_TYPE_ID.as_bytes(), &expected);
}

#[test]
fn test_type_generics() {
    assert_ne!(<Wrap<A>>::STABLE_TYPE_ID, <Wrap<B>>::STABLE_TYPE_ID);

    let base = sha2_const::Sha256::new()
        .update(b"test_identifier")
        .update(b"::")
        .update(b"Wrap")
        .finalize();
    let a = sha2_const::Sha256::new()
        .update(b"test_identifier")
        .update(b"::")
        .update(b"A")
        .finalize();
    let expected = sha2_const::Sha256::new()
        .update(&base)
        .update(&a)
        .finalize();
    assert_eq!(<Wrap<A>>::STABLE_TYPE_ID.as_bytes(), &expected);

    assert!(<Bound<A>>::STABLE_TYPE_ID.as_u64() != 0);
    assert!(<BoundDup<A, B>>::STABLE_TYPE_ID.as_u64() != 0);
    assert!(<BoundLifetime<'_>>::STABLE_TYPE_ID.as_u64() != 0);
}

#[test]
fn test_const_generics() {
    assert_ne!(<Usize<3>>::STABLE_TYPE_ID, <Usize<4>>::STABLE_TYPE_ID);

    let base: [u8; 32] = sha2_const::Sha256::new()
        .update(b"test_identifier")
        .update(b"::")
        .update(b"Usize")
        .finalize();
    let seven: [u8; 32] = sha2_const::Sha256::new()
        .update(&7u64.to_le_bytes())
        .finalize();
    let expected: [u8; 32] = sha2_const::Sha256::new()
        .update(&base)
        .update(&seven)
        .finalize();
    assert_eq!(<Usize<7>>::STABLE_TYPE_ID.as_bytes(), &expected);

    assert_ne!(<Bool<true>>::STABLE_TYPE_ID, <Bool<false>>::STABLE_TYPE_ID);
    assert_ne!(<Char<'a'>>::STABLE_TYPE_ID, <Char<'b'>>::STABLE_TYPE_ID);
    assert_ne!(<I32<-1>>::STABLE_TYPE_ID, <I32<1>>::STABLE_TYPE_ID);
}

#[test]
fn test_mixed_generics() {
    assert_ne!(<Mixed<A, 3>>::STABLE_TYPE_ID, <Mixed<B, 3>>::STABLE_TYPE_ID);
    assert_ne!(<Mixed<A, 3>>::STABLE_TYPE_ID, <Mixed<A, 4>>::STABLE_TYPE_ID);

    let base: [u8; 32] = sha2_const::Sha256::new()
        .update(b"test_identifier")
        .update(b"::")
        .update(b"Mixed")
        .finalize();
    let a: [u8; 32] = sha2_const::Sha256::new()
        .update(b"test_identifier")
        .update(b"::")
        .update(b"A")
        .finalize();
    let seven: [u8; 32] = sha2_const::Sha256::new()
        .update(&7u64.to_le_bytes())
        .finalize();
    let base_a: [u8; 32] = sha2_const::Sha256::new()
        .update(&base)
        .update(&a)
        .finalize();
    let expected: [u8; 32] = sha2_const::Sha256::new()
        .update(&base_a)
        .update(&seven)
        .finalize();
    assert_eq!(<Mixed<A, 7>>::STABLE_TYPE_ID.as_bytes(), &expected);
}

#[test]
fn test_stable_type_name() {
    // same type name, different modules
    assert_ne!(
        inner_a::SameName::STABLE_TYPE_ID,
        inner_b::SameName::STABLE_TYPE_ID,
    );

    // use statement should not affect the stable type id
    let expected = sha2_const::Sha256::new()
        .update(b"acktor::signal") // module_path!()
        .update(b"::")
        .update(b"Signal") // ident
        .finalize();
    assert_eq!(Signal::STABLE_TYPE_ID.as_bytes(), &expected);
}
