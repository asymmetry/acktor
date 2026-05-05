use anyhow::Result;
use pretty_assertions::assert_eq;
use rkyv::{Archive, Deserialize, Serialize};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use acktor::codec::{Decode, DecodeError, Encode};

#[derive(Debug, PartialEq, Encode, Decode)]
#[codec(prost, ProstBridge)]
struct Prost {
    string: String,
    number: u64,
}

#[derive(PartialEq, prost::Message, Encode, Decode)]
#[codec(prost)]
struct ProstBridge {
    #[prost(string, tag = "1")]
    string: prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    number: u64,
}

impl From<&Prost> for ProstBridge {
    fn from(value: &Prost) -> Self {
        ProstBridge {
            string: value.string.clone(),
            number: value.number,
        }
    }
}

impl TryFrom<ProstBridge> for Prost {
    type Error = DecodeError;

    fn try_from(value: ProstBridge) -> Result<Self, Self::Error> {
        Ok(Prost {
            string: value.string,
            number: value.number,
        })
    }
}

#[derive(Debug, PartialEq, Encode, Decode)]
#[codec(zerocopy, ZerocopyBridge)]
struct Zerocopy {
    number: u64,
    flags: u8,
}

#[derive(Debug, PartialEq, KnownLayout, Immutable, FromBytes, IntoBytes, Encode, Decode)]
#[codec(zerocopy)]
#[repr(C)]
struct ZerocopyBridge {
    number: u64,
    padding: [u8; 7],
    flags: u8,
}

impl From<&Zerocopy> for ZerocopyBridge {
    fn from(value: &Zerocopy) -> Self {
        ZerocopyBridge {
            number: value.number,
            padding: [0u8; 7],
            flags: value.flags,
        }
    }
}

impl TryFrom<ZerocopyBridge> for Zerocopy {
    type Error = DecodeError;

    fn try_from(value: ZerocopyBridge) -> Result<Self, Self::Error> {
        Ok(Zerocopy {
            number: value.number,
            flags: value.flags,
        })
    }
}

#[derive(Debug, PartialEq, Encode, Decode)]
#[codec(rkyv, RkyvBridge)]
struct Rkyv {
    number: u64,
    string: String,
    option: Option<Vec<u32>>,
}

#[derive(Debug, PartialEq, Archive, Deserialize, Serialize, Encode, Decode)]
#[codec(rkyv)]
struct RkyvBridge {
    number: u64,
    string: String,
    option: Option<Vec<u32>>,
}

impl From<&Rkyv> for RkyvBridge {
    fn from(value: &Rkyv) -> Self {
        RkyvBridge {
            number: value.number,
            string: value.string.clone(),
            option: value.option.clone(),
        }
    }
}

impl TryFrom<RkyvBridge> for Rkyv {
    type Error = DecodeError;

    fn try_from(value: RkyvBridge) -> Result<Self, Self::Error> {
        Ok(Rkyv {
            number: value.number,
            string: value.string,
            option: value.option,
        })
    }
}

#[test]
fn test_codec_prost() -> Result<()> {
    let value = Prost {
        string: "hello".to_owned(),
        number: 42,
    };

    let bridge_value = ProstBridge::from(&value);

    let value_bytes = Encode::encode_to_bytes(&value, None)?;
    let bridge_value_bytes = Encode::encode_to_bytes(&bridge_value, None)?;

    assert_eq!(value_bytes, bridge_value_bytes);

    let value_decoded = <Prost as Decode>::decode(value_bytes, None)?;
    let bridge_value_decoded = <ProstBridge as Decode>::decode(bridge_value_bytes, None)?;

    assert_eq!(value, value_decoded);
    assert_eq!(bridge_value, bridge_value_decoded);

    Ok(())
}

#[test]
fn test_codec_zerocopy() -> Result<()> {
    let value = Zerocopy {
        number: 42,
        flags: 7,
    };

    let bridge_value = ZerocopyBridge::from(&value);

    let value_bytes = Encode::encode_to_bytes(&value, None)?;
    let bridge_value_bytes = Encode::encode_to_bytes(&bridge_value, None)?;

    assert_eq!(value_bytes, bridge_value_bytes);

    let value_decoded = <Zerocopy as Decode>::decode(value_bytes, None)?;
    let bridge_value_decoded = <ZerocopyBridge as Decode>::decode(bridge_value_bytes, None)?;

    assert_eq!(value, value_decoded);
    assert_eq!(bridge_value, bridge_value_decoded);

    Ok(())
}

#[test]
fn test_codec_rkyv() -> Result<()> {
    let value = Rkyv {
        number: 42,
        string: "hello".to_owned(),
        option: Some(vec![1, 2, 3]),
    };

    let bridge_value = RkyvBridge::from(&value);

    let value_bytes = Encode::encode_to_bytes(&value, None)?;
    let bridge_value_bytes = Encode::encode_to_bytes(&bridge_value, None)?;

    assert_eq!(value_bytes, bridge_value_bytes);

    let value_decoded = <Rkyv as Decode>::decode(value_bytes, None)?;
    let bridge_value_decoded = <RkyvBridge as Decode>::decode(bridge_value_bytes, None)?;

    assert_eq!(value, value_decoded);
    assert_eq!(bridge_value, bridge_value_decoded);

    Ok(())
}
