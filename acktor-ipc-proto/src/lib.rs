#![cfg_attr(docsrs, feature(doc_cfg))]

mod proto;

pub mod control_message;
pub mod message;
pub mod utils;

#[cfg(feature = "adaptor")]
#[cfg_attr(docsrs, doc(cfg(feature = "adaptor")))]
pub mod adaptor;

#[inline]
pub fn length_delimited_field_encoded_len(tag: u32, value_len: usize) -> usize {
    if value_len == 0 {
        0
    } else {
        prost::encoding::key_len(tag) + prost::length_delimiter_len(value_len) + value_len
    }
}

#[inline]
pub fn optional_length_delimited_field_encoded_len(tag: u32, value_len: usize) -> usize {
    prost::encoding::key_len(tag) + prost::length_delimiter_len(value_len) + value_len
}

#[inline]
pub fn nested_message_encoded_len(tag: u32, value_len: usize) -> usize {
    prost::encoding::key_len(tag) + prost::length_delimiter_len(value_len) + value_len
}
