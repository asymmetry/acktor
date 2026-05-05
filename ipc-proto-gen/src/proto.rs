#![allow(unused_imports)]
#![allow(dead_code)]

pub mod control_message {
    include!(concat!(env!("OUT_DIR"), "/control_message.rs"));
}

pub mod message {
    include!(concat!(env!("OUT_DIR"), "/message.rs"));
}

pub mod node_message {
    include!(concat!(env!("OUT_DIR"), "/node_message.rs"));
}

pub mod utils {
    include!(concat!(env!("OUT_DIR"), "/utils.rs"));
}
