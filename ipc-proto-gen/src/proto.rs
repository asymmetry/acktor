#![allow(unused_imports)]
#![allow(dead_code)]

pub mod node_message {
    include!(concat!(env!("OUT_DIR"), "/node_message.rs"));
}

pub mod actor_message {
    include!(concat!(env!("OUT_DIR"), "/actor_message.rs"));
}

pub mod ipc_message {
    include!(concat!(env!("OUT_DIR"), "/ipc_message.rs"));
}

pub mod control_message {
    include!(concat!(env!("OUT_DIR"), "/control_message.rs"));
}

pub mod utils {
    include!(concat!(env!("OUT_DIR"), "/utils.rs"));
}
