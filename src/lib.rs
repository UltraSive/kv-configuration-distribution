//! kvdist: a replicated configuration key-value store modelled on
//! Cloudflare's Quicksilver.

pub mod api;
pub mod applier;
pub mod cache;
pub mod config;
pub mod error;
pub mod node;
pub mod protocol;
pub mod replication;
pub mod rpc;
pub mod server;
pub mod store;
pub mod types;
