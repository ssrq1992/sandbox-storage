//! Custom extension integration: an optional external HTTP service
//! (`[custom_extension].url`) extending sandbox behavior through lifecycle
//! hooks and an optional generic external block provider; see [`client`] and [`storage`].

pub(crate) mod client;
pub(crate) mod storage;

pub(crate) use client::{
    custom_extension_params_is_empty, CustomExtensionClient, CustomExtensionHookGuard,
    CustomExtensionParams,
};
