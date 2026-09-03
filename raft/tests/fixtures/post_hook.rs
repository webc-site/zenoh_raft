//! RPC 响应后拦截 Hook 定义

use std::{future::Future, pin::Pin};

use zenoh_raft::{
  errors::RPCError,
  testing::memstore::{MemNodeId, TypeConfig},
};

use super::{MemRpcRequest, MemRpcResponse, TypedRaftRouter};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// RPC 响应后调用
pub type PostHook = Box<
  dyn Fn(&TypedRaftRouter, MemRpcRequest, MemRpcResponse, MemNodeId, MemNodeId) -> PostHookResult
    + Send
    + 'static,
>;

/// Hook 返回结果
pub type PostHookResult = BoxFuture<'static, Result<(), RPCError<TypeConfig>>>;
