//! RPC 响应后拦截 Hook 定义

use std::future::Future;
use std::pin::Pin;

use zenoh_raft::errors::RPCError;
use zenoh_raft::testing::memstore::MemNodeId;
use zenoh_raft::testing::memstore::TypeConfig;

use super::MemRpcRequest;
use super::MemRpcResponse;
use super::TypedRaftRouter;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// RPC 响应后调用
pub type PostHook = Box<
    dyn Fn(&TypedRaftRouter, MemRpcRequest, MemRpcResponse, MemNodeId, MemNodeId) -> PostHookResult
        + Send
        + 'static,
>;

/// Hook 返回结果
pub type PostHookResult = BoxFuture<'static, Result<(), RPCError<TypeConfig>>>;
