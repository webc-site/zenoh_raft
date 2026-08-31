//! RPC 发送前拦截 Hook 定义

use std::future::Future;
use std::pin::Pin;

use zenoh_raft::errors::Infallible;
use zenoh_raft::errors::RPCError;
use zenoh_raft::testing::memstore::MemNodeId;
use zenoh_raft::testing::memstore::TypeConfig;

use super::MemRpcRequest;
use super::TypedRaftRouter;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// RPC 发送前调用
pub type PreHook = Box<
    dyn Fn(&TypedRaftRouter, MemRpcRequest, MemNodeId, MemNodeId) -> PreHookResult + Send + 'static,
>;

/// Hook 返回结果
pub type PreHookResult = BoxFuture<'static, Result<(), RPCError<TypeConfig, Infallible>>>;
