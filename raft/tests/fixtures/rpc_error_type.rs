//! 测试用 RPC 错误类型定义

use zenoh_raft::RaftTypeConfig;
use zenoh_raft::errors::NetworkError;
use zenoh_raft::errors::RPCError;
use zenoh_raft::errors::Unreachable;

use super::Direction;

/// 注入的 RPC 错误类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcErrorType {
    /// 模拟节点不可达 (Unreachable)
    Unreachable,
    /// 模拟网络临时错误 (NetworkError)
    NetworkError,
}

impl RpcErrorType {
    pub fn make_error<C: RaftTypeConfig>(&self, id: C::NodeId, dir: Direction) -> RPCError<C> {
        let msg = format!("error {dir} id={id}");
        match self {
            RpcErrorType::Unreachable => Unreachable::<C>::from_string(msg).into(),
            RpcErrorType::NetworkError => NetworkError::<C>::from_string(msg).into(),
        }
    }
}
