# 本轮验证证据

[summary.json](summary.json) 是逐项汇总；没有最终失败项，但真实环境验收未完成。本目录记录本机执行结果，未部署外部集群。

- `foundation/`：双仓 Go 单测/race/vet、Linux 编译、协议生成、OpenAPI、依赖边界、工具契约及源码稳定性。跳过项逐个记录。
- `database/`：临时本机 PostgreSQL，72 个顶层测试通过、无跳过；补测 foundation 的 36 个数据库跳过项。CSI/节点是 fixture。
- `runtime/`：外移 Rust crate 在 macOS 的 9 项可执行单测。
- `rust-linux/`：原 AgentENV Linux 目标全部 target 类型检查及双方 Rust 源码哈希，未运行 Linux 二进制。
- `deployment/`：两个 namespace 的离线 Kustomize/凭据验证，未执行 Docker 镜像构建或部署。
- `patch-replay.log`：原版补丁的 64 路径精确重放结果。
- `invasiveness.json`：相对原版的前后改动统计，含已有用户修复；内嵌测试仍计入其所在核心文件，独立测试文件单列。

仍未执行：3 项原生 Redis 测试（本机无 redis-server）、Kubernetes 集群测试、Linux 原生 Rust 可执行测试、Docker/systemd、KVM/厂商 CSI 与原生 HTTP/WS/P2P/recorder 真机回归。操作入口见 [真实环境验收](../../../design/14-agentenvadapter-real-environment-test-plan.md)。
