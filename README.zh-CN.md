# AgentFork

AgentFork 在 agent 的 tool call 完成后，保存环境和对话，并通过实际恢复验证这个断点能否用于分支。项目包含 Btrfs＋CRIU 后端、Claude Code harness、分支搜索、Terminal Bench 接口及本地轨迹网页。CPU 后端不要求 KVM。

**不是每个 tool call 都一定能保存。** 每次先确认工具结果已写入原生对话文件，暂停下一次模型请求，停止 actor，检查进程树和网络连接，随后保存内存与文件系统，再恢复父环境。在发布断点前，还会恢复一个隔离子环境并实际执行工具；失败时不发布该断点。可以添加自己的应用健康检查。

和纯磁盘快照相比，它还能恢复进程内存、寄存器、线程、文件描述符位置及受支持的内核资源。对外部服务已经造成的影响、外部连接对端、GPU 状态等不能靠这个后端回滚。

安装和命令见 [英文 README](README.md)。需要 Linux、rootful Podman、runc、CRIU、已挂载的 Btrfs，以及 Python 3.12+。项目不会自动格式化磁盘。配置使用环境变量，不包含 API key、模型供应商配置、实验数据或 Claude Code 二进制。

已完成任务默认保留恢复材料；只有显式指定 `--discard-completed-checkpoints` 才回收它们的磁盘快照。历史的“恢复验证通过”不等于今天材料仍完整，`agentfork audit` 会重新检查文件是否存在和校验和。

目前定位为研究基础设施，支持范围和证据见 [质量说明](docs/quality.md)。上游法律归属仅保留在版权声明中，项目名称、接口、搜索模式和网页均使用 AgentFork。
