# 开发进度日志

- 2026-06-02：Completed。调整规则文件路径至 .github/copilot-instructions.md；初始化 docs/ 目录与基础文档骨架。下一步：根据需求补充 SPEC 并确认范围。
- 2026-06-03：Completed。完成 SPEC 需求与技术规格编写（架构、API 契约、线程安全、TLS 边界、非功能指标与验收标准）。下一步：用户 Review 并确认后进入实现阶段。
- 2026-06-04：Completed。根据用户确认将 SPEC 收敛为“仅线程池 + CAS 队列”，移除 io_uring/协议层/TLS，补齐资源所有权、超时语义、CAS 内存序与防饥饿策略，并新增可测验收标准。下一步：用户逐节评审后进入实现阶段。
- 2026-06-04：Completed。根据用户纠偏恢复“网络接入层”为本阶段必做，确定 1A（固定长度前缀帧）与 2A（io_uring + epoll 双后端同等交付），并重构 SPEC 的架构、接口契约、背压策略与双后端验收标准。下一步：用户确认修订版 SPEC 后进入实现阶段。
- 2026-06-04：Completed。将 Robotaxi 通信冻结草案写入 SPEC：确认车端 gRPC Streaming（HTTP/2）主链路、30 万在线容量、关键事件集合、至少一次投递 + 5 分钟幂等去重、60 秒补传策略与乘客端推送频率，并补充对应参数与验收指标。下一步：进入实现骨架与接口落地。
- 2026-06-04：Completed。在 SPEC 新增“实现骨架清单（目录划分、头文件骨架、MVP 分阶段路径）与“测试分解清单”（单元/集成/性能/验收判定），形成编码阶段直接执行输入。下一步：按清单创建代码骨架并先完成 frame/queue/thread_pool 的最小实现。
- 2026-06-04：Completed。完成 RingQueue::Enqueue 的工业级实现（无锁 MPMC 环形队列槽位序号法），补齐最小依赖代码骨架（status/types/ring_queue/CMake）与 GoogleTest 边界用例；本地构建通过且 5/5 单测通过。下一步：实现 Dequeue 深化用例并推进 thread_pool 最小实现。
- 2026-06-08：Completed。按新注释规范对已开发模块执行“仅注释增强”：补齐头文件接口/字段注释（用途、参数语义、返回语义、生命周期、线程安全、错误语义），并强化 ring_queue.cpp 关键并发分支注释与状态流转示意；未改动任何行为逻辑，本地构建与测试 5/5 通过。下一步：继续按同等注释标准扩展后续模块实现。
- 2026-06-10：Completed。完成 ThreadPool 最小实现与接线：新增 include/robotaxi/thread_pool.h、src/thread_pool.cpp、tests/thread_pool_test.cpp，扩展 types.h 的 ThreadPoolMetrics，并更新 CMake 将 thread_pool 编译进 robotaxi_core；本地构建通过，ctest 10/10 全通过（原 ring_queue 5 项 + 新 thread_pool 5 项）。下一步：实现固定长度前缀帧切分模块（Frame Decoder）及其单元测试，为后续 NetworkIngress（epoll/io_uring）接入提供稳定输入。
- 2026-06-11：Completed。补齐仓库级说明与实现注释：更新 .github/copilot-instructions.md 的 Repository Facts，将已实现的 ThreadPool 模块纳入核心事实；增强 src/thread_pool.cpp 的类级说明、Start/Stop/Submit/Metrics/WorkerLoop 注释，以及成员变量与工厂函数说明，补充并发语义、内存序、风险点与优化指南；未改动行为逻辑，本地构建通过，ctest 10/10 全通过。下一步：实现固定长度前缀帧切分模块（Frame Decoder）及其单元测试，为后续 NetworkIngress（epoll/io_uring）接入提供稳定输入。
- 2026-06-15：Completed。完成固定长度前缀帧切分模块最小实现：在 SPEC 新增 FrameDecoder 契约与 FrameView/FrameDecodeResult 基础类型；新增 include/robotaxi/frame_decoder.h、src/frame_decoder.cpp、tests/frame_decoder_test.cpp，并在 types.h 落地 FrameConfig、在 CMake 接入 frame_decoder 构建与测试目标；实现大端长度解析、半包/粘包处理、超限/零长度错误判定及借用视图输出；本地聚焦测试 7/7 通过，ctest 全量回归通过。下一步：实现 NetworkIngress 抽象接口与 epoll 最小链路，将收包-切帧-入队-执行串成首条端到端路径。
- 2026-06-16：Completed。冻结 NetworkIngress 三项实现基线并落地 epoll 最小链路：在 SPEC 4.5.1 固化“每连接独立缓冲、阻塞式 PollOnce（含非阻塞探测语义）、高低水位迟滞背压并暂停读事件”；新增 include/robotaxi/network_ingress.h 与 src/network_ingress_epoll.cpp，实现 Start/Stop/PollOnce/Metrics、连接接入、收包切帧、任务入队与背压切换；扩展 types.h 的 IngressBackendType/IngressConfig/IngressMetrics，并新增 tests/network_ingress_epoll_test.cpp 覆盖配置校验、超时空轮询、收包入队与背压返回；更新 CMake 接线。下一步：补齐 NetworkIngress 与 ThreadPool 的端到端执行语义（含 payload 生命周期策略），并推进 io_uring 后端对齐实现。
