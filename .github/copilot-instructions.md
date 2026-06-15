# Copilot Workspace Instructions for iouring_cas_threadPool

Use this file as the repository-level baseline for AI agents. Task-level behavior should follow the active karpathy-guidelines skill when it is loaded; this file only defines repository-specific constraints and stable workflow rules.

## 1. Repository Facts

- The current core implementation is centered on the Frame Decoder, CAS ring queue, and ThreadPool:
  - Frame Decoder header: include/robotaxi/frame_decoder.h
  - Frame Decoder implementation: src/frame_decoder.cpp
  - Frame Decoder test: tests/frame_decoder_test.cpp
  - CAS ring queue header: include/robotaxi/ring_queue.h
  - CAS ring queue implementation: src/ring_queue.cpp
  - CAS ring queue test: tests/ring_queue_enqueue_test.cpp
  - ThreadPool header: include/robotaxi/thread_pool.h
  - ThreadPool implementation: src/thread_pool.cpp
  - ThreadPool test: tests/thread_pool_test.cpp
- Project documentation lives in docs/:
  - Specification: docs/SPEC.md
  - Progress log: docs/PROGRESS_LOG.md
  - Lessons learned: docs/LESSONS_LEARNT.md
- The project uses CMake, C++20, and GoogleTest.

### Current Scope Snapshot (Controlled Updates)

- Implemented core modules:
  - Frame Decoder：固定长度前缀切分、半包/粘包处理、非法长度判定。
  - CAS ring queue：MPMC 无锁入队/出队与基础指标。
  - ThreadPool：任务提交、工作线程循环、停止流程与基础指标。
- Pending core modules:
  - NetworkIngress（epoll / io_uring）
  - Observability / backpressure integration

## 2. Repository Rules

- Keep changes minimal and scoped to the user request.
- Do not refactor unrelated code, comments, or formatting.
- If a change creates unused imports, variables, or functions, remove only the ones introduced by your change.
- Do not introduce speculative abstractions, extra configuration, or future-proofing that was not requested.
- If a task is ambiguous, surface the ambiguity clearly before editing code.
- For multi-step work, state a short plan and define a verifiable success criterion for each step.

## 3. Workflow Expectations

- Before changing behavior, API contracts, concurrency semantics, or error codes, update docs/SPEC.md first.
- After completing a verifiable task, update docs/PROGRESS_LOG.md with the date, status, changes made, and next step.
- If a recurring mistake or a stable user preference is identified, update docs/LESSONS_LEARNT.md.
- If core module boundaries change, update "Current Scope Snapshot" in this file within the same task.
- Prefer the simplest implementation that fully satisfies the request.
- Validate the change with the narrowest relevant test or build step before broadening scope.

## 4. Code Standards

- Use C++20 and favor safe, readable, and verifiable implementations.
- Keep concurrency code explicit: explain atomic operations, memory ordering, invariants, and state transitions when they are non-trivial.
- Conflict arbitration for concurrency changes: if existing code patterns conflict with safe memory ordering or C++20 best practices, prioritize correctness and safety over style matching. Do not preserve unsafe patterns for consistency.
- Public headers should document ownership, lifetime, thread-safety, parameter meaning, return semantics, and error behavior.
- Source files should explain non-trivial control flow, concurrency tradeoffs, and algorithmic decisions.
- Do not leave TODO placeholders or empty implementations.
- Keep the codebase loosely coupled and use clean abstractions at module boundaries.

## 5. Communication Rules

- Repository communication rules override any external skill language defaults.
- Explain your thoughts, architectural designs, and answers in clear, professional Chinese (中文).
- All code, comments, logs, and markdown files (SPEC.md, PROGRESS_LOG.md, LESSONS_LEARNT.md) must be written in professional Chinese (中文).
- Be precise, direct, and authoritative. Skip conversational fluff.

## 6. Boundary Rules

- Only touch files directly related to the task.
- Do not delete existing code unless the task requires it.
- Do not expand the scope of a task just because you notice unrelated issues.
- If the active skill already covers a task-level method, let the skill guide the workflow and use this file for repository-specific constraints.