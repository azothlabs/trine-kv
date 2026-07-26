# Trine KV 逐文件逐行代码审计进度

> 开始日期：2026-07-25
> 实现后二次审计完成日期：2026-07-26
> 审计范围：仓库内全部 180 个 Rust 文件（当前 100,712 行），包括 `src/`、`tests/`、
> `examples/` 与 `benches/`。Cargo 配置、依赖和生成/包含边界另行复核。
> 方法：逐文件完整阅读；对外部输入、整数换算、格式解码、持久化原子性、并发、
> 资源生命周期、`unsafe`、panic/错误传播和拒绝服务风险做重点数据流追踪；辅以
> `cargo fmt`、Clippy、测试、依赖审计及针对性搜索。
> 状态标记：`已审` 表示该文件每一行已读并形成初步结论；跨文件结论可在后续复核时更新。

## 总体进度

- 已审：180 / 180 个 Rust 文件（100%）
- 已审行数：100,712 / 100,712（100%，含本次新增实现与测试）
- 当前阶段：逐文件阅读、根因修复、实现后二次审计和最终全仓验证均已完成
- 已确认问题：39 个（初审 24 个；实现后二次审计新增 15 个）
- 严重度分布：高 21 个，中 18 个
- 已根治：39 / 39 个
- 未解决：0 个
- 待跨文件复核项：0 个

## 结论摘要

- 第一修复批次（持久数据损坏/丢失）：F-012、F-015、F-016、F-022、F-023、F-024，
  已完成。
- 第二修复批次（隔离、MVCC 与并发生命周期）：F-002、F-003、F-005、F-006、F-007、
  F-014，已完成。
- 第三修复批次（可用性、资源上限与 API/benchmark 设计）：其余 12 个中等问题，
  已完成。
- 未升级的“候选/观察”并非未阅读：它们要么只在内部不变量被未来代码破坏时成立，
  要么当前影响限于额外工作、诊断偏差或测试维护性；均保留在对应文件记录中供重构时使用。
- 所有确认问题均已加入对应的回归测试、边界条件测试或正确性验证；原始证据与建议保留在
  下文，便于后续维护者理解为什么这些不变量不能回退。

## 修复状态

| 编号 | 状态 | 根因处置 |
|---|---|---|
| F-001 | 已根治 | `Snapshot` 绑定数据库谱系，所有 snapshot 读取入口统一校验 tracker 身份。 |
| F-002 | 已根治 | 精确 manifest v2 持久化单调 bucket generation；旧 handle 在 drop/recreate 后稳定返回 `BucketStale`，其他布局版本直接拒绝。 |
| F-003 | 已根治 | 任一半无限 range tombstone 都令 table bounds 保守退化为 unbounded，禁止错误跳表。 |
| F-004 | 已根治 | 公共 key/value/block 配置与实际 table/blob/WAL 编码预算统一，并在 commit 前验证最终编码长度。 |
| F-005 | 已根治 | commit、refresh、checkpoint、flush/compaction/maintenance 及 content 生命周期操作全部进入 close activity barrier。 |
| F-006 | 已根治 | 没有持久 reader-retirement 证明时，object-store table/blob 永不物理删除；逻辑删除与压实仍立即生效，旧 reader 数据不会被回收。 |
| F-007 | 已根治 | 文件 ID 在写对象前经 manifest 原子永久预留；远端 table/blob 使用 `IfNoneMatch` create-only，相同字节重试幂等，不同字节拒绝，删除接口失败关闭。 |
| F-008 | 已根治 | 远端 table/blob 打开改为 HEAD + 按需 range read；client/storage listing 均支持 continuation page，恢复检查逐页消费，S3 移除固定 100k 截断。 |
| F-009 | 已根治 | object WAL 将 admission 与 waiter 分离，排序锁只覆盖序列预留和入队，多并发提交可由 worker 真正合并。 |
| F-010 | 已根治 | open 时按最坏 WAL key 验证最终 lease 编码长度，encoder 再次执行 64 KiB 硬上限。 |
| F-011 | 已根治 | 所有 object prefix/key 使用同一 canonical relative form，S3 与内存适配器行为一致。 |
| F-012 | 已根治 | manifest 持久化 `next_file_id` 高水位；flush、compaction、blob GC 先原子预留，失败留下永久 gap，绝不重用。 |
| F-013 | 已根治 | CAS 传输错误按 readback 区分已应用、未应用和真实冲突；rebase 重试有 32 次上限。 |
| F-014 | 已根治 | snapshot admission 与 compaction floor 使用同一原子锁序；guard 覆盖 build、publish、install 全阶段。 |
| F-015 | 已根治 | manifest publish 返回 `PublishedDurabilityUnknown` 阶段结果；rename 已发生时安装新状态、关闭 handle 且保留输出。 |
| F-016 | 已根治 | 部分 bucket flush 在 publish lock 内扫描所有剩余 point/range 数据，WAL floor 只推进到全局最老未落盘序列之前。 |
| F-017 | 已根治 | blob full decode 同时限制单记录和 aggregate decoded value bytes，并在下一次分配前拒绝超额。 |
| F-018 | 已根治 | async blob inline 按 file id 缓存 read handle、长度和已验证 header，重复记录不再重复 open。 |
| F-019 | 已根治 | native manifest 先 open/metadata 校验上限，再按精确长度受限读取；超大 sparse 文件不触发整文件分配。 |
| F-020 | 已根治 | async borrowed native read 统一走 owned platform/blocking completion，await 后复制；同步接口使用独立 blocking 实现。 |
| F-021 | 已根治 | native/object WAL async admission 改用 `try_send`，队列满返回 `RuntimeBusy`，不再阻塞 executor。 |
| F-022 | 已根治 | 任一 append/persist/confirmed-marker 失败都会永久关闭该 WAL lane；只能经数据库重开和恢复后重新接纳写入。 |
| F-023 | 已根治 | sealed upload 清理改写为永久轻量 UploadId tombstone，旧 ID 永远不能重新绑定 chunk namespace。 |
| F-024 | 已根治 | 先持久化含最终 content identity 的 `Sealing` marker，再发布 descriptor；重试可补齐 descriptor 并幂等完成。 |

## 实现后二次审计

二次审计逐块重读全部修改 diff，并重新追踪跨模块持久化、并发、取消、错误阶段和资源
上限。以下问题都来自对本次实现本身的反向审查；不是只调整测试或错误文案。

| 编号 | 严重度 | 状态 | 二次审计发现与根因处置 |
|---|---|---|---|
| S-001 | 高 | 已根治 | native async manifest 原先在锁外发布，两个编辑可能从同一基线生成并互相覆盖。现在整个 read/edit/publish/install 都在同一 manifest 串行临界区内执行；browser 也使用独立 async manifest 锁。 |
| S-002 | 中 | 已根治 | manifest 结构校验曾把“一个 immutable blob 被多个输出 table 引用”“table 与其配套 blob 使用同一数值 ID、但位于不同对象命名空间”以及“仍被引用的 blob 同时带待删标记”误判为不可读取。现在 table ID 只在 table namespace 内唯一；blob 引用可共享；冲突待删状态允许恢复层读取，但清理器重新计算实时引用并保留仍被引用的对象及标记。 |
| S-003 | 高 | 已根治 | object content 写删及 upload-state retirement 曾存在只在操作前 fencing、传输结果未知时未做操作后 fencing 的窗口。所有远端内容 mutation 现在统一执行前后 lease fence，失败即关闭数据库。 |
| S-004 | 高 | 已根治 | object WAL group 的内存预算最初只统计真实 frame，未统计为序列 gap 生成的空 frame；admission 指标也可能在入队失败前递增。现在先受检计算完整 group 大小、限制分配，再入队，成功后才记账。 |
| S-005 | 中 | 已根治 | 内存 object client 的分页实现曾先构造完整 listing；远端 storage prefix 也可能扫到相邻命名空间。现在直接从 `BTreeMap::range` 取 `limit + 1`，远端目录 prefix 强制带 `/`。 |
| S-006 | 中 | 已根治 | blob inline 重写的 read-handle map 会随 file ID 数量无界增长。同步与异步路径都改为固定一个文件的有界缓存，并保留 header/长度验证结果。 |
| S-007 | 高 | 已根治 | object-store partial chunk 若按固定 key 覆盖，旧 writer 可在新 revision 提交后改写当前字节。upload state 升级为精确 `TRNUPLD5`；partial chunk 使用 revisioned immutable key，完整 chunk create-only，seal 先进入 `Sealing` 再提升最终 partial。 |
| S-008 | 高 | 已根治 | object WAL worker 在一次 publish/renew/rewrite 失败后仍可能继续处理后续命令，且退出竞态可遗留永不唤醒的 waiter。失败现在永久终止 lane，并持续排空、完成所有已排队或竞态进入的 completion。 |
| S-009 | 中 | 已根治 | object lease decoder 仍接受 raw/v2 旧编码，削弱了精确格式与 owner fencing 不变量。现在只接受当前 v3、精确 header、长度和尾部消费。 |
| S-010 | 高 | 已根治 | key 与 value 的公开上限分别合法时，组合到同一 blob record 仍可能超过最终编码预算。value 上限现在预留最大 key、固定 metadata 与 LZ4 最坏膨胀，并在 commit 前按 bucket codec 再验证。 |
| S-011 | 中 | 已根治 | publish activity 与 LSM write-admission 的 `saturating_sub` 会把 guard 双释放静默隐藏。现在显式检测 underflow，并只在有效计数上减一。 |
| S-012 | 高 | 已根治 | async WAL enqueue 对所有错误都把 commit slot 标成 skipped；channel 断开等错误可能发生在 durable outcome 已不可信的阶段。仅明确的入队前 `RuntimeBusy`/`InvalidOptions` 可跳过，其他错误关闭 handle 并要求重开恢复。 |
| S-013 | 高 | 已根治 | 新增高水位字段后 manifest 曾继续使用旧布局版本号，可能让旧字节以新 schema 解析。当前布局提升为精确 version 2，并加入旧 version 拒绝的边界条件测试。 |
| S-014 | 中 | 已根治 | eventually-consistent listing 若在首次 abort/prune 清理时漏掉 chunk，永久 tombstone 会让该对象以后永远不再被扫描。两类 upload maintenance 现在重扫 tombstone：Aborted 删除全部 chunk；Sealed 只删 revisioned partial，保留 descriptor 引用的 canonical chunk。 |
| S-015 | 高 | 已根治 | table decoder 仍接受不含 blob header identity 与 internal-key 绑定的 raw blob reference tag。该旧 variant、encoder、reader 和解码分支已整体删除；table 提升为精确 version 7，只接受带完整 record metadata 的 `BlobIndex`，tag 2 明确失败关闭。 |

## 逐文件记录

### 001 — `src/lib.rs`（204 行）— 已审

- 覆盖：第 1–204 行，完整阅读。
- 作用：crate 文档、模块可见性、公共 API 再导出、持久 WAL 集成测试入口。
- 安全结论：未发现直接漏洞；无 `unsafe`，无生产路径 `unwrap`/`panic`。
- 待跨文件复核：
  - `#[allow(dead_code)]` 用于 `cache`、`filter`、`memtable`、`mvcc`，可能掩盖已废弃或
    未接通的核心路径；需结合调用图确认是否为架构残留。
  - crate 级允许 `clippy::missing_errors_doc`，公共 API 的失败契约可能因此缺失；
    审计各公共方法时逐项核对，而不是仅依赖 lint。
- 备注：`include!("../tests/internal/persistent_wal.rs")` 仅在 `cfg(test)` 下生效，
  当前未见发布时的路径/代码注入风险。

### 002 — `src/error.rs`（623 行）— 已审

- 覆盖：第 1–623 行，完整阅读。
- 作用：统一公共错误模型及其显示、错误源转换。
- 安全结论：未发现直接漏洞；唯一保留的底层错误源是 `io::Error`，没有错误吞噬或
  生产路径 panic。
- 设计观察：错误枚举很大，但按调用方可恢复语义区分了 fencing、版本过期、租约、
  配额和上传冲突；`#[non_exhaustive]` 适合公共库演进。后续复核调用点是否把
  `InvalidFormat`/`Corruption` 混用而削弱故障处置。

### 003 — `src/limits.rs`（39 行）— 已审

- 覆盖：第 1–39 行，完整阅读。
- 作用：不可信持久化长度的统一上限及 `usize` 加法溢出保护。
- 安全结论：未发现直接漏洞；长度比较和 `checked_add` 均在分配前返回结构化错误。
- 待跨文件复核：确认所有解码器都实际调用这些保护，而不是存在绕过路径。

### 004 — `src/checksum.rs`（42 行）— 已审

- 覆盖：第 1–42 行，完整阅读。
- 作用：纯 Rust CRC32C 实现。
- 安全结论：未发现直接漏洞；表索引被 `& 0xff` 限定，转换安全，标准校验向量有测试。
- 设计观察：CRC32C 只提供意外损坏检测，不具备对抗恶意篡改的认证能力；是否构成
  威胁取决于存储信任模型，后续检查文档/选项是否误称其为安全认证。

### 005 — `src/codec.rs`（137 行）— 已审

- 覆盖：第 1–137 行，完整阅读。
- 作用：无压缩与 LZ4 block 编解码、codec tag 解析。
- 安全结论：解压前将声明长度限制为 64 MiB，并验证实际长度，能够阻断由伪造长度
  引发的无界单次分配；未知 tag 失败关闭。
- 待跨文件复核：64 MiB 是单块上限，仍需确认上层不会对攻击者控制的多个块并行或
  累积解压而形成内存拒绝服务。

### 006 — `src/types.rs`（188 行）— 已审

- 覆盖：第 1–188 行，完整阅读。
- 作用：读版本、内部序列、键值行、键范围和提交结果。
- 安全结论：未发现直接内存安全问题；`ReadVersion` 明确要求数据库谱系内使用。
- 待跨文件复核：`KeyRange` 字段公开且没有统一的合法性检查，因此可构造空范围或
  反向范围；需确认所有写入、扫描、WAL 恢复和表解码路径是否采用一致语义并失败关闭。

### 007 — `src/internal_key.rs`（126 行）— 已审

- 覆盖：第 1–126 行，完整阅读。
- 作用：内部键及 user key/sequence/batch index/value kind 的全序。
- 安全结论：未发现直接漏洞；同一用户键按 sequence 和 batch index 降序，边界哨兵
  与当前 `ValueKind` 顺序一致。
- 待跨文件复核：检查磁盘编码比较规则与这里的内存比较规则完全一致，避免恢复后读序变化。

### 008 — `src/point_value.rs`（153 行）— 已审

- 覆盖：第 1–153 行，完整阅读。
- 作用：零拷贝表值视图及 blob 延迟物化。
- 安全结论：共享字节范围在构造时验证且字段私有，后续切片不会因外部构造越界；
  同步/异步 blob 路径错误语义一致。
- 待跨文件复核：blob 长度和读取分配上限由 blob 子系统负责，需在那里确认拒绝服务边界。

### 009 — `src/prefix.rs`（54 行）— 已审

- 覆盖：第 1–54 行，完整阅读。
- 安全结论：切片使用 `get`/已找到的索引，无越界风险。
- 设计问题候选：`PrefixExtractor::Custom(_)` 的 `is_enabled()` 返回 `true`，但
  `extract()`、`query_filter_prefix()` 均返回 `None` 且 `supports_prefix_filter()` 为
  `false`；公共状态查询与实际能力语义不一致。待检查调用者后决定问题级别。

### 010 — `src/range_tombstone.rs`（356 行）— 已审

- 覆盖：第 1–356 行，完整阅读，包括随机参考模型测试。
- 安全结论：索引分区点与后续精确过滤组合正确，未见越界或未检查转换。
- 正确性问题候选：`ranges_overlap` 将空范围（如 `[b,b)`）视为可与其他范围重叠；
  当前随机测试只生成非空范围，未覆盖空/反向边界。结合 `KeyRange` 可公开构造这一点，
  后续追踪写入校验和压实裁剪，确认是否会导致错误墓碑或仅造成多余工作。

### 011 — `src/block.rs`（586 行）— 已审

- 覆盖：第 1–586 行，完整阅读。
- 作用：数据块头、校验和、压缩块编解码、按偏移读取及共享 payload。
- 正面结论：offset/length 的 `u64 → usize` 转换和大多数相加均检查溢出；checksum
  在解码前核验；声明的解压长度在解压前限制。
- 拒绝服务问题候选：块头的 `encoded_len` 没有独立上限。按源偏移读取会先按该
  `u32` 长度申请/读取完整编码块；如果上层只以实际文件大小作为 `payload_len`，
  恶意超大/稀疏表文件可能诱发接近 4 GiB 的分配。需结合表打开路径确认是否先受
  `MAX_WHOLE_TABLE_DECODE_BYTES` 或文件尺寸上限保护。
- 可移植性问题候选：第 245、341、283 行存在未经 `checked_add` 的
  `BLOCK_HEADER_LEN + encoded_len`。在 32 位目标上，恶意 `u32::MAX` 长度可能触发
  debug panic；需确认 wasm/native 32 位路径及上层限制后定级。
- 低级观察：共享 offset 读取允许 `offset == payload_len`，会先尝试读取 13 字节头
  再因块越界失败；应在 I/O 前用 `>=` 拒绝，减少越界区域探测/无效 I/O。

### 012 — `src/state_transition.rs`（12 行）— 已审

- 覆盖：第 1–12 行，完整阅读。
- 结论：单一内部枚举清晰表达幂等 durable transition；未发现问题。

### 013 — `src/mvcc.rs`（23 行）— 已审

- 覆盖：第 1–23 行，完整阅读。
- 结论：可见性规则为 `sequence <= read_sequence`，实现直接且无算术风险。

### 014 — `src/platform.rs`（33 行）— 已审

- 覆盖：第 1–33 行，完整阅读。
- 安全结论：native 时钟倒退到 epoch 前会返回错误；WASM 对 NaN、负数和非整数做
  了显式检查，且 ECMAScript TimeClip 范围使转换安全。
- 待跨文件复核：依赖可回拨墙钟判断租约过期，需在租约/回收状态机中确认时钟回拨
  不会错误延长或提前授权不可逆删除。

### 015 — `src/search.rs`（118 行）— 已审

- 覆盖：第 1–118 行，完整阅读。
- 结论：所有空切片、末尾和下溢边界均处理；二分中点写法无加法溢出。未发现问题。

### 016 — `src/durability.rs`（320 行）— 已审

- 覆盖：第 1–320 行，完整阅读；含 macOS 两个 `unsafe` 系统调用。
- `unsafe` 结论：`fcntl(F_FULLFSYNC)`/`fsync` 仅临时使用有效 `File` 所属 fd，
  ABI/返回值判断正确，`SAFETY` 说明覆盖生命周期与不保留约束。
- 可靠性问题候选：macOS 的 `SyncAllStrict` 在 `F_FULLFSYNC` 返回
  `ENOTSUP`/`ENOTTY`/`EINVAL` 时静默降级为普通 `fsync`，因此“strict/突然断电”
  契约可能在调用成功时并未满足。需对照 `DurabilityMode` 公共文档定级。
- 设计观察：Windows 对目录 flush 的 `PermissionDenied` 一律成功返回，这是明确的
  best-effort 取舍，但若公共 API 把 `SyncAll` 描述为 rename 元数据必然持久，则存在
  同类契约偏差。

### 017 — `src/write_batch.rs`（324 行）— 已审

- 覆盖：第 1–324 行，完整阅读。
- 安全结论：内部 commit-sequence stamp 的 operation/value offset 使用受检转换。
- 输入校验问题候选：`BatchOperation` 公开可构造，调用方可直接创建任意 bucket 名、
  超限 key/value 和非法 `KeyRange`，绕过 `WriteBatch::*_bucket` 的即时校验；安全性
  完全依赖 commit 边界再次验证。后续审计 commit 路径。
- 正确性问题候选：三个 range-delete 添加方法均不验证空/反向范围，确认了前述
  `KeyRange` 候选可进入 batch；继续追踪提交和压实。

### 018 — `src/snapshot.rs`（291 行）— 已审

- 覆盖：第 1–291 行，完整阅读。
- 正面结论：pin/clone/drop 在同一 mutex 下计数，RAII 释放路径清晰；poison 后继续
  使用数据是明确的可用性选择。
- 隔离问题候选：`Snapshot` 只携带 sequence 和所属 tracker 的 pin，不携带可验证的
  数据库/谱系身份；公共方法允许把 snapshot 与任意 `Bucket` 组合。需检查
  `Bucket::*_at` 是否验证句柄同源，否则跨数据库 snapshot 会以错误版本读取数据，
  与 `ReadVersion` 的数据库作用域契约冲突。

### 019 — `src/bucket.rs`（1,089 行）— 已审

- 覆盖：第 1–1,089 行，完整阅读；同步/异步及 eager/lazy/forward/reverse API 对称核对。
- **已确认设计缺陷候选（待行为验证定级）**：所有 snapshot 读取仅取
  `snapshot.read_sequence()`/`is_pinned()`，没有验证 snapshot 与 bucket 的数据库
  身份。`Bucket::get_at_sync`、range/prefix 及异步等价 API 均可接受其他数据库创建的
  snapshot。后续以集成测试è¡ä¸ºéªè¯错误读/版本错误。
- 数据隔离问题候选：普通最新读取通过 `latest_read_state()` 在需要时跟随 bucket
  registry，但 `get_at_sync/get_at` 固定使用句柄内的 `self.state`；range/prefix 的
  snapshot 路径则按 bucket 名重新查状态。drop/recreate 后，旧 `Bucket` 句柄上的点读
  与范围读可能观察不同代的数据，且点读可能暴露已删除代的值。需结合 drop 语义è¡ä¸ºéªè¯。
- API 设计观察：`BucketName::new` 是公开且不验证的构造器，类型本身不保证文档描述的
  bucket-name 不变量；目前 `Bucket` 构造私有，风险主要是“newtype 伪保证”和未来误用。
- 输入校验闭环：该文件不在 snapshot 入口验证 key/range/prefix 长度或范围方向，
  依赖更下层统一边界；继续审计 DB/commit。

### 020 — `src/db/commit/helpers.rs`（172 行）— 已审

- 覆盖：第 1–172 行，完整阅读。
- 正面结论：commit 前统一验证 key/value/range-bound 长度，并把配置上限再次夹到
  64 MiB 硬上限；WAL replay 检查 sequence 严格递增和 bucket 存在。
- 闭环结论：仅验证 range 两端的长度，不验证范围是否为空/反向，因此非法范围继续
  进入 WAL/LSM；此前候选保留。

### 021 — `src/db/commit/state.rs`（685 行）— 已审

- 覆盖：第 1–685 行，完整阅读。
- 正面结论：commit-sequence stamp 的 operation index、offset、加法和最终切片均
  受检；同步 waiter 使用条件变量循环；Future 注册 waker 后二次检查结果，避免丢唤醒；
  自制阻塞 executor 依赖 `Thread::unpark` token，wake-before-park 也不会丢。
- 设计观察：`BackgroundWriteFuture` 在完成后再次 poll 会 panic；符合常见 Future
  实现约定但不是 fused future，属于文档/可组合性问题而非漏洞。
- 待复核：`PreparedDeltaKeyBounds` 对反向范围仍会记录相互倒置的 lower/upper，
  可能影响冲突检测/分片元数据。

### 022 — `src/db/commit.rs`（787 行）— 已审

- 覆盖：第 1–787 行，完整阅读；同步、native async、WASM async、WAL 前门和
  memtable publication 顺序逐段对照。
- **高风险并发/生命周期问题候选**：同步提交（第 179–183 行）和 WASM async 提交
  （第 214–223 行）都持有 `publish_barrier.begin_activity()`，但 native async
  `commit_write_request_async`（第 189–193 行）没有。`close_sync/close_native_async`
  依赖该 activity 计数等待已接纳发布完成后才释放 writer lease；native async 写可能
  与 close 竞态，在 close 返回/租约释放后继续 WAL 或内存发布。需要故障注入è¡ä¸ºéªè¯并
  检查 runtime task 关闭是否偶然掩盖。
- 输入校验结论：commit 会复核操作数和 key/value/bound 大小；公开
  `BatchOperation` 不能被插入私有 `WriteBatch.operations`，所以“直接构造绕过 bucket
  名校验”候选撤销。实际 bucket 仍必须在 registry 中存在。
- **正确性问题确认**：range delete 的方向/空集在 commit 边界也未验证，随后直接
  进入 prepared delta、WAL 和 LSM。影响仍需结合 LSM 读/压实确定。
- 故障处置：WAL 接纳后若 memtable 发布失败会关闭 DB，而不是跳过 slot 让后续提交
  越过可恢复记录；该失败关闭策略正确。

### 023 — `src/db.rs`（1,117 行）— 已审

- 覆盖：第 1–1,117 行，完整阅读；commit tracker、publish barrier、维护协调器和
  Db/DbInner 生命周期均已覆盖。
- 并发正确性：commit slot 用原子 reserve + mutex 状态机推进连续 visible boundary；
  等待者在同一状态锁/二次检查下注册，未见丢唤醒。sequence 溢出失败关闭。
- **对 022 的证据加强**：`PublishBarrier::close` 明确只等待
  `begin_activity()` 增加的计数归零，然后调用方释放 writer lease。native async
  提交缺少 activity guard 不是等价路径差异；background write task 也未加入
  `DbInner.background_workers`（该列表用于维护 worker）。因此 close 无法据此等待该写入。
- 低级并发问题候选：`MaintenanceCoordinator::wait_for_progress` 每次虚假唤醒/无进度
  通知后重新使用完整 timeout，而非剩余 deadline；持续请求通知可能让名义上的 5 秒
  前台 backpressure 等待无限延长。
- 生命周期观察：最后一个用户 Db/Bucket 句柄会关闭数据库并释放 substrate lease；
  内部任务使用 `counts_as_user_handle = false`，设计意图明确。需结合 native async
  写入竞态确认最后句柄 drop 时的安全性。

### 024 — `src/db/sync_api/maintenance/background.rs`（707 行）— 已审

- 覆盖：第 1–707 行，完整阅读；后台维护请求与全部 point/range/prefix read plumbing。
- F-001 根因闭环：point 路径收到 `read_pin_held=true` 时完全跳过目标 DB pin；scan
  路径则无条件在目标 DB 以外来 sequence 创建未验证 pin。两类 API 不但都不校验
  lineage，对 foreign/future sequence 的 retention 行为也不一致。
- F-002 根因闭环：point state 可由调用方传入旧 `Arc<LsmTree>`，range/prefix 必定
  按 bucket 名从当前 registry 取 state，确认代际分裂是架构路径差异。
- 性能/设计观察：同步与异步 eager/lazy 的四组 scan 构造高度重复，容易出现修复只落
  在部分路径的问题；F-001/F-002 正是这种重复导致的入口不一致，建议收敛成一个已验证
  的 `(bucket generation, lineage, read sequence, pin)` 构造器。
- 后台 worker 启动后才写入 registry；若中途某个 spawn 失败，之前已启动 worker
  会继续存在并由 Db 生命周期回收，未见泄漏或悬空线程。

### 025 — `src/lsm/mod.rs`（15 行）— 已审

- 覆盖：第 1–15 行，完整阅读。纯模块边界与内部再导出，未发现问题。

### 026 — `src/lsm/write.rs`（187 行）— 已审

- 覆盖：第 1–187 行，完整阅读。
- 范围结论：非法/空 `KeyRange` 未被规范化，直接作为 tombstone 插入并进入冻结队列；
  点查的 `key_is_in_range` 对反向/空范围不会删除键，因此风险主要转移到冲突与压实。
- 并发观察：active memtable 指针与 range tombstones 分属两个 RwLock，但提交发布外层有
  `memtable_publish_lock`，freeze 的锁顺序有注释；继续结合 delta/读快照复核原子可见性。
- 低概率计数风险：`range_tombstone_bytes.fetch_add` 是回绕加法而非饱和加法；在当前
  每字段 64 MiB、batch 最多 `u32::MAX` 的边界下理论上仍可能接近数百 PiB，现实会先
  耗尽内存，因此不单列漏洞。

### 027 — `src/lsm/flush.rs`（118 行）— 已审

- 覆盖：第 1–118 行，完整阅读。
- 正面结论：先安装 L0 table 再移除 immutable memtable，读者至多见重复不会漏数据；
  removal 通过 freeze sequence + `Arc::ptr_eq` 精确匹配。
- 范围结论：墓碑原样转入 table，没有在 flush 边界修复非法范围。

### 028 — `src/lsm/tree.rs`（319 行）— 已审

- 覆盖：第 1–319 行，完整阅读。
- 正面结论：bucket drop gate 对写入采用 admission guard，并为同步/异步 drain 做了
  无丢唤醒等待；失败的 drop 通过 guard 回滚，成功后永久关闭写 gate。
- F-002 根因加强：成功 drop 只永久拒绝 `admit_write`；所有 LSM 读入口不调用
  `ensure_available()`。`ensure_available` 仅用于 bucket 创建/打开流程，因此持有旧
  `Arc<LsmTree>` 的句柄仍可读取旧代。
- 设计观察：若 lifecycle mutex 在 admission guard drop 时已 poisoned，计数不减且
  等待者不唤醒；不过 poison 需要在极短的纯状态临界区内 panic，风险较低。

### 029 — `src/lsm/scan.rs`（424 行）— 已审

- 覆盖：第 1–424 行，完整阅读。
- 正面结论：flush 期间先捕获 memtable sources 再捕获 version，允许重复但防止漏读；
  source 持有 Arc，iterator 生命周期内资源稳定。
- 范围观察：用户提供的空/反向 scan range 会贯穿 table 候选和 tombstone overlap；
  上层未验证。空 tombstone 可能因 `ranges_overlap` 缺陷被加入 scan tombstone 集，
  但最终逐键覆盖仍为 false，主要造成额外探测。
- 设计债务：同步/异步 tombstone 收集及四个 range/prefix iterator 构造高度复制，
  与 DB 层重复叠加，扩大安全修复遗漏面。

### 030 — `src/lsm/conflict.rs`（403 行）— 已审

- 覆盖：第 1–403 行，完整阅读。
- 正面结论：冲突快照按“memtable sources 后 version”获取，flush 顺序保证不会漏掉
  必须冲突的写；point conflict 同时检查较新点记录与覆盖墓碑。
- 空范围影响：由于 `ranges_overlap` 把部分空范围视为重叠，事务对空 range 的读取可能
  被后续空 tombstone 错误判定为冲突（假阳性）；尚未看到错误放行的假阴性。
- 性能问题候选：range conflict 对每个 memtable 做全量 `.iter().filter()`，并遍历
  version 的全部 table handle，而不是使用有序范围和 `range_scan_tables` 剪枝。大型
  库上，每次带 range read 的事务提交可能被放大为全库扫描，形成可被请求触发的 CPU/I/O
  拒绝服务面。待结合 transaction 对 read set 的数量限制定级。

### 031 — `src/lsm/delta.rs`（732 行）— 已审

- 覆盖：第 1–732 行，完整阅读；16-shard publication、epoch merge、snapshot 和测试。
- 正面结论：range tombstone 复制到全部 shard，使按单 key shard 快照仍能看到范围删除；
  commit 在全部 shard publish 后才推进 visible sequence，正常读序列不会观察部分提交。
- F-001 影响加强：外来 snapshot sequence 未经目标 DB visible-boundary 验证；若它高于
  目标当前 visible sequence，就可能在 16-shard delta 顺序发布期间看到尚未标记 visible
  的部分提交。跨谱系缺陷因此不仅是“读错历史版本”，还可能破坏原子可见性。
- 失败原子性候选：一个 in-memory delta 跨 shard 顺序 publish；若后续 shard 因 poisoned
  lock/merge 错误返回，前面 shard 已发布，但外层把整个调用视为
  `delta_publication_started=false`，无 WAL 时只 skip slot 而不关闭 DB。可返回错误却保留
  部分数据。触发需要内部锁 poison，可能性低，但失败处理的原子性判断不准确。
- 性能代价：每个 range delete 复制 16 份完整 bound `Vec`，大范围删除的内存/WAL 外
  内存开销被固定放大；应评估共享 Arc tombstone 或专门的全局 range shard。

### 032 — `src/lsm/version.rs`（878 行）— 已审

- 覆盖：第 1–878 行，完整阅读；包括 level 构建与校验、point/batch/range table
  选择、L0 压力指标和全部单元测试。
- 正面结论：L0 按新到旧排序；L1+ 按最小键排序并拒绝边界相交的 table；point
  lookup 的 partition-point 边界与 inclusive table span 一致，未见漏表的正确性问题。
- 性能问题候选：`range_scan_tables` 即使对已排序且互不重叠的 L1+ 也逐表
  `.filter()`；`has_overlapping_tables` 对 level 内 table 做 O(n²) 两两检查，并位于
  L0 pressure 查询路径。table 数量异常增大时，这些元数据操作会成为可放大的 CPU
  成本；待结合压缩调度频率和 level table 上限定级。
- 设计观察：多 key point lookup 只合并“输入中连续落到同一 table”的 key；未排序
  输入可对同一 table 重复回调，正确性仍成立但批量读放大。测试辅助创建的临时 table
  文件未统一清理，属于测试卫生问题，不影响生产数据安全。

### 033 — `src/lsm/read.rs`（1,069 行）— 已审

- 覆盖：第 1–1,069 行，完整阅读；同步/异步单点与批量点读、候选归并、范围墓碑覆盖、
  去重/scatter 和测试。
- 正面结论：point candidate 使用完整 internal-key 顺序选择最新可见记录；table
  `largest_sequence` 剪枝在相等 sequence 时仍保留 table，不会遗漏同批次更高
  batch index；Put 缺失 value 会以 corruption 失败而非 panic。
- 性能问题候选：每次需要解析 Put 时，`memtable_range_tombstones_in_snapshot` 会克隆
  delta、active 与所有 immutable 的全部 range tombstone，再构建索引；普通单点
  `get` 的分配和 CPU 因而可能与所有未 flush 墓碑数量线性相关，而非只与覆盖目标 key
  的墓碑相关。大量小范围删除可放大后续热点点读，待结合索引实现与写缓冲边界定级。
- 资源放大观察：超过 32 个 key 的批量读为每个唯一 key 额外复制一份 `Vec<u8>` 到
  `BTreeMap`；同步与异步实现也基本复制。现有 batch 数量/总字节验证可限制单次输入，
  但需要在公共批量读入口继续确认同样的限制。

### 034 — `src/lsm/compact.rs`（1,172 行）— 已审

- 覆盖：第 1–1,172 行，完整阅读；规划适配、table drop、point/range tombstone
  retention、payload 切分、安装及全部测试。
- 正面结论：整表删除要求单个墓碑完整覆盖、墓碑对最旧 retained reader 可见且其
  sequence **严格** 大于 table 最大 sequence，正确避开了同一 commit 内 batch
  index 不存在于 table properties 的信息缺口；部分压缩保留 point/range tombstone，
  不会错误暴露未参与的低层旧值。
- 资源问题候选：payload 构建先把全部压缩输出的 point records 保存在内存中；存在任意
  range tombstone 时又把 `target_table_bytes` 强制提升为 `u64::MAX`，导致所有 point
  records 汇入单个 chunk/table。大输入压缩可产生与输入有效数据量同阶的峰值内存和超大
  table，失去配置的 table 大小边界；待结合 planner 的输入上限和写表实现定级。
- CPU 放大候选：每个保留 record 对全部 range tombstone 做线性覆盖检查与标记，最坏
  O(records × tombstones)；大量小范围删除参与 compaction 时可造成长时间维护停顿。
- 保守但不错误：同 sequence 的 tombstone/Put 清理只按严格 sequence 删除，因此会
  暂时多保留一些已被同批次较高 batch index 删除的记录；没有看到因此丢失可见数据。

### 035 — `src/compaction.rs`（1,062 行）— 已审

- 覆盖：第 1–1,062 行，完整阅读；L0 closure、本地 seed、level score、leveled
  overlap、tombstone debt、范围计算及全部测试。
- 034 资源候选加强：planner 没有 compaction input 总 table 数或总字节硬上限。L0
  overlap closure 可吸收任意数量互相连通的 L0 table；一个宽 key-span 的 leveled
  input 也可带入下一层任意数量的 table。因此 payload 全量驻留和“墓碑时单 chunk”
  不能依赖 planner 保证有界。
- 维护缺口候选：tombstone debt 只在**紧邻下一层**存在重叠 table 时推进。若 L1
  墓碑覆盖的数据直接位于 L3 而 L2 没有重叠 table，计划被视为单表 pure move 并放弃，
  墓碑可能长期停在浅层污染读路径；属于性能/空间回收问题，不影响删除可见性。
- 性能与健壮性：多处以 `Vec::contains` 构造/扩展 table id 集合，closure 和候选评分
  在 table 多时出现 O(n²)；table byte 聚合使用普通 `sum::<u64>()`，理论溢出会在
  debug panic、release 回绕，虽真实文件总量达到该边界的可行性很低，仍宜统一饱和/
  checked 累加。
- 空 key 的 table 被当作“无 key bounds”，会保守匹配所有范围并扩大压缩输入；不会
  漏掉 table，但空 key 合法时元数据语义不够精确。

### 036 — `src/memtable.rs`（113 行）— 已审

- 覆盖：第 1–113 行，完整阅读。
- 安全结论：BTreeMap 的读写均受 `RwLock` 保护；替换同 internal key 时先减旧估值再加
  新估值，长度转换与相加饱和，未发现直接漏洞。
- 一致性观察：`estimated_bytes` 在持有 map 写锁时分两次原子更新，无锁读取者可短暂
  看到中间估值；当前它只驱动近似 flush/budget 决策，可能推迟或提前一次维护，不影响
  数据内容。锁 poison 被提升为上层结构化错误。

### 037 — `src/iterator.rs`（1,191 行）— 已审

- 覆盖：第 1–1,191 行，完整阅读；eager/lazy API、blob pin、同步/异步 heap merge、
  memtable cursor、范围边界、可见性与 tombstone 覆盖。
- 正面结论：正反向 memtable cursor 在返回每组后以 internal-key 哨兵排除整个当前
  user key；反向收集后重新按 internal-key 排序，因此仍选择最新可见版本。lazy blob
  value 持有 snapshot pin 到物化完成，能阻止引用期间回收；Put 缺值失败关闭。
- 错误状态候选：scan 没有 terminal/fused error 状态。heap 初始化中途失败时，已成功
  push 的 source entry 保留而 `source_heap_initialized` 仍为 false；调用方在收到
  `Err` 后继续 `next` 会重新 push 这些 source，产生重复 entry。迭代中途 take/push
  失败也可能让失败 source 的 entry 永久丢失，而后续调用继续返回其他 source 的行。
  因而“忽略单次错误继续迭代”可能得到重复、乱组或静默缺行；需通过 fault backend
  è¡ä¸ºéªè¯并决定 API 是否应在首错后永久 fused。
- 资源观察：heap 为每个 source 克隆当前 user key，且同 key 的所有 source records
  汇总后整体排序；table/source 数与单 key 历史版本数未在此层限界，属于预期的读放大面。

### 038 — `src/table/format.rs`（30 行）— 已审

- 覆盖：第 1–30 行，完整阅读。
- 作用与结论：table 格式子模块装配及父模块符号导入；没有独立执行逻辑或外部可见
  `unsafe`，风险由 `decode`/`io`/`primitives` 分文件追踪。

### 039 — `src/table/format/primitives.rs`（516 行）— 已审

- 覆盖：第 1–516 行，完整阅读；基础编码、游标解码、properties/blob reference、
  bound/prefix extractor 与长度保护。
- 正面结论：所有基于 offset 的固定宽度读取先 checked-add 再 `.get`；变长字段先验证
  剩余字节，count 在 `Vec::with_capacity` 前按最小条目字节约束；blob id/reference
  强制严格递增且引用 internal-key bounds 有序。未知 tag 全部失败关闭。
- 待下游复核：`read_properties` 本身不检查 smallest/largest user key、sequence 的
  交叉关系，也允许从磁盘读出 `FixedLen(0)` 或极大 prefix 长度；需确认 table 顶层打开
  校验和 prefix 使用路径会拒绝不合法元数据。
- 健壮性观察：若写入端接收接近地址空间上限的 key/value，若干 `*_encoded_len` 使用
  普通 `usize` 相加；实际公共大小上限应使其不可达，后续在 options/write batch
  验证中确认。

### 040 — `src/table/format/decode.rs`（739 行）— 已审

- 覆盖：第 1–739 行，完整阅读；properties/index/data/hash/restart/filter/range
  tombstone 解码及结构校验。
- 正面结论：所有 count 在分配前按剩余块字节和最小条目尺寸约束；hash ranges 要求
  对每条 record 恰好覆盖一次，使全量 hash 校验保持线性而非被恶意重叠放大；
  restart 必须严格递增、落在精确 record 边界且首项为 record 0；data record 最终
  强制完整 internal-key 严格升序。
- 校验缺口候选：`validate_index_partition` 验证 entry 位于 data section、相邻 entry
  无 gap、跨 entry key 有序，但自身未检查单 entry bounds，也未要求第一个 entry 从
  data section 起点开始或最后一个覆盖 section 终点/相邻 partition 的连续性。后续
  043 确认单 entry inverted bounds 会在转换为 `TableDataBlock` 时被拒绝；剩余问题是
  整个 data section 的全局覆盖没有证明。
- 范围缺口延续：磁盘 range tombstone 只解码并排序，不验证空/反向 range；这使公共
  写路径的范围合法性问题也会持久化并在恢复后继续存在。
- Filter 信任边界：false-negative 校验需要实际读取 data records；若 index filter
  在读取 block 前就排除候选，损坏但 CRC 重新匹配的 filter 是否会静默漏读，取决于
  open 模式是否全表验证，待后续路径确认。

### 041 — `src/table/metadata.rs`（1,048 行）— 已审

- 覆盖：第 1–1,048 行，完整阅读；table 写入/打开、pinned metadata、properties、
  blob references、filters、data block 构建。
- **确认 F-003**：`table_key_bounds` 只有在 range tombstone 的 start/end 都为有限
  bound 时才纳入其范围；任一端 `Unbounded` 就完全忽略该 tombstone。若同一 table
  还有点记录，properties 会得到看似精确的点记录 bounds；LSM 随后用它过滤
  `range_tombstone_tables_for_key`，导致 bounds 外、实际被半无限墓碑覆盖的旧值失去
  删除遮罩。纯墓碑 table 恰好使用 empty bounds 而保守匹配全部，问题集中在混合 table。
- 040 索引候选部分保留：lazy open 仅在实际加载 partition 时把 entry 转换为
  `TableDataBlock`，043 确认该转换会拒绝 inverted bounds；但整个 data section 首尾
  覆盖仍没有证明。浅层 pinned/filter 校验会读取全部 data block，深层 level 则延迟
  加载，因此未索引 block bytes 的发现能力不同。
- 自兼容候选：async table writer 允许 payload 直到 `u32::MAX`，但 async open 无随机
  metadata 路径，会整表读取并拒绝超过 256 MiB 的文件。加上 compaction 不限制输入且
  墓碑输出不切 table，异步存储可能成功产生之后无法重新打开的 table；待审浏览器/
  substrate 调用面后定级。
- 资源问题：writer 在编码、加 header 和 storage write 期间同时持有 `EncodedTable`
  payload 与第二份完整 `bytes`，峰值至少约为 table payload 的两倍；压缩层此前还持有
  materialized records，进一步放大大 compaction 的内存峰值。

### 042 — `src/table/format/io.rs`（934 行）— 已审

- 覆盖：第 1–934 行，完整阅读；table 全量编解码、section/footer 布局、block/index/
  filter/tombstone 写入及同步/异步随机读取。
- 正面结论：footer 要求五个 section 从 offset 0 开始首尾连续并精确抵达 footer；
  每个 block read 都检查 payload 边界、codec、checksum，data block 读取后核对 index
  首尾 internal key、完整排序、hash 与 block-local filters。
- 040/041 索引结论：这里没有额外验证 index entry 对整个 data section 的全局覆盖。
  whole-table decode 只遍历 index 指向的 block，随后用这些记录重算 properties；因此
  “未被 index 指向的合法 block bytes”也不会被纳入重算。该问题属于损坏/伪造格式的
  fail-open 校验缺口，正常 writer 生成的布局连续。
- 资源问题加强：`decode_table_from_storage_object` 先把完整 payload 读入 `Vec` 只为
  做总 checksum，然后又通过 source 逐个读取/解码各 section/block；whole-table/
  memory backend 路径因而重复持有和读取大量字节。async open 之前还把完整文件复制进
  MemoryStorageBackend，形成多份 table 内存峰值。
- 健壮性：`validate_footer_sections_by_len` 内部直接做
  `payload_len - FOOTER_LEN`，自身未防下溢；当前所有生产调用先经 footer 最小长度检查，
  所以不可由现有外部输入直接触发，但函数契约脆弱，宜改成 checked subtraction。

### 043 — `src/table.rs`（1,145 行）— 已审

- 覆盖：第 1–1,145 行，完整阅读；格式常量、metadata/data block/value view、统计
  shards、table 核心结构与子模块装配。
- 040 候选收窄：`TableDataBlock::from_index_entry` 明确拒绝
  `smallest_internal_key > largest_internal_key`；所有 lazy partition 加载最终都经该
  转换，因此单 entry inverted bounds 不会进入 block 候选查询。全局 data section
  首尾覆盖缺口仍存在。
- 正面结论：decoded record/value 所有共享切片边界在 view 构造时检查；inline value
  转为 `PointValueSource` 时再验证绝对 range，字段私有且没有 `unsafe`。统计使用
  32 个 cache-line 对齐 shard，聚合饱和，不参与数据正确性。
- 设计/健壮性：`SectionHandle::from_span` 对 `end < start` 使用 `saturating_sub`
  静默产生零长度；当前 writer 调用均传单调增长的 `Vec::len()`，不可由外部输入触发，
  但 checked subtraction 更能维持内部不变量。
- 写入排序接受相等 internal key（`<=` 快路），而磁盘 decode 要求严格递增；正常
  memtable 的 BTreeMap 会去重，仍应考虑在 writer 入口直接拒绝重复 internal key，
  避免内部调用方生成一个“成功写入、重开时报 corruption”的 table。

### 044 — `src/table/block_access.rs`（621 行）— 已审

- 覆盖：第 1–621 行，完整阅读；index/data block cache、同步/异步加载、partition
  映射及 point/range 首尾 block 查找。
- 正面结论：所有外部 block index 先与 `data_block_count`/partition bounds 核对；
  partition 的固定 128-entry 快路失败后会回退二分 metadata，不依赖 writer 固定布局；
  data block 无论是否命中 cache，最终都经 checksum、codec、index bounds 与内容校验。
- 并发结论：pinned partition 首次并发加载可能重复 I/O，但写锁 entry 合并后返回同一
  Arc；不影响正确性。cache lock 读 poison 会尝试重新加载，浅层最终写回时再结构化报错。
- 040 全局覆盖缺口延续：按 `data_block_count` 遍历的是 top-level/index 声明的 block，
  没有独立从 data section 物理布局枚举 block，所以未索引的 section 字节仍不可发现。
- 性能观察：无 block cache 的 L2+ point lookup 每次重新读/解码 index partition；
  属配置选择。错误后的 cache 行为和 singleflight 语义留到 `cache.rs` 复核。

### 045 — `src/table/read.rs`（925 行）— 已审

- 覆盖：第 1–925 行，完整阅读；range tombstone lazy cache、manifest properties、
  单/批点读、range/prefix block 选择与 filter 统计。
- F-003 调用链闭合：`key_bounds_may_contain_key` 和 `key_bounds_overlap_range` 在
  `has_key_bounds()` 为 true 时完全信任 properties bounds；混合 table 因
  `data_block_count > 0` 必定被视为有 bounds，即使将 properties 写成 empty/empty 也
  不会自动获得保守语义。修复需要显式表达 bounds 是否完整，或同时调整
  `has_key_bounds` 对“含 unbounded tombstone”的处理。
- 正面结论：manifest 只允许 level 因 trivial move 与文件 creation level 不同，其余
  properties 必须逐字段相等；point value 在同一 user key 跨 block 时会在当前 block
  没有可见版本后继续下一 block，没有把 block 边界误当作 key 边界。
- 并发/缓存：range tombstone 首次并发读取可重复 I/O，写锁下 `get_or_insert` 收敛到
  同一 Arc；锁 poison 结构化报错。同步/异步逻辑一致。
- 设计债务：单点、批量、同步、异步 block 扫描高度重复；批量路径每轮重新
  `sort_unstable_by_key` scans，在大量 key 跨多个 block 时可反复产生 O(round × k log k)
  CPU，待公共 batch 限额复核。

### 046 — `src/table/cursor.rs`（955 行）— 已审

- 覆盖：第 1–955 行，完整阅读；正反向 table cursor、block filter 决策、跨 block
  group、hash point lookup 与 restart 二分。
- 正面结论：反向遍历虽先读到同 key 的旧 internal record，但 `next_group` 会跨 block
  收齐同一 user key 并重新按 internal-key 排序，MVCC 选择仍正确；restart 搜索返回不
  晚于目标 key 的 restart，再逐 record 检查精确 bound。
- 037 错误状态候选加强：`next_group` 在已经取得 first record 后，收集同 key 的后续
  record 发生 I/O/error 时会直接返回 Err，已取出的 first/rest 不放回 pending；继续
  调用 cursor 会从错误后的状态向前走，静默丢掉这个 user-key group。顶层迭代器应在
  首错后 fused，或 cursor 必须事务式推进。
- Filter 信任边界：L2+ 的 block filter 来自 lazy index partition；false-negative
  会在 data block 加载与内容校验前直接 Skip。CRC 能处理普通损坏，但格式验证本身没有
  让 filter 在深层保持纯 advisory；是否纳入威胁模型待总体文档/存储信任说明复核。
- 同步/异步、正向/反向 block state 和 record loop 四份实现高度重复，属于高风险维护
  债务；当前逐分支对照未发现行为差异。

### 047 — `src/cache.rs`（666 行）— 已审

- 覆盖：第 1–666 行，完整阅读；128-way shard、两级优先 LRU、同步/异步加载、容量
  统计与测试。
- 正面结论：cache key 同时含 kind/table id/block index，data/index 类型错配会报
  corruption；每 shard 容量余数分配精确合计全局上限，超大 entry 插入后会被立即淘汰，
  不会绕过容量。计数溢出不参与正确性。
- 并发结论：实现不是 singleflight；miss 在锁外加载，同 key 并发请求可各自执行完整
  I/O/解码，之后仅一个值进入 cache。这样避免锁内 I/O，但热点冷启动或慢存储可形成
  thundering herd；异步路径同样如此。属于资源放大/性能问题，不会缓存半成品或错误。
- 锁 poison 策略：读/write cache state 失败时返回本次新加载值而不缓存，优先保持读取
  可用；不会吞掉底层 load 错误。LRU promotion 在 VecDeque 中线性查找，单 shard
  大量小 entry 时 hit 路径可能 O(n)。

### 048 — `src/filter.rs`（344 行）— 已审

- 覆盖：第 1–344 行，完整阅读；point/prefix Bloom 构造、磁盘 parts 校验、double
  hashing 与测试。
- 安全结论：磁盘 bit count 必须与实际 byte 长度一致，非空 filter 的 hash count
  限于 1..=30；所有索引先模 bit count 再换算 byte index。空 filter 在调用 hash
  逻辑前返回，未见除零/越界。
- 正确性：writer 即使配置 0 bits/item 也会构造至少 1 bit、1 hash 的极不精确 filter，
  只增加 false positive，不会产生 false negative。Custom/Disabled extractor 即使由
  磁盘构造成 prefix filter，查询端无法取得 filter prefix 时回退为不剪枝。
- 资源边界：bitset bytes 已作为 table 变长字段受 block/remaining-byte 上限约束；
  `from_parts` 不会按伪造 bit_count 单独分配。FNV Bloom 不是安全哈希，但 filter 不用于
  完整性或鉴权；攻击者可构造高碰撞 key 增加 I/O，属于概率索引的固有退化。

### 049 — `src/options.rs`（1,205 行）— 已审

- 覆盖：第 1–1,205 行，完整阅读；storage/durability、Db/Bucket/Write options、
  WAL shard、filter/blob 策略与测试。
- 格式自兼容候选：默认/最大 `max_key_bytes` 等于 64 MiB decoded-block 硬上限，但单条
  table data record 除 key 外还需要长度、internal-key、value tag、restart 与 hash
  index 开销。长度恰在公开上限内的 key 可通过 commit field validation，却必然使
  `BlockManager::append_checked` 的 data-block payload 超过 64 MiB，导致后续 flush
  无法写表。range tombstone block 对边界也有额外开销。待完整审
  `open_helpers`/commit validation 后升级为确认问题。
- 041/042 async 自兼容候选背景：默认 target table 64 MiB，小于 async open 的
  256 MiB 整表限制；但 target 是软切分目标而非 table/compaction 输入硬上限，因此
  不能单靠默认值排除大 table。
- 正面结论：durability strength 比较显式且单调；read-only builder 同时关闭
  create-if-missing；内容回收资格按 native/WASI/browser/object-store 分成不可互换的
  capability variant，接口层没有用一个布尔值混淆安全域。
- 待验证项：`BlobGcRatio::from_millionths` 和 `WalShardPolicy::Fixed` 构造本身不限制
  上界，Db/Bucket 的许多数值字段也公开；合法性依赖 open_helpers 集中校验，后续逐项
  对表检查，不能仅依据文档。

### 050 — `src/db/open_helpers.rs`（1,112 行）— 已审

- 覆盖：第 1–1,112 行，完整阅读；通用/只读/对象存储打开校验、WAL replay 连续性、
  bucket/table 加载、过期文件清理、blob GC replacement 与同步/异步文件删除。
- **确认 F-004**：`validate_common_options` 接受恰好 64 MiB 的 key 上限，而该大小只
  限制字段本身；落盘 data record 还必须编码长度、internal-key、value tag、restart
  和 hash-index 元数据，故合法 key 对应的 decoded block 必然越过同为 64 MiB 的
  `MAX_DECODED_BLOCK_BYTES`。`validate_bucket_options` 又只要求 `block_bytes != 0`，
  没有限制其不超过可写 block 上限；大量较小记录也可被配置聚合成无法编码的 block。
- 校验闭环：`BlobGcRatio` 在这里被限制为 `(0, 1_000_000]`，因此 049 对该字段的
  上界疑问撤销；key/value 上限、版本保留、GC 最小文件和主要 DB 数值的非零约束存在。
- WAL 恢复结论：对象存储路径对 committed coverage 强制从 replay floor 后一条开始
  严格连续，并用受检加法推进；缺洞、重复或越过 committed head 都会失败关闭。
- 删除重试候选：obsolete table/blob 删除循环在中途失败时会把整批（包括可能已删除
  的前缀）保留待重试；`remove_storage_files` 也可能先删 table 后在 blob 阶段失败。
  后续需审各 backend 的 delete-if-missing 语义，确认重试是否幂等，否则清理队列可能
  永久卡在已经不存在的文件上。
- 设计观察：`is_level_layout_compaction_error` 通过错误显示文本前缀分类是否可重试，
  对文案重构很脆弱；应使用结构化错误 variant。统计辅助函数会吞掉底层错误并返回
  零值/不完整统计，虽然不影响存储正确性，但公共 stats 调用方无法区分“确实为零”和
  “统计读取失败”。

### 051 — `src/db/sync_api/open.rs`（1,125 行）— 已审

- 覆盖：第 1–1,125 行，完整阅读；内存/native/WASI/browser/object-store 打开，
  writer lease、manifest、table、WAL 恢复与后台 worker 启动顺序。
- F-004 覆盖确认：native、内存、browser 和 object-store 打开最终都会经过
  `validate_common_options`，因此该问题是集中校验本身的边界错误，不是某个 backend
  漏调校验。
- 正面结论：native 可写打开先取进程 writer lock，再列目录和修复临时文件；对象存储
  可写打开先取 WAL-tier lease，再以 epoch claim manifest，避免旧 writer 在首次
  flush 前仍可发布。构造 `DbInner` 后才 replay，replay 失败会通过 RAII 释放租约。
- 跨进程读候选：object-store 文档承诺“单 writer + 多 reader”，read-only 打开会固定
  当前 manifest/table 集并按需读取对象；与此同时 writer 的 orphan GC 只依据自己的
  当前 manifest 判定旧 table 无引用，完全不知道其他 reader 仍持有旧 manifest。
  compaction 后立即 GC 可能删除远端 reader 尚未读取的 table，使其稳定视图变成 I/O
  错误。需在 `storage.rs` 的 orphan-GC 和 refresh 生命周期中继续闭环后定级。
- 配置设计：object-store options 默认 worker 数为 0，但字段公开；若调用方改为非零，
  `validate_runtime_options` 不拒绝，打开路径也不会调用 `start_background_workers`，
  配置被静默忽略。若该字段契约意图覆盖 object-store，应明确拒绝或实现；当前先记为
  低级易用性问题。
- native read-only 不取锁并不是未声明的数据一致性承诺：持久化文档明确限定它用于
  “稳定目录检查”，不支持与活跃 writer 的多进程协调，故不按缺陷记录。

### 052 — `src/db/sync_api.rs`（56 行）— 已审

- 覆盖：第 1–56 行，完整阅读。
- 作用：同步 API 子模块装配及共享符号导入，无执行逻辑、外部输入或 `unsafe`。
- 结论：未发现独立问题。该模块一次性把大量内部类型/函数导入所有子模块作用域，
  虽减少重复 import，却削弱每个文件的真实依赖可见性；属于维护性取舍，后续问题仍按
  实际定义与调用文件归属，不把 import 门面误计为实现证据。

### 053 — `src/db/sync_api/storage.rs`（1,210 行）— 已审

- 覆盖：第 1–1,210 行，完整阅读；各 backend 的 persist/flush/compaction、manifest
  checkout/publish/install、对象孤儿清理、close 与维护预算入口。
- **确认 F-005**：object-store flush 的注释以“close 对 object storage 是 no-op”为由
  明确不取得 publish activity guard；实际 `Db::close` 会走 `close_sync` 并同步释放
  object writer lease。native async commit 也已确认缺同一 guard。close 因而可在这些
  已接纳异步操作仍 await I/O 时返回并释放所有权，之后旧操作仍可能发布持久状态。
- **确认 F-006**：object orphan GC 只抓取当前 manifest 的 table/blob ID 集，随后立即
  删除其余对象；它既不检查本进程仍由旧 version/read/`LazyValue` 持有的对象，也没有
  跨节点 reader epoch、租约或宽限期。其“snapshot-safe”注释不成立。
- 并发正面结论：同一 Db 内，orphan GC 通过 maintenance flush guard 与 flush 和
  compaction 互斥；manifest CAS 发布也有 async mutex，且 std mutex 不跨 await。
  但这两层互斥都不覆盖普通读请求，也不覆盖其他只读节点。
- fencing 候选：table/blob 对象写入使用由 manifest 递增的普通 ID；若 object backend
  最终是无条件 PUT，则租约过期后的旧 writer 可与新 writer 选择同一未发布 ID，并在
  自身 manifest CAS 被 fencing 前覆盖新 writer 已引用的“不可变”对象。已从调用链看到
  普通 `write_table_with_backend_async`，后续完整审 `object_store.rs` 和 lease 状态机后
  定级。
- 设计观察：对象 flush 在写多个 table 中途失败会留下孤儿，由后续 GC 清理，这是合理
  的 write-before-publish 模式；manifest 已发布而本地 install 失败则关闭 DB，避免内存
  状态继续偏离持久状态。WAL rewrite 在 manifest replay floor 发布之后执行，失败只会
  暂留旧 WAL，不会丢失已确认提交。

### 054 — `src/object_store.rs`（1,824 行）— 已审

- 覆盖：第 1–1,824 行，完整阅读；key 规范化、ObjectClient 公共契约、CAS/ETag、
  reclamation capability 探针、内存实现、storage backend 适配和全部内嵌测试。
- **确认 F-007**：`ObjectStoreBackend::write_object` 对 table/blob 直接调用无条件
  `client.put`，delete 同样无 fencing；backend 不持有 writer epoch/owner。租约和
  manifest CAS 再严密，也无法阻止已过期旧 writer 在数据对象阶段覆盖或删除新 owner
  的对象。
- **确认 F-008**：`open_read` 先 HEAD，再把整个对象读入 `Arc<[u8]>`；打开数据库时又
  会打开 manifest 引用的全部 table。单对象虽有限制，总量无界。`ObjectClient::list`
  还强制一次返回 `Vec<ObjectMeta>`，令大规模 orphan GC 无法分页，形成第二条无界内存
  路径。
- F-006 影响校正：已经成功打开的 table 由 `ObjectStoreReadObject` 整体持有，所以其
  后续块读不会因远端对象删除而失败；风险集中在“manifest 已读但 table 尚未打开”的
  open/refresh 窗口，以及独立延迟读取的 blob。旧 reader/`LazyValue` 的 blob ID 仍不
  受当前 manifest reachability 保护，因此 F-006 保留。
- 正面结论：HEAD 声明长度在 GET 前按对象类型硬限制，返回长度也再次核对；offset/len
  转换与相加受检。内存 fake 的 `put_if` 在同一 mutex 临界区完成条件判断和写入，确实
  提供原子 CAS。对象 key 拒绝 NUL、父目录和非 UTF-8，并统一两种路径分隔符。
- 探针设计候选：普通 contract probe 在开始时、qualification probe 在失败清理时会
  无条件 delete 其时间戳+进程内 counter 生成的 key；若 key 预先存在，就会删除并非
  本次探针创建的对象。数据库前缀独占假设降低了意外碰撞概率，但更稳妥的实现应使用
  强随机 nonce、`IfNoneMatch`，并且只有确认本次取得所有权后才清理。
- 合约覆盖不足：`VerifyOnOpen` 只验证 put/head/get 与 put_if，不验证 backend 同样
  依赖的 range-read、幂等 delete、list 顺序/可见性；名称和文档容易让部署方误以为
  已验证完整 `ObjectClient` 契约。建议拆分 capability probe 或明确列出未覆盖项。

### 055 — `src/substrate.rs`（1,712 行）— 已审

- 覆盖：第 1–1,712 行，完整阅读；filesystem/object durability dispatch、object WAL
  worker、队列/完成通知、group commit、lease acquire/renew/release、WAL segment
  编解码、链恢复与边界验证。
- F-007 证据加强：WAL segment 已实现应有的安全模式——key 含 epoch、末序列与 SHA-256
  内容身份，写入使用 `IfNoneMatch`，已存在时只接受逐字节相同内容。table/blob 路径
  没有复用这一 helper，确认不是 ObjectClient 能力限制，而是 bulk-object 适配遗漏。
- F-007 触发面加强：worker 每 10 秒续租，但定时 `renew()` 的任何错误（包括明确
  `Fenced`）都在第 609 行被丢弃，既不终止 worker，也不通知/关闭 Db。旧 writer 的
  WAL 命令之后会在 refresh/CAS 处失败，但无 lease 检查的 table/blob/GC 仍可继续。
- F-006 扩展到 WAL：rewrite 先 CAS 新 WAL head，随后立即删除旧 chain。read-only open
  会先抓取 lease/head 快照、再加载 manifest/table、最后读取该旧 chain；并发 rewrite
  可在它读取前删掉 segment。故单写多读缺少 reader epoch 的问题不只影响 blob，也会
  让打开/refresh 在旧 WAL head 上失败。
- **确认 F-009**：object commit 外层 mutex 在 sequence 分配到同步等待 WAL 完成期间
  一直持有；worker 收到首个 Accept 后却等待 5 ms 收集更多 Accept。第二个普通 commit
  在 mutex 外无法入队，所以 group commit 实际退化为“每次单条 + 固定等待窗口”。
- 正面结论：lease 状态有 64 KiB 读取上限、owner 使用 128-bit 系统随机数、epoch/
  owner/ETag 三者联合判定 fencing；CAS 传输结果不确定时只在读回状态精确匹配时接受。
  WAL chain 验证根目录、规范 key、内容摘要、环、segment 数、累计 1 GiB 与严格连续
  sequence，未见路径逃逸或解码前无界单字段分配。
- 资源候选：object `DurabilityMode::Buffered` 把完整 WAL frame 放在无上限
  `Vec` 中；在调用 persist/flush 或更强写入前可随成功提交持续增长，未与
  `write_buffer_bytes` 或 backpressure 联动。需结合公开 durability 契约决定是否单列。
- 低级错误语义：多条 group 若失败会把原始 `Fenced`/`Corruption` 统一包装成 `Io`；
  当前顺序锁使普通 commit 几乎不会形成多条 group，但该分支一旦被性能修复启用就会
  丢失调用方必须识别的 fencing 语义。

### 056 — `src/substrate/lease_state.rs`（258 行）— 已审

- 覆盖：第 1–258 行，完整阅读；lease HEAD/范围读取、legacy/v2/v3 解码、v3 编码、
  时间换算和无 runtime 的 future driver。
- **确认 F-010**：读取端先把 lease object 限制为 64 KiB；写入端
  `encode_lease_state` 却只把 `current_wal_key` 限到 `u32::MAX`。object-store prefix
  又无长度上限，WAL key 会完整重复 prefix。首次 commit 可以发布一个超过 64 KiB 的
  lease，随后代码会拒绝读取自己刚写的状态。
- 正面结论：所有固定字段切片都由精确长度或 fallible conversion 保护；WAL key 的
  offset 使用受检加法并要求恰好消费剩余字节，没有 trailing-data 歧义。legacy
  8/16/20-byte 状态的 owner/expiry 置零，使下次 acquisition 必须升级 epoch。
- 合约依赖：HEAD 后的 `get_range` 没核对实际返回长度是否等于 `meta.size`，完全依赖
  ObjectClient 契约；而 F-008 所述 VerifyOnOpen 不覆盖 range read。正常 provider 下
  解码的精确末尾检查可发现多数短/长返回，但错误会被归类为 lease corruption，而不是
  明确的 backend contract violation。

### 057 — `src/substrate/tests.rs`（544 行）— 已审

- 覆盖：第 1–544 行，完整阅读；filesystem substrate、object WAL CAS 歧义结果、
  chain 根目录/sequence、lane batching、orphan WAL segment 与 lease takeover 测试。
- 正面覆盖：测试确认 WAL immutable segment 从不走 overwrite PUT；条件写响应丢失后
  会读回精确状态；跨数据库 predecessor、sequence hole、未发布 orphan segment 和
  live/expired lease 基本状态均有验证。
- F-009 测试缺口：`object_wal_lane_group_commits_queued_accepts` 直接调用内部
  `lane.send` 预先排入 8 条，绕过真实 Db commit 必须持有的
  `object_wal_commit_order`。它只证明 worker 能批处理，不证明集成路径能形成 batch，
  因而反而掩盖了生产路径固定等待 5 ms 的问题。
- 关键未覆盖：没有真实 Db 层 lease takeover 后 stale table/blob PUT/delete、续租
  `Fenced` 的状态传播、object maintenance 与 close、并发 read-only open 与 WAL
  rewrite、超长 prefix lease、Buffered WAL 内存增长等测试。
- 测试自身未发现会污染仓库或依赖不稳定全局状态的问题；临时目录含 PID+纳秒并在
  结束时清理。测试 helper 假定 in-memory future 首次 poll 即 Ready，仅用于对应 fake。

### 058 — `src/s3.rs`（1,259 行）— 已审

- 覆盖：第 1–1,259 行，完整阅读；`object_store` crate 适配、S3 builder、条件写、
  GET/range/HEAD/list/delete、内存适配测试与 ignored 的真实 R2 测量套件。
- **确认 F-011**：Trine 的 canonical object key 可保留前导 `/` 和未编码字符；
  `object_store::path::Path::from` 会丢弃空/前导组件并 percent-encode PathPart。put/get
  使用同一转换所以可工作，但 list 返回 `meta.location` 的转换后 key，和 Trine 原
  root 不再相等，破坏 direct-child 筛选与清理；不同逻辑 prefix 还可物理别名。
- F-008 加强：适配器确实将 list stream `try_collect` 成 Vec；为避免 OOM 加了
  100,000 条硬上限，但超过上限只返回 `RuntimeBusy`，没有 continuation token。也就是
  大 namespace 不再无界分配，却会让所有依赖 list 的 GC/WAL 清理永久无法推进。
- F-009 加强：ignored R2 suite 断言 12 个并发 `Db::put_sync` 只产生 1 WAL PUT +
  1 head CAS；但真实 Db object commit 在 WAL accept 期间持顺序锁。该测试只有在外部
  凭据和显式 `--ignored` 时运行，默认 CI 不会发现当前集成路径与断言冲突。
- 依赖契约偏差：当前锁定的 `object_store` 文档明确写着 list 返回顺序不保证；适配器
  注释却声称其按字典序返回且没有排序，违反 Trine `ObjectClient::list` 契约。内部
  `StorageObjectListBackend` 最终会再次排序，降低当前核心路径影响，但公共 adapter
  本身仍不合约。
- 正面结论：S3 条件写正确映射到 Create/Update(ETag)，CAS 冲突与普通 I/O 分开；成功
  PUT 必须从响应本身取得 ETag，不用有竞态的后续 HEAD 代替。delete 把 NotFound
  归为成功，HTTP 默认禁用，显式 opt-in 才允许非 TLS endpoint。
- 资源候选：`get`/`put` 都整对象聚合/复制；WAL chain 读取直接走 `get`，是在下载后才
  检查 128 MiB segment 上限。被篡改的超大 WAL/manifest 对象可能在边界校验前占用
  大量内存；后续审 manifest provider 路径后统一定级。

### 059 — `src/manifest.rs`（600 行）— 已审

- 覆盖：第 1–600 行，完整阅读；manifest 状态、object manifest 的 HEAD/GET/CAS、
  writer epoch、bucket/checkpoint/table 编辑及冲突重放。
- 高风险候选：`ManifestState::next_table_id` 每次只取**当前仍在 manifest 中**的最大
  table id 再加一，没有持久化高水位或原子 reservation；`add_tables` 又只检查目标
  bucket 内是否重号，不检查全 manifest。并发 flush/compaction 在真正发布前各自取号，
  或删除持有最高 id 的 bucket 后，均可能再次分配同一全局对象/文件名。需沿维护调度、
  native obsolete queue 和 manifest store 的发布锁确认完整可达链。
- 可用性候选：object CAS 写返回传输错误后，只要 readback 不是预期的新状态，
  `try_publish` 一律改报 `Conflict`；即使 readback 仍是原 base、足以证明写入没有生效，
  也会进入 `commit_edit` 的无上限立即重试。在“读权限正常、写端持续故障”时可能形成
  不退避的网络/CPU 忙循环并永不把真实 I/O 错误交给调用方。
- 幂等候选：注释声称模糊响应后的 edit 重放是幂等的，但 checkpoint create/delete
  的 closure 分别在已存在/已删除时返回语义错误。如果本次发布其实成功、随后状态又被
  推进，readback 不再与精确 `next_state` 相等，重放可能把已经成功的操作报告为
  `CheckpointAlreadyExists`/`CheckpointNotFound`；还可能先于下一次 epoch fencing
  检查返回错误。需结合单 writer/接管时序定级。
- 资源候选：manifest 先以 HEAD 校验 64 MiB 上限，再调用整对象 `get`；若对象在两次
  请求之间被换成更大版本，适配器会先完整下载，之后才再次检查长度。上限保护存在
  TOCTOU，需与 object client 的 range/条件 GET 能力统一整改。
- 正面结论：持久格式有 magic/version/长度/尾随字节检查；未知版本失败关闭；publish
  前检查 held writer epoch，CAS 成功后保留实际 ETag；table replacement 对“输入全在”
  和“输出已全部存在”的状态有明确幂等判定。

### 060 — `src/manifest/store.rs`（764 行）— 已审

- 覆盖：第 1–764 行，完整阅读；native/browser/object manifest 打开、同步和异步编辑、
  prepared publish、table replacement、blob 删除队列及内存状态安装。
- **确认 F-012**：同步/native 的 `add_tables` 与 replacement 不做全局 table id
  唯一性检查，object 实现也只检查同一 bucket；所有后端仍从当前 live manifest
  推导下一 id。结合已审维护协调器允许不同 bucket 的 compaction 同时 active，两个
  builder 可在发布前取得同一 id 并写同一路径/对象，随后 manifest 甚至允许两个
  bucket 同时引用它。drop 最高 id bucket 后也能重用仍在 obsolete queue 的 id。
- F-012 的 native 后果更严重：旧 `Arc<Table>` 使 obsolete queue 暂缓删除，但新表
  已可重用同名路径；旧引用释放后，清理仅按旧对象的 `properties.id` 计算路径并删除，
  会把当前 manifest 正在引用的新表一并删掉。object 后端则可能让两个逻辑 table
  互相覆盖或让旧 reader 观察到同名新内容。
- `commit_edit_async` 证实 object CAS conflict 是无次数、无退避的裸循环，因此 059
  中“持续写故障被误判为 conflict 后忙重试”的候选成立于该层；待在 manifest tests
  中检查现有故障客户端覆盖后定级。
- 设计问题：同步/async/prepared 三套 manifest mutation 基本复制，且约束并不一致。
  object `add_tables` 有重复输出判断，native/browser 直接 push；相同不变量需要在每套
  路径分别维护，已实际漏掉全局 id 唯一性和 replay floor 单调性。
- 正面结论：durable publish 成功前不会推进内存 state；prepared publish 安装时验证
  base state 未变化，避免 await 期间静默覆盖本地新状态；多 bucket replacement 先
  完整校验输入，再构造下一状态，不会主动发布部分 batch。

### 061 — `src/manifest/format.rs`（842 行）— 已审

- 覆盖：第 1–842 行，完整阅读；manifest header/checksum、BucketOptions、tables、
  blob references、checkpoints、writer epoch 的二进制编解码与 cursor 边界。
- F-012 加强：解码器也没有验证 table id 在同一 bucket 或全 manifest 唯一；重复
  table bucket/bucket name 会被 `BTreeMap::insert` 静默覆盖。格式边界因此没有恢复
  核心元数据不变量，重号状态可成功解码并继续进入打开流程。
- 完整性缺口：解码不要求 bucket/table-bucket 名称严格递增且唯一，不检查两张 map
  的 key 集合一致，也不拒绝空/非法/保留 namespace 的 bucket name。未知 table bucket
  会被打开逻辑忽略，却仍被 referenced-id 集合视为 live，形成不可访问且不可回收的
  ghost tables。应在 `decode_state` 后统一验证规范化 manifest 不变量。
- 正面结论：payload 上限、header 精确长度、CRC、magic/version、尾随字节均检查；
  所有 byte field 通过受检 offset；table/blob/checkpoint count 在 `Vec::with_capacity`
  前按剩余 payload 的最小编码尺寸约束，避免伪造大 count 直接触发巨额分配。
- 二次审计处置：`IndexSearchPolicy::Auto` 只接受当前 tag 4，tag 2/3 不再归一；
  manifest 布局提升为精确 version 2，其他 version 直接拒绝。

### 062 — `src/manifest/tests.rs`（535 行）— 已审

- 覆盖：第 1–535 行，完整阅读；长度加固、filter curve 往返、publish 失败原子性、
  object CAS、冲突 rebase、epoch fencing 与 object sync API 拒绝。
- **确认 F-013**：现有模糊结果测试只模拟“CAS 已落盘、响应丢失”，并验证精确 readback
  被识别为成功；没有“CAS 根本未落盘、写返回 I/O 错误而读仍成功”用例。结合 059/060
  的控制流，后一种情况会被误报为 conflict 并在公共 edit 中无限立即重试。
- F-012 测试缺口：没有断言 table id 全 manifest 唯一、drop 后 id 不重用或不同
  bucket 并发 builder 获得互斥号段；manifest mutation 的现有测试集中在 bucket 和
  CAS，未覆盖最核心的 table 命名不变量。
- 格式测试缺口：只测试一个伪造 table count 和 payload 长度，没有重复 bucket/table
  map key、跨 bucket 重复 table id、未知 bucket table list、非法 table level/sequence
  bounds 等结构不变量。
- 测试卫生：两个成功的 native manifest round-trip 测试创建唯一临时目录后不清理，
  会在长期/频繁 CI 运行中积累 `/tmp/trine-kv-manifest-*`；宜使用 RAII tempdir。
- 正面结论：测试明确验证 durable publish 失败不会推进内存 state，CAS loser 会刷新
  winning state 后重放 edit，且更高 writer epoch 会阻止旧 manifest writer。

### 063 — `src/db/sync_api/maintenance.rs`（18 行）— 已审

- 覆盖：第 1–18 行，完整阅读。
- 作用：同步 API 维护子模块的共享导入和 `background`/`compaction`/`flush` 模块声明。
- 结论：无执行逻辑、`unsafe` 或错误处理；未发现独立问题。集中式 `super` 导入较宽，
  会让子文件的真实依赖不够直观，但目前不构成功能风险。

### 064 — `src/db/sync_api/blob_cleanup.rs`（17 行）— 已审

- 覆盖：第 1–17 行，完整阅读。
- 作用：blob GC/发布清理子模块的共享导入和模块声明。
- 结论：无执行逻辑或独立安全问题；与 maintenance 根模块相同，过宽的聚合导入降低
  依赖可读性，后续问题归入实际的 `gc.rs`/`publish_cleanup.rs` 调用路径。

### 065 — `src/db/sync_api/stats_helpers.rs`（61 行）— 已审

- 覆盖：第 1–61 行，完整阅读；按 compaction trigger 聚合输入/输出 table 数量与字节。
- 低风险算术不一致：最终字段使用 `saturating_add`，table 数也做饱和换算，但每批
  `estimated_file_bytes()` 先用普通 `sum::<u64>()`。极端累计量在 debug 会 panic、
  release 会回绕，再把错误小值饱和相加；与函数主动选择的饱和统计语义不一致。建议
  内层也用 `fold(0_u64, u64::saturating_add)`。
- 影响限于遥测准确性；在现实存储容量下很难触达，不单列正式问题。其余聚合按枚举
  `BTreeMap` 确定性输出，未见越界、共享状态或生命周期风险。

### 066 — `src/db/sync_api/buckets.rs`（933 行）— 已审

- 覆盖：第 1–933 行，完整阅读；三类后端的 bucket 创建/删除、default bucket
  point/batch/range/prefix 同步便捷 API 及 registry compare-and-remove。
- F-002 语义补充：公开 drop 文档明确承诺旧 `Bucket` 句柄和 snapshot 在释放前继续
  工作，因此点读保留旧 `Arc<LsmTree>` 不是偶然；真正的严重缺陷是 `Bucket` 的 point
  和 range/prefix 实现没有共同遵守这一承诺，后者按名称回查新 generation，导致同一
  旧句柄混读两个 bucket 代。修复时要先明确选择“旧代句柄可读”或“drop 后统一失效”，
  不能维持当前分裂状态。
- F-006 加强：object drop 文档称立即 orphan GC 是 “snapshot-safe”，但实现只按当前
  manifest 标记；旧本地 handle 虽持有内存 table bytes，blob-backed value 仍需按需
  GET 已被 GC 删除的 blob。即使不考虑远端 reader，当前进程中被文档保证继续工作的
  旧句柄也可能在 drop 返回后读取失败。
- F-001 加强：default bucket 的 `Db::get_at_sync` 同样只提取 foreign snapshot 的
  sequence/`is_pinned`，无 lineage 校验；range/prefix snapshot API 也不校验来源。
- 持久安装候选：native/object/browser drop 都在 durable manifest 删除后才修改 bucket
  registry；若后一步因锁 poison/指针不符失败，这几条路径没有像 bucket creation 那样
  调用 `close_after_durable_publish_error`，可能让仍 open 的 Db 保留与磁盘相反的
  dropping tree。触发依赖内部 panic/状态破坏，暂不单列。
- 正面结论：bucket drop 在关闭该 tree 的写 admission 并等待已接纳写入后才 flush；
  registry 删除使用 `Arc::ptr_eq` 防止误删同名替换；对象/浏览器创建在取得 manifest
  串行锁后再次检查 registry，避免典型 check-then-create 竞争。

### 067 — `src/db/sync_api/metadata.rs`（641 行）— 已审

- 覆盖：第 1–641 行，完整阅读；snapshot/checkpoint、transaction 入口、统计聚合、
  retention floor、close 与 open-state 检查。
- **确认 F-014**：snapshot/checkpoint admission 与 compaction 的 retention snapshot
  之间没有同步。压实先取得 `oldest_retained_sequence` 并据此构建；之后创建的
  snapshot 虽成功 pin 一个当时仍被 API 判为 retained 的旧 sequence，也不能让在途
  压实重新检查/中止。发布后该 snapshot 所需版本可被永久丢弃。
- F-014 的额外窗口：`snapshot()` 先原子读取 visible sequence，后在另一把 mutex 中
  pin；两步间可有新 commit 和压实取 floor。`create_checkpoint_sync` 甚至在读取 latest
  后直接写 metadata，没有像 `create_checkpoint_at_*` 那样先建立临时 pin；但后者仍
  受“压实已经取过 floor”的在途窗口影响。
- F-001 扩展：`snapshot_at`/`create_checkpoint_at_*` 接受只含数字的 `ReadVersion`，
  仅按本 DB 的 latest/floor 比较，无法执行文档要求的“来自同一 database lineage”；
  外部 DB 的版本可被错误接受并在目标 DB 建立 durable checkpoint。
- F-005 扩展：checkpoint create/delete（以及上一文件的 bucket metadata publish）
  在 `ensure_open` 后没有取得 publish-barrier activity token；它们可与 close 竞争并
  在 lease 释放后继续发布，问题不限于数据 commit 和 maintenance。
- 观测性问题：`stats()` 对 bucket/version/统计 mutex 锁错误多处选择返回部分数据或
  静默跳过，没有 incomplete 标志；表计数及 `live_blob_bytes.sum::<u64>()` 又未统一
  饱和。不会直接改变数据，但可能在故障处置时给出看似完整的错误健康状态。
- 生命周期观察：`snapshot()`/`transaction()` 不调用 `ensure_open` 且签名不能报错；
  close 后仍可建立无实际读取能力的对象或 retention pin。建议要么使构造返回
  `Result`，要么文档明确 closed handle 的纯本地对象语义。

### 068 — `src/recovery.rs`（1,084 行）— 已审

- 覆盖：第 1–1,084 行，完整阅读；writer process lock、安全临时文件修复、孤立/缺失
  table/blob 检查、blob properties 交叉验证及 recovery report 编解码。
- I/O 错误分类问题：同步 `storage_object_exists_with_backend` 把 capability 缺失和
  `open_read_blocking` 的所有错误都折叠成 `false`；权限、句柄耗尽、瞬时 I/O 故障会
  被上层误报为“referenced blob missing”的 corruption。异步实现会传播原错误，两条
  路径契约不一致。建议返回 `Result<bool>`，只把明确 NotFound 映射为不存在。
- 资源候选：异步 missing-blob 检查为确认存在性调用 `read_object_bytes`，完整物化每个
  blob 后立刻丢弃；随后 invalid-blob 检查又完整读取同一批文件。browser open 还会在
  inline blob 阶段再次取值。单文件虽有上限，大量合法 blob 会造成数倍启动 I/O 与高
  峰值内存；结合 `blob/io.rs` 后决定是否单列。
- 格式/报告低风险问题：recovery report 无文件大小/条目数上限，公开读取 API 会整文件
  载入；Unix 文件名可含换行，而 `is_safe_temporary_file` 接受任意 `table-*.tmp`/
  `blob-*.tmp`，报告的行式编码没有转义，修复可能写出自身无法再次解码的报告。
- 后端防御观察：基于预取 directory entries 的修复只按 basename 判为“安全临时文件”，
  未再次断言 path 是 `db_path` 的直接子项。当前内置 native/browser listing 可约束
  来源；若将这些 storage traits 公开给不可信自定义后端，应在删除前做根目录/对象身份
  验证，避免后端返回越界路径让数据库代为删除。
- 正面结论：默认 fail-closed 不自动删除正式 table/blob；修复只识别已知 atomic-write
  临时命名且需显式策略。blob reference 会核对 file-id 集合、引用字节上限和 internal
  key span；报告 native 发布按 durability 要求同步父目录。

### 069 — `src/db/sync_api/maintenance/compaction.rs`（743 行）— 已审

- 覆盖：第 1–743 行，完整阅读；压实规划/预约、sync/native async/browser 构建、
  目录同步、manifest 发布、LSM 安装、obsolete retirement 与 blob GC 串联。
- F-014 直接确认：`oldest_retained_sequence` 只在 `prepare_compaction_run` 开头采样；
  构建后发布前的 `validate_compacted_tables` 只验证输入 table 仍 current，不重新验证
  retention generation/floor，新加入的低 sequence pin 不会阻止发布。
- F-012 直接确认：每个并发 run 都在 builder 内独立调用 `self.next_table_id()`；维护
  预约只让**同一 bucket**互斥，不同 bucket 可并行。失败清理还会按本任务认为自己写过
  的 id 删除文件，在重号时可能删除另一任务已经成功发布的表。
- **确认 F-015**：native manifest publish 的底层顺序是写/同步 temp → rename →
  父目录 sync；父目录 sync 可以在 rename 已经改变当前 namespace 后返回错误。压实把
  所有 publish error 都当成“未发布”，删除刚写的 output tables 并保留旧内存 state，
  会使磁盘 manifest 引用已删除表。async native/platform 路径有同类模糊完成边界。
- 生命周期正面结论：host async 压实在 durable publish 前取得 owned activity token；
  close 能等待已经进入发布阶段的任务。构建期间若 close，后续 admission 失败并清理
  尚未发布的输出。object-store 独立路径的缺口仍归 F-005。
- 设计观察：sync/native async/browser 三套 builder 有大段逐行复制，清理和 durability
  差异难以统一验证；F-012/F-015 这类跨后端不变量应下沉为单一“预留 id + 写输出 +
  可判定发布结果”的状态机。

### 070 — `src/db/sync_api/maintenance/flush.rs`（731 行）— 已审

- 覆盖：第 1–731 行，完整阅读；bucket lookup、write pressure、memtable freeze、
  sync/host async flush 构建、manifest publish、WAL rewrite 与预算采集。
- **确认 F-016**：flush 以本轮选中输入的最大 `freeze_sequence` 作为全数据库
  `wal_replay_floor`。后台/压力 flush 只会选有 immutable 的 bucket，压力路径甚至跳过
  未达到阈值的 bucket；其他 bucket 的 active/immutable memtable 可仍含更小 sequence。
  manifest 发布和 WAL rewrite 随后把这些尚未落表的记录当成已持久化并越过。
- F-015 直接确认：sync 和 async flush 都在任意 manifest publish error 后删除全部新
  table；native rename 已生效、父目录 sync 失败这一分支会立刻制造“manifest 引用的
  table 被删除”的损坏状态。
- F-012 加强：一次 flush guard 内部会给所选 bucket 顺序发号，因此单轮互斥 flush
  内不会自撞；但 allocator 仍不持久化 reservation，预算中被构造后丢弃的 input 号段、
  bucket drop 和并行 compaction 均可让 id 重用。
- F-005 扩展：`persist_bucket_creation` 直接持 manifest mutex 发布，没有 activity
  token；再次确认 metadata 变更未纳入 close barrier。
- 正面结论：公开完整 flush 在 `memtable_publish_lock` 下捕获 sequence 并冻结所有
  active memtable；table 文件先按目标 durability 写完并同步目录，再发布 manifest；
  durable publish 后安装内存 table，安装失败会强制关闭句柄。

### 071 — `src/db/sync_api/blob_cleanup/gc.rs`（811 行）— 已审

- 覆盖：第 1–811 行，完整阅读；GC candidate 选择、rewrite plan、同步/native async/
  browser blob 与 replacement table 构建、预约、发布、安装、删除和统计。
- F-012 加强：GC 用当前 `next_table_id` 同时派生新 blob id 和 replacement table ids，
  但在 plan 阶段没有 durable reservation；与另一 bucket compaction 并行时可写同名
  table/blob。失败清理按这些裸 id 同时删 table 与 blob，会放大重号后的破坏。
- F-014 加强：plan 在取得 compaction reservation **之前**读取 current tables/blob；
  预约后只验证输入 table id 仍存在。旧 snapshot admission 与 pending blob 删除之间
  也没有 generation 边界；删除侧仅瞬时查看 active snapshot 数，不能保护恰好在检查后
  建立、但被在途 rewrite 越过的 snapshot。
- F-015 加强：blob GC 和普通 compaction 一样，在 manifest publish 任意报错后删除
  新 blob + replacement tables；rename 已生效但目录 sync 失败会留下引用被删对象的
  manifest。
- 性能问题：async GC candidate 读取 `BlobFileProperties` 实际调用整文件
  `read_blob_file_with_backend_async`；随后 rewrite 又逐 record range-read 候选文件。
  与同步版只读 header/footer/properties 的实现不对称，大 blob 扫描会产生不必要的
  全量 I/O/分配。待 `blob/io.rs` 审计后统一定级。
- 正面结论：candidate/live-byte 算术使用饱和运算；rewrite 后再次确认所有 input table
  仍 current；同一 GC 跨 bucket 取得全 bucket reservation；candidate blob 只有在
  replacement manifest durable 且 table 安装后才进入延迟删除。

### 072 — `src/db/sync_api/blob_cleanup/publish_cleanup.rs`（786 行）— 已审

- 覆盖：第 1–786 行，完整阅读；manifest publish/本地安装、WAL rewrite、blob 引用
  统计、pending blob/table 删除、obsolete queue 与 compaction 遥测。
- F-012 直接后果：obsolete table queue 的安全性证明只覆盖“该 id 永不重用”。它以
  `Arc::strong_count == 1` 判断旧 table 无 reader 后，再按裸 id 重建路径删除；重号时
  old `Arc` 的引用计数与同名新 table 毫无关系，因而会安全地判断错对象并删除当前文件。
  `obsolete_blob_ids_for_compaction` 也把 id 当全局唯一，重号会错误跳过跨 bucket 引用。
- F-014 加强：pending blob 删除只做一次 `snapshots.active_count() == 0` 检查，没有与
  snapshot admission 同锁保持到删除完成，也不使用已记录的
  `pending_deletion_sequence`。通常新 snapshot 只读 rewrite 后对象，但被在途 compaction
  越过的旧 snapshot 可在计数检查后加入并立刻失去旧 blob。
- F-015 扩展：prepared manifest clear/publish 仍采用二值成功模型；对删除 metadata
  而言通常可幂等重试，但对 flush/compaction 的 output cleanup 不能如此。该文件的
  `close_after_durable_publish_error` 只处理“明确 durable publish 后本地安装失败”，
  不能识别“底层已 rename、却以 I/O error 返回”的模糊发布结果。
- 健壮性问题：`install_flushed_tables` 用 `inputs.iter().zip(tables)`，只用
  `debug_assert` 检查 bucket，没有在 release 验证数量和顺序。当前内部 builder 维持
  等长，但 durable manifest 已发布后的安装函数应主动失败关闭，而不是依赖调用者不变量。
- 删除重试要求：批量 blob/table 删除中途失败会把未处理项重排队，但已成功删除项仍在
  manifest pending 集合，后续必须依赖 delete-not-found 幂等。内置 native/object
  adapter 满足；storage trait 的契约应明确写出该要求并用 capability test 验证。
- 统计低风险问题：若干全局 atomic 用 `fetch_add` 回绕，而 level/trigger 明细使用
  `saturating_add`；输入/输出字节又先普通 `sum::<u64>()`，与饱和统计设计不一致。

### 073 — `src/db/async_api.rs`（1,003 行）— 已审

- 覆盖：第 1–1,003 行，完整阅读；async open/refresh、bucket/checkpoint、默认 bucket
  读写/扫描、persist/flush/compaction/maintenance 与 async close 路由。
- F-006 加强：read-only object refresh 重建并整体替换 bucket registry，但旧 snapshot
  不持有旧 database read-state；刷新后的 snapshot 读会按名称进入新 tree，而旧远端
  table/blob 可能已由 writer GC。旧 iterator 可持 table bytes，lazy blob 仍依赖已删
  对象，无法达到文档暗示的稳定点时视图。
- F-014 加强：async `create_checkpoint` 直接捕获
  `last_committed_sequence` 后发布，没有临时 snapshot pin；所有 async snapshot 参数
  入口也只提取 sequence。压实规划/发布窗口在 async 路径同样存在。
- F-005 加强：object/browser checkpoint create/delete、bucket creation，以及 object
  flush/compaction/GC 都从这里直接进入异步发布，但 API 入口没有统一 activity admission；
  close barrier 的覆盖取决于下游各自记得登记，当前已经存在漏项。
- 部分安装候选：`refresh_object_store` 依次替换 bucket registry、安装 manifest handle、
  reset commit tracker，中间任一步锁错误都会返回但不关闭 Db，也没有事务式回滚；句柄
  可能保持“新 buckets + 旧 manifest/visible sequence”。触发主要依赖内部 mutex poison，
  暂不单列。
- API/状态候选：object `persist` 分支不先 `ensure_open`，而 native/sync 路径的检查在
  更下层；closed handle 的跨后端结果可能不一致。`checkpoint_read_version` 等若干纯同步
  包装仍声明 `async`，crate 级 `unused_async` 掩盖了不必要的 Future/调度开销。
- 正面结论：object refresh 用独立 async lock 串行；在发布给读者前完整读取 manifest、
  tables 和已确认 WAL，并验证从 replay floor 到 lease committed head 的 sequence
  连续覆盖。object maintenance 正确拒绝 read-only handle。

### 074 — `src/blob.rs`（143 行）— 已审

- 覆盖：第 1–143 行，完整阅读；blob 常量、索引/header/record/properties/value-ref
  数据模型、文件命名和子模块边界。
- 结论：本文件无解码、I/O 或共享状态逻辑，未发现独立漏洞；`ValueRef::len` 对 inline
  使用 `usize as u64`，在现有 32/64 位目标安全，但若追求非常规超 64 位 `usize` 的
  可移植性可改为受检/饱和转换。
- 设计观察：blob file id 是裸 `u64`，table id 是独立 wrapper，但实际写入/清理让两者
  共享数字分配规则；类型系统没有表达“同一次 table/blob publication generation”，
  使 F-012 的重号问题更难在编译期或 API 边界阻断。

### 075 — `src/blob/listing.rs`（74 行）— 已审

- 覆盖：第 1–74 行，完整阅读；sync/async blob listing 与文件名到 id 的解析。
- 规范化候选：parser 接受任意可被 `u64::parse` 解析的数字宽度，并对扩展名大小写
  不敏感；`blob-1.trineb`、`blob-00000000000000000001.trineb`（以及大小写扩展）在
  大小写敏感文件系统可同时存在，却都折叠为 id 1。recovery/GC 后续只按 canonical
  `blob_path` 删除或判断引用，别名文件可逃过 orphan 检查并永久遗留。table listing
  有同型逻辑，后续统一定级。
- 建议解析成功后要求 `path.file_name()` 与 `blob_path(Path::new(\"\"), id)` 的 filename
  精确相等；任何看似 Trine blob 但不规范的名称应作为 corruption 报告，而不是折叠。
- 其余路径通过 capability 检查，聚合到 `BTreeSet` 后确定性返回；无算术或内存安全问题。

### 076 — `src/blob/io.rs`（331 行）— 已审

- 覆盖：第 1–331 行，完整阅读；sync/async blob properties/full-file 读取、编码写入、
  random-read handle、长度换算和错误映射。
- 资源候选确认到实现层：同步 properties 路径只读 header/footer/properties；同名
  async 函数却调用 full-file read + full decode。native async/browser GC 对每个 live
  blob 仅为了 `encoded_bytes` 就物化整个文件；browser recovery 还会在 existence、
  validation、inline-value 阶段重复全读。读完整文件后 decoder 又为 record value 建立
  owned 数据，峰值可能显著高于单文件硬上限。审完 codec 的精确上限后定级。
- 错误语义问题：`blob_read_error` 把底层所有 `Error`（包括权限、瞬时 I/O、runtime
  busy/capability 问题）统一包装成 `Corruption`。调用方无法区分“持久 bytes 损坏”和
  “当前无法读取”，可能错误触发 fail-closed/告警/运维动作。只应把已验证的格式、
  checksum、越界问题标为 corruption，保留原始 I/O 分类和 source。
- F-007 加强：async object backend 写 blob 只调用通用 `write_object`，本层不附加
  generation、epoch、create-only precondition 或内容身份；stale writer 的同名覆盖在
  blob 路径同样成立。
- 正面结论：whole-file 读取先受格式上限并做 `u64 → usize` 检查；metadata sync 路径
  验证最小长度、header id、footer 和 properties bounds 后才分配；写入前校验 header
  file id 与目标路径 id 一致。

### 077 — `src/blob/codec.rs`（860 行）— 已审

- 覆盖：第 1–860 行，完整阅读；整文件/按 index 编解码、header/footer/properties、
  record checksum、LZ4、长度上限、offset 换算和 cursor。
- **确认 F-017**：256 MiB whole-file 上限只约束编码 bytes；full decoder 对每条 record
  分别允许 64 MiB 解压值，并把所有 `Vec<u8>` 同时保存在返回的 `BlobFile.records`，
  没有累计 decoded-value budget。高压缩比的合法/校验和正确文件可在完整校验时膨胀
  到数十 GiB。
- F-017 生产侧加强：blob GC 把全部 candidate live records 汇成一个新 blob；
  `encode_blob_file` 只在所有 record 编码完后检查 256 MiB 文件上限，并额外 clone 每条
  原始 value 到 `indexed_records` 仅为计算 properties。可压缩数据能生成“编码合法但
  reopen 全解码峰值巨大”的文件；不可压缩超限数据也会先消耗巨量内存才失败。
- 格式严格性缺口：decoder 允许 `properties_end < footer_start`，两者之间的任意 gap
  完全忽略且不受 record/properties checksum 覆盖。文件可携带大量未校验尾部 junk 仍
  被接受；应要求 properties block 精确紧邻 footer。
- 排序规范：record order 只拒绝下降，不拒绝相等 internal key；若格式要求一个
  internal key 唯一，应使用严格递增并让 encoder/decoder一致验证。当前 index 含 offset，
  相等记录未直接造成越界，暂作格式候选。
- 正面结论：单 record frame/body、单 decoded value、properties 和整编码文件均有
  上限；offset/length 加法和平台换算受检；读取会交叉验证 header file id、record
  checksum、value checksum、完整 index metadata 及期望 internal key。

### 078 — `src/blob/values.rs`（404 行）— 已审

- 覆盖：第 1–404 行，完整阅读；大值分离、同步/异步 blob inline、按 index/legacy
  raw reference 读取和同步 handle cache。
- **确认 F-018**：sync inline 对同一 file id 缓存 open handle，async 版本却对每条
  blob-backed record 独立 `open_blob_read_object_with_backend_async`。object-store 的
  open handle 会整对象 GET 到内存，因此同一 blob 中 R 条记录在 reopen/refresh 时会
  把完整 blob 下载 R 次，复杂度和传输成本接近 `R × blob_size`。
- F-017 写侧加强：large-value 分离先把所有大 inline value clone 到 `blob_records`；
  encoder 又 clone 到 indexed records，并同时构建 encoded file。单次 flush 的峰值可
  包含原 table records、多份未压缩 value 和完整编码 blob，且超限只在末尾发现。
- 二次审计已根治：删除不含 blob header identity 与 internal-key 绑定的 raw
  `ValueRef` variant；table tag 2 现在直接失败关闭，当前格式只接受完整 `BlobIndex`。
- 正面结论：单 value 长度在分配前受 64 MiB 限制，indexed 读取验证 frame/record/value
  checksum 与完整 index metadata；同步 inline cache 在函数生命周期内按 file id 复用
  handle，不会重复打开同一 blob。

### 079 — `src/blob/tests.rs`（580 行）— 已审

- 覆盖：第 1–580 行，完整阅读；格式/checksum/上限、full/indexed/async read、
  large-value wrapper、properties fast path、listing 和同步 inline cache。
- F-018 测试缺口明确：现有 `inline_blob_values_reuses_open_blob_file` 只断言同步路径
  两条引用产生一次 open；没有 async 等价测试。async backend 测试只有单个 indexed
  value，无法发现同 file 多 record 的重复整对象 GET。
- F-017 测试缺口：分别覆盖单 properties/body/direct-value 长度上限，但没有累计
  decoded bytes/record count、高压缩比多记录，以及 encoder 在最终 whole-file 上限前
  的提前终止/内存预算。
- 075 规范化候选加强：listing 测试明确把大写 `.TRINEB` 当合法，但删除路径永远生成
  小写 canonical 扩展；在大小写敏感文件系统上，恢复可把它折叠为 referenced id，
  清理却不会命中真实文件。需决定“接受并保留实际 StorageObjectId”还是严格拒绝别名。
- 格式缺口：未测试 properties 与 footer 之间 gap、重复 internal key、metadata-only
  properties 的非法 bounds/count，以及 legacy raw blob ref 的 header-id/区域验证。
- 正面结论：测试覆盖 header/footer/properties/record/value 多层 checksum，未知 codec、
  unordered record、精确 BlobIndex 比对、targeted read 不扫描无关 record、异步基本读取
  与临时文件清理；临时目录均在成功路径显式删除。

### 080 — `src/io/platform_backend/freebsd_backend.rs`（25 行）— 已审

- 覆盖：第 1–25 行，完整阅读。
- 作用：声明 FreeBSD 下各 I/O 操作是平台部分异步还是线程池托管。
- 结论：纯常量能力矩阵，无执行逻辑/`unsafe`；注释与“open/metadata/rename 等仍阻塞”
  的分类一致，未发现独立问题。后续在 dispatcher 中核对矩阵是否真实决定执行路径。

### 081 — `src/io/platform_backend/linux_backend.rs`（25 行）— 已审

- 覆盖：第 1–25 行，完整阅读。
- 作用：Linux 平台 I/O 能力矩阵。
- 结论：除 directory listing 与 writer lease 外均声明 true platform async；本文件无
  执行逻辑。是否准确（尤其 temp-write/rename/directory-sync 的复合操作）需结合
  `platform_backend.rs` 的实际实现复核，当前不单独判错。

### 082 — `src/io/platform_backend/macos_backend.rs`（26 行）— 已审

- 覆盖：第 1–26 行，完整阅读。
- 结论：能力矩阵把 DispatchIO 数据阶段标为“平台异步但完整操作仍含阻塞阶段”，并把
  metadata/delete/listing/lease 归线程池；分类保守，未见独立问题。实际 FFI 安全性在
  `apple_dispatch.rs` 单独逐行检查。

### 083 — `src/io/platform_backend/solarish_backend.rs`（25 行）— 已审

- 覆盖：第 1–25 行，完整阅读。
- 结论：Solaris-family libc AIO 只覆盖读写/sync，复合操作标为 partial，其他阻塞步骤
  走托管线程池；纯常量声明，无独立问题。

### 084 — `src/io/platform_backend/unix_backend.rs`（22 行）— 已审

- 覆盖：第 1–22 行，完整阅读。
- 结论：通用 Unix fallback 将所有操作标为线程池托管，避免把阻塞 syscall 冒充平台
  completion；无执行逻辑和独立问题。

### 085 — `src/io/platform_backend/unsupported_backend.rs`（22 行）— 已审

- 覆盖：第 1–22 行，完整阅读。
- 结论：未知平台的全部操作显式标为 unsupported，选择失败关闭而非静默同步 fallback；
  纯常量矩阵，无独立问题。

### 086 — `src/io/platform_backend/windows_backend.rs`（28 行）— 已审

- 覆盖：第 1–28 行，完整阅读。
- 结论：IOCP 只覆盖 positioned read/write 子步骤，含 open/sync/rename/directory/lease
  的完整操作保守标为 partial 或线程池；未把 `FlushFileBuffers` 等同步阶段误标为 true
  async。本文件无执行逻辑和独立问题。

### 087 — `src/io/platform_backend/apple_dispatch.rs`（390 行）— 已审

- 覆盖：第 1–390 行，完整阅读；DispatchIO read/write/sync、block callback ABI cast、
  CString/path/fd 生命周期、fallback 与 durability。
- `unsafe` 结论：三处核心不安全边界均有局部约束。callback 中的
  `*mut DispatchData` 只在回调期借用并立即复制；handler block、queue、channel 和 data
  均活到 done event；barrier 使用的 fd 在 channel 存活且等待完成期间有效。`done`
  以 `u8` 承接 Apple C Boolean 并只判断非零，与 dispatch2 自身模式一致。未发现悬垂
  指针、double free、跨线程非 Send 引用或越界解引用。
- 健壮性候选：read 将所有 callback chunk 无预算地 append，最终才检查精确 `len`；
  正常 DispatchIO 不会超过请求，但 FFI 边界更稳妥的做法是在每块加入前
  `checked_add` 并拒绝累计超过 len。
- 错误处理观察：create/write 的绝大多数错误（除 AlreadyExists）会再走一次 blocking
  write。对无空间/权限等永久错误会做重复 I/O 并以第二个错误覆盖原始上下文；建议只对
  已知 DispatchIO 不支持/建文件兼容错误回退，并保留双错误链。
- cleanup block 的 errno 被忽略；I/O handler/barrier 已负责主要结果，但若系统只在
  channel cleanup 报告延迟 close 错误，当前 API 无法上报。需依据 Apple 明确契约补充
  注释或状态汇合测试，暂不判为 durability 缺陷。
- 正面结论：offset 转换受检、NUL path 被拒绝、空 write 使用合法 empty DispatchData；
  error/receiver-disconnect 会 STOP channel；sync 决策统一委托 durability 模块。

### 088 — `src/io/platform_backend.rs`（427 行）— 已审

- 覆盖：第 1–427 行，完整阅读；OS backend 选择、compio worker、random/whole read、
  temp-write-rename、append/persist/delete/directory/listing/lease。
- F-015 直接确认到 platform-io：`write_temp_rename` 在所有 OS 都先 rename，后
  `sync_parent_directory`，但返回类型仍只有 `Result<()>`；目录 sync 失败无法表达
  namespace 已切换，manifest 上层会误删输出。
- 并发契约候选：append 不是内核 `O_APPEND`；它先打开 handle，再对 path 单独
  metadata 取长度，最后 positioned write。正确性完全依赖同一路径调用在更高层严格
  串行，若 WAL rewrite/另一 append 并发，可能同 offset 覆盖或对旧 handle 使用新 path
  的长度。应在 API 类型/锁中表达 single-appender，而不只靠调用约定。
- 性能问题：非 macOS temp publish 和 append 把输入 `Arc<[u8]>` 再 `to_vec()` 后交给
  compio，整 table/blob/WAL buffer 会额外复制；大对象写峰值显著增加。可使用 compio
  接受的 owned Arc/Bytes buffer 或在提交任务时转移唯一 Vec 所有权。
- 错误语义：Windows directory sync 同时出现 close 与 sync failure 时优先返回 close
  并丢失更关键的 durability 错误；建议合并/保留两者。通用 listing 将 panic 映射为
  runtime busy，排序确定，且只返回普通文件。
- 正面结论：optional whole read 在分配前核对同一 open handle 的 metadata 上限；
  delete-not-found 幂等；未知平台由能力矩阵阻止；worker 对 runtime 启动失败和 task
  panic 都会完成等待方而非静默遗失任务。

### 089 — `src/io/platform_threadpool.rs`（306 行）— 已审

- 覆盖：第 1–306 行，完整阅读；阻塞线程池 I/O、optional bounded read、atomic
  temp/rename、append/persist/delete/listing、native writer lease 与目录 sync。
- F-015 再确认：thread-pool `write_temp_rename` 同样在 rename 后同步父目录并用单一
  `Result` 返回，模糊发布结果不是 compio 特有问题。
- 跨 backend 语义偏差：thread-pool append 使用内核 `O_APPEND`，native compio 路径
  却用“metadata 长度 + positioned write”；相同 storage API 的并发安全性随 feature/
  平台变化。应统一为 single-appender handle 或真正 atomic append。
- 错误分类：writer lease 已被占用返回 `Error::Corruption`，其实是正常的并发打开/
  ownership 冲突；会误导监控和自动修复策略。建议使用明确的 lease-held/runtime-busy
  错误，并保留 owner 信息。
- 正面结论：optional read 用同一 handle，metadata 上限后仍以 `max+1` 限流，能发现
  读取期间增长；append/create/delete 幂等边界清晰；lease 在写 owner 任一步失败时随
  RAII file drop 自动释放；目录 listing 过滤非普通文件并排序。

### 090 — `src/io.rs`（1,070 行）— 已审

- 覆盖：第 1–1,070 行，完整阅读；completion Future、inline/blocking/platform driver、
  task 分类/路由、lazy worker 初始化、panic completion 与所有 platform task dispatch。
- **里程碑：Rust 文件数与总行数均已超过 50%。**
- 背压设计问题：thread-pool 用 1,024 深度 bounded queue + `try_send`，native platform
  driver 却用无界 `std::sync::mpsc::channel`；并发 read/write 可无限排队并长期持有
  path/`Arc<[u8]>`。同时 native worker 对每个 task 调用一次
  `runtime.block_on(task.run())`，任务仍严格串行，所谓 true platform async 未形成多个
  in-flight I/O。建议有界队列并在单 runtime 中调度受限并发 Futures。
- 取消语义：调用方 drop `IoCompletion` 不会取消排队/进行中的任务；写入继续通常是
  原子发布所需，但纯读取也会继续消耗 I/O。应区分不可取消 mutation 与可取消 read，
  并在文档/统计中暴露 dropped waiter。
- Future 契约观察：`IoCompletion` 可 clone 但状态只保存一个 waker，多个等待者会互相
  覆盖；当前调用模式只有一个 waiter，worker clone 仅 complete。建议取消公开 Clone
  语义或改为明确 single-consumer 类型，防止后续内部调用误用后永久挂起。
- 错误/panic 正面结论：operation panic 被捕获并写入对应类型的 completion；runtime
  启动失败会持续 drain 队列并逐项返回错误；bounded queue full 在 admission 即报
  runtime busy；mutex poison 和 double-complete 都不会读取未初始化结果。
- 维护性观察：matrix 中有 `WalRewrite` 字段/操作枚举，但 `PlatformIoTask` 没有对应
  variant，实际 rewrite 复用其他复合任务；能力声明与可调度任务集合已出现漂移，建议
  用单一声明生成矩阵与 dispatch exhaustive match。

### 091 — `src/io/tests.rs`（315 行）— 已审

- 覆盖：第 1–315 行，完整阅读；inline/blocking completion、backend matrix 全平台
  断言和 double-complete。
- 测试缺口：matrix 测试只验证代码声明彼此一致，不执行真实 I/O，也无法证明 Linux
  “TruePlatformAsync”复合操作不阻塞、append 原子、rename 后错误语义或目录 durability。
- 090 候选未覆盖：没有 native 无界队列压力/单 worker 并发度、worker/runtime 启动失败、
  task panic、drop waiter、多个 cloned waiter waker 覆盖或 queue-full 的行为验证。
- 088 并发候选未覆盖：没有同一路径并发 append 与 WAL rewrite 的确定性测试；threadpool
  `O_APPEND` 和 native positioned write 的语义差异不会被当前 suite 发现。
- 测试 helper 以 5 ms sleep 轮询、2 秒墙钟 deadline 等待 blocking task，慢 CI 可能
  偶发失败且不能验证真实 wake 行为；可用 condvar/标准 executor 和同步 barrier 改进。
- 正面结论：所有 target-family matrix row（含 WalRewrite）均显式枚举，基础 completion
  类型和值传递、double finish 保护已有测试。

### 092 — `src/runtime.rs`（859 行）— 已审

- 覆盖：第 1–859 行，完整阅读；runtime capability、后台线程、bounded blocking pool、
  result Future、panic 隔离、关闭/join、取消 token、配置校验及单元测试。
- 关闭语义候选：`BlockingTaskPool::drop` 先设置 shutdown，但 worker 会优先取出并执行
  queue 中已有的所有任务，析构随后逐个 join。任一已接纳任务永久阻塞时，最后一个
  `Runtime`/Db owner 的析构也可永久阻塞；`CancellationToken` 没有与 blocking task
  自动关联，也没有取消、超时或“停止接纳并丢弃未开始任务”的关闭模式。需结合 Db
  close 对 Runtime 所有权和任务闭包捕获方式复核是否能形成用户可触发的关闭挂起。
- 可观测性问题：`spawn_blocking` 的 task panic 被 worker 静默捕获，仍增加
  `completed_tasks`；只有 `spawn_blocking_result` 自己再包一层并向 Future 返回
  `RuntimeBusy`。统计无法区分成功、错误与 panic，fire-and-forget 后台失败也没有日志
  或错误通道。
- API 观察：blocking pool 首次提交才一次性启动全部 worker；若中途线程创建失败，
  queue 被永久标记 shutdown，后续调用可能先成功创建新线程、再在 submit 返回 Closed，
  状态机和错误原因不直观。构造参数用 `.max(1)` 静默改写 0，而不是拒绝无效配置。
- 正面结论：queue admission 有明确容量上限；result state 的写入与 waker 注册在同一
  mutex 下，无丢唤醒窗口；task panic 不会杀死 worker；析构避免 join 当前线程；计数
  和时长换算使用原子/饱和转换；测试覆盖容量拒绝、panic 后 worker 存活和取消可见性。

### 093 — `src/stats.rs`（920 行）— 已审

- 覆盖：第 1–920 行，完整阅读；公共 Db/存储/平台 I/O/压实/filter/read-path 指标模型、
  原子采集、饱和聚合、比率 helper 与测试。
- 结论：该文件不参与持久化或控制流决策，未发现独立安全漏洞；公开 helper 对零分母
  返回 `None`，跨分类聚合均使用 `saturating_add`，避免诊断接口因溢出回绕。
- 一致性观察：`BlobReadMetrics`、`ScanWasteMetrics` 的进程内原子累计仍用
  `fetch_add`，极长生命周期后会回绕，而同文件的公开聚合明确承诺饱和。压实总计也有
  同类差异；建议统一为饱和原子更新，或明确原始计数器允许 modulo wrap。
- 指标语义观察：blocking runtime 的 `completed_tasks` 包含 panic 任务，而
  `DbStats` 文档称“completed”；没有 success/error/panic 分类。另有兼容别名
  `fallback_total` 把 partial native 与 unsupported 全部称为 fallback，虽已文档说明，
  仍容易让旧 dashboard 误读。
- 正面结论：平台 I/O 分类按 operation 展开且汇总覆盖全部字段；read/filter 累加器逐
  字段饱和；公共数据结构只承载快照，不暴露可变内部原子状态。

### 094 — `src/storage.rs`（1,151 行）— 已审

- 覆盖：第 1–1,151 行，完整阅读；对象/目录 identity、whole-read 上限、能力位集、
  sync/async storage traits、memory backend、native runtime 路由、分配/换算 helper。
- F-015 接口根因：`StorageManifestPublishBackend::publish_manifest` 与 blocking 版本只
  返回 `Result<()>`，类型层无法表达 `NotPublished/Published/OutcomeUnknown`；rename
  后目录同步失败只能被所有上层当作普通失败。
- F-007 接口根因：`StorageObjectWriteBackend::write_object` 和 `delete_object` 没有
  create-only、expected version/etag、writer epoch 或 generation 参数；任何 backend
  实现都无法在这层对 table/blob 的 stale writer 覆盖、删除做 fencing。
- F-008 接口根因：directory/object listing 必须构造并一次性返回 `Vec`，没有分页、
  cursor 或 streaming admission；大型远端 namespace 即使 client 支持 pagination，
  storage 抽象也会要求全量物化。
- 异步契约候选：`poll_ready_storage_future` 使用 noop waker 只 poll 一次，Pending 就
  返回 Unsupported 并丢弃 Future。它要求所有 `BlockingStorage*` 默认方法调用的 async
  实现首次 poll 必须同步完成，但 trait/type 没有表达该前置条件；若实现先做副作用再
  Pending，可向调用方报失败而操作随后部分发生/被取消。继续逐 backend 核对实际实现。
- 内存/复制观察：默认 `read_exact_at_owned` 会按调用者 len 直接分配，安全性依赖上层
  在调用前施加 object-kind 上限；`StorageReadBuffer::into_arc_bytes` 从 `Bytes` 切片
  重建 `Arc<[u8]>`，产生一次全量复制，名字容易让调用者误以为是零拷贝。
- 正面结论：内存 backend positioned read 使用 `checked_add` 和受检 slice；allocation
  failure、`usize/u64` 转换均返回结构化错误；能力位各占独立 bit，durability 要求按
  strict data/metadata 能力组合检查；native blocking 任务由 bounded runtime 接纳。

### 095 — `src/storage/backend.rs`（383 行）— 已审

- 覆盖：第 1–383 行，完整阅读；native/object-store enum dispatch、read/append/lease
  handle enum、全部 async 与 blocking trait 转发。
- blocking dispatch 设计缺陷：只有 `open_read_blocking` 和
  `read_object_bytes_blocking` 显式按 variant 转发；append、WAL rewrite、directory、
  manifest publish、object write/delete/list 的 blocking impl 都使用“poll async 一次”
  的默认方法。Native backend 配置 blocking runtime 时其 async Future 会 Pending，
  因而 enum wrapper 可能错误返回 Unsupported，尽管底层 native backend 有真正的
  blocking 方法；某些 Future 若在首次 poll 已提交任务，则调用方收到失败而任务仍执行。
  当前 enum 尚未广泛接入 Db 生产路径，先作为架构/API 候选，后续测试文件核实。
- object-store sync 边界不统一：read 的 blocking 方法显式拒绝 async-only backend，
  其余 blocking 默认方法却会实际 poll 一次 object client Future，再以是否 Pending
  决定结果；立即 Ready 的自定义 client 甚至会让标称“async-only”的同步写删成功。
  应逐项显式 match，native 直达 blocking 实现、object-store 一律稳定拒绝。
- 维护性问题：文件注释仍称 object-store “未来加入第二 variant”，而代码已经实现；
  大量空 marker impl 依赖容易被忽略的默认方法，正是上述行为分叉的来源。
- 正面结论：异步路径对所有 variant 做穷尽 match；read object/append/lease handle
  enum 不使用 downcast 或 `unsafe`；明确不受支持的 object-store append/lease/
  directory/manifest 操作均返回结构化错误。

### 096 — `src/storage/backend_tests.rs`（68 行）— 已审

- 覆盖：第 1–68 行，完整阅读；StorageBackend 两个 variant 的 capability、object byte
  ops 与 unsupported 操作测试。
- 测试缺口：只验证 native capability 位，不执行 enum wrapper 的 native blocking
  append/rewrite/publish/write/delete/list；因此 095 记录的“默认方法单次 poll 后错误
  Unsupported/后台仍执行”不会被发现。
- object-store 测试使用测试专用单次 poll helper 驱动 async byte op，而不是调用
  `BlockingStorageObject*`；故不能证明所有 blocking API 都稳定拒绝 async-only variant。
- 其余测试对象局部、内存 backend 无外部副作用，未见测试隔离问题。

### 097 — `src/storage/fault_injection.rs`（101 行）— 已审

- 覆盖：第 1–101 行，完整阅读；测试期全局 fault registry、路径/kind/调用序号匹配和
  RAII weak registration。
- 结论：模块仅在 `cfg(test)` 编译，未进入发布产物；匹配前在 mutex 内升级 weak 列表，
  离锁后执行计数，不持全局锁进入 I/O，未发现生产安全问题。
- 测试设计观察：fault point 只表示某操作入口失败，不能精确插在 rename 前后等状态
  转换中间；这正是 F-015 当前测试难以覆盖的原因。建议把 manifest publish 拆成
  temp-write、temp-sync、rename、parent-sync 独立 fault point。
- 稳健性观察：registry mutex poison 直接 panic，测试并发中的一次 panic 可污染后续
  测试；可恢复 poison inner 或为每次测试使用显式注入对象，减少全局隐式状态。

### 098 — `src/storage/metrics.rs`（384 行）— 已审

- 覆盖：第 1–384 行，完整阅读；native storage 路由计数、operation latency、平台
  operation/class 矩阵、快照和 timed result/Future wrapper。
- 结论：仅诊断路径，不参与安全决策，未发现独立漏洞；operation 与 platform enum 的
  match 穷尽，snapshot 字段映射完整，时长转换在 `u64` 上饱和。
- 一致性问题：所有底层 atomic counter 用 `fetch_add`，在溢出时回绕，与公开 stats
  helper 的饱和承诺不一致。`requests` 与 `total_latency_micros` 又是两次独立 atomic
  load，快照不保证来自同一时点；比率/平均延迟只能视为近似值，文档应明确。
- 指标语义问题：timed Future 只有正常完成时才记录；等待方 drop 后，底层已提交任务
  可能继续执行，却没有 operation request/latency 记录。平台 class 则在 dispatch 时
  计数，字段文档却称 completions，因此两类指标的 admission/completion 语义不同。
- 正面结论：所有 atomic 都只承载统计，Relaxed 更新配合 Acquire snapshot 不影响数据
  正确性；错误结果也会计入完成的 operation latency，避免只展示成功请求造成偏差。

### 099 — `src/storage/native_file.rs`（1,207 行）— 已审

- 覆盖：第 1–1,207 行，完整阅读；native 子模块装配，以及 wasm browser OPFS 的路径、
  read/write/append/rewrite/manifest、SyncAccessHandle、Web Locks lease 和 JS 换算。
- durability 候选：非 DedicatedWorker 的 WritableFileStream 路径把
  `DurabilityMode::Flush` 与 `Buffered` 同样处理，仅等待 `close()`；DedicatedWorker
  SyncAccessHandle 才显式调用 `flush()`。backend 却统一宣告 Flush capability。需依据
  浏览器/OPFS 的 close 持久性契约与项目对 Flush 的公开定义确认是否虚假承诺。
- lease 挂起候选：`acquire_browser_writer_lease` 保存 Web Locks `request` Promise 但
  从不 await/监听其 rejection，只等待 callback 发送 oneshot。若 request 在返回 Promise
  后异步拒绝且 callback 未执行，receiver 永远 Pending，没有超时或 rejection 到错误的
  桥接。
- WAL rewrite 语义观察：先完整提交 temporary，再另开 writable stream 覆盖 final，
  最后删除 temporary；temporary 没有用于 rename/swap。final close 后 delete temp 失败
  会向上返回失败，但 final 已经更新。内容相同使重试通常幂等，但接口仍无法报告阶段性
  结果，且 temporary 只充当恢复痕迹而非 publication primitive。
- I/O 健壮性：SyncAccessHandle 的 `write_all_at` 实际只调用一次 JS `write`，遇到合法
  short write 就报错而不循环；名字与通常的 write-all 契约不符。WAL 可留下 partial
  frame，table/blob/content 写可留下 partial object；上层必须保证失败对象永不发布并
  能在重试时 truncate。
- 路径正面结论：OPFS path 只允许 UTF-8 normal segments，拒绝 parent/prefix，消除了
  `..` 穿越；position/end 和 WAL growth 使用 checked add；JS 数值要求 finite、非负、
  整数且写 offset 不超过安全整数；whole read 在分配前和读取后各检查上限。
- 发布正面结论：browser manifest 始终使用 WritableFileStream 的 staged close 路径，
  明确不走 live-file SyncAccessHandle truncate；delete missing 幂等，listing 只纳入
  file entries 并最终排序。

### 100 — `src/storage/native_file/backend_impls.rs`（751 行）— 已审

- 覆盖：第 1–751 行，完整阅读；NativeFileBackend 全部 async/blocking trait 实现、
  platform driver 分流、runtime blocking adapter、计时与 durable delete。
- 095 候选确认到机制：native async fallback 的 Future 首次 poll 会调用
  `runtime.spawn_blocking_result` 提交实际 I/O，然后 Pending；StorageBackend enum 的
  默认 blocking 方法会立即丢弃该 Future 并返回 Unsupported，但已提交任务仍可写、
  删除或 publish。由于 enum 当前主要是预备架构/测试入口，尚未升级为生产漏洞；接入前
  必须显式实现所有 blocking dispatch 并增加副作用回归测试。
- F-015 再确认：native manifest async/platform/blocking 三条路径最后都落入相同的
  temp-write + rename + optional parent sync 协议，返回值始终是 `Result<()>`，没有任何
  路由层 readback/结果不确定协调。
- 指标观察加强：platform operation 在 submit 成功后、completion await 前计数；I/O
  随后失败或等待方取消仍被文档称为 completion。普通 operation 指标则只在 await 返回
  后计数，两个视图无法可靠对账。
- durable delete 观察：unlink 成功而 parent sync 失败会返回 Err；重试时 missing 被
  视为成功并再次 sync parent，具备可恢复性。但调用者若不重试，内存可认为对象仍在而
  namespace 已删除，接口同样缺少阶段结果。
- 正面结论：真正的 NativeFileBackend blocking 实现均直接调用同步 helper，没有经过
  单次 poll；platform blocking 路径显式等待 completion；async 所有输入被 owned move，
  不跨线程借用调用方 buffer；各操作在 dispatch 前执行 kind/capability 校验。

### 101 — `src/storage/native_file/helpers.rs`（774 行）— 已审

- 覆盖：第 1–774 行，完整阅读；native read/append/WAL rewrite/lease/directory/list、
  temp-write-rename、manifest publish、durability sync 与 WASI 特例。
- **确认 F-019**：`read_current_manifest_from_native_file` 直接 `fs::read`，在分配前没有
  检查 manifest object 的约 16 MiB whole-read 上限；decode 层虽会拒绝 oversized
  payload，但已经太晚。普通 native blocking/non-platform open 可被异常大 MANIFEST
  诱发巨量分配/OOM，而 platform optional-read 路径正确传入 max。
- F-015 直接实现再确认：WAL rewrite、普通 object publish、manifest publish 都在 rename
  后执行 parent directory sync；后者失败时原样返回 Err，无法撤销 namespace 更新。
- 并发/命名观察：同一目标的写入固定复用同一个 `.tmp` 路径，没有 create-new 或唯一
  nonce；并发 writer/重号 table 会相互 truncate/rename 对方临时文件，使 F-007/F-012
  后果不仅是覆盖 final，也可在构建阶段混写。
- 错误分类问题：`open_native_file` 把权限、fd 耗尽、瞬时 I/O 等所有 open error包装为
  Corruption；writer lease contention 也返回 Corruption。持久 bytes 损坏与环境/并发
  故障不应共享类别。
- listing 规范化候选加强：扩展名用 ASCII 大小写不敏感匹配，随后只保留构造出的 kind
  与原始 path；上层将文件名解析为裸 numeric id 后再用 canonical lowercase path 清理，
  会遗留/混淆大小写别名。
- 正面结论：position/length 转换受检；non-WASI append 使用内核 O_APPEND；WAL rewrite
  强制临时文件与 final 同目录且不同名；文件内容在 rename 前按 durability 同步；目录
  listing 只收普通文件、排序，并针对已知 WASI 重复-entry bug 失败关闭。

### 102 — `src/storage/native_file/objects.rs`（536 行）— 已审

- 覆盖：第 1–536 行，完整阅读；native read/append handle、runtime/platform 路由、
  blocking adapter、writer lease RAII、completion 等待与 block read source。
- **确认 F-020**：`NativeFileObject::read_exact_at` 不论 runtime/platform 配置都直接在
  Future 的 poll 线程执行 mutex + seek + `read_exact`；真实 async blob indexed read
  使用这个 borrowed-buffer 方法，单次 value 可达 64 MiB。async API 因而可长期阻塞
  executor，与 `PlatformIo`/blocking adapter 的能力声明不一致。
- 指标偏差：上述 borrowed read 既不增加 inline task，也不增加 platform task；开启
  platform driver 时，`len` 在方法调用、Future 尚未 poll 时就增加 platform 计数，
  dropped Future 也会被记作 completion。
- 性能问题：append 每次先把调用方 `&[u8]` 全量复制成 `Arc<[u8]>`，即使最终走 inline
  blocking 路径；大 WAL batch 会增加瞬时内存与 memcpy。可让 async API 接收 owned
  buffer，blocking 方法继续借用。
- lease 正面结论：owner 写失败会通过 File drop 自动释放 OS lock；Drop 只有文件内容
  仍等于自己的 owner token 才清空，并在清空后 unlock。WASI 先关闭 handle、再删除
  create-new lease 文件，未见双 owner admission 窗口。
- 其余正面结论：append handle 的 `&mut self` 与内部 mutex 串行本句柄操作；blocking
  read-owned/append/persist 直接走同步 helper；platform completion 等待虽为 1 ms
  polling，但不会持 storage mutex 或产生未唤醒死锁。

### 103 — `src/storage/tests.rs`（91 行）— 已审

- 覆盖：第 1–91 行，完整阅读；storage 测试公共 executor/waker、blocking worker
  barrier、临时目录生成与子模块装配。
- 结论：仅测试支持，无生产问题；waker 能唤醒当前测试线程，Future loop 能容忍伪唤醒。
- 测试稳健性：`park_timeout(1s)` 没有总 deadline，丢唤醒/永不完成会让测试永久挂起；
  `hold_runtime_blocking_worker` 依赖 1 秒墙钟，在重负载 CI 可能偶发失败。临时目录含
  pid+纳秒但不使用 create-new 随机目录，理论上仍可碰撞。

### 104 — `src/storage/tests/memory_capabilities.rs`（102 行）— 已审

- 覆盖：第 1–102 行，完整阅读；memory backend async/blocking read、owned buffer 与
  capability/durability 组合断言。
- 结论：基础边界覆盖正确，未发现测试自身副作用/遗漏清理。
- 测试缺口：没有 oversized offset/`checked_add` 溢出、short read、mutex poison、
  duplicate physical path with different kind，以及 memory object whole-read 上限行为；
  当前只覆盖小型成功路径与 missing object。

### 105 — `src/storage/tests/native_mutations.rs`（597 行）— 已审

- 覆盖：第 1–597 行，完整阅读；native list/write/delete/append/WAL rewrite/lease/
  directory create-list-sync 的 blocking 成功与 kind 拒绝测试。
- F-015 测试缺口：只验证 rename + directory sync 全部成功；fault injection 没有放在
  rename 之后，因此未断言“final 已更新但 API 返回 sync error”时上层必须保留输出并
  协调状态。
- F-012/F-007 测试缺口：table/blob 写只覆盖单 writer，不覆盖两个任务同时使用相同
  target/`.tmp`、stale writer overwrite 或 cleanup 删除同名新代对象。
- listing 别名行为被测试固化：测试明确期望大写 `.TRINET` 被当作 table object，
  但没有继续经过 numeric id 解析与 canonical cleanup，因而看不到大小写敏感文件系统
  上的遗留/identity 分叉。
- lease 测试充分验证 stale marker 不等于活锁、第二个 OS lock 被拒绝、drop 只清理
  自身 owner；但仅断言错误字符串，未要求正确的非 Corruption 分类。
- 正面结论：成功写后检查 temp 消失；delete missing 幂等；append 顺序和 persist、
  WAL temp/final 分离、同目录 publish、普通文件过滤/长度与排序、directory sync 均有
  基础行为覆盖；每个成功路径清理测试目录。

### 106 — `src/storage/tests/native_objects.rs`（298 行）— 已审

- 覆盖：第 1–298 行，完整阅读；inline/runtime owned read、whole-object read、manifest
  双次 publish/current read 与 storage 指标。
- F-019 测试缺口：manifest read 只使用十余字节；没有创建超过
  `MAX_MANIFEST_PAYLOAD_BYTES + 14` 的 sparse/regular 文件并验证分配前拒绝。
- F-020 测试缺口：只验证 owned read 会在被占用的 blocking worker 后 Pending；完全未
  测 borrowed `read_exact_at`，因此未发现它直接在 poll 线程执行文件 I/O且不计 routing
  stats。
- 测试语义观察：inline mutation 使用单次 poll helper，证明当前首次 poll Ready，但
  没有设置“不得先产生副作用再 Pending”的 guard；StorageBackend enum 的默认 blocking
  误用也未覆盖。
- 正面结论：runtime worker barrier 能确认 owned/whole read 确实 offload；指标基本
  值、missing optional read、manifest 覆盖更新和 inline capability 均有断言，成功路径
  清理本地文件。

### 107 — `src/storage/tests/platform_io.rs`（452 行）— 已审

- 覆盖：第 1–452 行，完整阅读；Linux true-platform、其他 OS partial-native、无 native
  feature 时 threadpool 的 read/append/management 路由与分类统计。
- F-020 测试缺口：所有 random read 都使用 owned API；没有 borrowed-buffer blob 路径，
  所以开启 platform-io 后同步 seek/read 仍跑在 poll 线程不会破坏现有断言。
- 090 并发缺口：只串行执行单个 operation，未验证 platform worker 是否允许多个
  in-flight I/O、bounded queue、drop waiter、同路径 append/rewrite 竞争。
- 统计测试偏向“声明自洽”：断言 Linux directory sync、temp-write-rename 等被计为
  true async，但没有 heartbeat/timer 证明复合操作端到端不阻塞；且只要求 task count
  `>=`，无法发现未 poll/dropped Future 被提前计数。
- 条件覆盖观察：主要测试受 `feature + target_os` 双重 cfg 限制；默认开发机只能执行
  自己平台的一组，跨平台矩阵需要 CI 明确跑 Linux native、各 partial-native target
  及 threadpool fallback，否则大量代码只是编译期存在。
- 正面结论：成功路径同时验证文件内容、temp 清理、lease owner cleanup 和 per-operation
  class；非 Linux 分支明确禁止误报 whole-operation true async。

### 108 — `src/storage/tests/runtime_adapter.rs`（353 行）— 已审

- 覆盖：第 1–353 行，完整阅读；runtime adapter 对 read/mutation/directory/lease/
  manifest/WAL/list/append 的排队行为，以及 inline completion。
- 正面结论：用占满单 worker 的 barrier 可靠确认各 NativeFileBackend async owned
  operation 会 Pending 并在释放后完成；同时确认显式 blocking API 不依赖 worker，
  这一组对 runtime 路由的覆盖较强。
- 095 缺口再次明确：所有测试直接调用 `NativeFileBackend`，没有包进
  `StorageBackend::Native` 再调用其 blocking traits；因此 enum 的空默认 impl 绕过这些
  已验证的 direct blocking 方法。
- F-020 缺口：开头虽调用 borrowed `read_exact_at`，但 backend 无 runtime 且文件仅
  6 bytes，只证明它首次 poll Ready；没有在 runtime worker 被占用或慢 read 条件下验证
  executor 是否被阻塞。
- 性能测试缺口：只断言 `Vec → Bytes` 保留 allocation pointer，未断言
  `StorageReadBuffer::into_arc_bytes`；后者当前从 slice 创建 Arc 并全量复制。
- 测试隔离观察：多数路径成功时显式清理；发生 expect panic 时会遗留临时目录，但命名
  足够细分，不会通常污染后续逻辑。

### 109 — `src/wal.rs`（868 行）— 已审

- 覆盖：第 1–868 行，完整阅读；WAL public constants/key classifier、front-door/
  lane command-completion、sync/async wait、shard admission、browser locks、worker drop。
- completion 正面结论：result 与 waker 分锁但 Future 在注册 waker 后二次检查 result，
  不存在经典丢唤醒窗口；Condvar wait 使用循环；poison 会转结构化错误；每个 command
  创建独立 waiter，没有共享 take-result 的多消费者问题。
- 生命周期候选：`WalFrontDoorLane::drop` 关闭 sender 后无条件 join worker；worker 会
  处理 channel 中全部已入队命令，任何底层 WAL I/O 永久阻塞都会让 Db 最后析构卡住，
  且没有 cancellation/timeout。与 runtime pool 的 graceful-only shutdown 同型。
- async 背压待 lane 复核：每 lane 是 64 深度 `sync_channel`，但 async admission 是否
  等待容量或立即 RuntimeBusy 由 `enqueue_wal_lane_command` 决定；继续在 `lane.rs`
  检查 queue-full 是否会导致已分配 commit sequence 的正确 skip/publish。
- API 精度问题：公开 `is_wal_object_key` 只检查最后段是否以 `"trine.wal"` 开头，会把
  `trine.wallet`、`trine.wal-unrelated` 等也分类为 WAL；这可能误导共享 client 的
  durable-tier/计费/批处理路由。应解析完整的有限 grammar，而非宽前缀。
- browser 正面结论：每个 active shard 用 async mutex 覆盖 open+append，防止同 shard
  size/append 竞争；rewrite 对 active shard 取同一锁，inactive discovered shard 不再
  接收新提交；shard index 换算先取 modulo 再转 usize。
- 统计观察：accepted counter 只在 append waiter 成功后增加，语义准确；仍使用
  `fetch_add` 而可能长期回绕。

### 110 — `src/wal/codec.rs`（396 行）— 已审

- 覆盖：第 1–396 行，完整阅读；batch/frame encode/decode、object WAL 文件名 grammar、
  operation/bound codec、cursor、CRC 与长度上限。
- 高风险候选：decoder 只允许忽略“文件最后一个不完整 header/payload”，适合 crash
  尾部；但 native `write_all` 报错前可能已写入部分 frame，commit 层只 mark sequence
  skipped 并允许后续提交。若同一 append handle 后续继续写成功，partial bytes 不再是
  尾部，recovery 会在其 header/checksum 处报 corruption，后面的已确认 commit 也无法
  恢复。需在 lane 错误状态与 recovery 合并规则中确认是否会关闭/隔离该 shard。
- 编码内存候选：先把完整 payload 构造到 Vec，随后才检查 64 MiB frame 上限，再复制
  到第二个 frame Vec；超限 batch 会先承受至少 payload+frame 的峰值。结合公共配置边界
  是 F-004 的 WAL 侧放大。
- decode 正面结论：payload len 在 slice 前受 64 MiB 上限和 checked-add 保护；
  op_count 先由最小编码尺寸约束再 reserve；每个 byte field 通过 cursor bounds；
  unknown tag、非 UTF-8 bucket、trailing payload 都失败关闭；完整 frame header/payload
  各有 CRC。
- object WAL 名称正面结论：sequence 强制 20 位十进制，identity 强制 64 位 hex且必须
  有分隔符；不符合完整 grammar 的疑似名称多数返回 corruption/None，不会把任意前缀
  直接解析为 sequence。
- 格式语义候选：`DeleteRange` 解码后不验证空/反向 range，WAL 可把这种 operation 带入
  replay；需与写入 admission 的 range 规则统一处理。

### 111 — `src/wal/lane.rs`（433 行）— 已审

- 覆盖：第 1–433 行，完整阅读；command enqueue、worker group commit、append/persist/
  marker、rewrite/reopen、错误 fan-out、durability rank/order 与 shard filename parser。
- **确认 F-021**：非 WASI 的 async `enqueue_wal_lane_command` 使用 bounded
  `SyncSender::send`；queue 满时在返回 waiter 之前阻塞调用线程。`accept_commit_async`
  因而能卡住 executor，而不是异步等待容量或立即返回 backpressure 错误。
- 高风险错误状态候选：
  - append 错误只完成当前 reply，保留 writer 并继续接收命令；partial frame 后续可接
    合法 frame，使 recovery 在中部坏帧处失败。
  - persist/confirmed-marker 错误让 commit 返回失败但保留已写 frame；更晚成功提交若把
    confirmed sequence 推高，可能把此前“skipped”的 frame一起纳入恢复。
  - rewrite 已 rename final、但 parent sync 或 reopen 失败时，旧 append handle 仍保留；
    在 Unix 上它可能指向已被替换/取消链接的旧 inode，后续 commit 可以成功却不进入
    当前 WAL path。
  继续结合 recovery 的 confirmed marker/sequence merge 确认影响并统一定级。
- 错误吞噬：Rewrite 命令先 flush pending append，但显式忽略 flush 返回值后继续
  rewrite；batch 末尾也忽略 flush 函数返回（虽 pending reply 已被完成）。状态机没有
  fail-stop/fenced 标志，I/O 故障后的每个后续命令都在不明确的持久状态上继续。
- 错误分类：fan-out 只保留 `Error::Io`，其他错误一律变成
  `"group commit persist failed"` Corruption，丢失 Closed/Unsupported/RuntimeBusy 等
  可恢复语义。
- 正面结论：正常 group commit 只在 persist/marker 成功后完成需要持久化的 waiters；
  durability 按强度取 max；sequence stream 要求严格递增；shard 文件名强制 4 位十进制
  且 shard 0 只能使用 legacy 名。

### 112 — `src/wal/recovery.rs`（647 行）— 已审

- 覆盖：第 1–647 行，完整阅读；native/object WAL path、discovery/read/merge、rewrite、
  confirmed marker codec/coverage、object cleanup 与 async/blocking storage adapter。
- **确认 F-022**：recovery 只容忍最终 truncated frame；lane 却在可能 partial-write 的
  append error 后继续复用同一 writer。后续 frame 会接在残片后，decoder 将在文件中部
  遇到 checksum/magic/长度错误，所有更晚已确认提交均无法恢复。
- F-015 后果加强：WAL rewrite 的 rename 已成功但 parent sync/reopen 失败时，
  `WalLaneWorkerState.writer` 仍保存 rewrite 前的 append handle；Unix 上后续提交可写入
  已被替换的旧 inode。recovery 只打开当前 path，完全看不到这些“成功”append。
- confirmed marker 观察：`validate_confirmed_wal_coverage` 只要求解码批次的
  `max(sequence) >= confirmed_sequence`，不要求 marker 指向的 sequence 在该 shard 中
  真实存在。完整删除一个中间 frame、但保留更高 frame 时可漏检；CRC 只能检测 byte
  修改，不能检测整帧移除。可要求 exact membership，并为每 lane 建立链式连续性证明。
- 已声明语义说明：完整 frame append 成功而后续 persist 报错时，项目文档明确把 commit
  结果定义为 unknown，恢复可包含或不包含该 frame；因此不把这一本身单列为缺陷。但
  lane 的内存 `mark_skipped` 与重开后可能出现该写仍会给调用方带来幂等要求，公共 API
  文档也应就近提示，而不能只放在 production-readiness 文档。
- 资源候选：每个 WAL shard 通过 whole-object read 一次性物化，再 decode/clones 全部
  operation；object recovery 又串行累积所有 streams。虽单对象有约 1 GiB 上限，多个
  shard 总峰值仍可很高，缺少跨 shard cumulative budget/streaming decode。
- 正面结论：discovery 拒绝 duplicate shard id；每 stream sequence 严格递增，跨 stream
  merge 拒绝 duplicate sequence；marker 长度/magic/version/CRC 完整校验；object WAL
  path sequence 与内部唯一 batch sequence 交叉验证。

### 113 — `src/wal/tests.rs`（628 行）— 已审

- 覆盖：第 1–628 行，完整阅读；front-door append/shard/group persist/rewrite、sync/
  async discovery/read/write、stream merge、path grammar 与 decode allocation bounds。
- F-021 测试缺口：所有 front-door 调用为同步或低并发；没有填满 64-slot queue 后在
  executor 线程调用 `accept_commit_async`，所以阻塞 send 不会暴露。
- F-022 测试缺口：所有故障边界之外的 append 都完整成功；没有 short-write+Err、
  partial tail 后再次 append，或“rewrite rename 成功但 reopen 失败”后的 stale-handle
  行为验证。
- confirmed marker 缺口：没有 marker 指向的 exact sequence 缺失但存在更高 sequence
  的场景；现有读测只能证明正常 coverage，不证明整帧删除可被检测。
- public classifier 缺口：只测已知合法 WAL 名和完全不同的 SST/manifest/lease，没有
  `trine.wallet`、`trine.wal-unrelated` 等过宽前缀反例。
- 正面结论：测试覆盖 shard routing/merge order/duplicate/non-increasing 拒绝、rewrite
  后 reopen 成功、dirty-only persist、oversized payload 与 hostile op_count 在大分配前
  拒绝，以及 replay floor 跳过旧 payload 解码。

### 114 — `src/iterator/tests.rs`（127 行）— 已审

- 覆盖：第 1–127 行，完整阅读；source heap 正/反向顺序与双 memtable lazy merge。
- 结论：基础 comparator/merge 顺序测试正确，无生产逻辑。
- 测试缺口：只含不同 user key 的 Put；未覆盖同 key 多 version/source tie-break、point/
  range delete、snapshot visibility、source error 后 fused 行为、limit、空/反向 range、
  lazy blob error和 stale bucket generation。iterator 主文件记录的错误迭代语义仍无保护。

### 115 — `src/table/tests.rs`（210 行）— 已审

- 覆盖：第 1–210 行，完整阅读；table 测试 fixture、owned-only source、block/index decode
  helper、options 与 filter miss 搜索。
- 正面结论：`OwnedOnlySource` 主动 panic borrowed read，能保证 metadata open 测试确实
  使用 owned I/O；fixture 的 offset 加法受检，构造数据确定性。
- 测试设计观察：`poll_ready` 只适用于 memory/inline backend，若误用于真实 async 会
  panic；helper 名未表达这一限制。filter miss 搜索固定最多 10,000 次，Bloom 参数变化
  后可能产生与正确性无关的脆弱失败。
- 覆盖边界：本文件只提供 fixture；具体格式 hardening、filter/block 与 write/open
  断言在三个子文件中分别继续审计。

### 116 — `src/table/tests/decode_hardening.rs`（93 行）— 已审

- 覆盖：第 1–93 行，完整阅读；未知 codec、table payload、index/data/restart/tombstone
  count 与 Bloom 结构 hardening。
- 正面结论：多个 attacker-controlled count 都有“大 reserve 前拒绝”的直接断言；
  unknown codec 失败关闭，filter byte length/hash count 也覆盖非法值。
- 测试缺口：未覆盖 section overlap/gap/重复 block、offset+len 边界、properties/footer
  gap、重复 internal key，以及 F-003 的“point + 单边 Unbounded tombstone”metadata
  bounds；现有 hardening 偏重 count，缺少跨 section 不变量。

### 117 — `src/table/tests/write_open.rs`（189 行）— 已审

- 覆盖：第 1–189 行，完整阅读；record sort、async write/read、owned metadata source、
  table listing 与 final/temp 行为。
- 095 测试边界：async write 使用 `StorageBackend::Native(NativeFileBackend::new())`，
  因 inline Future 首次 poll Ready 而成功；没有 runtime-backed enum，不能覆盖其
  blocking default method 副作用问题。
- listing 只生成 canonical 小写名称，没有大写/非固定宽度 alias；无法发现 table id
  折叠与 canonical cleanup 分叉。
- F-012 缺口：只写单个唯一 table id，没有并行相同 id、existing final overwrite、
  同 `.tmp` 竞争或 obsolete queue 删除重用 id。
- 正面结论：already-sorted fast path 与 unsorted repair均验证 internal-key order；
  async write 返回 loaded table且 readback一致；metadata header/footer 明确通过 owned
  reads；成功发布不遗留 temp。

### 118 — `src/table/tests/filters_blocks.rs`（834 行）— 已审

- 覆盖：第 1–834 行，完整阅读；逐层过滤器深度、跨多数据块索引、哈希索引错配与
  重叠、缓存命中、二分定位、固定过滤器、缓存文件句柄、搜索策略、块大小、blob
  阈值上限、分区过滤器以及过滤器假阴性失败路径。
- 未新增独立缺陷：现有用例仍缺少 F-003 的关键混合表边界条件——同表同时包含点
  记录、单侧 `Unbounded` 范围墓碑和旧表值时，表级边界不得使墓碑所在表被跳过。
- 缓存测试均为顺序访问，没有覆盖多个线程同时 miss 同一块时的重复读取与解码；
  这与 `cache.rs` 已记录的无 singleflight 设计成本一致，暂不重复记为新问题。
- `block_bytes = 1` 被当作合法配置验证；小样本下功能正确，但会制造大量块与元数据，
  进一步说明 F-004 所涉及的公开配置缺少资源成本下界约束。

### 119 — `src/transaction.rs`（2,089 行）— 已审

- 覆盖：第 1–2,089 行，完整阅读；同步/异步点读与范围读、读集合、批量写、上传令牌
  消费、内容活动、回收 intent/quarantine/grace/sweep 全状态门禁及提交路径。
- 事务把成功的点读和范围读纳入冲突集合，内部内容控制记录也通过同一读集合验证；
  authority 扫描对键后缀长度、记录身份和过期状态均失败关闭，未见绕过冲突检查的
  直接入口。
- 设计观察：普通 `get`/`range` 始终读取事务起始序列，不合并本事务已暂存的
  `WriteBatch`，因此不提供 read-your-writes。类型文档虽描述“一个读快照和暂存批次”，
  但常见事务调用者容易作相反假设；建议在公共方法文档显式声明，或提供合并视图 API。
- 待跨文件复核：回收流程通过对象存储直接读取 descriptor，而不是事务快照内记录；
  需要结合 upload/storage/reclaim 路径确认所有 descriptor 创建、替换和删除是否必然
  同时触碰已纳入冲突集合的 content-control key。
- 待跨文件复核：最终 sweep 接受调用者提供的 `ContentReclaimClockAttestation` 时间；
  需在 `content/reclaim/mod.rs` 核对该证明的构造权限、可信边界及是否能用未来时间绕过
  grace。

### 120 — `src/branch.rs`（1,510 行）— 已审

- 覆盖：第 1–1,510 行，完整阅读；registry codec、代际令牌、临时/持久分支读写、
  k-way range merge、嵌套 lineage、两阶段创建、可恢复删除、checkpoint pin 与异步
  对称路径。
- 正面结论：持久分支每次新操作都会校验 leaf generation 和 active lifecycle；父分支
  删除通过全 registry 范围读参与乐观冲突检查，能阻止并发新增子分支；删除顺序先让
  registry 进入不可见的 `Deleting`，再清数据、释放 pin、最后删 marker，重试边界清晰。
- codec 对 magic、UTF-8、重复 bucket、parent/lifecycle tag、generation 尾长均严格
  校验；branch/data bucket 名使用十六进制编码，未见分隔符碰撞。
- 设计观察：`written_buckets` 随分支触碰的用户 bucket 单调增长，并在每次首次/重复
  branch write 时整体重编码 registry。没有条目大小或 bucket 数上限；极多 bucket 会
  造成 O(n) 写放大，最终还可能撞上 batch/value 格式上限，使该分支难以继续写入。
- 待测试核对：`BranchRange` 只在构造时校验 leaf generation；返回迭代器后并发删除/
  替换分支的语义，以及 create checkpoint 与 registry 发布之间的故障恢复，将结合
  `src/branch/tests.rs` 判断是否需要单独问题。

### 121 — `src/branch/tests.rs`（696 行）— 已审

- 覆盖：第 1–696 行，完整阅读；临时/持久覆盖读、范围合并、fork pin、嵌套分支、
  冻结父视图、子分支删除门禁、代际失效、删除续跑、重建清空、checkpoint orphan
  调和、lineage cycle 与持久 reopen。
- 正面结论：测试直接验证 stale branch handle 不能写入同名新 generation，删除 marker
  会让旧句柄的后续 get/put 失败，fork checkpoint 能抵抗激进 GC，且同名重建不继承
  旧数据。
- 覆盖缺口：没有并发 create/create、create/delete、write/delete 交错，也没有在
  `BranchRange` 已返回后触发删除/重建的用例；现有故障注入只在 WAL append 前失败，
  未覆盖 checkpoint 已发布但 registry commit/activation 失败的每个中间点。
- 测试工程观察：native 临时目录只含进程 id，单进程内名称依赖各测试前缀避免碰撞；
  手工 `remove_dir_all` 不使用 RAII，断言 panic 会遗留目录，但不影响生产代码结论。

### 122 — `src/browser.rs`（194 行）— 已审

- 覆盖：第 1–194 行，完整阅读；`navigator.storage` 获取、estimate/persisted/persist
  Promise 调用、JS 异常映射与数值转换。
- 未发现直接缺陷：缺失 API、非 Promise、非布尔结果、异常、非数字、负数及非有限
  estimate 均显式报错；Promise 被实际 await，不存在此前 Web Locks 路径那种悬空拒绝。
- 设计观察：estimate 的浮点字节数直接 `as u64`，小数会静默截断；上界比较还受
  `u64::MAX as f64` 舍入影响。浏览器通常只返回安全整数范围内的字节估计，实际风险低，
  但用 `number.fract() == 0.0` 与 JavaScript safe-integer 上限校验会使契约更精确。

### 123 — `src/content/mod.rs`（86 行）— 已审

- 覆盖：第 1–86 行，完整阅读；内容格式常量、尺寸边界、内部 bucket、状态/tag 与模块
  导出边界。
- 未发现独立缺陷；descriptor/chunk/upload-state 固定长度和所有内部命名均集中定义。
  `AtomicBool`/`AtomicU64` 等导入供子模块共享，后续在实际生命周期实现中核对内存序。

### 124 — `src/content/codec.rs`（377 行）— 已审

- 覆盖：第 1–377 行，完整阅读；上传 bearer token 记录、content-token 二级索引、
  optional 字段、durability tag、ContentId 与定长数组解码。
- token 只以 SHA-256 摘要作为 KV key/记录绑定，原始 256-bit bearer 不落盘；scope、
  content、upload、expiry、durability 和幂等 change id 都在定长记录内，消费状态对
  不同 change id 失败关闭。
- 二级索引记录会同时核对 domain/content/token hash 与 protected key，expiry=0 被
  拒绝，未见索引换绑入口。
- 设计观察：通用 optional decoder 在 presence=0 时不校验后续占位字节为零，允许同一
  逻辑值存在非规范编码；目前这些记录没有签名/按编码比较，影响较低，但严格格式可拒绝
  非零 padding。

### 125 — `src/content/identity.rs`（1,227 行）— 已审

- 覆盖：第 1–1,227 行，完整阅读；domain/owner/upload/lease/hold 身份、物理 quota/
  reservation/account 记录、上传选项、ContentId、sealed/attachment 公共值对象。
- 随机 bearer 使用 32 字节熵且 `Debug` 固定脱敏；lease/hold id 带版本字节，未知版本
  失败关闭。ContentId 携带算法 tag，当前 SHA-256 的编码和显示无歧义。
- chunk size 被限制为 64 KiB..=16 MiB，token/lease/hold Duration 均换算为非零毫秒
  并检查 u64/截止时间溢出；quota 两计数解码时也检查求和溢出。
- 未发现直接缺陷：公开 `from_bytes` 的 domain/owner/change id 明确属于调用者已认证
  的不透明身份，库不把其可构造性误当作授权。

### 126 — `src/content/lease_hold.rs`（693 行）— 已审

- 覆盖：第 1–693 行，完整阅读；lease/physical-hold 定长记录、key 前缀、共享原子状态、
  hold 句柄、content 句柄范围读、逐 chunk 校验、stream 与完整 SHA-256 verify。
- record 解码会核对 key 所保护的 domain/content/id，未知 id/kind/state 失败关闭；
  Release/expiry 的进程内共享状态使用 Acquire/Release，发布时机在 DB 层持久提交之后。
- 读取每个 chunk 都核对 upload id、index、长度和 SHA-256，零进展与短 chunk 显式报
  corruption；range 的整数换算与位置推进有溢出保护。
- 设计观察：`read_range` 按调用者请求一次性预分配整个返回区，虽是 API 固有语义，
  但对超大内容/不受信请求缺少单次读取预算；流式接口更安全，文档宜提醒服务端不要把
  外部 length 原样传入。
- 生命周期边界：leased read 只保证每个异步 chunk 开始前本地截止时间仍有效，不保证
  跨越截止时间的在途 I/O；是否会与 sweep 删除竞争，留待 DB reclaim 实现闭环。

### 127 — `src/content/upload.rs`（955 行）— 已审

- 覆盖：第 1–955 行，完整阅读；upload 句柄、分块写、revision、Open/Sealing/Sealed/
  Aborting 状态 codec、descriptor 与 chunk codec。
- 写失败后句柄 fail-stop；chunk 先写、session state 后写，revision 冲突显式返回；
  session 校验会重算 complete/partial/total 长度，descriptor 校验 chunk count，
  chunk 校验完整 header、identity、index、payload len 和 SHA-256。
- 关键待跨文件复核：`into_sealing`/`into_sealed` 沿用旧 `updated_at_unix_ms`；若 sealed
  retention 以该字段清理，长时间上传可能刚 seal 就被当成旧记录移除。
- 关键待跨文件复核：chunk object 仅以调用者可恢复的 `UploadId + index` 命名；sealed
  session 被清理后若 `begin_content_upload_with_id` 允许同 id 重新创建，新上传可覆盖
  仍被旧 descriptor 引用的 chunk。需结合 DB 创建、维护清理和 object create/replace
  语义确认。
- 非规范编码：Open/Aborting 状态解码不要求 reserved 的 expiry/durability/content-id
  区域为全零；逻辑不受影响，但严格格式可补充 canonical 检查。

### 128 — `src/content/reclaim/mod.rs`（1,024 行）— 已审

- 覆盖：第 1–1,024 行，完整阅读；access barrier、reader-drain 外部证明、逻辑回收
  授权、quarantine/grace、clock attestation 与 sweep backend 公共类型和信任契约。
- `ContentReclaimClockAttestation` 的观察时间确由调用者提供，但文档明确它是“可信外部
  协调器声明”，digest 只是审计承诺而非签名或独立时间证明；因此事务层不读取本机时钟
  来替代它属于公开的系统信任边界，不作为新缺陷。
- 同理 reader-drain 与逻辑不可达证明都明确由高层授权/协调器负责；库只保证精确绑定、
  持久排序和状态冲突，集成方若把这些公开构造器直接暴露给不可信客户端会破坏回收安全。

### 129 — `src/content/reclaim/codec.rs`（1,167 行）— 已审

- 覆盖：第 1–1,167 行，完整阅读；access/drain/quarantine/grace/sweep/control 全部持久
  记录、commit-sequence 占位编码、后端证据与 prefix range。
- 定长解码普遍核对 magic、长度、protected key 身份、版本 tag 及 commit 坐标单调性；
  native/WASI/browser sweep 的未用 evidence 区必须全零，对象存储 evidence 带算法 tag。
- `ContentControlRecord` 将最后物理活动与 intent 接受序列分开保存，active/intent 的
  reserved 字段和序列关系均严格校验，未见通过畸形记录降低回收门禁的入口。
- 设计观察：durable sweep 只保存 grace 的 commit coordinate，不重复保存其
  `not_before_unix_ms`；正常路径在 Prepared 前已比较可信 clock attestation，恢复依赖
  Prepared 记录不可被无检测改写。若未来引入脱离 KV 完整性域的导入/迁移，应把完整
  grace deadline 纳入 sweep 记录或认证摘要。

### 130 — `src/db/content/upload.rs`（1,175 行）— 已审

- 覆盖：第 1–1,175 行，完整阅读；上传枚举与维护、quota reservation/accounting、
  begin/resume/seal/abort、dedup、token/control 事务及 chunk 清理。
- `write_upload_state` 会在实际写入前刷新 `updated_at_unix_ms`，因此 127 中关于 sealing/
  sealed 沿用旧时间的局部疑点已排除。
- **确认 F-023（高）**：prune 只删 sealed session，不留 UploadId 墓碑；随后
  `begin_content_upload_with_id` 把同 id 当新上传创建。chunk 路径仍是同一
  `UploadId/index`，无条件 object write 会覆盖旧 descriptor 正在引用的 immutable
  bytes。
- **确认 F-024（高）**：新内容 seal 先发布 descriptor，再写 `Sealing` session。
  descriptor 成功而 session 写失败/进程终止时，持久状态仍是 `Open`；普通 open 已能
  看到内容，而 abort/reaper 会按 Open 清掉 descriptor 正引用的 chunks，且没有
  recovery marker 能识别这次发布。
- 正面结论：partial chunk 的“chunk 新、state 旧”窗口会在 resume 时验证完整 frame 后
  只保留 durable prefix，确实能回到最后 session revision；quota 的 reservation、
  unique account 和总计数通过同一乐观事务更新，并有逐步溢出/下溢检查。

### 131 — `src/db/content/storage.rs`（444 行）— 已审

- 覆盖：第 1–444 行，完整阅读；content chunk/descriptor/barrier/upload-state 对象
  路径、所有 backend 路由、锁分片、listing 与 upload id 文件名解码。
- F-023 闭环：chunk 路径只含 upload id 和 index；`write_content_object` 在 memory、
  native、WASI、object store、browser 均调用普通 replace 写，没有 create-only、
  generation 或 descriptor 引用检查。
- F-024 闭环：descriptor 和 upload state 是两个独立对象写，没有事务或日志把二者的
  发布顺序绑定；`write_upload_state` 的时间刷新不能修复 descriptor-first 窗口。
- 继承问题：这些 async content 对象读写只 `ensure_open`，未登记 close activity，
  加强 F-005；object-store 对象也未带数据库 fencing token，加强 F-007。
- listing 接受大写十六进制别名并映射为同一 UploadId；正常 writer 只产小写，畸形
  backend 可制造重复 state identity。建议 listing 检查 canonical 文件名和重复 id。

### 132 — `src/db/content/mod.rs`（107 行）— 已审

- 覆盖：第 1–107 行，完整阅读；内容子模块依赖边界、初始 reservation、哈希分片函数
  与两项单元测试。
- 未发现独立缺陷：生产初始化保证 shard_count 非零，完整 Hash 输入避免只按 id 前缀
  聚集；但全局 seal lock 与分片锁仅为单 Db 进程内协调，跨进程安全仍依赖数据库独占
  租约。

### 133 — `src/db/content/reclaim.rs`（634 行）— 已审

- 覆盖：第 1–634 行，完整阅读；不可逆 leased-only barrier、reader-drain attestation、
  quarantine/grace/sweep 查询、Prepared sweep 续跑与 backend qualification。
- barrier 采用 direct content object 先行、KV coordinate 后行的失败关闭顺序；旧只读
  handle 会直接读取 barrier，不能仅靠陈旧 KV 视图继续无租约 open。
- sweep 先持有 seal gate，再按 manifest 删除 chunks、descriptor，最后在乐观事务中
  更新 quota、清 control/quarantine/grace 并标记 Reclaimed；删除失败保留 Prepared，
  completion 冲突也重新验证合法状态迁移。
- 未新增独立缺陷：在途读取跨 lease 截止时间不受保护是明确的短租约语义；Prepared 前
  已要求可信 reader-drain、无有效 lease/hold/token，删除流程本身未见跳过门禁。

### 134 — `src/db/content/lease_hold.rs`（759 行）— 已审

- 覆盖：第 1–759 行，完整阅读；普通/leased open、lease renewal、physical hold
  acquire/resume/renew/release、生命周期 vacuum 与 authority key 解码。
- leased open 先读 descriptor，再在同一事务写 lease 并触碰 content-control；与 sweep
  竞争时 Prepared/Reclaimed 会在 `stage_content_read_activity` 失败关闭。hold 获取和
  renewal 使用相同活动 fence。
- F-024 影响扩大：descriptor-first 窗口中 control 尚不存在时，leased open/hold 获取
  可把该未完成 seal 的内容推进 Active；之后 Open session abort/reaper 仍可删除其
  chunks，留下已返回但不可读的 handle。
- renewal 在旧截止时间已到后拒绝复活，成功提交后才更新共享原子 deadline；hold release
  以 durable Released tombstone 幂等收敛，未见本地状态先于持久状态发布。
- vacuum 全表扫描后单事务删除 token/index、lease、released/expired hold；并发 authority
  变化会由范围/点读集合触发冲突，代价是 authority 数量大时内存与事务批次无硬上限。

### 135 — `src/db/commit/tests.rs`（779 行）— 已审

- 覆盖：第 1–779 行，完整阅读；durability floor、commit slot 连续可见性、sync/async
  waiter、writer-local preparation、WAL 接受时序、memtable publish、部分发布 fail-stop、
  并发 group commit 与写入配置边界。
- 正面结论：第二次 terminal slot 转换被拒绝，后序 slot 不会越过前序空洞变为可见；
  transaction WAL 明确在串行读集校验/sequence 分配后才接受，普通 blind write 则可在
  publish barrier 外预接受。
- 测试缺口：WAL fault 仍只覆盖 append 调用前失败，没有 short write 后 error 再继续
  admission 的 F-022 场景；group commit 只验证成功数量，未校验故障后的 lane 隔离。
- 配置测试覆盖 max key/value 的零值与公共 batch field 上限，但没有覆盖 F-004 中
  table index/block/blob codec 的更窄表示边界。

### 136 — `src/db/tests.rs`（108 行）— 已审

- 覆盖：第 1–108 行，完整阅读；数据库测试模块装配、最小无运行时 Future 驱动器、
  不可信 object-store 测试包装器。
- 安全结论：未发现新的独立问题。`block_on_test_future` 的自定义 waker 与每秒
  `park_timeout` 只用于测试，可能让缺少正确 wake 的 Future 仍缓慢推进，但不进入库的
  生产执行路径。
- 正面结论：`UnsafePutIfObjectStore` 可声明“不安全的条件写”并验证客户端契约探测，
  为默认信任与 open-time 验证分支提供了可控夹具。

### 137 — `src/db/tests/basic.rs`（977 行）— 已审

- 覆盖：第 1–977 行，完整阅读；命名空间隔离、单写者租约、持久化重开、durability
  拒绝、客户端条件写信任、并发 WAL chain、不可变 segment、孤儿清理、截断拒绝、
  分层 WAL、只读 refresh、drop bucket 与 compaction。
- 安全结论：未发现新的独立问题；截断 segment、未被 manifest 引用的 WAL、object
  manifest durable install 失败等路径均有失败关闭检查。
- 测试缺口：
  - 第二个 live writer 会被拒绝，但没有让旧/失效 writer 继续写 content、table 或 blob
    的边界条件测试，不能消除 F-007。
  - object cleanup 仅覆盖顺序执行，没有持有活跃 reader/iterator 时并发删除对象的
    生命周期测试，不能消除 F-006。
  - WAL object-key 用例只使用规范 key，没有覆盖宽前缀误匹配的输入。

### 138 — `src/db/tests/maintenance.rs`（957 行）— 已审

- 覆盖：第 1–957 行，完整阅读；后台错误保留、runtime shutdown、pressure budget、
  平台能力、只读 WAL replay、WASI/browser guard、批量读、reservation、close/flush/
  compaction 等待语义。
- 安全结论：未发现新的独立问题。资源 reservation 的释放和错误路径、native storage
  能力声明、只读重开行为均有较系统的覆盖。
- 测试缺口：close 等待测试手工调用 `begin_activity` 构造活动，而没有通过真实异步
  content/manifest 操作验证活动注册，因此不能消除 F-005。
- 设计观察：compaction reservation 把半开区间 `[a,c)` 与 `[c,e)` 也视为冲突；这是
  偏保守的串行化选择，降低并发度但没有形成正确性问题。

### 139 — `src/content/tests.rs`（4,291 行）— 已审

- 覆盖：第 1–4,291 行，完整阅读；上传恢复、配额、token、租约、physical hold、访问
  barrier、reader-drain attestation、quarantine/grace/sweep、native/object-store/WASI
  持久化、故障注入与请求计数。
- 安全结论：未发现新的独立问题。回收协议对错误 proof、过期 authority、畸形内部记录、
  不匹配 barrier/evidence、并发 lease/hold/activity 和中途对象删除失败均有大量失败
  关闭及幂等性验证。
- F-023 覆盖空白得到确认：sealed session prune 用例只验证旧 descriptor 仍可读取，
  没有在 prune 后用同一 `UploadId` 再次 begin/write；因此没有触及旧 chunk 被同名
  replace 的关键边界。
- F-024 证据得到直接确认：`seal_retry_repairs_descriptor_session_crash_window` 故意让
  Sealing session 写失败，并断言 descriptor 已经可见，然后只测试同一进程内 seal retry；
  它没有覆盖窗口内 abort/inactive reaper 删除 chunks，也没有覆盖进程终止后 Open
  session 被维护流程清理。该测试把危险发布顺序当作可恢复路径记录了下来。
- 设计观察：大部分基于 `thread::sleep(10ms)` 的到期测试可能在极慢或虚拟化时钟环境中
  偶发抖动；更可测试的时钟注入能减少非确定性，但这不构成生产安全问题。

### 140 — `tests/scaffold.rs`（79 行）— 已审

- 覆盖：第 1–79 行，完整阅读；公共 API 脚手架、默认 durability 与 path-open 行为。
- 安全结论：未发现新问题；测试临时目录名只含固定标签与进程号，串行单次运行可用，
  但同进程并发重复执行相同用例时可能碰撞，属于测试夹具稳健性问题。

### 141 — `tests/in_memory_range_delete.rs`（110 行）— 已审

- 覆盖：第 1–110 行，完整阅读；范围删除的点读、扫描、snapshot、prefix 和批内顺序。
- 安全结论：未发现新问题；半开有限范围语义覆盖清晰。
- 测试缺口：没有覆盖无上界 range tombstone 经 flush/compaction 后的 table pruning，
  因而未触及 F-003。

### 142 — `tests/in_memory_transaction.rs`（177 行）— 已审

- 覆盖：第 1–177 行，完整阅读；sequence stamp、点/范围读冲突、命名 bucket 校验。
- 安全结论：未发现新问题；冲突事务不会部分发布 commit-sequence value。
- 覆盖边界：全部使用同一数据库对象，不涉及跨数据库 snapshot 谱系，未触及 F-001。

### 143 — `tests/in_memory_iteration.rs`（181 行）— 已审

- 覆盖：第 1–181 行，完整阅读；正反向 range/prefix、snapshot 可见性与 lazy value。
- 安全结论：未发现新问题；内存路径的迭代顺序与 snapshot 语义覆盖合理。

### 144 — `tests/in_memory_mvcc.rs`（315 行）— 已审

- 覆盖：第 1–315 行，完整阅读；MVCC、snapshot pin、retention window、checkpoint、
  多 bucket batch、失败原子性与批内同 key 顺序。
- 安全结论：未发现新问题。测试验证 snapshot 计数与历史保留，但没有绑定数据库谱系，
  因而不能发现 F-001。
- 小问题：`read_versions_track_latest_and_empty_batches_do_not_advance` 连续重复了同一条
  `first.read_version() == 1` 断言，属于无意义重复。

### 145 — `tests/model_reference.rs`（369 行）— 已审

- 覆盖：第 1–369 行，完整阅读；固定种子随机 point/range-delete/snapshot/flush/
  compaction 操作与 `BTreeMap` 模型对照及重开验证。
- 安全结论：未发现新问题；模型保留最近六个 snapshot，能覆盖常见 MVCC 跨层交互。
- 测试缺口：随机范围只生成两个有限端点，不生成全范围或单侧无界 tombstone，无法捕获
  F-003；随机序列固定且 key/value 空间很小，适合作为稳定回归而不是充分状态空间探索。

### 146 — `tests/async_api.rs`（872 行）— 已审

- 覆盖：第 1–872 行，完整阅读；异步公共 API、WAL replay、只读 open、blob lazy read、
  native/platform I/O 统计、maintenance、未 poll/已接受 Future 的取消语义。
- 安全结论：未发现新的独立问题；已接受写入 Future 被丢弃后仍达到可见终态并可重开，
  未 poll 的 Future 无副作用，契约区分明确。
- 覆盖边界：异步 maintenance 只验证调用在哪类执行器任务运行；没有让 close 与真实
  content/manifest Future 并发，仍未覆盖 F-005。自制 `block_on` 以 10ms 轮询作为
  lost-wake 兜底，只影响测试速度和诊断精度。

### 147 — `tests/browser_shared_worker_wasm.rs`（16 行）— 已审

- 覆盖：第 1–16 行，完整阅读；SharedWorker 下复用浏览器数据库 round-trip。
- 安全结论：未发现新问题；仅为目标环境入口，实质断言位于共享 helper。

### 148 — `tests/browser_dedicated_worker_wasm.rs`（207 行）— 已审

- 覆盖：第 1–207 行，完整阅读；DedicatedWorker 的 OPFS sync access、独占性、计时和
  数据库 round-trip。
- 安全结论：未发现新问题；内嵌 JS 的句柄均在 `finally` 关闭。
- 设计观察：计时 probe 内部 `(iterations - 1)` 假定参数非零；当前唯一调用固定传 64，
  所以不是外部输入或生产路径问题。

### 149 — `tests/support/browser_worker.rs`（124 行）— 已审

- 覆盖：第 1–124 行，完整阅读；浏览器 worker 共用 namespace、WAL/flush/compaction/
  blob round-trip 与只读重开。
- 安全结论：未发现新问题；随机 namespace 仅用于测试隔离，错误转换主动 drop 原错误
  后只保留 display 文本，诊断信息略有损失但不影响库行为。

### 150 — `tests/browser_persistent_wasm.rs`（590 行）— 已审

- 覆盖：第 1–590 行，完整阅读；浏览器内容回收、WAL 重开/temp repair、compaction、
  blob、bucket drop、namespace alias、Web Locks、storage manager 与超大 manifest。
- 安全结论：未发现新问题；超大 manifest 在 decode 前拒绝，临时 WAL 默认失败关闭，
  显式 repair 后才清理。
- 覆盖边界：bucket drop 只在丢弃旧句柄后执行，没有保留旧句柄并同名重建，未触及
  F-002；Web Locks 只验证第二个活跃 writer 无法 open，不验证失效 writer 的对象写
  fencing，未触及 F-007。

### 151 — `tests/internal/persistent_wal.rs`（190 行）— 已审

- 覆盖：第 1–190 行，完整阅读；持久 WAL 集成测试的共享 imports、fixture helper、
  table/blob 枚举、统计查询、文件写入和子模块装配。
- 安全结论：未发现新问题；所有文件破坏 helper 均只操作每个测试生成的临时路径。
- 设计观察：`wait_until` 固定最多等待约 2 秒，压力较高的 CI 可能偶发超时；属于测试
  稳定性而非生产逻辑。

### 152 — `tests/production_maturity.rs`（433 行）— 已审

- 覆盖：第 1–433 行，完整阅读；强制进程退出后的 confirmed-write 恢复、并发混合负载、
  cooperative maintenance、重开核验和 JSONL 报告输出。
- 安全结论：未发现新问题；child 路径和轮次只由父测试传入，报告路径由显式环境变量
  控制，均不属于库的非可信输入面。
- 覆盖边界：三个 maturity 用例默认均为 ignored，因此常规 `cargo test --all-targets`
  不会提供 crash/soak 证据；混合负载只有 point put/delete/get，不覆盖 range tombstone、
  bucket generation、content 或对象后端。

### 153 — `tests/internal/persistent_wal/destructive.rs`（274 行）— 已审

- 覆盖：第 1–274 行，完整阅读；WAL append/persist、table/manifest publish、目录同步、
  WAL rewrite 与 delete 的故障注入。
- 安全结论：未发现新问题；临时文件默认失败关闭、显式 repair 及原文件保留语义清晰。
- F-022 覆盖空白：`WalAppend` fault 在实际写入前返回错误，随后还断言 later write 可以
  成功；没有模拟 `write_all` 已推进部分字节再报错，不能证明损坏 lane 可安全继续。
- F-015 关联：目录同步失败用例只断言下一次 open 可使用已经 rename 的 manifest；
  没有验证原调用方把该错误视为未发布后在同一 handle 重试时的状态一致性。

### 154 — `tests/internal/persistent_wal/durability.rs`（319 行）— 已审

- 覆盖：第 1–319 行，完整阅读；obsolete table reader pin、范围 tombstone GC、
  snapshot retention 与严格 durability。
- 安全结论：未发现新问题；native table/iterator 生命周期有明确 pin 测试。
- 覆盖边界：全部是 native file backend，不能替代 F-006 所需的 object cleanup reader
  生命周期验证；range tombstone 均为有限半开范围，未触及 F-003。

### 155 — `tests/internal/persistent_wal/corruption.rs`（493 行）— 已审

- 覆盖：第 1–493 行，完整阅读；compaction 正确性、缺失/损坏 table、metadata mismatch、
  data-block 延迟校验、filter 与 WAL 尾部损坏。
- 安全结论：未发现新问题；已确认 confirmed WAL 截断与 checksum 破坏失败关闭，而未知
  的最终短尾可忽略。
- 覆盖边界：没有构造“中部部分 frame + 后续完整 frame”，因此未覆盖 F-022。

### 156 — `tests/internal/persistent_wal/flush_memtable.rs`（489 行）— 已审

- 覆盖：第 1–489 行，完整阅读；table/blob flush、immutable pressure、bucket-local
  freeze、事务冲突和 publish 失败清理。
- 安全结论：未发现新问题；失败 flush 保持 memtable 数据并删除未发布 table/blob。
- F-016 覆盖空白：双 bucket pressure 用例只检查选择性 flush 后即时可读，不在该状态
  终止并重开，因此没有验证全局 WAL replay floor 是否错误越过未 flush bucket。

### 157 — `tests/internal/persistent_wal/compaction.rs`（746 行）— 已审

- 覆盖：第 1–746 行，完整阅读；多层 compaction、trivial move、L0 pressure、全范围
  tombstone、budget、background maintenance、output split 与 deep-level 策略。
- 安全结论：未发现新问题；全范围 tombstone-only table 被读取路径纳入候选并有统计断言。
- F-003 覆盖空白：没有构造“同一 table 含普通 point rows 与单侧无界 tombstone，且
  point bounds 不覆盖被删旧表”的组合；全范围 tombstone-only 用例不会触发该剪枝错误。

### 158 — `tests/internal/persistent_wal/blob_gc.rs`（762 行）— 已审

- 覆盖：第 1–762 行，完整阅读；blob level merge、GC、lazy read、发布失败清理、
  snapshot/iterator pin 与 obsolete file 删除。
- 安全结论：未发现新问题；native blob/table 由活跃 iterator pin 住直至释放，随后才
  清理，失败发布也保留旧 manifest 所需对象。
- 覆盖边界：同样未覆盖 object-store reader 与 cleanup 并发，不能消除 F-006。

### 159 — `tests/internal/persistent_wal/recovery.rs`（897 行）— 已审

- 覆盖：第 1–897 行，完整阅读；WAL shard merge、bucket/option manifest、writer lock、
  read-only coexistence、safe temp repair、unreferenced/missing/corrupt 文件与 pending
  blob deletion。
- 安全结论：未发现新问题；正式孤儿文件默认保留供人工检查，只有明确安全的临时文件
  能在显式策略下自动清理。
- 覆盖边界：table id fixture 固定使用 999，但没有模拟重开后 allocator 重新分配已存在
  或曾发布的 id，未触及 F-012。

### 160 — `tests/internal/persistent_wal/read_stats.rs`（985 行）— 已审

- 覆盖：第 1–985 行，完整阅读；扫描浪费、table pruning、filter/level/cache 指标、
  lazy iterator、事务 range、block index、codec 和 tombstone authority。
- 安全结论：未发现新问题；finite range tombstone 与 point/prefix filter 混合 table
  的权威性有专门测试。
- F-003 覆盖空白：混合 table 用例使用 `[user:1,user:2)` 有限范围；没有单侧无界
  tombstone，因此仍未覆盖错误的 table bounds。
- 小问题：文件开头存在多余空行；纯格式问题。

### 161 — `examples/durability.rs`（44 行）— 已审

- 覆盖：第 1–44 行，完整阅读；默认 `SyncAll` 与单次/全局 `SyncAllStrict` 示例。
- 安全结论：未发现新问题；文档准确区分 macOS 普通 `fsync` 与 `F_FULLFSYNC`。
- 设计观察：临时目录只含进程号且启动时递归删除；同一进程并发运行两份示例可能互相
  清理目录，示例夹具宜加入随机 nonce。

### 162 — `examples/sync_quickstart.rs`（73 行）— 已审

- 覆盖：第 1–73 行，完整阅读；同步 bucket/batch/snapshot/prefix/range/transaction、
  flush 和重开。
- 安全结论：未发现新问题；同样存在只用进程号命名临时目录的示例并发碰撞风险。

### 163 — `examples/read_versions.rs`（87 行）— 已审

- 覆盖：第 1–87 行，完整阅读；checkpoint、snapshot retention、删除 checkpoint 后的
 过期行为。
- 安全结论：未发现新问题；示例只在同一数据库中使用 snapshot，没有误导用户跨库复用。
- 设计观察：临时目录仍只用进程号命名并主动递归清理。

### 164 — `examples/platform_io.rs`（125 行）— 已审

- 覆盖：第 1–125 行，完整阅读；platform I/O runtime 选择、完成类型统计、错误上下文和
  最小 executor。
- 安全结论：未发现新问题；`block_on` 的周期 poll 只属于示例兼容执行器。

### 165 — `examples/quickstart.rs`（140 行）— 已审

- 覆盖：第 1–140 行，完整阅读；异步公共 API、lazy iterator、事务、flush、只读重开及
  最小 executor。
- 安全结论：未发现新问题；临时路径并发碰撞问题与同步 quickstart 相同。

### 166 — `examples/user_store.rs`（204 行）— 已审

- 覆盖：第 1–204 行，完整阅读；应用层 user codec、prefix list 与条件 rename 事务。
- 安全结论：未发现库级新问题；长度使用 `checked_add` 并验证 UTF-8/尾随数据。
- 示例设计风险：`user:{id}` 未对 id 做 namespace 编码，任意 id 中的分隔字符可能破坏
  应用层 key 约定；真实应用应使用长度前缀或转义后的复合键。临时目录也只含进程号。

### 167 — `examples/event_index.rs`（205 行）— 已审

- 覆盖：第 1–205 行，完整阅读；事件主表与 account 二级索引的原子 batch、codec 和
  prefix 查询。
- 安全结论：未发现库级新问题；索引指向缺失主记录时明确报告 corruption。
- 示例设计风险：`account/{account_id}/event/{event_id}` 直接拼接未编码字段；含 `/` 的
  account/event id 会造成前缀边界歧义。若示例面向非可信标识，应改用长度前缀复合键。

### 168 — `benches/v1_bench.rs`（443 行）— 已审

- 覆盖：第 1–443 行，完整阅读；Criterion 分组、命令行入口、重复轮次、CSV/摘要输出。
- 安全结论：仅 benchmark 驱动，不进入库生产路径；未发现新的库级问题。
- 工具稳健性：`TRINE_BENCH_RUNS` 没有合理上限，误设超大值会造成极长运行和大量输出；
  建议解析后限制轮次并在拒绝时给出明确提示。

### 169 — `benches/v1_bench/fixtures.rs`（370 行）— 已审

- 覆盖：第 1–370 行，完整阅读；临时目录、数据生成、同步/异步数据库夹具、点读 checksum
  和 label helper。
- 安全结论：未发现新的库级问题；临时目录带进程号与原子 nonce，隔离性优于 examples。
- 内部前置条件未编码：`batched_point_read_checksum` 的 `batch_size == 0` 会触发
  `chunks(0)` panic，`repeated_bytes` 的空 prefix/非零 len 会无限循环，`seed_index`
  的空集合会除零；现有调用均传入合法常量，宜用 `NonZeroUsize`、assert 或返回错误表达。
- `Box::leak` 用于 Criterion 的动态 `'static` label；单轮规模有界，但重复运行会持续
  累积这些字符串，建议由 runner 持有 label 或预生成固定 label 集。

### 170 — `benches/v1_bench/writes.rs`（105 行）— 已审

- 覆盖：第 1–105 行，完整阅读；单条/批量写入与不同 durability 的吞吐测试。
- 安全结论：未发现新问题；每次 iteration 使用独立键，避免无意只测覆盖写。
- 设计观察：同步、异步模式的准备和计时边界一致，错误均由 benchmark 明确终止。

### 171 — `benches/v1_bench/transactions_wal.rs`（207 行）— 已审

- 覆盖：第 1–207 行，完整阅读；事务提交、WAL durability、并发与 shard 场景。
- 安全结论：未发现新问题；并发数和批大小均为固定非零值。
- 覆盖边界：只测成功写入吞吐，不注入 append/persist 中途失败，不能为 F-022 提供反证。

### 172 — `benches/v1_bench/bulk_workloads.rs`（222 行）— 已审

- 覆盖：第 1–222 行，完整阅读；bulk load、混合读写、批量扫描与数据准备。
- 安全结论：未发现新问题；数据规模与键生成均由固定 benchmark 参数约束。
- 设计观察：该文件重在端到端吞吐，未把准备阶段计入热路径，计时边界合理。

### 173 — `benches/v1_bench/point_reads.rs`（382 行）— 已审

- 覆盖：第 1–382 行，完整阅读；memtable/table/blob、命中/未命中、批量点读与 checksum。
- 安全结论：未发现新问题；读结果被 checksum 消费，避免优化器消除工作。
- 覆盖边界：固定正数 batch size 使 fixtures 中的零 batch 前置条件不会在当前用例触发。

### 174 — `benches/v1_bench/cache.rs`（264 行）— 已审

- 覆盖：第 1–264 行，完整阅读；block/blob cache 冷热命中、容量压力与统计采样。
- 安全结论：未发现新问题；容量和迭代规模均受 benchmark 常量控制。
- 设计观察：缓存预热发生在计时外，冷热场景区分清楚；统计只作诊断，不参与正确性判断。

### 175 — `benches/v1_bench/runtime_codec.rs`（305 行）— 已审

- 覆盖：第 1–305 行，完整阅读；runtime 调度、blocking task、CRC/LZ4 编解码微基准。
- 安全结论：未发现新问题；buffer 尺寸固定且 checksum/解码结果被消费。
- 解释边界：LZ4 项直接测底层 codec，不等同于完整 table/blob 格式路径；输出说明应避免
  被当作数据库端到端压缩开销。

### 176 — `benches/v1_bench/write_path.rs`（294 行）— 已审

- 覆盖：第 1–294 行，完整阅读；写路径吞吐、诊断 delta、memtable/WAL 指标输出。
- 安全结论：未发现新的库级问题。
- benchmark 统计缺陷：`record_delta` 对 `wal_bytes_pending_sync` 累加的是每次采样的绝对
  gauge，而不是 `before/after` 差值；汇总标签却像累计字节，长跑时会系统性高估并误导
  写路径判断。应报告最终/峰值 gauge，或明确计算有意义的变化量。

### 177 — `benches/v1_bench/blob_maintenance.rs`（347 行）— 已审

- 覆盖：第 1–347 行，完整阅读；blob flush、GC、维护放大与指标快照。
- 安全结论：未发现新问题；维护调用的错误均传播给 benchmark runner。
- 覆盖边界：使用 native backend 和正常发布流程，不覆盖 F-006/F-007 的 object-store
  reader lifetime 与 fencing 问题。

### 178 — `benches/v1_bench/maintenance.rs`（458 行）— 已审

- 覆盖：第 1–458 行，完整阅读；flush/compaction、后台 contention、worker 数量和
  maintenance latency。
- 安全结论：未发现新问题；0/1 worker 对照与计时范围明确。
- 设计观察：固定负载适合相对比较，但不构成 crash durability 或故障路径验证。

### 179 — `benches/v1_bench/read_tail.rs`（386 行）— 已审

- 覆盖：第 1–386 行，完整阅读；读尾延迟、并发 group commit、后台 compaction 和
  percentile 汇总。
- 安全结论：未发现新的库级问题；percentile 对空输入有保护并夹紧索引。
- benchmark 可信度问题：后台 compactor 对 `put_sync`、`flush_sync`、`compact_sync`
  错误全部使用 `let _ =` 丢弃；维护失败时仍可能输出看似有效的延迟结果。应记录首个
  后台错误并使本轮失败，至少在报告中标明 degraded run。

### 180 — `benches/v1_bench/cold_reads.rs`（1,059 行）— 已审

- 覆盖：第 1–1,059 行，完整阅读；反复重开、只读打开、L0/read pruning、range/prefix
  guard、storage request/latency delta 与诊断结果展开。
- 安全结论：未发现新的库级问题；所有计数差值和累加采用饱和运算，固定 batch size
  非零，读取结果均用断言/checksum 验证。
- benchmark 口径：反复 drop/reopen 会清空数据库实例内缓存，但不会驱逐操作系统页缓存；
  “cold”结果应解释为库内冷启动/首次读取，不应当作物理设备冷缓存延迟。
- 覆盖边界：range guard 仍只使用有限半开 tombstone，不能消除 F-003 的单侧无界
  tombstone 剪枝问题；诊断只测正常成功路径，不覆盖 I/O 故障后的指标可信度。

## 问题清单

### F-001 — 跨数据库 Snapshot 未校验谱系，可读取另一数据库的错误版本

- 严重度：**中**（正确性/租户隔离；若应用把不同安全域放在同一进程中，可能升级）。
- 证据：
  - `Snapshot` 只有 `read_sequence` 与 tracker pin，没有数据库身份（`src/snapshot.rs`）。
  - `Bucket::*_at` 仅转交 `snapshot.read_sequence()` 和 `is_pinned()`（`src/bucket.rs`）。
  - 目标 DB 看到 `read_pin_held = true` 后不会为自身版本建立 pin
    （`src/db/sync_api/maintenance/background.rs` 第 197–244 行）。
- 公开 API 行为验证：数据库 A 写入 `k=A` 并创建 snapshot；数据库 B 写入 `k=B`；调用
  `snapshot_a.get_sync(&bucket_b, b"k")` 成功返回 `Some(b"B")`，没有拒绝跨谱系组合。
- 影响：违反 `ReadVersion` 明示的数据库作用域契约；可能返回错误历史版本，且错误地
  跳过目标数据库的 retention pin，使并发压实时的一致性保证失效。
- 建议：Snapshot 持有不可伪造的数据库 lineage/token（例如 `Arc` 身份或稳定
  database id），所有 snapshot/reader 入口先做同源验证；不要用单一 `is_pinned`
  布尔值替代“当前 DB 的该版本已被 pin”证明。

### F-002 — 删除并同名重建 Bucket 后，旧句柄点读泄露旧代数据且 API 互相矛盾

- 严重度：**高**（删除语义/数据隔离）。
- 证据：
  - `Bucket` 永久保存创建时的 `Arc<LsmTree>`。
  - 普通可写/本地 DB 的 `latest_read_state()` 返回该旧 state；snapshot 点读也直接
    使用 `self.state`。
  - range/prefix 路径按 bucket 名回查当前 registry，因此与点读使用不同代。
- 公开 API 行为验证：`scratch` 写入 `k=secret` → drop bucket → 同名重建并写入
  `k=public` →
  用旧句柄和当前 snapshot：
  - `stale.get_at_sync(..., b"k") == Some(b"secret")`
  - `stale.range_at_sync(..., KeyRange::all()) == [(k, public)]`
- 影响：删除 bucket 后仍可通过旧句柄读取被删除代的数据；同一句柄的点读和范围读
  对同一 snapshot 返回互相矛盾的数据。若 drop 被用于租户删除/权限撤销，这是直接的
  进程内数据隔离破坏。
- 建议：给 bucket registry 条目增加 generation/id，句柄每次操作验证 generation；
  drop 时使旧 LSM state 的读写 admission 都失效。所有 point/range/prefix 路径必须
  统一解析同一代，不能一部分用保存的 Arc、另一部分按名称回查。

### F-003 — 混合 table 中的半无限范围墓碑被错误 key bounds 剪枝，删除后旧值恢复可见

- 严重度：**高**（删除正确性/旧数据重新可见）。
- 静态证据链：
  - `src/table/metadata.rs::table_key_bounds` 仅在 tombstone 两端都能由
    `finite_bound_bytes` 取出时更新 bounds；`Unbounded` 任一端会 `continue`。
  - 同 table 有点记录时，properties 因点记录而具有非空精确 bounds，不会触发
    “无 bounds 时墓碑 table 保守匹配全部”的兜底。
  - `src/lsm/version.rs::range_tombstone_tables_for_key` 对 L0 直接要求
    `table.key_bounds_may_contain_key(key)`；L1+ 也先按同一 bounds 选 table。
- 可达场景：旧 table 保存 key `a`；后续同一 flush 的新 table 包含远处 point key
  `m` 与 `(-∞, c)` range delete。新 table properties 只记录 `[m,m]`，读取 `a` 时
  跳过新 table 的墓碑，旧 table 的 `a` 因而重新可见。右侧半无限范围同理。
- 建议：在 properties 中显式编码 unbounded/`bounds_complete`，并让所有 table
  candidate 选择在 bounds 不完整时保守包含；不能只写 empty/empty，因为当前
  `has_key_bounds` 在 `data_block_count > 0` 时仍把它当精确边界。增加“点记录 +
  左/右半无限墓碑 + 旧 table”的 flush、reopen、compaction 回归测试。

### F-004 — 公共配置允许产生自身持久化格式无法编码的合法写入

- 严重度：**中**（持久化可用性/资源耗尽）。
- 静态证据链：
  - `DbOptions::DEFAULT_MAX_KEY_BYTES` 与允许的 `MAX_WRITE_FIELD_BYTES` 都等于
    `codec::MAX_DECODED_BLOCK_BYTES`（64 MiB）。
  - `validate_common_options` 接受该边界值；commit 校验也允许 `key.len()` 等于配置
    上限。
  - table data block 除 key 外还需保存 record count、key length、sequence、kind、
    batch index、value tag、restart 信息和 hash index，因此单条记录 payload 必然
    大于 key 本身。
  - `BlockManager::append_checked` 最终以同一个 64 MiB 常量拒绝超大 decoded block。
  - `validate_bucket_options` 仅检查 `block_bytes` 非零，未把它夹到格式硬上限以内。
- 影响：公开 API 可以成功接纳并写入 WAL/memtable 的数据或配置，但后续 flush/压实
  无法生成 table；持续写入会令不可刷新的 immutable memtable/WAL 积压，最终触发
  写入停顿或资源耗尽。半无限范围墓碑边界以及超大 `block_bytes` 聚合也有同类风险。
- 建议：按最坏记录和 block 元数据开销推导真正的 `max_key_bytes`；在打开时拒绝超过
  安全上限的 `block_bytes`，并保证 tombstone block 也可切分。增加“精确字段上限提交
  后 flush/reopen”和“超大 block 配置”的边界条件回归测试。

### F-005 — 异步写入/维护未完整登记 close barrier，关闭返回后仍可继续发布

- 严重度：**高**（writer lease 生命周期/跨进程一致性）。
- 证据：
  - `PublishBarrier::close` 只等待 `begin_activity()` 登记的活动归零，然后 close 路径
    释放 filesystem 或 object-store writer lease。
  - 同步 commit 和 WASM commit 会登记 activity；native async commit 没有。
  - object-store flush/compaction 也不登记；`storage.rs` 第 250–254 行甚至明确写出
    原因是“close 对 object storage 是 no-op”。
  - 实际 `Db::close` 对 object-store 因没有 native path 会调用 `close_sync`，后者调用
    `substrate.release_writer_lease()`；object WAL worker 把 lease expiry 置零并退出。
- 影响：close 可先返回并允许下一 writer 取得所有权，而旧 future 仍在写 WAL/table、
  CAS manifest 或发布 memtable。object manifest 在没有新 writer 抢先 claim epoch 时
  仍可能接受 close 后的旧 epoch 写；native 路径则可能在 OS lock 已交给另一进程后继续
  完成原提交。关闭边界不再表示“此句柄不会继续改变 durable state”。
- 建议：所有会越过 await/后台队列的提交、flush、compaction、bucket/metadata publish
  在接纳点取得可跨 await 的 owned activity token；close 先停止 admission，再等待全部
  token，最后释放 lease。为 native async commit 和 object-store maintenance 增加
  “操作已接纳 → 并发 close → close 必须等待”的并发回归测试。

### F-006 — object-store 清理会删除并发 reader 尚未取得的 WAL/table/blob

- 严重度：**高**（稳定读视图/单写多读可用性）。
- 证据：
  - `cleanup_object_store_orphans_async` 只以 writer 当前 manifest 的引用 ID 建集合，
    list 后直接删除集合外所有 table/blob。
  - compaction 发布新 manifest 后，旧输入 table 立刻成为集合外对象；维护 API 随后
    自动调用该 GC。
  - 已返回的延迟 blob 值没有登记到 GC；远端 read-only Db 更不共享 blob
    liveness/snapshot tracker。
  - WAL rewrite 在 CAS 新 head 后立即删除旧 chain；read-only open 先读取 head 快照，
    再完成 table 加载并读取 WAL，期间没有 reader pin。
  - 项目对象存储文档明确把“单 writer + 多 reader，reader 节点从缓存对象服务查询”
    作为支持的并发模型。
- 影响：table 在成功打开后会整对象缓存在内存，因此普通既有 table 扫描不会因随后
  删除而中断；但 read-only open/refresh 在“读到旧 manifest、尚未加载完全部 table”
  的窗口可失败，捕获的旧 WAL head 也可能在读取前消失。更关键的是 blob 独立延迟
  读取：旧 reader 或已返回的 `LazyValue` 在 writer 重写索引并清理后会收到对象缺失，
  point-in-time view 不再完整可读。
- 建议：采用带代际的两阶段 mark/sweep 和足够的保留窗口，或建立可观测的 reader
  epoch/lease；至少本进程先尊重 blob 活跃引用。远端模型若无法提供 reader liveness
  证明，就必须给不可变对象定义明确且保守的最短保留期。增加延迟 blob、并发
  read-only open/refresh 跨 compaction/GC 的回归测试。

### F-007 — object-store table/blob IO 未受 writer epoch fencing，可被旧 writer 覆盖或删除

- 严重度：**高**（持久数据完整性/分布式 fencing 失效）。
- 静态证据链：
  - lease 允许过期 owner 被更高 epoch 接管；这是正常故障恢复路径。
  - table ID 从 manifest 状态顺序分配。旧 writer 若在 ID 分配后停顿而未发布，新
    writer 会从相同 manifest 分配同一 ID。
  - `ObjectStoreBackend::write_object` 对 table/blob key 使用无条件 `put`，不传 epoch、
    owner、ETag 或 `IfNoneMatch`；delete 也无条件。
  - 旧 writer 先写对象、后做 manifest CAS。即使后者因较低 epoch 正确返回 fenced，
    对同名对象的覆盖已经发生。stale orphan GC 还可按旧 manifest 删除新 writer 对象。
- 影响：新 writer 的 manifest 可能继续引用已被旧 writer 替换的 table/blob。旧对象
  自身 checksum 可以完全有效，若 properties 外形相同，错误数据甚至可能在 reopen/
  reader 节点上静默通过；否则数据库变为 corruption/不可打开。该链直接破坏“fencing
  后旧 writer 不能影响 durable state”的核心保证。
- 建议：不可变对象名至少包含 fencing epoch + owner nonce（或全局随机内容身份），
  创建使用 `IfNoneMatch`，冲突时只接受经过内容摘要验证的完全相同对象。GC 也必须先
  证明当前 owner/manifest generation，并使用条件删除或延迟代际清理；不能让旧进程
  对当前命名空间执行无条件 delete。增加“旧 writer 在对象 PUT 前暂停 → lease 接管 →
  新 writer 发布 → 旧 writer 恢复”的确定性并发回归测试。

### F-008 — object-store 打开与列举接口强制无界整批物化

- 严重度：**中**（可用性/规模上限）。
- 证据：
  - `ObjectStoreBackend::open_read` 对每个 table 做 HEAD 后一次读取完整对象，并将全部
    bytes 常驻 `ObjectStoreReadObject`。
  - 打开数据库会遍历 manifest 的全部 table 并逐一 open；只有单对象大小上限，没有
    累计打开字节、并发/总量预算或按需加载。
  - `ObjectClient::list` 的返回类型固定为 `Vec<ObjectMeta>`；GC 必须先物化前缀下全部
    元数据，trait 层没有 continuation token/streaming 的实现空间。
- 影响：合法的大数据库可仅因 table 总量在冷启动时耗尽内存；失败写入积累的大量
  orphan 或被污染的独占 prefix 也可令“用于回收空间”的 GC 自身先耗尽内存。S3
  adapter 虽用 100,000 条上限避免继续增长，但超限后没有分页处理能力，清理会永久
  失败。对象存储后端因此无法达到文档所描述的按 working-set 冷启动模型。
- 建议：table read handle 保留 client/key/size/ETag 并按 footer/index/block 范围
  读取，只缓存实际工作集；为 open 增加累计元数据预算。把 list 改为分页游标或 async
  stream，让 GC 分页 mark/sweep 并限制每轮工作量。

### F-009 — object WAL 顺序锁使 group commit 退化为每提交固定等待 5 ms

- 严重度：**中**（吞吐/延迟设计）。
- 证据：
  - object-store commit 在取得 `object_wal_commit_order` 后才分配 sequence，并一直
    持锁到 `accept_commit` 同步等待 worker 返回。
  - worker 的 `collect_object_wal_accepts` 收到首条后用 5 ms `recv_timeout` 等待更多
    Accept，以实现 group commit。
  - 其他普通 commit 必须先取得同一个顺序锁才能发送 Accept，因此等待窗口内不可能
    贡献第二条；首条每次仍完整支付 timeout。
- 影响：默认 Flush durability 下，单是人为收集窗口就把串行提交延迟增加约 5 ms，并
  将单 Db 吞吐压到约 200 次提交/秒量级，同时没有获得批量对象写/CAS 的收益。代码的
  复杂 group 分支和错误分发也基本成为不可达设计。
- 建议：在顺序锁下只完成 sequence reservation 和有序入队，随后释放锁并分别等待
  completion；worker 才能在固定 deadline 内收集连续 sequence。批量失败必须保留每个
  调用可识别的 `Fenced`/`Corruption` 分类。增加并发提交确实合并为一个 segment、首条
  不因滚动 timeout 无限等待的性能回归测试。

### F-010 — 超长 object-store prefix 可写出读取端拒绝的 lease 状态

- 严重度：**中**（持久化自兼容/数据库不可继续写与不可重开）。
- 证据：
  - `canonical_object_prefix` 只校验 NUL、父目录和规范形式，不限制字节长度。
  - WAL segment key 包含完整数据库 prefix；lease 的 `current_wal_key` 因而也包含它。
  - `encode_lease_state` 只要求 key 长度可转成 `u32`，没有执行
    `OBJECT_LEASE_MAX_BYTES`（64 KiB）上限。
  - `read_lease_state` 在任何解码前按 HEAD size 拒绝超过 64 KiB 的 lease。
- 影响：支持长 key 的自定义/内存 ObjectClient 可让首次 durable commit 成功 CAS 一个
  过大的 lease head；定时续租错误随后被静默丢弃，下一 commit、read-only open 和重开
  都会报告 corruption。真实 provider 较小的 key 上限可能更早报错，但公共抽象并未把
  该限制作为要求，不能依赖它修正格式矛盾。
- 建议：打开时按“prefix + 最长 WAL 文件名 + v3 header”计算并拒绝不可能编码的
  prefix；`encode_lease_state` 在写前也必须执行相同硬上限。更稳妥的是 lease 只保存
  数据库根下的相对 WAL key，读取后再做规范化和根目录约束。增加临界 prefix 的
  commit/renew/reopen 边界条件回归测试。

### F-011 — S3 adapter 与 Trine key 规范不一致，造成 namespace 别名和清理失效

- 严重度：**中**（命名空间隔离/持久对象泄漏）。
- 证据：
  - `canonical_object_prefix` 保留绝对形式的前导 `/`，也保留空格、`%` 等合法字符。
  - adapter 对每个入参调用 `object_store::path::Path::from`；该类型不保留前导空组件，
    并把 raw PathPart 编码成 provider location。
  - list 把 `meta.location.as_ref()` 原样返回；`ObjectStoreBackend::list_objects` 再拿
    此值与原始 Trine root 做 `path.parent() == root` 比较。
- 影响：`tenant` 与 `/tenant` 在 Trine 中是不同 canonical prefix，却由 adapter 映射
  到同一物理对象前缀；带前导 `/` 或需编码字符的数据库，其普通 get/put 可正常，
  但 table/blob orphan GC 与按 listing 的 WAL 清理看不到自己的对象，空间持续泄漏。
  reclamation probe 的 exact-key listing 校验也会对这种 prefix 产生假失败。
- 建议：数据库打开时统一采用 `object_store::Path` 能往返的单一、相对、编码后 key
  规范；拒绝会发生别名的形式，或在 adapter 边界保存可逆映射。list 返回前按该规范
  还原 key 并显式排序，不要依赖上游未承诺的顺序。增加 `/tenant`、空格、百分号与
  Unicode prefix 的 put/list/GC/reopen 边界条件测试。

### F-012 — table id 没有持久高水位/原子预留，可重号并破坏当前表

- 严重度：**高**（持久数据损坏/跨 bucket 数据混淆）。
- 证据：
  - `ManifestState::next_table_id` 只扫描当前 manifest 中的 live tables；manifest
    没有 `next_table_id` 高水位，删除最高 id 的 bucket 会使计数回退。
  - flush、compaction 和 blob rewrite 都在耗时写表前调用该 getter，本身不预留 id。
  - `MaintenanceCoordinator` 只禁止同一 bucket 的并发 compaction，明确允许不同
    bucket 并发；它们可从同一 manifest 快照取得同一个起始 id。
  - `ManifestStore::add_tables`/replacement 不检查重复；object 版本也只在目标 bucket
    中查重，没有强制全 manifest 的 id 唯一。
  - table/blob 文件名是仅由数字 id 决定的数据库全局路径；native obsolete 清理最终
    也只按旧 `TableProperties.id` 重新构造删除路径。
- 影响：并发 builder 可同时写同一个 table 文件/对象，两个 bucket 的 manifest 条目
  之后还可能同时引用同一 id，造成覆盖、校验失败或跨 bucket 错误数据。另一条无需
  并发 builder 的路径是：保留旧 table `Arc` → drop 持有最高 id 的 bucket → 新 flush
  重用该 id → 旧引用释放；延迟 obsolete 清理会删除新 manifest 正在引用的文件，
  数据库随后读失败或重开报损坏。
- 建议：在 manifest 格式中持久化永不回退的 table-id 高水位；构建任务必须在一个
  串行 durable edit 中一次性预留连续 id 区间，再写对象。发布入口再次验证所有
  table id 在全 manifest 唯一，并让清理携带不可重用的对象 generation/内容身份，
  不能只按裸数字 id 删除。增加多 worker、不同 bucket 并发 compaction，以及
  drop/重建后延迟释放旧 reader 的确定性回归测试。

### F-013 — 未生效的 object manifest 写故障被误判为冲突并无限忙重试

- 严重度：**中**（故障放大/请求永久挂起）。
- 证据：
  - `ObjectManifestStore::try_publish` 在 `put_if` 返回任意错误后 readback；仅当当前
    state 精确等于 intended next state 才确认成功，除此之外一律返回 `Conflict`，
    没有区分“当前仍等于原 base，证明本次写没有生效”。
  - `ObjectManifestStore::commit_edit` 和 `ManifestStore::commit_edit_async` 对
    `Conflict` 都执行无次数上限、无 backoff、无 cancellation check 的立即循环。
  - 现有测试只覆盖“服务端已应用、客户端丢响应”，没有覆盖“服务端拒写/写路径持续
    故障而 GET/HEAD 正常”的常见分离故障。
- 影响：对象存储写权限撤销、PUT 路由故障、配额/服务异常但读取仍正常时，bucket、
  checkpoint、flush/compaction manifest publish 等调用不会返回原始 I/O 错误，而会
  持续 GET + PUT，既挂住调用又放大后端负载；close/cancellation 也无法及时收敛。
- 建议：保留原 base state/ETag。readback 等于 intended 时确认成功；与 base 不同才
  视为真实 CAS conflict；仍等于 base 时返回原始写错误。对真正 conflict 加总重试
  预算、抖动退避和 cancellation/fencing 检查。增加“写前失败且状态未变”“已应用后
  丢响应”“真实竞争者获胜”三分支的确定性回归测试。

### F-014 — 新建 snapshot/checkpoint 可被已经规划的压实越过并丢失所需版本

- 严重度：**高**（MVCC/检查点正确性，可能静默返回错误历史）。
- 证据：
  - `Snapshot` 只 pin sequence，不捕获 `Arc<LsmVersion>` 或具体 table 集合。
  - compaction 在 `prepare_compaction_run`/object 等路径开头读取一次
    `oldest_retained_sequence`，随后用该固定值构建输出；发布前只检查 input tables
    仍 current，不重新比较 retention floor。
  - snapshot tracker 的 pin/oldest 查询虽然各自在同一 mutex 下，但压实释放该 mutex
    后仍可长时间构建；稍后加入的更老 pin 无法影响已经计算出的裁剪决策。
  - `snapshot()` 的 visible-sequence load 与 pin 不是一个跨 commit/compaction 的原子
    边界；`snapshot_at` 的“仍 retained”判断/插入 pin 也未与在途 compaction 发布互斥。
- 可达时序：压实按 floor `S+1` 开始并准备丢弃仅版本 `S` 需要的记录；在它发布前，
  `snapshot_at(S)` 根据尚未替换的当前 LSM 成功返回并 pin `S`；压实随后仍发布输出，
  后续通过该 snapshot 的读取只能从已经裁剪的 current version 查找，可能得到新值、
  `None` 或与此前读取不同的结果。checkpoint 也可永久记录这个已丢失的版本。
- 建议：让 snapshot 捕获并读取一个不可变 `Arc<LsmVersion>`（跨 bucket 需要一致的
  database read-state），或给 retention admission/compaction publish 建立 generation
  协议：压实记录规划 generation/floor，发布前在同一同步边界确认期间未加入更低 pin，
  否则废弃输出重建。checkpoint 必须先完成同样的安全 pin，再 durable publish。
  增加带 barrier 的并发回归测试：压实取 floor 后暂停、建立旧 snapshot/checkpoint、
  再放行发布并验证历史读保持一致。

### F-015 — native manifest 在 rename 后同步失败会被当成未发布并删除已引用输出

- 严重度：**高**（确定性的磁盘/内存分叉与数据库损坏）。
- 证据：
  - `publish_manifest_to_native_file` 先 `fs::rename(MANIFEST.tmp, MANIFEST)`，随后在
    `SyncAll` 模式同步父目录；后一步失败时以普通 `Err` 返回，但新 manifest 已是当前
    namespace 可见版本，无法再按“发布没发生”处理。
  - `publish_manifest_with_backend` 只有完整 `Ok` 才返回 `Published`，`ManifestStore`
    因而不更新内存 state，也没有读回协调或“结果不确定”状态。
  - compaction 在任何 publish error 后删除所有新 output table；flush 及其他发布调用
    也依赖相同的二值错误模型。新磁盘 manifest 随即可能引用这些已删除文件。
- 影响：一次父目录打开/fsync 故障即可让当前进程继续持旧 manifest，而磁盘已经是新
  manifest；压实/flush 清理进一步把新 manifest 引用的表删除。随后的读取、close/reopen
  会报缺失或损坏，且调用者只收到普通 I/O 错误，无法知道需要立刻停止使用句柄。
- 建议：publication API 必须区分 `NotPublished`、`Published`、`OutcomeUnknown`。
  rename 后任何错误都不得删除 candidate outputs或继续使用旧内存状态；应在独占 writer
  lease 下 readback 当前 manifest，若等于 intended 就安装并把 durability failure
  单独上报，若无法确认则强制关闭/隔离句柄。为“temp sync 前失败”“rename 前失败”
  “rename 后目录 sync 失败”分别设置 fault point 和状态机回归测试。

### F-016 — 部分 bucket flush 错误推进全局 WAL replay floor，崩溃后可丢数据

- 严重度：**高**（已确认提交的持久数据丢失）。
- 证据：
  - `write_flush_inputs*` 将选中 inputs 的
    `max(input.freeze_sequence)` 直接传给 manifest `add_tables`，后者覆盖全局
    `wal_replay_floor`；成功后 WAL rewrite 只保留该 floor 之后的提交。
  - `collect_flush_inputs_with_budget` 只收集已经 frozen 的 immutable memtables，不会
    自动冻结/刷出其他 bucket 的 active memtable。
  - pressure 版本只处理 immutable 数达到 `max_immutable_memtables` 的 bucket；低压力
    bucket 即使有更老 immutable 也会被跳过。
  - commit sequence 是数据库全局的，并不保证本轮所刷 bucket 的最大 sequence 小于
    所有其他 bucket 尚未落表记录的最小 sequence。
- 可达时序：bucket B 在 sequence 1 写入小值并留在 active memtable；bucket A 持续写入
  并在 sequence N 冻结，后台只把 A 刷成 table；manifest floor 被推进到 N，WAL rewrite
  删除/忽略 `<= N` 的记录。B 尚未 flush 时进程崩溃，重开既无 B 的 table，也不会重放
  sequence 1，已成功返回的值永久消失。
- 建议：replay floor 必须是“所有 bucket 中任何尚未落表记录的最小 sequence减一”，
  并按完整 commit batch 考虑跨 bucket 原子性；不能由本轮输出的最大值推导。可先采用
  保守方案：只有冻结并成功刷出所有 bucket 的 checkpoint/full flush 才推进 floor，
  后台部分 flush 保持旧 floor。增加双 bucket 回归测试：B 小写留 active、A 触发后台
  flush、模拟重开并验证 B 仍存在；再覆盖预算截断与跨 bucket 单 batch。

### F-017 — blob 整文件校验缺少累计解压预算，可将 256 MiB 文件膨胀到数十 GiB

- 严重度：**中**（内存拒绝服务/合法数据库不可重开）。
- 证据：
  - `decode_blob_file` 限制输入文件为 256 MiB，但 `decode_records` 循环调用
    `decode_record_body`，每条分别允许解压到 64 MiB，并把所有 value 保留在 Vec。
  - 没有累计 `value_len`、record count、总解压 bytes 或分配预算；LZ4 高压缩比允许
    数百至上千个大 decoded value 塞进编码上限。
  - recovery 的 sync/async invalid-blob 检查会完整 decode 每个 referenced blob；
    async properties 读取也错误地走 full decode，browser open 可能重复多次。
  - GC 将全库候选 live records 汇成单文件；encoder 只在末尾限制编码 bytes，因而可由
    正常数据生成累计 decoded size 远高于文件上限、但格式完全合法的 blob。
- 影响：打开、恢复或 GC candidate 扫描可出现数十 GiB 级峰值、OOM/进程终止；单文件
  限制给调用者造成错误安全感。encoder 在最终拒绝超限前也会累积完整 bytes、body 和
  克隆 values，形成写侧内存峰值。
- 建议：格式/decoder 增加累计 decoded bytes 与 record count 硬上限，在任何解压和
  `Vec` 增长前扣减 budget；recovery 校验改为逐 record streaming 校验，不保留 values。
  async properties 实现应像 sync 版只读 header/footer/properties。GC 按目标 decoded
  bytes 分片输出多个 blob，encoder 增量统计并在越界前终止，不克隆完整 record values。
  增加大量高压缩比小 encoded record 的边界条件测试。

### F-018 — async inline blob 不复用文件句柄，使 object-store 打开产生 R×整对象下载

- 严重度：**中**（启动/刷新可用性与远端流量费用）。
- 证据：
  - `inline_blob_values_with_backend_async` 逐 record 调用
    `read_value_for_internal_key_with_backend_async`，没有像 sync 版的
    `BTreeMap<file_id, CachedBlobFile>`。
  - 每个 `BlobIndex` 读取都会重新 `open_read` 同一 file id；object-store backend 的
    `open_read` 不是轻量 range handle，而是 HEAD 后完整 GET 并把 bytes 放进 `Arc`。
  - object-store open/refresh 调用 `buckets_from_manifest_async(..., inline_blob_values =
    true)`，因此这一路径会在数据库对外可用前执行。
- 影响：一个 64 MiB blob 若含 1,000 条被 table 引用的记录，单次打开可传输约 64 GiB
  而非约 64 MiB，并反复分配同一对象；大量 table 时会造成超时、OOM、远端限流或显著
  egress 成本。输入完全可以由正常工作负载形成。
- 建议：object read handle 必须支持真正 range read且不预取整对象；async inline 至少
  按 file id 复用 handle/metadata，并对并发、累计读取 bytes 和打开对象数设预算。更好
  的方案是 table 保留 lazy blob references，按用户实际读取需求获取值。增加一个 blob
  多记录的计数型 ObjectClient 回归测试，断言 open/refresh 的 GET 次数与 blob 文件数
  成正比，而不是与 record 数成正比。

### F-019 — native blocking manifest 读取在格式上限检查前无界分配

- 严重度：**中**（打开阶段内存拒绝服务/进程终止）。
- 证据：
  - `max_whole_object_read_bytes(StorageObjectKind::Manifest)` 将 manifest 限为
    `MAX_MANIFEST_PAYLOAD_BYTES + 14`，约 16 MiB。
  - platform-I/O 的 `read_current_manifest` 将这个 max 传给 optional bounded read，
    会在分配前比较 open handle 的 metadata，并以 `max + 1` 限制实际读取。
  - 普通 native blocking/non-platform 路径最终调用
    `read_current_manifest_from_native_file`，其中直接执行 `fs::read(object.path())`，
    没有 metadata/stream budget。
  - `decode_manifest` 的 payload 上限只在整个文件已经进入 `Arc<[u8]>` 后执行。
- 影响：损坏、误操作或不可信数据库目录中的超大 `MANIFEST` 会在 `Db::open_sync` 的
  早期触发与文件大小成比例的内存申请和读取；进程可能在获得可处理的 Corruption 错误
  前就 OOM。是否启用 platform-io 会改变抗损坏边界。
- 建议：复用 bounded optional-read helper：从同一已打开 handle 取 metadata，若大于
  object-kind max 立即返回 Corruption，再以 `max + 1` 的 `take`/受限循环读取并复查
  EOF/长度；不要用裸 `fs::read`。增加 sparse/oversized manifest 的边界条件测试，断言
  在小额读取和分配内失败。

### F-020 — native async borrowed read 在 Future poll 线程执行阻塞文件 I/O

- 严重度：**中**（async executor 饥饿/请求可用性）。
- 证据：
  - `NativeFileObject::read_exact_at` 构造的 Future 内直接调用
    `read_exact_at_offset`，后者锁 `std::fs::File`、seek 并 `Read::read_exact`。
  - 该方法没有检查 `platform_io`，也不通过 `Runtime::spawn_blocking_result`；与同类型
    `read_exact_at_owned` 的 platform/blocking-adapter 分流明显不对称。
  - async blob codec 的 header/footer/record/value 路径
    `read_blob_exact_at_async` 正是调用 borrowed `read_exact_at(...).await`。
  - 单 value 允许达到 64 MiB，慢盘、网络挂载或资源压力下，单次 poll 可阻塞 executor
    线程很长时间；同线程上的取消、lease refresh、提交和其他数据库 Future 都无法推进。
- 影响：选择 `RuntimeMode::PlatformIo` 仍不能保证 async blob random read 非阻塞；受控
  的大 blob read 或慢文件可造成事件循环延迟尖峰，在单线程 executor 上接近全局停顿。
  对应 I/O 还不计入 inline/platform task 指标，诊断会错误显示 async 覆盖率。
- 建议：async storage 内部统一使用 owned-buffer completion；先校验 len/预算，再把
  owned buffer 交给 platform driver 或 bounded blocking adapter，await 后复制/返回。
  若必须保留 borrowed API，应在 native async backend 明确拒绝或注明会阻塞，不能在
  async blob 热路径使用。增加慢 read fake/backend 的调度回归测试，验证并行 timer/
  maintenance Future 在大 blob read 期间仍能推进。

### F-021 — async WAL admission 在 bounded queue 满时同步阻塞 executor

- 严重度：**中**（async 可用性/调度饥饿）。
- 证据：
  - 每个 native WAL lane 使用容量 64 的 `std::sync::mpsc::sync_channel`。
  - `accept_commit_async` 在 `.await` waiter 前调用 `enqueue_wal_lane_command`；后者用
    `SyncSender::send`，容量耗尽时同步阻塞当前线程，没有 Future/waker 或 try-send
    backpressure。
  - lane worker 可在 file write/fsync、confirmed marker publish 或 platform completion
    上长时间停顿，足以让并发 async commits 填满 queue。
- 影响：第 65 个以上同 shard async 提交可能把单线程 executor 整体堵住；同 executor
  上的超时、取消、close、maintenance 和其他数据库请求不能运行。多线程 executor 也会
  被大量 blocked worker 消耗，违背 async API 的非阻塞调度预期。
- 建议：改用真正 async 的 bounded channel，send capacity 通过 Future 等待；或使用
  `try_send` 并立即返回明确的 RuntimeBusy/Backpressure，让上层重试。同步 API 可保留
  blocking send。增加容量设为 1、lane I/O barrier 卡住时的回归测试，断言第二个 async
  send Pending/返回背压但 executor heartbeat 继续推进。

### F-022 — WAL partial append 失败后 lane 继续写，后续已确认提交可永久无法恢复

- 严重度：**高**（持久数据库拒绝打开/已确认提交不可用）。
- 证据：
  - native append 最终调用 `File::write_all`；真实 I/O 错误允许在返回 Err 前已写入 frame
    的任意前缀，尤其 ENOSPC、quota、设备/网络文件系统故障。
  - `process_wal_lane_batch` 的 append error 只完成当前 reply；不记录 append 前长度、
    不 truncate、不 drop writer、不把 lane 标为 failed，也不拒绝后续命令。
  - 后续提交继续通过同一 O_APPEND handle，把新完整 frame 接在残片之后，并可成功
    persist、写 confirmed marker、对调用方返回成功。
  - recovery 的 `decode_frames_after` 只把不完整数据作为可忽略的**最终尾部**；残片后
    一旦还有 bytes，它会形成坏 header/payload/checksum并返回 corruption，没有 resync
    到下一 magic 的安全机制。
- 影响：一次部分 WAL 写失败后，即使存储恢复且后续提交成功，重开数据库也会在中部坏
  frame 失败；更晚已经确认且可能严格 sync 的数据全部不可用。当前 fault injection 在
  write 前失败，覆盖不到 write-all 的合法部分进展。
- 建议：append 前记录可信 file length；任何 append error 后立即 fail-stop lane，关闭
  writer并拒绝后续 commit。恢复/显式修复时只允许把**最终**不完整 frame 截到已验证
  boundary，且要同步截断；不能扫描 magic 猜测跳过中部 bytes。若希望在线恢复，则在
  独占 lane 下 truncate 回起始长度并 sync 成功后才能重新开放。增加可注入 short-write
  后 Err 的回归测试：后续 admission 必须失败，修复/重开不得越过残片误接新提交。

### F-023 — sealed state 清理后可重用 UploadId，覆盖仍被 descriptor 引用的不可变 chunk

- 严重度：**高**（已发布 immutable content 被破坏/跨内容别名）。
- 证据：
  - `prune_sealed_content_uploads` 只删除 `uploads/{id}.trineu` session；文档明确 descriptor
    和它选择的 chunks 保留。
  - `UploadId::from_bytes` 和 `begin_content_upload_with_id` 是公共 API；后者在 session
    缺失时直接用同一个 id 创建新 Open state，不检查该 id 是否仍被任一 descriptor 引用。
  - chunk object 路径固定为 `chunks/{upload_id}/{index}.trinec`；所有 backend 的
    `write_content_object` 都是 replace 写，而不是 generation/create-only 写。
  - `ContentAttachment::upload_id()` 会把原上传 id 返回给调用者，因此该 id 不是不可知
    的内部随机值；即使不知道别人的 id，应用对自己的 sealed upload 做正常 prune 后
    重试旧请求也能触发。
- 影响：新上传第一次写 chunk 0 就可能覆盖旧内容的物理字节。旧 descriptor 仍固定到
  同一个 upload id；旧读取会报 digest mismatch/返回不可用，若新上传继续 seal，不同
  ContentId 的两个 descriptor 还会引用同一组不断变化的 chunks，违背不可变内容契约。
- 建议：物理 chunk namespace 使用不可复用的内部 generation，而不是公开 UploadId；
  descriptor 保存 generation。或者永久保留轻量 UploadId tombstone/反向引用，并在
  begin 时拒绝曾 seal 的 id。写 chunk 还应校验当前 session generation。增加回归测试：
  seal A、清理 sealed session、以相同 id 上传不同字节，断言 begin 被拒绝且 A 仍能
  verify/read。

### F-024 — descriptor 在 Sealing recovery marker 之前发布，失败后可见内容会被当作 Open 清理

- 严重度：**高**（发布原子性破坏/已返回 handle 的内容丢失）。
- 证据：
  - `prepare_open_upload_seal` 在新 ContentId 路径先
    `write_content_descriptor(...)`，随后才构造并写 `Sealing` session。
  - 这两个是独立 storage object replace；descriptor 写成功后，session 写可报错或进程
    可终止。旧持久 session 此时仍为 Open，没有记录已经发布的 ContentId。
  - `open_content_unchecked` 只要求 descriptor 存在且格式正确；leased open/hold 还能在
    control 缺失时通过 `stage_content_*_activity` 创建 Active control，因此窗口中的
    内容真实可见、可返回读取句柄。
  - `abort_content_upload`、inactive reaper 和 Aborting 续跑会依据 Open state 删除该
    UploadId 的所有 chunks，但不知道要同步删除刚发布的 descriptor。
- 影响：一次 seal 后续状态写失败会留下“可发现 descriptor + 无 recovery marker”。
  调用者按 API 对仍为 Open 的上传 abort，或维护任务清理它后，descriptor/control/handle
  继续存在但 chunks 已删除；不可变内容永久转为 corruption/NotFound，quota 和 token
  状态也可能不完整。
- 建议：先持久写包含最终 ContentId、expiry、durability、descriptor claims 的 Sealing
  marker，再幂等发布 descriptor；Sealing 恢复必须允许 descriptor 尚不存在并完成它。
  descriptor 发布后再推进 quota/token/Sealed。若保留现顺序，则必须用单一事务性日志
  或让 Open recovery 能从 descriptor 反向识别并禁止 abort。增加 descriptor 成功、
  Sealing state 写失败的故障注入回归测试，断言 resume 能完成且 abort/reaper 不删除
  已发布 chunks。

## 自动化检查记录

- `cargo test --all-targets`：通过。库测试 574 个通过；其他集成测试通过；
  production maturity 的 3 个破坏性/soak 测试按配置被忽略；all-targets 也成功执行
  benchmark harness。
- `cargo test --all-targets --all-features`：通过。库测试 580 个通过、3 个需要真实
  S3/外部凭据的测试被忽略；全部本机集成测试通过，production maturity 的 3 个长时/
  强制退出测试仍按配置忽略；benchmark harness 和全部 examples 成功运行。
- `cargo fmt --all -- --check`：通过。
- `cargo clippy --all-targets --all-features -- -D warnings`：通过，零 warning。
- `cargo audit`：使用最新 RustSec 数据库扫描 `Cargo.lock` 的 297 个依赖，未发现已登记
  的安全公告。
- `cargo tree -d --target all`：重复版本仅来自目标平台兼容依赖链，主要是 `getrandom`
  与三代 `windows-sys`；没有 git/path 覆盖依赖，lockfile 中 registry package 均带 checksum。
- `cargo check --target wasm32-unknown-unknown --lib --all-features`：通过。
- `cargo clippy --target wasm32-unknown-unknown --lib --all-features -- -D warnings`：
  通过，零 warning。
- `RUSTFLAGS="-D warnings" cargo check --target wasm32-wasip1 --lib --all-features`：
  通过，零 warning。
- WASI lib 与三个 browser integration target 的 `cargo test --no-run` 均通过；当前
  macOS 环境没有配置 WASI/browser test runner，因此没有把不可执行 `.wasm` 误记为
  运行时通过。
- `cargo check --target aarch64-unknown-linux-gnu --features platform-io-native`：通过。
- `cargo check --target aarch64-unknown-linux-gnu --lib`：通过。
- Linux `--all-features` 交叉检查被本机缺少 `aarch64-linux-gnu-gcc` 阻断于
  `aws-lc-sys` C 构建；本机 macOS 的 S3/all-feature 构建、Clippy 和测试均已通过，
  因此该项记录为环境覆盖缺口，不判为源码失败。
- `unsafe` 逐处复核：生产代码共有 7 个实际 unsafe block/expression——2 个 macOS
  `fcntl/fsync` FFI、5 个 Apple DispatchIO FFI；均局部允许并附有安全依据，未发现裸指针
  越过回调生命周期、未检查长度进入指针运算或手工所有权重建。
- `cargo package --allow-dirty --no-verify --list`：发布清单只包含 Cargo/许可证/README、
  examples 与 `src/`，没有把本地配置、审计文档或 benchmark 误打包。
- `native_async_close_waits_for_active_publish_before_releasing_lease` 与真实 async
  commit/content/maintenance 路径现在共享同一 activity guard；close 只能在这些操作
  完成后释放 lease。

## 覆盖清单

- [x] `src/` Rust 文件（139）
- [x] `tests/` Rust 文件（21）
- [x] `examples/` Rust 文件（7）
- [x] `benches/` Rust 文件（13）
