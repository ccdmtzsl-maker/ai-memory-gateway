# 🧸 AI Memory Gateway

**让你的 AI 拥有长期记忆。**

一个轻量级转发网关，在你和 LLM 之间加一层记忆系统。支持任何 OpenAI 兼容客户端（Kelivo、ChatBox、NextChat 等）和任何 LLM 服务商（OpenRouter、OpenAI、本地 Ollama 等）。

---

## ✨ 功能

- **上游系统消息透传** — 直接使用客户端发来的 system prompt，网关不覆盖
- **记忆宫殿** — 七房间架构，自动提取 / 事件盒打包 / 每日审视 / 向量搜索 / 导入导出
- **日印象** — AI 对每天对话生成日记式总结，带标签和心情，支持自定义提示词
- **用户画像** — 自动生成并维护用户人格画像，支持增量更新（只看新增记忆，不全量重抽）
- **分区缓存** — A/B 区轮转，利用 prompt caching 大幅省 token
- **图片归档** — 对话图片存 Cloudflare R2，数据库只留引用；离开缓存区自动清理
- **对话线管理** — 跨平台对话衔接，多对话线切换
- **对话记录** — 浏览、搜索、批量管理，支持 session 合并
- **全端点鉴权** — `GATEWAY_SECRET` 保护所有 API
- **设置面板** — 网页端管理所有配置，热更新无需重启
- **性能诊断** — 可选的 API 性能日志，设置页一键开关
- **零成本起步** — Render / Zeabur 免费额度即可部署


## 🏗️ 架构

```
你的客户端（Kelivo / ChatBox / ...）
        ↓
   AI Memory Gateway（本项目）
   ├── 搜索相关记忆 → 注入上下文
   ├── 转发请求 → LLM API
   └── 后台提取新记忆 → 存入数据库
        ↓
   LLM API（OpenRouter / OpenAI / Ollama / ...）
```

## 🚀 快速开始

### 第一阶段：纯转发网关（不需要数据库）

1. Fork 或上传代码到 GitHub 仓库
2. 注册 [Render](https://render.com)，创建 Web Service → 连接仓库
3. 设置环境变量：

| 环境变量 | 说明 | 示例 |
|---------|------|------|
| `API_KEY` | LLM API Key | `sk-or-v1-xxxx` |
| `API_BASE_URL` | LLM API 地址 | `https://openrouter.ai/api/v1/chat/completions` |
| `DEFAULT_MODEL` | 默认模型 | `anthropic/claude-sonnet-4.5` |
| `PORT` | 端口 | `8000` |
| `GATEWAY_SECRET`（可选） | 鉴权密钥 | `your-secret-key` |

4. 部署，访问看到 `{"status":"running"}` 就成功了
5. 客户端 API 地址填 `https://你的网关地址/v1`，API Key 随便填

> ⚠️ Render 免费层无活动时会休眠，首次访问需等几十秒。其他平台（Zeabur、Railway、Fly.io）也行。

### 第二阶段：加上记忆系统

加一个 PostgreSQL 数据库即可开启全部记忆功能。

| 环境变量 | 说明 | 示例 |
|---------|------|------|
| `DATABASE_URL` | PostgreSQL 连接字符串 | `postgresql://user:pass@host:port/db` |
| `MEMORY_ENABLED` | 开启记忆 | `true` |
| `MEMORY_MODEL` | 提取记忆用的模型 | `anthropic/claude-haiku-4.5` |
| `MAX_MEMORIES_INJECT` | 每次注入最大记忆条数 | `15` |
| `MIN_SCORE_THRESHOLD` | 记忆搜索最低分数阈值 | `0.15` |
| `TIMEZONE_HOURS` | 时区偏移（小时） | `8` |
| `MEMORY_EXTRACT_ENABLED`（可选） | 记忆提取总开关 | `true` |
| `FORCE_STREAM`（可选） | 强制流式传输 | `false` |
| `REASONING_EFFORT`（可选） | 推理强度 | 留空不注入 |

部署后访问 `/dashboard` 打开管理页面。

### 第三阶段：分区缓存（省 token 费）

A/B 区轮转管理对话上下文，利用 prompt caching 大幅降低 token 开销。每聊 N 轮自动轮转一次，旧消息区走缓存读取（0.1x 价格）。

| 环境变量 | 说明 | 示例 |
|---------|------|------|
| `CACHE_PARTITION_ENABLED` | 分区缓存开关 | `true` |
| `CACHE_PARTITION_X` | 轮转周期（轮数） | `15` |
| `PARTITION_SESSION_ID` | 固定 session ID | `my-thread` |
| `CACHE_PARTITION_TRIGGER`（可选） | 轮转触发：`rounds` 或 `time` | `rounds` |
| `CACHE_PARTITION_WINDOW`（可选） | 时间窗口（分钟） | `30` |

> 💡 不需要记忆功能也能用分区缓存：`MEMORY_ENABLED=true` + `MEMORY_EXTRACT_ENABLED=false` + `CACHE_PARTITION_ENABLED=true`

### 第四阶段：图片归档（可选）

把对话里的图片存到 Cloudflare R2，数据库只保存引用链接。一张图的 base64 约11 万字符，直接入库会撑爆 `conversations` 表；归档后每条记录只留 300 字符左右的JSON 引用。

**前置条件**：必须先开启分区缓存（`CACHE_PARTITION_ENABLED=true`）。图片的生命周期跟随缓存区，没有分区就没有清理边界。

| 环境变量 | 说明 | 示例 |
|---------|------|------|
| `IMAGE_ARCHIVE_ENABLED` | 图片归档开关 | `true` |
| `R2_ENDPOINT` | R2 S3 兼容端点 | `https://<account_id>.r2.cloudflarestorage.com` |
| `R2_ACCESS_KEY` | R2 API Token 的 Access Key ID | `abc123...` |
| `R2_SECRET_KEY` | R2 API Token 的 Secret Access Key | `def456...` |
| `R2_BUCKET` | bucket 名称 | `chat-images` |
| `R2_PUBLIC_URL` | bucket 的公开访问域名 | `https://pub-xxxx.r2.dev` |

**Cloudflare 侧准备**：

1. 创建 R2 bucket
2. 在 bucket 设置里开启 **Public Development URL**（否则仪表盘加载不出图片）
3. 创建 API Token，权限选**Object Read & Write**（只给读会导致上传和删除都 403）

**工作方式**：

```
客户端发带图消息
  ├─ 转发上游：原图 base64 原样传递，模型正常看图
  └─ 落库前：图片上传 R2，content 存成 JSON
```

落库后的 `content` 长这样，文字和图片保持原始顺序：

```json
[{"type": "text", "text": "看这张图"},
 {"type": "image_ref", "url": "https://pub-xxx.r2.dev/conversation-images/ab/abc...def.jpg",
  "mime": "image/jpeg", "sha256": "abc...def", "size": 82004}]
```

**自动清理规则**：
| 触发场景 | 行为 |
|------|------|
| 消息离开缓存区（A/B 区轮转） | 剔除 `image_ref` 引用 + 删除 R2 文件 |
| 删除会话 / 批量删除 | 删除该会话所有图片 |
| 删除单条消息 | 删除该消息独占的图片 |
| 编辑消息移除图片 | 删除不再被引用的图片 |

清理后消息在数据库里变成纯文字，不留占位符。删除前会检查是否有其他消息引用同一张图（sha256 去重会让多条消息共用一个文件），有引用则跳过。

**设计细节**：

- **内容寻址**：object key 是 `conversation-images/{sha前2位}/{完整sha256}.{ext}`，不含时间戳。同一张图无论何时、哪个会话、进程是否重启，永远指向同一个对象——这保证了 re-roll 时生成的 JSON 完全一致，不会重复存储消息记录。
- **静默降级**：开关关闭、配置不全、上传失败时，一律退回纯文本保存（与未启用时行为一致），不影响对话。
- **记忆隔离**：记忆提取和向量检索读到的是 `[图片附件]` 占位文本，URL 和 JSON 结构不会污染提取内容。
- **删除白名单**：只删除 URL 前缀匹配 `R2_PUBLIC_URL` 的对象，外部链接无法触发删除。
- **零新增依赖**：AWS SigV4 签名手写实现，不引入 boto3。

> 💡 配置状态可在 设置 → 其他 底部查看（只读展示，改值需改环境变量并重启）。

## 📁 文件说明

```
ai-memory-gateway/
├── main.py                    # 网关主程序
├── database.py                # 数据库操作（PostgreSQL）
├── memory_extractor.py        # AI 记忆提取
├── requirements.txt           # Python 依赖
├── Dockerfile                 # 容器配置
├── templates/
│   └── dashboard.html         # 主控制台页面
├── static/
│   ├── css/
│   │   ├── dashboard.css
│   │   └── main.css
│   └── js/
│       ├── dashboard.js       # 主控制台脚本
│       ├── memory_palace.js   # 记忆宫殿模块
│       └── daily_impressions.js # 日印象模块
├── LICENSE
└── README.md
```

## 🌐 支持的 LLM 服务商

| 服务商 | API_BASE_URL |
|--------|-------------|
| OpenRouter | `https://openrouter.ai/api/v1/chat/completions` |
| OpenAI | `https://api.openai.com/v1/chat/completions` |
| Ollama（本地） | `http://localhost:11434/v1/chat/completions` |

## 💡 记忆系统原理

1. **你发消息** → 网关搜索相关记忆
2. **记忆注入** → 相关记忆 + 应用规则拼接到 system prompt
3. **AI 回复** → 网关捕获完整回复
4. **后台提取** → 小模型从对话中提取关键信息
5. **存入数据库** → 下次可检索

> **向量搜索：** 默认 jieba 中文分词 + 关键词匹配。设置 `MEMORY_VECTOR_ENABLED=true` + `EMBEDDING_API_KEY` 启用语义搜索，关键词 + 语义相似度混合排序。支持 pgvector 自动检测。

## ❓ 常见问题

**Q: 部署后 502？** 检查端口，Render 用 `PORT` 环境变量，设为 `8000`。

**Q: 数据库连接失败？** 连接字符串末尾加 `?sslmode=require`。

**Q: 怎么备份？** Dashboard「导出备份」下载 JSON，迁移后「导入记忆」恢复。

**Q: 不会写代码能搞吗？** 能，部署看文档，管理在 Dashboard 点按钮。

## 📋 更新日志

### v4.1（2026-07）

- **图片归档** — 对话图片存 Cloudflare R2，数据库只保存引用链接
  - 内容寻址的 object key，同图去重、re-roll 幂等
  - 生命周期跟随缓存区：离开 A/B 区或消息被删除时自动清理 R2 文件
  - 删除前检查跨消息引用，避免共享图片被误删
  - 仪表盘对话记录渲染缩略图，编辑文字不丢图
  - 记忆提取端转 `[图片附件]` 占位，防止 URL 污染
  - 需配合分区缓存开启；未启用时完全不影响原有行为
- **用户画像 v4.0** — 改为混合式结构：必填核心（整体印象 + 当前状态）+ 自选标签
  - 20 个标签池分四组（价值与喜恶 / 思维与能力 / 情绪与相处 / 生活与关注），模型按证据自选
  - 白名单外的自创标签归入「其他」，不再丢弃
  - 注入文本按组分块输出，替代原本的扁平列表
  - 增量更新时保留旧标签，模型漏写自动补回

### v4.0（2026-07）

- **记忆宫殿** — 全新七房间架构，替代原有三层记忆
  - 节点自动提取 + 事件盒打包 + 每日审视（记忆消化）
  - 手动创建/编辑/删除节点，事件盒压缩/解绑/撤销
  - 从对话历史批量提取记忆，支持预览后确认
- **日印象** — AI 对每天对话生成日记式总结，带标签和心情，支持自定义提示词
- **用户画像** — 自动生成用户人格画像（价值观、行为模式、情绪特征等）
  - 增量更新：通过消费水位线只取新增记忆，不全量重抽
  - 待处理记忆数量显示，更新原则约束（允许替换旧印象）
- **对话列表** — 包含没有消息的空对话线
- **性能诊断开关** — 设置页一键控制，默认关闭，重启后从数据库恢复
- **合并接口** — 记忆宫殿房间列表+节点一次查询，减少请求次数
- **后端缓存** — 记忆宫殿、日印象、用户画像、导出统计等接口 15 分钟缓存
- **加载优化** — 日印象/用户画像有数据时跳过"加载中"闪烁

### v3.6（2026-05-10）

- 时间窗口模式、非 Claude 模型兼容、时区修复

### v3.5（2026-05-06）

- 设置面板、模型列表 API、Dashboard 美化

### v3.3（2026-05-05）

- 三层记忆架构、记忆整理、手动合并、撤回合并、软删除、全端点鉴权

### v3.2（2026-05-04）

- Tool 消息精确去重、Race condition 防护、reasoning_content 存储、对话线重命名

### v3.1（2026-05-02）

- 记忆向量搜索、自动 embedding、pgvector 自动检测、TF-IDF 关键词提取

### v3.0（2026-05-01）

- 分区缓存、对话线管理、对话记录管理、架构拆分

### v2.5（2026-03-06）

- jieba 分词、最低分数阈值、流式修复、推理参数注入

### v2.0（2026-03-01）

- 完整上下文提取、记忆注入提示词优化

### v1.0（2026-02-26）

- 初始版本

## 📄 许可证

[MIT License](LICENSE) — 随便用，改了也不用告诉我。

## 🙏 致谢

> "记忆库不是数据库，是家。"