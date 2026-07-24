# 🧸 AI Memory Gateway

**让你的 AI 拥有长期记忆。**

一个轻量级转发网关，在你和 LLM 之间加一层记忆系统。支持任何 OpenAI 兼容客户端（Kelivo、ChatBox、NextChat 等）和任何 LLM 服务商（OpenRouter、OpenAI、本地 Ollama 等）。

---

## ✨ 功能

- **自定义人设** — system prompt 每次对话自动注入
- **记忆宫殿** — 七房间架构，自动提取 / 事件盒打包 / 每日审视 / 向量搜索 / 导入导出
- **日印象** — AI 对每天对话生成日记式总结，带标签和心情
- **用户画像** — 自动生成并维护用户人格画像，支持增量更新（只看新增记忆，不全量重抽）
- **分区缓存** — A/B 区轮转 + 摘要压缩，利用 prompt caching 大幅省 token
- **对话线管理** — 跨平台对话衔接，多对话线切换
- **对话记录** — 浏览、搜索、批量管理，支持 session 合并
- **Token 统计** — 自动记录 token 消耗，按 session 汇总
- **全端点鉴权** — `GATEWAY_SECRET` 保护所有 API
- **设置面板** — 网页端管理所有配置，热更新无需重启
- **性能诊断** — 可选的 API 性能日志，设置页一键开关
- **零成本起步** — Render / Zeabur 免费额度即可部署


## 🏗️ 架构

```
你的客户端（Kelivo / ChatBox / ...）
        ↓
   AI Memory Gateway（本项目）
   ├── 注入 system prompt（人设）
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

```
[人设区]  system prompt，永远不变    ← 缓存命中
[摘要区]  历史压缩摘要              ← 正常轮次命中
[历史A区] 15轮原始消息              ← 正常轮次命中
[历史B区] 当前周期消息              ← 通过lookback命中
[当前输入] 时间+记忆+用户消息       ← 不缓存
```

| 环境变量 | 说明 | 示例 |
|---------|------|------|
| `CACHE_PARTITION_ENABLED` | 分区缓存开关 | `true` |
| `CACHE_PARTITION_X` | 轮转周期（轮数） | `15` |
| `CACHE_SUMMARY_MODEL` | 摘要压缩模型 | `anthropic/claude-haiku-4.5` |
| `PARTITION_SESSION_ID` | 固定 session ID | `my-thread` |
| `CACHE_PARTITION_TRIGGER`（可选） | 轮转触发：`rounds` 或 `time` | `rounds` |
| `CACHE_PARTITION_WINDOW`（可选） | 时间窗口（分钟） | `30` |

> 💡 不需要记忆功能也能用分区缓存：`MEMORY_ENABLED=true` + `MEMORY_EXTRACT_ENABLED=false` + `CACHE_PARTITION_ENABLED=true`

## 📁 文件说明

```
ai-memory-gateway/
├── main.py                    # 网关主程序
├── database.py                # 数据库操作（PostgreSQL）
├── memory_extractor.py        # AI 记忆提取
├── system_prompt.txt          # AI 人设（自行编辑）
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

> **向量搜索：** 默认 jieba 中文分词 + 关键词匹配。设置 `MEMORY_VECTOR_ENABLED=true` + `EMBEDDING_API_KEY` 启用语义搜索，四维加权排序。支持 pgvector 自动检测。

## ❓ 常见问题

**Q: 部署后 502？** 检查端口，Render 用 `PORT` 环境变量，设为 `8000`。

**Q: 数据库连接失败？** 连接字符串末尾加 `?sslmode=require`。

**Q: 怎么备份？** Dashboard「导出备份」下载 JSON，迁移后「导入记忆」恢复。

**Q: 不会写代码能搞吗？** 能，部署看文档，管理在 Dashboard 点按钮。

## 📋 更新日志

### v4.0（2026-07）

- **记忆宫殿** — 全新七房间架构，替代原有三层记忆
  - 节点自动提取 + 事件盒打包 + 每日审视（记忆消化）
  - 手动创建/编辑/删除节点，事件盒压缩/解绑/撤销
  - 从对话历史批量提取记忆，支持预览后确认
- **日印象** — AI 对每天对话生成日记式总结，带标签和心情
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

- 分区缓存、对话线管理、对话记录管理、Token 统计、架构拆分

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