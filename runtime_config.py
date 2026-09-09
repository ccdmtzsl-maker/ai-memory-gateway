"""运行时配置 —— 热更新配置的集中管理。

这个模块解决 main.py 原有的 globals()[key] = value 热更新问题：
那种写法只在 main.py 自己的命名空间生效，其他模块 import 到的是值拷贝。

改用单例对象 cfg 持有所有配置，任何模块 `from runtime_config import cfg`
拿到的都是同一个对象，设置页改 cfg.MEMORY_ENABLED 后全局立即生效。

注意：这里只放会被热更新的配置变量，启动时只读的配置（如 TIMEZONE_HOURS）
仍然放在各自模块里从环境变量读取。
"""

import os


class RuntimeConfig:
    """运行时可热更新的配置单例。
    
    启动时从环境变量加载，运行时通过 /api/settings 修改。
    所有模块共享同一个实例，确保热更新全局生效。
    """
    
    def __init__(self):
        # ── LLM API 配置 ──────────────────────────────────────
        self.API_BASE_URL: str = os.getenv("API_BASE_URL", "")
        self.API_KEY: str = os.getenv("API_KEY", "")
        self.DEFAULT_MODEL: str = os.getenv("DEFAULT_MODEL", "anthropic/claude-3.5-sonnet")
        self.CHAT_TEMPERATURE: str = os.getenv("CHAT_TEMPERATURE", "1")
        self.REASONING_EFFORT: str = os.getenv("REASONING_EFFORT", "medium")
        self.FORCE_STREAM: bool = self._parse_bool(os.getenv("FORCE_STREAM", "false"))
        
        # ── 记忆系统配置 ──────────────────────────────────────
        self.MEMORY_ENABLED: bool = self._parse_bool(os.getenv("MEMORY_ENABLED", "true"))
        self.MEMORY_API_KEY: str = os.getenv("MEMORY_API_KEY", "")
        self.MEMORY_API_BASE_URL: str = os.getenv("MEMORY_API_BASE_URL", "")
        
        # ── 记忆宫殿配置 ──────────────────────────────────────
        self.MEMORY_PALACE_DEFAULT_LIMIT: int = int(os.getenv("MEMORY_PALACE_DEFAULT_LIMIT", "10"))
        self.MEMORY_PALACE_INJECTION_DEPTH: int = int(os.getenv("MEMORY_PALACE_INJECTION_DEPTH", "1"))
        
        # ── 分区缓存配置 ──────────────────────────────────────
        self.CACHE_PARTITION_ENABLED: bool = self._parse_bool(os.getenv("CACHE_PARTITION_ENABLED", "false"))
        self.CACHE_PARTITION_X: int = int(os.getenv("CACHE_PARTITION_X", "8192"))
        self.CACHE_PARTITION_B_LIMIT: int = int(os.getenv("CACHE_PARTITION_B_LIMIT", "3"))
        self.CACHE_PARTITION_EXTRACT_LIMIT: int = int(os.getenv("CACHE_PARTITION_EXTRACT_LIMIT", "100"))
        self.CACHE_PARTITION_TRIGGER: str = os.getenv("CACHE_PARTITION_TRIGGER", "auto")
        self.CACHE_PARTITION_WINDOW: int = int(os.getenv("CACHE_PARTITION_WINDOW", "20"))
        self.CACHE_PARTITION_KEEP_A_TOOLS: bool = self._parse_bool(os.getenv("CACHE_PARTITION_KEEP_A_TOOLS", "false"))
        self.CACHE_SUMMARY_MODEL: str = os.getenv("CACHE_SUMMARY_MODEL", "")
        
        # ── 关键词上下文配置 ──────────────────────────────────
        self.KEYWORD_CONTEXT_ENABLED: bool = self._parse_bool(os.getenv("KEYWORD_CONTEXT_ENABLED", "false"))
        self.KEYWORD_CONTEXT_RULES: str = os.getenv("KEYWORD_CONTEXT_RULES", "")
        
        # ── 上下文模板配置 ────────────────────────────────────
        self.CONTEXT_TEMPLATE_ENABLED: bool = self._parse_bool(os.getenv("CONTEXT_TEMPLATE_ENABLED", "false"))
        self.CONTEXT_TEMPLATE: str = os.getenv("CONTEXT_TEMPLATE", "")
        
        # ── 时间戳配置 ────────────────────────────────────────
        self.SPARSE_TIMESTAMP_ENABLED: bool = self._parse_bool(os.getenv("SPARSE_TIMESTAMP_ENABLED", "false"))
        
        # ── 人设配置 ──────────────────────────────────────────
        self.USER_NICKNAME: str = os.getenv("USER_NICKNAME", "用户")
        self.CHARACTER_NAME: str = os.getenv("CHARACTER_NAME", "澈")
        
        # ── 调试与诊断配置 ────────────────────────────────────
        self.TOOL_CHAIN_DEBUG: bool = self._parse_bool(os.getenv("TOOL_CHAIN_DEBUG", "false"))
        self.PERF_DIAGNOSTIC_ENABLED: bool = self._parse_bool(os.getenv("PERF_DIAGNOSTIC_ENABLED", "false"))
        
        # ── 响应转换配置 ──────────────────────────────────────
        self.RESPONSE_TRANSFORM_ENABLED: bool = self._parse_bool(os.getenv("RESPONSE_TRANSFORM_ENABLED", "false"))
        self.RESPONSE_TRANSFORM_RULES: str = os.getenv("RESPONSE_TRANSFORM_RULES", "")
    
    @staticmethod
    def _parse_bool(v) -> bool:
        """解析布尔值：支持 true/false、1/0、yes/no。"""
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.strip().lower() in ("true", "1", "yes")
        return bool(v)
    
    def to_dict(self) -> dict:
        """导出所有配置为字典（供 /api/settings 读取）。"""
        return {
            "API_BASE_URL": self.API_BASE_URL,
            "API_KEY": self.API_KEY,
            "DEFAULT_MODEL": self.DEFAULT_MODEL,
            "CHAT_TEMPERATURE": self.CHAT_TEMPERATURE,
            "REASONING_EFFORT": self.REASONING_EFFORT,
            "FORCE_STREAM": self.FORCE_STREAM,
            "MEMORY_ENABLED": self.MEMORY_ENABLED,
            "MEMORY_API_KEY": self.MEMORY_API_KEY,
            "MEMORY_API_BASE_URL": self.MEMORY_API_BASE_URL,
            "MEMORY_PALACE_DEFAULT_LIMIT": self.MEMORY_PALACE_DEFAULT_LIMIT,
            "MEMORY_PALACE_INJECTION_DEPTH": self.MEMORY_PALACE_INJECTION_DEPTH,
            "CACHE_PARTITION_ENABLED": self.CACHE_PARTITION_ENABLED,
            "CACHE_PARTITION_X": self.CACHE_PARTITION_X,
            "CACHE_PARTITION_B_LIMIT": self.CACHE_PARTITION_B_LIMIT,
            "CACHE_PARTITION_EXTRACT_LIMIT": self.CACHE_PARTITION_EXTRACT_LIMIT,
            "CACHE_PARTITION_TRIGGER": self.CACHE_PARTITION_TRIGGER,
            "CACHE_PARTITION_WINDOW": self.CACHE_PARTITION_WINDOW,
            "CACHE_PARTITION_KEEP_A_TOOLS": self.CACHE_PARTITION_KEEP_A_TOOLS,
            "CACHE_SUMMARY_MODEL": self.CACHE_SUMMARY_MODEL,
            "KEYWORD_CONTEXT_ENABLED": self.KEYWORD_CONTEXT_ENABLED,
            "KEYWORD_CONTEXT_RULES": self.KEYWORD_CONTEXT_RULES,
            "CONTEXT_TEMPLATE_ENABLED": self.CONTEXT_TEMPLATE_ENABLED,
            "CONTEXT_TEMPLATE": self.CONTEXT_TEMPLATE,
            "SPARSE_TIMESTAMP_ENABLED": self.SPARSE_TIMESTAMP_ENABLED,
            "USER_NICKNAME": self.USER_NICKNAME,
            "CHARACTER_NAME": self.CHARACTER_NAME,
            "TOOL_CHAIN_DEBUG": self.TOOL_CHAIN_DEBUG,
            "PERF_DIAGNOSTIC_ENABLED": self.PERF_DIAGNOSTIC_ENABLED,
            "RESPONSE_TRANSFORM_ENABLED": self.RESPONSE_TRANSFORM_ENABLED,
            "RESPONSE_TRANSFORM_RULES": self.RESPONSE_TRANSFORM_RULES,
        }
    
    def update(self, key: str, value):
        """热更新单个配置项（供 /api/settings 调用）。
        
        会自动做类型转换，确保和初始化时的类型一致。
        """
        if not hasattr(self, key):
            raise ValueError(f"Unknown config key: {key}")
        
        # 获取原有值的类型
        original = getattr(self, key)
        
        # 类型转换
        if isinstance(original, bool):
            value = self._parse_bool(value)
        elif isinstance(original, int):
            value = int(value)
        elif isinstance(original, str):
            value = str(value)
        
        setattr(self, key, value)


# 全局单例
cfg = RuntimeConfig()
