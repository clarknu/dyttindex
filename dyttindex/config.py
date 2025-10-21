from __future__ import annotations

BASE_URL = "http://www.dydytt.net"  # 使用 HTTP 以提升可访问性
# 备用镜像列表（按优先级排列）
BASE_MIRRORS = [
    "http://www.dydytt.net",
    "https://www.dydytt.net",
    "http://www.ygdy8.net",
    "https://www.ygdy8.net",
    "http://www.ygdy8.com",
]

REQUEST_TIMEOUT = 15
REQUEST_RETRY = 2
REQUEST_SLEEP = (0.8, 1.8)  # 每次请求之间的随机睡眠区间（秒）

DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "no-cache",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15",
]

SQLITE_PATH = "c:/Code/dyttindex/data/movies.db"


# 自动遍历模式的总量默认值
DEFAULT_MAX_PAGES_TOTAL = 300
DEFAULT_MAX_ITEMS_TOTAL = 3000

# 是否屏蔽 HTTPS 证书相关警告
SUPPRESS_TLS_WARNINGS = True