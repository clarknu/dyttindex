from __future__ import annotations

import random
import time
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple, Callable

import requests
from bs4 import BeautifulSoup

from . import config
from .db import get_conn, upsert_movie, create_db

FIELD_PATTERNS = {
    "alias": re.compile(r"^◎\s*(译名|又名)\s*(.*)$"),
    "title": re.compile(r"^◎\s*(片名|剧名)\s*(.*)$"),
    "year": re.compile(r"^◎\s*(年代|年份)\s*(.*)$"),
    "country": re.compile(r"^◎\s*(国家|产地|地区)\s*(.*)$"),
    "language": re.compile(r"^◎\s*(语言)\s*(.*)$"),
    "genres": re.compile(r"^◎\s*(类别|类型)\s*(.*)$"),
    "director": re.compile(r"^◎\s*(导演)\s*(.*)$"),
    "actors": re.compile(r"^◎\s*(主演|演员)\s*(.*)$"),
    "douban": re.compile(r"^◎\s*(豆瓣评分)\s*([0-9]\.[0-9])"),
    "imdb": re.compile(r"^◎\s*(IMDb|IMDB)评分\s*([0-9]\.[0-9])", re.I),
    "desc": re.compile(r"^◎\s*(简介|剧情介绍|内容简介)\s*(.*)$"),
}

DL_SCHEMES = {
    "magnet": re.compile(r"^magnet:"),
    "ed2k": re.compile(r"^ed2k:"),
    "ftp": re.compile(r"^ftp://"),
    "thunder": re.compile(r"^thunder://"),
    "torrent": re.compile(r"\.torrent($|\?)"),
    "pan": re.compile(r"pan\.baidu\.com|cloud\.baidu\.com"),
}

@dataclass
class ListPage:
    detail_urls: List[str]
    next_url: Optional[str]


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(config.DEFAULT_HEADERS)
    s.headers["User-Agent"] = random.choice(config.USER_AGENTS)
    s.verify = True
    return s


def _sleep():
    lo, hi = config.REQUEST_SLEEP
    time.sleep(random.uniform(lo, hi))


def fetch(url: str, s: requests.Session, retry: int = config.REQUEST_RETRY) -> Optional[str]:
    for i in range(retry + 1):
        try:
            r = s.get(url, timeout=config.REQUEST_TIMEOUT)
            if r.status_code == 200:
                # 修正编码，避免 GB2312/GBK 乱码
                try:
                    r.encoding = r.apparent_encoding or r.encoding
                except Exception:
                    pass
                if r.text:
                    return r.text
            # 如果返回非200，尝试 http/https 回退
            if url.startswith("https://"):
                alt = url.replace("https://", "http://", 1)
                r2 = s.get(alt, timeout=config.REQUEST_TIMEOUT)
                if r2.status_code == 200:
                    try:
                        r2.encoding = r2.apparent_encoding or r2.encoding
                    except Exception:
                        pass
                    if r2.text:
                        return r2.text
        except requests.RequestException:
            # https 失败则尝试 http
            if url.startswith("https://"):
                try:
                    alt = url.replace("https://", "http://", 1)
                    r3 = s.get(alt, timeout=config.REQUEST_TIMEOUT)
                    if r3.status_code == 200:
                        try:
                            r3.encoding = r3.apparent_encoding or r3.encoding
                        except Exception:
                            pass
                        if r3.text:
                            return r3.text
                except requests.RequestException:
                    pass
        _sleep()
    return None


def _abs(url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"):
        return url
    base = config.BASE_URL.rstrip('/')
    url = url if url.startswith('/') else '/' + url
    return base + url


def parse_list_page(html: str, base_url: str) -> ListPage:
    soup = BeautifulSoup(html, "lxml")
    detail_urls, next_url = [], None

    # 放宽选择器：全页所有 a[href]，过滤到详情页模式
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if not href:
            continue
        if "javascript:" in href:
            continue
        if "#" in href and href.endswith("#"):
            continue
        if "/html/" in href and re.search(r"/html/.+/\d+/.+\.html$", href):
            detail_urls.append(_abs(href))

    # 下一页：优先“下一页”，其次 list_*.html 或 index_*.html
    for a in soup.select("a[href]"):
        t = (a.get_text() or "").strip()
        h = a.get("href") or ""
        if t in {"下一页", "下一页"}:
            next_url = _abs(h)
            break
    if not next_url:
        for a in soup.select("a[href]"):
            h = a.get("href") or ""
            if re.search(r"(list_|index_).*\.html$", h):
                next_url = _abs(h)
                break

    detail_urls = list(dict.fromkeys(detail_urls))
    return ListPage(detail_urls, next_url)


# 新增：剧集解析，基于标签或文件名提取集数
_EP_LABEL_PATTERNS = [
    re.compile(r"第\s*(\d{1,3})\s*[集话]"),
    re.compile(r"[Ee][Pp]?\s*(\d{1,3})"),
    re.compile(r"[Ss]\d{1,2}[Ee](\d{1,2})"),
]


def _parse_episode(label: str, href: str) -> Optional[int]:
    text = (label or "") + " " + (href or "")
    # 先按常见模式匹配
    for pat in _EP_LABEL_PATTERNS:
        m = pat.search(text)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
    # 回退：文件名中的短数字（避免 720/1080 等分辨率）
    m2 = re.search(r"[-_\s](\d{1,3})(?!\d)", text)
    if m2:
        try:
            num = int(m2.group(1))
            if 1 <= num <= 150:
                return num
        except Exception:
            pass
    # 进一步回退：含“集/话/期”的数字
    m3 = re.search(r"(\d{1,3})\s*(?:集|话|期)", text)
    if m3:
        try:
            num = int(m3.group(1))
            if 1 <= num <= 150:
                return num
        except Exception:
            pass
    return None


def _collect_download_links(soup: BeautifulSoup) -> List[dict]:
    links = []
    for a in soup.select("#Zoom a[href], a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        label = (a.get_text() or "").strip()
        kind = None
        for k, pat in DL_SCHEMES.items():
            if pat.search(href):
                kind = k
                break
        if kind:
            links.append({"url": href, "kind": kind, "label": label, "episode": _parse_episode(label, href)})
    # 去重
    uniq, out = set(), []
    for dl in links:
        tup = (dl["url"], dl.get("kind"))
        if tup not in uniq:
            uniq.add(tup)
            out.append(dl)
    return out


def parse_detail_page(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    zoom = soup.select_one("#Zoom") or soup
    text = zoom.get_text("\n", strip=True)
    lines = [l for l in text.split("\n") if l.strip()]

    data = {
        "title": None,
        "original_title": None,
        "year": None,
        "kind": None,
        "country": None,
        "language": None,
        "director": None,
        "actors": None,
        "rating_source": None,
        "rating_value": None,
        "rating_votes": None,
        "description": None,
        "cover_url": None,
        "detail_url": url,
        "raw_html": html,
        "tags": [],
        "download_links": _collect_download_links(zoom),
    }

    # 海报图
    img = zoom.select_one("img[src]")
    if img:
        data["cover_url"] = img.get("src")

    # 逐行匹配“◎ 字段”
    i = 0
    actors_block = []
    desc_collecting = False
    while i < len(lines):
        l = lines[i].strip()
        matched = False
        for key, pat in FIELD_PATTERNS.items():
            m = pat.match(l)
            if m:
                matched = True
                if key == "alias":
                    data["original_title"] = m.group(2).strip()
                elif key == "title":
                    data["title"] = m.group(2).strip()
                elif key == "year":
                    try:
                        data["year"] = int(re.findall(r"\d{4}", m.group(2))[0])
                    except Exception:
                        data["year"] = None
                elif key == "country":
                    data["country"] = m.group(2).strip()
                elif key == "language":
                    data["language"] = m.group(2).strip()
                elif key == "genres":
                    genres = re.split(r"[、,/\s]", m.group(2).strip())
                    data["tags"].extend([g for g in genres if g])
                elif key == "director":
                    data["director"] = m.group(2).strip()
                elif key == "actors":
                    # 主演可能跨多行直到下一个“◎”
                    first = m.group(2).strip()
                    if first:
                        actors_block.append(first)
                    j = i + 1
                    while j < len(lines) and not lines[j].startswith("◎"):
                        if lines[j].strip():
                            actors_block.append(lines[j].strip())
                        j += 1
                    i = j - 1
                elif key == "douban":
                    data["rating_source"] = "Douban"
                    try:
                        data["rating_value"] = float(m.group(2))
                    except Exception:
                        pass
                elif key == "imdb":
                    data["rating_source"] = "IMDB"
                    try:
                        data["rating_value"] = float(m.group(2))
                    except Exception:
                        pass
                elif key == "desc":
                    desc_collecting = True
                    data["description"] = m.group(2).strip() if m.group(2) else ""
                break
        if not matched and desc_collecting:
            if l.startswith("◎"):
                desc_collecting = False
            else:
                data["description"] = (data["description"] or "") + "\n" + l
        i += 1

    if actors_block:
        data["actors"] = "\n".join(actors_block)

    # 回退标题：页面标题或下载区块中的文件名
    if not data["title"]:
        h1 = soup.select_one("div#header h1, h1")
        if h1 and h1.get_text().strip():
            data["title"] = h1.get_text().strip()
        else:
            title_tag = soup.title
            data["title"] = title_tag.get_text().strip() if title_tag else "未命名"

    # 通过类别或路径推断 kind
    if not data["kind"]:
        if any(t in (data["tags"] or []) for t in ["电视剧", "剧集", "连续剧"]):
            data["kind"] = "tv"
        elif re.search(r"/tv/", url):
            data["kind"] = "tv"
        elif re.search(r"/dongman/", url):
            data["kind"] = "anime"
        elif re.search(r"/zongyi/", url):
            data["kind"] = "variety"
        else:
            data["kind"] = "movie"

    # 清洗标签
    data["tags"] = list(dict.fromkeys([t.strip() for t in data["tags"] if t and t.strip()]))
    return data


class DyttScraper:
    def __init__(self):
        self.s = _session()
        self._stop = False

    def stop(self) -> None:
        self._stop = True

    def crawl_category(self, name: str, path: str, max_pages: int, max_items: int, progress_cb: Optional[Callable[[dict], None]] = None) -> int:
        db = get_conn()
        count, pages = 0, 0
        url = _abs(path)
        while url and pages < max_pages and count < max_items and not self._stop:
            html = fetch(url, self.s)
            if not html:
                break
            lp = parse_list_page(html, config.BASE_URL)
            if progress_cb:
                try:
                    progress_cb({"event": "page", "category": name, "page": pages + 1, "url": url, "found": len(lp.detail_urls)})
                except Exception:
                    pass
            for du in lp.detail_urls:
                if count >= max_items or self._stop:
                    break
                detail_html = fetch(du, self.s)
                if not detail_html:
                    continue
                data = parse_detail_page(detail_html, du)
                upsert_movie(db, data)
                count += 1
                if progress_cb:
                    try:
                        progress_cb({"event": "item", "category": name, "count": count, "detail_url": du})
                    except Exception:
                        pass
                _sleep()
            url = lp.next_url
            pages += 1
            _sleep()
        db.close()
        return count

    def crawl_all(self, max_pages_per_category: int, max_items_per_category: int, progress_cb: Optional[Callable[[dict], None]] = None) -> int:
        total = 0
        for name, path in config.CATEGORIES.items():
            try:
                total += self.crawl_category(name, path, max_pages_per_category, max_items_per_category, progress_cb=progress_cb)
            except Exception:
                # 跳过异常，以保证整体流程
                pass
        return total


def init_db(drop: bool = False):
    create_db(drop=drop)