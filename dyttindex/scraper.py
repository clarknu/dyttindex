from __future__ import annotations

import random
import time
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple, Callable

import requests
from bs4 import BeautifulSoup

from . import config
from .db import get_conn, upsert_movie, create_db, ensure_session, get_visited, mark_visited, append_event

FIELD_PATTERNS = {
    "alias": re.compile(r"^◎\s*(译名|又名)\s*(.*)$"),
    "title": re.compile(r"^◎\s*(片名|剧名)\s*(.*)$"),
    "year": re.compile(r"^◎\s*(年代|年份)\s*(.*)$"),
    "country": re.compile(r"^(?:◎\s*(国家|产地|地区)|制片国家/地区)\s*[:：]?\s*(.*)$"),
    "language": re.compile(r"^◎\s*(语言)\s*(.*)$"),
    "genres": re.compile(r"^◎\s*(类别|类型)\s*(.*)$"),
    "director": re.compile(r"^◎\s*(导演)\s*(.*)$"),
    "actors": re.compile(r"^◎\s*(主演|演员)\s*(.*)$"),
    "douban": re.compile(r"^◎\s*(豆瓣评分)\s*([0-9]+(?:\\.[0-9]+)?)"),
    "imdb": re.compile(r"^◎\s*(IMDb|IMDB)评分\s*([0-9]+(?:\\.[0-9]+)?)", re.I),
    "desc": re.compile(r"^◎\s*(简介|剧情介绍|内容简介)\s*(.*)$"),
    # 新增：上映信息用于回退年份
    "release": re.compile(r"^◎\s*(上映日期|上映时间|首映|首播)\s*(.*)$"),
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
    # 部分镜像 HTTPS 证书配置不规范，禁用证书校验以提升可用性
    s.verify = False
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


# 屏蔽 HTTPS 校验相关警告（根据配置）
try:
    import warnings
    import urllib3
    from urllib3.exceptions import InsecureRequestWarning
    if getattr(config, "SUPPRESS_TLS_WARNINGS", False):
        warnings.filterwarnings("ignore", category=InsecureRequestWarning)
        try:
            urllib3.disable_warnings(InsecureRequestWarning)
        except Exception:
            pass
except Exception:
    pass


def resolve_base(s: requests.Session) -> str:
    chosen = config.BASE_URL
    for base in getattr(config, "BASE_MIRRORS", [config.BASE_URL]):
        try:
            resp = s.get(base.rstrip('/'), timeout=config.REQUEST_TIMEOUT)
            if resp.status_code == 200 and (resp.text or ""):
                chosen = base.rstrip('/')
                break
        except requests.RequestException:
            pass
        _sleep()
    config.BASE_URL = chosen
    # 设置 Referer 以降低 403/503 风险
    s.headers["Referer"] = chosen
    return chosen


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
        if t in {"下一页", "下一頁"}:
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
    # 新增：基于页面关键词的标签提取（轻量分词）
    _TAG_KEYWORDS = [
        "科幻","喜剧","动作","剧情","爱情","犯罪","战争","悬疑","奇幻","动画","纪录片",
        "青春","古装","武侠","家庭","短片","音乐","综艺","冒险","传记","历史","灾难","体育",
        "恐怖","惊悚","励志","黑色幽默","同性","西部","儿童","校园","公路","现实","励志"
    ]
    _QUALITY_KEYWORDS = [
        "4K","UHD","蓝光","原盘","HDR","杜比视界","DV","Remux","BluRay","BDRip","WEB-DL","WEBRip","HDRip",
        "1080p","720p","2160p","480p"
    ]
    _LANG_TAGS = [
        "中字","双语","国配","粤语","国语","英语","日语","韩语","法语","德语","俄语","西班牙语","泰语"
    ]
    _SERIES_TAGS = [
        "全集","完结","合集","系列","全季","第一季","第二季","第三季","第四季"
    ]
    
    def _extract_additional_tags(data: dict, text: str, url: str) -> list:
        tset = set()
        # 题材关键词：标题、简介、全文
        title = (data.get("title") or "")
        desc = (data.get("description") or "")
        blob = "\n".join([title, desc, text])
        for kw in _TAG_KEYWORDS:
            if kw and (kw in blob):
                tset.add(kw)
        # 语言/字幕/配音
        for kw in _LANG_TAGS:
            if kw and (kw in blob):
                tset.add(kw)
        # 画质/媒介
        lower = blob.lower()
        for kw in _QUALITY_KEYWORDS:
            if (kw.lower() in lower) or (kw in blob):
                tset.add(kw)
        # 系列/完结等结构性标签
        for kw in _SERIES_TAGS:
            if kw in blob:
                tset.add(kw)
        # URL 路径结构：电视剧/动漫/综艺提示及细分标签
        lp = (url or "").lower()
        if "/tv/" in lp:
            tset.add("电视剧")
            if "/tv/gj/" in lp:
                tset.add("国产")
            elif "/tv/ous/" in lp:
                tset.add("欧美")
            elif "/tv/rihan/" in lp:
                # 进一步根据语言加细分（可选）
                lang = (data.get("language") or "")
                if "韩语" in lang:
                    tset.add("韩剧")
                elif "日语" in lang:
                    tset.add("日剧")
        elif "/dongman/" in lp:
            tset.add("动漫")
        elif "/zongyi/" in lp:
            tset.add("综艺")
        elif "/gndy/hd/" in lp:
            tset.add("蓝光")
            tset.add("高清")
        # 评分标签（高分）
        try:
            rv = float(data.get("rating_value") or 0)
            src = (data.get("rating_source") or "")
            if rv >= 7.5:
                if src.lower().startswith("douban"):
                    tset.add("豆瓣高分")
                elif src.lower().startswith("imdb"):
                    tset.add("IMDB高分")
                else:
                    tset.add("高分")
        except Exception:
            pass
        return list(tset)

    while i < len(lines):
        l = lines[i].strip()
        # 规范化特殊空格，去除全角空格以提升匹配命中率
        l2 = l.replace("\u3000", "").replace("\xa0", " ")
        matched = False
        for key, pat in FIELD_PATTERNS.items():
            m = pat.match(l2)
            if m:
                matched = True
                if key == "alias":
                    data["original_title"] = m.group(2).strip()
                elif key == "title":
                    data["title"] = m.group(2).strip()
                elif key == "year":
                    try:
                        data["year"] = int(re.findall(r"(?:19|20)\d{2}", m.group(2))[0])
                    except Exception:
                        data["year"] = None
                elif key == "country":
                    data["country"] = m.group(2).strip()
                elif key == "language":
                    data["language"] = m.group(2).strip()
                elif key == "genres":
                    genres = re.split(r"[、,\/\s]", m.group(2).strip())
                    data["tags"].extend([g for g in genres if g])
                elif key == "director":
                    data["director"] = m.group(2).strip()
                elif key == "actors":
                    # 主演可能跨多行直到下一个“◎”
                    first = m.group(2).strip()
                    if first:
                        actors_block.append(first)
                    j = i + 1
                    while j < len(lines) and not lines[j].replace("\u3000", "").startswith("◎"):
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
                elif key == "release":
                    # 作为年份回退来源
                    try:
                        ys = re.findall(r"(?:19|20)\d{2}", m.group(2))
                        if ys:
                            data["year"] = int(ys[0])
                    except Exception:
                        pass
                elif key == "desc":
                    desc_collecting = True
                    data["description"] = m.group(2).strip() if m.group(2) else ""
                break
        if not matched and desc_collecting:
            if l2.startswith("◎"):
                desc_collecting = False
            else:
                data["description"] = (data["description"] or "") + "\n" + l
        i += 1

    # 追加：对全文进行关键词标签提取
    try:
        extra_tags = _extract_additional_tags(data, text, url)
        if extra_tags:
            data["tags"].extend(extra_tags)
    except Exception:
        pass

    # 回退：从全文提取年份
    if not data.get("year"):
        try:
            ys = re.findall(r"(?:19|20)\d{2}", text)
            if ys:
                data["year"] = int(ys[0])
        except Exception:
            pass

    # 回退：从语言推断产地
    if not data.get("country"):
        lang = (data.get("language") or "").lower()
        language_cn = data.get("language") or ""
        if ("日语" in language_cn) or ("japanese" in lang):
            data["country"] = "日本"
        elif ("韩语" in language_cn) or ("korean" in lang):
            data["country"] = "韩国"
        elif ("粤语" in language_cn) or ("cantonese" in lang):
            data["country"] = "中国香港"
        elif any(x in language_cn for x in ["国语", "汉语", "中文"]):
            data["country"] = "中国"

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

    # 通过类别或路径推断 kind（弱化为粗粒度）
    if not data["kind"]:
        lower_path = (url or "").lower()
        ts = data["tags"] or []
        # 粗粒度：电视剧/动漫/综艺/蓝光，其余默认电影
        if ("/tv/" in lower_path) or any(t in ts for t in ["电视剧","剧集","连续剧"]):
            data["kind"] = "tv"
        elif ("/dongman/" in lower_path) or any(t in ts for t in ["动漫","动画"]):
            data["kind"] = "anime"
        elif ("/zongyi/" in lower_path) or any("综艺" in t for t in ts):
            data["kind"] = "variety"
        elif ("/gndy/hd/" in lower_path) or any(t in ts for t in ["蓝光","原盘","4K","UHD"]):
            data["kind"] = "uhd"
        elif any("纪录片" in t for t in ts):
            data["kind"] = "doc"
        elif any("短片" in t for t in ts):
            data["kind"] = "short"
        elif any(t in ts for t in ["音乐","演唱","MV"]):
            data["kind"] = "music"
        else:
            data["kind"] = "movie"
        # 电影细分：按产地/国家进一步划分
        if data["kind"] == "movie":
            cc = (data.get("country") or "").lower()
            if any(x in cc for x in ["中国", "大陆", "china", "cn"]):
                data["kind"] = "movie_cn"
            elif any(x in cc for x in ["香港", "hong kong", "hk"]):
                data["kind"] = "movie_hk"
            elif any(x in cc for x in ["台湾", "taiwan", "tw"]):
                data["kind"] = "movie_tw"
            elif any(x in cc for x in ["美国", "英国", "法国", "德国", "西班牙", "意大利", "加拿大", "澳大利亚", "欧洲", "usa", "uk", "fr", "de", "es", "it", "ca", "au", "europe"]):
                data["kind"] = "movie_en"
            elif any(x in cc for x in ["日本", "japan", "jp"]):
                data["kind"] = "movie_jp"
            elif any(x in cc for x in ["韩国", "korea", "kr"]):
                data["kind"] = "movie_kor"

    # 清洗标签
    data["tags"] = list(dict.fromkeys([t.strip() for t in data["tags"] if t and t.strip()]))
    return data


class DyttScraper:
    def __init__(self, session_id: Optional[str] = None):
        self.s = _session()
        # 选择镜像并更新 Referer
        resolve_base(self.s)
        self._stop = False
        self.session_id = session_id

    def stop(self) -> None:
        self._stop = True

    def crawl_site(self, start_url: str, max_pages_total: int, max_items_total: int, progress_cb: Optional[Callable[[dict], None]] = None) -> int:
        """从根路径逐层遍历，动态发现列表页与详情页并抓取。
        仅在同站点内遍历，限制总页面与总条目数量，避免无限递归。
        """
        from collections import deque
        from urllib.parse import urljoin, urlparse

        # 允许跨镜像主机，但限制在已知镜像集合内
        mirror_hosts = set()
        try:
            for b in getattr(config, "BASE_MIRRORS", [config.BASE_URL]):
                try:
                    h = urlparse(b).hostname
                    if h:
                        mirror_hosts.add(h.lower())
                except Exception:
                    pass
            base_host = urlparse(config.BASE_URL).hostname
            if base_host:
                mirror_hosts.add(base_host.lower())
        except Exception:
            pass
        # 辅助判断函数（不跨站，仅限镜像主机）
        def is_allowed(u: str) -> bool:
            try:
                pu = urlparse(u)
                host_u = (pu.hostname or "").lower()
                return (not host_u) or (host_u in mirror_hosts)
            except Exception:
                return True

        def is_detail_url(u: str) -> bool:
            try:
                p = urlparse(u)
                return "/html/" in (p.path or "") and bool(re.search(r"/html/.+/\d+/.+\.html$", p.path or ""))
            except Exception:
                return False

        def is_list_like(href: str) -> bool:
            if re.search(r"(index|list)_\d+\.html$", href):
                return True
            if re.search(r"/(gndy|tv|dongman|zongyi)(/|$)", href):
                return True
            if href.endswith("/") or href.endswith(".html"):
                return True
            return False
        # 会话与已访问集合
        db = get_conn()
        ensure_session(db, self.session_id)
        total_items = 0
        visited_pages = get_visited(db, self.session_id, 'page')
        visited_details = get_visited(db, self.session_id, 'detail')
        start = start_url or config.BASE_URL
        q = deque([start])
        if progress_cb:
            try:
                progress_cb({"event": "site_start", "url": start})
            except Exception:
                pass
        append_event(db, self.session_id, {"event": "site_start", "url": start})
        while q and len(visited_pages) < max_pages_total and total_items < max_items_total and not self._stop:
            url = q.popleft()
            if url in visited_pages:
                continue
            visited_pages.add(url)
            mark_visited(db, self.session_id, url, 'page')
            html = fetch(url, self.s)
            if not html:
                if progress_cb:
                    try:
                        progress_cb({"event": "error", "section": "site", "url": url, "message": "页面获取失败"})
                    except Exception:
                        pass
                append_event(db, self.session_id, {"event": "error", "section": "site", "url": url, "message": "页面获取失败"})
                continue
            lp = parse_list_page(html, config.BASE_URL)
            # 解析候选链接
            soup = BeautifulSoup(html, "lxml")
            new_pages = []
            for a in soup.select("a[href]"):
                h = a.get("href") or ""
                if not h or "javascript:" in h:
                    continue
                absu = urljoin(url, h) if not h.startswith("http") else h
                if not is_allowed(absu):
                    continue
                if is_detail_url(absu):
                    absd = absu
                    if absd in visited_details:
                        continue
                    if total_items >= max_items_total:
                        break
                    detail_html = fetch(absd, self.s)
                    if not detail_html:
                        if progress_cb:
                            try:
                                progress_cb({"event": "warn", "category": "detail", "detail_url": absd, "message": "详情页获取失败"})
                            except Exception:
                                pass
                        append_event(db, self.session_id, {"event": "warn", "category": "detail", "detail_url": absd, "message": "详情页获取失败"})
                        continue
                    data = parse_detail_page(detail_html, absd)
                    upsert_movie(db, data)
                    total_items += 1
                    visited_details.add(absd)
                    mark_visited(db, self.session_id, absd, 'detail')
                    if progress_cb:
                        try:
                            # 动态用内容分类作为 category 展示
                            progress_cb({"event": "item", "category": data.get("kind"), "count": total_items, "detail_url": absd, "title": data.get("title"), "year": data.get("year"), "kind": data.get("kind")})
                        except Exception:
                            pass
                    append_event(db, self.session_id, {"event": "item", "category": data.get("kind"), "count": total_items, "detail_url": absd, "title": data.get("title"), "year": data.get("year"), "kind": data.get("kind")})
                    _sleep()
                else:
                    if is_list_like(h):
                        new_pages.append(absu)
            # 如果首页未解析到入口，尝试常见分类入口的启发式种子
            if not new_pages and url == start:
                seeds = [
                    "/html/gndy/dyzz/index.html",
                    "/html/gndy/hd/index.html",
                    "/html/gndy/index.html",
                    "/html/tv/gj/index.html",
                    "/html/tv/ous/",
                    "/html/tv/rihan/",
                    "/html/tv/rjb/",
                    "/html/zongyi/",
                    "/html/dongman/",
                ]
                try:
                    base = config.BASE_URL.rstrip('/') + "/"
                    for p in seeds:
                        new_pages.append(urljoin(base, p))
                except Exception:
                    pass
            # 也把下一页加入队列
            if lp.next_url:
                new_pages.append(lp.next_url)
            # 去重与过滤已访问
            uniq_pages = [p for p in dict.fromkeys(new_pages) if p not in visited_pages]
            for p in uniq_pages:
                if len(visited_pages) + len(q) >= max_pages_total:
                    break
                q.append(p)
            if progress_cb:
                try:
                    # section 简化为路径段用于展示
                    sec = "site"
                    try:
                        from urllib.parse import urlparse
                        sec = "/".join([seg for seg in (urlparse(url).path or "/").split("/") if seg]) or "site"
                    except Exception:
                        pass
                    progress_cb({"event": "page", "section": sec, "url": url, "found": len(lp.detail_urls), "queued": len(uniq_pages)})
                except Exception:
                    pass
            append_event(db, self.session_id, {"event": "page", "section": sec if 'sec' in locals() else 'site', "url": url, "found": len(lp.detail_urls), "queued": len(uniq_pages)})
            _sleep()
        db.close()
        if progress_cb:
            try:
                progress_cb({"event": "site_done", "total": total_items})
            except Exception:
                pass
        # 会话事件
        conn2 = get_conn()
        append_event(conn2, self.session_id, {"event": "site_done", "total": total_items})
        conn2.close()
        return total_items

    def crawl_category(self, name: str, path: str, max_pages: int, max_items: int, progress_cb: Optional[Callable[[dict], None]] = None) -> int:
        db = get_conn()
        ensure_session(db, self.session_id)
        count, pages = 0, 0
        url = _abs(path)
        while url and pages < max_pages and count < max_items and not self._stop:
            html = fetch(url, self.s)
            if not html:
                if progress_cb:
                    try:
                        progress_cb({"event": "error", "category": name, "page": pages + 1, "url": url, "message": "列表页获取失败"})
                    except Exception:
                        pass
                append_event(db, self.session_id, {"event": "error", "category": name, "page": pages + 1, "url": url, "message": "列表页获取失败"})
                break
            lp = parse_list_page(html, config.BASE_URL)
            mark_visited(db, self.session_id, url, 'page')
            if progress_cb:
                try:
                    progress_cb({"event": "page", "category": name, "page": pages + 1, "url": url, "found": len(lp.detail_urls)})
                except Exception:
                    pass
            append_event(db, self.session_id, {"event": "page", "category": name, "page": pages + 1, "url": url, "found": len(lp.detail_urls)})
            for du in lp.detail_urls:
                if count >= max_items or self._stop:
                    break
                detail_html = fetch(du, self.s)
                if not detail_html:
                    if progress_cb:
                        try:
                            progress_cb({"event": "warn", "category": name, "detail_url": du, "message": "详情页获取失败"})
                        except Exception:
                            pass
                    append_event(db, self.session_id, {"event": "warn", "category": name, "detail_url": du, "message": "详情页获取失败"})
                    continue
                data = parse_detail_page(detail_html, du)
                upsert_movie(db, data)
                count += 1
                mark_visited(db, self.session_id, du, 'detail')
                if progress_cb:
                    try:
                        progress_cb({"event": "item", "category": name, "count": count, "detail_url": du, "title": data.get("title"), "year": data.get("year"), "kind": data.get("kind")})
                    except Exception:
                        pass
                append_event(db, self.session_id, {"event": "item", "category": name, "count": count, "detail_url": du, "title": data.get("title"), "year": data.get("year"), "kind": data.get("kind")})
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
                if progress_cb:
                    try:
                        progress_cb({"event": "category_start", "category": name})
                    except Exception:
                        pass
                n = self.crawl_category(name, path, max_pages_per_category, max_items_per_category, progress_cb=progress_cb)
                total += n
                if progress_cb:
                    try:
                        progress_cb({"event": "category_done", "category": name, "count": n})
                    except Exception:
                        pass
            except Exception:
                # 跳过异常，以保证整体流程
                pass
        return total


def init_db(drop: bool = False):
    create_db(drop=drop)