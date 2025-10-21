from __future__ import annotations

import random
import time
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple, Callable

import requests
from bs4 import BeautifulSoup

from . import config
from .db import get_conn, upsert_movie, create_db, ensure_session, get_visited, mark_visited, append_event, enqueue_urls, get_frontier_urls, mark_queue_done

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
    # 扩展简介识别：兼容“剧情(介绍|简介)/故事梗概/简 介/介绍”等，允许冒号
    "desc": re.compile(r"^◎\s*(?:简介|剧情(?:介绍|简介)|内容简介|故事梗概|简\s*介|介绍)\s*[:：]?\s*(.*)$"),
    # 备用简介格式：不带“◎”的【内容简介】/剧情简介/简 介 等
    "desc_alt": re.compile(r"^(?:【?\s*(?:内容简介|剧情简介|故事梗概|剧情|介绍|简\s*介)\s*】?)\s*[:：]?\s*(.*)$"),
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
    s.headers.update({
        "User-Agent": getattr(config, "USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0 Safari/537.36"),
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
    })
    s.verify = False
    return s


def _sleep():
    time.sleep(random.uniform(0.3, 0.9))


def fetch(url: str, s: requests.Session, retry: int = config.REQUEST_RETRY) -> Optional[str]:
    for i in range(max(1, retry)):
        try:
            r = s.get(url, timeout=getattr(config, "REQUEST_TIMEOUT", 15))
            if r.status_code == 200:
                return r.text
        except Exception:
            pass
        _sleep()
    return None


def _abs(url: str) -> str:
    if url.startswith("http"):
        return url
    base = config.BASE_URL.rstrip("/")
    url2 = url.lstrip("/")
    return f"{base}/{url2}"

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
    base = getattr(config, "BASE_URL", "")
    mirrors = getattr(config, "BASE_MIRRORS", [])
    # 简单探测镜像可达性
    for u in [base] + list(mirrors):
        try:
            r = s.get(u, timeout=getattr(config, "REQUEST_TIMEOUT", 15))
            if r.status_code == 200:
                s.headers.update({"Referer": u})
                return u
        except Exception:
            pass
    return base


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
        "alt_titles": [],
    }

    # 辅助：拆分名称与判定是否含中文
    def _contains_cjk(s: str) -> bool:
        try:
            return bool(re.search(r"[\u4e00-\u9fff]", s or ""))
        except Exception:
            return False
    def _split_names(s: str) -> list:
        if not s:
            return []
        # 常见分隔符：/ | 、 ， , ; ；
        toks = [t.strip() for t in re.split(r"[\/\\|、,，;；]+", s) if t and t.strip()]
        # 去掉重复括号包裹但保留内部文本
        out = []
        for t in toks:
            tt = t.strip()
            if len(tt) >= 2 and ((tt[0] in "([【『「" and tt[-1] in ")]】』」") ):
                tt = tt[1:-1].strip()
            if tt:
                out.append(tt)
        return out
    def _add_alt(n: str):
        if not n:
            return
        if n not in data["alt_titles"]:
            data["alt_titles"].append(n)

    # 海报图
    img = zoom.select_one("img[src]")
    if img:
        data["cover_url"] = img.get("src")

    # 逐行匹配“◎ 字段”
    i = 0
    actors_block = []
    desc_collecting = False
    # 标签关键词增强：补充综艺相关词，减少误判为电影
    _TAG_KEYWORDS = [
        "科幻","喜剧","动作","剧情","爱情","犯罪","战争","悬疑","奇幻","动画","纪录片",
        "青春","古装","武侠","家庭","短片","音乐","综艺","冒险","传记","历史","灾难","体育",
        "恐怖","惊悚","励志","黑色幽默","同性","西部","儿童","校园","公路","现实","励志",
        # 综艺常见类型
        "真人秀","脱口秀","访谈","选秀","竞技","美食","旅行","婚恋","曲艺","晚会"
    ]
    _QUALITY_KEYWORDS = [
        "4K","UHD","蓝光","原盘","HDR","杜比视界","DV","Remux","BluRay","BDRip","WEB-DL","WEBRip","HDRip",
        "1080p","720p","2160p","480p"
    ]
    _LANG_TAGS = [
        "中字","双语","国配","粤语","国语","英语","日语","韩语","法语","德语","俄语","西班牙语","泰语"
    ]
    _SERIES_TAGS = [
        "全集","完结","合集","系列","全季","第一季","第二季","第三季","第四季","特别篇","SP"
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
        elif ("/zongyi/" in lp) or ("zongyi" in lp):
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
                    raw = m.group(2).strip() if (m.lastindex and m.lastindex >= 2) else (m.group(m.lastindex) or "")
                    for nm in _split_names(raw):
                        _add_alt(nm)
                elif key == "title":
                    raw = m.group(2).strip()
                    if raw:
                        data["title"] = raw
                        for nm in _split_names(raw):
                            _add_alt(nm)
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
                    genres = [g for g in genres if g]
                    data["tags"].extend(genres)
                    # 利用“◎类别/类型”直接辅助判定 kind
                    if not data.get("kind"):
                        gl = " ".join(genres).lower()
                        if any(x in gl for x in ["综艺","真人秀","脱口秀","访谈","选秀","竞技","美食","旅行","婚恋","晚会"]):
                            data["kind"] = "variety"
                        elif any(x in gl for x in ["电视剧","剧集","连续剧","drama"]):
                            data["kind"] = "tv"
                        elif any(x in gl for x in ["动漫","动画","cartoon","anime"]):
                            data["kind"] = "anime"
                        elif any(x in gl for x in ["纪录片","纪录","documentary"]):
                            data["kind"] = "doc"
                        elif any(x in gl for x in ["短片","短片集","short"]):
                            data["kind"] = "short"
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
                elif key in ("desc", "desc_alt"):
                    desc_collecting = True
                    # 兼容不同捕获组：取最后一个捕获组作为正文
                    try:
                        grp_idx = m.lastindex or 1
                        data["description"] = m.group(grp_idx).strip() if m.group(grp_idx) else ""
                    except Exception:
                        data["description"] = (m.group(1) or "").strip()
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
        title = (data.get("title") or "")
        # 优先识别综艺：标题/标签提示“期/真人秀/脱口秀”等
        if ("/zongyi/" in lower_path) or ("zongyi" in lower_path) or any("综艺" in t for t in ts) or re.search(r"第\s*\d{1,3}\s*期", title):
            data["kind"] = "variety"
        # 粗粒度：电视剧/动漫，其余默认电影
        elif ("/tv/" in lower_path) or any(t in ts for t in ["电视剧","剧集","连续剧"]):
            data["kind"] = "tv"
        elif ("/dongman/" in lower_path) or any(t in ts for t in ["动漫","动画"]):
            data["kind"] = "anime"
        elif ("/gndy/hd/" in lower_path) or any(t in ts for t in ["蓝光","原盘","4K","UHD"]):
            data["kind"] = "uhd"
        elif any("纪录片" in t or "纪录" in t for t in ts):
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
            elif any(x in cc for x in ["日本", "japan", "jp"]):
                data["kind"] = "movie_jp"
            elif any(x in cc for x in ["韩国", "korea", "kr"]):
                data["kind"] = "movie_kor"
            elif any(x in cc for x in ["美国", "英国", "法国", "德国", "西班牙", "意大利", "加拿大", "澳大利亚", "欧洲", "usa", "uk", "fr", "de", "es", "it", "ca", "au", "europe"]):
                data["kind"] = "movie_en"
    # 和谐覆盖：若路径/标签强提示综艺/电视剧/动漫，则覆盖 movie 细分
    lower_path2 = (url or "").lower()
    ts2 = data.get("tags") or []
    title2 = (data.get("title") or "")
    if ("zongyi" in lower_path2) or any("综艺" in t for t in ts2) or re.search(r"第\s*\d{1,3}\s*期", title2):
        data["kind"] = "variety"
    elif ("/tv/" in lower_path2) or any(t in ts2 for t in ["电视剧","剧集","连续剧"]):
        data["kind"] = "tv"
    elif ("/dongman/" in lower_path2) or any(t in ts2 for t in ["动漫","动画"]):
        data["kind"] = "anime"
    # 规范主标题与原名：优先中文作为主标题，英文/非中文作为原名
    try:
        cn_names = [t for t in data.get("alt_titles") or [] if _contains_cjk(t)]
        non_cn = [t for t in data.get("alt_titles") or [] if not _contains_cjk(t)]
        # 若原片名缺失，尝试从集合中选第一个非中文
        if not data.get("original_title"):
            if non_cn:
                data["original_title"] = non_cn[0]
            elif data.get("title") and (not _contains_cjk(data["title"])):
                data["original_title"] = data["title"]
        # 若主标题为空或为非中文且存在中文译名，选第一个中文名
        if (not data.get("title")) or ((data.get("title") and not _contains_cjk(data["title"])) and cn_names):
            if cn_names:
                data["title"] = cn_names[0]
        # 将主标题/原名也纳入别名集合
        for nm in [data.get("title"), data.get("original_title")]:
            _add_alt(nm or "")
        # 去重（保持顺序）
        seen = set()
        data["alt_titles"] = [x for x in data.get("alt_titles") or [] if (x and not (x in seen or seen.add(x)))]
    except Exception:
        pass

    return data

def init_db(drop: bool = False) -> None:
    create_db(drop=drop)

class DyttScraper:
    def __init__(self, session_id: Optional[str] = None):
        self.s = _session()
        try:
            self.base_url = resolve_base(self.s)
        except Exception:
            self.base_url = getattr(config, "BASE_URL", "")
        self.conn = get_conn()
        self.session_id = ensure_session(self.conn, session_id)
        self._stop = False
        self._visited_pages = get_visited(self.conn, self.session_id, "page") if self.session_id else set()
        self._visited_detail = get_visited(self.conn, self.session_id, "detail") if self.session_id else set()

    def stop(self) -> None:
        self._stop = True

    def _emit(self, event: dict, cb: Optional[Callable[[dict], None]] = None) -> None:
        try:
            if cb:
                cb(event)
        except Exception:
            pass
        try:
            append_event(self.conn, self.session_id, event)
        except Exception:
            pass

    def crawl_all(self, max_pages_per_category: int, max_items_per_category: int, progress_cb: Optional[Callable[[dict], None]] = None) -> int:
        # 兼容旧接口：改为从根路径进行遍历，不再使用分类URL
        return self.crawl_site(None, max_pages_per_category, max_items_per_category, progress_cb=progress_cb)

    def crawl_site(self, start_url: Optional[str], max_pages_total: int, max_items_total: int, progress_cb: Optional[Callable[[dict], None]] = None) -> int:
        from urllib.parse import urljoin, urlparse
        from collections import deque
        from bs4 import BeautifulSoup
        # 起点与域名白名单
        start = (start_url or self.base_url or getattr(config, "BASE_URL", "")).strip()
        if not start:
            raise ValueError("缺少起始 URL")
        base_host = urlparse(start).netloc
        mirror_hosts = {urlparse(m).netloc for m in getattr(config, "BASE_MIRRORS", []) if m}
        allowed_hosts = {h for h in ({base_host} | mirror_hosts) if h}
        # 计数与限制
        limit_pages = max_pages_total if max_pages_total and max_pages_total > 0 else float("inf")
        limit_items = max_items_total if max_items_total and max_items_total > 0 else float("inf")
        total = 0
        pages = 0
        # 队列与去重
        q = deque()
        q.append(start)
        # 断点续跑：加载历史前沿队列，补充到当前队列
        try:
            frontier = get_frontier_urls(self.conn, self.session_id, limit=int(limit_pages) if limit_pages != float("inf") else 1000)
            for u in frontier:
                if u not in q:
                    q.append(u)
        except Exception:
            pass
        seen: set[str] = set(self._visited_pages) if self._visited_pages else set()
        seen_detail: set[str] = set(self._visited_detail) if self._visited_detail else set()
        def _emit(evt: dict):
            self._emit(evt, progress_cb)
        _emit({"event": "site_start", "url": start})
        while q and pages < limit_pages and total < limit_items and not self._stop:
            cur = q.popleft()
            # 去除 fragment
            cur = cur.split('#')[0]
            if cur in seen:
                # 允许起始页再次解析以重建队列（断点续跑）
                if cur != start:
                    try:
                        mark_queue_done(self.conn, self.session_id, cur)
                    except Exception:
                        pass
                    continue
            try:
                resp = self.s.get(cur, timeout=getattr(config, "REQUEST_TIMEOUT", 15))
                if resp.status_code != 200:
                    _emit({"event": "warn", "url": cur, "message": f"HTTP {resp.status_code}"})
                    seen.add(cur)
                    mark_visited(self.conn, self.session_id, cur, "page")
                    continue
                html = decode_response(resp)
                # 优先尝试解析为详情页（不依赖 URL 结构）
                parsed_detail = False
                try:
                    data = parse_detail_page(html, cur)
                    # 仅当解析到有效标题时视为详情页，避免误入库
                    if data and data.get("title"):
                        upsert_movie(self.conn, data)
                        total += 1
                        parsed_detail = True
                        _emit({"event": "detail_saved", "detail_url": cur})
                        mark_visited(self.conn, self.session_id, cur, "detail")
                        if self.session_id:
                            seen_detail.add(cur)
                    else:
                        _emit({"event": "not_detail", "url": cur})
                except Exception:
                    parsed_detail = False
                # 解析普通页面的链接，继续遍历
                found = 0
                queued = 0
                try:
                    soup = BeautifulSoup(html, "lxml")
                    for a in soup.select("a[href]"):
                        href = a.get("href") or ""
                        if not href:
                            continue
                        if href.startswith("javascript:") or href.startswith("mailto:") or href.startswith("magnet:") or href.startswith("thunder:") or href.startswith("ed2k:"):
                            continue
                        nxt = urljoin(cur, href)
                        if not (nxt.startswith("http://") or nxt.startswith("https://")):
                            continue
                        pu = urlparse(nxt)
                        if allowed_hosts and pu.netloc not in allowed_hosts:
                            continue
                        if re.search(r"\.(?:jpg|jpeg|png|gif|webp|css|js|svg|ico|pdf|zip|rar)(?:\?|$)", pu.path, re.IGNORECASE):
                            continue
                        nxt = nxt.split('#')[0]
                        found += 1
                        if nxt not in seen and nxt not in q:
                            q.append(nxt)
                            queued += 1
                            try:
                                enqueue_urls(self.conn, self.session_id, [nxt])
                            except Exception:
                                pass
                            try:
                                enqueue_urls(self.conn, self.session_id, [nxt])
                            except Exception:
                                pass
                    # 额外提取 frame/iframe 的 src
                    for f in soup.select("frame[src], iframe[src]"):
                        src = f.get("src") or ""
                        if not src:
                            continue
                        nxt = urljoin(cur, src)
                        if not (nxt.startswith("http://") or nxt.startswith("https://")):
                            continue
                        pu = urlparse(nxt)
                        if allowed_hosts and pu.netloc not in allowed_hosts:
                            continue
                        nxt = nxt.split('#')[0]
                        found += 1
                        if nxt not in seen and nxt not in q:
                            q.append(nxt)
                            queued += 1
                    # 处理 meta refresh 重定向
                    for m in soup.select("meta[http-equiv]"):
                        try:
                            hev = (m.get("http-equiv") or "").lower()
                            if hev == "refresh":
                                content = m.get("content") or ""
                                mm = re.search(r"url=([^;]+)", content, re.I)
                                if mm:
                                    nxt = urljoin(cur, mm.group(1).strip())
                                    pu = urlparse(nxt)
                                    if (nxt.startswith("http://") or nxt.startswith("https://")) and (not allowed_hosts or pu.netloc in allowed_hosts):
                                        nxt = nxt.split('#')[0]
                                        found += 1
                                        if nxt not in seen and nxt not in q:
                                            q.append(nxt)
                                            queued += 1
                                            try:
                                                enqueue_urls(self.conn, self.session_id, [nxt])
                                            except Exception:
                                                pass
                        except Exception:
                            pass
                    _emit({"event": "page", "url": cur, "found": found, "queued": queued})
                except Exception as e:
                    _emit({"event": "error", "url": cur, "message": str(e)})
                # 标记访问
                pages += 1
                seen.add(cur)
                mark_visited(self.conn, self.session_id, cur, "page")
                try:
                    mark_queue_done(self.conn, self.session_id, cur)
                except Exception:
                    pass
                try:
                    mark_queue_done(self.conn, self.session_id, cur)
                except Exception:
                    pass
                if self.session_id:
                    self._visited_pages.add(cur)
            except Exception as e:
                _emit({"event": "error", "url": cur, "message": str(e)})
                pages += 1
                seen.add(cur)
                mark_visited(self.conn, self.session_id, cur, "page")
        _emit({"event": "site_done", "total": total})
        return int(total)

    def crawl_all(self, max_pages_per_category: int, max_items_per_category: int, progress_cb: Optional[Callable[[dict], None]] = None) -> int:
        # 兼容旧接口：改为从根路径进行遍历，不再使用分类URL
        return self.crawl_site(None, max_pages_per_category, max_items_per_category, progress_cb=progress_cb)

# 健壮解码与乱码检测
_GARBLED_RE = re.compile(r"(?:\uFFFD|Ã|Â|â[€™”’“]|œ|™)")

def looks_garbled(text: str) -> bool:
    if not text:
        return False
    return len(_GARBLED_RE.findall(text)) >= 2


def decode_response(resp: requests.Response) -> str:
    b = resp.content or b""
    # 从头部提取编码
    encs = []
    ct = resp.headers.get("Content-Type", "")
    m_ct = re.search(r"charset=([\-\w\d]+)", ct, re.I)
    if m_ct:
        encs.append(m_ct.group(1).lower())
    # 从字节内容探测 meta charset
    m_meta = re.search(rb"charset\s*=\s*['\"]?([\-\w\d]+)", b, re.I)
    if m_meta:
        try:
            encs.insert(0, m_meta.group(1).decode("ascii", "ignore").lower())
        except Exception:
            pass
    if resp.encoding:
        encs.append(str(resp.encoding).lower())

    def _norm(e: Optional[str]) -> Optional[str]:
        if not e:
            return None
        e = e.lower()
        if e in ("gb2312", "gbk", "gb-2312"):
            return "gb18030"
        return e

    cands = []
    for e in encs:
        ne = _norm(e)
        if ne and ne not in cands:
            cands.append(ne)
    for e in ["utf-8", "gb18030", "big5", "shift_jis"]:
        if e not in cands:
            cands.append(e)

    for e in cands:
        try:
            text = b.decode(e, errors="replace")
            resp.encoding = e
            return text.lstrip("\ufeff")
        except Exception:
            continue
    try:
        return b.decode("utf-8", errors="replace")
    except Exception:
        return b.decode("latin-1", errors="replace")




