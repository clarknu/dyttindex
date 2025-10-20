from __future__ import annotations

from typing import Optional, List
import re

import typer
from rich.console import Console
from rich.table import Table

from .db import (
    create_db,
    get_conn,
    search_movies,
    get_movie,
    get_download_links,
)
from .scraper import DyttScraper, init_db
from . import config

app = typer.Typer(add_completion=False, help="DYTT 电影数据库构建与查询 CLI")
console = Console()

@app.command("init-db")
def init_db_cmd(drop: bool = typer.Option(False, help="是否清空并重建数据库")):
    create_db(drop=drop)
    console.print("[green]SQLite 初始化完成[/green]: ", config.SQLITE_PATH)

@app.command()
def crawl(
    auto: bool = typer.Option(True, "--auto/--fixed", help="自动从根路径遍历抓取"),
    start_url: Optional[str] = typer.Option(None, help="起始URL，默认使用 BASE_URL"),
    max_pages_total: int = typer.Option(config.DEFAULT_MAX_PAGES_TOTAL, help="总页面遍历上限（auto模式）"),
    max_items_total: int = typer.Option(config.DEFAULT_MAX_ITEMS_TOTAL, help="总条目上限（auto模式）"),
    max_pages_per_category: int = typer.Option(config.DEFAULT_MAX_PAGES_PER_CATEGORY, help="固定分类模式每类最多分页数"),
    max_items_per_category: int = typer.Option(config.DEFAULT_MAX_ITEMS_PER_CATEGORY, help="固定分类模式每类最多抓取条目数"),
    verbose: bool = typer.Option(True, "--verbose/--no-verbose", help="打印抓取进度"),
    jsonl: bool = typer.Option(False, "--json/--no-json", help="以 JSON 行输出进度事件"),
    session_id: Optional[str] = typer.Option(None, "--session-id", help="会话ID，用于断点续爬与事件日志"),
):
    init_db(drop=False)
    s = DyttScraper(session_id=session_id)
    def _progress(ev: dict):
        try:
            if jsonl:
                import json
                print(json.dumps(ev, ensure_ascii=False))
                return
            et = ev.get("event")
            src = ev.get("category") or ev.get("section") or "site"
            if et == "site_start":
                console.print(f"[bold blue]开始遍历[/bold blue]: {ev.get('url')}")
            elif et == "category_start":
                console.print(f"[bold blue]开始分类[/bold blue]: {src}")
            elif et == "page":
                if not verbose:
                    return
                console.print(f"[cyan]页面[/cyan] [{src}] -> {ev.get('url')} 详情{ev.get('found')}条 队列+{ev.get('queued')}")
            elif et == "item":
                if not verbose:
                    return
                t = ev.get("title") or ""
                yr = ev.get("year") or ""
                kd = ev.get("kind") or ""
                console.print(f"[green]条目{ev.get('count')}[/green] [{src}] {t} ({yr}) {kd} -> {ev.get('detail_url')}")
            elif et == "warn":
                console.print(f"[yellow]警告[/yellow] [{src}] {ev.get('message','')} -> {ev.get('detail_url') or ev.get('url')}")
            elif et == "error":
                console.print(f"[red]错误[/red] [{src}] {ev.get('message','')} -> {ev.get('url')}")
            elif et == "category_done":
                console.print(f"[bold green]分类完成[/bold green]: {src} 抓取 {ev.get('count')} 条")
            elif et == "site_done":
                console.print(f"[bold green]遍历完成[/bold green]: 累计条目 {ev.get('total')}")
        except Exception:
            pass
    if auto:
        total = s.crawl_site(start_url or config.BASE_URL, max_pages_total, max_items_total, progress_cb=_progress)
    else:
        total = s.crawl_all(max_pages_per_category, max_items_per_category, progress_cb=_progress)
    console.print(f"[green]抓取完成[/green]，累计条目: {total}")

@app.command()
def search(title: Optional[str] = typer.Option(None, help="按标题关键词"),
           kind: Optional[str] = typer.Option(None, help="类别: movie/tv/variety/anime"),
           country: Optional[str] = typer.Option(None, help="产地/国家关键词"),
           language: Optional[str] = typer.Option(None, help="语言关键词，如 中文/日语/英语"),
           director: Optional[str] = typer.Option(None, help="导演名包含"),
           actors: Optional[str] = typer.Option(None, help="演员名包含"),
           rating_source: Optional[str] = typer.Option(None, help="评分来源：Douban/IMDB"),
           tag: Optional[List[str]] = typer.Option(None, help="包含的分类标签，可多次指定"),
           rating_min: Optional[float] = typer.Option(None, help="评分下限"),
           year_from: Optional[int] = typer.Option(None, help="年份起"),
           year_to: Optional[int] = typer.Option(None, help="年份止"),
           limit: int = typer.Option(50, help="返回数量上限"),
           show_downloads: bool = typer.Option(True, help="是否展示下载链接")):
    conn = get_conn()
    results = search_movies(
        conn,
        title=title,
        kind=kind,
        country=country,
        tags=tag,
        rating_min=rating_min,
        year_from=year_from,
        year_to=year_to,
        language=language,
        director=director,
        actors_substr=actors,
        rating_source=rating_source,
        limit=limit,
    )

    if not results:
        console.print("[yellow]未找到匹配结果[/yellow]")
        return

    table = Table(title=f"查询结果 ({len(results)}条)")
    table.add_column("ID", justify="right", width=6)
    table.add_column("标题", width=40)
    table.add_column("类别")
    table.add_column("年份")
    table.add_column("产地")
    table.add_column("评分")
    table.add_column("标签", width=36)
    table.add_column("详情页")

    for r in results:
        table.add_row(
            str(r["id"]), r["title"] or "", r["kind"] or "", str(r["year"] or ""), r["country"] or "",
            f"{r['rating_source'] or ''} {r['rating_value'] or ''}",
            r["tags_text"] or "", r["detail_url"] or "",
        )
    console.print(table)

    if show_downloads:
        for r in results:
            console.rule(f"{r['title']} 的下载链接")
            dls = get_download_links(conn, r["id"])
            if not dls:
                console.print("[red]暂无下载链接[/red]")
                continue
            dl = Table(show_lines=True)
            dl.add_column("类型", width=10)
            dl.add_column("链接")
            dl.add_column("说明")
            for d in dls:
                dl.add_row(d["kind"] or "", d["url"], d["label"] or "")
            console.print(dl)
    conn.close()

@app.command()
def show(movie_id: int):
    conn = get_conn()
    m = get_movie(conn, movie_id)
    if not m:
        console.print("[red]未找到该电影[/red]")
        raise typer.Exit(code=1)
    console.print(f"[bold]{m['title']}[/bold] ({m['year'] or ''}) | {m['kind'] or ''}")
    console.print(f"产地: {m['country'] or ''}  语言: {m['language'] or ''}")
    console.print(f"导演: {m['director'] or ''}")
    if m['actors']:
        console.print("主演:")
        console.print(m['actors'])
    console.print(f"评分: {m['rating_source'] or ''} {m['rating_value'] or ''}")
    console.print(f"标签: {m['tags_text'] or ''}")
    console.print(f"详情页: {m['detail_url']}")
    if m['description']:
        console.rule("简介")
        console.print(m['description'])
    console.rule("下载链接")
    dls = get_download_links(conn, movie_id)
    if not dls:
        console.print("[red]暂无下载链接[/red]")
    else:
        for d in dls:
            console.print(f"- {d['kind'] or ''}: {d['url']}  ({d['label'] or ''})")
    conn.close()

@app.command("probe")
def probe(
    category: str = typer.Option(None, "--category", help="指定分类键，例如 'movies'"),
    pages: int = typer.Option(1, "--pages", min=1, help="探测列表页数量"),
):
    """探测分类列表页与解析效果，输出每页提取到的详情链接数量与下一页链接。"""
    if category and category not in config.CATEGORIES:
        console.print(f"[red]未知分类 {category}，可选：{list(config.CATEGORIES.keys())}[/red]")
        raise typer.Exit(1)
    cats = {category: config.CATEGORIES[category]} if category else config.CATEGORIES
    scraper = DyttScraper()
    for name, path in cats.items():
        url = path if path.startswith("http") else f"{config.BASE_URL.rstrip('/')}/{path.lstrip('/')}"
        console.print(f"[bold green]分类[/bold green]: {name} -> {url}")
        cur = url
        for i in range(pages):
            html = scraper.s.get(cur, timeout=config.REQUEST_TIMEOUT)
            if html.status_code != 200:
                console.print(f"[red]请求失败[/red]: {cur} -> {html.status_code}")
                break
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html.text, "lxml")
            # 简单统计
            links = []
            for a in soup.select("a[href]"):
                h = a.get("href") or ""
                if "/html/" in h and re.search(r"/html/.+/\d+/.+\.html$", h):
                    links.append(h)
            next_url = None
            for a in soup.select("a[href]"):
                t = (a.get_text() or "").strip()
                h = a.get("href") or ""
                if t in {"下一页", "下一頁"}:
                    next_url = h
                    break
            if not next_url:
                for a in soup.select("a[href]"):
                    h = a.get("href") or ""
                    if re.search(r"(list_|index_).*\.html$", h):
                        next_url = h
                        break
            console.print(f"第{i+1}页: 详情链接 {len(links)} 条, 下一页: {next_url}")
            if not next_url:
                break
            cur = next_url if next_url.startswith("http") else f"{config.BASE_URL.rstrip('/')}/{next_url.lstrip('/')}"

if __name__ == "__main__":
    app()