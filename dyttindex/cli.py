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
    upsert_movie,
)
from .scraper import DyttScraper, init_db, parse_detail_page, decode_response, looks_garbled
from . import config

app = typer.Typer(add_completion=False, help="DYTT 电影数据库构建与查询 CLI")
console = Console()

@app.command("init-db")
def init_db_cmd(drop: bool = typer.Option(False, help="是否清空并重建数据库")):
    """初始化 SQLite 数据库，创建必要表结构。"""
    create_db(drop=drop)
    console.print("[green]SQLite 初始化完成[/green]: ", config.SQLITE_PATH)

@app.command()
def crawl(
    start_url: Optional[str] = typer.Option(None, help="起始URL，默认使用 BASE_URL"),
    max_pages_total: int = typer.Option(config.DEFAULT_MAX_PAGES_TOTAL, help="总页面遍历上限"),
    max_items_total: int = typer.Option(config.DEFAULT_MAX_ITEMS_TOTAL, help="总条目上限"),
    verbose: bool = typer.Option(True, "--verbose/--no-verbose", help="打印抓取进度"),
    jsonl: bool = typer.Option(False, "--json/--no-json", help="以 JSON 行输出进度事件"),
    session_id: Optional[str] = typer.Option(None, "--session-id", help="会话ID，用于断点续爬与事件日志"),
):
    """从根路径进行广度优先遍历抓取详情页；支持会话断点续跑（session_id）。"""
    init_db(drop=False)
    s = DyttScraper(session_id=session_id)
    def _progress(evt: dict):
        if jsonl:
            import json
            console.print(json.dumps(evt, ensure_ascii=False))
        elif verbose:
            if evt.get("event") == "item":
                console.print(f"条目: {evt.get('title')} ({evt.get('year')}) {evt.get('kind')} -> {evt.get('detail_url')}")
            elif evt.get("event") == "page":
                console.print(f"页面: {evt.get('url')} | 链接={evt.get('found')} | 入队={evt.get('queued')}")
            elif evt.get("event") == "detail_saved":
                console.print(f"保存详情: {evt.get('detail_url')}")
            elif evt.get("event") == "warn":
                console.print(f"[yellow]警告[/yellow]: {evt.get('url')} -> {evt.get('message')}")
            elif evt.get("event") == "error":
                console.print(f"[red]错误[/red]: {evt.get('url') or evt.get('detail_url')} -> {evt.get('message')}")
    total = s.crawl_site(start_url or config.BASE_URL, max_pages_total, max_items_total, progress_cb=_progress)
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
           show_downloads: bool = typer.Option(True, help="是否展示下载链接"),
           keyword: Optional[str] = typer.Option(None, help="跨字段关键字（标题/简介/演员等）")):
    """按标题、类别、地区、语言、导演、演员、标签、评分与年份过滤检索结果。支持 keyword 跨字段搜索。"""
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
        keyword=keyword,
    )
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("ID", justify="right", style="cyan", no_wrap=True)
    table.add_column("标题")
    table.add_column("类别")
    table.add_column("年份", justify="right")
    table.add_column("地区")
    table.add_column("评分")
    table.add_column("标签")
    for row in results:
        tags_text = row[9] or ""
        table.add_row(str(row[0]), row[1] or "", row[2] or "", str(row[3] or ""), row[4] or "", f"{row[7] or ''}:{row[8] or ''}", tags_text)
    console.print(table)

    if show_downloads and results:
        conn = get_conn()
        first_id = results[0][0]
        dl = get_download_links(conn, first_id)
        if dl:
            console.print("\n[bold green]下载链接（第一条）[/bold green]")
            for kind, url, label, episode in dl:
                ep = f"  EP{episode}" if episode else ""
                console.print(f"- [{kind}] {label}{ep}: {url}")

@app.command("probe")
def probe(
    start_url: Optional[str] = typer.Option(None, "--start-url", help="起始URL，默认使用 BASE_URL"),
    pages: int = typer.Option(1, "--pages", min=1, help="探测遍历的页面数量"),
):
    """从根路径探测页面与链接提取效果，输出每页链接数量。"""
    s = DyttScraper()
    collected = 0
    def _progress(evt: dict):
        nonlocal collected
        if evt.get("event") == "page":
            console.print(f"页面: {evt.get('url')} | 链接={evt.get('found')} | 入队={evt.get('queued')}")
            collected += 1
    s.crawl_site(start_url or config.BASE_URL, pages, 0, progress_cb=_progress)
    console.print(f"探测完成，遍历页面: {collected}")

# 新增：批量修复（重新解析 raw_html 回填 kind/标签/简介 等）
@app.command("repair")
def repair(
    only_kind: Optional[str] = typer.Option(None, "--only-kind", help="仅处理指定 kind 的记录，如 movie"),
    limit: int = typer.Option(0, "--limit", help="限制处理数量，0 表示不限"),
):
    """重新解析数据库中已抓取条目的 raw_html，回填分类、标签与简介。"""
    conn = get_conn()
    cur = conn.cursor()
    sql = "SELECT id, detail_url, raw_html FROM movies WHERE 1=1"
    params: List[str] = []
    if only_kind:
        sql += " AND kind = ?"
        params.append(only_kind)
    if limit and limit > 0:
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(str(limit))
    cur.execute(sql, params)
    rows = cur.fetchall()
    if not rows:
        console.print("[yellow]没有可修复的记录[/yellow]")
        raise typer.Exit(0)
    fixed = 0
    scraper = DyttScraper()
    for r in rows:
        mid = r[0]
        url = r[1]
        html = r[2]
        # 若库中 HTML 为空或疑似乱码，则重抓并使用健壮解码
        if not html or looks_garbled(html):
            try:
                resp = scraper.s.get(url, timeout=config.REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    html = decode_response(resp)
                else:
                    console.print(f"[yellow]获取HTML失败[/yellow] id={mid} status={resp.status_code} -> {url}")
            except Exception as e:
                console.print(f"[yellow]网络错误[/yellow] id={mid} -> {e}")
        try:
            if not html:
                continue
            data = parse_detail_page(html, url)
            upsert_movie(conn, data)
            fixed += 1
        except Exception as e:
            console.print(f"[red]解析失败[/red] id={mid} url={url}: {e}")
    console.print(f"[bold green]修复完成[/bold green]：更新 {fixed} 条记录")

if __name__ == "__main__":
    app()