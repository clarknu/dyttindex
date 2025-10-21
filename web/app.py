from __future__ import annotations

import threading
import time
import os
import sys
from typing import Optional, List

from flask import Flask, request, jsonify, render_template_string

# 让父目录加入模块搜索路径
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from dyttindex.db import get_conn, search_movies, get_movie, get_download_links, count_movies
from dyttindex.scraper import DyttScraper, init_db
from dyttindex import config

app = Flask(__name__)

# 简单的抓取任务状态
crawl_state = {
    "status": "idle",  # idle|running|stopping|done|error
    "started_at": None,
    "last_update": None,
    "total": 0,
    "messages": [],
}

_scraper: Optional[DyttScraper] = None
_crawl_thread: Optional[threading.Thread] = None


def _progress(event: dict):
    crawl_state["last_update"] = time.time()
    crawl_state["messages"].append(event)
    # 控制消息长度
    if len(crawl_state["messages"]) > 300:
        crawl_state["messages"] = crawl_state["messages"][-300:]


def _run_crawl(max_pages: int, max_items: int, sessionid: Optional[str] = None):
    global _scraper
    try:
        init_db(drop=False)
        _scraper = DyttScraper(session_id=sessionid)
        crawl_state["status"] = "running"
        crawl_state["started_at"] = time.time()
        crawl_state["messages"] = []
        total = _scraper.crawl_site(None, max_pages, max_items, progress_cb=_progress)
        crawl_state["total"] = total
        crawl_state["status"] = "done"
    except Exception as e:
        crawl_state["status"] = "error"
        crawl_state["messages"].append({"event": "error", "message": str(e)})
    finally:
        _scraper = None


@app.post("/api/crawl/start")
def api_crawl_start():
    global _crawl_thread
    if crawl_state["status"] == "running":
        return jsonify({"ok": False, "message": "已有抓取任务在运行"}), 400
    max_pages = int(request.json.get("max_pages", config.DEFAULT_MAX_PAGES_TOTAL))
    max_items = int(request.json.get("max_items", config.DEFAULT_MAX_ITEMS_TOTAL))
    sessionid = (request.json.get("sessionid") or "").strip() or None
    _crawl_thread = threading.Thread(target=_run_crawl, args=(max_pages, max_items, sessionid), daemon=True)
    _crawl_thread.start()
    return jsonify({"ok": True})


@app.post("/api/crawl/stop")
def api_crawl_stop():
    global _scraper
    if _scraper:
        _scraper.stop()
        crawl_state["status"] = "stopping"
        return jsonify({"ok": True})
    return jsonify({"ok": False, "message": "当前无运行抓取"}), 400


@app.get("/api/crawl/status")
def api_crawl_status():
    return jsonify({
        "status": crawl_state["status"],
        "started_at": crawl_state["started_at"],
        "last_update": crawl_state["last_update"],
        "total": crawl_state["total"],
        "messages": crawl_state["messages"],
    })


@app.get("/")
def index():
    html = r"""
    <!doctype html>
    <html lang="zh">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width,initial-scale=1" />
      <title>DYTT 抓取与检索 · 重设计</title>
      <style>
        :root {
          --bg: #f7f9fc; --card: #fff; --border: #e5e7eb; --muted: #6b7280; --text: #111827; --accent: #2563eb;
        }
        * { box-sizing: border-box; }
        body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Noto Sans, Arial, "Microsoft YaHei"; margin: 0; background: var(--bg); color: var(--text); }
        header { padding: 16px 24px; background: #fff; border-bottom: 1px solid var(--border); position: sticky; top: 0; z-index: 999; }
        header h1 { margin: 0; font-size: 18px; }
        .container { padding: 16px 24px; display: grid; grid-template-columns: 1.3fr 0.7fr; gap: 16px; }
        .card { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 14px; box-shadow: 0 1px 2px rgba(0,0,0,0.03); }
        .card h3 { margin: 0 0 10px 0; font-size: 16px; }
        .grid { display: grid; gap: 10px; }
        .grid-2 { grid-template-columns: 1fr 1fr; }
        label { font-size: 12px; color: var(--muted); display: block; margin-bottom: 4px; }
        input, select, textarea { font-size: 14px; padding: 8px; border: 1px solid var(--border); border-radius: 8px; width: 100%; background: #fff; }
        button { font-size: 14px; padding: 8px 12px; border: 1px solid var(--border); border-radius: 8px; background: #fff; cursor: pointer; }
        button.primary { background: var(--accent); color: #fff; border-color: var(--accent); }
        button.ghost { background: transparent; }
        .actions { display: flex; gap: 8px; }

        table { width: 100%; border-collapse: collapse; table-layout: fixed; }
        thead th { position: sticky; top: 0; background: #fff; z-index: 2; }
-        th, td { border-bottom: 1px solid var(--border); padding: 8px; text-align: left; font-size: 13px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
+        th, td { border-bottom: 1px solid var(--border); padding: 8px; text-align: left; font-size: 13px; white-space: normal; overflow-wrap: anywhere; word-break: break-all; }
         tbody tr:hover { background: #f3f6fc; }
         tbody tr.selected { background: #e8f0fe; }
+        /* 强制详情区内容自动换行 */
+        #detail_box, #detail_box * { white-space: normal; overflow-wrap: anywhere; word-break: break-word; }
         .muted { color: var(--muted); }
        .status { font-size: 12px; color: var(--muted); white-space: pre-line; }
        .pill { display: inline-block; padding: 2px 6px; border-radius: 999px; font-size: 12px; border: 1px solid var(--border); background: #fff; }
        .dl-item { font-size: 13px; margin: 4px 0; }
        .dl-item a { color: var(--accent); text-decoration: none; }
        .dl-item a:hover { text-decoration: underline; }
        /* column sizing */
        table thead th:nth-child(1), table tbody td:nth-child(1) { width: 64px; }
        table thead th:nth-child(2), table tbody td:nth-child(2) { width: 220px; }
        table thead th:nth-child(3), table tbody td:nth-child(3) { width: 90px; }
        table thead th:nth-child(4), table tbody td:nth-child(4) { width: 80px; }
        table thead th:nth-child(5), table tbody td:nth-child(5) { width: 120px; }
        table thead th:nth-child(6), table tbody td:nth-child(6) { width: 160px; }
        table thead th:nth-child(7), table tbody td:nth-child(7) { width: 240px; }
        table thead th:nth-child(8), table tbody td:nth-child(8) { width: 120px; }
        table thead th:nth-child(9), table tbody td:nth-child(9) { width: 200px; }
        table thead th:nth-child(10), table tbody td:nth-child(10) { width: 70px; }
      </style>
    </head>
    <body>
      <header>
        <h1>DYTT 抓取与检索 · 重设计</h1>
      </header>
      <div class="container">
        <div class="grid">
          <div class="card">
            <h3>多条件检索</h3>
            <div class="grid grid-2">
              <div>
                <label>关键字</label>
                <input id="q_title" placeholder="标题/简介/演员关键字" />
              </div>
              <div>
                <label>类别</label>
                <select id="q_kind">
                  <option value="">(不限)</option>
                  <option value="movie">电影</option>
                  <option value="tv">电视剧</option>
                  <option value="variety">综艺</option>
                  <option value="anime">动漫</option>
                </select>
              </div>
              <div>
                <label>国别</label>
                <input id="q_country" placeholder="中国/日本/美国..." />
              </div>
              <div>
                <label>语言</label>
                <input id="q_language" placeholder="中文/日语/英语..." />
              </div>
              <div>
                <label>导演</label>
                <input id="q_director" placeholder="导演名包含" />
              </div>
              <div>
                <label>演员</label>
                <input id="q_actors" placeholder="演员名包含" />
              </div>
              <div>
                <label>评分来源</label>
                <select id="q_rating_src">
                  <option value="">(不限)</option>
                  <option value="Douban">豆瓣评分</option>
                  <option value="IMDB">IMDB</option>
                </select>
              </div>
              <div>
                <label>评分下限</label>
                <input id="q_rating_min" type="number" step="0.1" min="0" max="10" />
              </div>
              <div>
                <label>年代起止</label>
                <div class="actions">
                  <input id="q_year_from" type="number" min="1900" max="2100" placeholder="起" />
                  <input id="q_year_to" type="number" min="1900" max="2100" placeholder="止" />
                </div>
              </div>
              <div>
                <label>标签（逗号分隔）</label>
                <input id="q_tags" placeholder="科幻, 喜剧, 动作" />
              </div>
              <div>
                <label>页码 / 每页条数</label>
                <div class="actions">
                  <input id="q_page" type="number" min="1" value="1" />
                  <input id="q_page_size" type="number" min="10" value="50" />
                </div>
              </div>
              <div>
                <label>排序字段</label>
                <select id="q_order_by">
                  <option value="updated_at">更新时间</option>
                  <option value="year">年份</option>
                  <option value="rating">评分</option>
                  <option value="title">标题</option>
                  <option value="id">ID</option>
                </select>
              </div>
              <div>
                <label>排序方向</label>
                <select id="q_order_dir">
                  <option value="desc">降序</option>
                  <option value="asc">升序</option>
                </select>
              </div>
            </div>
            <div class="actions" style="margin-top:8px">
              <button id="btn_search" class="primary">检索</button>
              <span class="muted" id="search_hint">输入条件后点击检索</span>
            </div>
          </div>

          <div class="card">
            <h3>检索结果</h3>
            <div style="max-height: 460px; overflow: auto; border: 1px solid var(--border); border-radius: 8px;">
              <table>
                <thead>
                  <tr><th>ID</th><th>标题</th><th>类别</th><th>年份</th><th>国别</th><th>导演</th><th>演员</th><th>评分</th><th>标签</th><th>详情</th></tr>
                </thead>
                <tbody id="tbody"></tbody>
              </table>
            </div>
            <div class="muted" id="results_hint" style="margin-top:6px">无结果</div>
            <div class="actions" style="margin-top:6px">
              <button id="btn_prev">上一页</button>
              <button id="btn_next">下一页</button>
              <span class="muted" id="pager_hint"></span>
            </div>
          </div>

          <div class="card">
            <h3>抓取控制</h3>
            <div class="grid grid-2">
              <div>
                <label>会话ID（断点续爬）</label>
                <input id="sessionid" placeholder="如 2025-10-taskA" />
              </div>
              <div>
                <label>页面遍历上限</label>
                <input id="max_pages" type="number" min="1" value="5" />
              </div>
              <div>
                <label>条目上限</label>
                <input id="max_items" type="number" min="1" value="200" />
              </div>
              <div class="actions" style="align-items: end;">
                <button id="btn_start" class="primary">开始抓取</button>
                <button id="btn_stop">停止</button>
              </div>
            </div>
            <div class="status" style="margin-top:8px">
              <div>状态：<span id="st_text">idle</span> · 累计条目：<span id="st_total">0</span></div>
              <details style="margin-top:6px">
                <summary>最近进度消息</summary>
                <pre id="st_msgs" style="max-height:180px; overflow:auto; background:#fafafa; padding:8px; border:1px solid var(--border); border-radius: 8px"></pre>
              </details>
            </div>
          </div>
        </div>

        <div class="card">
          <h3>影片详情</h3>
          <div id="detail_empty" class="muted">在左侧检索并点击条目以加载详情</div>
          <div id="detail_box" style="display:none">
            <div class="actions" style="margin-bottom:8px">
              <button id="btn_save" class="primary">保存</button>
              <button id="btn_delete">删除</button>
            </div>
            <div style="margin-bottom:8px">
              <div style="font-size:18px; font-weight:600" id="mgr_title"></div>
              <div class="muted" id="mgr_meta" style="margin-top:2px"></div>
            </div>
            <div class="grid grid-2">
              <div>
                <div><span class="pill" id="mgr_rating"></span></div>
                <div style="margin-top:6px">导演：<span id="mgr_director"></span></div>
                <div style="margin-top:4px">主演：<span id="mgr_actors"></span></div>
                <div style="margin-top:4px">别名：<span id="mgr_aliases"></span></div>
              </div>
              <div>
                <label>标签（逗号分隔）</label>
                <textarea id="mgr_tags" rows="2" placeholder="科幻,动作,喜剧"></textarea>
              </div>
            </div>
            <div style="margin-top:8px">
              <label>简介</label>
              <textarea id="mgr_desc" rows="4" placeholder="简介文本"></textarea>
            </div>
            <div style="margin-top:8px">
              <details>
                <summary>下载链接（含剧集）</summary>
                <div id="mgr_dl"></div>
              </details>
            </div>
          </div>
        </div>
      </div>

      <script>
      var current_id = null;
      var currentRow = null;
      function el(id){ return document.getElementById(id); }
      function setText(id, v){ el(id).textContent = v || ''; }

      function doSearch(){
        try{
          var p = new URLSearchParams();
          function add(k,v){ if(v) p.append(k,v); }
          add('keyword', el('q_title').value);
          add('kind', el('q_kind').value);
          add('country', el('q_country').value);
          add('language', el('q_language').value);
          add('director', el('q_director').value);
          add('actors', el('q_actors').value);
          add('rating_source', el('q_rating_src').value);
          add('rating_min', el('q_rating_min').value);
          add('year_from', el('q_year_from').value);
          add('year_to', el('q_year_to').value);
          var rawTags = (el('q_tags').value||'').split(',');
          for(var i=0;i<rawTags.length;i++){
            var t = rawTags[i].replace(/^[\s\u3000]+|[\s\u3000]+$/g, '');
            if(t) p.append('tag', t);
          }
          var page = parseInt(el('q_page').value||'1', 10); if(!page||page<1) page=1;
          var pageSize = parseInt(el('q_page_size').value||'50', 10); if(!pageSize||pageSize<1) pageSize=50;
          add('page', page);
          add('page_size', pageSize);
          add('order_by', el('q_order_by').value);
          add('order_dir', el('q_order_dir').value);
          fetch('/api/search?'+p.toString())
            .then(function(r){ return r.json(); })
            .then(function(j){ renderResults(j.results || [], j.total||0, j.page||page, j.page_size||pageSize); })
            .catch(function(e){ console.error(e); alert('检索失败'); });
        }catch(e){ console.error(e); alert('检索异常'); }
      }

      function renderResults(list, total, page, pageSize){
        var tb = el('tbody');
        tb.innerHTML = '';
        el('results_hint').textContent = (total ? ('共 '+total+' 条结果') : '无结果');
        for(var k=0;k<list.length;k++){
          var row = list[k];
          var tr = document.createElement('tr');
          tr.innerHTML = ''+
            '<td>'+(row.id||'')+'</td>'+
            '<td>'+(row.title||'')+'</td>'+
            '<td>'+(row.kind||'')+'</td>'+
            '<td>'+(row.year||'')+'</td>'+
            '<td>'+(row.country||'')+'</td>'+
            '<td>'+(row.director||'')+'</td>'+
            '<td>'+(row.actors||'')+'</td>'+
            '<td>'+((row.rating_source||'')+' '+(row.rating_value||''))+'</td>'+
            '<td>'+(row.tags_text||'')+'</td>'+
            '<td><a href="'+(row.detail_url||'#')+'" target="_blank">详情</a></td>';
          tr.style.cursor = 'pointer';
          (function(id, trEl){ tr.onclick = function(){ selectRow(trEl); loadDetail(id); }; })(row.id, tr);
          tb.appendChild(tr);
        }
        var pages = Math.max(1, Math.ceil((total||0) / (pageSize||1)));
        el('pager_hint').textContent = '第 '+page+' / '+pages+' 页';
        el('btn_prev').disabled = (page<=1);
        el('btn_next').disabled = (page>=pages);
      }

      el('btn_prev').onclick = function(){ var p = parseInt(el('q_page').value||'1',10); if(p>1){ el('q_page').value = (p-1); doSearch(); } };
      el('btn_next').onclick = function(){ var p = parseInt(el('q_page').value||'1',10); el('q_page').value = (p+1); doSearch(); };
      function selectRow(tr){
        if(currentRow) currentRow.classList.remove('selected');
        currentRow = tr; if(tr) tr.classList.add('selected');
      }

      function loadDetail(id){
        current_id = id;
        fetch('/api/movie/'+id)
          .then(function(r){ if(!r.ok) throw new Error('未找到'); return r.json(); })
          .then(function(j){
            var m = j.movie || {};
            el('detail_empty').style.display = 'none';
            el('detail_box').style.display = 'block';
            setText('mgr_title', m.title);
            setText('mgr_meta', (m.kind||'')+' / '+(m.year||'')+' / '+(m.country||''));
            setText('mgr_rating', ((m.rating_source||'')+': '+(m.rating_value||'')));
            setText('mgr_director', m.director);
            setText('mgr_actors', m.actors);
            setText('mgr_aliases', m.alt_titles_text);
            el('mgr_tags').value = m.tags_text || '';
            el('mgr_desc').value = m.description || '';
            var box = el('mgr_dl'); box.innerHTML = '';
            var dls = j.downloads || [];
            if(!dls.length){ box.innerHTML = '<div class="muted">无下载链接</div>'; return; }
            for(var i=0;i<dls.length;i++){
              var d = dls[i];
              var epi = d.episode ? ('第'+d.episode+'集 ') : '';
              var linkText = (d.url||'').startsWith('magnet:?') || (d.url||'').startsWith('ed2k://') ? (d.url||'') : (d.label||d.url||'');
              var div = document.createElement('div');
              div.className = 'dl-item';
              div.innerHTML = '<span class="muted">['+(d.kind||'')+']</span> '+epi+'<a href="'+(d.url||'#')+'" target="_blank">'+linkText+'</a>';
              if((d.url||'').startsWith('magnet:?')){
                var btn = document.createElement('button');
                btn.textContent = '复制磁链';
                btn.className = 'ghost';
                btn.style.marginLeft = '8px';
                btn.onclick = (function(u){ return function(){ navigator.clipboard.writeText(u).then(function(){ alert('已复制磁链'); }); }; })(d.url||'');
                div.appendChild(btn);
              }
              box.appendChild(div);
            }
          })
          .catch(function(e){ alert('加载失败: '+e.message); });
      }

      el('btn_save').onclick = function(){
        var id = current_id; if(!id){ alert('请先在检索结果中点击某条目'); return; }
        var body = JSON.stringify({ tags_text: el('mgr_tags').value, description: el('mgr_desc').value });
        fetch('/api/movie/'+id, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: body })
          .then(function(r){ return r.json(); })
          .then(function(j){ alert(j.ok ? '保存成功' : '保存失败'); })
          .catch(function(e){ alert('保存失败: '+e.message); });
      };
      el('btn_delete').onclick = function(){
        var id = current_id; if(!id){ alert('请先在检索结果中点击某条目'); return; }
        if(!confirm('确认删除该条目及其下载链接？')) return;
        fetch('/api/movie/'+id, { method: 'DELETE' })
          .then(function(r){ return r.json(); })
          .then(function(j){ alert(j.ok ? '删除成功' : '删除失败'); })
          .catch(function(e){ alert('删除失败: '+e.message); });
      };

      function pollStatus(){
        fetch('/api/crawl/status')
          .then(function(r){ return r.json(); })
          .then(function(j){
            setText('st_text', j.status || 'idle');
            setText('st_total', j.total || 0);
            var msgs = j.messages || [];
            var lines = msgs.map(function(ev){
              var et = ev.event || ''; var src = ev.category || ev.section || ''; var msg = ev.message || ''; var url = ev.detail_url || ev.url || '';
              if(et==='item'){ return '['+src+'] #'+(ev.count||'')+' '+(ev.title||'')+' ('+(ev.year||'')+') '+(ev.kind||''); }
              else if(et==='page'){ return '['+src+'] page '+(ev.page||'')+': '+(ev.url||'')+' found='+ev.found; }
              else if(et==='category_done'){ return '['+src+'] done count='+ev.count; }
              else if(et==='error'){ return '['+src+'] ERROR '+msg+' -> '+url; }
              else if(et==='warn'){ return '['+src+'] WARN '+msg+' -> '+url; }
              else if(et==='site_start'){ return 'site_start: '+(ev.url||''); }
              else { return (et||'event')+': '+url; }
            }).join('\n');
            el('st_msgs').textContent = lines;
          })
          .catch(function(_){ /* ignore */ });
      }
      var crawlTimer = null;
      el('btn_start').onclick = function(){
        var body = JSON.stringify({
          max_pages: parseInt(el('max_pages').value, 10) || 5,
          max_items: parseInt(el('max_items').value, 10) || 200,
          sessionid: (el('sessionid').value||'').trim()
        });
        fetch('/api/crawl/start', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: body })
          .then(function(r){ return r.json(); })
          .then(function(j){ if(!j.ok){ alert(j.message || '启动失败'); return; } setText('st_text', 'running'); if(crawlTimer) clearInterval(crawlTimer); crawlTimer = setInterval(pollStatus, 1000); })
          .catch(function(e){ alert('启动失败: '+e.message); });
      };
      el('btn_stop').onclick = function(){
        fetch('/api/crawl/stop', { method: 'POST' })
          .then(function(r){ return r.json(); })
          .then(function(j){ if(!j.ok){ alert(j.message || '停止失败'); return; } setTimeout(pollStatus, 500); })
          .catch(function(e){ alert('停止失败: '+e.message); });
      };

      el('btn_search').onclick = doSearch;
      pollStatus();
      </script>
    </body>
    </html>
    """
    return render_template_string(html)

@app.get("/api/search")
def api_search():
    conn = get_conn()
    # 分页与排序参数
    page = int(request.args.get("page", "1"))
    page_size = int(request.args.get("page_size", "50"))
    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 50
    offset = (page - 1) * page_size
    order_by = request.args.get("order_by") or None
    order_dir = request.args.get("order_dir") or "desc"
    # 总数
    total = count_movies(
        conn,
        title=request.args.get("title") or None,
        kind=request.args.get("kind") or None,
        country=request.args.get("country") or None,
        tags=request.args.getlist("tag") or None,
        rating_min=float(request.args.get("rating_min")) if request.args.get("rating_min") else None,
        year_from=int(request.args.get("year_from")) if request.args.get("year_from") else None,
        year_to=int(request.args.get("year_to")) if request.args.get("year_to") else None,
        language=request.args.get("language") or None,
        director=request.args.get("director") or None,
        actors_substr=request.args.get("actors") or None,
        rating_source=request.args.get("rating_source") or None,
        keyword=request.args.get("keyword") or None,
    )
    results = search_movies(
        conn,
        title=request.args.get("title") or None,
        kind=request.args.get("kind") or None,
        country=request.args.get("country") or None,
        tags=request.args.getlist("tag") or None,
        rating_min=float(request.args.get("rating_min")) if request.args.get("rating_min") else None,
        year_from=int(request.args.get("year_from")) if request.args.get("year_from") else None,
        year_to=int(request.args.get("year_to")) if request.args.get("year_to") else None,
        language=request.args.get("language") or None,
        director=request.args.get("director") or None,
        actors_substr=request.args.get("actors") or None,
        rating_source=request.args.get("rating_source") or None,
        limit=page_size,
        keyword=request.args.get("keyword") or None,
        offset=offset,
        order_by=order_by,
        order_dir=order_dir,
    )
    conn.close()
    return jsonify({"results": [dict(r) for r in results], "total": total, "page": page, "page_size": page_size})

@app.get("/api/movie/<int:movie_id>")
def api_movie_get(movie_id: int):
    conn = get_conn()
    m = get_movie(conn, movie_id)
    if not m:
        conn.close()
        return jsonify({"ok": False, "message": "未找到条目"}), 404
    dls = get_download_links(conn, movie_id) or []
    conn.close()
    return jsonify({"ok": True, "movie": dict(m), "downloads": [dict(d) for d in dls]})

@app.put("/api/movie/<int:movie_id>")
def api_movie_update(movie_id: int):
    payload = request.get_json(force=True) or {}
    tags_text = payload.get("tags_text")
    description = payload.get("description")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE movies SET tags_text=?, description=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
        (tags_text, description, movie_id),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

@app.delete("/api/movie/<int:movie_id>")
def api_movie_delete(movie_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM download_links WHERE movie_id=?", (movie_id,))
    cur.execute("DELETE FROM movies WHERE id=?", (movie_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

@app.get("/api/debug")
def api_debug():
    import os
    return jsonify({"file": __file__, "mtime": os.path.getmtime(__file__)})


if __name__ == "__main__":
    import os
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", "5000")))