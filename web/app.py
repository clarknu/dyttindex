from __future__ import annotations

import threading
import time
import os
import sys
from typing import Optional, List

from flask import Flask, request, jsonify, render_template_string

# 让父目录加入模块搜索路径
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from dyttindex.db import get_conn, search_movies, get_movie, get_download_links
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


def _run_crawl(max_pages: int, max_items: int):
    global _scraper
    try:
        init_db(drop=False)
        _scraper = DyttScraper()
        crawl_state["status"] = "running"
        crawl_state["started_at"] = time.time()
        crawl_state["messages"] = []
        total = _scraper.crawl_all(max_pages, max_items, progress_cb=_progress)
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
    max_pages = int(request.json.get("max_pages", config.DEFAULT_MAX_PAGES_PER_CATEGORY))
    max_items = int(request.json.get("max_items", config.DEFAULT_MAX_ITEMS_PER_CATEGORY))
    _crawl_thread = threading.Thread(target=_run_crawl, args=(max_pages, max_items), daemon=True)
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
    <html lang=zh>
    <head>
      <meta charset="utf-8" />
      <title>DYTT 抓取与检索 v2</title>
      <style>
        body { font-family: system-ui, sans-serif; margin: 20px; }
        .row { display: flex; gap: 24px; }
        .card { border: 1px solid #ddd; padding: 16px; border-radius: 8px; flex: 1; }
        input, select { padding: 6px; margin: 4px 0; width: 100%; }
        button { padding: 8px 12px; margin-top: 8px; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #eee; padding: 8px; text-align: left; }
        .dl { font-size: 12px; color: #555; }
        .status { font-size: 12px; color: #666; }
      </style>
    </head>
    <body>
      <h2>DYTT 抓取与检索 v2</h2>
      <div class="row">
        <div class="card">
          <h3>抓取控制</h3>
          <label>每类最多页数</label>
          <input id=max_pages type=number min=1 value=10 />
          <label>每类最多条目</label>
          <input id=max_items type=number min=1 value=500 />
          <button id=btn_start>开始抓取</button>
          <button id=btn_stop>停止</button>
          <div class=status>
            <p>状态: <span id=st_text>idle</span></p>
            <p>累计条目: <span id=st_total>0</span></p>
            <details>
              <summary>最近进度消息</summary>
              <pre id=st_msgs style="max-height:200px; overflow:auto; background:#fafafa"></pre>
            </details>
          </div>
        </div>
        <div class="card">
          <h3>多条件检索</h3>
          <label>关键字</label>
          <input id=q_title placeholder="标题关键词" />
          <label>类别</label>
          <select id=q_kind>
            <option value="">(不限)</option>
            <option value=movie>电影</option>
            <option value=tv>电视剧</option>
            <option value=variety>综艺</option>
            <option value=anime>动漫</option>
          </select>
          <label>国别</label>
          <input id=q_country placeholder="中国/日本/美国..." />
          <label>语言</label>
          <input id=q_language placeholder="中文/日语/英语..." />
          <label>导演</label>
          <input id=q_director placeholder="导演名包含" />
          <label>演员</label>
          <input id=q_actors placeholder="演员名包含" />
          <label>评分来源</label>
          <select id=q_rating_src>
            <option value="">(不限)</option>
            <option value=豆瓣评分>豆瓣评分</option>
            <option value=IMDB>IMDB</option>
          </select>
          <label>评分下限</label>
          <input id=q_rating_min type=number step=0.1 min=0 max=10 />
          <label>年代范围</label>
          <div style="display:flex; gap:8px">
            <input id=q_year_from type=number min=1900 max=2100 placeholder="起" />
            <input id=q_year_to type=number min=1900 max=2100 placeholder="止" />
          </div>
          <label>题材/标签（逗号分隔）</label>
          <input id=q_tags placeholder="科幻, 喜剧, 动作" />
          <button id=btn_search>检索</button>
        </div>
      </div>
      <div class="card" style="margin-top:16px">
        <h3>检索结果</h3>
        <table id=tbl>
          <thead><tr><th>ID</th><th>标题</th><th>类别</th><th>年份</th><th>国别</th><th>评分</th><th>标签</th><th>详情页</th></tr></thead>
          <tbody id=tbody></tbody>
        </table>
      </div>

      <div class="card" style="margin-top:16px">
        <h3>数据管理（按ID）</h3>
        <label>条目ID</label>
        <div style="display:flex; gap:8px">
          <input id=mgr_id type=number min=1 placeholder="如 123" />
          <button id=btn_load>加载</button>
          <button id=btn_save>保存</button>
          <button id=btn_delete>删除</button>
        </div>
        <div style="margin-top:8px; font-size:14px; color:#555">
          <p>标题：<span id=mgr_title></span></p>
          <p>类别/年份/国别：<span id=mgr_meta></span></p>
          <p>评分：<span id=mgr_rating></span></p>
        </div>
        <label>标签（逗号分隔）</label>
        <textarea id=mgr_tags rows=2 placeholder="科幻,动作,喜剧"></textarea>
        <label>简介</label>
        <textarea id=mgr_desc rows=4 placeholder="简介文本"></textarea>
        <details style="margin-top:8px">
          <summary>下载链接（含剧集）</summary>
          <ul id=mgr_dl></ul>
        </details>
      </div>

      <script>
      // 恢复搜索功能，保留简化的拼接与容错
      document.getElementById('btn_search').onclick = function(){
        try{
          var p = new URLSearchParams();
          function add(k,v){ if(v) p.append(k,v); }
          add('title', document.getElementById('q_title').value);
          add('kind', document.getElementById('q_kind').value);
          add('country', document.getElementById('q_country').value);
          add('language', document.getElementById('q_language').value);
          add('director', document.getElementById('q_director').value);
          add('actors', document.getElementById('q_actors').value);
          add('rating_source', document.getElementById('q_rating_src').value);
          add('rating_min', document.getElementById('q_rating_min').value);
          add('year_from', document.getElementById('q_year_from').value);
          add('year_to', document.getElementById('q_year_to').value);
          var rawTags = document.getElementById('q_tags').value.split(',');
          for(var i=0;i<rawTags.length;i++){
            var t = rawTags[i].replace(/^[\s\u3000]+|[\s\u3000]+$/g, '');
            if(t){ p.append('tag', t); }
          }
          fetch('/api/search?'+p.toString())
            .then(function(r){ return r.json(); })
            .then(function(j){
              var tb = document.getElementById('tbody');
              tb.innerHTML = '';
              for(var k=0;k<(j.results||[]).length;k++){
                var row = j.results[k];
                var tr = document.createElement('tr');
                var html = '';
                html += '<td>'+(row.id||'')+'</td>';
                html += '<td>'+(row.title||'')+'</td>';
                html += '<td>'+(row.kind||'')+'</td>';
                html += '<td>'+(row.year||'')+'</td>';
                html += '<td>'+(row.country||'')+'</td>';
                html += '<td>'+((row.rating_source||'')+' '+(row.rating_value||''))+'</td>';
                html += '<td>'+(row.tags_text||'')+'</td>';
                html += '<td><a href="'+(row.detail_url||'#')+'" target="_blank">详情</a></td>';
                tr.innerHTML = html;
                tb.appendChild(tr);
              }
            })
            .catch(function(e){ console.error(e); alert('检索失败'); });
        }catch(e){ console.error(e); alert('检索异常'); }
      };

      // 管理：加载、保存、删除
      document.getElementById('btn_load').onclick = function(){
        var id = parseInt(document.getElementById('mgr_id').value, 10);
        if(!id){ alert('请输入ID'); return; }
        fetch('/api/movie/'+id)
          .then(function(r){ if(!r.ok) throw new Error('未找到'); return r.json(); })
          .then(function(j){
            var m = j.movie || {};
            document.getElementById('mgr_title').textContent = m.title || '';
            document.getElementById('mgr_meta').textContent = (m.kind||'')+' / '+(m.year||'')+' / '+(m.country||'');
            document.getElementById('mgr_rating').textContent = ((m.rating_source||'')+': '+(m.rating_value||''));
            document.getElementById('mgr_tags').value = m.tags_text || '';
            document.getElementById('mgr_desc').value = m.description || '';
            var ul = document.getElementById('mgr_dl');
            ul.innerHTML = '';
            var dls = j.downloads || [];
            for(var i=0;i<dls.length;i++){
              var d = dls[i];
              var li = document.createElement('li');
              var epi = d.episode ? ('第'+d.episode+'集 ') : '';
              li.textContent = (d.kind||'')+' '+epi+(d.label||'');
              ul.appendChild(li);
            }
          })
          .catch(function(e){ alert('加载失败: '+e.message); });
      };

      document.getElementById('btn_save').onclick = function(){
        var id = parseInt(document.getElementById('mgr_id').value, 10);
        if(!id){ alert('请输入ID'); return; }
        var body = JSON.stringify({
          tags_text: document.getElementById('mgr_tags').value,
          description: document.getElementById('mgr_desc').value,
        });
        fetch('/api/movie/'+id, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: body })
          .then(function(r){ return r.json(); })
          .then(function(j){ alert(j.ok ? '保存成功' : '保存失败'); })
          .catch(function(e){ alert('保存失败: '+e.message); });
      };

      document.getElementById('btn_delete').onclick = function(){
        var id = parseInt(document.getElementById('mgr_id').value, 10);
        if(!id){ alert('请输入ID'); return; }
        if(!confirm('确认删除该条目及其下载链接？')) return;
        fetch('/api/movie/'+id, { method: 'DELETE' })
          .then(function(r){ return r.json(); })
          .then(function(j){ alert(j.ok ? '删除成功' : '删除失败'); })
          .catch(function(e){ alert('删除失败: '+e.message); });
      };
      </script>
    </body>
    </html>
    """
    return render_template_string(html)


@app.get("/api/search")
def api_search():
    conn = get_conn()
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
        limit=int(request.args.get("limit", "100")),
    )
    conn.close()
    return jsonify({"results": [dict(r) for r in results]})

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