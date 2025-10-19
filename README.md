# DYTT Index

将 `dydytt.net`（电影天堂）站点的电影/剧集信息抓取到本地 SQLite 数据库，并提供命令行检索与详情展示（含下载链接）。

## 功能概述
- 解析代表性页面结构：列表页与详情页（`◎片名/◎年代/◎产地/◎类别/◎豆瓣评分/◎IMDB评分/◎简介/下载链接` 等）
- 提取基本分类：电影/电视剧/综艺/动漫；地区/语言；类型标签（科幻、武侠等）
- 抓取详情页全部内容（简介、演员、导演、封面图、原始HTML），保存下载链接（磁力/ed2k/ftp/torrent/thunder/网盘）
- 命令行检索：按标题、类别、地区、标签、评分、年份范围过滤；展示下载链接

## 安装依赖
```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
```

## 使用方法
1. 初始化数据库：
```bash
python -m dyttindex.cli init-db
```
2. 抓取数据（可限制分页与条目）：
```bash
python -m dyttindex.cli crawl --max-pages-per-category 30 --max-items-per-category 1000
```
3. 查询示例：
```bash
# 按标签与评分查询
python -m dyttindex.cli search --tag 科幻 --rating-min 7.0 --limit 30

# 只看电视剧，产地包含“日本”，年份范围
python -m dyttindex.cli search --kind tv --country 日本 --year-from 2015 --year-to 2024

# 查看某条目的详情（含下载链接）
python -m dyttindex.cli show 123
```

## 设计说明
- 分类与字段来源：
  - 详情页以“◎字段”行做解析（如 `◎片名`、`◎年代`、`◎类别`、`◎豆瓣评分` 等），并对主演/简介多行进行合并
  - 下载链接抓取所有 `#Zoom` 区域内的 `a[href]`，识别 `magnet/ed2k/ftp/torrent/thunder/网盘`
  - 电影/电视剧等 `kind` 用路径与标签推断，难以识别时默认 `movie`
- 数据库：
  - `movies`（基础信息+冗余 `tags_text`），`tags`，`movie_tags`（多对多），`download_links`
  - 以 `detail_url` 作为唯一键进行 upsert；下载链接对每个电影去重

## 备注
- 不同镜像/版本的“电影天堂”可能存在结构差异，本工具针对“多数代表性页面”设计，无法解析的页面会被自动跳过或降级处理
- 抓取时内置随机 UA 与间隔，仍可能受到站点限流或反爬影响；可调整 `dyttindex/config.py` 的 `REQUEST_SLEEP/DEFAULT_MAX_*` 参数
- 数据库存放路径：`c:/Code/dyttindex/data/movies.db`