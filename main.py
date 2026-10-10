# -*- coding: utf-8 -*-
"""
TERMINAL // MARKET INTELLIGENCE
--------------------------------
Bloomberg端末風のリアルタイム株価・ニュース監視サーバー。

主な設計方針:
  - すべての設定値は本ファイル冒頭の CONFIG セクションに集約し、環境変数で上書き可能にする
  - 例外は必ず logging に記録する（print は使わない）
  - 誰も購読していないクエリの監視スレッドは一定時間で自動的に停止し、スレッドリークを防ぐ
  - 外部通信（yfinance / Google News RSS）はリトライ・タイムアウトを必ず設定する
  - 外部データ（Excelマスタ, yfinance）が取得できなくてもアプリ全体は落とさず、
    呼び出し元に分かりやすい status を返す
"""
import hashlib
import json
import logging
import re
import time
import threading
import queue
import os
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus, quote as url_quote
from collections import defaultdict
from typing import Dict, List, Optional, Set

import numpy as np
import yfinance as yf
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from flask import Flask, Response, render_template, request, stream_with_context, jsonify
import pandas as pd
import pytz

# ============================================================
# CONFIG
# ============================================================
EXCEL_MASTER_PATH = os.environ.get("STOCK_MASTER_PATH", "data_j.xls")
DEFAULT_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "8"))
STOCK_POLL_INTERVAL_SEC = float(os.environ.get("STOCK_POLL_INTERVAL_SEC", "1"))
NEWS_POLL_INTERVAL_SEC = float(os.environ.get("NEWS_POLL_INTERVAL_SEC", "30"))
IDLE_SLEEP_SEC = 5                 # 購読者がいない間の待機間隔
IDLE_MONITOR_TTL_SEC = int(os.environ.get("IDLE_MONITOR_TTL_SEC", "600"))  # 10分無購読で監視停止
NEWS_LOOKBACK_HOURS = 24           # 初回接続時に遡ってニュースを新着扱いする範囲
NEWS_PAGE_SIZE_PER_SOURCE = int(os.environ.get("NEWS_PAGE_SIZE_PER_SOURCE", "15"))
# クエリのバリエーション。会社名だけでなく「株価」「決算」等でも横断検索することで
# 単一キーワード検索では拾えない関連ニュースまで広く収集する。
NEWS_QUERY_VARIANTS = [
    v.strip() for v in os.environ.get("NEWS_QUERY_VARIANTS", "{q}|{q} 株価|{q} 決算").split("|") if v.strip()
]
SSE_HEARTBEAT_SEC = 15
HISTORY_INTERVAL_DEFAULT = "5m"
HISTORY_PERIOD_DEFAULT = "1d"
STOCK_DETAIL_REFRESH_SEC = float(os.environ.get("STOCK_DETAIL_REFRESH_SEC", "20"))

# チャートパネルで選択できる足（interval）と、Yahoo側の制約に沿った取得期間（period）。
# Yahoo Finance は 1分足=直近7日以内、5分/15分足=直近60日以内 などの制限があるため、
# 実用上安全な期間をあらかじめマッピングしておく。
MAX_CHART_POINTS = int(os.environ.get("MAX_CHART_POINTS", "600"))
CHART_INTERVAL_PERIOD_MAP = {
    "1m": "5d",
    "2m": "5d",
    "5m": "1mo",
    "15m": "1mo",
    "30m": "3mo",
    "60m": "6mo",
    "1d": "2y",
    "1wk": "10y",
    "1mo": "max",
}

# 検索していなくても常時流れるグローバル市場ヘッドライン
GLOBAL_CHANNEL = "__market_pulse__"
MARKET_PULSE_QUERY = os.environ.get("MARKET_PULSE_QUERY", "日経平均")
MARKET_PULSE_REFRESH_SEC = float(os.environ.get("MARKET_PULSE_REFRESH_SEC", "90"))

# Yahoo! リアルタイム検索設定
YAHOO_REALTIME_POLL_INTERVAL_SEC = float(os.environ.get("YAHOO_REALTIME_POLL_INTERVAL_SEC", "5"))

# 空売り情報（karauri.net）設定
KARAURI_CACHE_TTL_SEC = int(os.environ.get("KARAURI_CACHE_TTL_SEC", "3600"))  # TTL内は通信せずキャッシュを返す
# システムプロキシが原因で WinError 10061 等が出る環境向けに、既定ではプロキシを使わず直接接続する。
# プロキシ経由で接続したい場合は KARAURI_BYPASS_PROXY=0 を指定する。
KARAURI_BYPASS_PROXY = os.environ.get("KARAURI_BYPASS_PROXY", "1") == "1"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("terminal")

app = Flask(__name__)

# ============================================================
# HTTP SESSION（接続プール + リトライ/バックオフ）
# ============================================================
session = requests.Session()
_retry_cfg = Retry(
    total=2,
    connect=2,
    read=2,
    backoff_factor=0.3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["GET"]),
)
_adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=_retry_cfg)
session.mount("http://", _adapter)
session.mount("https://", _adapter)
session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "ja,en;q=0.8"})


def now_iso() -> str:
    return datetime.now().strftime("%H:%M:%S")


def clean_text(text: str, limit: int = 300) -> str:
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > limit:
        text = text[:limit].rstrip() + "…"
    return text


# ============================================================
# 銘柄マスタ（Excel）ロード & 正規化検索
# ============================================================

def normalize(s) -> str:
    """全角/半角・カタカナ/ひらがな・大文字/小文字の揺れを吸収する正規化。"""
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKC", s).lower()
    res = []
    for char in s:
        code = ord(char)
        if 0x30A1 <= code <= 0x30F6:  # カタカナ -> ひらがな
            res.append(chr(code - 0x60))
        else:
            res.append(char)
    return "".join(res)


class StockMaster:
    """東証銘柄マスタ（data_j.xls 等）の遅延ロード + mtime に基づく自動再読込。"""

    def __init__(self, path: str):
        self.path = path
        self._df: Optional[pd.DataFrame] = None
        self._mtime: Optional[float] = None
        self._lock = threading.Lock()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            logger.warning("銘柄マスタが見つかりません: %s （銘柄名検索は無効化されます）", self.path)
            self._df = None
            return
        try:
            df = pd.read_excel(self.path)
            code_col = df.columns[1]
            name_col = df.columns[2]
            df["_norm_name"] = df[name_col].astype(str).apply(normalize)
            # 長音符「ー」の有無による表記ゆれ（例: 正式名称「ＱＤレーザ」 vs 検索語「QDレーザー」）
            # を吸収するための緩いマッチ用カラム。
            df["_norm_name_loose"] = df["_norm_name"].str.replace("ー", "", regex=False)
            df["_norm_code"] = df[code_col].astype(str).apply(normalize)
            self._df = df
            self._mtime = os.path.getmtime(self.path)
            logger.info("銘柄マスタを読み込みました: %s (%d件)", self.path, len(df))
        except Exception:
            logger.exception("銘柄マスタの読み込みに失敗しました: %s", self.path)
            self._df = None

    def _ensure_fresh(self) -> None:
        with self._lock:
            if self._df is None:
                self._load()
                return
            try:
                mtime = os.path.getmtime(self.path)
                if mtime != self._mtime:
                    self._load()
            except OSError:
                pass

    @property
    def df(self) -> Optional[pd.DataFrame]:
        self._ensure_fresh()
        return self._df

    def search(self, query: str, limit: int = 20) -> List[dict]:
        df = self.df
        if df is None or df.empty:
            return []
        code_col = df.columns[1]
        name_col = df.columns[2]
        q_norm = normalize(query)
        matches = df[
            df["_norm_name"].str.contains(q_norm, na=False, regex=False)
            | df["_norm_code"].str.contains(q_norm, na=False, regex=False)
        ]
        if matches.empty:
            # 完全一致で見つからない場合、長音符「ー」の有無による表記ゆれを
            # 吸収した緩いマッチにフォールバックする。
            # 例: 「QDレーザー」で検索しても正式名称「ＱＤレーザ」にヒットさせる。
            q_norm_loose = q_norm.replace("ー", "")
            if q_norm_loose:
                matches = df[df["_norm_name_loose"].str.contains(q_norm_loose, na=False, regex=False)]
        matches = matches.head(limit)
        return [
            {"code": str(row[code_col]), "name": str(row[name_col])}
            for _, row in matches.iterrows()
        ]

    def resolve_code(self, query: str) -> str:
        if len(query) == 4 and query.isalnum() and query.isascii():
            return query
        hits = self.search(query, limit=1)
        if hits:
            return hits[0]["code"]
        return query


stock_master = StockMaster(EXCEL_MASTER_PATH)


def to_ticker_symbol(query: str) -> str:
    """検索クエリ（銘柄名 or コード）を yfinance のティッカーシンボルへ変換する。"""
    resolved = stock_master.resolve_code(query)
    if len(resolved) == 4 and resolved.isalnum() and resolved.isascii():
        return f"{resolved}.T"
    return resolved


# ============================================================
# 市場ステータス
# ============================================================
_JST = pytz.timezone("Asia/Tokyo")


def get_market_status() -> str:
    now = datetime.now(_JST)
    if now.weekday() >= 5:
        return "CLOSED"
    t = now.time()
    if datetime.strptime("09:00", "%H:%M").time() <= t <= datetime.strptime("11:30", "%H:%M").time():
        return "OPEN"
    if datetime.strptime("12:30", "%H:%M").time() <= t <= datetime.strptime("15:00", "%H:%M").time():
        return "OPEN"
    if datetime.strptime("11:30", "%H:%M").time() < t < datetime.strptime("12:30", "%H:%M").time():
        return "LUNCH"
    return "CLOSED"


# ============================================================
# スクレイパー基底クラス
# ============================================================
class BaseScraper:
    site_name: str = "Unknown"
    icon: str = "◆"
    color: str = "#ff9900"

    def result(self, *, items=None, summary="", hero_image=None, pages_fetched=0, status="ok") -> dict:
        items = items or []
        return {
            "site": self.site_name,
            "icon": self.icon,
            "color": self.color,
            "status": status,
            "hero_image": hero_image,
            "summary": summary,
            "pages_fetched": pages_fetched,
            "total_items": len(items),
            "items": items,
            "fetched_at": now_iso(),
        }

    def empty(self, msg="No data found") -> dict:
        return self.result(summary=msg, status="empty")

    def error(self, msg="Connection failed") -> dict:
        return self.result(summary=msg, status="error")


# ============================================================
# ニュースプロバイダ（複数ソース × 複数クエリを並列収集）
# ============================================================

def _safe_parse_date(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        return parsedate_to_datetime(date_str)
    except Exception:
        return None


def _normalize_url(url: str) -> str:
    """重複排除用にURLを正規化する（クエリパラメータ・フラグメントを除去）。"""
    if not url:
        return ""
    return url.split("?")[0].split("#")[0].strip().lower()


def _normalize_title(title: str) -> str:
    """異なるソース間で同一記事を検出するためのタイトル正規化。"""
    return re.sub(r"\s+", "", title or "").lower()


class RssNewsSource:
    """RSS形式のニュース検索エンジンを扱う汎用基底クラス。"""

    name: str = "RSS"

    def build_url(self, query: str) -> str:
        raise NotImplementedError

    def parse_item(self, item) -> Optional[dict]:
        raise NotImplementedError

    def fetch(self, query: str, limit: int) -> List[dict]:
        try:
            url = self.build_url(query)
            r = session.get(url, timeout=DEFAULT_TIMEOUT)
            if r.status_code != 200:
                logger.info("%s: HTTP %s (query=%r)", self.name, r.status_code, query)
                return []
            soup = BeautifulSoup(r.content, "xml")
            entries = soup.find_all("item")[:limit]
            items = []
            for it in entries:
                try:
                    parsed = self.parse_item(it)
                except Exception:
                    parsed = None
                if parsed and parsed.get("title"):
                    items.append(parsed)
            return items
        except requests.RequestException as e:
            logger.info("%s: 取得失敗 query=%r: %s", self.name, query, e)
            return []
        except Exception:
            logger.warning("%s: 予期しないエラー query=%r", self.name, query, exc_info=True)
            return []


class GoogleNewsSource(RssNewsSource):
    name = "Google News"

    def build_url(self, query: str) -> str:
        return f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=ja&gl=JP&ceid=JP:ja"

    def parse_item(self, it) -> Optional[dict]:
        pub_dt = _safe_parse_date(it.find("pubDate").text if it.find("pubDate") else "")
        source_tag = it.find("source")
        return {
            "title": it.find("title").text if it.find("title") else "",
            "url": it.find("link").text if it.find("link") else "",
            "snippet": clean_text(it.find("description").text if it.find("description") else "", 150),
            "meta": (source_tag.text if source_tag else "") or self.name,
            "image": None,
            "pub_date": pub_dt.isoformat() if pub_dt else "",
            "_pub_dt": pub_dt,  # 内部ソート/重複排除用（送信時に除去）
            "_source": self.name,
        }


class BingNewsSource(RssNewsSource):
    """Bing News検索のRSSビュー。Googleとは異なるインデックスのため収集範囲が広がる。"""

    name = "Bing News"

    def build_url(self, query: str) -> str:
        return (
            "https://www.bing.com/news/search?q="
            + quote_plus(query)
            + '&format=rss&setmkt=ja-JP&cc=JP&qft=sortbydate%3d%221%22'
        )

    def parse_item(self, it) -> Optional[dict]:
        pub_dt = _safe_parse_date(it.find("pubDate").text if it.find("pubDate") else "")
        source_tag = it.find("source") or it.find("Source")
        return {
            "title": it.find("title").text if it.find("title") else "",
            "url": it.find("link").text if it.find("link") else "",
            "snippet": clean_text(it.find("description").text if it.find("description") else "", 150),
            "meta": (source_tag.text if source_tag else "") or self.name,
            "image": None,
            "pub_date": pub_dt.isoformat() if pub_dt else "",
            "_pub_dt": pub_dt,
            "_source": self.name,
        }


class NewsAggregator(BaseScraper):
    """
    複数ニュースソース（Google News / Bing News）× 複数クエリバリエーションを
    並列に取得し、URL・タイトルの二段階で重複排除したうえで新着順にまとめる。
    単一ソース・単一キーワードの検索よりも広い範囲の関連ニュースを収集できる。
    """

    site_name = "News"
    icon = "⌘"
    color = "#00c2ff"

    def __init__(self):
        self.sources: List[RssNewsSource] = [GoogleNewsSource(), BingNewsSource()]

    def fetch(self, query: str) -> dict:
        jobs = [
            (source, variant.format(q=query))
            for source in self.sources
            for variant in NEWS_QUERY_VARIANTS
        ]

        collected: List[dict] = []
        with ThreadPoolExecutor(max_workers=max(4, len(jobs))) as pool:
            futures = {
                pool.submit(source.fetch, q, NEWS_PAGE_SIZE_PER_SOURCE): (source.name, q)
                for source, q in jobs
            }
            for fut in as_completed(futures, timeout=DEFAULT_TIMEOUT + 5):
                source_name, q = futures[fut]
                try:
                    collected.extend(fut.result())
                except Exception:
                    logger.warning("ニュース取得タスクが失敗 source=%s query=%r", source_name, q, exc_info=True)

        if not collected:
            return self.empty("ニュースが見つかりませんでした")

        # 1. URL正規化での重複排除（同一ソース内・Google記事間の重複に有効）
        by_url: Dict[str, dict] = {}
        for item in collected:
            key = _normalize_url(item.get("url", ""))
            if key and key not in by_url:
                by_url[key] = item

        # 2. タイトル正規化での重複排除（ソースをまたいだ同一記事の重複に有効）
        seen_titles: Set[str] = set()
        deduped: List[dict] = []
        for item in by_url.values():
            key = _normalize_title(item.get("title", ""))
            if key and key in seen_titles:
                continue
            if key:
                seen_titles.add(key)
            deduped.append(item)

        deduped.sort(
            key=lambda x: x["_pub_dt"] or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        deduped = deduped[: NEWS_PAGE_SIZE_PER_SOURCE * 2]

        source_names = sorted({it.get("_source", "") for it in deduped if it.get("_source")})
        summary = f"{len(deduped)}件のニュースを取得（{' / '.join(source_names)}）"
        return self.result(items=deduped, pages_fetched=len(jobs), summary=summary)


# ============================================================
# Yahoo! リアルタイム検索スクレイパー
# ============================================================
class YahooRealtimeScraper(BaseScraper):
    """
    Yahoo! リアルタイム検索（https://search.yahoo.co.jp/realtime/）から
    キーワードに関する投稿を取得する。
    公式APIではなくHTML/JSONスクレイピングのため、サイト仕様変更で動作しなくなる
    可能性がある点に注意。
    """

    site_name = "Yahoo! RT"
    icon = "🔴"
    color = "#ff0033"
    BASE_URL = "https://search.yahoo.co.jp/realtime/search"

    _SCRAPE_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ja,en;q=0.8",
    }

    def _extract_media(self, post: dict) -> tuple:
        """
        投稿に添付された画像・動画のURLを抽出する。

        Yahoo!リアルタイム検索はX(Twitter)の旧APIに近いデータ構造
        （entities/extended_entities配下にmedia配列を持つ形）を使っていることが多いため、
        まずはその構造を優先的に探索する。見つからない場合は、投稿JSON全体を走査して
        画像・動画URLらしき文字列（拡張子やCDNドメインで判定）を拾うフォールバックを行う。
        アイコン・アバター画像は誤検出しないよう、キー名に "icon"/"avatar"/"profile"/"user"
        を含むサブツリーは探索対象から除外する。
        """
        image_url = None
        video_url = None

        def _from_media_list(media_list):
            nonlocal image_url, video_url
            for m in media_list or []:
                if not isinstance(m, dict):
                    continue
                mtype = m.get("type", "")
                if mtype in ("video", "animated_gif"):
                    variants = (m.get("video_info") or {}).get("variants") or []
                    mp4s = [v for v in variants if v.get("content_type") == "video/mp4" and v.get("url")]
                    if mp4s and not video_url:
                        best = max(mp4s, key=lambda v: v.get("bitrate", 0))
                        video_url = best["url"]
                    elif variants and not video_url:
                        for v in variants:
                            if v.get("url"):
                                video_url = v["url"]
                                break
                    if not image_url:
                        image_url = m.get("media_url_https") or m.get("media_url")
                elif mtype == "photo" or "media_url_https" in m or "media_url" in m:
                    if not image_url:
                        image_url = m.get("media_url_https") or m.get("media_url")

        extended = post.get("extended_entities") or post.get("extendedEntities") or {}
        _from_media_list(extended.get("media"))

        if not image_url and not video_url:
            entities = post.get("entities") or {}
            _from_media_list(entities.get("media"))

        if not image_url and not video_url:
            for key in ("media", "photos", "images", "attachments"):
                val = post.get(key)
                if isinstance(val, list):
                    _from_media_list(val)
                    if image_url or video_url:
                        break

        if not image_url and not video_url:
            def _walk(obj, depth=0):
                nonlocal image_url, video_url
                if depth > 6:
                    return
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if isinstance(k, str) and any(
                            w in k.lower() for w in ("icon", "avatar", "profileimage", "user", "author")
                        ):
                            continue
                        _walk(v, depth + 1)
                elif isinstance(obj, list):
                    for v in obj:
                        _walk(v, depth + 1)
                elif isinstance(obj, str) and obj.startswith("http"):
                    low = obj.lower()
                    if not video_url and (".m3u8" in low or low.endswith(".mp4")):
                        video_url = obj
                    elif not image_url and ("yimg.jp" in low or "pbs.twimg.com" in low) and "profile_images" not in low:
                        image_url = obj

            _walk(post)

        return image_url, video_url

    def fetch(self, keyword: str) -> dict:
        """Yahoo! リアルタイム検索から投稿一覧と設定を取得する。"""
        try:
            url = f"{self.BASE_URL}?p={url_quote(keyword)}"
            resp = requests.get(url, headers=self._SCRAPE_HEADERS, timeout=DEFAULT_TIMEOUT)
            if resp.status_code != 200:
                return self.error(f"Yahoo RT HTTP {resp.status_code}")

            soup = BeautifulSoup(resp.text, "html.parser")
            script_tag = soup.find("script", id="__NEXT_DATA__")
            if not script_tag or not script_tag.string:
                return self.error("Yahoo RT: __NEXT_DATA__ が見つかりません")

            data = json.loads(script_tag.string)
            if not isinstance(data, dict):
                logger.warning(
                    "Yahoo RT: __NEXT_DATA__ 全体の型が想定と異なります keyword=%r type=%s", keyword, type(data)
                )
                return self.error("データ構造が想定と異なります（__NEXT_DATA__）")

            page = data.get("props", {}).get("pageProps", {}).get("pageData")
            if not isinstance(page, dict):
                logger.warning(
                    "Yahoo RT: pageData が見つかりません keyword=%r props keys=%s",
                    keyword, list(data.get("props", {}).keys()),
                )
                return self.error("データ構造が想定と異なります（pageData）")

            timeline = page.get("timeline")
            if not isinstance(timeline, dict):
                logger.warning(
                    "Yahoo RT: pageData の構造が想定と異なります keyword=%r pageData keys=%s",
                    keyword, list(page.keys()) if isinstance(page, dict) else type(page),
                )
                return self.error("データ構造が想定と異なります（timeline）")

            posts = timeline.get("entry")
            if posts is None:
                # 該当ゼロ件のときは entry が存在しない/nullになるケースがあるため、
                # これはエラーではなく「0件」として扱う。
                posts = []
            if not isinstance(posts, list):
                logger.warning(
                    "Yahoo RT: entry の型が想定と異なります keyword=%r type=%s value=%r",
                    keyword, type(posts), str(posts)[:300],
                )
                return self.error("データ構造が想定と異なります（entry）")

            settings = page.get("timelineSettings") or {}

            items = []
            for post in posts:
                if not isinstance(post, dict):
                    continue
                post_id = post.get("id", "")
                text = post.get("displayTextBody", "") or ""
                screen_name = post.get("screenName", "") or ""
                detail_url = post.get("detailUrl", "") or ""
                if detail_url and not detail_url.startswith(("http://", "https://")):
                    # Yahoo側のJSONは "/realtime/search/tweet/xxxx?..." のような
                    # 相対パスで返ってくることがあるため、絶対URLに補完する。
                    detail_url = "https://search.yahoo.co.jp" + (
                        detail_url if detail_url.startswith("/") else f"/{detail_url}"
                    )

                def _safe_int(v) -> int:
                    try:
                        return int(v)
                    except (TypeError, ValueError):
                        return 0

                likes = _safe_int(post.get("likesCount", 0))
                rt_count = _safe_int(post.get("rtCount", 0))
                created_at_raw = post.get("createdAt") or post.get("created_at")

                # 投稿日時を datetime に変換。
                # Yahoo側の実装変更により ISO8601文字列 / UNIXタイムスタンプ(秒 or ミリ秒)
                # のどちらで返ってきても対応できるようにする。
                pub_dt = None
                if isinstance(created_at_raw, str) and created_at_raw:
                    try:
                        pub_dt = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
                    except ValueError:
                        pub_dt = None
                elif isinstance(created_at_raw, (int, float)) and created_at_raw:
                    try:
                        ts = created_at_raw / 1000 if created_at_raw > 10**12 else created_at_raw
                        pub_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    except (ValueError, OSError, OverflowError):
                        pub_dt = None

                engagement = f"♥ {likes:,}  ⟲ {rt_count:,}"
                image_url, video_url = self._extract_media(post)
                # Yahoo! RTの検索ワードハイライト用タグ(\tSTART\t / \tEND\t)をclean_textの空白除去で損なわないよう保護
                text_marked = text.replace("\tSTART\t", "\x01").replace("\tEND\t", "\x02")
                cleaned_title = clean_text(text_marked, 280).replace("\x01", "\tSTART\t").replace("\x02", "\tEND\t")

                items.append({
                    "id": post_id,
                    "title": cleaned_title,
                    "url": detail_url,
                    "snippet": engagement,
                    "meta": f"@{screen_name}" if screen_name else "Yahoo RT",
                    "image": image_url,
                    "video": video_url,
                    "pub_date": pub_dt.isoformat() if pub_dt else "",
                    "_pub_dt": pub_dt,
                    "_post_id": post_id,
                })

            latest_id = settings.get("latestTweetId", "")
            summary = f"{len(items)}件の投稿を取得（latestId: {latest_id}）"
            return self.result(items=items, pages_fetched=1, summary=summary)

        except (KeyError, json.JSONDecodeError) as e:
            logger.warning("Yahoo RT: パース失敗 keyword=%r: %s", keyword, e)
            return self.error(f"データ解析に失敗しました: {e}")
        except requests.RequestException as e:
            logger.warning("Yahoo RT: 取得失敗 keyword=%r: %s", keyword, e)
            return self.error(str(e))
        except Exception as e:
            # ここに来る場合は KeyError/JSONDecodeError/RequestException 以外の
            # 想定外のエラー（Yahoo側のページ構造変更などが主な原因）。
            # 原因を切り分けやすくするため、例外の型・メッセージをそのまま返す。
            logger.exception("Yahoo RT: 予期しないエラー keyword=%r", keyword)
            return self.error(f"internal error: {type(e).__name__}: {e}")


# ============================================================
# 空売り情報スクレイパー（karauri.net）
# ============================================================
class KarauriError(Exception):
    """空売り情報の取得・解析に失敗したときの例外。status は API で返す HTTP ステータス。"""

    def __init__(self, message: str, status: int = 502):
        super().__init__(message)
        self.message = message
        self.status = status


class KarauriScraper:
    """
    karauri.net から銘柄ごとの空売り残高情報（大量保有している空売り者の一覧）を取得する。

    負荷とレート制限への配慮として2段階のキャッシュを持つ:
      1. TTL（既定1時間）以内は通信せずパース済みデータをそのまま返す
      2. TTL経過後に再取得してもHTMLのハッシュが前回と同じならパースを省略する
    ネットワークエラー時に古いキャッシュがあれば、それを stale=True 付きで返す。
    Flask は threaded=True で動くため、辞書操作はロックで保護し、
    同一銘柄の同時リクエストは1回の通信にまとめる。
    """

    URL_TEMPLATE = "https://karauri.net/{code}/"
    SHORT_HEADERS = ["計算日", "空売り者", "残高割合", "増減率", "残高数量", "増減量", "備考"]

    def __init__(self, cache_ttl_seconds: int = KARAURI_CACHE_TTL_SEC):
        self.cache_ttl = cache_ttl_seconds
        self._html_hashes: Dict[str, str] = {}
        self._cache: Dict[str, dict] = {}
        self._last_fetch_time: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._code_locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)

        self._session = requests.Session()
        self._session.trust_env = not KARAURI_BYPASS_PROXY
        self._session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "ja,en;q=0.8"})
        self._session.mount("https://", HTTPAdapter(max_retries=_retry_cfg))

    def _cached(self, code: str, *, ignore_ttl: bool = False) -> Optional[dict]:
        with self._lock:
            data = self._cache.get(code)
            fetched = self._last_fetch_time.get(code)
        if data is None or fetched is None:
            return None
        if not ignore_ttl and time.time() - fetched >= self.cache_ttl:
            return None
        return data

    def get_short_selling_data(self, code: str) -> dict:
        """指定した銘柄コードの空売り情報を返す。失敗時は KarauriError を送出する。"""
        fresh = self._cached(code)
        if fresh is not None:
            return dict(fresh, cached=True)

        with self._code_locks[code]:
            # 待っている間に他スレッドが取得を終えていれば、それを使う
            fresh = self._cached(code)
            if fresh is not None:
                return dict(fresh, cached=True)

            try:
                resp = self._session.get(
                    self.URL_TEMPLATE.format(code=code), timeout=DEFAULT_TIMEOUT
                )
                if resp.status_code == 404:
                    raise KarauriError(f"karauri.net に銘柄コード {code} のページがありません", 404)
                resp.raise_for_status()
            except KarauriError:
                raise
            except requests.RequestException as e:
                logger.info("karauri: 取得失敗 code=%s: %s", code, e)
                stale = self._cached(code, ignore_ttl=True)
                if stale is not None:
                    return dict(stale, cached=True, stale=True)
                raise KarauriError(f"karauri.net への接続に失敗しました: {e}", 502)

            html = resp.text
            current_hash = hashlib.md5(html.encode("utf-8")).hexdigest()

            with self._lock:
                unchanged = self._html_hashes.get(code) == current_hash and code in self._cache
                if unchanged:
                    # 更新なし: パースを省略し、TTL の起点だけ更新する
                    self._last_fetch_time[code] = time.time()
                    return dict(self._cache[code], cached=True)

            result = self._parse(html, code)
            with self._lock:
                self._html_hashes[code] = current_hash
                self._cache[code] = result
                self._last_fetch_time[code] = time.time()
            return dict(result, cached=False)

    def _parse(self, html: str, code: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")
        result = {
            "銘柄コード": code,
            "基本情報": {},
            "空売り状況": [],
            "source_url": self.URL_TEMPLATE.format(code=code),
            "fetched_at": now_iso(),
        }

        tables = soup.find_all("table")

        # 1. 基本情報（主市場, 業種, 証券コード/EDINET, 英名称）
        if len(tables) > 0:
            ths = [th.get_text(" ", strip=True) for th in tables[0].find_all("th")]
            tds = [td.get_text(" ", strip=True) for td in tables[0].find_all("td")]
            basic: Dict[str, str] = {}
            for th, td in zip(ths, tds):
                if "市場" in th:
                    basic["主市場"] = td
                elif "業種" in th:
                    basic["業種"] = td
                elif "証券コード" in th or "EDINET" in th:
                    basic["証券コード/EDINET"] = td
                elif "英名称" in th:
                    basic["英名称"] = td
                else:
                    basic[th] = td
            result["基本情報"] = basic

        # 2. 空売り状況（計算日, 空売り者, 残高割合, 増減率, 残高数量, 増減量, 備考）
        if len(tables) > 1:
            short_table = tables[1]
            headers_text = [th.get_text(" ", strip=True) for th in short_table.find_all("th")]
            for row in short_table.find_all("tr"):
                tds = row.find_all("td")
                if not tds:
                    continue
                row_data = {
                    headers_text[i]: td.get_text(" ", strip=True)
                    for i, td in enumerate(tds)
                    if i < len(headers_text)
                }
                result["空売り状況"].append(row_data)

        return result


shared_karauri_scraper = KarauriScraper()


def resolve_karauri_code(query: str) -> Optional[str]:
    """銘柄名・コード・'7203.T' 形式のいずれからでも東証4桁コードを求める。該当なしは None。"""
    q = unicodedata.normalize("NFKC", query or "").strip()
    if q.upper().endswith(".T"):
        q = q[:-2]
    code = stock_master.resolve_code(q).upper()
    if re.fullmatch(r"[0-9A-Z]{4}", code):
        return code
    return None


# ============================================================
# 株価プロバイダ
# ============================================================
class YahooStockProvider(BaseScraper):
    site_name = "Stock"
    icon = "¥"
    color = "#ffcc00"

    def __init__(self):
        self.info_cache: Dict[str, str] = {}
        # symbol -> (日付文字列, 前営業日終値)。fast_info.previous_close は銘柄によって
        # 直近値と食い違う既知の不具合があるため使わず、日足ヒストリーから算出したものを
        # 1日1回だけ再計算してキャッシュする。
        self._prev_close_cache: Dict[str, tuple] = {}

    def get_ticker(self, symbol: str) -> yf.Ticker:
        # yfinanceのTickerインスタンスを長期間使い回すと、内部キャッシュ（fast_infoなど）により
        # 前場終了時などに古い株価が残り続ける場合があるため、毎回インスタンスを生成する。
        # 通信の安定化のため、リトライ設定済みのグローバルsessionを渡す。
        return yf.Ticker(symbol, session=session)

    @staticmethod
    def _format_num(val) -> str:
        return f"{val:,.1f}" if isinstance(val, (int, float)) else str(val)

    @staticmethod
    def _format_signed_num(val: float) -> str:
        return f"+{val:,.1f}" if val > 0 else f"{val:,.1f}"

    @staticmethod
    def _format_pct(val: float) -> str:
        return f"+{val:.2f}%" if val > 0 else f"{val:.2f}%"

    @staticmethod
    def _fast_info_get(fi, *keys):
        """yfinanceのバージョン差異（snake_case/camelCase）を吸収して fast_info から値を取り出す。"""
        for k in keys:
            try:
                v = fi.get(k) if hasattr(fi, "get") else getattr(fi, k, None)
            except Exception:
                v = None
            if v is not None:
                return v
        return None

    def _get_prev_close(self, ticker_symbol: str, ticker: yf.Ticker) -> Optional[float]:
        """
        前営業日終値を取得する。fast_info の previous_close は当日の値や誤った値を
        返すことがあるため信用せず、日足ヒストリー（history(period="2d")）の
        「最後から2番目」の終値を正とする。市場が動いている間は変わらない値なので
        日付単位でキャッシュし、毎秒の history() 呼び出しを避ける。
        """
        today = datetime.now(_JST).strftime("%Y-%m-%d")
        cached = self._prev_close_cache.get(ticker_symbol)
        if cached and cached[0] == today:
            return cached[1]

        try:
            hist = ticker.history(period="2d")
            if hist is not None and not hist.empty:
                hist = hist[hist["Close"].notna()]
            if hist is None or hist.empty:
                return None

            if len(hist) >= 2:
                prev_close = float(hist.iloc[-2]["Close"])
            else:
                # 新規上場直後などで前営業日データが無い場合は、それ以前を辿れないので None
                return None

            self._prev_close_cache[ticker_symbol] = (today, prev_close)
            return prev_close
        except Exception:
            logger.warning("前日終値の取得に失敗 symbol=%r", ticker_symbol, exc_info=True)
            return None

    def fetch(self, query: str) -> dict:
        """
        毎秒呼ばれるティック取得。history()（日足取得）より軽量・高速な fast_info を
        現在値の取得に優先的に使うことで、UI側への反映をできるだけ遅延なく行う。
        ただし前日終値は fast_info を信用せず、日足ヒストリーから求めた値（日次キャッシュ）
        を使うことで前日比・前日比率の誤りを防ぐ。
        """
        try:
            ticker_symbol = to_ticker_symbol(query)
            ticker = self.get_ticker(ticker_symbol)

            price = None
            try:
                fi = ticker.fast_info
                fast_price = self._fast_info_get(fi, "last_price", "lastPrice")
                price = float(fast_price) if fast_price is not None else None
            except Exception:
                price = None

            prev_close = self._get_prev_close(ticker_symbol, ticker)

            if price is None or prev_close is None:
                hist = ticker.history(period="2d")
                if hist is not None and not hist.empty:
                    hist = hist[hist["Close"].notna()]
                if hist is None or hist.empty:
                    return self.empty(f"株価データが見つかりませんでした: {query}")
                if price is None:
                    price = float(hist.iloc[-1]["Close"])
                if prev_close is None and len(hist) >= 2:
                    prev_close = float(hist.iloc[-2]["Close"])
                    self._prev_close_cache[ticker_symbol] = (
                        datetime.now(_JST).strftime("%Y-%m-%d"),
                        prev_close,
                    )

            change = 0.0
            change_pct = 0.0
            if prev_close:
                change = price - prev_close
                change_pct = (change / prev_close) * 100

            if ticker_symbol not in self.info_cache:
                try:
                    self.info_cache[ticker_symbol] = ticker.info.get("shortName", query)
                except Exception:
                    self.info_cache[ticker_symbol] = query

            name = self.info_cache[ticker_symbol]

            return {
                "status": "ok",
                "name": name,
                "code": ticker_symbol,
                "price": self._format_num(price),
                "change": self._format_signed_num(change),
                "change_pct": self._format_pct(change_pct),
                "raw_change": change,
                "raw_price": price,
                "raw_prev_close": prev_close,
            }
        except Exception as e:
            logger.warning("株価取得に失敗 query=%r: %s", query, e)
            return self.error(str(e))

    def fetch_detail(self, query: str) -> dict:
        """
        始値・高値・安値・出来高・52週レンジ・時価総額などの拡張スナップショット。
        fast_info（軽量）と info（やや重い）の両方から値を補完し合う。
        毎秒ではなく STOCK_DETAIL_REFRESH_SEC 間隔で呼び出す想定。
        """
        try:
            ticker_symbol = to_ticker_symbol(query)
            ticker = self.get_ticker(ticker_symbol)

            fi = {}
            try:
                fi = dict(ticker.fast_info)
            except Exception:
                fi = {}

            info = {}
            try:
                info = ticker.info or {}
            except Exception:
                info = {}

            def pick(*keys):
                for k in keys:
                    if k in fi and fi[k] is not None:
                        return fi[k]
                    if k in info and info[k] is not None:
                        return info[k]
                return None

            return {
                "status": "ok",
                "code": ticker_symbol,
                "open": pick("open", "regularMarketOpen"),
                "day_high": pick("day_high", "dayHigh", "regularMarketDayHigh"),
                "day_low": pick("day_low", "dayLow", "regularMarketDayLow"),
                "prev_close": self._get_prev_close(ticker_symbol, ticker) or pick(
                    "previous_close", "previousClose", "regularMarketPreviousClose"
                ),
                "volume": pick("last_volume", "volume", "regularMarketVolume"),
                "year_high": pick("year_high", "fiftyTwoWeekHigh"),
                "year_low": pick("year_low", "fiftyTwoWeekLow"),
                "market_cap": pick("market_cap", "marketCap"),
                "currency": pick("currency"),
            }
        except Exception as e:
            logger.info("詳細情報の取得に失敗 query=%r: %s", query, e)
            return {"status": "error", "message": str(e)}


# 単発クエリ（/api/quote, /api/history）用に共有インスタンスを使い回し、
# ティッカー/銘柄名キャッシュを無駄に再生成しない
shared_stock_provider = YahooStockProvider()

# ============================================================
# イベントブローカー（SSE配信）
# ============================================================
_last_news_cache: Dict[str, dict] = {}
_last_yahoo_realtime_cache: Dict[str, dict] = {}


class EventManager:
    # 「詰まっている」とみなして自動的に購読解除するまでの連続フル回数
    MAX_CONSECUTIVE_FULL = 3

    def __init__(self):
        self.clients: Dict[str, List[queue.Queue]] = defaultdict(list)
        self.lock = threading.Lock()
        self._full_streak: Dict[int, int] = {}  # id(queue) -> 連続で満杯だった回数

    def subscribe(self, query: str, q: queue.Queue) -> None:
        with self.lock:
            self.clients[query].append(q)
            self._full_streak[id(q)] = 0

    def unsubscribe(self, query: str, q: queue.Queue) -> None:
        with self.lock:
            if query in self.clients and q in self.clients[query]:
                self.clients[query].remove(q)
                if not self.clients[query]:
                    del self.clients[query]
            self._full_streak.pop(id(q), None)

    def has_subscribers(self, query: str) -> bool:
        with self.lock:
            return len(self.clients.get(query, [])) > 0

    def broadcast(self, query: str, event_type: str, data: dict) -> None:
        """
        購読者のキューへ配信する。ブラウザ側の消費が追いつかずキューが満杯の場合は、
        単に新しいデータを捨てるのではなく「一番古いメッセージを間引いて最新のものを
        優先」する。それでも詰まり続けるクライアントは応答なしとみなし、自動的に
        購読解除してリソースリーク・ログスパムを防ぐ。
        """
        msg = f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
        with self.lock:
            subscribers = list(self.clients.get(query, []))

        dead: List[queue.Queue] = []
        for q in subscribers:
            try:
                q.put_nowait(msg)
                self._full_streak[id(q)] = 0
                continue
            except queue.Full:
                pass

            # 満杯だった場合: 最も古いメッセージを1つ間引いてから最新分の投入を試みる
            try:
                q.get_nowait()
                q.put_nowait(msg)
                self._full_streak[id(q)] = 0
            except (queue.Empty, queue.Full):
                streak = self._full_streak.get(id(q), 0) + 1
                self._full_streak[id(q)] = streak
                if streak >= self.MAX_CONSECUTIVE_FULL:
                    dead.append(q)
                else:
                    logger.warning(
                        "SSEクライアントの消費が追いついていません query=%r streak=%d", query, streak
                    )

        if dead:
            with self.lock:
                for q in dead:
                    if query in self.clients and q in self.clients[query]:
                        self.clients[query].remove(q)
                    self._full_streak.pop(id(q), None)
                if query in self.clients and not self.clients[query]:
                    del self.clients[query]
            logger.info("応答のないSSEクライアントを自動切断しました query=%r count=%d", query, len(dead))


event_manager = EventManager()


# ============================================================
# 監視スレッド管理（アイドルクエリの自動停止でスレッドリークを防止）
# ============================================================
class MonitorRegistry:
    """
    各クエリにつき「株価監視」「ニュース監視」「Yahoo!リアルタイム監視」の
    3スレッドを起動する。
    誰も購読していない状態が IDLE_MONITOR_TTL_SEC を超えたスレッドは自ら終了し、
    その旨をレジストリへ報告する。全スレッドが終了して初めてクエリを「未起動」に戻す。
    """

    THREADS_PER_QUERY = 3

    def __init__(self):
        self._alive_threads: Dict[str, int] = defaultdict(int)
        self.lock = threading.Lock()

    def ensure_monitors(self, query: str) -> None:
        with self.lock:
            if self._alive_threads.get(query, 0) > 0:
                return
            self._alive_threads[query] = self.THREADS_PER_QUERY
            threading.Thread(
                target=self._run_stock_monitor, args=(query,), daemon=True, name=f"stock-monitor:{query}"
            ).start()
            threading.Thread(
                target=self._run_news_monitor, args=(query,), daemon=True, name=f"news-monitor:{query}"
            ).start()
            threading.Thread(
                target=self._run_yahoo_realtime_monitor, args=(query,), daemon=True,
                name=f"yahoo-rt-monitor:{query}"
            ).start()
            logger.info("監視スレッドを起動しました query=%r", query)

    def _mark_stopped(self, query: str) -> None:
        with self.lock:
            if query in self._alive_threads:
                self._alive_threads[query] -= 1
                if self._alive_threads[query] <= 0:
                    del self._alive_threads[query]
                    logger.info("query=%r の監視スレッドが全て停止しました（アイドル）", query)

    def _run_stock_monitor(self, query: str) -> None:
        try:
            stock_monitor_loop(query)
        finally:
            self._mark_stopped(query)

    def _run_news_monitor(self, query: str) -> None:
        try:
            news_monitor_loop(query)
        finally:
            self._mark_stopped(query)

    def _run_yahoo_realtime_monitor(self, query: str) -> None:
        try:
            yahoo_realtime_monitor_loop(query)
        finally:
            self._mark_stopped(query)


registry = MonitorRegistry()


def _wait_while_idle(query: str) -> bool:
    """
    購読者が現れるまで待機する。IDLE_MONITOR_TTL_SEC 秒を超えて誰も現れなければ
    False を返し、呼び出し元のループを終了させる。
    """
    idle_since = time.time()
    while not event_manager.has_subscribers(query):
        if time.time() - idle_since > IDLE_MONITOR_TTL_SEC:
            return False
        time.sleep(IDLE_SLEEP_SEC)
    return True


def stock_monitor_loop(query: str) -> None:
    provider = YahooStockProvider()
    last_status = None
    last_detail_ts = 0.0

    while True:
        if not _wait_while_idle(query):
            logger.info("株価監視をアイドルタイムアウトで終了 query=%r", query)
            return

        status = get_market_status()
        if status != last_status:
            event_manager.broadcast(query, "market_status", {"status": status})
            last_status = status

        try:
            data = provider.fetch(query)
            if data.get("status") == "ok":
                event_manager.broadcast(query, "stock", data)
        except Exception:
            logger.exception("株価監視ループでエラー query=%r", query)

        # 始値/高値/安値/出来高/52週レンジ/時価総額は変動が緩やかなため、
        # 毎秒ではなく STOCK_DETAIL_REFRESH_SEC 間隔で別イベントとして送る
        now = time.time()
        if now - last_detail_ts >= STOCK_DETAIL_REFRESH_SEC:
            try:
                detail = provider.fetch_detail(query)
                if detail.get("status") == "ok":
                    event_manager.broadcast(query, "stock_detail", detail)
            except Exception:
                logger.exception("株価詳細取得でエラー query=%r", query)
            last_detail_ts = now

        time.sleep(STOCK_POLL_INTERVAL_SEC)


def news_monitor_loop(query: str) -> None:
    scraper = NewsAggregator()
    seen_urls: Set[str] = set()
    last_fetch_time: Optional[datetime] = None

    while True:
        if not _wait_while_idle(query):
            logger.info("ニュース監視をアイドルタイムアウトで終了 query=%r", query)
            return

        try:
            now_utc = datetime.now(timezone.utc)
            data = scraper.fetch(query)
            if data.get("status") == "ok":
                cutoff = last_fetch_time or (now_utc - timedelta(hours=NEWS_LOOKBACK_HOURS))

                new_items = []
                for item in data.get("items", []):
                    url = item.get("url", "")
                    pub_dt: Optional[datetime] = item.pop("_pub_dt", None)

                    if not url or url in seen_urls:
                        seen_urls.add(url)
                        continue
                    if pub_dt is not None and pub_dt <= cutoff:
                        seen_urls.add(url)
                        continue

                    seen_urls.add(url)
                    new_items.append(item)

                last_fetch_time = now_utc

                if new_items:
                    send_data = dict(data)
                    send_data["items"] = new_items
                    send_data["total_items"] = len(new_items)
                    send_data["summary"] = f"{len(new_items)}件の新着ニュース"
                    _last_news_cache[query] = send_data
                    event_manager.broadcast(query, "result", send_data)
                    event_manager.broadcast(query, "log", {"message": f"{len(new_items)}件の新着記事を取得しました"})
        except Exception:
            logger.exception("ニュース監視ループでエラー query=%r", query)

        time.sleep(NEWS_POLL_INTERVAL_SEC)



def yahoo_realtime_monitor_loop(query: str) -> None:
    """
    Yahoo! リアルタイム検索をポーリングし、キーワードに関する投稿を継続的に配信する。

    5秒ごとにポーリングし、取得できたスナップショットを毎回配信する
    （新着が無いポーリングでも、いいね数・リポスト数などの変動を反映するため
    最新のスナップショットを送り続ける。「新着0件だから配信しない」とはしない）。
    新着投稿があった場合のみ、ログにその件数を通知する。
    """
    scraper = YahooRealtimeScraper()
    seen_ids: Set[str] = set()

    while True:
        if not _wait_while_idle(query):
            logger.info("Yahoo!RT監視をアイドルタイムアウトで終了 query=%r", query)
            return

        try:
            data = scraper.fetch(query)

            if data.get("status") == "ok":
                items = data.get("items", [])
                new_count = 0

                for item in items:
                    # 内部処理用の一時キーはフロントエンドに送らないので取り除く
                    item.pop("_pub_dt", None)
                    post_id = item.pop("_post_id", None) or item.get("id") or item.get("url", "")

                    if post_id and post_id not in seen_ids:
                        new_count += 1
                    if post_id:
                        seen_ids.add(post_id)

                if len(seen_ids) > 2000:  # メモリ肥大化防止
                    seen_ids.clear()

                # 新着の有無にかかわらず、毎ポーリングごとに最新スナップショットを配信する。
                # これによりフロントエンドは常に最新のいいね数・投稿一覧に更新され続ける。
                send_data = dict(data)
                send_data["items"] = items
                send_data["total_items"] = len(items)
                send_data["new_count"] = new_count
                send_data["summary"] = (
                    f"{len(items)}件の投稿（うち新着{new_count}件・Yahoo! RT）"
                )
                _last_yahoo_realtime_cache[query] = send_data
                event_manager.broadcast(query, "yahoo_realtime", send_data)

                if new_count > 0:
                    event_manager.broadcast(query, "log", {
                        "message": f"Yahoo!RTで{new_count}件の新着投稿を検知しました"
                    })
            elif data.get("status") == "error":
                event_manager.broadcast(query, "log", {
                    "message": f"Yahoo!RT取得エラー: {data.get('summary', '')}"
                })
        except Exception:
            logger.exception("Yahoo!RT監視ループでエラー query=%r", query)

        time.sleep(YAHOO_REALTIME_POLL_INTERVAL_SEC)


def market_pulse_loop() -> None:
    """
    ユーザーが何も検索していなくても常時流れる市場全体のヘッドライン。
    実際のBloomberg端末の下部ニュースクロールに相当する。
    起動時から常時稼働し、全クライアントへブロードキャストする。
    """
    aggregator = NewsAggregator()
    seen_urls: Set[str] = set()

    while True:
        try:
            data = aggregator.fetch(MARKET_PULSE_QUERY)
            if data.get("status") == "ok":
                new_items = []
                now_utc = datetime.now(timezone.utc)
                cutoff = now_utc - timedelta(hours=24)
                for item in data.get("items", []):
                    pub_dt = item.pop("_pub_dt", None)
                    if pub_dt is not None and pub_dt <= cutoff:
                        continue
                    key = _normalize_url(item.get("url", ""))
                    if key and key not in seen_urls:
                        seen_urls.add(key)
                        new_items.append(item)

                if len(seen_urls) > 800:  # メモリ肥大化を防ぐため定期的にクリア
                    seen_urls.clear()

                if new_items:
                    send_data = dict(data)
                    send_data["items"] = new_items[:12]
                    send_data["total_items"] = len(send_data["items"])
                    send_data["summary"] = f"{len(send_data['items'])}件の市場ヘッドライン"
                    event_manager.broadcast(GLOBAL_CHANNEL, "pulse", send_data)
        except Exception:
            logger.exception("マーケットパルス取得でエラー")

        time.sleep(MARKET_PULSE_REFRESH_SEC)


threading.Thread(target=market_pulse_loop, daemon=True, name="market-pulse").start()


# ============================================================
# Flask ルーティング
# ============================================================
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/suggest")
def api_suggest():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify([])
    return jsonify(stock_master.search(query, limit=20))


@app.route("/api/quote")
def api_quote():
    """ウォッチリスト追加時などに使う単発の株価取得（SSEなし）。"""
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"status": "error", "message": "query is empty"}), 400
    data = shared_stock_provider.fetch(query)
    return jsonify(data)


def _format_earnings_dt(idx, fmt: str = "%Y-%m-%d %H:%M") -> str:
    """
    yfinance の get_earnings_dates() が返すタイムスタンプは米国東部時間
    （America/New_York）で入っていることが多く、そのまま表示すると日本時間との
    時差（標準時で14時間、夏時間で13時間）分ズレて見える。日本時間(JST)に変換
    してからフォーマットする。tzinfoが無い場合は米国東部時間とみなして
    localizeしてから変換する。
    """
    if not hasattr(idx, "strftime"):
        return str(idx)
    try:
        ts = idx
        if getattr(ts, "tzinfo", None) is None:
            ts = ts.tz_localize("America/New_York")
        ts_jst = ts.tz_convert(_JST)+timedelta(minutes=30)
        return ts_jst.strftime(fmt)
    except Exception:
        return idx.strftime(fmt)


def _safe_float(val) -> Optional[float]:
    try:
        f = float(val)
        if f != f:  # NaN チェック（NaN は自分自身と等しくない）
            return None
        return f
    except (TypeError, ValueError):
        return None


_earnings_cache = {}
EARNINGS_CACHE_TTL = 86400  # 24 hours


@app.route("/api/earnings")
def api_earnings():
    """
    決算カレンダー用: 指定した複数銘柄（ウォッチリスト等）の次回決算発表予定日をまとめて返す。
    yfinance の get_earnings_dates() に依存しており、銘柄・取引所によっては
    データが提供されない場合がある（その場合は空リストを返す）。
    """
    symbols_param = request.args.get("symbols", "").strip()
    if not symbols_param:
        return jsonify({"status": "error", "message": "symbols is empty"}), 400

    queries = [s.strip() for s in symbols_param.split(",") if s.strip()][:30]
    results = []

    def _fetch_one(q: str) -> dict:
        try:
            ticker_symbol = to_ticker_symbol(q)
            
            now_ts = time.time()
            if ticker_symbol in _earnings_cache:
                cached_ts, cached_data = _earnings_cache[ticker_symbol]
                if now_ts - cached_ts < EARNINGS_CACHE_TTL:
                    ret = dict(cached_data)
                    ret["query"] = q
                    return ret

            ticker = shared_stock_provider.get_ticker(ticker_symbol)
            name = shared_stock_provider.info_cache.get(ticker_symbol)
            if not name:
                try:
                    name = ticker.info.get("shortName", q)
                    shared_stock_provider.info_cache[ticker_symbol] = name
                except Exception:
                    name = q

            earnings = []
            try:
                edf = ticker.get_earnings_dates(limit=6)
                if edf is not None and not edf.empty:
                    for idx, row in edf.iterrows():
                        date_str = _format_earnings_dt(idx, "%Y-%m-%d %H:%M")
                        earnings.append({
                            "date": date_str,
                            "eps_estimate": _safe_float(row.get("EPS Estimate")),
                            "reported_eps": _safe_float(row.get("Reported EPS")),
                            "surprise_pct": _safe_float(row.get("Surprise(%)")),
                        })
            except Exception:
                logger.info("決算日程が取得できませんでした symbol=%r", ticker_symbol)

            result_data = {"query": q, "code": ticker_symbol, "name": name, "earnings": earnings}
            _earnings_cache[ticker_symbol] = (now_ts, result_data)
            return result_data
        except Exception as e:
            logger.warning("決算情報の取得に失敗 query=%r: %s", q, e)
            return {"query": q, "code": q, "name": q, "earnings": [], "error": str(e)}

    # 一次タイムアウト: キャッシュ済みの銘柄はほぼ即座に終わるが、新規追加銘柄など
    # キャッシュが無いものは info / get_earnings_dates の実通信が発生し時間がかかる。
    EARNINGS_FETCH_TIMEOUT = DEFAULT_TIMEOUT + 10
    # 二次タイムアウト: 一次タイムアウト時点でまだ実行中のタスクに与える追加の猶予。
    # with ブロックを抜ける際にはどのみち全タスクの完了を待つ（ThreadPoolExecutorの仕様）ため、
    # ここで待つこと自体に追加コストは無く、結果を確実に拾い切ることが目的。
    EARNINGS_FETCH_EXTRA_WAIT = 20

    with ThreadPoolExecutor(max_workers=max(4, len(queries))) as pool:
        futures = {pool.submit(_fetch_one, q): q for q in queries}
        pending = set(futures.keys())

        def _collect(iterable):
            for fut in iterable:
                pending.discard(fut)
                try:
                    results.append(fut.result())
                except Exception:
                    logger.exception("決算情報タスクが失敗 query=%r", futures.get(fut))

        try:
            _collect(as_completed(futures, timeout=EARNINGS_FETCH_TIMEOUT))
        except FuturesTimeoutError:
            # ここで打ち切って全体を失敗にすると、既にキャッシュ済みの銘柄まで
            # 巻き込んでカレンダー全体が失敗表示になってしまうため、未完了分にのみ
            # 追加の猶予を与えて改めて回収を試みる（＝取りこぼしを防ぐ）。
            if pending:
                try:
                    _collect(as_completed(list(pending), timeout=EARNINGS_FETCH_EXTRA_WAIT))
                except FuturesTimeoutError:
                    pass
            if pending:
                logger.warning(
                    "決算情報の取得が最終的にタイムアウトしました（未完了 %d件、"
                    "次回リクエストではキャッシュ経由で返ります）: %r",
                    len(pending), [futures[f] for f in pending],
                )

    return jsonify({"status": "ok", "items": results})


@app.route("/api/history")
def api_history():
    """
    ウォッチリストのスパークライン、および銘柄チャートパネル向けの値動きを返す。
    interval を指定することで 1分足～月足まで切り替えられる（Yahoo側の取得可能期間の
    制約に合わせて period を自動選択する）。
    """
    query = request.args.get("q", "").strip()
    interval = request.args.get("interval", HISTORY_INTERVAL_DEFAULT).strip()
    if not query:
        return jsonify({"status": "error", "message": "query is empty"}), 400
    if interval not in CHART_INTERVAL_PERIOD_MAP:
        return jsonify({
            "status": "error",
            "message": f"invalid interval: {interval}",
            "allowed": list(CHART_INTERVAL_PERIOD_MAP.keys()),
        }), 400

    try:
        ticker_symbol = to_ticker_symbol(query)
        ticker = shared_stock_provider.get_ticker(ticker_symbol)
        period = CHART_INTERVAL_PERIOD_MAP[interval]

        hist = ticker.history(period=period, interval=interval)
        if hist.empty:
            # 銘柄・時間帯によっては指定足のデータが無いため、日足にフォールバック
            hist = ticker.history(period="1y", interval="1d")
            interval = "1d"

        is_intraday = interval not in ("1d", "1wk", "1mo")
        points = []
        for idx, row in hist.iterrows():
            if pd.isna(row.get("Close")):
                continue
            points.append({
                "t": idx.strftime("%Y-%m-%d %H:%M") if is_intraday else idx.strftime("%Y-%m-%d"),
                "o": round(float(row["Open"]), 2) if pd.notna(row.get("Open")) else None,
                "h": round(float(row["High"]), 2) if pd.notna(row.get("High")) else None,
                "l": round(float(row["Low"]), 2) if pd.notna(row.get("Low")) else None,
                "p": round(float(row["Close"]), 2),  # 'p' = close（従来のスパークライン互換用）
                "v": int(row["Volume"]) if pd.notna(row.get("Volume")) else None,
            })

        # チャート描画が重くなりすぎないよう、直近の一定本数に制限
        if len(points) > MAX_CHART_POINTS:
            points = points[-MAX_CHART_POINTS:]

        return jsonify({"status": "ok", "code": ticker_symbol, "interval": interval, "points": points})
    except Exception as e:
        logger.warning("履歴取得に失敗 query=%r interval=%r: %s", query, interval, e)
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# GRAND CROSS
# ------------------------------------------------------------
# v2: 単純な条件充足数の合算(0/1判定)だった従来のスコアリングに対し、
#   1) ADXによる「トレンドの質」フィルタ（レンジ相場でのダマシを減点）
#   2) 出来高条件の連続性チェック（単日スパイクではなく2〜3日の継続を評価）
#   3) RSI弱気ダイバージェンス検知（高値更新中でも失速していれば警告）
#   4) 週足トレンドとの整合性確認（日足だけのノイズを抑制）
#   5) 直近スコア推移の返却（フロント側でモメンタムの向きを可視化）
# を追加し、「本当に使える」シグナルに近づける。
#
# さらに、クロス"成立後"にしか反応できない上記指標を補うため、
# calculate_grand_cross_precursor() で「クロスが成立する前」の予兆
# （移動平均線の収束速度、MACDの収束、出来高の積み上がり、
#   ボリンジャーバンドのスクイーズ、RSIの底上げ）を検知し、
# 「あと何営業日程度でクロスしそうか」を推定する早期警戒システムを提供する。
# ============================================================
GRAND_CROSS_CACHE_TTL = 300
_grand_cross_cache: Dict[str, tuple] = {}
_grand_cross_precursor_cache: Dict[str, tuple] = {}


def _linreg_slope(series: pd.Series, window: int = 5) -> Optional[float]:
    """直近 window 本を単回帰し、1本あたりの平均変化量（傾き）を返す。
    ノイズの多い日次差分よりも、複数日の趨勢を安定して捉えるために使う。
    """
    tail = series.dropna().tail(window)
    if len(tail) < max(3, window - 1):
        return None
    y = tail.values
    x = list(range(len(y)))
    try:
        slope = float(np.polyfit(x, y, 1)[0])
    except Exception:
        return None
    if not np.isfinite(slope):
        return None
    return slope


def _compute_adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14):
    """ADX（平均方向性指数）と +DI/-DI を計算する。
    ADXは値の大小そのものではなく「トレンドの強さ・有無」を測る指標で、
    レンジ相場（トレンドなし）で発生したゴールデンクロス等の"ダマシ"を
    判別するために使う（プロのテクニカル分析でも定番のフィルタ）。
    """
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()

    safe_atr = atr.replace(0, np.nan)
    plus_di = 100 * (plus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / safe_atr)
    minus_di = 100 * (minus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / safe_atr)
    di_sum = (plus_di + minus_di).replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / di_sum
    adx = dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    return adx, plus_di, minus_di


def _detect_rsi_divergence(close: pd.Series, rsi: pd.Series, lookback: int = 20) -> bool:
    """弱気ダイバージェンス（価格は直近高値を更新しているのに RSI は
    それに追随できていない状態）を簡易検出する。
    スコア上は好条件が揃っていても、内実は勢いが失速している
    "ダマシ"の兆候をユーザーに警告するための補助シグナル。
    """
    if len(close) < lookback + 2:
        return False
    window_close = close.iloc[-(lookback + 1):-1]
    window_rsi = rsi.iloc[-(lookback + 1):-1]
    if window_close.dropna().empty or window_rsi.dropna().empty:
        return False
    prior_high_idx = window_close.idxmax()
    prior_high_val = window_close.loc[prior_high_idx]
    prior_high_rsi = window_rsi.loc[prior_high_idx]

    cur_close = close.iloc[-1]
    cur_rsi = rsi.iloc[-1]
    if pd.isna(prior_high_val) or pd.isna(prior_high_rsi) or pd.isna(cur_close) or pd.isna(cur_rsi):
        return False

    return bool(cur_close > prior_high_val and cur_rsi < prior_high_rsi)


def _weekly_trend_aligned(hist: pd.DataFrame) -> Optional[bool]:
    """週足に変換し、週足終値が週足10週MAより上にあるかで長期トレンドとの
    整合性を確認する。日足だけで判定するとダマシが多いクロスに対する
    追加の裏付けとして使う（十分なデータがない場合は None）。
    """
    try:
        weekly = hist["Close"].resample("W").last().dropna()
        if len(weekly) < 12:
            return None
        w_ma10 = weekly.rolling(window=10).mean()
        if pd.isna(w_ma10.iloc[-1]):
            return None
        return bool(weekly.iloc[-1] > w_ma10.iloc[-1])
    except Exception:
        return None


def calculate_grand_cross(hist: pd.DataFrame) -> dict:
    if len(hist) < 75:
        return {"status": "error", "message": "DATA INSUFFICIENT"}

    try:
        close = hist["Close"]
        high = hist["High"]
        low = hist["Low"]
        volume = hist["Volume"]

        # 1. TREND (25)
        ma5 = close.rolling(window=5).mean()
        ma25 = close.rolling(window=25).mean()
        ma75 = close.rolling(window=75).mean()

        # 2. MOMENTUM (20)
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()

        # 3. RSI (15)
        delta = close.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        ema_up = up.ewm(com=13, adjust=False).mean()
        ema_down = down.ewm(com=13, adjust=False).mean()
        rs = ema_up / ema_down
        rsi = 100 - (100 / (1 + rs))

        # 4. VOLUME (20)
        vol20 = volume.rolling(window=20).mean()

        # 5. トレンド品質フィルタ (ADX) ※新規
        adx, plus_di, minus_di = _compute_adx(high, low, close)

        def get_row_score(idx_offset):
            if len(hist) < abs(idx_offset) + 20:
                return 0, {}

            c_v = close.iloc[idx_offset]
            h_v = high.iloc[idx_offset]
            l_v = low.iloc[idx_offset]
            vol_v = volume.iloc[idx_offset]

            ma5_v = ma5.iloc[idx_offset]
            ma25_v = ma25.iloc[idx_offset]
            ma75_v = ma75.iloc[idx_offset]

            p5_idx = idx_offset - 5
            p5_ma75 = ma75.iloc[p5_idx] if p5_idx >= -len(ma75) else ma75_v

            macd_v = macd.iloc[idx_offset]
            sig_v = signal.iloc[idx_offset]
            rsi_v = rsi.iloc[idx_offset]
            v20_v = vol20.iloc[idx_offset]

            # 過去20日の高値 (前日まで)
            h20_slice = high.iloc[idx_offset-20 : idx_offset] if idx_offset < -1 else high.iloc[-21:-1]
            h20_v = h20_slice.max() if not h20_slice.empty else h_v

            # 直近の重要高値 (過去5日)
            h5_slice = high.iloc[idx_offset-5 : idx_offset] if idx_offset < -1 else high.iloc[-6:-1]
            h5_v = h5_slice.max() if not h5_slice.empty else h_v

            # 出来高の"継続性"（新規）: 直近3日のうち何日が20日平均を上回っているか。
            # 単日の出来高スパイクだけでは仕手化・ノイズと区別できないため、
            # 継続日数を見ることでより信頼度の高い"資金流入"を判定する。
            vol3_start = idx_offset - 2
            if vol3_start >= -len(volume):
                vol3_slice = volume.iloc[vol3_start: idx_offset + 1 if idx_offset != -1 else None]
            else:
                vol3_slice = volume.iloc[-3:]
            v20_3_slice = vol20.iloc[vol3_start: idx_offset + 1 if idx_offset != -1 else None] if vol3_start >= -len(vol20) else vol20.iloc[-3:]
            persistent_days = 0
            try:
                persistent_days = int((vol3_slice.values > v20_3_slice.values).sum())
            except Exception:
                persistent_days = 0

            score = 0
            conds = {
                "ma_trend": False, "golden_cross": False, "macd": False, "macd_zero": False,
                "rsi": False, "volume": False, "breakout": False, "volume_persistent": False,
            }

            # TREND (25)
            if pd.notna(ma5_v) and pd.notna(ma25_v) and ma5_v > ma25_v:
                score += 10
                conds["golden_cross"] = True
            if pd.notna(ma25_v) and pd.notna(ma75_v) and ma25_v > ma75_v:
                score += 10
            if pd.notna(ma75_v) and pd.notna(p5_ma75) and ma75_v > p5_ma75:
                score += 5
                conds["ma_trend"] = True

            # MOMENTUM (20)
            if pd.notna(macd_v) and pd.notna(sig_v) and macd_v > sig_v:
                score += 10
                conds["macd"] = True
            if pd.notna(macd_v) and macd_v > 0:
                score += 10
                conds["macd_zero"] = True

            # RSI (15)
            if pd.notna(rsi_v):
                if rsi_v > 50:
                    score += 10
                    conds["rsi"] = True
                if rsi_v < 70:
                    score += 5

            # VOLUME (20) ― 単日条件 + 継続性ボーナス
            if pd.notna(vol_v) and pd.notna(v20_v) and v20_v > 0:
                if vol_v > v20_v:
                    score += 5
                    conds["volume"] = True
                if vol_v > v20_v * 1.5:
                    score += 5
                if persistent_days >= 2:
                    score += 5
                    conds["volume_persistent"] = True

            # BREAKOUT (20)
            if pd.notna(c_v) and pd.notna(h20_v) and c_v > h20_v:
                score += 10
                conds["breakout"] = True
            if pd.notna(c_v) and pd.notna(h5_v) and c_v > h5_v:
                score += 5

            if pd.notna(h_v) and pd.notna(l_v) and pd.notna(c_v) and h_v > l_v:
                if (c_v - l_v) / (h_v - l_v) >= 0.8:
                    score += 5

            return score, conds

        raw_score, conds = get_row_score(-1)
        prev_raw_score, _ = get_row_score(-2)

        # --- トレンド品質フィルタ（ADX） ---
        # ADX < 20: レンジ相場（トレンドなし）とみなし、シグナルの信頼度を下げる。
        # ADX >= 25: 明確なトレンドがあり、クロス系シグナルの信頼度が高い。
        adx_v = adx.iloc[-1] if pd.notna(adx.iloc[-1]) else None
        if adx_v is None:
            trend_quality = "UNKNOWN"
            confidence = 0.85
        elif adx_v >= 25:
            trend_quality = "STRONG"
            confidence = 1.0
        elif adx_v >= 20:
            trend_quality = "MODERATE"
            confidence = 0.9
        else:
            trend_quality = "WEAK"
            confidence = 0.65

        # --- 弱気ダイバージェンス警告 ---
        divergence = _detect_rsi_divergence(close, rsi)
        conds["divergence_warning"] = divergence

        # --- 週足トレンド整合性ボーナス ---
        weekly_aligned = _weekly_trend_aligned(hist)
        weekly_bonus = 5 if weekly_aligned else 0

        adjusted = raw_score * confidence + weekly_bonus
        if divergence:
            adjusted -= 10  # 失速の兆候があれば減点（過信を防ぐ）
        adjusted = max(0, min(100, round(adjusted)))

        prev_adjusted = max(0, min(100, round(prev_raw_score * confidence)))

        if adjusted >= 90:
            sig_text = "GRAND CROSS"
        elif adjusted >= 75:
            sig_text = "STRONG BUY"
        elif adjusted >= 60:
            sig_text = "WATCH"
        elif adjusted >= 40:
            sig_text = "WAIT"
        else:
            sig_text = "WEAK"

        is_new = (adjusted >= 90 and prev_adjusted < 90)

        latest_date = hist.iloc[-1].name
        if hasattr(latest_date, 'strftime'):
            detected_at = latest_date.strftime("%Y-%m-%d")
        else:
            detected_at = str(latest_date)

        # 安全な数値変換
        def _f(val):
            return float(val) if pd.notna(val) else None

        vol_ratio = 0
        if pd.notna(volume.iloc[-1]) and pd.notna(vol20.iloc[-1]) and vol20.iloc[-1] > 0:
            vol_ratio = float(volume.iloc[-1] / vol20.iloc[-1])

        # 直近10日分のスコア推移（フロントでモメンタムの向きを表示するため）
        score_history = []
        max_lookback = min(10, len(hist) - 20)
        for i in range(max_lookback, 0, -1):
            s, _c = get_row_score(-i)
            score_history.append(round(s * confidence))
        score_history.append(adjusted)

        warnings = []
        if divergence:
            warnings.append("弱気ダイバージェンス: 価格は高値更新中だがRSIが追随せず失速の兆候")
        if trend_quality == "WEAK":
            warnings.append("ADX低水準: 明確なトレンドが確認できずレンジ相場の可能性（ダマシに注意）")

        details = {
            "ma5": _f(ma5.iloc[-1]),
            "ma25": _f(ma25.iloc[-1]),
            "ma75": _f(ma75.iloc[-1]),
            "macd": _f(macd.iloc[-1]),
            "signal": _f(signal.iloc[-1]),
            "rsi": _f(rsi.iloc[-1]),
            "adx": _f(adx_v) if adx_v is not None else None,
            "volume_ratio": round(vol_ratio, 2),
            "breakout": conds["breakout"],
        }

        return {
            "status": "ok",
            "score": adjusted,
            "raw_score": raw_score,
            "signal": sig_text,
            "is_new": is_new,
            "conditions": conds,
            "details": details,
            "detected_at": detected_at,
            "trend_quality": trend_quality,
            "confidence": round(confidence, 2),
            "weekly_aligned": weekly_aligned,
            "score_history": score_history,
            "warnings": warnings,
        }
    except Exception as e:
        logger.exception("GRAND CROSS計算エラー")
        return {"status": "error", "message": str(e)}


# ------------------------------------------------------------
# GRAND CROSS 予兆検知（クロス成立"前"の早期警戒システム）
# ------------------------------------------------------------
# 上の calculate_grand_cross() はクロスが"成立した後"にしか反応できない
# （ma5>ma25 等は結果でしかない）。実運用では「あと数日でクロスしそうだ」
# という予兆を事前に掴みたいというニーズがあるため、
#   ・移動平均線の乖離幅の収束速度から交差までの推定営業日数を算出
#   ・MACDとシグナルの収束
#   ・出来高の積み上がり（仕込みの兆候）
#   ・RSIの底上げ（下から50へ向かうモメンタム）
#   ・ボリンジャーバンドのスクイーズ（値動き収縮＝放れの前兆）
# を統合した「予兆スコア」を新設する。
PRECURSOR_CACHE_TTL = 300


def calculate_grand_cross_precursor(hist: pd.DataFrame) -> dict:
    if len(hist) < 80:
        return {"status": "error", "message": "DATA INSUFFICIENT"}

    try:
        close = hist["Close"]
        volume = hist["Volume"]

        ma5 = close.rolling(window=5).mean()
        ma25 = close.rolling(window=25).mean()
        ma75 = close.rolling(window=75).mean()

        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()

        delta = close.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        ema_up = up.ewm(com=13, adjust=False).mean()
        ema_down = down.ewm(com=13, adjust=False).mean()
        rs = ema_up / ema_down
        rsi = 100 - (100 / (1 + rs))

        vol20 = volume.rolling(window=20).mean()
        vol5 = volume.rolling(window=5).mean()

        bb_mid = close.rolling(window=20).mean()
        bb_std = close.rolling(window=20).std()
        bb_width = ((bb_mid + 2 * bb_std) - (bb_mid - 2 * bb_std)) / bb_mid * 100

        last_close = close.iloc[-1]
        if pd.isna(last_close) or last_close == 0:
            return {"status": "error", "message": "PRICE UNAVAILABLE"}

        def _eta(gap_series: pd.Series, window: int = 5) -> Optional[int]:
            """gap_series（短期MA - 長期MA 等）が 0（=クロス）へ到達するまでの
            推定営業日数。gapが負（未達）かつ収束方向(velocity>0)の時のみ算出。
            収束が止まっている/発散している場合は None（見込みなし）を返す。
            """
            gap_now = gap_series.dropna().iloc[-1] if not gap_series.dropna().empty else None
            velocity = _linreg_slope(gap_series, window=window)
            if gap_now is None or velocity is None:
                return None
            if gap_now >= 0:
                return 0  # 既に交差済み
            if velocity <= 0:
                return None  # 収束していない（むしろ広がっている）
            days = -gap_now / velocity
            if days <= 0 or days > 60:
                return None
            return int(round(days))

        # 1) 短期/中期MA (5/25) の収束状況 — 価格に対する%乖離で正規化
        gap_5_25 = (ma5 - ma25) / close * 100
        gap_5_25_now = gap_5_25.iloc[-1]
        vel_5_25 = _linreg_slope(gap_5_25, window=5)
        eta_5_25 = _eta(gap_5_25, window=5)

        # 2) 中期/長期MA (25/75) の収束状況
        gap_25_75 = (ma25 - ma75) / close * 100
        gap_25_75_now = gap_25_75.iloc[-1]
        vel_25_75 = _linreg_slope(gap_25_75, window=5)
        eta_25_75 = _eta(gap_25_75, window=5)

        # 3) MACD と シグナルの収束
        gap_macd = macd - signal
        gap_macd_now = gap_macd.iloc[-1]
        vel_macd = _linreg_slope(gap_macd, window=5)
        eta_macd = _eta(gap_macd, window=5)

        # 4) 出来高の積み上がり（直近5日平均が20日平均に対して上昇傾向か）
        vol_ratio_series = (vol5 / vol20.replace(0, np.nan))
        vol_ratio_now = vol_ratio_series.iloc[-1]
        vol_ratio_slope = _linreg_slope(vol_ratio_series, window=5)
        volume_buildup = bool(
            pd.notna(vol_ratio_now) and vol_ratio_now > 1.0
            and vol_ratio_slope is not None and vol_ratio_slope > 0
        )

        # 5) RSIの底上げ（50未満から上昇トレンド）
        rsi_now = rsi.iloc[-1]
        rsi_slope = _linreg_slope(rsi, window=3)
        rsi_building = bool(
            pd.notna(rsi_now) and rsi_now < 50
            and rsi_slope is not None and rsi_slope > 0
        )

        # 6) ボリンジャーバンドのスクイーズ（過去60日のバンド幅の下位20%以内）
        bb_hist = bb_width.dropna().tail(60)
        bb_squeeze = False
        bb_percentile = None
        if len(bb_hist) >= 20 and pd.notna(bb_width.iloc[-1]):
            bb_percentile = float((bb_hist < bb_width.iloc[-1]).mean() * 100)
            bb_squeeze = bb_percentile <= 20

        def _convergence_score(gap_now, velocity, max_pts, tolerance_pct=3.0):
            """gapがゼロに近く、かつプラス方向（収束）の速度が大きいほど高得点。
            すでに交差済み(gap_now>=0)の場合は対象外として0点。
            """
            if gap_now is None or pd.isna(gap_now) or gap_now >= 0:
                return 0.0
            closeness = max(0.0, 1 - min(abs(gap_now), tolerance_pct) / tolerance_pct)
            momentum = 0.0
            if velocity is not None and velocity > 0:
                momentum = min(1.0, velocity / (tolerance_pct / 5))
            return max_pts * (0.5 * closeness + 0.5 * momentum)

        score_5_25 = _convergence_score(gap_5_25_now, vel_5_25, 30)
        score_25_75 = _convergence_score(gap_25_75_now, vel_25_75, 20)
        score_macd = _convergence_score(gap_macd_now, vel_macd, 20, tolerance_pct=max(abs(gap_macd_now) if pd.notna(gap_macd_now) else 1, 1))
        score_volume = 15.0 if volume_buildup else (7.0 if pd.notna(vol_ratio_now) and vol_ratio_now > 1.0 else 0.0)
        score_rsi = 10.0 if rsi_building else 0.0
        score_bb = 5.0 if bb_squeeze else 0.0

        precursor_score = round(score_5_25 + score_25_75 + score_macd + score_volume + score_rsi + score_bb)
        precursor_score = max(0, min(100, precursor_score))

        if precursor_score >= 70:
            classification = "IMMINENT"
            label = "🔺 交差直前"
        elif precursor_score >= 45:
            classification = "APPROACHING"
            label = "🔸 接近中"
        elif precursor_score >= 20:
            classification = "WATCHING"
            label = "・兆候あり"
        else:
            classification = "NONE"
            label = "--"

        already_crossed = bool(pd.notna(gap_5_25_now) and gap_5_25_now >= 0)

        narrative_parts = []
        if already_crossed:
            narrative_parts.append("短期線(5)は既に中期線(25)を上抜けています。")
        elif eta_5_25 is not None:
            narrative_parts.append(f"短期線(5)が中期線(25)に接近中。このペースだと約{eta_5_25}営業日でゴールデンクロスの可能性。")
        elif pd.notna(gap_5_25_now) and gap_5_25_now < 0:
            narrative_parts.append("短期線(5)は中期線(25)の下にあり、収束の兆候はまだ見られません。")

        if eta_25_75 is not None:
            narrative_parts.append(f"中期線(25)と長期線(75)も約{eta_25_75}営業日で交差する可能性があります。")

        if volume_buildup:
            narrative_parts.append("出来高が平均を上回るペースで積み上がっており、資金流入の兆候があります。")

        if bb_squeeze:
            narrative_parts.append("ボリンジャーバンドが収縮しており、大きな値動き（放れ）が近い可能性があります。")

        if not narrative_parts:
            narrative_parts.append("現時点で明確な予兆は検出されていません。")

        return {
            "status": "ok",
            "precursor_score": precursor_score,
            "classification": classification,
            "label": label,
            "already_crossed": already_crossed,
            "eta_business_days": {
                "ma5_25": eta_5_25,
                "ma25_75": eta_25_75,
                "macd_signal": eta_macd,
            },
            "components": {
                "ma5_25": {"gap_pct": _round_or_none(gap_5_25_now), "velocity": _round_or_none(vel_5_25), "score": round(score_5_25, 1)},
                "ma25_75": {"gap_pct": _round_or_none(gap_25_75_now), "velocity": _round_or_none(vel_25_75), "score": round(score_25_75, 1)},
                "macd_signal": {"gap": _round_or_none(gap_macd_now), "velocity": _round_or_none(vel_macd), "score": round(score_macd, 1)},
                "volume_buildup": {"active": volume_buildup, "ratio": _round_or_none(vol_ratio_now), "score": score_volume},
                "rsi_building": {"active": rsi_building, "rsi": _round_or_none(rsi_now), "score": score_rsi},
                "bb_squeeze": {"active": bb_squeeze, "percentile": _round_or_none(bb_percentile), "score": score_bb},
            },
            "narrative": " ".join(narrative_parts),
        }
    except Exception as e:
        logger.exception("GRAND CROSS予兆計算エラー")
        return {"status": "error", "message": str(e)}


def _round_or_none(val, digits: int = 3):
    if val is None or (isinstance(val, float) and (pd.isna(val) or not np.isfinite(val))):
        return None
    return round(float(val), digits)


def _fetch_gc_history(query: str):
    """グランドクロス系エンドポイントで共通利用する日足履歴取得処理。"""
    ticker_symbol = to_ticker_symbol(query)
    ticker = shared_stock_provider.get_ticker(ticker_symbol)
    # 少なくとも150日分（約半年）取得してMA75やMACDを安定させる
    hist = ticker.history(period="1y", interval="1d")
    return ticker_symbol, hist


@app.route("/api/grand-cross")
def api_grand_cross():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"status": "error", "message": "query is empty"}), 400

    try:
        now_ts = time.time()
        # キャッシュキーは正規化前のクエリではなく銘柄コードにしたいが、
        # コード解決自体にコストがあるため、まず簡易キャッシュを試みる。
        cached_by_query = _grand_cross_cache.get(query)
        if cached_by_query and now_ts - cached_by_query[0] < GRAND_CROSS_CACHE_TTL:
            return jsonify(cached_by_query[1])

        ticker_symbol, hist = _fetch_gc_history(query)

        cached = _grand_cross_cache.get(ticker_symbol)
        if cached and now_ts - cached[0] < GRAND_CROSS_CACHE_TTL:
            _grand_cross_cache[query] = cached
            return jsonify(cached[1])

        if hist.empty:
            payload = {"status": "error", "message": f"データが見つかりませんでした: {query}"}
            return jsonify(payload)

        result = calculate_grand_cross(hist)
        if result.get("status") == "ok":
            result["symbol"] = ticker_symbol

        _grand_cross_cache[ticker_symbol] = (now_ts, result)
        _grand_cross_cache[query] = (now_ts, result)
        return jsonify(result)
    except Exception as e:
        logger.warning("GRAND CROSS取得失敗 query=%r: %s", query, e)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/grand-cross-precursor")
def api_grand_cross_precursor():
    """グランドクロス"成立前"の予兆（早期警戒）を返すエンドポイント。"""
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"status": "error", "message": "query is empty"}), 400

    try:
        now_ts = time.time()
        cached_by_query = _grand_cross_precursor_cache.get(query)
        if cached_by_query and now_ts - cached_by_query[0] < PRECURSOR_CACHE_TTL:
            return jsonify(cached_by_query[1])

        ticker_symbol, hist = _fetch_gc_history(query)

        cached = _grand_cross_precursor_cache.get(ticker_symbol)
        if cached and now_ts - cached[0] < PRECURSOR_CACHE_TTL:
            _grand_cross_precursor_cache[query] = cached
            return jsonify(cached[1])

        if hist.empty:
            payload = {"status": "error", "message": f"データが見つかりませんでした: {query}"}
            return jsonify(payload)

        result = calculate_grand_cross_precursor(hist)
        if result.get("status") == "ok":
            result["symbol"] = ticker_symbol

        _grand_cross_precursor_cache[ticker_symbol] = (now_ts, result)
        _grand_cross_precursor_cache[query] = (now_ts, result)
        return jsonify(result)
    except Exception as e:
        logger.warning("GRAND CROSS予兆取得失敗 query=%r: %s", query, e)
        return jsonify({"status": "error", "message": str(e)}), 500




# ============================================================
# 注文・約定分析（出来高ベースの簡易近似）
#
# 注意: 証券会社が提供する「超大口／大口／中口／小口」等の注文サイズ別の
# 約定内訳は、個々の注文明細に基づく専有データであり、yfinance を含む
# 公開データからは取得できない。そのため実際の注文サイズ別データは
# 再現せず、代わりに「陽線=買い優勢の出来高、陰線=売り優勢の出来高」
# とみなす簡易な tick-rule 方式で、値動きから買い/売り優勢を近似する。
# あくまで参考値であり、実際の注文フローとは異なる点をUI側にも明記する。
# ============================================================
ORDER_FLOW_CACHE_TTL = 20  # 秒。頻繁な再計算を避けるための短期キャッシュ
_order_flow_cache: Dict[str, tuple] = {}


@app.route("/api/order-flow")
def api_order_flow():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"status": "error", "message": "query is empty"}), 400

    try:
        ticker_symbol = to_ticker_symbol(query)

        now_ts = time.time()
        cached = _order_flow_cache.get(ticker_symbol)
        if cached and now_ts - cached[0] < ORDER_FLOW_CACHE_TTL:
            return jsonify(cached[1])

        ticker = shared_stock_provider.get_ticker(ticker_symbol)

        hist = None
        used_interval = None
        for interval, period in (("1m", "1d"), ("5m", "5d"), ("1d", "1mo")):
            try:
                h = ticker.history(period=period, interval=interval)
            except Exception:
                h = None
            if h is not None and not h.empty:
                hist = h
                used_interval = interval
                break

        if hist is None or hist.empty:
            payload = {"status": "empty", "message": f"出来高データが見つかりませんでした: {query}"}
            return jsonify(payload)

        hist = hist[hist["Volume"].notna() & hist["Close"].notna() & hist["Open"].notna()]

        buy_value = 0.0
        sell_value = 0.0
        buy_volume = 0
        sell_volume = 0
        prev_close = None
        for _, row in hist.iterrows():
            o, c, v = float(row["Open"]), float(row["Close"]), float(row["Volume"])
            if v <= 0:
                continue
            value = c * v
            if c > o:
                is_buy = True
            elif c < o:
                is_buy = False
            elif prev_close is not None:
                is_buy = c >= prev_close
            else:
                is_buy = True
            if is_buy:
                buy_value += value
                buy_volume += int(v)
            else:
                sell_value += value
                sell_volume += int(v)
            prev_close = c

        total_value = buy_value + sell_value
        if total_value <= 0:
            payload = {"status": "empty", "message": f"出来高データが見つかりませんでした: {query}"}
            return jsonify(payload)

        buy_oku = buy_value / 1e8
        sell_oku = sell_value / 1e8
        net_oku = buy_oku - sell_oku
        buy_pct = buy_value / total_value * 100
        sell_pct = sell_value / total_value * 100

        name = shared_stock_provider.info_cache.get(ticker_symbol)
        if not name:
            try:
                name = ticker.info.get("shortName", query)
                shared_stock_provider.info_cache[ticker_symbol] = name
            except Exception:
                name = query

        payload = {
            "status": "ok",
            "code": ticker_symbol,
            "name": name,
            "interval": used_interval,
            "is_approximation": True,
            "dominance": "buy" if net_oku >= 0 else "sell",
            "net_oku": round(net_oku, 2),
            "buy_oku": round(buy_oku, 2),
            "sell_oku": round(sell_oku, 2),
            "buy_pct": round(buy_pct, 2),
            "sell_pct": round(sell_pct, 2),
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
        }
        _order_flow_cache[ticker_symbol] = (now_ts, payload)
        return jsonify(payload)
    except Exception as e:
        logger.warning("約定分析(出来高近似)の取得に失敗 query=%r: %s", query, e)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/calendar")
def api_calendar():
    """
    決算予定日・配当落ち日などのカレンダー情報を返す。
    銘柄・地域によっては yfinance 側にデータが無いこともあるため、
    取得できない項目は null を返し、フロント側で「--」表示にフォールバックする。
    """
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"status": "error", "message": "query is empty"}), 400

    try:
        ticker_symbol = to_ticker_symbol(query)
        ticker = shared_stock_provider.get_ticker(ticker_symbol)

        earnings_date = None
        try:
            edf = ticker.get_earnings_dates(limit=8)
            if edf is not None and not edf.empty:
                now_ts = pd.Timestamp.now(tz=edf.index.tz) if edf.index.tz else pd.Timestamp.now()
                future = edf[edf.index >= now_ts]
                target_idx = future.index[0] if len(future) > 0 else edf.index[0]
                earnings_date = _format_earnings_dt(target_idx, "%Y-%m-%d")
        except Exception:
            earnings_date = None

        dividend_date = None
        try:
            cal = ticker.calendar
            if isinstance(cal, dict):
                ed = cal.get("Ex-Dividend Date")
                if isinstance(ed, (list, tuple)) and ed:
                    dividend_date = str(ed[0])
                elif ed:
                    dividend_date = str(ed)
            elif cal is not None and not cal.empty and "Ex-Dividend Date" in getattr(cal, "index", []):
                dividend_date = str(cal.loc["Ex-Dividend Date"].iloc[0])
        except Exception:
            dividend_date = None

        name = shared_stock_provider.info_cache.get(ticker_symbol)
        if not name:
            try:
                name = ticker.info.get("shortName", query)
                shared_stock_provider.info_cache[ticker_symbol] = name
            except Exception:
                name = query

        return jsonify({
            "status": "ok",
            "code": ticker_symbol,
            "name": name,
            "earnings_date": earnings_date,
            "dividend_date": dividend_date,
        })
    except Exception as e:
        logger.warning("カレンダー取得に失敗 query=%r: %s", query, e)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/karauri")
def api_karauri():
    """
    空売り情報（karauri.net）を返す。code には銘柄コード・銘柄名のどちらも指定できる。
    失敗時は {"error": "..."} を返す（フロント側の loadKarauri がこの形式を参照する）。
    """
    raw = (request.args.get("code") or request.args.get("q") or "").strip()
    if not raw:
        return jsonify({"error": "code is empty"}), 400

    code = resolve_karauri_code(raw)
    if not code:
        return jsonify({"error": f"空売り情報は東証の銘柄コード（4桁）のみ対象です: {raw}"}), 404

    try:
        data = shared_karauri_scraper.get_short_selling_data(code)
        return jsonify(data)
    except KarauriError as e:
        return jsonify({"error": e.message, "code": code}), e.status
    except Exception as e:
        logger.exception("空売り情報の取得で予期しないエラー query=%r", raw)
        return jsonify({"error": f"internal error: {type(e).__name__}: {e}"}), 500


@app.route("/market-pulse-stream")
def market_pulse_stream():
    """検索クエリに依存しない、常時稼働の市場ヘッドラインSSE。"""

    def generate():
        q: queue.Queue = queue.Queue(maxsize=50)
        event_manager.subscribe(GLOBAL_CHANNEL, q)
        yield f"event: start\ndata: {json.dumps({'channel': 'market_pulse'})}\n\n"
        try:
            while True:
                try:
                    msg = q.get(timeout=SSE_HEARTBEAT_SEC)
                    yield msg
                except queue.Empty:
                    yield f"event: heartbeat\ndata: {json.dumps({'timestamp': now_iso()})}\n\n"
        except GeneratorExit:
            pass
        finally:
            event_manager.unsubscribe(GLOBAL_CHANNEL, q)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


@app.route("/search-stream")
def search_stream():
    query = request.args.get("q", "").strip()

    def generate():
        if not query:
            yield f"event: error\ndata: {json.dumps({'message': 'query is empty'})}\n\n"
            return

        yield f"event: start\ndata: {json.dumps({'query': query})}\n\n"
        yield ":" + " " * 2048 + "\n\n"  # プロキシのバッファリング回避

        q: queue.Queue = queue.Queue(maxsize=100)
        event_manager.subscribe(query, q)
        registry.ensure_monitors(query)

        yield f"event: log\ndata: {json.dumps({'message': f'Subscribed to real-time events for {query}'})}\n\n"

        # 既存の監視スレッドは STOCK_DETAIL_REFRESH_SEC 周期でしか詳細情報を配信しないため、
        # 再訪問・銘柄切り替え直後は最大で周期分の待ち時間が発生してしまう。
        # それを避けるため、購読直後に一度だけ同期的にスナップショットを取得して即座に送る。
        try:
            quick_quote = shared_stock_provider.fetch(query)
            if quick_quote.get("status") == "ok":
                yield f"event: stock\ndata: {json.dumps(quick_quote, ensure_ascii=False)}\n\n"
            quick_detail = shared_stock_provider.fetch_detail(query)
            if quick_detail.get("status") == "ok":
                yield f"event: stock_detail\ndata: {json.dumps(quick_detail, ensure_ascii=False)}\n\n"
        except Exception:
            logger.exception("初回スナップショット取得でエラー query=%r", query)

        if query in _last_news_cache:
            cached = _last_news_cache[query]
            yield f"event: result\ndata: {json.dumps(cached, ensure_ascii=False)}\n\n"

        if query in _last_yahoo_realtime_cache:
            cached_yahoo = _last_yahoo_realtime_cache[query]
            yield f"event: yahoo_realtime\ndata: {json.dumps(cached_yahoo, ensure_ascii=False)}\n\n"

        try:
            while True:
                try:
                    msg = q.get(timeout=SSE_HEARTBEAT_SEC)
                    yield msg
                except queue.Empty:
                    yield f"event: heartbeat\ndata: {json.dumps({'timestamp': now_iso()})}\n\n"
        except GeneratorExit:
            pass
        finally:
            event_manager.unsubscribe(query, q)
            logger.info("クライアントが切断しました query=%r", query)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


# ============================================================
# AI ニュース要約 / 材料判定 (ローカル llama-server, port 8000)
# ============================================================
LOCAL_LLM_URL = os.environ.get("LOCAL_LLM_URL", "http://localhost:8000/v1/chat/completions")
LOCAL_LLM_MODEL = os.environ.get("LOCAL_LLM_MODEL", "local-model")
LOCAL_LLM_TIMEOUT = float(os.environ.get("LOCAL_LLM_TIMEOUT", "60"))

_AI_SYSTEM_PROMPT = (
    "あなたは日本の株式市場に詳しい金融アナリストです。"
    "与えられたニュース記事を読み、以下のフォーマットで日本語で回答してください。\n\n"
    "【要約】\n（記事の要点を2〜3文で要約）\n\n"
    "【材料判定】\n（「好材料」「悪材料」「中立」のいずれか1つだけを記載）\n\n"
    "【判定理由】\n（判定した理由を1〜2文で説明）\n\n"
    "暴落の可能性を感じたら、悪材料。上昇の可能性を感じたら好材料。変化がないと思えば中立。"
)


@app.route("/api/ai-summary", methods=["POST"])
def api_ai_summary():
    """
    ローカル llama-server (OpenAI互換 /v1/chat/completions) を呼び出し、
    ニュース記事の要約と好材料/悪材料の判定を返す。

    Request JSON:
        { "title": "記事タイトル", "body": "記事本文（省略可）" }

    Response JSON:
        {
            "status": "ok",
            "summary": "【要約】...\n【材料判定】...\n【判定理由】...",
            "sentiment": "good" | "bad" | "neutral"
        }
    """
    try:
        body = request.get_json(force=True, silent=True) or {}
        title = (body.get("title") or "").strip()
        text = (body.get("body") or "").strip()

        if not title:
            return jsonify({"status": "error", "message": "title is required"}), 400

        user_content = f"タイトル: {title}"
        if text:
            # 本文が長すぎると LLM のコンテキスト制限に引っかかるため 600 字に切る
            user_content += f"\n\n本文:\n{text[:600]}"

        payload = {
            "model": LOCAL_LLM_MODEL,
            "messages": [
                {"role": "system", "content": _AI_SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            "temperature": 0.3,
            "max_tokens": 400,
        }

        resp = requests.post(
            LOCAL_LLM_URL,
            json=payload,
            timeout=LOCAL_LLM_TIMEOUT,
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()

        answer = data["choices"][0]["message"]["content"].strip()

        # 材料判定をキーワードで分類（フロントエンドで色分けに使う）
        sentiment = "neutral"
        if "好材料" in answer:
            sentiment = "good"
        elif "悪材料" in answer:
            sentiment = "bad"

        return jsonify({"status": "ok", "summary": answer, "sentiment": sentiment})

    except requests.exceptions.ConnectionError:
        logger.warning("AI summary: llama-server への接続に失敗しました (port 8000)")
        return jsonify({"status": "error", "message": "AI サーバー (port 8000) に接続できません。llama-server が起動しているか確認してください。"}), 503
    except requests.exceptions.Timeout:
        logger.warning("AI summary: llama-server がタイムアウトしました")
        return jsonify({"status": "error", "message": "AI サーバーの応答がタイムアウトしました。"}), 504
    except Exception as e:
        logger.exception("AI summary エラー")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5050"))
    debug = os.environ.get("FLASK_DEBUG", "1") == "1"
    logger.info("起動: port=%d debug=%s", port, debug)
    app.run(debug=debug, port=port, threaded=True)
