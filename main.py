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
import json
import logging
import re
import time
import threading
import queue
import os
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus
from collections import defaultdict
from typing import Dict, List, Optional, Set

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

# X (旧Twitter) 連携。
# 2023年のAPI改定以降、X の投稿を正規に取得するには公式 API v2（有料プラン）の
# Bearer Token が必須。無断スクレイピングはX社の利用規約に反するため実装しない。
# 環境変数 TWITTER_BEARER_TOKEN が未設定の場合、この機能は自動的に無効化され、
# フロントエンドにはその旨だけが通知される（アプリ全体には影響しない）。
TWITTER_BEARER_TOKEN = os.environ.get("TWITTER_BEARER_TOKEN", "").strip()
TWITTER_API_BASE = os.environ.get("TWITTER_API_BASE", "https://api.twitter.com/2")
TWEET_POLL_INTERVAL_SEC = float(os.environ.get("TWEET_POLL_INTERVAL_SEC", "30"))
TWEET_MAX_RESULTS = int(os.environ.get("TWEET_MAX_RESULTS", "20"))  # X API 仕様上 10〜100
TWEET_LOOKBACK_HOURS = 24

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
        ].head(limit)
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
# X (旧Twitter) 検索プロバイダ
# ============================================================
class TwitterScraper(BaseScraper):
    """
    X API v2 の「最近の検索」エンドポイントを使ってキーワード関連ポストを取得する。
    公式APIのみを利用し、認証情報が無い場合は明示的に status="unavailable" を返す
    （非公式スクレイピングは規約違反のため実装しない）。
    """

    site_name = "X (Twitter)"
    icon = "𝕏"
    color = "#1d9bf0"
    SEARCH_ENDPOINT = f"{TWITTER_API_BASE}/tweets/search/recent"

    def __init__(self):
        self._warned_no_token = False

    def fetch(self, query: str) -> dict:
        if not TWITTER_BEARER_TOKEN:
            if not self._warned_no_token:
                logger.warning(
                    "TWITTER_BEARER_TOKEN が未設定のため X(旧Twitter)検索は無効化されています"
                )
                self._warned_no_token = True
            return self.result(
                status="unavailable",
                summary="X検索は未設定です（環境変数 TWITTER_BEARER_TOKEN に公式APIのBearer Tokenを設定してください）",
            )

        try:
            params = {
                "query": f"{query} -is:retweet",
                "max_results": max(10, min(100, TWEET_MAX_RESULTS)),
                "tweet.fields": "created_at,public_metrics,author_id,lang",
                "expansions": "author_id",
                "user.fields": "username,name,profile_image_url,verified",
            }
            headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
            r = session.get(self.SEARCH_ENDPOINT, headers=headers, params=params, timeout=DEFAULT_TIMEOUT)

            if r.status_code == 401:
                return self.error("X APIの認証に失敗しました（TWITTER_BEARER_TOKENを確認してください）")
            if r.status_code == 429:
                return self.error("X APIのレート制限に達しました。次回のポーリングで再試行します")
            if r.status_code != 200:
                return self.error(f"X API HTTP {r.status_code}")

            payload = r.json()
            tweets = payload.get("data", [])
            if not tweets:
                return self.empty("該当するポストが見つかりませんでした")

            users_by_id = {u["id"]: u for u in payload.get("includes", {}).get("users", [])}

            items = []
            for t in tweets:
                author = users_by_id.get(t.get("author_id"), {})
                metrics = t.get("public_metrics", {}) or {}
                username = author.get("username", "")
                tweet_id = t.get("id", "")
                url = f"https://x.com/{username}/status/{tweet_id}" if username else f"https://x.com/i/web/status/{tweet_id}"
                engagement = (
                    f"♥ {metrics.get('like_count', 0):,}  "
                    f"⟲ {metrics.get('retweet_count', 0):,}  "
                    f"💬 {metrics.get('reply_count', 0):,}"
                )
                items.append({
                    "title": clean_text(t.get("text", ""), 280),
                    "url": url,
                    "snippet": engagement,
                    "meta": f"@{username}" if username else "X",
                    "image": author.get("profile_image_url"),
                    "pub_date": t.get("created_at", ""),
                })

            items.sort(key=lambda x: x.get("pub_date") or "", reverse=True)
            return self.result(items=items, pages_fetched=1, summary=f"{len(items)}件のポストを取得")
        except requests.RequestException as e:
            logger.warning("X検索取得に失敗 query=%r: %s", query, e)
            return self.error(str(e))
        except Exception:
            logger.exception("X検索取得中に予期しないエラー query=%r", query)
            return self.error("internal error")


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
_last_tweet_cache: Dict[str, dict] = {}


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
    各クエリにつき「株価監視」「ニュース監視」「X(旧Twitter)監視」の3スレッドを起動する。
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
                target=self._run_tweet_monitor, args=(query,), daemon=True, name=f"tweet-monitor:{query}"
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

    def _run_tweet_monitor(self, query: str) -> None:
        try:
            tweet_monitor_loop(query)
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


def _parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except ValueError:
        return None


def tweet_monitor_loop(query: str) -> None:
    """
    X API v2 の「最近の検索」をポーリングし、キーワードに関する新着ポストのみを
    差分配信する（ニュース監視と同じ「既知URLは無視・カットオフより古い投稿は無視」方式）。
    TWITTER_BEARER_TOKEN が未設定の場合は一度だけ unavailable を通知し、
    無駄なポーリングをしないよう待機間隔を延ばす。
    """
    scraper = TwitterScraper()
    seen_urls: Set[str] = set()
    last_fetch_time: Optional[datetime] = None
    unavailable_notified = False

    while True:
        if not _wait_while_idle(query):
            logger.info("X監視をアイドルタイムアウトで終了 query=%r", query)
            return

        try:
            data = scraper.fetch(query)

            if data.get("status") == "unavailable":
                if not unavailable_notified:
                    event_manager.broadcast(query, "tweet", data)
                    unavailable_notified = True
                time.sleep(max(TWEET_POLL_INTERVAL_SEC * 4, 60))
                continue

            if data.get("status") == "ok":
                now_utc = datetime.now(timezone.utc)
                cutoff = last_fetch_time or (now_utc - timedelta(hours=TWEET_LOOKBACK_HOURS))

                new_items = []
                for item in data.get("items", []):
                    url = item.get("url", "")
                    pub_dt = _parse_iso(item.get("pub_date"))

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
                    send_data["summary"] = f"{len(new_items)}件の新着ポスト"
                    _last_tweet_cache[query] = send_data
                    event_manager.broadcast(query, "tweet", send_data)
                    event_manager.broadcast(query, "log", {"message": f"Xで{len(new_items)}件の新着ポストを検知しました"})
            elif data.get("status") == "error":
                event_manager.broadcast(query, "log", {"message": f"X検索エラー: {data.get('summary', '')}"})
        except Exception:
            logger.exception("X監視ループでエラー query=%r", query)

        time.sleep(TWEET_POLL_INTERVAL_SEC)


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
                        date_str = idx.strftime("%Y-%m-%d %H:%M") if hasattr(idx, "strftime") else str(idx)
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

    with ThreadPoolExecutor(max_workers=max(4, len(queries))) as pool:
        futures = [pool.submit(_fetch_one, q) for q in queries]
        for fut in as_completed(futures, timeout=DEFAULT_TIMEOUT + 10):
            try:
                results.append(fut.result())
            except Exception:
                logger.exception("決算情報タスクが失敗")

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
                earnings_date = target_idx.strftime("%Y-%m-%d")
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

        if query in _last_tweet_cache:
            cached_tweet = _last_tweet_cache[query]
            yield f"event: tweet\ndata: {json.dumps(cached_tweet, ensure_ascii=False)}\n\n"

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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5050"))
    debug = os.environ.get("FLASK_DEBUG", "1") == "1"
    logger.info("起動: port=%d debug=%s", port, debug)
    app.run(debug=debug, port=port, threaded=True)