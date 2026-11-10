"""生活插件：让她有自己的日子。

每天一次调用，产出「今天想做 + 时间线 + 刷到了什么」，之后全天不再调用模型。
再低频地把这段生活注入到对话的 system prompt 里，让她聊天时也有自己的事。

设计要点：
- timeline 只记「她一个人干了什么」，不记她与用户的对话（避免自我引用）。
- 所有素材来自真实抓取（抖音热榜 / B站 / 网易云），模型只能按编号引用。
- 幂等 + 重试：同一天只生成一次；失败按 2 / 5 / 15 分钟退避。
- 晚上 01:30 把当天发生的事压成一两句写进日记素材。
"""

import asyncio
import json
import re
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import httpx

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain
from astrbot.api.provider import ProviderRequest
from astrbot.api.star import Context, Star, register

PLUGIN_NAME = "astrbot_plugin_life_engine"
DATA_DIR = Path(__file__).resolve().parents[2]
PLUGIN_DIR = DATA_DIR / "plugin_data" / "life_engine"
LIFE_PATH = PLUGIN_DIR / "life.json"
STATE_PATH = PLUGIN_DIR / "state.json"
PROFILE_PATH = PLUGIN_DIR / "profile.json"
LIBRARY_PATH = PLUGIN_DIR / "library.json"
DIARY_DB = DATA_DIR / "plugin_data" / "memory_diary" / "memory.db"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
HEADERS = {"User-Agent": UA, "Referer": "https://www.bilibili.com/"}
RETRY_BACKOFF = [120, 300, 900]

DEFAULTS = {
    "enabled": True,
    "generate_hour": 8,
    "generate_minute": 30,
    "wake_time": "08:30",
    "sleep_time": "00:30",
    "events_min": 4,
    "events_max": 6,
    "keep_days": 7,
    "retry_times": 3,
    "umo": "",
    "inject_mode": "low",
    "inject_gap_minutes": 30,
    "write_to_diary": True,
    "diary_at": "01:30",
    "diary_sender": "助手",
    "persona": "",
    "search_enrich": True,
    "search_api_key": "",
    "recent_days": 3,
    "discover_enabled": True,
    "discover_per_day": 2,
    "cooldown_days": 60,
}

DEFAULT_PROFILE = {
    "identity": "18 岁，学生，宅，话不多",
    "platforms": ["B站", "抖音", "网易云"],
    "games": ["王者荣耀", "原神", "Steam 独立游戏", "3A 单机"],
    "anime": ["日番", "国创"],
    "preferences": {
        "anime": ["悬疑推理", "日常治愈", "科幻"],
        "games": ["roguelike", "独立解谜", "模拟经营"],
        "avoid": ["后宫", "恐怖", "竞技"],
    },
    "topics": ["猫", "生活区vlog", "美食探店", "番剧吐槽", "游戏攻略", "独立游戏新作"],
    "habits": ["熬夜", "赖床", "半夜饿了找吃的"],
    "avoid": ["出门玩", "和网友私信互动", "线下活动", "见任何人"],
}


# ---------------- 工具 ----------------


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today() -> str:
    return date.today().isoformat()


def _clean(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text or "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _short(text: str, limit: int = 60) -> str:
    text = _clean(text)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _hm(value: str, fallback: str) -> tuple:
    try:
        hour, minute = str(value).split(":", 1)
        return max(0, min(int(hour), 23)), max(0, min(int(minute), 59))
    except Exception:
        hour, minute = str(fallback).split(":", 1)
        return int(hour), int(minute)


def _minutes_of_day(hour: int, minute: int) -> int:
    return hour * 60 + minute


class LifeEngine(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        self._task = None
        self._lock = asyncio.Lock()
        self._last_inject: dict = {}
        try:
            self._task = asyncio.create_task(self._loop())
        except RuntimeError:
            pass

    # ---------------- 配置 / 数据 ----------------

    def _config_path(self) -> Path:
        return DATA_DIR / "config" / f"{PLUGIN_NAME}_config.json"

    def _cfg(self) -> dict:
        cfg = dict(DEFAULTS)
        data = _load_json(self._config_path(), {})
        if isinstance(data, dict):
            cfg.update(data)
        injected = getattr(self, "config", None)
        if isinstance(injected, dict):
            for key, value in injected.items():
                if value not in (None, ""):
                    cfg[key] = value
        if not self._config_path().exists():
            _write_json(self._config_path(), cfg)
        return cfg

    def _profile(self) -> dict:
        if not PROFILE_PATH.exists():
            _write_json(PROFILE_PATH, DEFAULT_PROFILE)
            return dict(DEFAULT_PROFILE)
        data = _load_json(PROFILE_PATH, {})
        merged = dict(DEFAULT_PROFILE)
        if isinstance(data, dict):
            merged.update(data)
        return merged

    def _state(self) -> dict:
        data = _load_json(STATE_PATH, {})
        return data if isinstance(data, dict) else {}

    def _save_state(self, data: dict) -> None:
        _write_json(STATE_PATH, data)

    def _life(self) -> dict:
        data = _load_json(LIFE_PATH, {})
        if not isinstance(data, dict) or not isinstance(data.get("days"), dict):
            data = {"days": {}}
        return data

    def _save_life(self, data: dict, keep_days: int) -> None:
        days = data.get("days") or {}
        if len(days) > keep_days:
            for key in sorted(days)[: len(days) - keep_days]:
                days.pop(key, None)
        _write_json(LIFE_PATH, {"days": days})

    def _today_record(self):
        return self._life()["days"].get(_today())

    # ---------------- 作品库（长期去重） ----------------

    def _library(self) -> dict:
        data = _load_json(LIBRARY_PATH, {})
        if not isinstance(data, dict) or not isinstance(data.get("works"), dict):
            return {"works": {}}
        return data

    def _save_library(self, data: dict) -> None:
        _write_json(LIBRARY_PATH, data)

    def _note_works(self, works, day: str = "") -> None:
        """把作品记进长期库：玩过/看过的，以后不再当新坑。"""
        lib = self._library()
        items = lib["works"]
        changed = False
        for raw in works or []:
            if isinstance(raw, dict):
                name = _clean(str(raw.get("name") or ""))[:30]
                wtype = str(raw.get("type") or "").strip().lower()
            else:
                name = _clean(str(raw))[:30]
                wtype = ""
            if not name:
                continue
            stamp = day or _today()
            record = items.get(name)
            if isinstance(record, dict):
                record["last"] = stamp
                try:
                    record["count"] = int(record.get("count") or 0) + 1
                except Exception:
                    record["count"] = 1
                if wtype and not record.get("type"):
                    record["type"] = wtype
            else:
                items[name] = {
                    "type": wtype or "?",
                    "first": stamp,
                    "last": stamp,
                    "count": 1,
                }
            changed = True
        if changed:
            lib["works"] = items
            self._save_library(lib)

    def _library_block(self, cooldown_days: int = 60) -> str:
        """告诉模型：这些已经玩过/看过了，最近别再当新发现。"""
        items = self._library().get("works") or {}
        if not items:
            return ""
        cutoff = (date.today() - timedelta(days=max(1, cooldown_days))).isoformat()
        fresh = [
            name
            for name, record in items.items()
            if isinstance(record, dict) and str(record.get("last") or "") >= cutoff
        ]
        bits = [f"你已经看过或玩过 {len(items)} 部作品，不要当成今天新发现的"]
        if fresh:
            bits.append("最近还碰过的：" + "、".join(fresh[:12]))
        return "；".join(bits) + "。\n"

    # ---------------- 抓冲浪素材 ----------------

    async def _get(self, client, url: str):
        try:
            resp = await client.get(url, headers=HEADERS, timeout=12.0)
            if resp.status_code == 200:
                return resp.text
            logger.warning(f"[life_engine] {url} -> HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"[life_engine] {url} 拉取失败: {e}")
        return None

    async def _douyin(self, client, limit: int = 10):
        text = await self._get(
            client, "https://www.douyin.com/aweme/v1/web/hot/search/list/"
        )
        if not text:
            return []
        try:
            data = json.loads(text)
            words = (data.get("data") or {}).get("word_list") or []
        except Exception:
            return []
        out = []
        for word in words[:limit]:
            title = _clean(str(word.get("word") or ""))
            if title:
                out.append(
                    {
                        "kind": "抖音热榜",
                        "title": title,
                        "link": "https://www.baidu.com/s?wd=" + quote(title),
                    }
                )
        return out

    async def _bili_hot_words(self, client, limit: int = 10):
        text = await self._get(
            client, "https://api.bilibili.com/x/web-interface/search/square?limit=10"
        )
        if not text:
            return []
        try:
            data = json.loads(text)
            items = ((data.get("data") or {}).get("trending") or {}).get("list") or []
        except Exception:
            return []
        out = []
        for item in items[:limit]:
            title = _clean(str(item.get("show_name") or item.get("keyword") or ""))
            if title:
                out.append(
                    {
                        "kind": "B站热搜",
                        "title": title,
                        "link": "https://search.bilibili.com/all?keyword=" + quote(title),
                    }
                )
        return out

    async def _bili_popular(self, client, limit: int = 8):
        text = await self._get(
            client, "https://api.bilibili.com/x/web-interface/popular?ps=10&pn=1"
        )
        if not text:
            return []
        try:
            data = json.loads(text)
            items = (data.get("data") or {}).get("list") or []
        except Exception:
            return []
        out = []
        for item in items[:limit]:
            title = _clean(str(item.get("title") or ""))
            bvid = str(item.get("bvid") or "")
            desc = _clean(str(item.get("desc") or ""))
            if not title:
                continue
            out.append(
                {
                    "kind": "B站热门",
                    "title": title,
                    "link": f"https://www.bilibili.com/video/{bvid}" if bvid else "",
                    "desc": desc[:80],
                }
            )
        return out

    async def _netease(self, client, limit: int = 10):
        text = await self._get(
            client, "https://music.163.com/api/playlist/detail?id=3778678"
        )
        if not text:
            return []
        try:
            data = json.loads(text)
            tracks = (data.get("playlist") or {}).get("tracks") or []
        except Exception:
            return []
        out = []
        for track in tracks[:limit]:
            name = _clean(str(track.get("name") or ""))
            artists = "、".join(
                _clean(str(a.get("name") or "")) for a in (track.get("artists") or [])[:2]
            )
            if name:
                out.append({"kind": "网易云热歌", "title": f"{name} - {artists}".strip(" -")})
        return out

    async def _bili_game_rank(self, client, limit: int = 6):
        """B站游戏区排行：直接带具体游戏/版本名，等于‘最近大家在玩什么’。"""
        text = await self._get(
            client, "https://api.bilibili.com/x/web-interface/ranking/v2?rid=4&type=all"
        )
        if not text:
            return []
        try:
            data = json.loads(text)
            items = (data.get("data") or {}).get("list") or []
        except Exception:
            return []
        out = []
        for item in items[:limit]:
            title = _clean(str(item.get("title") or ""))
            bvid = str(item.get("bvid") or "")
            if not title:
                continue
            out.append(
                {
                    "kind": "游戏榜",
                    "title": title,
                    "link": f"https://www.bilibili.com/video/{bvid}" if bvid else "",
                }
            )
        return out

    async def _bili_anime_rank(self, client, limit: int = 6):
        """B站番剧排行：带具体番名和评分。"""
        text = await self._get(
            client, "https://api.bilibili.com/pgc/web/rank/list?day=3&season_type=1"
        )
        if not text:
            return []
        try:
            data = json.loads(text)
            items = (data.get("result") or {}).get("list") or []
        except Exception:
            return []
        out = []
        for item in items[:limit]:
            name = _clean(str(item.get("title") or ""))
            rating = _clean(str(item.get("rating") or ""))
            if not name:
                continue
            out.append(
                {
                    "kind": "番剧榜",
                    "title": name,
                    "link": str(item.get("link") or ""),
                    "desc": f"评分 {rating}" if rating else "",
                }
            )
        return out

    async def _anysearch(self, client, query: str, limit: int = 3, api_key: str = ""):
        """调 AnySearch 兜底。key 留空就走匿名额度。

        这是一路“锦上添花”的素材：连不上、超时、返回非 200 一律直接放弃，
        不重试、不等待，也绝不影响当天生活的生成。
        """
        headers = {"Content-Type": "application/json"}
        key = (api_key or "").strip()
        if key:
            headers["Authorization"] = f"Bearer {key}"
        try:
            resp = await client.post(
                "https://api.anysearch.com/v1/search",
                json={"query": query, "max_results": limit},
                headers=headers,
                timeout=httpx.Timeout(12.0, connect=5.0),
            )
            if resp.status_code != 200:
                logger.info(f"[life_engine] 搜索兜底跳过（HTTP {resp.status_code}）")
                return []
            data = resp.json()
        except Exception as e:
            logger.info(f"[life_engine] 搜索兜底放弃（{type(e).__name__}）")
            return []
        out = []
        for item in ((data.get("data") or {}).get("results") or [])[:limit]:
            title = _clean(str(item.get("title") or ""))
            if not title:
                continue
            out.append(
                {
                    "kind": "搜索补充",
                    "title": title,
                    "link": str(item.get("url") or ""),
                    "desc": _clean(str(item.get("snippet") or item.get("content") or ""))[:120],
                }
            )
        return out

    async def _search_enrich(self, client, material, api_key: str = ""):
        """拿番剧榜第一名去搜一下进度，补一条‘更新到第几集’这类细节。"""
        if not material:
            return []
        names = []
        for item in material:
            if item.get("kind") != "番剧榜":
                continue
            name = re.sub(r"[（(].*?[)）]", "", str(item.get("title") or "")).strip()
            if name and name not in names:
                names.append(name)
        if not names:
            return []
        return await self._anysearch(
            client, f"{names[0]} 更新到第几集 最新进度", limit=3, api_key=api_key
        )

    async def _search_discover(self, client, profile, cfg, api_key: str = ""):
        """按口味去搜“最近有什么能开的新坑”，结果作为候选素材。

        查询词是门学问：带一堆顿号分隔的口味词会让搜索结果跑偏，
        实测「2026年10月新番 一览」这种“找清单页”的问法才拿得到作品名。
        """
        prefs = profile.get("preferences") or {}
        if not isinstance(prefs, dict):
            return []
        anime = [str(x).strip() for x in (prefs.get("anime") or []) if str(x).strip()]
        games = [str(x).strip() for x in (prefs.get("games") or []) if str(x).strip()]
        today = date.today()
        year = today.year
        queries = []
        if anime:
            queries.append(f"{year}年{today.month}月新番 一览 {anime[0]} 推荐")
        if games:
            queries.append(f"Steam {games[0]} 游戏 推荐 {year}")
        try:
            limit = max(1, min(int(cfg.get("discover_per_day", 2)), 3))
        except (TypeError, ValueError):
            limit = 2
        out = []
        for query in queries[:limit]:
            for item in await self._anysearch(client, query, limit=4, api_key=api_key):
                item["kind"] = "发现"
                item["desc"] = (item.get("desc") or "")[:200]
                out.append(item)
        return out

    async def _collect(self, cfg=None):
        cfg = cfg or {}
        timeout = httpx.Timeout(12.0, connect=8.0)
        async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
            results = await asyncio.gather(
                self._douyin(client),
                self._bili_hot_words(client),
                self._bili_popular(client),
                self._netease(client),
                self._bili_game_rank(client),
                self._bili_anime_rank(client),
                return_exceptions=True,
            )
            material = []
            for res in results:
                if isinstance(res, Exception) or not res:
                    continue
                material.extend(res)
            api_key = str(cfg.get("search_api_key") or "")
            if cfg.get("search_enrich", True):
                try:
                    material.extend(await self._search_enrich(client, material, api_key))
                except Exception:
                    logger.exception("[life_engine] 搜索补充失败，跳过")
            if cfg.get("discover_enabled", True):
                try:
                    material.extend(
                        await self._search_discover(client, self._profile(), cfg, api_key)
                    )
                except Exception:
                    logger.exception("[life_engine] 发现候选失败，跳过")
        return material

    # ---------------- 人设 ----------------

    def _persona_text(self, umo: str) -> str:
        persona_id = ""
        try:
            cfg = self.context.get_config(umo) or {}
            persona_id = str(
                (((cfg.get("agent_runner") or {}).get("config") or {}).get("persona") or {}).get(
                    "persona_id"
                )
                or ""
            )
        except Exception:
            persona_id = ""
        try:
            con = sqlite3.connect(f"file:{DATA_DIR / 'data_v4.db'}?mode=ro", uri=True)
            try:
                if persona_id:
                    row = con.execute(
                        "SELECT system_prompt FROM personas WHERE persona_id=?",
                        (persona_id,),
                    ).fetchone()
                else:
                    row = con.execute(
                        "SELECT system_prompt FROM personas WHERE persona_id<>'default' LIMIT 1"
                    ).fetchone()
            finally:
                con.close()
            if row and row[0]:
                return str(row[0]).strip()
        except Exception:
            logger.exception("[life_engine] 读人设失败")
        return str(self._cfg().get("persona") or "").strip() or (
            "你是一个普通的年轻人，说话简短、口语化，有自己的生活节奏。"
        )

    # ---------------- 生成 ----------------

    @staticmethod
    def _guess_type(name: str, material) -> str:
        """看这个名字出自哪一路素材，判断是番还是游戏。"""
        for item in material:
            if name and name in str(item.get("title") or ""):
                kind = item.get("kind")
                if kind == "番剧榜":
                    return "anime"
                if kind == "游戏榜":
                    return "game"
        return ""

    def _recent_block(self, days: int = 3) -> str:
        """最近几天自己看过、玩过什么——用来防止天天重复同一件事。"""
        life = self._life()
        today = _today()
        keys = [k for k in sorted(life.get("days") or {}) if k < today][-max(1, days) :]
        lines = []
        for key in keys:
            record = life["days"][key]
            bits = []
            works = record.get("works") or []
            if works:
                bits.append("碰过 " + "、".join(works[:4]))
            events = [e.get("text", "") for e in (record.get("timeline") or [])][-2:]
            if events:
                bits.append("；".join(_short(t, 16) for t in events))
            if bits:
                lines.append(f"- {key}：" + "；".join(bits))
        if not lines:
            return ""
        return "最近几天你自己是这样过的（今天别原地重复）：\n" + "\n".join(lines) + "\n"

    @staticmethod
    def _extract_works(day, material) -> list:
        """从素材里认出今天实际写到的作品名，存下来给后几天做去重。"""
        text = " ".join(day.get("plan") or [])
        text += " " + " ".join(e.get("text", "") for e in day.get("timeline") or [])
        names = []
        for item in material:
            raw = str(item.get("title") or "")
            candidates = re.findall(r"《([^》]{2,20})》", raw)
            if item.get("kind") in ("番剧榜", "游戏榜"):
                candidates.append(re.sub(r"[（(].*?[)）]", "", raw).strip())
            for cand in candidates:
                cand = cand.strip()
                if 2 <= len(cand) <= 20 and cand in text and cand not in names:
                    names.append(cand)
        return names[:6]

    def _build_prompt(self, material, profile, cfg, yesterday) -> str:
        wake = cfg.get("wake_time", "08:30")
        sleep = cfg.get("sleep_time", "00:30")
        lines = []
        for idx, item in enumerate(material, start=1):
            extra = f"（简介：{item['desc']}）" if item.get("desc") else ""
            lines.append(f"{idx}. [{item['kind']}] {_short(item['title'], 50)}{extra}")

        y_block = ""
        if yesterday:
            events = yesterday.get("timeline") or []
            if events:
                y_block = "昨天你干了这些：" + "；".join(
                    _short(e.get("text", ""), 20) for e in events[-3:]
                ) + "\n"
        recent_block = self._recent_block(int(cfg.get("recent_days", 3)))
        lib_block = self._library_block(int(cfg.get("cooldown_days", 60)))

        return (
            "你在给自己安排今天。\n"
            f"你的兴趣：{json.dumps(profile, ensure_ascii=False)}\n"
            f"你的作息：{wake} 起床（爱赖床），{sleep} 睡觉。\n"
            f"{y_block}"
            f"{recent_block}"
            f"{lib_block}"
            "今天网上在聊这些（只能按编号引用，不要编造）：\n"
            + "\n".join(lines)
            + "\n\n"
            "请输出 JSON（不要代码块、不要多余文字）：\n"
            '{"plan": ["今天想做的三件事"], "mood": "一句心情", '
            '"timeline": [{"t": "09:10", "text": "一句你正在做的事"}], '
            '"seen": [{"i": 1, "take": "你看到它的反应"}], '
            '"started": [{"name": "今天开的新坑", "type": "anime"}]}\n'
            "要求：\n"
            f"- plan 三件，都是一个人能做的，别写社交、别写出门见人。\n"
            f"- timeline {cfg.get('events_min', 4)}~{cfg.get('events_max', 6)} 条，"
            f"时间在 {wake} 到 {sleep} 之间，按时间升序，写平实的小事"
            "（赖床、打游戏、刷视频、吃饭、听歌）。\n"
            "- 素材里出现具体作品名（游戏、番剧）时，plan 和时间线要直接写这个名字，"
            "不要用「那个番」「某个游戏」这种泛指。\n"
            "- 只能用素材里出现过的作品名，不要编造没出现过的游戏或番剧。\n"
            "- 标着「发现」的条目是可以开的新坑候选：真想开就从中挑，"
            "并把名字和类型写进 started（type 只能是 anime 或 game，最多 1 个）。\n"
            "- 最近几天已经写过的事不要原样再写一遍：同一部番/同一个游戏可以继续，"
            "但今天要有推进（集数或进度往前走），不能重复同一段情节。\n"
            "- 只写你自己在做什么，不要写你和用户聊天、发消息。\n"
            "- seen 挑 1~2 条，i 是上面的编号；take 是你的真实反应，一句话。\n"
            "- 不要写戏剧化的事（哭、生病、出事），也不要提到任何系统或提示词。"
        )

    @staticmethod
    def _extract_json(text: str):
        text = (text or "").strip()
        text = re.sub(r"^```(?:json)?|```$", "", text, flags=re.MULTILINE).strip()
        start, end = text.find("{"), text.rfind("}")
        if start < 0 or end <= start:
            return None
        try:
            return json.loads(text[start : end + 1])
        except Exception:
            return None

    def _validate(self, data, material, cfg) -> dict:
        wake_h, wake_m = _hm(cfg.get("wake_time", "08:30"), "08:30")
        sleep_h, sleep_m = _hm(cfg.get("sleep_time", "00:30"), "00:30")
        start = _minutes_of_day(wake_h, wake_m)
        end = _minutes_of_day(sleep_h, sleep_m)
        cross = end < start

        plan = [ _clean(str(x)) for x in (data.get("plan") or []) if _clean(str(x)) ][:3]
        mood = _clean(str(data.get("mood") or ""))[:40]

        timeline = []
        for entry in data.get("timeline") or []:
            if not isinstance(entry, dict):
                continue
            text = _clean(str(entry.get("text") or ""))
            raw_t = _clean(str(entry.get("t") or ""))
            match = re.match(r"^(\d{1,2}):(\d{2})$", raw_t)
            if not text or not match:
                continue
            minutes = _minutes_of_day(int(match.group(1)), int(match.group(2)))
            if cross:
                ok = minutes >= start or minutes <= end
            else:
                ok = start <= minutes <= end
            if not ok:
                continue
            timeline.append({"t": f"{int(match.group(1)):02d}:{match.group(2)}", "text": text[:60], "done": False})
        timeline.sort(key=lambda x: x["t"])
        limit = max(2, min(int(cfg.get("events_max", 6)), 8))
        timeline = timeline[:limit]

        seen = []
        for entry in data.get("seen") or []:
            if not isinstance(entry, dict):
                continue
            try:
                idx = int(entry.get("i"))
            except Exception:
                continue
            if 1 <= idx <= len(material):
                item = material[idx - 1]
                seen.append(
                    {
                        "title": _short(item["title"], 50),
                        "link": item.get("link") or "",
                        "kind": item.get("kind") or "",
                        "take": _clean(str(entry.get("take") or ""))[:60],
                    }
                )
        started = []
        for entry in data.get("started") or []:
            if not isinstance(entry, dict):
                continue
            name = _clean(str(entry.get("name") or ""))[:30]
            kind = str(entry.get("type") or "").strip().lower()
            if name and kind in ("anime", "game") and name not in [s["name"] for s in started]:
                started.append({"name": name, "type": kind})
        return {
            "plan": plan,
            "mood": mood,
            "timeline": timeline,
            "seen": seen[:2],
            "started": started[:1],
        }

    def _fallback_day(self, cfg) -> dict:
        """调用失败时的兜底，别让她那天没有生活。"""
        wake_h, wake_m = _hm(cfg.get("wake_time", "08:30"), "08:30")
        base = [
            ("赖床，不想起", 15),
            ("刷了会儿手机", 120),
            ("随便吃了点", 300),
            ("打了两把游戏", 420),
            ("有点困了", 600),
        ]
        timeline = []
        for text, offset in base:
            minutes = wake_h * 60 + wake_m + offset
            hour = (minutes // 60) % 24
            timeline.append(
                {"t": f"{hour:02d}:{minutes % 60:02d}", "text": text, "done": False}
            )
        return {
            "plan": ["打两把游戏", "刷会儿视频", "早点睡"],
            "mood": "有点无聊",
            "timeline": timeline,
            "seen": [],
        }

    async def _generate_day(self, cfg) -> bool:
        umo = str(cfg.get("umo") or "").strip()
        if not umo:
            logger.error("[life_engine] 没有配置 umo")
            return False
        today = _today()
        life = self._life()
        material = await self._collect(cfg)
        profile = self._profile()
        yesterday = None
        keys = sorted(k for k in life["days"] if k < today)
        if keys:
            yesterday = life["days"][keys[-1]]

        prompt = self._build_prompt(material, profile, cfg, yesterday)
        provider = await self.context.get_using_provider_async(umo)
        if provider is None:
            logger.error("[life_engine] 没有可用模型")
            return False

        data = None
        if material:
            try:
                resp = await provider.text_chat(
                    prompt=prompt,
                    system_prompt=self._persona_text(umo),
                    session_id=f"life:{umo}",
                )
                raw = (getattr(resp, "completion_text", "") or "").strip()
                data = self._extract_json(raw)
            except Exception:
                logger.exception("[life_engine] 生成失败")
                data = None

        day = self._validate(data, material, cfg) if data else self._fallback_day(cfg)
        if not day["timeline"]:
            day = self._fallback_day(cfg)
        day["generated_at"] = _now()
        day["diary_written"] = False
        day["source_count"] = len(material)
        day["works"] = self._extract_works(day, material)
        try:
            started = day.get("started") or []
            if started:
                self._note_works(started)
            if day["works"]:
                self._note_works(
                    [
                        {"name": name, "type": self._guess_type(name, material)}
                        for name in day["works"]
                    ]
                )
        except Exception:
            logger.exception("[life_engine] 更新作品库失败")

        life["days"][today] = day
        self._save_life(life, max(3, min(int(cfg.get("keep_days", 7)), 30)))
        logger.info(
            f"[life_engine] 生成 {today}：plan {len(day['plan'])} 条，"
            f"timeline {len(day['timeline'])} 条，素材 {len(material)} 条"
        )
        return True

    # ---------------- 注入 ----------------

    def _inject_block(self, umo: str) -> str:
        record = self._today_record()
        if not record:
            return ""
        now_hm = datetime.now().strftime("%H:%M")
        past = [e for e in record.get("timeline") or [] if e.get("t", "") <= now_hm]
        lines = []
        if record.get("plan"):
            lines.append("今天想做：" + "、".join(record["plan"][:3]))
        if past:
            lines.append("刚发生：" + past[-1].get("text", ""))
        if record.get("mood"):
            lines.append("心情：" + record["mood"])
        works = record.get("works") or []
        if works:
            lines.append("在追/在玩：" + "、".join(works[:3]))
        seen = record.get("seen") or []
        if seen:
            lines.append("刷到的：" + seen[0].get("title", ""))
        if not lines:
            return ""
        return "【你最近】\n" + "\n".join(lines) + "\n"

    @filter.on_llm_request()
    async def inject_life(self, event: AstrMessageEvent, req: ProviderRequest):
        try:
            cfg = self._cfg()
            mode = str(cfg.get("inject_mode", "low")).lower()
            if not cfg.get("enabled") or mode == "off":
                return
            umo = event.unified_msg_origin
            if mode == "low":
                gap = max(1, int(cfg.get("inject_gap_minutes", 30))) * 60
                last = self._last_inject.get(umo, 0)
                if time.time() - last < gap:
                    return
            block = self._inject_block(umo)
            if not block:
                return
            req.system_prompt = (req.system_prompt or "") + "\n" + block
            self._last_inject[umo] = time.time()
            logger.info(f"[life_engine] 注入生活状态（{mode}）")
        except Exception:
            logger.exception("[life_engine] 注入失败")

    # ---------------- 写日记 ----------------

    def _write_diary(self, cfg) -> None:
        if not cfg.get("write_to_diary", True):
            return
        umo = str(cfg.get("umo") or "").strip()
        if not umo or not DIARY_DB.exists():
            return
        # 01:30 时"昨天"的那一天
        day = (datetime.now() - timedelta(hours=6)).date().isoformat()
        life = self._life()
        record = life["days"].get(day)
        if not record or record.get("diary_written"):
            return
        events = record.get("timeline") or []
        if not events:
            return
        parts = [e.get("text", "") for e in events]
        mood = record.get("mood") or ""
        content = "今天我自己：" + "；".join(parts[:4]) + "。"
        if mood:
            content += f"心情{mood}。"
        content = content[:400]
        try:
            con = sqlite3.connect(str(DIARY_DB), timeout=15)
            try:
                con.execute("PRAGMA busy_timeout=15000")
                row = con.execute("SELECT enabled FROM sessions WHERE umo=?", (umo,)).fetchone()
                if not row or not row[0]:
                    return
                con.execute(
                    "INSERT INTO buffer (umo, day, ts, role, sender_name, content) "
                    "VALUES (?,?,?,?,?,?)",
                    (umo, day, _now(), "assistant", str(cfg.get("diary_sender") or "助手"), content),
                )
                con.commit()
            finally:
                con.close()
            record["diary_written"] = True
            self._save_life(life, max(3, min(int(cfg.get("keep_days", 7)), 30)))
            logger.info(f"[life_engine] 已把 {day} 的生活写进日记素材")
        except Exception:
            logger.exception("[life_engine] 写日记失败")

    # ---------------- 调度 ----------------

    def _next_ts(self, hour: int, minute: int) -> float:
        now = datetime.now()
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return target.timestamp()

    async def _run_generate(self) -> None:
        async with self._lock:
            cfg = self._cfg()
            today = _today()
            state = self._state()
            if state.get("generate_date") == today and state.get("generate_status") == "ok":
                return
            try:
                retries = max(1, min(int(cfg.get("retry_times", 3)), 5))
            except (TypeError, ValueError):
                retries = 3
            for attempt in range(1, retries + 1):
                ok = False
                try:
                    ok = await self._generate_day(cfg)
                except Exception:
                    logger.exception("[life_engine] 生成异常")
                if ok:
                    self._save_state({"generate_date": today, "generate_status": "ok", "attempts": attempt})
                    return
                self._save_state(
                    {"generate_date": today, "generate_status": "failed", "attempts": attempt}
                )
                if attempt < retries:
                    wait = RETRY_BACKOFF[min(attempt - 1, len(RETRY_BACKOFF) - 1)]
                    logger.info(f"[life_engine] 生成失败，{wait} 秒后重试")
                    await asyncio.sleep(wait)

    async def _run_diary(self) -> None:
        cfg = self._cfg()
        # 只在凌晨 4 点前写，否则日记可能已经生成过了
        if datetime.now().hour >= 4:
            return
        self._write_diary(cfg)

    async def _loop(self):
        await asyncio.sleep(15)
        # 启动时：先把昨天没写的日记补上，再看今天的生活生成没
        try:
            await self._run_diary()
        except Exception:
            logger.exception("[life_engine] 启动补写日记失败")
        while True:
            try:
                cfg = self._cfg()
                if not cfg.get("enabled"):
                    await asyncio.sleep(600)
                    continue
                gh, gm = int(cfg.get("generate_hour", 8)), int(cfg.get("generate_minute", 30))
                dh, dm = _hm(cfg.get("diary_at", "01:30"), "01:30")
                gen_at = self._next_ts(gh, gm)
                diary_at = self._next_ts(dh, dm)
                target = min(gen_at, diary_at)
                await asyncio.sleep(max(1.0, target - time.time()))
                if target == gen_at:
                    await self._run_generate()
                else:
                    await self._run_diary()
                await asyncio.sleep(61)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[life_engine] 调度异常")
                await asyncio.sleep(300)

    # ---------------- 手动 ----------------

    @filter.command("生活")
    async def cmd_life(self, event: AstrMessageEvent):
        """重新生成今天的生活：/生活"""
        event.stop_event()
        yield event.plain_result("我去过会儿日子……")
        cfg = self._cfg()
        ok = False
        try:
            ok = await self._generate_day(cfg)
        except Exception:
            logger.exception("[life_engine] 手动生成失败")
        record = self._today_record() or {}
        if not ok:
            yield event.plain_result("没生成成，看后台日志。")
            return
        plan = "、".join(record.get("plan") or [])
        yield event.plain_result(f"今天想做：{plan or '（空）'}")

    async def terminate(self):
        if self._task:
            self._task.cancel()
            self._task = None
