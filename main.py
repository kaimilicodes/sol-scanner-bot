#!/usr/bin/env python3
"""
scanner_bot.py

A Solana early-memecoin SCANNER bot (not a trading bot). Discovers new tokens
via pump.fun's real-time feed, enriches them with DexScreener (market cap,
liquidity, volume, buy/sell counts) and Helius RPC (mint/freeze authority,
top-holder concentration), scores them, and sends Telegram alerts.

Runs TWO independent scanning lanes from the same discovery feed:
  - MAIN lane:    MC $10K-$250K (the original spotter)
  - LOW-CAP lane: MC $10K-$50K  (extra, does not affect the main lane)

Supports multiple authorized Telegram users (e.g. you + a friend), each with
their own independent /threshold setting.

⚠️ REALITY CHECK ⚠️
This bot does NOT predict winners. Every alert needs your own 15-second look
before you do anything with real money. Not financial advice. Never trades
automatically.
"""

import asyncio
import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import requests
import websockets

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "PUT_YOUR_BOT_TOKEN_HERE")
OWNER_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "PUT_YOUR_CHAT_ID_HERE")
FRIEND_CHAT_ID = os.environ.get("FRIEND_CHAT_ID", "").strip()
HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "PUT_YOUR_HELIUS_KEY_HERE")

PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"
DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint}"
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

MC_MIN = 10_000
MC_MAX = 250_000
LOWCAP_MC_MIN = 10_000
LOWCAP_MC_MAX = 50_000

MAX_AGE_HOURS = 24
PRIORITY_AGE_HOURS = 6
MIN_LIQUIDITY_USD = 5_000
GOOD_LIQ_MC_RATIO = 0.10
SCAN_INTERVAL_SEC = 20
MAX_TRACK_AGE_SEC = 12 * 3600
ALERTED_TRACK_AGE_SEC = 3 * 24 * 3600
HELIUS_MIN_LIQUIDITY_TO_CHECK = MIN_LIQUIDITY_USD
SCORE_THRESHOLDS = {"watch": 65, "high": 80}
RECENT_HISTORY_MAX = 40
MILESTONE_MULTIPLES = [2, 5, 10, 20, 30, 40, 50]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scanner_bot")

TARGET_MULTIPLES = [2, 5, 10, 20, 30, 40, 50]

AUTHORIZED_USERS: Dict[str, dict] = {}
if OWNER_CHAT_ID and "PUT_YOUR" not in OWNER_CHAT_ID:
    AUTHORIZED_USERS[str(OWNER_CHAT_ID)] = {"name": "watch", "value": SCORE_THRESHOLDS["watch"]}
if FRIEND_CHAT_ID:
    AUTHORIZED_USERS[str(FRIEND_CHAT_ID)] = {"name": "watch", "value": SCORE_THRESHOLDS["watch"]}


def is_authorized(chat_id: str) -> bool:
    return str(chat_id) in AUTHORIZED_USERS


CLOSE_BUTTON_MARKUP = {"inline_keyboard": [[{"text": "❌ Close", "callback_data": "close"}]]}

THRESHOLD_KEYBOARD_MARKUP = {
    "inline_keyboard": [
        [{"text": "🟡 Watch+ (65+)", "callback_data": "thresh_watch"}],
        [{"text": "🟢 High Interest only (80+)", "callback_data": "thresh_high"}],
    ]
}

PERSISTENT_MENU_MARKUP = {
    "keyboard": [
        [{"text": "🕵️ Recent"}, {"text": "🎯 Threshold"}],
        [{"text": "🪙 Coins"}, {"text": "📊 Stats"}],
        [{"text": "ℹ️ Help"}, {"text": "🙈 Hide menu"}],
    ],
    "resize_keyboard": True,
    "is_persistent": True,
}

HIDE_MENU_MARKUP = {"remove_keyboard": True}


def send_telegram_message(text: str, chat_id: Optional[str] = None, markup: Optional[dict] = None) -> Optional[int]:
    target = chat_id or OWNER_CHAT_ID
    if "PUT_YOUR" in TELEGRAM_BOT_TOKEN or not target or "PUT_YOUR" in str(target):
        log.warning("Telegram not configured — printing message instead:\n%s", text)
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": target,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    if markup is not None:
        payload["reply_markup"] = json.dumps(markup)
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code != 200:
            log.error("Telegram send failed: %s %s", resp.status_code, resp.text)
            return None
        return resp.json().get("result", {}).get("message_id")
    except requests.RequestException as e:
        log.error("Telegram send exception: %s", e)
        return None


def broadcast_to(chat_ids, text: str, markup: Optional[dict] = None) -> None:
    for cid in chat_ids:
        send_telegram_message(text, chat_id=cid, markup=markup)


def delete_telegram_message(chat_id, message_id) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteMessage"
    try:
        requests.post(url, json={"chat_id": chat_id, "message_id": message_id}, timeout=10)
    except requests.RequestException as e:
        log.error("Telegram delete exception: %s", e)


def answer_callback_query(callback_query_id: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    try:
        requests.post(url, json={"callback_query_id": callback_query_id}, timeout=10)
    except requests.RequestException as e:
        log.error("Telegram answerCallbackQuery exception: %s", e)


def fetch_dexscreener_pair(mint: str) -> Optional[dict]:
    try:
        resp = requests.get(DEXSCREENER_TOKEN_URL.format(mint=mint), timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()
        pairs = [p for p in (data.get("pairs") or []) if p.get("chainId") == "solana"]
        if not pairs:
            return None
        pairs.sort(key=lambda p: (p.get("liquidity") or {}).get("usd", 0) or 0, reverse=True)
        return pairs[0]
    except requests.RequestException as e:
        log.error("DexScreener fetch error for %s: %s", mint, e)
        return None


def helius_rpc(method: str, params: list) -> Optional[dict]:
    if "PUT_YOUR" in HELIUS_API_KEY:
        return None
    try:
        resp = requests.post(
            HELIUS_RPC_URL,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            timeout=10,
        )
        if resp.status_code != 200:
            log.error("Helius RPC %s failed: %s %s", method, resp.status_code, resp.text)
            return None
        result = resp.json()
        if "error" in result:
            log.error("Helius RPC %s error: %s", method, result["error"])
            return None
        return result.get("result")
    except requests.RequestException as e:
        log.error("Helius RPC exception (%s): %s", method, e)
        return None


def get_mint_authorities(mint: str) -> Dict[str, Optional[str]]:
    result = helius_rpc("getAccountInfo", [mint, {"encoding": "jsonParsed"}])
    if not result or not result.get("value"):
        return {"mint_authority": "UNKNOWN", "freeze_authority": "UNKNOWN"}
    try:
        info = result["value"]["data"]["parsed"]["info"]
        return {
            "mint_authority": info.get("mintAuthority"),
            "freeze_authority": info.get("freezeAuthority"),
        }
    except (KeyError, TypeError):
        return {"mint_authority": "UNKNOWN", "freeze_authority": "UNKNOWN"}


def get_token_supply(mint: str) -> Optional[float]:
    result = helius_rpc("getTokenSupply", [mint])
    if not result:
        return None
    try:
        return float(result["value"]["uiAmount"])
    except (KeyError, TypeError, ValueError):
        return None


def get_top_holders(mint: str) -> List[float]:
    result = helius_rpc("getTokenLargestAccounts", [mint])
    if not result:
        return []
    try:
        return [float(a["uiAmount"] or 0) for a in result["value"]]
    except (KeyError, TypeError, ValueError):
        return []


@dataclass
class LaneState:
    alerted_for: Set[str] = field(default_factory=set)
    entry_mc: float = 0.0
    max_mc: float = 0.0
    milestones_hit: set = field(default_factory=set)
    last_score: float = 0.0
    tier: str = "AVOID"

    @property
    def alerted(self) -> bool:
        return bool(self.alerted_for)


@dataclass
class Candidate:
    mint: str
    name: str
    symbol: str
    created_at: float = field(default_factory=time.time)
    last_checked: float = 0.0
    status: str = "pending"
    last_mc: float = 0.0
    last_liquidity: float = 0.0
    top_holders_snapshot: List[float] = field(default_factory=list)
    mint_authority: Optional[str] = None
    freeze_authority: Optional[str] = None
    authorities_checked: bool = False
    main: LaneState = field(default_factory=LaneState)
    lowcap: LaneState = field(default_factory=LaneState)

    @property
    def any_alerted(self) -> bool:
        return self.main.alerted or self.lowcap.alerted


candidates: Dict[str, Candidate] = {}
recent_history: deque = deque(maxlen=RECENT_HISTORY_MAX)


def age_hours(c: Candidate) -> float:
    return (time.time() - c.created_at) / 3600.0


def target_mc_lines(mc: float) -> str:
    return "\n".join(f"{m}X: ${mc * m:,.0f}" for m in TARGET_MULTIPLES)


def score_liquidity(liquidity: float, mc: float) -> float:
    if liquidity <= 0 or mc <= 0:
        return 0
    ratio = liquidity / mc
    points = 0
    if liquidity >= MIN_LIQUIDITY_USD:
        points += 8
    if ratio >= GOOD_LIQ_MC_RATIO:
        points += 7
    elif ratio >= 0.05:
        points += 4
    return min(points, 15)


def score_volume(pair: dict, mc: float) -> float:
    vol = pair.get("volume") or {}
    h1 = vol.get("h1", 0) or 0
    m5 = vol.get("m5", 0) or 0
    if mc <= 0:
        return 0
    vol_mc_ratio = h1 / mc if mc else 0
    points = 0
    if vol_mc_ratio >= 0.5:
        points += 12
    elif vol_mc_ratio >= 0.2:
        points += 8
    elif vol_mc_ratio >= 0.05:
        points += 4
    avg_5m_pace = h1 / 12 if h1 else 0
    if m5 > avg_5m_pace * 1.3:
        points += 8
    elif m5 > avg_5m_pace:
        points += 4
    return min(points, 20)


def score_buying_pressure(pair: dict) -> float:
    txns = (pair.get("txns") or {}).get("h1") or {}
    buys = txns.get("buys", 0) or 0
    sells = txns.get("sells", 0) or 0
    if sells == 0:
        ratio = float("inf") if buys > 0 else 0
    else:
        ratio = buys / sells
    if ratio >= 2.0:
        return 15
    if ratio >= 1.5:
        return 11
    if ratio >= 1.3:
        return 7
    return 0


def score_holder_distribution(top_holders: List[float], supply: Optional[float]) -> float:
    if not top_holders or not supply or supply <= 0:
        return 5
    top10_pct = sum(top_holders[:10]) / supply * 100
    if top10_pct <= 30:
        return 10
    if top10_pct <= 50:
        return 6
    if top10_pct <= 70:
        return 2
    return 0


def score_security(mint_auth: Optional[str], freeze_auth: Optional[str]) -> float:
    if mint_auth == "UNKNOWN" or freeze_auth == "UNKNOWN":
        return 3
    points = 0
    if mint_auth is None:
        points += 5
    if freeze_auth is None:
        points += 5
    return points


def check_hard_red_flags(pair: dict, top_holders: List[float], supply: Optional[float],
                          mint_auth: Optional[str], freeze_auth: Optional[str]) -> List[str]:
    flags = []
    liquidity = (pair.get("liquidity") or {}).get("usd", 0) or 0
    mc = pair.get("marketCap") or pair.get("fdv") or 0
    if mc and liquidity / mc < 0.03:
        flags.append("Liquidity/MC ratio critically low (<3%) — easy to rug")
    if mint_auth not in (None, "UNKNOWN"):
        flags.append("Mint authority still enabled — supply can be inflated")
    if freeze_auth not in (None, "UNKNOWN"):
        flags.append("Freeze authority still enabled — wallets can be frozen")
    if top_holders and supply and supply > 0:
        top10_pct = sum(top_holders[:10]) / supply * 100
        if top10_pct > 70:
            flags.append(f"Extreme supply concentration — top 10 holders own {top10_pct:.0f}%")
    return flags


def compute_score(pair: dict, mc: float, liquidity: float, top_holders: List[float],
                   supply: Optional[float], mint_auth: Optional[str], freeze_auth: Optional[str]):
    flags = check_hard_red_flags(pair, top_holders, supply, mint_auth, freeze_auth)
    raw_score = (
        score_liquidity(liquidity, mc)
        + score_volume(pair, mc)
        + score_buying_pressure(pair)
        + score_holder_distribution(top_holders, supply)
        + score_security(mint_auth, freeze_auth)
    )
    score_pct = (raw_score / 70) * 100
    if flags:
        tier = "AVOID"
    elif score_pct >= SCORE_THRESHOLDS["high"]:
        tier = "HIGH_INTEREST"
    elif score_pct >= SCORE_THRESHOLDS["watch"]:
        tier = "WATCH"
    else:
        tier = "AVOID"
    return score_pct, tier, flags


def build_alert_text(c: Candidate, pair: dict, score_pct: float, tier: str,
                      top_holders: List[float], supply: Optional[float], lane_label: str = "") -> str:
    liquidity = (pair.get("liquidity") or {}).get("usd", 0) or 0
    mc = pair.get("marketCap") or pair.get("fdv") or 0
    vol = pair.get("volume") or {}
    txns_h1 = (pair.get("txns") or {}).get("h1") or {}
    txns_m5 = (pair.get("txns") or {}).get("m5") or {}
    buys_5m, sells_5m = txns_m5.get("buys", 0), txns_m5.get("sells", 0)
    ratio = (txns_h1.get("buys", 0) / txns_h1.get("sells", 1)) if txns_h1.get("sells") else float("inf")

    top10_pct = (sum(top_holders[:10]) / supply * 100) if (top_holders and supply) else None
    top20_pct = (sum(top_holders[:20]) / supply * 100) if (top_holders and supply) else None

    tier_emoji = "🟢 HIGH-INTEREST ALERT" if tier == "HIGH_INTEREST" else "🟡 WATCHLIST ALERT"
    prefix = f"{lane_label} " if lane_label else ""

    lines = [
        f"🚨 {prefix}{tier_emoji}",
        "",
        f"Token: *{c.name}*",
        f"Ticker: ${c.symbol}",
        f"Contract: `{c.mint}`",
        "",
        f"MC: ${mc:,.0f}",
        f"Liquidity: ${liquidity:,.0f}",
        f"Liquidity/MC: {(liquidity / mc * 100) if mc else 0:.1f}%",
        f"Age: {age_hours(c) * 60:.0f} min" if age_hours(c) < 1 else f"Age: {age_hours(c):.1f}h",
        "",
        f"5M Volume: ${vol.get('m5', 0):,.0f}",
        f"1H Volume: ${vol.get('h1', 0):,.0f}",
        f"5M Buys/Sells: {buys_5m}/{sells_5m}",
        f"1H Buy/Sell Ratio: {ratio:.2f}" if ratio != float("inf") else "1H Buy/Sell Ratio: buys only",
        "",
        f"Top 10 Holders: {top10_pct:.0f}%" if top10_pct is not None else "Top 10 Holders: unknown",
        f"Top 20 Holders: {top20_pct:.0f}%" if top20_pct is not None else "Top 20 Holders: unknown",
        "",
        f"Mint Authority: {'disabled ✅' if c.mint_authority is None else ('unknown' if c.mint_authority == 'UNKNOWN' else 'ENABLED ⚠️')}",
        f"Freeze Authority: {'disabled ✅' if c.freeze_authority is None else ('unknown' if c.freeze_authority == 'UNKNOWN' else 'ENABLED ⚠️')}",
        "",
        f"Score: {score_pct:.0f}/100",
        "",
        "Target MCs (mathematical only, NOT guaranteed):",
        target_mc_lines(mc),
        "",
        f"[DexScreener](https://dexscreener.com/solana/{pair.get('pairAddress', '')}) | "
        f"[pump.fun](https://pump.fun/{c.mint}) | [Solscan](https://solscan.io/token/{c.mint})",
        "",
        "⚠️ Not financial advice. Check holders and socials yourself before doing anything.",
    ]
    return "\n".join(lines)


def check_milestones(c: Candidate, lane: LaneState, lane_label: str) -> None:
    if lane.entry_mc <= 0:
        return
    multiple = lane.max_mc / lane.entry_mc
    for m in MILESTONE_MULTIPLES:
        if multiple >= m and m not in lane.milestones_hit:
            lane.milestones_hit.add(m)
            broadcast_to(
                lane.alerted_for,
                f"🎉 {lane_label}*{c.name}* (${c.symbol}) just hit {m}X from call!\n"
                f"Entry MC: ${lane.entry_mc:,.0f} → Now: ${lane.max_mc:,.0f}\n"
                f"https://pump.fun/{c.mint}",
                markup=CLOSE_BUTTON_MARKUP,
            )


def monitor_lane(c: Candidate, lane: LaneState, lane_label: str, mc: float,
                  liquidity: float, prev_liquidity: float) -> None:
    if not lane.alerted:
        return
    if mc > lane.max_mc:
        lane.max_mc = mc
        check_milestones(c, lane, lane_label)

    if prev_liquidity > 0:
        drop_pct = (prev_liquidity - liquidity) / prev_liquidity * 100
        if drop_pct >= 50:
            broadcast_to(
                lane.alerted_for,
                f"💀 {lane_label}*MAJOR RUG WARNING*\n*{c.name}* (${c.symbol})\n"
                f"Liquidity dropped {drop_pct:.0f}% since last check.\n"
                f"https://pump.fun/{c.mint}",
                markup=CLOSE_BUTTON_MARKUP,
            )
        elif drop_pct >= 20:
            broadcast_to(
                lane.alerted_for,
                f"💧 {lane_label}*LIQUIDITY DROP*\n*{c.name}* (${c.symbol})\n"
                f"Liquidity dropped {drop_pct:.0f}% since last check.\n"
                f"https://pump.fun/{c.mint}",
                markup=CLOSE_BUTTON_MARKUP,
            )


def maybe_alert_lane(c: Candidate, lane: LaneState, lane_label: str, pair: dict,
                      score_pct: float, tier: str, top_holders: List[float], supply: Optional[float]) -> None:
    if tier not in ("HIGH_INTEREST", "WATCH"):
        return
    for chat_id, settings in AUTHORIZED_USERS.items():
        if chat_id in lane.alerted_for:
            continue
        if score_pct >= settings["value"]:
            text = build_alert_text(c, pair, score_pct, tier, top_holders, supply, lane_label=lane_label)
            send_telegram_message(text, chat_id=chat_id, markup=CLOSE_BUTTON_MARKUP)
            first_ever = not lane.alerted_for
            lane.alerted_for.add(chat_id)
            if first_ever:
                mc = pair.get("marketCap") or pair.get("fdv") or 0
                lane.entry_mc = mc
                lane.max_mc = mc


async def analyze_candidate(c: Candidate) -> None:
    pair = await asyncio.to_thread(fetch_dexscreener_pair, c.mint)
    if not pair:
        if not c.any_alerted:
            c.status = "pending"
        return

    if not c.any_alerted:
        c.status = "active"

    liquidity = (pair.get("liquidity") or {}).get("usd", 0) or 0
    mc = pair.get("marketCap") or pair.get("fdv") or 0
    c.last_mc = mc
    prev_liquidity = c.last_liquidity
    c.last_liquidity = liquidity

    recent_history.append({
        "mint": c.mint, "name": c.name, "symbol": c.symbol,
        "mc": mc, "liquidity": liquidity, "time": time.time(),
        "tier": c.main.tier if c.main.tier != "AVOID" else c.lowcap.tier,
    })

    in_main_range = MC_MIN <= mc <= MC_MAX
    in_lowcap_range = LOWCAP_MC_MIN <= mc <= LOWCAP_MC_MAX
    needs_scoring = in_main_range or in_lowcap_range or c.any_alerted

    if not needs_scoring:
        return

    top_holders: List[float] = c.top_holders_snapshot
    supply: Optional[float] = None

    if liquidity >= HELIUS_MIN_LIQUIDITY_TO_CHECK:
        if not c.authorities_checked:
            auth = await asyncio.to_thread(get_mint_authorities, c.mint)
            c.mint_authority = auth["mint_authority"]
            c.freeze_authority = auth["freeze_authority"]
            c.authorities_checked = True

        new_top_holders = await asyncio.to_thread(get_top_holders, c.mint)
        supply = await asyncio.to_thread(get_token_supply, c.mint)

        if new_top_holders and top_holders and (c.main.alerted or c.lowcap.alerted):
            for old_amt, new_amt in zip(top_holders[:5], new_top_holders[:5]):
                if old_amt > 0 and (old_amt - new_amt) / old_amt > 0.3:
                    alerted_ids = c.main.alerted_for | c.lowcap.alerted_for
                    broadcast_to(
                        alerted_ids,
                        f"🐋 *LARGE WALLET SELL*\n*{c.name}* (${c.symbol})\n"
                        f"A top holder's balance dropped >30% since last check.\n"
                        f"https://pump.fun/{c.mint}",
                        markup=CLOSE_BUTTON_MARKUP,
                    )
                    break
        if new_top_holders:
            top_holders = new_top_holders
            c.top_holders_snapshot = new_top_holders

    score_pct, tier, _flags = compute_score(pair, mc, liquidity, top_holders, supply,
                                             c.mint_authority, c.freeze_authority)

    c.main.last_score = score_pct
    c.main.tier = tier
    monitor_lane(c, c.main, "", mc, liquidity, prev_liquidity)
    if in_main_range and not all(cid in c.main.alerted_for for cid in AUTHORIZED_USERS):
        maybe_alert_lane(c, c.main, "", pair, score_pct, tier, top_holders, supply)

    c.lowcap.last_score = score_pct
    c.lowcap.tier = tier
    monitor_lane(c, c.lowcap, "🔎 LOW-CAP ", mc, liquidity, prev_liquidity)
    if in_lowcap_range and not all(cid in c.lowcap.alerted_for for cid in AUTHORIZED_USERS):
        maybe_alert_lane(c, c.lowcap, "🔎 LOW-CAP ", pair, score_pct, tier, top_holders, supply)


async def scan_loop() -> None:
    while True:
        now = time.time()
        stale = [
            m for m, c in candidates.items()
            if now - c.created_at > (ALERTED_TRACK_AGE_SEC if c.any_alerted else MAX_TRACK_AGE_SEC)
        ]
        for m in stale:
            del candidates[m]

        to_check = [c for c in candidates.values() if now - c.last_checked >= SCAN_INTERVAL_SEC]
        for c in to_check:
            c.last_checked = now
            try:
                await analyze_candidate(c)
            except Exception as e:
                log.error("Error analyzing %s: %s", c.mint, e)
            await asyncio.sleep(0.3)

        await asyncio.sleep(2)


async def handle_new_token(msg: dict) -> None:
    mint = msg.get("mint")
    if not mint or mint in candidates:
        return
    name = msg.get("name", "")
    symbol = msg.get("symbol", "")
    candidates[mint] = Candidate(mint=mint, name=name, symbol=symbol)
    log.info("New candidate: %s (%s) mint=%s", name, symbol, mint)


async def _reconnecting_websocket(url: str, retry_delay: int = 5):
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                yield ws
        except Exception as e:
            log.error("WebSocket connection error: %s — retrying in %ss", e, retry_delay)
            await asyncio.sleep(retry_delay)


async def run_discovery() -> None:
    log.info("Connecting to pump.fun discovery feed...")
    async for ws in _reconnecting_websocket(PUMPPORTAL_WS_URL):
        try:
            await ws.send(json.dumps({"method": "subscribeNewToken"}))
            log.info("Subscribed to new-token feed.")
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if msg.get("txType") == "create":
                    await handle_new_token(msg)
        except websockets.ConnectionClosed:
            log.warning("WebSocket closed, reconnecting...")
            continue


def build_recent_message() -> str:
    if not recent_history:
        return "No tokens scanned yet — still watching."
    lines = ["🕵️ *Recently scanned tokens:*"]
    for entry in reversed(list(recent_history)[-15:]):
        tier_icon = {"HIGH_INTEREST": "🟢", "WATCH": "🟡"}.get(entry["tier"], "🔴")
        lines.append(
            f"{tier_icon} *{entry['name']}* (${entry['symbol']}) — "
            f"MC ${entry['mc']:,.0f} | Liq ${entry['liquidity']:,.0f}\n"
            f"  https://pump.fun/{entry['mint']}"
        )
    return "\n".join(lines)


def build_stats_message() -> str:
    called = [c for c in candidates.values() if c.any_alerted]
    if not called:
        return "No calls yet."

    def best_multiple(c: Candidate) -> float:
        m1 = (c.main.max_mc / c.main.entry_mc) if c.main.alerted and c.main.entry_mc else 0
        m2 = (c.lowcap.max_mc / c.lowcap.entry_mc) if c.lowcap.alerted and c.lowcap.entry_mc else 0
        return max(m1, m2)

    called.sort(key=best_multiple, reverse=True)
    lines = ["📊 *Call stats:*"]
    for c in called[:20]:
        if c.main.alerted and c.main.entry_mc:
            mult = c.main.max_mc / c.main.entry_mc
            icon = "✅" if c.last_mc >= c.main.entry_mc else "❌"
            lines.append(f"{icon} ${c.symbol} — peak {mult:.1f}x (entry ${c.main.entry_mc:,.0f})")
        if c.lowcap.alerted and c.lowcap.entry_mc:
            mult = c.lowcap.max_mc / c.lowcap.entry_mc
            icon = "✅" if c.last_mc >= c.lowcap.entry_mc else "❌"
            lines.append(f"🔎 {icon} ${c.symbol} — peak {mult:.1f}x (entry ${c.lowcap.entry_mc:,.0f})")
    return "\n".join(lines)


def build_coins_keyboard() -> dict:
    called = [c for c in candidates.values() if c.any_alerted]

    def best_multiple(c: Candidate) -> float:
        m1 = (c.main.max_mc / c.main.entry_mc) if c.main.alerted and c.main.entry_mc else 0
        m2 = (c.lowcap.max_mc / c.lowcap.entry_mc) if c.lowcap.alerted and c.lowcap.entry_mc else 0
        return max(m1, m2)

    called.sort(key=best_multiple, reverse=True)
    rows = []
    for c in called[:15]:
        lane = c.main if (c.main.alerted and (not c.lowcap.alerted or c.main.max_mc / c.main.entry_mc >= c.lowcap.max_mc / max(c.lowcap.entry_mc, 1))) else c.lowcap
        if not lane.alerted or not lane.entry_mc:
            continue
        mult = lane.max_mc / lane.entry_mc
        icon = "✅" if c.last_mc >= lane.entry_mc else "❌"
        prefix = "🔎 " if lane is c.lowcap else ""
        label = f"{prefix}{icon} ${c.symbol} {mult:.1f}x"
        rows.append([{"text": label, "callback_data": f"coininfo_{c.mint}"}])
    if not rows:
        rows = [[{"text": "No calls yet", "callback_data": "noop"}]]
    return {"inline_keyboard": rows}


async def build_live_coin_info(mint: str) -> str:
    c = candidates.get(mint)
    if not c:
        return "That coin is no longer being tracked."
    pair = await asyncio.to_thread(fetch_dexscreener_pair, mint)
    if not pair:
        return f"${c.symbol}: no live data available right now."

    liquidity = (pair.get("liquidity") or {}).get("usd", 0) or 0
    mc = pair.get("marketCap") or pair.get("fdv") or 0
    vol = pair.get("volume") or {}

    lines = [f"*{c.name}* (${c.symbol}) — live snapshot", ""]
    lines.append(f"MC: ${mc:,.0f}")
    lines.append(f"Liquidity: ${liquidity:,.0f}")
    lines.append(f"1H Volume: ${vol.get('h1', 0):,.0f}")
    lines.append(f"5M Volume: ${vol.get('m5', 0):,.0f}")

    if c.main.alerted and c.main.entry_mc:
        cur_mult = mc / c.main.entry_mc
        peak_mult = c.main.max_mc / c.main.entry_mc
        lines.append("")
        lines.append(f"Main call entry MC: ${c.main.entry_mc:,.0f}")
        lines.append(f"Current: {cur_mult:.2f}x | Peak: {peak_mult:.2f}x")
    if c.lowcap.alerted and c.lowcap.entry_mc:
        cur_mult = mc / c.lowcap.entry_mc
        peak_mult = c.lowcap.max_mc / c.lowcap.entry_mc
        lines.append("")
        lines.append(f"🔎 Low-cap call entry MC: ${c.lowcap.entry_mc:,.0f}")
        lines.append(f"Current: {cur_mult:.2f}x | Peak: {peak_mult:.2f}x")

    lines.append("")
    lines.append(
        f"[DexScreener](https://dexscreener.com/solana/{pair.get('pairAddress', '')}) | "
        f"[pump.fun](https://pump.fun/{mint})"
    )
    return "\n".join(lines)


def send_threshold_picker(chat_id: str) -> None:
    settings = AUTHORIZED_USERS.get(chat_id, {"name": "watch", "value": SCORE_THRESHOLDS["watch"]})
    send_telegram_message(
        f"Pick your alert threshold (current: *{settings['name']}*, score ≥ {settings['value']}).",
        chat_id=chat_id,
        markup=THRESHOLD_KEYBOARD_MARKUP,
    )


def _fetch_telegram_updates(offset: int) -> list:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    params = {"timeout": 25, "offset": offset}
    resp = requests.get(url, params=params, timeout=30)
    if resp.status_code != 200:
        log.error("getUpdates failed: %s %s", resp.status_code, resp.text)
        return []
    return resp.json().get("result", [])


async def telegram_command_listener() -> None:
    if "PUT_YOUR" in TELEGRAM_BOT_TOKEN or not AUTHORIZED_USERS:
        log.warning("Telegram not configured — command listener disabled.")
        return

    next_offset = 0
    log.info("Listening for Telegram commands...")
    while True:
        try:
            updates = await asyncio.to_thread(_fetch_telegram_updates, next_offset)
            for update in updates:
                next_offset = update["update_id"] + 1

                callback = update.get("callback_query")
                if callback:
                    cb_chat_id = str(callback.get("message", {}).get("chat", {}).get("id", ""))
                    data = callback.get("data", "")
                    if not is_authorized(cb_chat_id):
                        continue

                    if data == "close":
                        message_id = callback.get("message", {}).get("message_id")
                        await asyncio.to_thread(delete_telegram_message, cb_chat_id, message_id)
                    elif data in ("thresh_watch", "thresh_high"):
                        name = "watch" if data == "thresh_watch" else "high"
                        AUTHORIZED_USERS[cb_chat_id] = {"name": name, "value": SCORE_THRESHOLDS[name]}
                        send_telegram_message(
                            f"🎯 Alert threshold set to *{name}* (score ≥ {SCORE_THRESHOLDS[name]}).",
                            chat_id=cb_chat_id,
                        )
                    elif data.startswith("coininfo_"):
                        mint = data[len("coininfo_"):]
                        text = await build_live_coin_info(mint)
                        send_telegram_message(text, chat_id=cb_chat_id, markup=CLOSE_BUTTON_MARKUP)
                    await asyncio.to_thread(answer_callback_query, callback.get("id"))
                    continue

                message = update.get("message") or update.get("edited_message") or {}
                chat_id = str(message.get("chat", {}).get("id", ""))
                text = (message.get("text") or "").strip()
                if not is_authorized(chat_id):
                    continue

                if text.startswith("/recent") or text == "🕵️ Recent":
                    send_telegram_message(build_recent_message(), chat_id=chat_id)
                elif text.startswith("/threshold") or text == "🎯 Threshold":
                    send_threshold_picker(chat_id)
                elif text.startswith("/coins") or text == "🪙 Coins":
                    send_telegram_message("🪙 *Your calls:*", chat_id=chat_id, markup=build_coins_keyboard())
                elif text.startswith("/stats") or text == "📊 Stats":
                    send_telegram_message(build_stats_message(), chat_id=chat_id)
                elif text == "🙈 Hide menu":
                    send_telegram_message("Menu hidden. Send /start to bring it back.", chat_id=chat_id, markup=HIDE_MENU_MARKUP)
                elif text.startswith("/start") or text.startswith("/help") or text == "ℹ️ Help":
                    settings = AUTHORIZED_USERS.get(chat_id, {"name": "watch", "value": SCORE_THRESHOLDS["watch"]})
                    send_telegram_message(
                        "Commands:\n"
                        "/recent — recently scanned tokens\n"
                        "/threshold — set your alert sensitivity (Watch+ / High Interest only)\n"
                        "/coins — your called coins with peak X and live stats\n"
                        "/stats — quick call performance summary\n\n"
                        f"Your current threshold: *{settings['name']}* (score ≥ {settings['value']})\n\n"
                        "⚠️ This is a scanner, not a trading bot. No purchases are ever made automatically.",
                        chat_id=chat_id,
                        markup=PERSISTENT_MENU_MARKUP,
                    )
        except Exception as e:
            log.error("Telegram command listener error: %s", e)
            await asyncio.sleep(5)


async def main() -> None:
    broadcast_to(
        AUTHORIZED_USERS.keys(),
        "✅ Solana scanner bot started.\n"
        "Watching pump.fun launches on two lanes:\n"
        "• Main: MC $10K–$250K\n"
        "• 🔎 Low-cap: MC $10K–$50K\n\n"
        "Send /recent, /threshold, /coins, or /stats anytime.\n"
        "⚠️ Scanner only — never trades automatically. Not financial advice.",
        markup=PERSISTENT_MENU_MARKUP,
    )
    await asyncio.gather(run_discovery(), scan_loop(), telegram_command_listener())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped by user.")
