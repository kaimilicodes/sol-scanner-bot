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

Every call scoring 65+ (WATCH) or 80+ (HIGH INTEREST) is sent to every
authorized user — no per-user threshold filtering.

⚠️ REALITY CHECK ⚠️
This bot does NOT predict winners. Every alert needs your own 15-second look
before you do anything with real money. Not financial advice. Buy buttons
only execute when a user taps them — nothing is automatic.

SETUP
1. pip install websockets requests solders
2. Telegram bot token (BotFather)
3. Owner chat ID (TELEGRAM_CHAT_ID) and optionally a friend's chat ID
   (FRIEND_CHAT_ID) via @userinfobot
4. A free Helius API key from helius.dev
5. Optional trading: WALLET_PRIVATE_KEY_OWNER / WALLET_PRIVATE_KEY_FRIEND
   (each person's own dedicated trading wallet — only fund with what you're
   okay losing entirely; this key cannot be revoked if it ever leaks)
6. Env vars: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, FRIEND_CHAT_ID (optional),
   HELIUS_API_KEY, WALLET_PRIVATE_KEY_OWNER (optional), WALLET_PRIVATE_KEY_FRIEND (optional)
7. Run: python main.py
"""

import asyncio
import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import base64
import requests
import websockets

try:
    from solders.keypair import Keypair
    from solders.transaction import VersionedTransaction
    from solders.pubkey import Pubkey
    SOLDERS_AVAILABLE = True
except ImportError:
    SOLDERS_AVAILABLE = False

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
ALERTED_SCAN_INTERVAL_SEC = 5

JUPITER_QUOTE_URL = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP_URL = "https://quote-api.jup.ag/v6/swap"
SOL_MINT = "So11111111111111111111111111111111111111112"
BUY_PERCENT_OPTIONS = [10, 25, 50, 100]
SLIPPAGE_BPS = 500
MAX_BUY_SOL_CAP = 1.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scanner_bot")

TARGET_MULTIPLES = [2, 5, 10, 20, 30, 40, 50]

AUTHORIZED_USERS: Set[str] = set()
if OWNER_CHAT_ID and "PUT_YOUR" not in OWNER_CHAT_ID:
    AUTHORIZED_USERS.add(str(OWNER_CHAT_ID))
if FRIEND_CHAT_ID:
    AUTHORIZED_USERS.add(str(FRIEND_CHAT_ID))


def is_authorized(chat_id: str) -> bool:
    return str(chat_id) in AUTHORIZED_USERS


WALLET_KEYPAIRS: Dict[str, "Keypair"] = {}
user_buy_size_sol: Dict[str, float] = {}

if SOLDERS_AVAILABLE:
    owner_key_str = os.environ.get("WALLET_PRIVATE_KEY_OWNER", "").strip()
    friend_key_str = os.environ.get("WALLET_PRIVATE_KEY_FRIEND", "").strip()
    if owner_key_str and str(OWNER_CHAT_ID) in AUTHORIZED_USERS:
        try:
            WALLET_KEYPAIRS[str(OWNER_CHAT_ID)] = Keypair.from_base58_string(owner_key_str)
        except Exception as e:
            log.error("Failed to load owner wallet key: %s", e)
    if friend_key_str and str(FRIEND_CHAT_ID) in AUTHORIZED_USERS:
        try:
            WALLET_KEYPAIRS[str(FRIEND_CHAT_ID)] = Keypair.from_base58_string(friend_key_str)
        except Exception as e:
            log.error("Failed to load friend wallet key: %s", e)


CLOSE_BUTTON_MARKUP = {"inline_keyboard": [[{"text": "❌ Close", "callback_data": "close"}]]}

PERSISTENT_MENU_MARKUP = {
    "keyboard": [
        [{"text": "🕵️ Recent"}, {"text": "📞 Calls"}],
        [{"text": "💰 Wallet"}, {"text": "ℹ️ Help"}],
        [{"text": "🙈 Hide menu"}],
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


def edit_telegram_message(chat_id: str, message_id: int, text: str, markup: Optional[dict] = None) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    if markup is not None:
        payload["reply_markup"] = json.dumps(markup)
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code != 200:
            log.warning("Telegram edit failed (will send new instead): %s %s", resp.status_code, resp.text)
            return False
        return True
    except requests.RequestException as e:
        log.error("Telegram edit exception: %s", e)
        return False


def build_buy_markup(mint: str) -> dict:
    buy_row = [
        {"text": f"{p}%", "callback_data": f"buy_{p}_{mint}"} for p in BUY_PERCENT_OPTIONS
    ]
    return {
        "inline_keyboard": [
            buy_row,
            [{"text": "❌ Close", "callback_data": "close"}],
        ]
    }


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


def get_sol_balance(pubkey: "Pubkey") -> Optional[float]:
    result = helius_rpc("getBalance", [str(pubkey)])
    if result is None:
        return None
    try:
        return result["value"] / 1_000_000_000
    except (KeyError, TypeError):
        return None


def fetch_jupiter_quote(output_mint: str, lamports: int) -> Optional[dict]:
    try:
        resp = requests.get(
            JUPITER_QUOTE_URL,
            params={
                "inputMint": SOL_MINT,
                "outputMint": output_mint,
                "amount": lamports,
                "slippageBps": SLIPPAGE_BPS,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            log.error("Jupiter quote failed: %s %s", resp.status_code, resp.text)
            return None
        return resp.json()
    except requests.RequestException as e:
        log.error("Jupiter quote exception: %s", e)
        return None


def fetch_jupiter_swap_tx(quote: dict, user_pubkey: str) -> Optional[str]:
    try:
        resp = requests.post(
            JUPITER_SWAP_URL,
            json={
                "quoteResponse": quote,
                "userPublicKey": user_pubkey,
                "wrapAndUnwrapSol": True,
                "prioritizationFeeLamports": "auto",
            },
            timeout=15,
        )
        if resp.status_code != 200:
            log.error("Jupiter swap build failed: %s %s", resp.status_code, resp.text)
            return None
        return resp.json().get("swapTransaction")
    except requests.RequestException as e:
        log.error("Jupiter swap build exception: %s", e)
        return None


def sign_and_send_transaction(swap_tx_b64: str, keypair: "Keypair") -> Optional[str]:
    try:
        raw_bytes = base64.b64decode(swap_tx_b64)
        unsigned_tx = VersionedTransaction.from_bytes(raw_bytes)
        signed_tx = VersionedTransaction(unsigned_tx.message, [keypair])
        signed_bytes = bytes(signed_tx)
        signed_b64 = base64.b64encode(signed_bytes).decode("utf-8")

        result = helius_rpc("sendTransaction", [
            signed_b64,
            {"encoding": "base64", "skipPreflight": False, "maxRetries": 3},
        ])
        return result
    except Exception as e:
        log.error("Sign/send transaction failed: %s", e)
        return None


async def execute_buy(chat_id: str, mint: str, symbol: str, pct: int) -> str:
    if not SOLDERS_AVAILABLE:
        return "⚠️ Trading isn't available — the 'solders' package isn't installed."

    keypair = WALLET_KEYPAIRS.get(chat_id)
    if not keypair:
        return "⚠️ No wallet configured for your account. Ask to have your wallet key added."

    buy_size = user_buy_size_sol.get(chat_id, 0)
    if buy_size <= 0:
        return "⚠️ Set your buy size first, e.g. `/setbuysize 0.1` (SOL)."

    amount_sol = min(buy_size * pct / 100, MAX_BUY_SOL_CAP)
    lamports = int(amount_sol * 1_000_000_000)

    quote = await asyncio.to_thread(fetch_jupiter_quote, mint, lamports)
    if not quote:
        return f"❌ Couldn't get a swap quote for ${symbol}. It may have too little liquidity right now."

    swap_tx_b64 = await asyncio.to_thread(fetch_jupiter_swap_tx, quote, str(keypair.pubkey()))
    if not swap_tx_b64:
        return f"❌ Couldn't build the swap transaction for ${symbol}."

    signature = await asyncio.to_thread(sign_and_send_transaction, swap_tx_b64, keypair)
    if not signature:
        return f"❌ Transaction failed to send for ${symbol}. Nothing was spent."

    return (
        f"✅ Buy sent: {amount_sol:.4f} SOL → ${symbol} ({pct}%)\n"
        f"[View on Solscan](https://solscan.io/tx/{signature})\n\n"
        f"Confirmation can take a few seconds — check the link above."
    )


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
    for chat_id in AUTHORIZED_USERS:
        if chat_id in lane.alerted_for:
            continue
        text = build_alert_text(c, pair, score_pct, tier, top_holders, supply, lane_label=lane_label)
        send_telegram_message(text, chat_id=chat_id, markup=build_buy_markup(c.mint))
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

        to_check = [
            c for c in candidates.values()
            if now - c.last_checked >= (ALERTED_SCAN_INTERVAL_SEC if c.any_alerted else SCAN_INTERVAL_SEC)
        ]
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


calls_board_message_id: Dict[str, int] = {}


def build_calls_board_text() -> str:
    called = [c for c in candidates.values() if c.any_alerted]
    if not called:
        return "📞 *Calls Board*\n\nNo calls yet — still watching."

    def peak_multiple(c: Candidate) -> float:
        m1 = (c.main.max_mc / c.main.entry_mc) if c.main.alerted and c.main.entry_mc else 0
        m2 = (c.lowcap.max_mc / c.lowcap.entry_mc) if c.lowcap.alerted and c.lowcap.entry_mc else 0
        return max(m1, m2)

    def is_lowcap_best(c: Candidate) -> bool:
        m1 = (c.main.max_mc / c.main.entry_mc) if c.main.alerted and c.main.entry_mc else 0
        m2 = (c.lowcap.max_mc / c.lowcap.entry_mc) if c.lowcap.alerted and c.lowcap.entry_mc else 0
        return m2 > m1

    ranked = sorted(called, key=peak_multiple, reverse=True)
    peaks = [peak_multiple(c) for c in ranked]

    lines = ["📞 *Calls Board*", ""]
    for i, c in enumerate(ranked[:20], start=1):
        mult = peak_multiple(c)
        prefix = "🔎 " if is_lowcap_best(c) else ""
        lines.append(f"{i}. {prefix}${c.symbol} — [{mult:.1f}x]")

    hits = sum(1 for p in peaks if p >= 2.0)
    hit_rate = (hits / len(peaks) * 100) if peaks else 0
    sorted_peaks = sorted(peaks)
    n = len(sorted_peaks)
    median = sorted_peaks[n // 2] if n % 2 == 1 else (sorted_peaks[n // 2 - 1] + sorted_peaks[n // 2]) / 2 if n else 0
    total_return = sum(peaks)
    avg_return = total_return / len(peaks) if peaks else 0

    lines += [
        "",
        "📊 *Stats*",
        f"Calls: {len(peaks)}",
        f"Hit Rate (≥2x): {hit_rate:.1f}%",
        f"Median: {median:.1f}x",
        f"Return: {total_return:.1f}x (Avg: {avg_return:.1f}x)",
        "",
        "_Hit rate = reached at least 2x from call price._",
    ]
    return "\n".join(lines)


CALLS_BOARD_REFRESH_MARKUP = {
    "inline_keyboard": [[{"text": "🔄 Refresh", "callback_data": "refresh_board"}]]
}


def send_or_update_calls_board(chat_id: str) -> None:
    text = build_calls_board_text()
    existing_id = calls_board_message_id.get(chat_id)
    if existing_id:
        ok = edit_telegram_message(chat_id, existing_id, text, markup=CALLS_BOARD_REFRESH_MARKUP)
        if ok:
            return
    new_id = send_telegram_message(text, chat_id=chat_id, markup=CALLS_BOARD_REFRESH_MARKUP)
    if new_id:
        calls_board_message_id[chat_id] = new_id


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
                    elif data == "refresh_board":
                        send_or_update_calls_board(cb_chat_id)
                    elif data.startswith("buy_"):
                        _, pct_str, mint = data.split("_", 2)
                        pct = int(pct_str)
                        c = candidates.get(mint)
                        symbol = c.symbol if c else mint[:6]
                        result_text = await execute_buy(cb_chat_id, mint, symbol, pct)
                        send_telegram_message(result_text, chat_id=cb_chat_id, markup=CLOSE_BUTTON_MARKUP)
                    await asyncio.to_thread(answer_callback_query, callback.get("id"))
                    continue

                message = update.get("message") or update.get("edited_message") or {}
                chat_id = str(message.get("chat", {}).get("id", ""))
                text = (message.get("text") or "").strip()
                if not is_authorized(chat_id):
                    continue

                if text.startswith("/recent") or text == "🕵️ Recent":
                    send_telegram_message(build_recent_message(), chat_id=chat_id)
                elif text.startswith("/calls") or text == "📞 Calls":
                    send_or_update_calls_board(chat_id)
                elif text.startswith("/setbuysize"):
                    parts = text.split()
                    if len(parts) < 2:
                        send_telegram_message("Usage: `/setbuysize 0.1` (amount in SOL)", chat_id=chat_id)
                    else:
                        try:
                            amount = float(parts[1])
                            if amount <= 0 or amount > MAX_BUY_SOL_CAP:
                                send_telegram_message(
                                    f"Please choose an amount between 0 and {MAX_BUY_SOL_CAP} SOL.",
                                    chat_id=chat_id,
                                )
                            else:
                                user_buy_size_sol[chat_id] = amount
                                send_telegram_message(
                                    f"✅ Buy size set to {amount} SOL. Buy buttons will use % of this.",
                                    chat_id=chat_id,
                                )
                        except ValueError:
                            send_telegram_message("That doesn't look like a number. Example: `/setbuysize 0.1`", chat_id=chat_id)
                elif text.startswith("/wallet") or text == "💰 Wallet":
                    keypair = WALLET_KEYPAIRS.get(chat_id)
                    if not SOLDERS_AVAILABLE:
                        send_telegram_message("⚠️ Trading isn't available (solders package not installed).", chat_id=chat_id)
                    elif not keypair:
                        send_telegram_message(
                            "⚠️ No wallet connected to your account yet.\n"
                            "Use a dedicated wallet with only funds you're okay risking.",
                            chat_id=chat_id,
                        )
                    else:
                        balance = await asyncio.to_thread(get_sol_balance, keypair.pubkey())
                        bal_text = f"{balance:.4f} SOL" if balance is not None else "unknown (couldn't fetch)"
                        buy_size = user_buy_size_sol.get(chat_id, 0)
                        send_telegram_message(
                            f"💰 *Your wallet*\n"
                            f"Address: `{keypair.pubkey()}`\n"
                            f"Balance: {bal_text}\n"
                            f"Buy size: {buy_size if buy_size else 'not set'} SOL "
                            f"(set with `/setbuysize <amount>`)",
                            chat_id=chat_id,
                        )
                elif text == "🙈 Hide menu":
                    send_telegram_message("Menu hidden. Send /start to bring it back.", chat_id=chat_id, markup=HIDE_MENU_MARKUP)
                elif text.startswith("/start") or text.startswith("/help") or text == "ℹ️ Help":
                    send_telegram_message(
                        "Commands:\n"
                        "/recent — recently scanned tokens\n"
                        "/calls — live-updating leaderboard of all calls\n"
                        "/wallet — your connected wallet + balance\n"
                        "/setbuysize <SOL> — set your buy-button base amount\n\n"
                        "Every call scoring 65+ (WATCH) or 80+ (HIGH INTEREST) is sent to "
                        "everyone — each alert is labeled with which tier it hit.\n\n"
                        "⚠️ This is a scanner. Buy buttons only execute when YOU tap them — nothing is automatic.",
                        chat_id=chat_id,
                        markup=PERSISTENT_MENU_MARKUP,
                    )
        except Exception as e:
            log.error("Telegram command listener error: %s", e)
            await asyncio.sleep(5)


async def main() -> None:
    broadcast_to(
        AUTHORIZED_USERS,
        "✅ Solana scanner bot started.\n"
        "Watching pump.fun launches on two lanes:\n"
        "• Main: MC $10K–$250K\n"
        "• 🔎 Low-cap: MC $10K–$50K\n\n"
        "Every call scoring 65+ (WATCH) or 80+ (HIGH INTEREST) goes to everyone — no threshold filtering.\n"
        "Send /recent, /calls, or /wallet anytime.\n"
        "⚠️ Scanner only — never trades automatically. Not financial advice.",
        markup=PERSISTENT_MENU_MARKUP,
    )
    await asyncio.gather(run_discovery(), scan_loop(), telegram_command_listener())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped by user.")
