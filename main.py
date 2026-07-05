"""
HyperBot AI — Backend Python FastAPI
Serveur principal avec authentification, API et moteur de scan
"""

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional, List
import sqlite3
import hashlib
import secrets
import json
import asyncio
import httpx
import time
import os
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

# ── BASE DE DONNÉES ──────────────────────────────────────────
DB_PATH = "/data/hyperbot.db"

bot_logs_memory = {}  # user_id -> list of log entries (max 50)
rsi_history = {}  # user_id -> {coin: last_rsi} for confirmation tracking

def add_bot_log(user_id: int, message: str, level: str = "info"):
    if user_id not in bot_logs_memory:
        bot_logs_memory[user_id] = []
    import datetime
    entry = {
        "time": datetime.datetime.utcnow().strftime("%H:%M:%S"),
        "message": message,
        "level": level
    }
    bot_logs_memory[user_id].insert(0, entry)
    if len(bot_logs_memory[user_id]) > 50:
        bot_logs_memory[user_id] = bot_logs_memory[user_id][:50]
    print(message)
    # Sauvegarde persistante en DB (garder 500 derniers logs)
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO bot_activity_log (user_id, level, message) VALUES (?,?,?)",
            (user_id, level, message)
        )
        # Nettoyer les vieux logs au-dela de 500
        conn.execute("""DELETE FROM bot_activity_log WHERE user_id=? AND id NOT IN (
            SELECT id FROM bot_activity_log WHERE user_id=? ORDER BY id DESC LIMIT 500
        )""", (user_id, user_id))
        conn.commit()
        conn.close()
    except:
        pass

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    # Migration: ajouter les nouvelles colonnes si elles n'existent pas
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN trading_mode TEXT DEFAULT 'paper'")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN max_position_usdc REAL DEFAULT 50.0")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN max_open_trades INTEGER DEFAULT 5")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE paper_portfolio ADD COLUMN initial_balance REAL DEFAULT 1000.0")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE paper_portfolio ADD COLUMN reset_at TEXT")
        conn.commit()
    except: pass
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS trading_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            session_date TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            closing_phase INTEGER DEFAULT 0,
            total_trades INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            net_pnl REAL DEFAULT 0,
            capital_start REAL DEFAULT 1000.0,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )""")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE paper_trades ADD COLUMN tp1_hit INTEGER DEFAULT 0")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE paper_trades ADD COLUMN trailing_sl REAL")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE paper_trades ADD COLUMN highest_price REAL")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE paper_trades ADD COLUMN lowest_price REAL")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE paper_trades ADD COLUMN peak_pnl REAL DEFAULT 0")
        conn.commit()
    except: pass
    # Ne pas forcer les coins — conserver le dernier état connu
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN ai_continuous INTEGER DEFAULT 0")
        conn.commit()
    except: pass
    try:
        conn.execute("UPDATE bot_config SET position_pct=5.0 WHERE position_pct=8.0 OR position_pct IS NULL")
        conn.commit()
    except: pass
    try:
        conn.execute("UPDATE bot_config SET filter_weekend=0")
        conn.commit()
    except: pass
    try:
        conn.execute("UPDATE bot_config SET max_loss_usd=0.75 WHERE max_loss_usd=0.5 OR max_loss_usd IS NULL")
        conn.commit()
    except: pass
    try:
        conn.execute("UPDATE bot_config SET quick_profit_usd=1.1 WHERE quick_profit_usd=1.0 OR quick_profit_usd IS NULL")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN filter_hours INTEGER DEFAULT 1")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN position_pct REAL DEFAULT 5.0")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN quick_profit_usd REAL DEFAULT 1.1")
        conn.commit()
    except: pass
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS coin_confidence (
            user_id INTEGER,
            coin TEXT,
            action TEXT,
            consecutive_losses INTEGER DEFAULT 0,
            updated_at TEXT,
            PRIMARY KEY (user_id, coin, action)
        )""")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN max_loss_usd REAL DEFAULT 0.5")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE users ADD COLUMN finnhub_key TEXT")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE users ADD COLUMN hl_api_key TEXT DEFAULT ''")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE users ADD COLUMN hl_wallet TEXT DEFAULT ''")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN filter_weekend INTEGER DEFAULT 0")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN filter_macro INTEGER DEFAULT 0")
        conn.commit()
    except: pass
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            wallet TEXT DEFAULT '',
            api_key TEXT DEFAULT '',
            finnhub_key TEXT DEFAULT '',
            hl_api_key TEXT DEFAULT '',
            hl_wallet TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            expires_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            coin TEXT NOT NULL,
            action TEXT NOT NULL,
            confidence INTEGER NOT NULL,
            entry REAL,
            stop_loss REAL,
            take_profit1 REAL,
            take_profit2 REAL,
            leverage INTEGER,
            position_size INTEGER,
            risk_reward REAL,
            timeframe TEXT,
            reasoning TEXT,
            key_signals TEXT,
            price REAL,
            rsi REAL,
            atr REAL,
            vwap REAL,
            status TEXT DEFAULT 'ACTIVE',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS bot_config (
            user_id INTEGER PRIMARY KEY,
            active_coins TEXT DEFAULT '["HYPE","SOL","INJ"]',
            is_running INTEGER DEFAULT 0,
            trading_mode TEXT DEFAULT 'paper',
            max_position_usdc REAL DEFAULT 50.0,
            position_pct REAL DEFAULT 5.0,
            quick_profit_usd REAL DEFAULT 1.1,
            max_loss_usd REAL DEFAULT 0.75,
            max_open_trades INTEGER DEFAULT 5,
            last_scan TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS prices (
            coin TEXT PRIMARY KEY,
            price REAL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS paper_portfolio (
            user_id INTEGER PRIMARY KEY,
            balance REAL DEFAULT 1000.0,
            initial_balance REAL DEFAULT 1000.0,
            reset_at TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS bot_activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            level TEXT DEFAULT 'info',
            message TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            coin TEXT NOT NULL,
            action TEXT NOT NULL,
            entry_price REAL NOT NULL,
            current_price REAL,
            size_usdc REAL NOT NULL,
            leverage INTEGER DEFAULT 1,
            stop_loss REAL,
            take_profit1 REAL,
            take_profit2 REAL,
            pnl REAL DEFAULT 0,
            pnl_pct REAL DEFAULT 0,
            status TEXT DEFAULT 'OPEN',
            signal_id INTEGER,
            opened_at TEXT DEFAULT CURRENT_TIMESTAMP,
            closed_at TEXT,
            close_reason TEXT,
            tp1_hit INTEGER DEFAULT 0,
            trailing_sl REAL,
            highest_price REAL,
            lowest_price REAL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    """)
    conn.commit()
    conn.close()

# ── AUTHENTIFICATION ─────────────────────────────────────────
def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def create_token() -> str:
    return secrets.token_urlsafe(32)

def verify_token(token: str) -> Optional[int]:
    conn = get_db()
    row = conn.execute(
        "SELECT user_id FROM sessions WHERE token=? AND expires_at>?",
        (token, datetime.utcnow().isoformat())
    ).fetchone()
    conn.close()
    return row["user_id"] if row else None

security = HTTPBearer()

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    user_id = verify_token(credentials.credentials)
    if not user_id:
        raise HTTPException(status_code=401, detail="Session expirée, veuillez vous reconnecter")
    return user_id

# ── MOTEUR D'ANALYSE TECHNIQUE ───────────────────────────────
def calc_ema(prices, period):
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    emas = [sum(prices[:period]) / period]
    for i in range(period, len(prices)):
        emas.append(prices[i] * k + emas[-1] * (1 - k))
    return emas

def calc_rsi(prices, period=14):
    if len(prices) < period + 1:
        return None
    gains, losses = 0, 0
    for i in range(1, period + 1):
        d = prices[i] - prices[i-1]
        if d > 0: gains += d
        else: losses -= d
    ag, al = gains/period, losses/period
    for i in range(period+1, len(prices)):
        d = prices[i] - prices[i-1]
        ag = (ag*(period-1) + max(d,0)) / period
        al = (al*(period-1) + max(-d,0)) / period
    if al == 0: return 100
    return 100 - 100/(1 + ag/al)

def calc_macd(prices):
    e12 = calc_ema(prices, 12)
    e26 = calc_ema(prices, 26)
    if not e12 or not e26:
        return None
    off = len(e12) - len(e26)
    macd_line = [e12[i+off] - v for i, v in enumerate(e26)]
    sig = calc_ema(macd_line, 9)
    if not sig:
        return None
    off_m = len(macd_line) - len(sig)
    return {
        "macd": macd_line[-1],
        "signal": sig[-1],
        "histogram": macd_line[-1] - sig[-1],
        "crossBull": macd_line[-1] > sig[-1] and macd_line[-2+off_m] <= sig[-2],
        "crossBear": macd_line[-1] < sig[-1] and macd_line[-2+off_m] >= sig[-2],
    }

def calc_bb(prices, period=20, mult=2):
    if len(prices) < period:
        return None
    sl = prices[-period:]
    mean = sum(sl) / period
    std = (sum((x-mean)**2 for x in sl) / period) ** 0.5
    return {"upper": mean+mult*std, "middle": mean, "lower": mean-mult*std}

def calc_atr(candles, period=14):
    if len(candles) < period+1:
        return None
    trs = [max(c["h"]-c["l"], abs(c["h"]-candles[i]["c"]), abs(c["l"]-candles[i]["c"]))
           for i, c in enumerate(candles[1:])]
    return sum(trs[-period:]) / period

def calc_vwap(candles):
    tv, v = 0, 0
    for c in candles:
        tp = (c["h"]+c["l"]+c["c"]) / 3
        tv += tp * c["v"]
        v += c["v"]
    return tv/v if v else None

# ── HYPERLIQUID API ──────────────────────────────────────────
HL_BASE = "https://api.hyperliquid.xyz"

async def hl_post(client, endpoint, payload):
    try:
        r = await client.post(f"{HL_BASE}{endpoint}", json=payload, timeout=10)
        return r.json()
    except:
        return None

async def fetch_all_metas(client):
    data = await hl_post(client, "/info", {"type": "metaAndAssetCtxs"})
    if not data:
        return {}
    meta, ctxs = data
    prices = {}
    for i, asset in enumerate(meta["universe"]):
        if ctxs[i].get("markPx"):
            prices[asset["name"]] = float(ctxs[i]["markPx"])
    return prices

async def fetch_candles(client, coin, interval="15m", count=200):
    now = int(time.time() * 1000)
    ms = {"1m":60000,"5m":300000,"15m":900000,"1h":3600000}.get(interval, 900000)
    data = await hl_post(client, "/info", {
        "type": "candleSnapshot",
        "req": {"coin": coin, "interval": interval, "startTime": now - ms*count, "endTime": now}
    })
    return data or []

async def fetch_positions(client, address):
    if not address:
        return []
    data = await hl_post(client, "/info", {"type": "clearinghouseState", "user": address})
    return data.get("assetPositions", []) if data else []

# ── ANALYSE IA ───────────────────────────────────────────────
def cache_market_data(coin: str, tech: dict, price: float):
    """Sauvegarde les indicateurs calculés dans le cache mémoire"""
    from datetime import datetime as dt
    market_data_cache[coin] = {
        "price": round(price, 6),
        "rsi": round(tech.get("rsi", 0), 2),
        "macd_bull": tech.get("macd_bull", False),
        "macd_bear": tech.get("macd_bear", False),
        "ema20": round(tech.get("ema20", 0), 4),
        "ema50": round(tech.get("ema50", 0), 4),
        "ema200": round(tech.get("ema200", 0), 4),
        "bb_upper": round(tech.get("bb_upper", 0), 4),
        "bb_lower": round(tech.get("bb_lower", 0), 4),
        "bb_mid": round(tech.get("bb_mid", 0), 4),
        "atr": round(tech.get("atr", 0), 4),
        "vwap": round(tech.get("vwap", 0), 4),
        "volume_trend": tech.get("volume_trend", "N/A"),
        "btc_trend": tech.get("btc_trend", "neutral"),
        "btc_change": round(tech.get("btc_change", 0), 2),
        "updated_at": dt.utcnow().strftime("%H:%M:%S")
    }

def get_compact_prompt(coin: str, tech: dict, price: float) -> str:
    """Génère un prompt compact depuis le cache — moins de tokens"""
    d = market_data_cache.get(coin, {})
    if not d:
        cache_market_data(coin, tech, price)
        d = market_data_cache[coin]
    
    # Déterminer position relative au BB
    bb_pos = "MIDDLE"
    if d["price"] > d["bb_upper"]: bb_pos = "ABOVE_UPPER"
    elif d["price"] < d["bb_lower"]: bb_pos = "BELOW_LOWER"
    elif d["price"] > d["bb_mid"]: bb_pos = "UPPER_HALF"
    else: bb_pos = "LOWER_HALF"
    
    # Alignement EMA
    ema_align = "BULL" if d["ema20"] > d["ema50"] > d["ema200"] else                 "BEAR" if d["ema20"] < d["ema50"] < d["ema200"] else "MIXED"
    
    return f"""Crypto scalp analyst. Analyze {coin}/USDC.
DATA: price={d['price']} rsi={d['rsi']} macd={'BULL' if d['macd_bull'] else 'BEAR' if d['macd_bear'] else 'NEUTRAL'} ema={ema_align} bb={bb_pos} vol={d['volume_trend']} vwap={d['vwap']} atr={d['atr']} btc={d['btc_trend']}({d['btc_change']}%)
RULES: RSI<25=LONG RSI>75=SHORT RSI25-45=LONG_BIAS RSI55-75=SHORT_BIAS min_RR=2 leverage=2-5 size=5-15%
Respond ONLY JSON: {{"action":"LONG"|"SHORT"|"WAIT","confidence":0-100,"entry":number,"stopLoss":number,"takeProfit1":number,"takeProfit2":number,"leverage":2-5,"positionSize":5-15,"reasoning":"2 phrases FR","keySignals":["s1","s2","s3"],"riskReward":number,"timeframe":"court-terme"|"moyen-terme"}}"""

async def analyze_with_ai(client, coin, tech, ob, price, api_key):
    # Cacher les données et utiliser prompt compact — 60-70% moins de tokens
    cache_market_data(coin, tech, price)
    prompt = get_compact_prompt(coin, tech, price)

    try:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
            json={"model": "claude-sonnet-4-6", "max_tokens": 1000, "messages": [{"role": "user", "content": prompt}]},
            timeout=30
        )
        if r.status_code != 200:
            print(f"Anthropic API erreur {r.status_code}: {r.text[:200]}")
            return None
        data = r.json()
        if "error" in data:
            print(f"Anthropic API erreur: {data['error']}")
            return None
        text = "".join(b.get("text","") for b in data.get("content",[]))
        clean = text.replace("```json","").replace("```","").strip()
        return json.loads(clean)
    except json.JSONDecodeError as e:
        print(f"JSON parse erreur pour {coin}: {e} | text={text[:100] if 'text' in dir() else 'N/A'}")
        return None
    except Exception as e:
        print(f"Anthropic appel erreur pour {coin}: {type(e).__name__}: {e}")
        return None

# ── MOTEUR DE SCAN ───────────────────────────────────────────
scanning_tasks = {}   # user_id -> asyncio Task (scan IA)
positions_tasks = {}  # user_id -> asyncio Task (suivi positions)

async def scan_markets(user_id: int):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    config = conn.execute("SELECT * FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
    conn.close()

    if not user or not config:
        return

    active_coins = json.loads(config["active_coins"])
    api_key = user["api_key"]

    # Reset auto filtre macro si annonce passée
    await auto_reset_macro_filter(user_id)
    
    # Gestion cycle de vie session
    await check_session_lifecycle(user_id)
    if await is_session_closing(user_id):
        add_bot_log(user_id, "🔒 Session en clôture — nouveaux trades bloqués", "info")
        return

    # === LECTURE FILTRES ===
    filter_hours = config["filter_hours"] if config and "filter_hours" in config.keys() else 1
    filter_weekend = config["filter_weekend"] if config and "filter_weekend" in config.keys() else 1
    filter_macro = config["filter_macro"] if config and "filter_macro" in config.keys() else 0

    # === CALENDRIER MACRO FINNHUB ===
    finnhub_key = user["finnhub_key"] if user and "finnhub_key" in user.keys() else None
    if finnhub_key and not filter_macro:
        macro_data = await check_macro_calendar(user_id, finnhub_key)
        for event in macro_data.get("events", []):
            hours = event["hours_left"]
            name = event["event"]
            if hours <= 0:
                add_bot_log(user_id, f"📰 {name} vient d'être publié — volatilité possible", "warning")
            elif hours <= 2:
                # Auto-activer le filtre macro
                conn_m = get_db()
                conn_m.execute("UPDATE bot_config SET filter_macro=1 WHERE user_id=?", (user_id,))
                conn_m.commit()
                conn_m.close()
                add_bot_log(user_id, f"🔴 AUTO-PAUSE: {name} dans {hours}h — filtre macro activé automatiquement", "error")
                return
            elif hours <= 24:
                add_bot_log(user_id, f"⚠️ MACRO ALERT: {name} dans {round(hours)}h — préparez-vous", "warning")
    
    # === FILTRES TEMPORELS ===
    from datetime import datetime as dt
    now_utc = dt.utcnow()
    hour_utc = now_utc.hour
    weekday = now_utc.weekday()  # 0=lundi, 5=samedi, 6=dimanche

    if filter_hours and 21 <= hour_utc < 23:
        add_bot_log(user_id, f"🌙 Session creuse ({hour_utc}h UTC) — pas de nouveaux trades", "info")
        return

    if filter_weekend and weekday >= 5:
        day_name = "Samedi" if weekday == 5 else "Dimanche"
        add_bot_log(user_id, f"📅 {day_name} — trading suspendu (week-end)", "info")
        return

    if filter_macro:
        add_bot_log(user_id, f"⚠️ Pause macro activée manuellement — pas de nouveaux trades", "warning")
        return

    async with httpx.AsyncClient() as client:
        # Fetch prices
        prices = await fetch_all_metas(client)

        # Update prices en DB uniquement pour les coins actifs
        conn = get_db()
        for coin in active_coins:
            if coin in prices:
                conn.execute("INSERT OR REPLACE INTO prices (coin, price, updated_at) VALUES (?,?,?)",
                            (coin, prices[coin], datetime.utcnow().isoformat()))
        conn.commit()
        conn.close()

        # Tendance BTC globale sur 4h
        btc_trend = "neutral"
        btc_change = 0
        btc_candles_4h = await fetch_candles(client, "BTC", "1h", 8)
        if btc_candles_4h and len(btc_candles_4h) >= 4:
            btc_open = float(btc_candles_4h[0]["c"])
            btc_close = float(btc_candles_4h[-1]["c"])
            btc_change = (btc_close - btc_open) / btc_open * 100
            if btc_change > 2.0:
                btc_trend = "bullish"
                add_bot_log(user_id, f"🟢 BTC HAUSSIER (+{btc_change:.1f}%) - mode tendance LONG actif", "success")
            elif btc_change < -2.0:
                btc_trend = "bearish"
                add_bot_log(user_id, f"🔴 BTC BAISSIER ({btc_change:.1f}%) - mode tendance SHORT actif", "error")
            else:
                add_bot_log(user_id, f"⚪ BTC NEUTRE ({btc_change:.1f}%) - mode retournement actif", "info")

        # Analyze each coin
        # Coins opportunistes (75%+) = tous les 30 coins disponibles
        all_available_coins = ["BTC","ETH","SOL","ARB","AVAX","LINK","OP","INJ","TIA","BNB","HYPE","PAXG","TAO","WIF","JUP","PENDLE","EIGEN","RENDER","SUI","APT","SEI","DOGE","XRP","NEAR","FTM","AAVE","UNI","CRV","SUSHI","GMX"]
        opportunist_coins = [c for c in all_available_coins if c not in active_coins]
        
        # Scanner d'abord les coins actifs, puis les opportunistes
        coins_to_scan = active_coins + opportunist_coins

        for coin in coins_to_scan:
            is_opportunist = coin not in active_coins
            if coin not in prices:
                continue

            price = prices[coin]
            candles_raw = await fetch_candles(client, coin)
            
            # Pré-filtre RSI pour les coins opportunistes — économise les crédits IA
            if is_opportunist and candles_raw:
                closes = [float(c[4]) for c in candles_raw[-15:] if len(c) > 4]
                if len(closes) >= 14:
                    gains = [max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
                    losses = [max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
                    avg_gain = sum(gains[-14:])/14
                    avg_loss = sum(losses[-14:])/14
                    rsi_quick = 100-(100/(1+avg_gain/max(avg_loss,0.0001)))
                    # Appeler l'IA seulement si RSI en zone extrême (<30 ou >70)
                    if 30 <= rsi_quick <= 70:
                        continue  # Zone neutre — pas d'opportunité
            if not candles_raw or len(candles_raw) < 50:
                continue

            candles = [{"h":float(cd["h"]),"l":float(cd["l"]),"c":float(cd["c"]),"v":float(cd["v"])} for cd in candles_raw]
            closes = [cd["c"] for cd in candles]
            vols = [cd["v"] for cd in candles]

            e20 = calc_ema(closes, 20)
            e50 = calc_ema(closes, 50)
            e200 = calc_ema(closes, 200)
            macd = calc_macd(closes)
            bb = calc_bb(closes)
            atr = calc_atr(candles)
            vwap = calc_vwap(candles)
            rsi = calc_rsi(closes)
            vol_avg = sum(vols[-20:]) / 20
            vol_cur = vols[-1]

            tech = {
                "rsi": round(rsi, 2) if rsi else None,
                "macd_bull": macd["crossBull"] if macd else False,
                "macd_bear": macd["crossBear"] if macd else False,
                "ema20": round(e20[-1], 4) if e20 else None,
                "ema50": round(e50[-1], 4) if e50 else None,
                "ema200": round(e200[-1], 4) if e200 else None,
                "bb_upper": round(bb["upper"], 4) if bb else None,
                "bb_lower": round(bb["lower"], 4) if bb else None,
                "atr": round(atr, 4) if atr else None,
                "vwap": round(vwap, 4) if vwap else None,
                "volume_trend": "SPIKE" if vol_cur > vol_avg*1.5 else "ABOVE_AVG" if vol_cur > vol_avg else "BELOW_AVG",
                "btc_trend": btc_trend,
                "btc_change": btc_change,
            }

            # Pre-filter
            has_signal = (rsi and (rsi < 35 or rsi > 65)) or (macd and (macd["crossBull"] or macd["crossBear"])) or vol_cur > vol_avg*1.5 or True

            if not has_signal or not api_key:
                continue

            # Filtre RSI — eviter de shorter en survente ou longer en surachat
            rsi_val = tech.get("rsi")
            if rsi_val:
                # Info seulement - l'IA decide du timing
                if rsi_val < 30:
                    add_bot_log(user_id, f"📊 {coin}: RSI {rsi_val:.1f} - survente (IA juge)", "info")
                elif rsi_val > 70:
                    add_bot_log(user_id, f"📊 {coin}: RSI {rsi_val:.1f} - surachat (IA juge)", "info")

            # Un seul signal par coin par scan (5 min)
            conn_check = get_db()
            recent = conn_check.execute(
                "SELECT id FROM signals WHERE user_id=? AND coin=? AND created_at > datetime('now', '-5 minutes')",
                (user_id, coin)
            ).fetchone()
            last_action = conn_check.execute(
                "SELECT action FROM signals WHERE user_id=? AND coin=? AND created_at > datetime('now', '-30 minutes') ORDER BY created_at DESC LIMIT 1",
                (user_id, coin)
            ).fetchone()
            conn_check.close()
            if recent:
                add_bot_log(user_id, f"🔄 {coin}: Signal récent, ignoré", "info")
                continue

            # Verifier si coin deja en position ouverte
            conn_check = get_db()
            coin_already_open = conn_check.execute(
                "SELECT id FROM paper_trades WHERE user_id=? AND coin=? AND status='OPEN'",
                (user_id, coin)
            ).fetchone()
            ai_continuous = config["ai_continuous"] if config and "ai_continuous" in config.keys() else 0
            conn_check.close()

            if coin_already_open and not ai_continuous:
                add_bot_log(user_id, f"💰 {coin}: Position déjà ouverte - analyse IA skippée", "info")
                continue
            
            # Skip les coins opportunistes si confiance pas encore connue
            # (on les analyse quand même mais on filtre après)

            ai = await analyze_with_ai(client, coin, tech, None, price, api_key)
            if not ai:
                add_bot_log(user_id, f"⚠️ {coin}: Pas de réponse IA", "warning")
                continue
            action_ia = ai.get("action", "WAIT")
            confidence_ia = ai.get("confidence", 0)
            if coin_already_open and ai_continuous:
                add_bot_log(user_id, f"💡 {coin} (déjà ouvert): IA → {action_ia} ({confidence_ia}%) — info seulement", "info")
                continue
            cache_market_data(coin, tech, price)  # Mettre à jour le cache
            add_bot_log(user_id, f"🤖 {coin}: IA → {action_ia} ({confidence_ia}%) RSI={tech.get('rsi','?')}", "info" if action_ia=="WAIT" else "success")
            required_conf = get_required_confidence(user_id, coin, action_ia)
            # Pour les coins opportunistes, seuil minimum 75%
            opportunist_threshold = 75
            if is_opportunist:
                if action_ia == "WAIT" or confidence_ia < opportunist_threshold:
                    continue  # Skip silencieux pour les opportunistes
                add_bot_log(user_id, f"🎯 {coin}: Trade opportuniste ({confidence_ia}%) hors sélection !", "success")
            elif action_ia == "WAIT" or confidence_ia < required_conf:
                add_bot_log(user_id, f"⛔ {coin}: Confiance insuffisante ({confidence_ia}% < {required_conf}%) — ignoré", "info")
                continue

            rsi_now = tech.get("rsi") or 50
            action = ai.get("action")

            # === MODE TENDANCE HAUSSIERE (BTC +2%) ===
            if btc_trend == "bullish" and coin != "PAXG":
                # En tendance haussiere: chercher LONG sur pullbacks
                if action == "SHORT":
                    add_bot_log(user_id, f"↩️ {coin}: SHORT ignoré - marché haussier", "info")
                    continue
                if action == "LONG" and rsi_now > 75:
                    add_bot_log(user_id, f"📊 {coin}: RSI {rsi_now:.1f} haut en tendance - IA juge", "info")
                    continue
                # Autoriser LONG meme avec RSI entre 50-75 en tendance haussiere
                add_bot_log(user_id, f"✅ {coin}: LONG autorisé - tendance haussière (RSI {rsi_now:.1f})", "success")

            # === MODE TENDANCE BAISSIERE (BTC -2%) ===
            elif btc_trend == "bearish" and coin != "PAXG":
                # En tendance baissiere: chercher SHORT sur rebonds
                if action == "LONG":
                    add_bot_log(user_id, f"↩️ {coin}: LONG ignoré - marché baissier", "info")
                    continue
                if action == "SHORT" and rsi_now < 25:
                    add_bot_log(user_id, f"📊 {coin}: RSI {rsi_now:.1f} bas en tendance - IA juge", "info")
                    continue
                add_bot_log(user_id, f"✅ {coin}: SHORT autorisé - tendance baissière (RSI {rsi_now:.1f})", "success")

            # === MODE NEUTRE (retournements) ===
            else:
                # L'IA decide - on lui fait confiance sur le timing
                add_bot_log(user_id, f"🤖 {coin}: Mode neutre - IA analyse (RSI {rsi_now:.1f})", "info")

            # Ignorer signaux contradictoires avec signal recent
            if last_action and last_action["action"] != ai.get("action") and last_action["action"] != "WAIT":
                add_bot_log(user_id, f"⚡ {coin}: Signal contradictoire ignoré (dernier: {last_action['action']})", "warning")
                continue

            # Bloquer si position ouverte dans le sens inverse
            conn_pos = get_db()
            open_opposite = conn_pos.execute(
                """SELECT action FROM paper_trades 
                   WHERE user_id=? AND coin=? AND status='OPEN'""",
                (user_id, coin)
            ).fetchone()
            conn_pos.close()
            if open_opposite and open_opposite["action"] != ai.get("action"):
                add_bot_log(user_id, f"🚫 {coin}: Position {open_opposite['action']} déjà ouverte — signal {ai.get('action')} bloqué", "warning")
                continue

            # Save signal
            conn = get_db()
            conn.execute("""
                INSERT INTO signals (user_id,coin,action,confidence,entry,stop_loss,take_profit1,take_profit2,
                leverage,position_size,risk_reward,timeframe,reasoning,key_signals,price,rsi,atr,vwap)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                user_id, coin, ai["action"], ai["confidence"],
                ai.get("entry"), ai.get("stopLoss"), ai.get("takeProfit1"), ai.get("takeProfit2"),
                ai.get("leverage"), ai.get("positionSize"), ai.get("riskReward"),
                ai.get("timeframe"), ai.get("reasoning"),
                json.dumps(ai.get("keySignals", [])),
                price, tech["rsi"], tech["atr"], tech["vwap"]
            ))
            sig_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

            # Auto-execute en mode paper
            cfg = conn.execute("SELECT trading_mode, max_position_usdc, max_open_trades FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
            if cfg and cfg["trading_mode"] == "paper":
                open_count = conn.execute("SELECT COUNT(*) FROM paper_trades WHERE user_id=? AND status='OPEN'", (user_id,)).fetchone()[0]
                portfolio = conn.execute("SELECT balance FROM paper_portfolio WHERE user_id=?", (user_id,)).fetchone()
                max_trades = cfg["max_open_trades"] or 5
                # Taille en % du capital disponible
                position_pct = cfg["position_pct"] if cfg and "position_pct" in cfg.keys() else 8.0
                portfolio_now = conn.execute("SELECT balance FROM paper_portfolio WHERE user_id=?", (user_id,)).fetchone()
                capital = portfolio_now["balance"] if portfolio_now else 1000.0
                size = round(capital * position_pct / 100, 2)
                size = max(10.0, min(size, capital * 0.5))  # min 10 USDC, max 50% du capital
                add_bot_log(user_id, f"📐 Taille trade: {size} USDC ({position_pct}% de {round(capital,2)} USDC)", "info")
                # Verifier si coin deja en position ouverte
                coin_open = conn.execute("SELECT id FROM paper_trades WHERE user_id=? AND coin=? AND status='OPEN'", (user_id, coin)).fetchone()
                # Logs de diagnostic
                if not portfolio:
                    add_bot_log(user_id, f"⚠️ {coin}: Pas de portefeuille trouvé", "warning")
                elif open_count >= max_trades:
                    add_bot_log(user_id, f"⛔ {coin}: Max trades atteint ({open_count}/{max_trades})", "warning")
                elif portfolio["balance"] < size:
                    add_bot_log(user_id, f"⛔ {coin}: Solde insuffisant ({round(portfolio['balance'],2)} < {size} USDC)", "warning")
                elif coin_open:
                    add_bot_log(user_id, f"💰 {coin}: Position déjà ouverte", "info")
                if portfolio and open_count < max_trades and portfolio["balance"] >= size and not coin_open:
                    entry_price = ai.get("entry") or price
                    conn.execute("""
                        INSERT INTO paper_trades (user_id, coin, action, entry_price, current_price,
                        size_usdc, leverage, stop_loss, take_profit1, take_profit2, signal_id)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """, (user_id, coin, ai["action"], entry_price, price, size,
                           ai.get("leverage") or 1, ai.get("stopLoss"),
                           ai.get("takeProfit1"), ai.get("takeProfit2"), sig_id))
                    conn.execute("UPDATE paper_portfolio SET balance=balance-? WHERE user_id=?", (size, user_id))
                    add_bot_log(user_id, f"💰 PAPER TRADE: {ai['action']} {coin} @ ${entry_price} | {size} USDC", "success")

            conn.commit()
            conn.close()

        # Auto-update paper trades
        async with httpx.AsyncClient() as client2:
            open_trades = conn.execute(
                "SELECT DISTINCT coin FROM paper_trades WHERE user_id=? AND status='OPEN'", (user_id,)
            ).fetchall() if False else []
        conn = get_db()
        paper_trades = conn.execute(
            "SELECT * FROM paper_trades WHERE user_id=? AND status='OPEN'", (user_id,)
        ).fetchall()
        for trade in paper_trades:
            price_row = conn.execute("SELECT price FROM prices WHERE coin=?", (trade["coin"],)).fetchone()
            if not price_row: continue
            cur = price_row["price"]
            direction = 1 if trade["action"] == "LONG" else -1
            pnl = (cur - trade["entry_price"]) / trade["entry_price"] * trade["size_usdc"] * trade["leverage"] * direction
            close_reason = None
            trailing_pct = 0.015  # 1.5% trailing distance
            tp1_hit = trade["tp1_hit"] if trade["tp1_hit"] else 0
            trailing_sl = trade["trailing_sl"]
            highest = trade["highest_price"] or cur
            lowest = trade["lowest_price"] or cur

            # Mettre a jour highest/lowest price
            new_highest = max(highest, cur)
            new_lowest = min(lowest, cur)

            if trade["action"] == "LONG":
                # TP1 pas encore atteint
                if not tp1_hit and trade["take_profit1"] and cur >= trade["take_profit1"]:
                    # TP1 atteint : fermer 50%, SL remonte a breakeven, activer trailing
                    pnl_tp1 = (cur - trade["entry_price"]) / trade["entry_price"] * (trade["size_usdc"] * 0.5) * trade["leverage"]
                    conn.execute("""UPDATE paper_trades SET tp1_hit=1, trailing_sl=?,
                        highest_price=?, current_price=? WHERE id=?""",
                        (trade["entry_price"], new_highest, cur, trade["id"]))
                    conn.execute("UPDATE paper_portfolio SET balance=balance+? WHERE user_id=?",
                        (trade["size_usdc"] * 0.5 + pnl_tp1, user_id))
                    add_bot_log(user_id, f"🎯 {trade['coin']} TP1 atteint! +{round(pnl_tp1,2)} USDC | SL → breakeven | Trailing actif", "success")
                    conn.commit()
                    continue
                # Trailing SL actif apres TP1
                if tp1_hit:
                    new_trailing_sl = new_highest * (1 - trailing_pct)
                    effective_sl = max(trailing_sl or trade["entry_price"], new_trailing_sl)
                    conn.execute("UPDATE paper_trades SET trailing_sl=?, highest_price=?, current_price=? WHERE id=?",
                        (effective_sl, new_highest, cur, trade["id"]))
                    if cur <= effective_sl:
                        close_reason = "TRAILING_SL"
                else:
                    # SL normal avant TP1
                    # SL technique désactivé — Max Loss gère la protection
                # if trade["stop_loss"] and cur <= trade["stop_loss"]:
                #     close_reason = "STOP_LOSS"
                    conn.execute("UPDATE paper_trades SET highest_price=?, current_price=? WHERE id=?",
                        (new_highest, cur, trade["id"]))

            elif trade["action"] == "SHORT":
                # TP1 pas encore atteint
                if not tp1_hit and trade["take_profit1"] and cur <= trade["take_profit1"]:
                    pnl_tp1 = (trade["entry_price"] - cur) / trade["entry_price"] * (trade["size_usdc"] * 0.5) * trade["leverage"]
                    conn.execute("""UPDATE paper_trades SET tp1_hit=1, trailing_sl=?,
                        lowest_price=?, current_price=? WHERE id=?""",
                        (trade["entry_price"], new_lowest, cur, trade["id"]))
                    conn.execute("UPDATE paper_portfolio SET balance=balance+? WHERE user_id=?",
                        (trade["size_usdc"] * 0.5 + pnl_tp1, user_id))
                    add_bot_log(user_id, f"🎯 {trade['coin']} TP1 atteint! +{round(pnl_tp1,2)} USDC | SL → breakeven | Trailing actif", "success")
                    conn.commit()
                    continue
                # Trailing SL actif apres TP1
                if tp1_hit:
                    new_trailing_sl = new_lowest * (1 + trailing_pct)
                    effective_sl = min(trailing_sl or trade["entry_price"], new_trailing_sl)
                    conn.execute("UPDATE paper_trades SET trailing_sl=?, lowest_price=?, current_price=? WHERE id=?",
                        (effective_sl, new_lowest, cur, trade["id"]))
                    if cur >= effective_sl:
                        close_reason = "TRAILING_SL"
                else:
                    # SL technique désactivé — Max Loss gère la protection
                # if trade["stop_loss"] and cur >= trade["stop_loss"]:
                #     close_reason = "STOP_LOSS"
                    conn.execute("UPDATE paper_trades SET lowest_price=?, current_price=? WHERE id=?",
                        (new_lowest, cur, trade["id"]))

            # === TRAILING PROFIT & MAX LOSS ===
            cfg_qp = conn.execute("SELECT quick_profit_usd, max_loss_usd FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
            quick_profit_target = cfg_qp["quick_profit_usd"] if cfg_qp and "quick_profit_usd" in cfg_qp.keys() else 1.0
            max_loss_target = cfg_qp["max_loss_usd"] if cfg_qp and "max_loss_usd" in cfg_qp.keys() else 0.75
            trail_trigger = quick_profit_target * 1.5  # Active le trailing à 1.5$ si QP = 1$
            trail_gap = 0.5  # TSL = pic - 0.5$
            hl_fees = trade["size_usdc"] * 0.001

            # Mettre à jour le pic de PnL
            peak_pnl = float(trade["peak_pnl"]) if trade["peak_pnl"] is not None else 0.0
            if pnl > peak_pnl:
                peak_pnl = pnl
                conn.execute("UPDATE paper_trades SET peak_pnl=? WHERE id=?", (peak_pnl, trade["id"]))

            if not close_reason:
                if peak_pnl >= trail_trigger:
                    # Trailing actif — TSL = pic - 0.5$
                    trail_sl = peak_pnl - trail_gap
                    if pnl <= trail_sl:
                        # Si TSL descend sous Quick Profit → fermer au Quick Profit
                        if trail_sl <= quick_profit_target:
                            close_reason = "QUICK_PROFIT"
                            add_bot_log(user_id, f"⚡ {trade['coin']}: Quick Profit +{round(pnl,2)} USDC (protection descente) !", "success")
                        else:
                            close_reason = "TRAILING_PROFIT"
                            add_bot_log(user_id, f"🎯 {trade['coin']}: Trailing Profit +{round(pnl,2)} USDC (pic: +{round(peak_pnl,2)}$) !", "success")
                elif peak_pnl > quick_profit_target and pnl <= quick_profit_target:
                    # Prix redescend à exactement 1$ après avoir dépassé — Quick Profit filet
                    # Le WebSocket étant temps réel, la précision est au centime
                    close_reason = "QUICK_PROFIT"
                    add_bot_log(user_id, f"⚡ {trade['coin']}: Quick Profit filet +{round(pnl,2)} USDC (descente depuis +{round(peak_pnl,2)}$) !", "success")
                elif pnl <= -max_loss_target:
                    # Max Loss
                    close_reason = "MAX_LOSS"
                    add_bot_log(user_id, f"🛡️ {trade['coin']}: Max Loss -{round(abs(pnl),2)} USDC — protection activée", "warning")

            # Mettre à jour prix seulement si changement significatif (> 0.05%)
            if not close_reason:
                last_price = trade["current_price"] if trade["current_price"] else trade["entry_price"]
                if last_price and abs(cur - last_price) / last_price > 0.0005:
                    conn.execute(
                        "UPDATE paper_trades SET current_price=?, pnl=?, pnl_pct=? WHERE id=?",
                        (cur, round(pnl,2), round(pnl/trade["size_usdc"]*100,2), trade["id"])
                    )

            if close_reason:
                conn.execute("""UPDATE paper_trades SET status='CLOSED', current_price=?, pnl=?, pnl_pct=?,
                    closed_at=?, close_reason=? WHERE id=?""",
                    (cur, round(pnl,2), round(pnl/trade["size_usdc"]*100,2),
                     datetime.utcnow().isoformat(), close_reason, trade["id"]))
                conn.execute("UPDATE paper_portfolio SET balance=balance+?+? WHERE user_id=?",
                            (trade["size_usdc"], round(pnl,2), user_id))
                add_bot_log(user_id, f"🏁 {trade['coin']} fermé: {close_reason} | PnL: {round(pnl,2)} USDC", "success" if pnl >= 0 else "error")
            else:
                conn.execute("UPDATE paper_trades SET current_price=?, pnl=?, pnl_pct=? WHERE id=?",
                            (cur, round(pnl,2), round(pnl/trade["size_usdc"]*100,2), trade["id"]))
        conn.commit()
        conn.close()

        # Update last scan
        conn = get_db()
        conn.execute("UPDATE bot_config SET last_scan=? WHERE user_id=?",
                    (datetime.utcnow().isoformat(), user_id))
        conn.commit()
        conn.close()

def get_required_confidence(user_id: int, coin: str, action: str, base_confidence: int = 62) -> int:
    """Retourne la confiance requise selon les pertes consécutives"""
    conn = get_db()
    row = conn.execute(
        "SELECT consecutive_losses FROM coin_confidence WHERE user_id=? AND coin=? AND action=?",
        (user_id, coin, action)
    ).fetchone()
    conn.close()
    losses = row["consecutive_losses"] if row else 0
    # +5% par perte consécutive, max 90%
    required = min(base_confidence + (losses * 5), 90)
    return required

def update_coin_confidence(user_id: int, coin: str, action: str, won: bool):
    """Met à jour le compteur de pertes consécutives"""
    conn = get_db()
    if won:
        # Victoire → reset compteur
        conn.execute("""INSERT OR REPLACE INTO coin_confidence 
            (user_id, coin, action, consecutive_losses, updated_at) VALUES (?,?,?,0,?)""",
            (user_id, coin, action, datetime.utcnow().isoformat()))
        add_bot_log(user_id, f"✅ {coin} {action}: Confiance reset à 62% (gain)", "info")
    else:
        # Défaite → incrémenter
        current = conn.execute(
            "SELECT consecutive_losses FROM coin_confidence WHERE user_id=? AND coin=? AND action=?",
            (user_id, coin, action)
        ).fetchone()
        losses = (current["consecutive_losses"] + 1) if current else 1
        new_conf = min(62 + (losses * 5), 90)
        conn.execute("""INSERT OR REPLACE INTO coin_confidence 
            (user_id, coin, action, consecutive_losses, updated_at) VALUES (?,?,?,?,?)""",
            (user_id, coin, action, losses, datetime.utcnow().isoformat()))
        add_bot_log(user_id, f"📈 {coin} {action}: Confiance requise → {new_conf}% ({losses} pertes consécutives)", "warning")
    conn.commit()
    conn.close()

async def check_macro_calendar(user_id: int, finnhub_key: str) -> dict:
    """Vérifie les annonces macro importantes dans les prochaines 24h via Finnhub"""
    try:
        from datetime import datetime as dt, timedelta
        now = dt.utcnow()
        date_from = now.strftime("%Y-%m-%d")
        date_to = (now + timedelta(days=2)).strftime("%Y-%m-%d")
        
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"https://finnhub.io/api/v1/calendar/economic",
                params={"from": date_from, "to": date_to, "token": finnhub_key},
                timeout=10
            )
            if r.status_code != 200:
                return {}
            data = r.json()
        
        # Filtrer les événements à fort impact USD
        high_impact_keywords = ["FOMC", "Fed", "CPI", "NFP", "Nonfarm", "GDP", "PCE", "Interest Rate"]
        upcoming = []
        
        for event in data.get("economicCalendar", []):
            if event.get("country") != "US":
                continue
            event_name = event.get("event", "")
            if not any(kw.lower() in event_name.lower() for kw in high_impact_keywords):
                continue
            
            # Calculer le temps restant
            try:
                event_time = dt.strptime(event.get("time", "")[:16], "%Y-%m-%d %H:%M")
                hours_left = (event_time - now).total_seconds() / 3600
                if -1 <= hours_left <= 48:  # Entre -1h (vient de passer) et 48h
                    upcoming.append({
                        "event": event_name,
                        "time": event.get("time", ""),
                        "hours_left": round(hours_left, 1),
                        "impact": "HIGH"
                    })
            except:
                continue
        
        return {"events": upcoming}
    except Exception as e:
        return {}

# Cache des prix en temps réel via WebSocket
ws_prices = {}
ws_connected = False

# Cache des données de marché structurées (indicateurs pré-calculés)
market_data_cache = {}  # coin -> {rsi, macd, ema, bb, volume, timestamp}

async def process_trade_on_price(user_id: int, trade: dict, cur: float, conn):
    """Traite un trade ouvert avec le nouveau prix - appelé par le WebSocket"""
    try:
        pnl_direction = 1 if trade["action"] == "LONG" else -1
        price_diff = (cur - trade["entry_price"]) / trade["entry_price"]
        pnl = price_diff * trade["size_usdc"] * trade["leverage"] * pnl_direction

        # Récupérer config
        cfg_qp = conn.execute("SELECT quick_profit_usd, max_loss_usd FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
        quick_profit_target = float(cfg_qp["quick_profit_usd"]) if cfg_qp and "quick_profit_usd" in cfg_qp.keys() else 1.0
        max_loss_target = float(cfg_qp["max_loss_usd"]) if cfg_qp and "max_loss_usd" in cfg_qp.keys() else 0.75
        trail_trigger = quick_profit_target * 1.5
        trail_gap = 0.5
        hl_fees = trade["size_usdc"] * 0.001

        # Mettre à jour peak_pnl
        peak_pnl = float(trade["peak_pnl"]) if trade["peak_pnl"] is not None else 0.0
        if pnl > peak_pnl:
            peak_pnl = pnl
            conn.execute("UPDATE paper_trades SET peak_pnl=?, current_price=?, pnl=?, pnl_pct=? WHERE id=?",
                (peak_pnl, cur, round(pnl,2), round(pnl/trade["size_usdc"]*100,2), trade["id"]))
        else:
            conn.execute("UPDATE paper_trades SET current_price=?, pnl=?, pnl_pct=? WHERE id=?",
                (cur, round(pnl,2), round(pnl/trade["size_usdc"]*100,2), trade["id"]))

        close_reason = None

        if peak_pnl >= trail_trigger:
            trail_sl = peak_pnl - trail_gap
            if pnl <= trail_sl:
                close_reason = "TRAILING_PROFIT" if trail_sl > quick_profit_target else "QUICK_PROFIT"
                add_bot_log(user_id, f"🎯 {trade['coin']}: {'Trailing' if close_reason=='TRAILING_PROFIT' else 'Quick'} Profit +{round(pnl,2)}$ (pic: +{round(peak_pnl,2)}$) ⚡ WS", "success")
        elif pnl > 0 and pnl <= quick_profit_target and peak_pnl > quick_profit_target:
            close_reason = "QUICK_PROFIT"
            add_bot_log(user_id, f"⚡ {trade['coin']}: Quick Profit filet +{round(pnl,2)}$ ⚡ WS", "success")
        elif pnl <= -max_loss_target:
            close_reason = "MAX_LOSS"
            add_bot_log(user_id, f"🛡️ {trade['coin']}: Max Loss -{round(abs(pnl),2)}$ ⚡ WS", "warning")

        if close_reason:
            tp1_hit = trade["tp1_hit"] or 0
            remaining = trade["size_usdc"] * (0.5 if tp1_hit else 1)
            conn.execute("""UPDATE paper_trades SET status='CLOSED', current_price=?, pnl=?, pnl_pct=?,
                closed_at=?, close_reason=? WHERE id=?""",
                (cur, round(pnl,2), round(pnl/trade["size_usdc"]*100,2),
                 datetime.utcnow().isoformat(), close_reason, trade["id"]))
            conn.execute("UPDATE paper_portfolio SET balance=balance+?+? WHERE user_id=?",
                (remaining, pnl, user_id))
            conn.commit()
            update_coin_confidence(user_id, trade["coin"], trade["action"], pnl > 0)
            return True
        conn.commit()
        return False
    except Exception as e:
        print(f"WS trade error {trade.get('coin','?')}: {e}")
        return False

async def startup_cleanup(user_id: int):
    """Nettoyage complet au démarrage — orphelins, doublons, sessions historiques"""
    conn = get_db()
    cleaned = []

    # 1. Reconstruire les sessions historiques depuis les trades existants
    dates = conn.execute("""
        SELECT DISTINCT date(opened_at) as day FROM paper_trades 
        WHERE user_id=? AND opened_at IS NOT NULL
        ORDER BY day
    """, (user_id,)).fetchall()
    
    for row in dates:
        day = row["day"]
        if not day:
            continue
        existing = conn.execute(
            "SELECT id FROM trading_sessions WHERE user_id=? AND session_date=?",
            (user_id, day)
        ).fetchone()
        if not existing:
            # Calculer stats de cette journée
            stats = conn.execute("""
                SELECT COUNT(*) as total,
                    SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN pnl<=0 THEN 1 ELSE 0 END) as losses,
                    SUM(CASE WHEN status='CLOSED' THEN pnl ELSE 0 END) as net
                FROM paper_trades WHERE user_id=? AND date(opened_at)=?
            """, (user_id, day)).fetchone()
            
            # Vérifier si des trades sont encore ouverts
            open_count = conn.execute(
                "SELECT COUNT(*) FROM paper_trades WHERE user_id=? AND date(opened_at)=? AND status='OPEN'",
                (user_id, day)
            ).fetchone()[0]
            
            ended_at = datetime.utcnow().isoformat() if open_count == 0 else None
            conn.execute("""INSERT INTO trading_sessions 
                (user_id, session_date, started_at, ended_at, closing_phase, total_trades, wins, losses, net_pnl)
                VALUES (?,?,?,?,1,?,?,?,?)""",
                (user_id, day, day+"T00:00:00", ended_at,
                 stats["total"] or 0, stats["wins"] or 0, 
                 stats["losses"] or 0, stats["net"] or 0))
            cleaned.append(f"📅 Session {day} reconstruite ({stats['total']} trades)")
    
    # 2. Supprimer les signaux en double (garder le plus récent par coin+action+jour)
    result = conn.execute("""
        DELETE FROM signals WHERE id NOT IN (
            SELECT MAX(id) FROM signals
            WHERE user_id=?
            GROUP BY coin, action, date(created_at)
        ) AND user_id=?
    """, (user_id, user_id))
    dup_count = conn.execute("SELECT changes()").fetchone()[0]
    if dup_count > 0:
        cleaned.append(f"🗑️ {dup_count} signaux en double supprimés")

    # 3. Supprimer les signaux des actifs désactivés
    config = conn.execute("SELECT active_coins FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
    if config:
        import json as json_mod
        active_coins = json_mod.loads(config["active_coins"])
        placeholders = ",".join("?" * len(active_coins))
        result = conn.execute(
            f"DELETE FROM signals WHERE user_id=? AND coin NOT IN ({placeholders})",
            [user_id] + active_coins
        )
        inactive_count = conn.execute("SELECT changes()").fetchone()[0]
        if inactive_count > 0:
            cleaned.append(f"🗑️ {inactive_count} signaux d'actifs désactivés supprimés")

    # 4. Supprimer les signaux de plus de 7 jours
    result = conn.execute("""
        DELETE FROM signals WHERE user_id=? 
        AND created_at < datetime('now', '-7 days')
    """, (user_id,))
    old_count = conn.execute("SELECT changes()").fetchone()[0]
    if old_count > 0:
        cleaned.append(f"🗑️ {old_count} signaux anciens (>7j) supprimés")

    # 5. Nettoyer les trades orphelins (ouverts depuis plus de 48h sans mise à jour)
    orphan_trades = conn.execute("""
        SELECT id, coin, action, pnl FROM paper_trades 
        WHERE user_id=? AND status='OPEN'
        AND opened_at < datetime('now', '-48 hours')
    """, (user_id,)).fetchall()
    
    for trade in orphan_trades:
        trade = dict(trade)
        pnl = trade["pnl"] or 0
        conn.execute("""UPDATE paper_trades SET status='CLOSED', 
            close_reason='ORPHAN_CLEANUP', closed_at=?
            WHERE id=?""", (datetime.utcnow().isoformat(), trade["id"]))
        conn.execute("UPDATE paper_portfolio SET balance=balance+? WHERE user_id=?",
            (pnl, user_id))
        cleaned.append(f"🧹 Trade orphelin fermé: {trade['action']} {trade['coin']} (PnL: {round(pnl,2)}$)")

    # 6. Nettoyer les logs persistants > 500 entrées
    conn.execute("""DELETE FROM bot_activity_log WHERE user_id=? AND id NOT IN (
        SELECT id FROM bot_activity_log WHERE user_id=? ORDER BY id DESC LIMIT 500
    )""", (user_id, user_id))
    
    # 7. Nettoyer la confiance dynamique des coins qui n'ont pas eu de perte depuis 24h
    conn.execute("""DELETE FROM coin_confidence 
        WHERE user_id=? AND updated_at < datetime('now', '-24 hours')
        AND consecutive_losses = 0""", (user_id,))

    conn.commit()
    conn.close()

    if cleaned:
        add_bot_log(user_id, f"🧹 Nettoyage démarrage: {len(cleaned)} actions", "info")
        for msg in cleaned:
            add_bot_log(user_id, msg, "info")
    else:
        add_bot_log(user_id, "✅ Nettoyage démarrage: rien à nettoyer", "info")

async def check_session_lifecycle(user_id: int):
    """Gère le cycle de vie des sessions de trading (minuit UTC)"""
    conn = get_db()
    now = datetime.utcnow()
    today = now.strftime("%Y-%m-%d")
    
    # Vérifier si une session existe pour aujourd'hui
    session = conn.execute(
        "SELECT * FROM trading_sessions WHERE user_id=? AND session_date=?",
        (user_id, today)
    ).fetchone()
    
    # Créer session du jour si elle n'existe pas
    if not session:
        portfolio_now = conn.execute("SELECT balance FROM paper_portfolio WHERE user_id=?", (user_id,)).fetchone()
        capital_start = portfolio_now["balance"] if portfolio_now else 1000.0
        conn.execute("""INSERT INTO trading_sessions 
            (user_id, session_date, started_at, closing_phase, capital_start) 
            VALUES (?,?,?,0,?)""",
            (user_id, today, now.isoformat(), capital_start))
        conn.commit()
        add_bot_log(user_id, f"📅 Nouvelle session démarrée: {today}", "success")
    
    # Vérifier si on est en phase de clôture (après 23h45 UTC)
    session = conn.execute(
        "SELECT * FROM trading_sessions WHERE user_id=? AND session_date=?",
        (user_id, today)
    ).fetchone()
    
    if session and not session["closing_phase"]:
        # Déclencher phase clôture à 23h45
        if now.hour == 23 and now.minute >= 45:
            conn.execute(
                "UPDATE trading_sessions SET closing_phase=1 WHERE user_id=? AND session_date=?",
                (user_id, today))
            conn.commit()
            add_bot_log(user_id, "🔒 Session en clôture — plus de nouveaux trades jusqu'à minuit", "warning")
    
    # À minuit : générer rapport et attendre fin des trades
    yesterday = (now - __import__('datetime').timedelta(days=1)).strftime("%Y-%m-%d")
    prev_session = conn.execute(
        "SELECT * FROM trading_sessions WHERE user_id=? AND session_date=? AND closing_phase=1 AND ended_at IS NULL",
        (user_id, yesterday)
    ).fetchone()
    
    if prev_session:
        # Vérifier si tous les trades sont fermés
        open_count = conn.execute(
            "SELECT COUNT(*) FROM paper_trades WHERE user_id=? AND status='OPEN'",
            (user_id,)
        ).fetchone()[0]
        
        if open_count == 0:
            # Calculer stats de la session précédente
            stats = conn.execute("""
                SELECT COUNT(*) as total,
                    SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN pnl<=0 THEN 1 ELSE 0 END) as losses,
                    SUM(pnl) as net
                FROM paper_trades
                WHERE user_id=? AND status='CLOSED' 
                AND date(opened_at)=?
            """, (user_id, yesterday)).fetchone()
            
            conn.execute("""UPDATE trading_sessions 
                SET ended_at=?, total_trades=?, wins=?, losses=?, net_pnl=?
                WHERE user_id=? AND session_date=?""",
                (now.isoformat(), stats["total"] or 0, stats["wins"] or 0,
                 stats["losses"] or 0, stats["net"] or 0, user_id, yesterday))
            conn.commit()
            add_bot_log(user_id, 
                f"✅ Session {yesterday} clôturée | {stats['total']} trades | NET: {round(stats['net'] or 0, 2)}$ | Win rate: {round((stats['wins'] or 0)/max(stats['total'] or 1,1)*100,1)}%",
                "success")
    
    conn.close()
    return session

async def is_session_closing(user_id: int) -> bool:
    """Vérifie si la session est en phase de clôture"""
    conn = get_db()
    now = datetime.utcnow()
    today = now.strftime("%Y-%m-%d")
    session = conn.execute(
        "SELECT closing_phase FROM trading_sessions WHERE user_id=? AND session_date=?",
        (user_id, today)
    ).fetchone()
    conn.close()
    if session and session["closing_phase"]:
        return True
    # Aussi bloquer si entre 23h45 et minuit
    return now.hour == 23 and now.minute >= 45

async def connect_hyperliquid_ws():
    """Connexion WebSocket Hyperliquid — prix temps réel + gestion trades"""
    global ws_prices, ws_connected
    import websockets
    import json as json_mod
    uri = "wss://api.hyperliquid.xyz/ws"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20) as ws:
                ws_connected = True
                print("🔌 WebSocket Hyperliquid connecté !")
                await ws.send(json_mod.dumps({
                    "method": "subscribe",
                    "subscription": {"type": "allMids"}
                }))
                async for msg in ws:
                    data = json_mod.loads(msg)
                    if data.get("channel") == "allMids" and "data" in data:
                        mids = data["data"].get("mids", {})
                        ws_prices.update({k: float(v) for k, v in mids.items()})
                        
                        # Traiter les trades ouverts pour chaque coin mis à jour
                        conn = get_db()
                        try:
                            open_trades = conn.execute(
                                "SELECT pt.*, bc.user_id FROM paper_trades pt "
                                "JOIN bot_config bc ON pt.user_id = bc.user_id "
                                "WHERE pt.status='OPEN' AND bc.is_running=1"
                            ).fetchall()
                            for trade in open_trades:
                                trade = dict(trade)
                                coin = trade["coin"]
                                if coin in mids:
                                    cur = float(mids[coin])
                                    await process_trade_on_price(trade["user_id"], trade, cur, conn)
                        except Exception as e:
                            print(f"WS trades error: {e}")
                        finally:
                            conn.close()
        except Exception as e:
            ws_connected = False
            print(f"🔌 WebSocket déconnecté: {e} — reconnexion dans 5s")
            await asyncio.sleep(5)

async def get_current_price(coin: str, client=None) -> float:
    """Retourne le prix temps réel depuis WS ou fallback REST"""
    if ws_connected and coin in ws_prices:
        return ws_prices[coin]
    # Fallback REST si WS non connecté
    if client:
        prices = await fetch_all_metas(client)
        return prices.get(coin, 0)
    return 0

async def check_positions_loop(user_id: int):
    """Boucle rapide 15s - suivi SL/TP/Trailing des positions ouvertes"""
    try:
        while True:
            conn = get_db()
            config = conn.execute("SELECT is_running FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
            conn.close()
            if not config or not config["is_running"]:
                break
            try:
                await update_open_positions(user_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                add_bot_log(user_id, f"⚠️ Erreur suivi positions: {e}", "error")
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        raise

async def auto_reset_macro_filter(user_id: int):
    """Désactive le filtre macro 1h après une annonce"""
    conn = get_db()
    user = conn.execute("SELECT finnhub_key FROM users WHERE id=?", (user_id,)).fetchone()
    cfg = conn.execute("SELECT filter_macro FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    
    if not cfg or not cfg["filter_macro"]:
        return
    finnhub_key = user["finnhub_key"] if user and "finnhub_key" in user.keys() else None
    if not finnhub_key:
        return
    
    macro_data = await check_macro_calendar(user_id, finnhub_key)
    events = macro_data.get("events", [])
    # Si aucune annonce dans les prochaines 2h, désactiver le filtre
    critical = [e for e in events if e["hours_left"] <= 2 and e["hours_left"] >= -1]
    if not critical:
        conn2 = get_db()
        conn2.execute("UPDATE bot_config SET filter_macro=0 WHERE user_id=?", (user_id,))
        conn2.commit()
        conn2.close()
        add_bot_log(user_id, "✅ Filtre macro désactivé automatiquement — fenêtre macro passée", "success")

async def update_open_positions(user_id: int):
    """Met a jour SL/TP/Trailing sur les positions ouvertes"""
    conn = get_db()
    paper_trades = conn.execute(
        "SELECT * FROM paper_trades WHERE user_id=? AND status='OPEN'",
        (user_id,)
    ).fetchall()
    if not paper_trades:
        conn.close()
        return
    # Utiliser WebSocket si disponible, sinon REST
    if ws_connected and ws_prices:
        prices = ws_prices.copy()
        client_ctx = None
    else:
        async with httpx.AsyncClient() as client:
            prices = await fetch_all_metas(client)
    for trade in paper_trades:
        trade = dict(trade)
        cur = prices.get(trade["coin"])
        if not cur:
            continue
        pnl = (cur - trade["entry_price"]) / trade["entry_price"] * trade["size_usdc"] * trade["leverage"] if trade["action"] == "LONG" else (trade["entry_price"] - cur) / trade["entry_price"] * trade["size_usdc"] * trade["leverage"]
        close_reason = None
        trailing_pct = 0.015
        tp1_hit = trade["tp1_hit"] if trade["tp1_hit"] else 0
        trailing_sl = trade["trailing_sl"]
        highest = trade["highest_price"] or cur
        lowest = trade["lowest_price"] or cur
        new_highest = max(highest, cur)
        new_lowest = min(lowest, cur)

        if trade["action"] == "LONG":
            if not tp1_hit and trade["take_profit1"] and cur >= trade["take_profit1"]:
                pnl_tp1 = (cur - trade["entry_price"]) / trade["entry_price"] * (trade["size_usdc"] * 0.5) * trade["leverage"]
                conn.execute("UPDATE paper_trades SET tp1_hit=1, trailing_sl=?, highest_price=?, current_price=? WHERE id=?",
                    (trade["entry_price"], new_highest, cur, trade["id"]))
                conn.execute("UPDATE paper_portfolio SET balance=balance+? WHERE user_id=?",
                    (trade["size_usdc"] * 0.5 + pnl_tp1, user_id))
                add_bot_log(user_id, f"🎯 {trade['coin']} TP1 atteint! +{round(pnl_tp1,2)} USDC | SL → breakeven | Trailing actif", "success")
                conn.commit()
                continue
            if tp1_hit:
                # Trailing progressif selon niveaux atteints
                tp2 = trade["take_profit2"]
                if tp2 and cur >= tp2 * 1.02:
                    # Au-dela de TP2 + 2% → trailing ultra serré 0.5%
                    trailing_pct = 0.005
                elif tp2 and cur >= tp2:
                    # TP2 atteint → trailing serré 0.8%
                    trailing_pct = 0.008
                else:
                    # Entre TP1 et TP2 → trailing normal 1.5%
                    trailing_pct = 0.015
                new_trailing_sl = new_highest * (1 - trailing_pct)
                effective_sl = max(trailing_sl or trade["entry_price"], new_trailing_sl)
                conn.execute("UPDATE paper_trades SET trailing_sl=?, highest_price=?, current_price=? WHERE id=?",
                    (effective_sl, new_highest, cur, trade["id"]))
                if cur <= effective_sl:
                    close_reason = "TRAILING_SL"
                    if tp2 and trade["highest_price"] and trade["highest_price"] >= tp2:
                        close_reason = "TRAILING_SL_POST_TP2"
            else:
                # SL technique désactivé - Max Loss gère
                pass
                conn.execute("UPDATE paper_trades SET highest_price=?, current_price=? WHERE id=?",
                    (new_highest, cur, trade["id"]))
        elif trade["action"] == "SHORT":
            if not tp1_hit and trade["take_profit1"] and cur <= trade["take_profit1"]:
                pnl_tp1 = (trade["entry_price"] - cur) / trade["entry_price"] * (trade["size_usdc"] * 0.5) * trade["leverage"]
                conn.execute("UPDATE paper_trades SET tp1_hit=1, trailing_sl=?, lowest_price=?, current_price=? WHERE id=?",
                    (trade["entry_price"], new_lowest, cur, trade["id"]))
                conn.execute("UPDATE paper_portfolio SET balance=balance+? WHERE user_id=?",
                    (trade["size_usdc"] * 0.5 + pnl_tp1, user_id))
                add_bot_log(user_id, f"🎯 {trade['coin']} TP1 atteint! +{round(pnl_tp1,2)} USDC | SL → breakeven | Trailing actif", "success")
                conn.commit()
                continue
            if tp1_hit:
                # Trailing progressif selon niveaux atteints
                tp2 = trade["take_profit2"]
                if tp2 and cur <= tp2 * 0.98:
                    # Au-dela de TP2 - 2% → trailing ultra serré 0.5%
                    trailing_pct = 0.005
                elif tp2 and cur <= tp2:
                    # TP2 atteint → trailing serré 0.8%
                    trailing_pct = 0.008
                else:
                    # Entre TP1 et TP2 → trailing normal 1.5%
                    trailing_pct = 0.015
                new_trailing_sl = new_lowest * (1 + trailing_pct)
                effective_sl = min(trailing_sl or trade["entry_price"], new_trailing_sl)
                conn.execute("UPDATE paper_trades SET trailing_sl=?, lowest_price=?, current_price=? WHERE id=?",
                    (effective_sl, new_lowest, cur, trade["id"]))
                if cur >= effective_sl:
                    close_reason = "TRAILING_SL"
                    if tp2 and trade["lowest_price"] and trade["lowest_price"] <= tp2:
                        close_reason = "TRAILING_SL_POST_TP2"
            else:
                # SL technique désactivé - Max Loss gère
                pass
                conn.execute("UPDATE paper_trades SET lowest_price=?, current_price=? WHERE id=?",
                    (new_lowest, cur, trade["id"]))

        if close_reason:
            conn.execute("""UPDATE paper_trades SET status='CLOSED', current_price=?, pnl=?, pnl_pct=?,
                closed_at=?, close_reason=? WHERE id=?""",
                (cur, round(pnl,2), round(pnl/trade["size_usdc"]*100,2),
                 datetime.utcnow().isoformat(), close_reason, trade["id"]))
            # Si TP1 deja touche, on rend seulement la moitie restante + PnL sur cette moitie
            remaining = trade["size_usdc"] * (0.5 if tp1_hit else 1)
            conn.execute("UPDATE paper_portfolio SET balance=balance+?+? WHERE user_id=?",
                (remaining, pnl, user_id))
            emoji = "🎯" if close_reason in ("TP1","TP2","TRAILING_PROFIT","QUICK_PROFIT") else "🏁"
            add_bot_log(user_id, f"{emoji} {trade['coin']} fermé: {close_reason} | PnL: {round(pnl,2)} USDC", "success" if pnl >= 0 else "error")
            # Mettre à jour confiance dynamique
            won = pnl > 0
            update_coin_confidence(user_id, trade["coin"], trade["action"], won)
        conn.commit()
    conn.close()

async def run_bot_loop(user_id: int):
    """Boucle principale 3min - analyse IA et nouveaux signaux"""
    try:
        while True:
            conn = get_db()
            config = conn.execute("SELECT is_running FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
            conn.close()
            if not config or not config["is_running"]:
                break
            try:
                await scan_markets(user_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                add_bot_log(user_id, f"⚠️ Erreur scan: {e}", "error")
            await asyncio.sleep(180)
    except asyncio.CancelledError:
        add_bot_log(user_id, "🛑 Boucle de scan interrompue", "info")
        raise

# ── FASTAPI APP ──────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # Auto-redemarrer les bots actifs au redemarrage du serveur
    conn = get_db()
    running_users = conn.execute("SELECT user_id FROM bot_config WHERE is_running=1").fetchall()
    conn.close()
    for row in running_users:
        user_id = row["user_id"]
        scanning_tasks[user_id] = asyncio.create_task(run_bot_loop(user_id))
        positions_tasks[user_id] = asyncio.create_task(check_positions_loop(user_id))
        asyncio.create_task(startup_cleanup(user_id))
        print(f"Bot auto-redemarre pour user {user_id}")
    # Démarrer WebSocket Hyperliquid automatiquement au démarrage du serveur
    asyncio.create_task(connect_hyperliquid_ws())
    print("🔌 WebSocket Hyperliquid démarré automatiquement")
    yield

app = FastAPI(title="HyperBot AI", lifespan=lifespan)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── MODÈLES ──────────────────────────────────────────────────
class RegisterRequest(BaseModel):
    email: str
    password: str

class LoginRequest(BaseModel):
    email: str
    password: str

class UpdateConfigRequest(BaseModel):
    wallet: Optional[str] = None
    api_key: Optional[str] = None
    active_coins: Optional[List[str]] = None
    trading_mode: Optional[str] = None
    max_position_usdc: Optional[float] = None
    max_open_trades: Optional[int] = None
    position_pct: Optional[float] = None
    quick_profit_usd: Optional[float] = None
    max_loss_usd: Optional[float] = None

# ── ROUTES AUTH ──────────────────────────────────────────────
@app.post("/api/register")
def register(req: RegisterRequest):
    conn = get_db()
    try:
        conn.execute("INSERT INTO users (email, password_hash) VALUES (?,?)",
                    (req.email.lower(), hash_password(req.password)))
        user_id = conn.execute("SELECT id FROM users WHERE email=?", (req.email.lower(),)).fetchone()["id"]
        conn.execute("INSERT INTO bot_config (user_id) VALUES (?)", (user_id,))
        conn.commit()
        return {"message": "Compte créé avec succès"}
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="Cet email est déjà utilisé")
    finally:
        conn.close()

@app.post("/api/login")
def login(req: LoginRequest):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE email=? AND password_hash=?",
                       (req.email.lower(), hash_password(req.password))).fetchone()
    if not user:
        conn.close()
        raise HTTPException(status_code=401, detail="Email ou mot de passe incorrect")
    token = create_token()
    expires = (datetime.utcnow() + timedelta(days=30)).isoformat()
    conn.execute("INSERT INTO sessions (token, user_id, expires_at) VALUES (?,?,?)",
                (token, user["id"], expires))
    conn.commit()
    conn.close()
    return {"token": token, "email": user["email"]}

@app.post("/api/logout")
def logout(credentials: HTTPAuthorizationCredentials = Depends(security)):
    conn = get_db()
    conn.execute("DELETE FROM sessions WHERE token=?", (credentials.credentials,))
    conn.commit()
    conn.close()
    return {"message": "Déconnecté"}

# ── ROUTES BOT ───────────────────────────────────────────────
@app.get("/api/config")
def get_config(user_id: int = Depends(get_current_user)):
    conn = get_db()
    user = conn.execute("SELECT email, wallet, api_key, finnhub_key, hl_api_key, hl_wallet FROM users WHERE id=?", (user_id,)).fetchone()
    config = conn.execute("SELECT * FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return {
        "email": user["email"],
        "wallet": user["wallet"],
        "has_api_key": bool(user["api_key"]),
        "has_finnhub_key": bool(user["finnhub_key"]),
        "finnhub_key_preview": ("****" + user["finnhub_key"][-4:]) if user["finnhub_key"] else "",
        "has_hl_api_key": bool(user["hl_api_key"]) if "hl_api_key" in user.keys() else False,
        "hl_wallet": user["hl_wallet"] if "hl_wallet" in user.keys() else "",
        "active_coins": json.loads(config["active_coins"]),
        "is_running": bool(config["is_running"]),
        "trading_mode": config["trading_mode"] or "paper",
        "max_position_usdc": config["max_position_usdc"] or 50.0,
        "position_pct": config["position_pct"] if config and "position_pct" in config.keys() else 5.0,
        "quick_profit_usd": config["quick_profit_usd"] if config and "quick_profit_usd" in config.keys() else 1.0,
        "max_loss_usd": config["max_loss_usd"] if config and "max_loss_usd" in config.keys() else 0.75,
        "max_open_trades": config["max_open_trades"] or 5,
        "last_scan": config["last_scan"],
        "ai_continuous": config["ai_continuous"] if config and "ai_continuous" in config.keys() else 0,
        "filter_hours": config["filter_hours"] if config and "filter_hours" in config.keys() else 1,
        "filter_weekend": config["filter_weekend"] if config and "filter_weekend" in config.keys() else 1,
        "filter_macro": config["filter_macro"] if config and "filter_macro" in config.keys() else 0,
    }

@app.put("/api/config")
def update_config(req: UpdateConfigRequest, user_id: int = Depends(get_current_user)):
    conn = get_db()
    if req.wallet is not None:
        conn.execute("UPDATE users SET wallet=? WHERE id=?", (req.wallet, user_id))
    if req.api_key is not None:
        conn.execute("UPDATE users SET api_key=? WHERE id=?", (req.api_key, user_id))
    if req.active_coins is not None:
        conn.execute("UPDATE bot_config SET active_coins=? WHERE user_id=?",
                    (json.dumps(req.active_coins), user_id))
    if req.trading_mode is not None:
        old_mode = conn.execute("SELECT trading_mode FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
        conn.execute("UPDATE bot_config SET trading_mode=? WHERE user_id=?",
                    (req.trading_mode, user_id))
        # Log le changement de mode
        print(f"Mode change: {old_mode['trading_mode'] if old_mode else 'unknown'} -> {req.trading_mode} pour user {user_id}")
    if req.max_position_usdc is not None:
        conn.execute("UPDATE bot_config SET max_position_usdc=? WHERE user_id=?",
                    (req.max_position_usdc, user_id))
    if req.max_open_trades is not None:
        conn.execute("UPDATE bot_config SET max_open_trades=? WHERE user_id=?",
                    (req.max_open_trades, user_id))
    if req.position_pct is not None:
        pct = max(1.0, min(50.0, req.position_pct))
        conn.execute("UPDATE bot_config SET position_pct=? WHERE user_id=?",
                    (pct, user_id))
    if req.quick_profit_usd is not None:
        conn.execute("UPDATE bot_config SET quick_profit_usd=? WHERE user_id=?",
                    (req.quick_profit_usd, user_id))
    if req.max_loss_usd is not None:
        conn.execute("UPDATE bot_config SET max_loss_usd=? WHERE user_id=?",
                    (req.max_loss_usd, user_id))
    conn.commit()
    conn.close()
    return {"message": "Configuration mise à jour"}

@app.post("/api/bot/start")
async def start_bot(background_tasks: BackgroundTasks, user_id: int = Depends(get_current_user)):
    conn = get_db()
    user = conn.execute("SELECT api_key FROM users WHERE id=?", (user_id,)).fetchone()
    conn.execute("UPDATE bot_config SET is_running=1 WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()
    if not user["api_key"]:
        raise HTTPException(status_code=400, detail="Clé API Anthropic manquante dans les paramètres")
    # Annuler taches zombies avant d'en creer de nouvelles
    if user_id in scanning_tasks and not scanning_tasks[user_id].done():
        scanning_tasks[user_id].cancel()
    if user_id in positions_tasks and not positions_tasks[user_id].done():
        positions_tasks[user_id].cancel()
    # Lancer boucle scan IA (3min) et boucle suivi positions (60s)
    scanning_tasks[user_id] = asyncio.create_task(run_bot_loop(user_id))
    positions_tasks[user_id] = asyncio.create_task(check_positions_loop(user_id))
    asyncio.create_task(startup_cleanup(user_id))
    add_bot_log(user_id, "▶️ Bot démarré — Scan IA: 3min | Suivi positions: 5s | WS: temps réel", "success")
    return {"message": "Bot démarré"}

@app.put("/api/config/hyperliquid")
async def save_hl_config(req: dict, user_id: int = Depends(get_current_user)):
    conn = get_db()
    if req.get("hl_api_key"):
        conn.execute("UPDATE users SET hl_api_key=? WHERE id=?", (req["hl_api_key"].strip(), user_id))
    if req.get("hl_wallet"):
        conn.execute("UPDATE users SET hl_wallet=? WHERE id=?", (req["hl_wallet"].strip(), user_id))
    conn.commit()
    conn.close()
    add_bot_log(user_id, "🔑 Configuration Hyperliquid sauvegardée", "success")
    return {"message": "Configuration Hyperliquid sauvegardée"}

@app.put("/api/config/finnhub")
async def save_finnhub_key(req: dict, user_id: int = Depends(get_current_user)):
    key = req.get("finnhub_key", "").strip()
    if not key:
        return {"message": "Clé vide ignorée"}
    conn = get_db()
    conn.execute("UPDATE users SET finnhub_key=? WHERE id=?", (key, user_id))
    conn.commit()
    # Vérifier que c'est bien sauvegardé
    saved = conn.execute("SELECT finnhub_key FROM users WHERE id=?", (user_id,)).fetchone()
    conn.close()
    if saved and saved["finnhub_key"] == key:
        add_bot_log(user_id, f"🔑 Clé Finnhub sauvegardée ({key[:8]}...) — calendrier macro actif", "success")
        return {"message": "Clé Finnhub sauvegardée", "saved": True}
    return {"message": "Erreur sauvegarde", "saved": False}

@app.put("/api/config/filters")
def update_filters(req: dict, user_id: int = Depends(get_current_user)):
    conn = get_db()
    updates = []
    values = []
    for field in ["filter_hours", "filter_weekend", "filter_macro"]:
        if field in req:
            updates.append(f"{field}=?")
            values.append(1 if req[field] else 0)
    if updates:
        values.append(user_id)
        conn.execute(f"UPDATE bot_config SET {','.join(updates)} WHERE user_id=?", values)
        conn.commit()
    conn.close()
    return {"message": "Filtres mis à jour"}

@app.put("/api/config/ai-continuous")
def toggle_ai_continuous(req: dict, user_id: int = Depends(get_current_user)):
    value = 1 if req.get("enabled") else 0
    conn = get_db()
    conn.execute("UPDATE bot_config SET ai_continuous=? WHERE user_id=?", (value, user_id))
    conn.commit()
    conn.close()
    status = "activée" if value else "désactivée"
    add_bot_log(user_id, f"🔄 Analyse IA continue {status}", "info")
    return {"ai_continuous": value}

@app.post("/api/bot/stop")
def stop_bot(user_id: int = Depends(get_current_user)):
    conn = get_db()
    conn.execute("UPDATE bot_config SET is_running=0 WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()
    # Annulation immediate de la tache en cours, sans attendre la fin du sleep(60)
    if user_id in scanning_tasks and not scanning_tasks[user_id].done():
        scanning_tasks[user_id].cancel()
        del scanning_tasks[user_id]
    if user_id in positions_tasks and not positions_tasks[user_id].done():
        positions_tasks[user_id].cancel()
        del positions_tasks[user_id]
    add_bot_log(user_id, "⏹️ Bot arrêté manuellement", "info")
    return {"message": "Bot arrêté"}

@app.get("/api/signals")
def get_signals(limit: int = 50, user_id: int = Depends(get_current_user)):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM signals WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
        (user_id, limit)
    ).fetchall()
    conn.close()
    signals = [dict(r) for r in rows]
    return {"signals": signals, "total": len(signals)}

@app.get("/api/prices")
async def get_prices():
    conn = get_db()
    rows = conn.execute("SELECT coin, price FROM prices").fetchall()
    conn.close()
    prices = {r["coin"]: r["price"] for r in rows}
    # Si pas de prix en DB, fetcher depuis Hyperliquid
    if not prices:
        try:
            async with httpx.AsyncClient() as client:
                all_prices = await fetch_all_metas(client)
                prices = all_prices
                # Sauvegarder en DB
                conn2 = get_db()
                for coin, price in all_prices.items():
                    conn2.execute("INSERT OR REPLACE INTO prices (coin, price, updated_at) VALUES (?,?,?)",
                                (coin, price, datetime.utcnow().isoformat()))
                conn2.commit()
                conn2.close()
        except:
            pass
    return {"prices": prices}

@app.get("/api/positions")
async def get_positions(user_id: int = Depends(get_current_user)):
    conn = get_db()
    user = conn.execute("SELECT wallet FROM users WHERE id=?", (user_id,)).fetchone()
    conn.close()
    if not user["wallet"]:
        return []
    async with httpx.AsyncClient() as client:
        return await fetch_positions(client, user["wallet"])

@app.get("/api/stats")
def get_stats(user_id: int = Depends(get_current_user)):
    conn = get_db()
    signals = conn.execute(
        "SELECT action, confidence, risk_reward FROM signals WHERE user_id=? AND action!='WAIT'",
        (user_id,)
    ).fetchall()
    conn.close()
    total = len(signals)
    longs = sum(1 for s in signals if s["action"] == "LONG")
    shorts = sum(1 for s in signals if s["action"] == "SHORT")
    avg_conf = int(sum(s["confidence"] for s in signals) / total) if total else 0
    avg_rr = round(sum(s["risk_reward"] or 0 for s in signals) / total, 2) if total else 0
    return {"total": total, "longs": longs, "shorts": shorts, "avg_confidence": avg_conf, "avg_rr": avg_rr}

# ── PAPER TRADING ───────────────────────────────────────────
class PaperTradeRequest(BaseModel):
    signal_id: int
    size_usdc: float = 50.0

class PaperCloseRequest(BaseModel):
    trade_id: int
    reason: str = "MANUEL"

def ensure_portfolio(user_id: int, conn):
    existing = conn.execute("SELECT * FROM paper_portfolio WHERE user_id=?", (user_id,)).fetchone()
    if not existing:
        conn.execute("INSERT INTO paper_portfolio (user_id) VALUES (?)", (user_id,))
        conn.commit()

@app.get("/api/paper/portfolio")
def get_paper_portfolio(user_id: int = Depends(get_current_user)):
    conn = get_db()
    ensure_portfolio(user_id, conn)
    portfolio = conn.execute("SELECT * FROM paper_portfolio WHERE user_id=?", (user_id,)).fetchone()
    trades = conn.execute(
        "SELECT * FROM paper_trades WHERE user_id=? AND status='OPEN' ORDER BY opened_at DESC",
        (user_id,)
    ).fetchall()
    closed = conn.execute(
        "SELECT * FROM paper_trades WHERE user_id=? AND status='CLOSED' ORDER BY closed_at DESC LIMIT 20",
        (user_id,)
    ).fetchall()
    conn.close()
    open_trades = [dict(t) for t in trades]
    closed_trades = [dict(t) for t in closed]
    total_pnl = sum(t["pnl"] for t in open_trades)
    # Capital total = balance disponible + marges bloquees dans les trades ouverts
    open_margin = sum(t["size_usdc"] for t in open_trades)
    total_capital = portfolio["balance"] + open_margin
    performance_pct = round((total_capital + total_pnl - portfolio["initial_balance"]) / portfolio["initial_balance"] * 100, 2)
    return {
        "balance": portfolio["balance"],
        "initial_balance": portfolio["initial_balance"],
        "total_pnl": round(total_pnl, 2),
        "total_pnl_pct": performance_pct,
        "open_trades": open_trades,
        "closed_trades": closed_trades,
    }

@app.post("/api/paper/trade")
def open_paper_trade(req: PaperTradeRequest, user_id: int = Depends(get_current_user)):
    conn = get_db()
    ensure_portfolio(user_id, conn)
    # Get signal
    sig = conn.execute("SELECT * FROM signals WHERE id=? AND user_id=?", (req.signal_id, user_id)).fetchone()
    if not sig:
        conn.close()
        raise HTTPException(status_code=404, detail="Signal introuvable")
    # Get current price
    price_row = conn.execute("SELECT price FROM prices WHERE coin=?", (sig["coin"],)).fetchone()
    if not price_row:
        conn.close()
        raise HTTPException(status_code=400, detail="Prix non disponible")
    # Check balance
    portfolio = conn.execute("SELECT balance FROM paper_portfolio WHERE user_id=?", (user_id,)).fetchone()
    if portfolio["balance"] < req.size_usdc:
        conn.close()
        raise HTTPException(status_code=400, detail="Solde insuffisant")
    # Open trade
    conn.execute("""
        INSERT INTO paper_trades (user_id, coin, action, entry_price, current_price, size_usdc, leverage,
        stop_loss, take_profit1, take_profit2, signal_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (user_id, sig["coin"], sig["action"], sig["entry"] or price_row["price"],
          price_row["price"], req.size_usdc, sig["leverage"] or 1,
          sig["stop_loss"], sig["take_profit1"], sig["take_profit2"], sig["id"]))
    conn.execute("UPDATE paper_portfolio SET balance=balance-? WHERE user_id=?", (req.size_usdc, user_id))
    conn.commit()
    conn.close()
    return {"message": f"Trade {sig['action']} {sig['coin']} ouvert pour {req.size_usdc} USDC"}

@app.post("/api/paper/close")
def close_paper_trade(req: PaperCloseRequest, user_id: int = Depends(get_current_user)):
    conn = get_db()
    trade = conn.execute(
        "SELECT * FROM paper_trades WHERE id=? AND user_id=? AND status='OPEN'",
        (req.trade_id, user_id)
    ).fetchone()
    if not trade:
        conn.close()
        raise HTTPException(status_code=404, detail="Trade introuvable")
    price_row = conn.execute("SELECT price FROM prices WHERE coin=?", (trade["coin"],)).fetchone()
    cur_price = price_row["price"] if price_row else trade["entry_price"]
    direction = 1 if trade["action"] == "LONG" else -1
    pnl = (cur_price - trade["entry_price"]) / trade["entry_price"] * trade["size_usdc"] * trade["leverage"] * direction
    conn.execute("""
        UPDATE paper_trades SET status='CLOSED', current_price=?, pnl=?, pnl_pct=?,
        closed_at=?, close_reason=? WHERE id=?
    """, (cur_price, round(pnl,2), round(pnl/trade["size_usdc"]*100,2),
          datetime.utcnow().isoformat(), req.reason, req.trade_id))
    conn.execute("UPDATE paper_portfolio SET balance=balance+?+? WHERE user_id=?",
                (trade["size_usdc"], round(pnl,2), user_id))
    conn.commit()
    conn.close()
    return {"message": f"Trade fermé avec PnL: {round(pnl,2)} USDC"}

@app.post("/api/paper/reset")
def reset_paper_portfolio(user_id: int = Depends(get_current_user)):
    conn = get_db()
    # Reset portfolio
    conn.execute("UPDATE paper_portfolio SET balance=1000.0, initial_balance=1000.0, reset_at=? WHERE user_id=?", 
        (datetime.utcnow().isoformat(), user_id))
    # Supprimer tous les trades
    conn.execute("DELETE FROM paper_trades WHERE user_id=?", (user_id,))
    # Supprimer tous les signaux
    conn.execute("DELETE FROM signals WHERE user_id=?", (user_id,))
    # Supprimer tous les logs persistants
    conn.execute("DELETE FROM bot_activity_log WHERE user_id=?", (user_id,))
    # Reset confiance dynamique
    conn.execute("DELETE FROM coin_confidence WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()
    add_bot_log(user_id, "🔄 Reset complet effectué — Capital: 1000 USDC | Historique effacé", "success")
    return {"message": "Reset complet effectué — Capital réinitialisé à 1000 USDC"}

@app.put("/api/paper/update")
async def update_paper_trades(user_id: int = Depends(get_current_user)):
    conn = get_db()
    trades = conn.execute(
        "SELECT * FROM paper_trades WHERE user_id=? AND status='OPEN'", (user_id,)
    ).fetchall()
    for trade in trades:
        price_row = conn.execute("SELECT price FROM prices WHERE coin=?", (trade["coin"],)).fetchone()
        if not price_row: continue
        cur = price_row["price"]
        direction = 1 if trade["action"] == "LONG" else -1
        pnl = (cur - trade["entry_price"]) / trade["entry_price"] * trade["size_usdc"] * trade["leverage"] * direction
        close_reason = None
        # SL technique désactivé - Max Loss gère la protection
        if trade["take_profit2"]:
            if trade["action"] == "LONG" and cur >= trade["take_profit2"]: close_reason = "TP2"
            elif trade["action"] == "SHORT" and cur <= trade["take_profit2"]: close_reason = "TP2"
        elif trade["take_profit1"]:
            if trade["action"] == "LONG" and cur >= trade["take_profit1"]: close_reason = "TP1"
            elif trade["action"] == "SHORT" and cur <= trade["take_profit1"]: close_reason = "TP1"
        if close_reason:
            conn.execute("""UPDATE paper_trades SET status='CLOSED', current_price=?, pnl=?, pnl_pct=?,
                closed_at=?, close_reason=? WHERE id=?""",
                (cur, round(pnl,2), round(pnl/trade["size_usdc"]*100,2),
                 datetime.utcnow().isoformat(), close_reason, trade["id"]))
            conn.execute("UPDATE paper_portfolio SET balance=balance+?+? WHERE user_id=?",
                        (trade["size_usdc"], round(pnl,2), user_id))
        else:
            conn.execute("UPDATE paper_trades SET current_price=?, pnl=?, pnl_pct=? WHERE id=?",
                        (cur, round(pnl,2), round(pnl/trade["size_usdc"]*100,2), trade["id"]))
    conn.commit()
    conn.close()
    return {"message": "Trades mis à jour"}

# ── REINITIALISATION COMPLETE ────────────────────────────────
@app.post("/api/reset-all")
def reset_all(user_id: int = Depends(get_current_user)):
    conn = get_db()
    # Fermer tous les trades paper ouverts
    conn.execute("UPDATE paper_trades SET status='CLOSED', close_reason='RESET', closed_at=? WHERE user_id=? AND status='OPEN'",
                (datetime.utcnow().isoformat(), user_id))
    # Supprimer tout l'historique paper
    conn.execute("DELETE FROM paper_trades WHERE user_id=?", (user_id,))
    # Remettre le portefeuille a zero
    conn.execute("UPDATE paper_portfolio SET balance=1000.0, initial_balance=1000.0 WHERE user_id=?", (user_id,))
    # Supprimer tous les signaux
    conn.execute("DELETE FROM signals WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()
    return {"message": "Reinitialisation complete — portefeuille remis a 1000 USDC, signaux et historique effaces"}

# ── LOGS BOT ─────────────────────────────────────────────────
@app.get("/api/market-data")
def get_market_data():
    """Retourne les données de marché pré-calculées depuis le cache"""
    return {"data": market_data_cache, "coins": len(market_data_cache)}

@app.get("/api/sessions")
def get_sessions(user_id: int = Depends(get_current_user)):
    conn = get_db()
    sessions = conn.execute("""
        SELECT ts.*, 
            (SELECT COUNT(DISTINCT coin) FROM paper_trades pt 
             WHERE pt.user_id=ts.user_id AND date(pt.opened_at)=ts.session_date) as coins_count,
            (SELECT COUNT(*) FROM paper_trades pt 
             WHERE pt.user_id=ts.user_id AND date(pt.opened_at)=ts.session_date AND pt.status='OPEN') as pending_trades
        FROM trading_sessions ts
        WHERE ts.user_id=?
        ORDER BY ts.session_date DESC LIMIT 30
    """, (user_id,)).fetchall()
    
    # Stats par actif pour chaque session
    result = []
    for s in sessions:
        s = dict(s)
        by_coin = conn.execute("""
            SELECT coin,
                COUNT(*) as total,
                SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN pnl<=0 THEN 1 ELSE 0 END) as losses,
                SUM(pnl) as net,
                SUM(CASE WHEN pnl>0 THEN pnl ELSE 0 END) as gains,
                SUM(CASE WHEN pnl<=0 THEN pnl ELSE 0 END) as pertes
            FROM paper_trades
            WHERE user_id=? AND status='CLOSED' AND date(closed_at)=?
            GROUP BY coin ORDER BY net DESC
        """, (user_id, s["session_date"])).fetchall()
        s["by_coin"] = [dict(r) for r in by_coin]
        result.append(s)
    
    conn.close()
    return {"sessions": result}

@app.get("/api/bilan")
def get_bilan(user_id: int = Depends(get_current_user)):
    conn = get_db()
    # Balance actuelle et initiale
    portfolio = conn.execute("SELECT balance, initial_balance, reset_at FROM paper_portfolio WHERE user_id=?", (user_id,)).fetchone()
    
    # Stats aujourd'hui (UTC)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    today_stats = conn.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) as gains,
            SUM(CASE WHEN pnl <= 0 THEN pnl ELSE 0 END) as pertes,
            SUM(pnl) as net
        FROM paper_trades
        WHERE user_id=? AND status='CLOSED' AND date(closed_at)=?
    """, (user_id, today)).fetchone()
    
    # Stats totales depuis le début
    total_stats = conn.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) as gains,
            SUM(CASE WHEN pnl <= 0 THEN pnl ELSE 0 END) as pertes,
            SUM(pnl) as net
        FROM paper_trades
        WHERE user_id=? AND status='CLOSED'
    """, (user_id,)).fetchone()
    
    # Trades ouverts PnL
    open_pnl = conn.execute("""
        SELECT SUM(pnl) as open_pnl, COUNT(*) as open_count, SUM(size_usdc) as open_margin
        FROM paper_trades WHERE user_id=? AND status='OPEN'
    """, (user_id,)).fetchone()
    
    # Stats par jour (7 derniers)
    daily = conn.execute("""
        SELECT date(closed_at) as day,
            COUNT(*) as total,
            SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses,
            SUM(pnl) as net
        FROM paper_trades
        WHERE user_id=? AND status='CLOSED'
        GROUP BY date(closed_at)
        ORDER BY day DESC LIMIT 7
    """, (user_id,)).fetchall()
    
    # Stats par actif
    by_coin = conn.execute("""
        SELECT coin,
            COUNT(*) as total,
            SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses,
            SUM(pnl) as net,
            AVG(CASE WHEN pnl > 0 THEN pnl ELSE NULL END) as avg_gain,
            AVG(CASE WHEN pnl <= 0 THEN pnl ELSE NULL END) as avg_loss,
            SUM(CAST((julianday(closed_at) - julianday(opened_at)) * 24 * 60 AS INTEGER)) as total_minutes
        FROM paper_trades
        WHERE user_id=? AND status='CLOSED'
        GROUP BY coin
        ORDER BY net DESC
    """, (user_id,)).fetchall()
    
    conn.close()
    
    balance = portfolio["balance"] if portfolio else 1000
    initial = portfolio["initial_balance"] if portfolio else 1000
    open_margin = open_pnl["open_margin"] or 0
    open_pnl_val = open_pnl["open_pnl"] or 0
    total_capital = balance + open_margin + open_pnl_val
    
    return {
        "balance": round(balance, 2),
        "initial_balance": round(initial, 2),
        "total_capital": round(total_capital, 2),
        "performance_pct": round((total_capital - initial) / initial * 100, 2),
        "open_pnl": round(open_pnl_val, 2),
        "open_count": open_pnl["open_count"] or 0,
        "reset_at": portfolio["reset_at"] if portfolio and portfolio["reset_at"] else None,
        "today": {
            "total": today_stats["total"] or 0,
            "wins": today_stats["wins"] or 0,
            "losses": today_stats["losses"] or 0,
            "gains": round(today_stats["gains"] or 0, 2),
            "pertes": round(today_stats["pertes"] or 0, 2),
            "net": round(today_stats["net"] or 0, 2),
            "win_rate": round((today_stats["wins"] or 0) / max(today_stats["total"] or 1, 1) * 100, 1)
        },
        "total": {
            "total": total_stats["total"] or 0,
            "wins": total_stats["wins"] or 0,
            "losses": total_stats["losses"] or 0,
            "gains": round(total_stats["gains"] or 0, 2),
            "pertes": round(total_stats["pertes"] or 0, 2),
            "net": round(total_stats["net"] or 0, 2),
            "win_rate": round((total_stats["wins"] or 0) / max(total_stats["total"] or 1, 1) * 100, 1)
        },
        "daily": [dict(r) for r in daily],
        "by_coin": [{
            "coin": r["coin"],
            "total": r["total"],
            "wins": r["wins"],
            "losses": r["losses"],
            "net": round(r["net"] or 0, 2),
            "avg_gain": round(r["avg_gain"] or 0, 2),
            "avg_loss": round(r["avg_loss"] or 0, 2),
            "win_rate": round((r["wins"] or 0) / max(r["total"] or 1, 1) * 100, 1),
            "total_minutes": int(r["total_minutes"] or 0)
        } for r in by_coin]
    }

@app.get("/api/stats/daily")
def get_daily_stats(user_id: int = Depends(get_current_user)):
    conn = get_db()
    # Stats par jour - trades gagnants et perdants
    rows = conn.execute("""
        SELECT 
            date(closed_at) as day,
            COUNT(*) as total,
            SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) as total_wins_usdc,
            SUM(CASE WHEN pnl <= 0 THEN pnl ELSE 0 END) as total_losses_usdc,
            SUM(pnl) as net_pnl,
            close_reason
        FROM paper_trades
        WHERE user_id=? AND status='CLOSED'
        GROUP BY date(closed_at)
        ORDER BY day DESC
        LIMIT 30
    """, (user_id,)).fetchall()
    
    # Detail wins
    wins = conn.execute("""
        SELECT coin, action, pnl, pnl_pct, entry_price, current_price, 
               close_reason, closed_at, leverage
        FROM paper_trades
        WHERE user_id=? AND status='CLOSED' AND pnl > 0
        ORDER BY closed_at DESC
    """, (user_id,)).fetchall()
    
    # Detail losses
    losses = conn.execute("""
        SELECT coin, action, pnl, pnl_pct, entry_price, current_price,
               close_reason, closed_at, leverage
        FROM paper_trades
        WHERE user_id=? AND status='CLOSED' AND pnl <= 0
        ORDER BY closed_at DESC
    """, (user_id,)).fetchall()
    
    conn.close()
    return {
        "daily": [dict(r) for r in rows],
        "wins": [dict(r) for r in wins],
        "losses": [dict(r) for r in losses],
        "summary": {
            "total_wins": len(wins),
            "total_losses": len(losses),
            "total_wins_usdc": round(sum(r["pnl"] for r in wins), 2),
            "total_losses_usdc": round(sum(r["pnl"] for r in losses), 2),
            "win_rate": round(len(wins) / (len(wins) + len(losses)) * 100, 1) if (wins or losses) else 0
        }
    }

@app.get("/api/bot/logs")
def get_bot_logs(user_id: int = Depends(get_current_user), persistent: bool = False):
    if persistent:
        # Logs persistants depuis la DB
        conn = get_db()
        rows = conn.execute(
            """SELECT level, message, created_at FROM bot_activity_log 
               WHERE user_id=? ORDER BY id DESC LIMIT 200""",
            (user_id,)
        ).fetchall()
        conn.close()
        logs = [{"time": r["created_at"][11:19], "message": r["message"], "level": r["level"]} for r in rows]
        return {"logs": logs}
    # Logs en memoire (session courante)
    logs = bot_logs_memory.get(user_id, [])
    return {"logs": logs}

# ── NETTOYAGE ────────────────────────────────────────────────
@app.post("/api/cleanup")
def cleanup_signals(user_id: int = Depends(get_current_user)):
    conn = get_db()
    # Recuperer les coins actifs
    config = conn.execute("SELECT active_coins FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
    active_coins = json.loads(config["active_coins"]) if config else []
    # Supprimer les signaux des actifs desactives
    if active_coins:
        placeholders = ",".join("?" * len(active_coins))
        conn.execute(f"DELETE FROM signals WHERE user_id=? AND coin NOT IN ({placeholders})",
            [user_id] + active_coins)
        inactive_deleted = conn.execute("SELECT changes()").fetchone()[0]
    else:
        inactive_deleted = 0
    # Supprimer les doublons — garder seulement le signal le plus récent par coin+action
    conn.execute("""
        DELETE FROM signals WHERE id NOT IN (
            SELECT MAX(id) FROM signals
            WHERE user_id=?
            GROUP BY coin, action, date(created_at)
        ) AND user_id=?
    """, (user_id, user_id))
    deleted = conn.execute("SELECT changes()").fetchone()[0]
    # Supprimer aussi les signaux de plus de 24h
    conn.execute("DELETE FROM signals WHERE user_id=? AND created_at < datetime('now', '-24 hours')", (user_id,))
    deleted2 = conn.execute("SELECT changes()").fetchone()[0]
    remaining = conn.execute("SELECT COUNT(*) FROM signals WHERE user_id=?", (user_id,)).fetchone()[0]
    conn.commit()
    conn.close()
    return {"message": f"{deleted + deleted2} signaux supprimés · {remaining} restants"}

# ── SERVIR L'INTERFACE ───────────────────────────────────────
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def serve_index():
    if os.path.exists("static/index.html"):
        return FileResponse("static/index.html")
    return {"message": "HyperBot AI API — Interface non trouvée"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
