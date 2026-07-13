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
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # Permet lectures/écritures simultanées
    conn.execute("PRAGMA busy_timeout=5000") # Attendre 5s si DB occupée
    return conn

def init_db():
    conn = get_db()
    # Migration: ajouter les nouvelles colonnes si elles n'existent pas
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN trading_mode TEXT DEFAULT 'paper'")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN ai_mode_paper TEXT DEFAULT 'ai'")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN pause_until TEXT")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN loss_streak_size INTEGER DEFAULT 3")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN pause_hours REAL DEFAULT 2.0")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN base_confidence REAL DEFAULT 60")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN conf_step1 REAL DEFAULT 72")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN conf_step2 REAL DEFAULT 82")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN conf_step3 REAL DEFAULT 90")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN rsi_oversold REAL DEFAULT 35")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN rsi_overbought REAL DEFAULT 65")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN volume_spike_mult REAL DEFAULT 1.5")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN btc_trend_threshold REAL DEFAULT 2.0")
        conn.commit()
    except: pass
    try:
        # Anti-corrélation : plafond de trades ouverts simultanément dans LA MÊME direction
        # (LONG ou SHORT), indépendant du plafond total max_open_trades. Les altcoins étant
        # très corrélés entre eux, plusieurs signaux SHORT (ou LONG) simultanés sur des coins
        # différents ne sont pas diversifiés — c'est le même pari répété. Plafond plus bas en
        # mode neutre (BTC calme, le risque de faux-signaux corrélés est le plus élevé),
        # plus haut en tendance BTC confirmée (le filtre de tendance bloque déjà la direction opposée).
        conn.execute("ALTER TABLE bot_config ADD COLUMN max_same_direction_neutral INTEGER DEFAULT 2")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN max_same_direction_trend INTEGER DEFAULT 3")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN trailing_activation_mult REAL DEFAULT 1.0")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN trailing_gap_usd REAL DEFAULT 0.3")
        conn.commit()
    except: pass
    try:
        # Plancher de protection : garantit qu'on ne rend jamais plus que
        # (peak_pnl_atteint - trail_gap) UNE FOIS que le pic a dépassé ce seuil.
        # Ex: qp_lock_trigger_usd=1.5 et quick_profit_usd=1.1 → dès que le pic
        # dépasse 1.5$, le trade ne peut plus se fermer sous 1.1$ de PnL,
        # même si le trailing_gap seul aurait autorisé une chute plus large.
        conn.execute("ALTER TABLE bot_config ADD COLUMN qp_lock_trigger_usd REAL DEFAULT 1.5")
        conn.commit()
    except: pass
    try:
        # ── Migration vers un système 100% en % du prix d'entrée (indépendant du levier/taille) ──
        # Remplace quick_profit_usd/max_loss_usd/trailing_gap_usd/qp_lock_trigger_usd,
        # retirés de l'interface. Défauts calculés à partir des anciennes valeurs $ (levier x3, position 8%/1000$).
        conn.execute("ALTER TABLE bot_config ADD COLUMN quick_profit_pct REAL DEFAULT 0.46")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN max_loss_pct REAL DEFAULT 0.31")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN trailing_gap_pct REAL DEFAULT 0.42")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN qp_lock_trigger_pct REAL DEFAULT 0.63")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN rsi_period INTEGER DEFAULT 14")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN macd_fast INTEGER DEFAULT 12")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN macd_slow INTEGER DEFAULT 26")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN macd_signal INTEGER DEFAULT 9")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN bb_period INTEGER DEFAULT 20")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN bb_stddev REAL DEFAULT 2")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN atr_period INTEGER DEFAULT 14")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN hours_creuses_start INTEGER DEFAULT 21")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN hours_creuses_end INTEGER DEFAULT 23")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN macro_blackout_before_min INTEGER DEFAULT 120")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE bot_config ADD COLUMN macro_blackout_after_min INTEGER DEFAULT 60")
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
        conn.execute("ALTER TABLE trading_sessions ADD COLUMN capital_start REAL DEFAULT 1000.0")
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
    try:
        # Pic du mouvement de prix brut (%, non-levierisé, ajusté selon la direction) —
        # utilisé pour les décisions Trailing/Max Loss en % (indépendant de la taille et du levier).
        conn.execute("ALTER TABLE paper_trades ADD COLUMN peak_price_pct REAL DEFAULT 0")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE paper_trades ADD COLUMN session_date TEXT")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE paper_trades ADD COLUMN tp1_pnl REAL DEFAULT 0")
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
        # Fusion Quick Profit / Trailing Take Profit (voir diagnostic bug) :
        # trailing actif dès que le PnL dépasse quick_profit_usd (mult=1.0), gap resserré à 0.3$.
        # Uniquement pour les configs encore sur l'ancien défaut (1.5/0.5) — ne touche pas
        # aux réglages déjà personnalisés manuellement par l'utilisateur.
        conn.execute("UPDATE bot_config SET trailing_activation_mult=1.0 WHERE trailing_activation_mult=1.5 OR trailing_activation_mult IS NULL")
        conn.commit()
    except: pass
    try:
        conn.execute("UPDATE bot_config SET trailing_gap_usd=0.3 WHERE trailing_gap_usd=0.5 OR trailing_gap_usd IS NULL")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE paper_trades ADD COLUMN is_live INTEGER DEFAULT 0")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE paper_trades ADD COLUMN hl_sl_oid INTEGER")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE paper_trades ADD COLUMN hl_size REAL")
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
            consecutive_wins INTEGER DEFAULT 0,
            updated_at TEXT,
            PRIMARY KEY (user_id, coin, action)
        )""")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE coin_confidence ADD COLUMN consecutive_wins INTEGER DEFAULT 0")
        conn.commit()
    except: pass
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS coin_pause (
            user_id INTEGER,
            coin TEXT,
            paused_until TEXT,
            reason TEXT,
            PRIMARY KEY (user_id, coin)
        )""")
        conn.commit()
    except: pass
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS ai_usage (
            user_id INTEGER,
            date TEXT,
            calls INTEGER DEFAULT 0,
            input_tokens INTEGER DEFAULT 0,
            output_tokens INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, date)
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
        conn.execute("ALTER TABLE users ADD COLUMN sendgrid_key TEXT DEFAULT ''")
        conn.commit()
    except: pass
    try:
        conn.execute("ALTER TABLE users ADD COLUMN alert_email TEXT DEFAULT ''")
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
            sendgrid_key TEXT DEFAULT '',
            alert_email TEXT DEFAULT '',
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
            ai_mode_paper TEXT DEFAULT 'ai',
            pause_until TEXT,
            loss_streak_size INTEGER DEFAULT 3,
            pause_hours REAL DEFAULT 2.0,
            base_confidence REAL DEFAULT 60,
            conf_step1 REAL DEFAULT 72,
            conf_step2 REAL DEFAULT 82,
            conf_step3 REAL DEFAULT 90,
            rsi_oversold REAL DEFAULT 35,
            rsi_overbought REAL DEFAULT 65,
            volume_spike_mult REAL DEFAULT 1.5,
            btc_trend_threshold REAL DEFAULT 2.0,
            max_same_direction_neutral INTEGER DEFAULT 2,
            max_same_direction_trend INTEGER DEFAULT 3,
            trailing_activation_mult REAL DEFAULT 1.0,
            trailing_gap_usd REAL DEFAULT 0.3,
            qp_lock_trigger_usd REAL DEFAULT 1.5,
            quick_profit_pct REAL DEFAULT 0.46,
            max_loss_pct REAL DEFAULT 0.31,
            trailing_gap_pct REAL DEFAULT 0.42,
            qp_lock_trigger_pct REAL DEFAULT 0.63,
            rsi_period INTEGER DEFAULT 14,
            macd_fast INTEGER DEFAULT 12,
            macd_slow INTEGER DEFAULT 26,
            macd_signal INTEGER DEFAULT 9,
            bb_period INTEGER DEFAULT 20,
            bb_stddev REAL DEFAULT 2,
            atr_period INTEGER DEFAULT 14,
            hours_creuses_start INTEGER DEFAULT 21,
            hours_creuses_end INTEGER DEFAULT 23,
            macro_blackout_before_min INTEGER DEFAULT 120,
            macro_blackout_after_min INTEGER DEFAULT 60,
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
            session_date TEXT,
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

def calc_macd(prices, fast=12, slow=26, signal=9):
    e12 = calc_ema(prices, fast)
    e26 = calc_ema(prices, slow)
    if not e12 or not e26:
        return None
    off = len(e12) - len(e26)
    macd_line = [e12[i+off] - v for i, v in enumerate(e26)]
    sig = calc_ema(macd_line, signal)
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

# ── HYPERLIQUID LIVE TRADING (ordres réels via API wallet) ───
# ⚠️ Utilise UNIQUEMENT la clé privée de l'API wallet (agent), jamais celle du wallet principal.
# La clé privée n'est JAMAIS stockée en base : uniquement via variable d'environnement.
HL_AGENT_PRIVATE_KEY = os.environ.get("HL_AGENT_PRIVATE_KEY", "")
HL_USE_TESTNET = os.environ.get("HL_USE_TESTNET", "true").lower() != "false"

try:
    import eth_account
    from hyperliquid.info import Info as HLInfo
    from hyperliquid.exchange import Exchange as HLExchange
    from hyperliquid.utils import constants as hl_constants
    HL_SDK_AVAILABLE = True
except ImportError:
    HL_SDK_AVAILABLE = False

def hl_base_url():
    return hl_constants.TESTNET_API_URL if HL_USE_TESTNET else hl_constants.MAINNET_API_URL

def get_hl_exchange(account_address: str):
    """Crée un client Exchange signé par l'API wallet (agent), agissant pour le compte account_address."""
    if not HL_SDK_AVAILABLE:
        raise RuntimeError("hyperliquid-python-sdk non installé (pip install hyperliquid-python-sdk)")
    if not HL_AGENT_PRIVATE_KEY:
        raise RuntimeError("HL_AGENT_PRIVATE_KEY non configurée dans les variables d'environnement")
    wallet = eth_account.Account.from_key(HL_AGENT_PRIVATE_KEY)
    return HLExchange(wallet, hl_base_url(), account_address=account_address)

def get_hl_account_value(account_address: str) -> float:
    """Récupère la valeur réelle du compte (equity) sur Hyperliquid — utilisé pour le sizing en mode live."""
    if not HL_SDK_AVAILABLE or not account_address:
        return 0.0
    try:
        info = HLInfo(hl_base_url(), skip_ws=True)
        state = info.user_state(account_address)
        return float(state["marginSummary"]["accountValue"])
    except Exception as e:
        print(f"HL account_value error: {e}")
        return 0.0

def hl_open_position(account_address: str, coin: str, action: str, size_usdc: float,
                      leverage: int, cur_price: float, max_loss_pct: float):
    """Ouvre une position réelle sur Hyperliquid (market order) puis pose un SL de sécurité
    (trigger order, reduce_only) sur l'exchange — filet en cas de défaillance du bot.
    Retourne (coin_size, sl_oid) ou lève une exception."""
    exchange = get_hl_exchange(account_address)
    is_buy = (action == "LONG")
    coin_size = round((size_usdc * leverage) / cur_price, 4)
    if coin_size <= 0:
        raise ValueError("Taille de position calculée nulle ou négative")

    result = exchange.market_open(coin, is_buy, coin_size, slippage=0.01)
    if result.get("status") != "ok":
        raise RuntimeError(f"Échec ouverture position Hyperliquid: {result}")

    # Prix de fill réel (si l'exchange le renvoie) — plus fiable qu'une estimation locale
    # potentiellement périmée. Fallback sur cur_price si la structure est inattendue.
    fill_price = cur_price
    try:
        statuses = result["response"]["data"]["statuses"]
        if statuses and "filled" in statuses[0]:
            fill_price = float(statuses[0]["filled"]["avgPx"])
    except Exception:
        pass

    # SL de sécurité large — le bot ferme normalement bien avant via Trailing Profit/Max Loss (en %).
    # Ce stop n'est qu'un filet en cas de panne/déconnexion du bot. Marge = 3x le Max Loss configuré (en % de prix).
    safety_move_pct = max(max_loss_pct, 0.1) * 3 / 100
    sl_price = round(fill_price * (1 - safety_move_pct), 6) if is_buy else round(fill_price * (1 + safety_move_pct), 6)

    sl_oid = None
    try:
        sl_result = exchange.order(
            coin, not is_buy, coin_size, sl_price,
            {"trigger": {"triggerPx": sl_price, "isMarket": True, "tpsl": "sl"}},
            reduce_only=True,
        )
        if sl_result.get("status") == "ok":
            statuses = sl_result["response"]["data"]["statuses"]
            if statuses and "resting" in statuses[0]:
                sl_oid = statuses[0]["resting"]["oid"]
    except Exception as e:
        print(f"⚠️ SL de sécurité Hyperliquid non posé pour {coin}: {e}")

    return coin_size, sl_oid, fill_price

def hl_close_position(account_address: str, coin: str, sl_oid: Optional[int] = None):
    """Ferme une position réelle sur Hyperliquid (market order) et annule le SL de sécurité associé."""
    exchange = get_hl_exchange(account_address)
    if sl_oid:
        try:
            exchange.cancel(coin, sl_oid)
        except Exception as e:
            print(f"⚠️ Annulation SL Hyperliquid échouée pour {coin} (oid={sl_oid}): {e}")
    return exchange.market_close(coin)

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

# Tarifs Claude Sonnet 4.6 (Anthropic API) — $ par million de tokens
AI_PRICE_INPUT_PER_M = 3.0
AI_PRICE_OUTPUT_PER_M = 15.0

def record_ai_usage(user_id: int, input_tokens: int, output_tokens: int):
    """Enregistre l'usage réel de l'API IA (tokens retournés par Anthropic) pour suivi de coût"""
    try:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        conn = get_db()
        conn.execute("""INSERT INTO ai_usage (user_id, date, calls, input_tokens, output_tokens)
            VALUES (?,?,1,?,?)
            ON CONFLICT(user_id, date) DO UPDATE SET
                calls = calls + 1,
                input_tokens = input_tokens + excluded.input_tokens,
                output_tokens = output_tokens + excluded.output_tokens""",
            (user_id, today, input_tokens or 0, output_tokens or 0))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Erreur record_ai_usage: {e}")

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

def analyze_with_rules(coin: str, tech: dict, price: float, max_loss_usd: float = 0.75, size_usdc: float = 50.0) -> dict:
    """Décision 100% gratuite basée sur des règles techniques fixes (RSI/MACD/EMA/Volume) —
    utilisée en mode Paper quand l'utilisateur ne veut pas payer d'appels IA sur des trades simulés.
    Reproduit la même heuristique que celle suggérée à l'IA (RSI<25=LONG, RSI>75=SHORT, etc.)
    mais de façon 100% mécanique et gratuite. SL calculé pour correspondre à 1.5× Max Loss
    (cohérent avec la protection $ réelle, pas de conflit) ; TP1/TP2 restent basés sur l'ATR."""
    rsi = tech.get("rsi") or 50
    atr = tech.get("atr") or price * 0.01
    ema20, ema50, ema200 = tech.get("ema20"), tech.get("ema50"), tech.get("ema200")
    macd_bull, macd_bear = tech.get("macd_bull"), tech.get("macd_bear")
    vol_trend = tech.get("volume_trend")

    ema_bull = bool(ema20 and ema50 and ema200 and ema20 > ema50 > ema200)
    ema_bear = bool(ema20 and ema50 and ema200 and ema20 < ema50 < ema200)

    action, confidence, signals = "WAIT", 50, []

    if rsi < 25:
        action, confidence = "LONG", 78
        signals.append(f"RSI {rsi} survente forte")
    elif rsi > 75:
        action, confidence = "SHORT", 78
        signals.append(f"RSI {rsi} surachat fort")
    elif rsi <= 45:
        action, confidence = "LONG", 64
        signals.append(f"RSI {rsi} zone de rebond")
    elif rsi >= 55:
        action, confidence = "SHORT", 64
        signals.append(f"RSI {rsi} zone de repli")

    if action == "LONG":
        if macd_bull:
            confidence += 8; signals.append("MACD haussier confirmé")
        elif macd_bear:
            confidence -= 12; signals.append("MACD contredit (baissier)")
        if ema_bull:
            confidence += 6; signals.append("Structure EMA haussière")
        elif ema_bear:
            confidence -= 10
    elif action == "SHORT":
        if macd_bear:
            confidence += 8; signals.append("MACD baissier confirmé")
        elif macd_bull:
            confidence -= 12; signals.append("MACD contredit (haussier)")
        if ema_bear:
            confidence += 6; signals.append("Structure EMA baissière")
        elif ema_bull:
            confidence -= 10

    if vol_trend == "SPIKE":
        confidence += 5; signals.append("pic de volume")

    confidence = max(0, min(95, confidence))

    if action == "WAIT" or confidence < 55:
        return {
            "action": "WAIT", "confidence": confidence, "entry": price,
            "stopLoss": price, "takeProfit1": price, "takeProfit2": price,
            "leverage": 1, "positionSize": 5,
            "reasoning": "Règles techniques (mode Paper, sans IA) : pas de signal net",
            "keySignals": signals, "riskReward": 0, "timeframe": "court-terme"
        }

    leverage = 3 if confidence >= 70 else 2
    # SL cohérent avec 1.5× Max Loss ($) — évite tout conflit avec la protection dollar réelle
    sl_distance_pct = (max_loss_usd * 1.5) / max(size_usdc * leverage, 1)
    if action == "LONG":
        stop_loss = round(price * (1 - sl_distance_pct), 6)
        tp1 = round(price + atr * 2, 6)
        tp2 = round(price + atr * 3, 6)
    else:
        stop_loss = round(price * (1 + sl_distance_pct), 6)
        tp1 = round(price - atr * 2, 6)
        tp2 = round(price - atr * 3, 6)

    return {
        "action": action,
        "confidence": confidence,
        "entry": price,
        "stopLoss": stop_loss,
        "takeProfit1": tp1,
        "takeProfit2": tp2,
        "leverage": leverage,
        "positionSize": 10 if confidence >= 70 else 6,
        "reasoning": f"Règles techniques (mode Paper, sans IA) : {', '.join(signals) if signals else 'signal RSI'}",
        "keySignals": signals[:3] if signals else [f"RSI {rsi}"],
        "riskReward": 2.0,
        "timeframe": "court-terme"
    }

async def analyze_with_ai(client, user_id, coin, tech, ob, price, api_key):
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
        usage = data.get("usage", {})
        record_ai_usage(user_id, usage.get("input_tokens", 0), usage.get("output_tokens", 0))
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

def check_losing_streak(user_id: int, streak_size: int = 3) -> bool:
    """Détecte X pertes consécutives (tous actifs confondus, les plus récentes fermées)"""
    try:
        conn = get_db()
        recent = conn.execute(
            "SELECT pnl FROM paper_trades WHERE user_id=? AND status='CLOSED' ORDER BY closed_at DESC LIMIT ?",
            (user_id, streak_size)
        ).fetchall()
        conn.close()
        if len(recent) < streak_size:
            return False
        return all((r["pnl"] or 0) < 0 for r in recent)
    except Exception as e:
        print(f"⚠️ Erreur check_losing_streak: {e}")
        return False

def cleanup_orphan_signals(user_id: int):
    """Supprime automatiquement les signaux jamais tradés (orphelins) : signaux générés en
    mode Live (pas d'auto-exécution), ou refusés faute de solde/slot/position déjà ouverte.
    Marge de 60 min pour ne jamais supprimer un signal en cours de traitement."""
    try:
        conn = get_db()
        conn.execute("""
            DELETE FROM signals WHERE user_id=? AND id NOT IN (
                SELECT signal_id FROM paper_trades WHERE signal_id IS NOT NULL
            ) AND created_at < datetime('now', '-60 minutes')
        """, (user_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Erreur cleanup_orphan_signals: {e}")

# ── SYSTEME UNIQUE DE GESTION DES TRADES OUVERTS ──────────────
# Seul point de fermeture d'un paper_trade : Trailing Profit + Max Loss.
# Aucun autre emplacement du code ne doit fixer status='CLOSED' pour la logique
# de trading (les fermetures manuelles utilisateur et le reset restent à part).
# Le SL de sécurité réel posé sur Hyperliquid (mode live) est le seul autre
# déclencheur possible, côté exchange, en cas de défaillance du bot.
def manage_open_trade(user_id: int, trade: dict, cur: float, conn):
    """Évalue un trade ouvert et le ferme si Trailing Profit ou Max Loss est déclenché.
    Retourne un dict {"pnl":..., "close_reason":...} si fermé, sinon None.
    C'est la SEULE fonction autorisée à fermer un paper_trade automatiquement.

    Les seuils sont exprimés en % de mouvement de prix depuis l'entrée (indépendants
    du levier et de la taille de position) — comme un stop/take-profit classique posé
    sur le prix. Le PnL en $ reste calculé pour le portefeuille/les logs, mais toutes
    les décisions de fermeture se basent sur price_move_pct."""
    direction = 1 if trade["action"] == "LONG" else -1
    pnl = (cur - trade["entry_price"]) / trade["entry_price"] * trade["size_usdc"] * trade["leverage"] * direction
    price_move_pct = (cur - trade["entry_price"]) / trade["entry_price"] * direction * 100

    cfg_qp = conn.execute("""SELECT quick_profit_pct, max_loss_pct, trailing_activation_mult,
        trailing_gap_pct, qp_lock_trigger_pct FROM bot_config WHERE user_id=?""", (user_id,)).fetchone()
    quick_profit_pct = float(cfg_qp["quick_profit_pct"]) if cfg_qp and "quick_profit_pct" in cfg_qp.keys() and cfg_qp["quick_profit_pct"] else 0.46
    max_loss_pct = float(cfg_qp["max_loss_pct"]) if cfg_qp and "max_loss_pct" in cfg_qp.keys() and cfg_qp["max_loss_pct"] else 0.31
    trail_mult = float(cfg_qp["trailing_activation_mult"]) if cfg_qp and "trailing_activation_mult" in cfg_qp.keys() and cfg_qp["trailing_activation_mult"] else 1.0
    trail_gap_pct = float(cfg_qp["trailing_gap_pct"]) if cfg_qp and "trailing_gap_pct" in cfg_qp.keys() and cfg_qp["trailing_gap_pct"] else 0.42
    trail_trigger_pct = quick_profit_pct * trail_mult  # réglable via "Réglages avancés" (Activation trailing × QP)
    qp_lock_trigger_pct = float(cfg_qp["qp_lock_trigger_pct"]) if cfg_qp and "qp_lock_trigger_pct" in cfg_qp.keys() and cfg_qp["qp_lock_trigger_pct"] else 0.63

    peak_pnl = float(trade["peak_pnl"]) if trade["peak_pnl"] is not None else 0.0
    if pnl > peak_pnl:
        peak_pnl = pnl
        conn.execute("UPDATE paper_trades SET peak_pnl=? WHERE id=?", (peak_pnl, trade["id"]))
    peak_pct = float(trade["peak_price_pct"]) if trade.get("peak_price_pct") is not None else 0.0
    if price_move_pct > peak_pct:
        peak_pct = price_move_pct
        conn.execute("UPDATE paper_trades SET peak_price_pct=? WHERE id=?", (peak_pct, trade["id"]))

    close_reason = None
    # Deux protections à deux seuils DISTINCTS, pour laisser le trade s'exprimer avant de verrouiller :
    #  1. Trailing dynamique : pic% - trail_gap_pct, actif dès que le pic% dépasse trail_trigger_pct (≈1.1$)
    #     → suit le pic de près, mais ne verrouille rien de fixe, laisse le trade continuer à monter
    #  2. Plancher Quick Profit : garantit quick_profit_pct (≈1.1$) dès que le pic% dépasse qp_lock_trigger_pct
    #     (≈1.5$, seuil plus haut) → seulement une fois le trade nettement au-dessus de l'armement initial
    candidate_stops = []
    if peak_pct >= trail_trigger_pct:
        candidate_stops.append(("TRAILING_PROFIT", peak_pct - trail_gap_pct))
    if peak_pct >= qp_lock_trigger_pct:
        candidate_stops.append(("TRAILING_PROFIT", quick_profit_pct))

    if candidate_stops:
        reason, stop_level_pct = max(candidate_stops, key=lambda x: x[1])
        if price_move_pct <= stop_level_pct:
            close_reason = reason
            add_bot_log(user_id, f"🎯 {trade['coin']}: Trailing Profit +{round(pnl,2)} USDC (pic prix: +{round(peak_pct,3)}%, seuil: {round(stop_level_pct,3)}%) !", "success")
    if not close_reason and price_move_pct <= -max_loss_pct:
        close_reason = "MAX_LOSS"
        add_bot_log(user_id, f"🛡️ {trade['coin']}: Max Loss -{round(abs(pnl),2)} USDC (mouvement prix: {round(price_move_pct,3)}%) — protection activée", "warning")

    if not close_reason:
        conn.execute("UPDATE paper_trades SET current_price=?, pnl=?, pnl_pct=? WHERE id=?",
                    (cur, round(pnl,2), round(pnl/trade["size_usdc"]*100,2), trade["id"]))
        conn.commit()
        return None

    is_live = bool(trade.get("is_live"))
    if is_live:
        user_row = conn.execute("SELECT hl_wallet FROM users WHERE id=?", (user_id,)).fetchone()
        account_address = user_row["hl_wallet"] if user_row and "hl_wallet" in user_row.keys() else None
        try:
            hl_close_position(account_address, trade["coin"], trade.get("hl_sl_oid"))
            add_bot_log(user_id, f"🔴 {trade['coin']}: position réelle fermée sur Hyperliquid ({close_reason})", "success")
        except Exception as e:
            add_bot_log(user_id, f"⛔ {trade['coin']}: ÉCHEC de fermeture réelle sur Hyperliquid — {e} — vérifiez manuellement sur l'exchange !", "error")

    conn.execute("""UPDATE paper_trades SET status='CLOSED', current_price=?, pnl=?, pnl_pct=?,
        closed_at=?, close_reason=? WHERE id=?""",
        (cur, round(pnl,2), round(pnl/trade["size_usdc"]*100,2),
         datetime.utcnow().isoformat(), close_reason, trade["id"]))
    if not is_live:
        # Le solde réel vit sur Hyperliquid pour les trades live — on ne touche pas paper_portfolio
        conn.execute("UPDATE paper_portfolio SET balance=balance+?+? WHERE user_id=?",
                    (trade["size_usdc"], round(pnl,2), user_id))
    add_bot_log(user_id, f"🏁 {trade['coin']} fermé: {close_reason} | PnL: {round(pnl,2)} USDC", "success" if pnl >= 0 else "error")
    conn.commit()
    return {"pnl": pnl, "close_reason": close_reason}

async def finalize_closed_trade(user_id: int, trade: dict, pnl: float, conn):
    """Bookkeeping post-fermeture (confiance dynamique + stats de session temps réel).
    À appeler après un manage_open_trade qui a retourné un résultat non-None."""
    won = pnl > 0
    await update_coin_confidence(user_id, trade["coin"], trade["action"], won)
    try:
        trade_date = trade.get("session_date") or (trade.get("opened_at") or "")[:10] or datetime.utcnow().strftime("%Y-%m-%d")
        session_stats = conn.execute("""
            SELECT COUNT(*) as total,
                SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN pnl<=0 THEN 1 ELSE 0 END) as losses,
                SUM(pnl) as net
            FROM paper_trades
            WHERE user_id=? AND status='CLOSED'
            AND COALESCE(session_date, date(opened_at), date(closed_at))=?
        """, (user_id, trade_date)).fetchone()
        if session_stats:
            conn.execute("""UPDATE trading_sessions
                SET total_trades=?, wins=?, losses=?, net_pnl=?
                WHERE user_id=? AND session_date=?""",
                (session_stats["total"] or 0, session_stats["wins"] or 0,
                 session_stats["losses"] or 0, round(session_stats["net"] or 0, 2),
                 user_id, trade_date))
            conn.commit()
    except Exception:
        pass

async def scan_markets(user_id: int):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    config = conn.execute("SELECT * FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
    conn.close()

    if not user or not config:
        return

    active_coins = json.loads(config["active_coins"])
    api_key = user["api_key"]

    cleanup_orphan_signals(user_id)

    # === PAUSE GÉNÉRALE — MANUELLE UNIQUEMENT (plus de déclenchement auto ici,
    # remplacée par la pause par actif ci-dessous, plus précise) ===
    pause_until = config["pause_until"] if config and "pause_until" in config.keys() else None
    if pause_until:
        try:
            pu = datetime.fromisoformat(pause_until)
        except Exception:
            pu = None
        if pu and datetime.utcnow() < pu:
            add_bot_log(user_id, f"⏸️ Pause générale active jusqu'à {pu.strftime('%H:%M')} UTC (déclenchée manuellement)", "warning")
            return
        else:
            conn_p = get_db()
            conn_p.execute("UPDATE bot_config SET pause_until=NULL WHERE user_id=?", (user_id,))
            conn_p.commit()
            conn_p.close()
            add_bot_log(user_id, "▶️ Pause générale terminée — reprise des scans", "info")

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
        macro_before_min = config["macro_blackout_before_min"] if config and "macro_blackout_before_min" in config.keys() and config["macro_blackout_before_min"] else 120
        macro_data = await check_macro_calendar(user_id, finnhub_key)
        for event in macro_data.get("events", []):
            hours = event["hours_left"]
            name = event["event"]
            if hours <= 0:
                add_bot_log(user_id, f"📰 {name} vient d'être publié — volatilité possible", "warning")
            elif hours*60 <= macro_before_min:
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

    hc_start = config["hours_creuses_start"] if config and "hours_creuses_start" in config.keys() and config["hours_creuses_start"] is not None else 21
    hc_end = config["hours_creuses_end"] if config and "hours_creuses_end" in config.keys() and config["hours_creuses_end"] is not None else 23
    if filter_hours and hc_start <= hour_utc < hc_end:
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
        # Liste complète des actifs disponibles (30) — scannés dans tous les cas
        all_available_coins = ["BTC","ETH","SOL","ARB","AVAX","LINK","OP","INJ","TIA","BNB","HYPE","PAXG","TAO","WIF","JUP","PENDLE","EIGEN","RENDER","SUI","APT","SEI","DOGE","XRP","NEAR","FTM","AAVE","UNI","CRV","SUSHI","GMX"]

        # Prix : WebSocket temps réel en priorité (déjà en mémoire, gratuit), REST en fallback
        if ws_connected and ws_prices:
            prices = dict(ws_prices)
            missing = [c for c in all_available_coins if c not in prices]
            if missing:
                rest_prices = await fetch_all_metas(client)
                for c in missing:
                    if c in rest_prices:
                        prices[c] = rest_prices[c]
        else:
            prices = await fetch_all_metas(client)

        # Update prices en DB pour tous les actifs scannés (pas seulement les 7 "actifs")
        conn = get_db()
        for coin in all_available_coins:
            if coin in prices:
                conn.execute("INSERT OR REPLACE INTO prices (coin, price, updated_at) VALUES (?,?,?)",
                            (coin, prices[coin], datetime.utcnow().isoformat()))
        conn.commit()
        conn.close()

        # Tendance BTC globale sur 4h
        btc_trend = "neutral"
        btc_change = 0
        btc_thresh = config["btc_trend_threshold"] if config and "btc_trend_threshold" in config.keys() and config["btc_trend_threshold"] else 2.0
        btc_candles_4h = await fetch_candles(client, "BTC", "1h", 8)
        if btc_candles_4h and len(btc_candles_4h) >= 4:
            btc_open = float(btc_candles_4h[0]["c"])
            btc_close = float(btc_candles_4h[-1]["c"])
            btc_change = (btc_close - btc_open) / btc_open * 100
            if btc_change > btc_thresh:
                btc_trend = "bullish"
                add_bot_log(user_id, f"🟢 BTC HAUSSIER (+{btc_change:.1f}%) - mode tendance LONG actif", "success")
            elif btc_change < -btc_thresh:
                btc_trend = "bearish"
                add_bot_log(user_id, f"🔴 BTC BAISSIER ({btc_change:.1f}%) - mode tendance SHORT actif", "error")
            else:
                add_bot_log(user_id, f"⚪ BTC NEUTRE ({btc_change:.1f}%) - mode retournement actif", "info")

        # Analyze each coin — tous les actifs sont traités à égalité (le pré-filtre technique
        # décide seul qui mérite un appel IA, "active_coins" ne sert plus qu'à l'ordre de scan)
        opportunist_coins = [c for c in all_available_coins if c not in active_coins]
        
        # Scanner d'abord les coins actifs (priorité d'affichage), puis les autres
        coins_to_scan = active_coins + opportunist_coins

        # Seuils stratégie réglables (Paramètres > Réglages avancés), avec repli sur les défauts
        rsi_os = config["rsi_oversold"] if config and "rsi_oversold" in config.keys() and config["rsi_oversold"] else 35
        rsi_ob = config["rsi_overbought"] if config and "rsi_overbought" in config.keys() and config["rsi_overbought"] else 65
        vol_mult = config["volume_spike_mult"] if config and "volume_spike_mult" in config.keys() and config["volume_spike_mult"] else 1.5
        rsi_period = config["rsi_period"] if config and "rsi_period" in config.keys() and config["rsi_period"] else 14
        macd_fast = config["macd_fast"] if config and "macd_fast" in config.keys() and config["macd_fast"] else 12
        macd_slow = config["macd_slow"] if config and "macd_slow" in config.keys() and config["macd_slow"] else 26
        macd_sig = config["macd_signal"] if config and "macd_signal" in config.keys() and config["macd_signal"] else 9
        bb_period = config["bb_period"] if config and "bb_period" in config.keys() and config["bb_period"] else 20
        bb_stddev = config["bb_stddev"] if config and "bb_stddev" in config.keys() and config["bb_stddev"] else 2
        atr_period = config["atr_period"] if config and "atr_period" in config.keys() and config["atr_period"] else 14

        for coin in coins_to_scan:
            is_opportunist = coin not in active_coins
            if coin not in prices:
                continue

            if is_coin_paused(user_id, coin):
                continue

            price = prices[coin]
            candles_raw = await fetch_candles(client, coin)
            
            if not candles_raw or len(candles_raw) < 50:
                continue

            candles = [{"h":float(cd["h"]),"l":float(cd["l"]),"c":float(cd["c"]),"v":float(cd["v"])} for cd in candles_raw]
            closes = [cd["c"] for cd in candles]
            vols = [cd["v"] for cd in candles]

            e20 = calc_ema(closes, 20)
            e50 = calc_ema(closes, 50)
            e200 = calc_ema(closes, 200)
            macd = calc_macd(closes, int(macd_fast), int(macd_slow), int(macd_sig))
            bb = calc_bb(closes, int(bb_period), bb_stddev)
            atr = calc_atr(candles, int(atr_period))
            vwap = calc_vwap(candles)
            rsi = calc_rsi(closes, int(rsi_period))
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
                "volume_trend": "SPIKE" if vol_cur > vol_avg*vol_mult else "ABOVE_AVG" if vol_cur > vol_avg else "BELOW_AVG",
                "btc_trend": btc_trend,
                "btc_change": btc_change,
            }

            # Pré-filtre technique — un vrai signal (RSI extrême, croisement MACD ou pic de volume)
            # est requis pour justifier l'appel IA, pour TOUS les actifs (actifs ou non)
            has_signal = (rsi and (rsi < rsi_os or rsi > rsi_ob)) or (macd and (macd["crossBull"] or macd["crossBear"])) or vol_cur > vol_avg*vol_mult

            # BTC/ETH : ces deux actifs tendent en continu — un pullback en pleine tendance
            # (RSI modéré, ~40-60) mérite quand même une analyse IA si la structure EMA est claire
            if coin in ("BTC", "ETH") and e20 and e50 and e200:
                if (e20[-1] > e50[-1] > e200[-1]) or (e20[-1] < e50[-1] < e200[-1]):
                    has_signal = True

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

            ai_mode_paper = config["ai_mode_paper"] if config and "ai_mode_paper" in config.keys() else "ai"
            use_rules_engine = (config["trading_mode"] == "paper") and (ai_mode_paper == "rules")

            if use_rules_engine:
                conn_sl = get_db()
                cfg_sl = conn_sl.execute("SELECT max_loss_usd, position_pct FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
                portfolio_sl = conn_sl.execute("SELECT balance FROM paper_portfolio WHERE user_id=?", (user_id,)).fetchone()
                conn_sl.close()
                _max_loss = cfg_sl["max_loss_usd"] if cfg_sl and cfg_sl["max_loss_usd"] else 0.75
                _pct = cfg_sl["position_pct"] if cfg_sl and cfg_sl["position_pct"] else 8.0
                _capital = portfolio_sl["balance"] if portfolio_sl else 1000.0
                _size_est = max(10.0, min(round(_capital * _pct / 100, 2), _capital * 0.5))
                ai = analyze_with_rules(coin, tech, price, _max_loss, _size_est)
                cache_market_data(coin, tech, price)
            else:
                ai = await analyze_with_ai(client, user_id, coin, tech, None, price, api_key)
            if not ai:
                add_bot_log(user_id, f"⚠️ {coin}: Pas de réponse IA", "warning")
                continue
            action_ia = ai.get("action", "WAIT")
            confidence_ia = ai.get("confidence", 0)
            if coin_already_open and ai_continuous:
                add_bot_log(user_id, f"💡 {coin} (déjà ouvert): IA → {action_ia} ({confidence_ia}%) — info seulement", "info")
                continue
            cache_market_data(coin, tech, price)  # Mettre à jour le cache
            add_bot_log(user_id, f"{'📐' if use_rules_engine else '🤖'} {coin}: {'Règles' if use_rules_engine else 'IA'} → {action_ia} ({confidence_ia}%) RSI={tech.get('rsi','?')}", "info" if action_ia=="WAIT" else "success")
            required_conf = get_required_confidence(user_id, coin, action_ia)
            if action_ia == "WAIT":
                add_bot_log(user_id, f"⛔ {coin}: aucun signal net (WAIT) — ignoré", "info")
                continue
            if confidence_ia < required_conf:
                add_bot_log(user_id, f"⛔ {coin}: Confiance insuffisante ({confidence_ia}% < {required_conf}%) — ignoré", "info")
                continue
            if is_opportunist:
                add_bot_log(user_id, f"🎯 {coin}: Trade hors sélection ({confidence_ia}%) — actif ouvert dynamiquement !", "success")

            rsi_now = tech.get("rsi") or 50
            action = ai.get("action")

            # === RÈGLE RENFORCÉE BTC/ETH — ces deux actifs suivent des tendances
            # persistantes plutôt que des retournements fréquents (contrairement aux alts).
            # On bloque tout trade à contre-tendance de leur PROPRE structure EMA,
            # même si le filtre global BTC ±2%/8h considère la zone "neutre".
            if coin in ("BTC", "ETH") and e20 and e50 and e200:
                own_ema_align = "BULL" if e20[-1] > e50[-1] > e200[-1] else "BEAR" if e20[-1] < e50[-1] < e200[-1] else "MIXED"
                if own_ema_align == "BULL" and action == "SHORT":
                    add_bot_log(user_id, f"🛡️ {coin}: SHORT bloqué — structure EMA haussière (20>50>200)", "warning")
                    continue
                if own_ema_align == "BEAR" and action == "LONG":
                    add_bot_log(user_id, f"🛡️ {coin}: LONG bloqué — structure EMA baissière (20<50<200)", "warning")
                    continue
                if own_ema_align != "MIXED":
                    add_bot_log(user_id, f"📈 {coin}: Structure EMA {own_ema_align} confirmée — trade dans le sens de tendance", "success")

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
            cfg = conn.execute("SELECT trading_mode, max_position_usdc, max_open_trades, position_pct, max_same_direction_neutral, max_same_direction_trend FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
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
                # Anti-corrélation : plafond de trades ouverts dans la même direction
                same_dir_count = conn.execute("SELECT COUNT(*) FROM paper_trades WHERE user_id=? AND status='OPEN' AND action=?", (user_id, ai["action"])).fetchone()[0]
                max_same_dir = (cfg["max_same_direction_trend"] if "max_same_direction_trend" in cfg.keys() and cfg["max_same_direction_trend"] else 3) if btc_trend in ("bullish", "bearish") else (cfg["max_same_direction_neutral"] if "max_same_direction_neutral" in cfg.keys() and cfg["max_same_direction_neutral"] else 2)
                same_dir_blocked = same_dir_count >= max_same_dir
                # Logs de diagnostic
                if not portfolio:
                    add_bot_log(user_id, f"⚠️ {coin}: Pas de portefeuille trouvé", "warning")
                elif open_count >= max_trades:
                    add_bot_log(user_id, f"⛔ {coin}: Max trades atteint ({open_count}/{max_trades})", "warning")
                elif portfolio["balance"] < size:
                    add_bot_log(user_id, f"⛔ {coin}: Solde insuffisant ({round(portfolio['balance'],2)} < {size} USDC)", "warning")
                elif coin_open:
                    add_bot_log(user_id, f"💰 {coin}: Position déjà ouverte", "info")
                elif same_dir_blocked:
                    add_bot_log(user_id, f"🔗 {coin}: {ai['action']} bloqué — {same_dir_count} trades {ai['action']} déjà ouverts (max {max_same_dir}, anti-corrélation)", "warning")
                if portfolio and open_count < max_trades and portfolio["balance"] >= size and not coin_open and not same_dir_blocked:
                    # Rafraîchir le prix juste avant l'exécution : le "price" du snapshot de début
                    # de scan peut être périmé de plusieurs secondes pour les derniers coins traités
                    # (chaque itération fait un appel réseau fetch_candles). On repioche la dernière
                    # valeur WebSocket en direct pour que l'entrée reflète le marché réel au moment T.
                    exec_price = ws_prices.get(coin) if (ws_connected and ws_prices and coin in ws_prices) else price
                    # entry_price = prix d'exécution réel au moment T (ordre marché simulé),
                    # pas le prix "entry" suggéré par l'analyse (potentiellement périmé de plusieurs secondes)
                    entry_price = exec_price
                    conn.execute("""
                        INSERT INTO paper_trades (user_id, coin, action, entry_price, current_price,
                        size_usdc, leverage, stop_loss, take_profit1, take_profit2, signal_id, opened_at, session_date)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (user_id, coin, ai["action"], entry_price, exec_price, size,
                           ai.get("leverage") or 1, ai.get("stopLoss"),
                           ai.get("takeProfit1"), ai.get("takeProfit2"), sig_id,
                           datetime.utcnow().isoformat(),
                           datetime.utcnow().strftime("%Y-%m-%d")))
                    conn.execute("UPDATE paper_portfolio SET balance=balance-? WHERE user_id=?", (size, user_id))
                    add_bot_log(user_id, f"💰 PAPER TRADE: {ai['action']} {coin} @ ${entry_price} | {size} USDC", "success")

            elif cfg and cfg["trading_mode"] == "live":
                user_row = conn.execute("SELECT hl_wallet FROM users WHERE id=?", (user_id,)).fetchone()
                account_address = user_row["hl_wallet"] if user_row and "hl_wallet" in user_row.keys() else None
                if not account_address:
                    add_bot_log(user_id, "⛔ Mode live: aucune adresse de wallet Hyperliquid configurée", "error")
                elif not HL_SDK_AVAILABLE:
                    add_bot_log(user_id, "⛔ Mode live: hyperliquid-python-sdk non installé sur le serveur", "error")
                elif not HL_AGENT_PRIVATE_KEY:
                    add_bot_log(user_id, "⛔ Mode live: HL_AGENT_PRIVATE_KEY non configurée", "error")
                else:
                    open_count = conn.execute("SELECT COUNT(*) FROM paper_trades WHERE user_id=? AND status='OPEN'", (user_id,)).fetchone()[0]
                    max_trades = cfg["max_open_trades"] or 5
                    position_pct = cfg["position_pct"] if "position_pct" in cfg.keys() and cfg["position_pct"] else 8.0
                    capital = get_hl_account_value(account_address)
                    size = round(capital * position_pct / 100, 2)
                    size = max(10.0, min(size, capital * 0.5)) if capital > 0 else 0.0
                    coin_open = conn.execute("SELECT id FROM paper_trades WHERE user_id=? AND coin=? AND status='OPEN'", (user_id, coin)).fetchone()
                    same_dir_count = conn.execute("SELECT COUNT(*) FROM paper_trades WHERE user_id=? AND status='OPEN' AND action=?", (user_id, ai["action"])).fetchone()[0]
                    max_same_dir = (cfg["max_same_direction_trend"] if "max_same_direction_trend" in cfg.keys() and cfg["max_same_direction_trend"] else 3) if btc_trend in ("bullish", "bearish") else (cfg["max_same_direction_neutral"] if "max_same_direction_neutral" in cfg.keys() and cfg["max_same_direction_neutral"] else 2)
                    same_dir_blocked = same_dir_count >= max_same_dir
                    net_env = "TESTNET" if HL_USE_TESTNET else "MAINNET ⚠️ ARGENT RÉEL"
                    if capital <= 0:
                        add_bot_log(user_id, f"⛔ {coin}: Impossible de récupérer le capital réel Hyperliquid ({net_env})", "error")
                    elif open_count >= max_trades:
                        add_bot_log(user_id, f"⛔ {coin}: Max trades atteint ({open_count}/{max_trades})", "warning")
                    elif capital < size:
                        add_bot_log(user_id, f"⛔ {coin}: Solde insuffisant ({round(capital,2)} < {size} USDC)", "warning")
                    elif coin_open:
                        add_bot_log(user_id, f"💰 {coin}: Position déjà ouverte", "info")
                    elif same_dir_blocked:
                        add_bot_log(user_id, f"🔗 {coin}: {ai['action']} bloqué — {same_dir_count} trades {ai['action']} déjà ouverts (max {max_same_dir}, anti-corrélation)", "warning")
                    else:
                        try:
                            cfg_ml = conn.execute("SELECT max_loss_pct FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
                            max_loss_pct_val = float(cfg_ml["max_loss_pct"]) if cfg_ml and "max_loss_pct" in cfg_ml.keys() and cfg_ml["max_loss_pct"] else 0.31
                            leverage = ai.get("leverage") or 1
                            coin_size, sl_oid, fill_price = hl_open_position(account_address, coin, ai["action"], size, leverage, price, max_loss_pct_val)
                            # entry_price = prix de fill réel renvoyé par Hyperliquid (pas une estimation locale
                            # potentiellement périmée) — capital réel en jeu, la précision compte.
                            entry_price = fill_price
                            conn.execute("""
                                INSERT INTO paper_trades (user_id, coin, action, entry_price, current_price,
                                size_usdc, leverage, stop_loss, take_profit1, take_profit2, signal_id, opened_at,
                                session_date, is_live, hl_sl_oid, hl_size)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)
                            """, (user_id, coin, ai["action"], entry_price, fill_price, size,
                                   leverage, ai.get("stopLoss"),
                                   ai.get("takeProfit1"), ai.get("takeProfit2"), sig_id,
                                   datetime.utcnow().isoformat(),
                                   datetime.utcnow().strftime("%Y-%m-%d"), sl_oid, coin_size))
                            add_bot_log(user_id, f"🔴 LIVE TRADE ({net_env}): {ai['action']} {coin} @ ${entry_price} | {size} USDC | SL sécurité posé: {'oui' if sl_oid else 'NON — vérifier manuellement'}", "success")
                        except Exception as e:
                            add_bot_log(user_id, f"⛔ {coin}: Échec ouverture live Hyperliquid — {e}", "error")

            conn.commit()
            conn.close()

        # Auto-update paper trades — UNIQUEMENT si le WebSocket est déconnecté
        # (sinon double-gestion des mêmes trades = contention DB + timeouts WS, voir bug résolu)
        if not ws_connected:
            conn = get_db()
            paper_trades = conn.execute(
                "SELECT * FROM paper_trades WHERE user_id=? AND status='OPEN'", (user_id,)
            ).fetchall()
            for trade in paper_trades:
                price_row = conn.execute("SELECT price FROM prices WHERE coin=?", (trade["coin"],)).fetchone()
                if not price_row: continue
                cur = price_row["price"]
                trade_dict = dict(trade)
                # Seul point de fermeture : Trailing Profit + Max Loss (voir manage_open_trade)
                result = manage_open_trade(user_id, trade_dict, cur, conn)
                if result:
                    await finalize_closed_trade(user_id, trade_dict, result["pnl"], conn)
            conn.commit()
            conn.close()

        # Update last scan
        conn = get_db()
        conn.execute("UPDATE bot_config SET last_scan=? WHERE user_id=?",
                    (datetime.utcnow().isoformat(), user_id))
        conn.commit()
        conn.close()

REWARD_WIN_THRESHOLD = 3   # gains consécutifs avant que la confiance requise commence à baisser
REWARD_STEP_PCT = 5        # baisse en points de % par tranche de REWARD_WIN_THRESHOLD gains supplémentaires
REWARD_FLOOR_PCT = 50      # plancher minimum, jamais en dessous même sur une longue série de gains

def get_required_confidence(user_id: int, coin: str, action: str, base_confidence: int = None) -> int:
    """Retourne la confiance requise selon l'historique récent du coin/direction :
    - pertes consécutives → paliers réglables à la hausse (défaut 60/72/82/90), plus dur à déclencher
    - gains consécutifs (≥3) → baisse symétrique sous la base (5%/3 gains, plancher 50%), plus facile
      à déclencher pour un actif qui a démontré qu'il fonctionne dans cette direction récemment"""
    conn = get_db()
    row = conn.execute(
        "SELECT consecutive_losses, consecutive_wins FROM coin_confidence WHERE user_id=? AND coin=? AND action=?",
        (user_id, coin, action)
    ).fetchone()
    cfg = conn.execute(
        "SELECT base_confidence, conf_step1, conf_step2, conf_step3 FROM bot_config WHERE user_id=?",
        (user_id,)
    ).fetchone()
    conn.close()
    losses = row["consecutive_losses"] if row else 0
    wins = row["consecutive_wins"] if row and "consecutive_wins" in row.keys() else 0
    steps = get_confidence_steps(cfg)
    if losses > 0:
        return steps[min(losses, len(steps) - 1)]
    if wins >= REWARD_WIN_THRESHOLD:
        base = steps[0]
        reward_tiers = wins // REWARD_WIN_THRESHOLD
        return max(REWARD_FLOOR_PCT, base - reward_tiers * REWARD_STEP_PCT)
    return steps[0]

def get_confidence_steps(cfg) -> list:
    """Construit la liste des paliers [base, step1, step2, step3] à partir de la config utilisateur,
    avec repli sur les valeurs par défaut si non réglées."""
    if not cfg:
        return [60, 72, 82, 90]
    keys = cfg.keys()
    return [
        cfg["base_confidence"] if "base_confidence" in keys and cfg["base_confidence"] else 60,
        cfg["conf_step1"] if "conf_step1" in keys and cfg["conf_step1"] else 72,
        cfg["conf_step2"] if "conf_step2" in keys and cfg["conf_step2"] else 82,
        cfg["conf_step3"] if "conf_step3" in keys and cfg["conf_step3"] else 90,
    ]

async def pause_coin(user_id: int, coin: str, reason: str):
    """Met un actif spécifique en pause automatique (3e perte consécutive atteinte)"""
    conn = get_db()
    cfg = conn.execute("SELECT pause_hours FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
    pause_h = cfg["pause_hours"] if cfg and "pause_hours" in cfg.keys() and cfg["pause_hours"] else 2.0
    resume_at = datetime.utcnow() + timedelta(hours=pause_h)
    conn.execute("""INSERT OR REPLACE INTO coin_pause (user_id, coin, paused_until, reason) VALUES (?,?,?,?)""",
        (user_id, coin, resume_at.isoformat(), reason))
    conn.commit()
    conn.close()
    add_bot_log(user_id, f"⏸️ {coin}: mis en pause automatique jusqu'à {resume_at.strftime('%H:%M')} UTC — {reason}", "error")
    await send_alert_email(user_id, f"⏸️ {coin} mis en pause automatique",
        f"{coin} vient d'accumuler 3 pertes consécutives (paliers de confiance 72% → 82% → 90% tous franchis puis perdants).\n"
        f"Cet actif est mis en pause automatique jusqu'à {resume_at.strftime('%H:%M')} UTC.\n"
        "Les autres actifs continuent de trader normalement.")

def is_coin_paused(user_id: int, coin: str) -> bool:
    """Vérifie si un actif est actuellement en pause automatique (et nettoie si expiré)"""
    conn = get_db()
    row = conn.execute("SELECT paused_until FROM coin_pause WHERE user_id=? AND coin=?", (user_id, coin)).fetchone()
    if not row or not row["paused_until"]:
        conn.close()
        return False
    try:
        paused_until = datetime.fromisoformat(row["paused_until"])
    except Exception:
        conn.close()
        return False
    if datetime.utcnow() < paused_until:
        conn.close()
        return True
    # Pause expirée → nettoyer, et exiger 82% de confiance (palier 2) au retour, par précaution
    conn.execute("DELETE FROM coin_pause WHERE user_id=? AND coin=?", (user_id, coin))
    conn.execute("""INSERT OR REPLACE INTO coin_confidence (user_id, coin, action, consecutive_losses, updated_at)
        VALUES (?,?,'LONG',2,?)""", (user_id, coin, datetime.utcnow().isoformat()))
    conn.execute("""INSERT OR REPLACE INTO coin_confidence (user_id, coin, action, consecutive_losses, updated_at)
        VALUES (?,?,'SHORT',2,?)""", (user_id, coin, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()
    add_bot_log(user_id, f"▶️ {coin}: pause terminée — confiance requise fixée à 82% pour la reprise", "info")
    return False

async def update_coin_confidence(user_id: int, coin: str, action: str, won: bool):
    """Met à jour les compteurs de pertes ET de gains consécutifs (mutuellement exclusifs) :
    - pertes consécutives → paliers 60/72/82/90 à la hausse, pause auto à la 3e perte
    - gains consécutifs → confiance requise abaissée symétriquement (5%/3 gains, plancher 50%)"""
    conn = get_db()
    if won:
        # Victoire → incrémenter la série de gains, reset la série de pertes
        current = conn.execute(
            "SELECT consecutive_wins FROM coin_confidence WHERE user_id=? AND coin=? AND action=?",
            (user_id, coin, action)
        ).fetchone()
        wins = ((current["consecutive_wins"] or 0) + 1) if current and "consecutive_wins" in current.keys() else 1
        conn.execute("""INSERT OR REPLACE INTO coin_confidence 
            (user_id, coin, action, consecutive_losses, consecutive_wins, updated_at) VALUES (?,?,?,0,?,?)""",
            (user_id, coin, action, wins, datetime.utcnow().isoformat()))
        if wins >= REWARD_WIN_THRESHOLD:
            cfg_steps = conn.execute(
                "SELECT base_confidence, conf_step1, conf_step2, conf_step3 FROM bot_config WHERE user_id=?",
                (user_id,)
            ).fetchone()
            base = get_confidence_steps(cfg_steps)[0]
            reward_conf = max(REWARD_FLOOR_PCT, base - (wins // REWARD_WIN_THRESHOLD) * REWARD_STEP_PCT)
            add_bot_log(user_id, f"🏆 {coin} {action}: Confiance requise → {reward_conf}% ({wins} gains consécutifs, actif favorisé)", "success")
        else:
            add_bot_log(user_id, f"✅ {coin} {action}: Confiance reset à {get_confidence_steps(None)[0]}% (gain, série: {wins})", "info")
        conn.commit()
        conn.close()
    else:
        # Défaite → incrémenter la série de pertes, reset la série de gains
        current = conn.execute(
            "SELECT consecutive_losses FROM coin_confidence WHERE user_id=? AND coin=? AND action=?",
            (user_id, coin, action)
        ).fetchone()
        losses = (current["consecutive_losses"] + 1) if current else 1
        cfg_steps = conn.execute(
            "SELECT base_confidence, conf_step1, conf_step2, conf_step3 FROM bot_config WHERE user_id=?",
            (user_id,)
        ).fetchone()
        steps = get_confidence_steps(cfg_steps)
        new_conf = steps[min(losses, len(steps) - 1)]
        conn.execute("""INSERT OR REPLACE INTO coin_confidence 
            (user_id, coin, action, consecutive_losses, consecutive_wins, updated_at) VALUES (?,?,?,?,0,?)""",
            (user_id, coin, action, losses, datetime.utcnow().isoformat()))
        add_bot_log(user_id, f"📈 {coin} {action}: Confiance requise → {new_conf}% ({losses} pertes consécutives)", "warning")
        conn.commit()
        conn.close()
        conn2 = get_db()
        cfg = conn2.execute("SELECT loss_streak_size FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
        conn2.close()
        streak_size = cfg["loss_streak_size"] if cfg and "loss_streak_size" in cfg.keys() and cfg["loss_streak_size"] else 3
        if losses >= streak_size:
            await pause_coin(user_id, coin, f"{losses} pertes consécutives en {action}")

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
    """Traite un trade ouvert avec le nouveau prix - appelé par le WebSocket.
    Simple relais vers manage_open_trade : AUCUNE logique de fermeture ici,
    pour éviter toute divergence avec la boucle de polling."""
    try:
        result = manage_open_trade(user_id, trade, cur, conn)
        if result:
            await finalize_closed_trade(user_id, trade, result["pnl"], conn)
            return True
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
        # Valider format date YYYY-MM-DD strictement
        import re as re_mod
        if not re_mod.match(r'^\d{4}-\d{2}-\d{2}$', str(day)):
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
                FROM paper_trades WHERE user_id=? AND status='CLOSED' AND date(opened_at)=?
            """, (user_id, day)).fetchone()
            
            open_count = conn.execute(
                "SELECT COUNT(*) FROM paper_trades WHERE user_id=? AND date(opened_at)=? AND status='OPEN'",
                (user_id, day)
            ).fetchone()[0]
            
            ended_at = datetime.utcnow().isoformat() if open_count == 0 else None
            conn.execute("""INSERT INTO trading_sessions 
                (user_id, session_date, started_at, ended_at, closing_phase, total_trades, wins, losses, net_pnl, capital_start)
                VALUES (?,?,?,?,1,?,?,?,?,1000.0)""",
                (user_id, day, day+"T00:00:00", ended_at,
                 stats["total"] or 0, stats["wins"] or 0, 
                 stats["losses"] or 0, stats["net"] or 0))
            cleaned.append(f"📅 Session {day} reconstruite ({stats['total']} trades)")
        else:
            # Mettre à jour les stats de la session existante si elles sont à 0
            existing = dict(existing)
            if existing.get("total_trades", 0) == 0:
                stats = conn.execute("""
                    SELECT COUNT(*) as total,
                        SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins,
                        SUM(CASE WHEN pnl<=0 THEN 1 ELSE 0 END) as losses,
                        SUM(CASE WHEN status='CLOSED' THEN pnl ELSE 0 END) as net
                    FROM paper_trades WHERE user_id=? AND status='CLOSED' AND date(opened_at)=?
                """, (user_id, day)).fetchone()
                if stats and stats["total"]:
                    conn.execute("""UPDATE trading_sessions SET 
                        total_trades=?, wins=?, losses=?, net_pnl=?
                        WHERE user_id=? AND session_date=?""",
                        (stats["total"] or 0, stats["wins"] or 0,
                         stats["losses"] or 0, stats["net"] or 0,
                         user_id, day))
                    cleaned.append(f"🔄 Session {day} mise à jour ({stats['total']} trades)")
    
    # 1b. Nettoyer les sessions avec dates invalides (ex: 2026-07-060)
    conn.execute("""DELETE FROM trading_sessions 
        WHERE user_id=? AND (
            length(session_date) != 10 
            OR session_date NOT LIKE '____-__-__'
        )""", (user_id,))
    conn.commit()
    
    # 1c. Corriger session_date NULL dans les trades existants
    today_fix = datetime.utcnow().strftime("%Y-%m-%d")
    # Mettre session_date = date propre basée sur opened_at ou closed_at
    conn.execute("""UPDATE paper_trades 
        SET session_date = CASE
            WHEN opened_at LIKE '____-__-__T%' OR opened_at LIKE '____-__-__ %' 
                THEN substr(opened_at, 1, 10)
            WHEN closed_at LIKE '____-__-__T%' OR closed_at LIKE '____-__-__ %'
                THEN substr(closed_at, 1, 10)
            ELSE ?
        END
        WHERE user_id=? AND (session_date IS NULL OR session_date = '' 
            OR length(session_date) != 10)""", (today_fix, user_id))
    conn.commit()

    # 2. Supprimer les signaux en double (garder le plus récent par coin+action+jour)
    # — jamais un signal lié à un trade (ouvert ou fermé), sinon on casse l'historique
    result = conn.execute("""
        DELETE FROM signals WHERE id NOT IN (
            SELECT MAX(id) FROM signals
            WHERE user_id=?
            GROUP BY coin, action, date(created_at)
        ) AND user_id=?
        AND id NOT IN (SELECT signal_id FROM paper_trades WHERE signal_id IS NOT NULL)
    """, (user_id, user_id))
    dup_count = conn.execute("SELECT changes()").fetchone()[0]
    if dup_count > 0:
        cleaned.append(f"🗑️ {dup_count} signaux en double supprimés")

    # 3. Supprimer les signaux des actifs désactivés — sauf s'ils sont liés à un trade
    # (ex: PAXG hors sélection mais tradé en opportuniste : son signal doit rester pour l'historique)
    config = conn.execute("SELECT active_coins FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
    if config:
        import json as json_mod
        active_coins = json_mod.loads(config["active_coins"])
        placeholders = ",".join("?" * len(active_coins))
        result = conn.execute(
            f"""DELETE FROM signals WHERE user_id=? AND coin NOT IN ({placeholders})
                AND id NOT IN (SELECT signal_id FROM paper_trades WHERE signal_id IS NOT NULL)""",
            [user_id] + active_coins
        )
        inactive_count = conn.execute("SELECT changes()").fetchone()[0]
        if inactive_count > 0:
            cleaned.append(f"🗑️ {inactive_count} signaux d'actifs désactivés supprimés")

    # 4. Supprimer les signaux de plus de 7 jours — sauf s'ils sont liés à un trade
    result = conn.execute("""
        DELETE FROM signals WHERE user_id=? 
        AND created_at < datetime('now', '-7 days')
        AND id NOT IN (SELECT signal_id FROM paper_trades WHERE signal_id IS NOT NULL)
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

    # Toujours recalculer les stats de la session du jour
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    today_stats = conn.execute("""
        SELECT COUNT(*) as total,
            SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN pnl<=0 THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN status='CLOSED' THEN pnl ELSE 0 END) as net
        FROM paper_trades WHERE user_id=? AND status='CLOSED' AND date(opened_at)=?
    """, (user_id, today_str)).fetchone()
    if today_stats and today_stats["total"]:
        conn.execute("""UPDATE trading_sessions SET total_trades=?, wins=?, losses=?, net_pnl=?
            WHERE user_id=? AND session_date=?""",
            (today_stats["total"], today_stats["wins"] or 0,
             today_stats["losses"] or 0, today_stats["net"] or 0,
             user_id, today_str))
        conn.commit()

    if cleaned:
        add_bot_log(user_id, f"🧹 Nettoyage démarrage: {len(cleaned)} actions", "info")
        for msg in cleaned:
            add_bot_log(user_id, msg, "info")
    else:
        add_bot_log(user_id, "✅ Nettoyage démarrage: rien à nettoyer", "info")

async def send_alert_email(user_id: int, subject: str, body: str):
    """Envoie un email d'alerte via SendGrid"""
    try:
        conn = get_db()
        user = conn.execute(
            "SELECT email, sendgrid_key, alert_email FROM users WHERE id=?", (user_id,)
        ).fetchone()
        conn.close()
        
        if not user or not user["sendgrid_key"]:
            return
        
        to_email = user["alert_email"] or user["email"]
        sg_key = user["sendgrid_key"]
        
        async with httpx.AsyncClient() as client:
            r = await client.post(
                "https://api.sendgrid.com/v3/mail/send",
                headers={
                    "Authorization": f"Bearer {sg_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "personalizations": [{"to": [{"email": to_email}]}],
                    "from": {"email": "smeesxm@wanadoo.fr", "name": "HyperBot AI"},
                    "subject": f"🤖 HyperBot Alert: {subject}",
                    "content": [{"type": "text/plain", "value": body}]
                },
                timeout=10
            )
            if r.status_code in (200, 202):
                print(f"📧 Email envoyé à {to_email}: {subject}")
            else:
                print(f"📧 Erreur email: {r.status_code}")
    except Exception as e:
        print(f"📧 Erreur SendGrid: {e}")

# Compteur d'alertes pour éviter le spam
last_alerts = {}

async def send_alert_if_needed(user_id: int, alert_key: str, subject: str, body: str, cooldown_minutes: int = 30):
    """Envoie alerte seulement si pas envoyée récemment (anti-spam)"""
    now = datetime.utcnow()
    last = last_alerts.get(f"{user_id}_{alert_key}")
    if last and (now - last).total_seconds() < cooldown_minutes * 60:
        return  # Déjà alerté récemment
    last_alerts[f"{user_id}_{alert_key}"] = now
    await send_alert_email(user_id, subject, body)

async def notify_daily_summary(user_id: int, session_date: str, total: int, wins: int, losses: int, net_pnl: float):
    """Envoie le résumé quotidien par email à la clôture de session (minuit UTC)"""
    win_rate = round((wins or 0) / max(total or 1, 1) * 100, 1)
    emoji = "📈" if (net_pnl or 0) >= 0 else "📉"
    subject = f"{emoji} Résumé du {session_date} — NET: {round(net_pnl or 0, 2)}$"
    body = (
        f"Résumé de la session du {session_date}\n"
        f"Trades: {total or 0} (Gagnants: {wins or 0} / Perdants: {losses or 0})\n"
        f"Win rate: {win_rate}%\n"
        f"NET PnL: {round(net_pnl or 0, 2)} USDC"
    )
    await send_alert_email(user_id, subject, body)

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
            await notify_daily_summary(user_id, yesterday, stats["total"], stats["wins"], stats["losses"], stats["net"])
            
            # Reset confiance dynamique à minuit pour nouvelle session
            conn.execute("DELETE FROM coin_confidence WHERE user_id=?", (user_id,))
            conn.commit()
            add_bot_log(user_id, "🔄 Confiance dynamique remise à 60% pour tous les actifs — nouvelle session", "info")
    
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
            # Alerter tous les utilisateurs actifs
            try:
                conn = get_db()
                active_users = conn.execute("SELECT user_id FROM bot_config WHERE is_running=1").fetchall()
                conn.close()
                for row in active_users:
                    await send_alert_if_needed(
                        row["user_id"], "ws_disconnected",
                        "WebSocket Déconnecté",
                        f"Le WebSocket Hyperliquid s'est déconnecté à {datetime.utcnow().strftime('%H:%M:%S')} UTC.\nReconnexion automatique dans 5 secondes.\nSi le problème persiste, vérifiez votre connexion Railway.",
                        cooldown_minutes=30
                    )
            except: pass
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
    """Boucle backup 5s - mise à jour prix si WebSocket déconnecté"""
    try:
        while True:
            conn = get_db()
            config = conn.execute("SELECT is_running FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
            conn.close()
            if not config or not config["is_running"]:
                break
            try:
                # Seulement si WebSocket déconnecté — sinon WS gère en temps réel
                if not ws_connected:
                    await update_open_positions(user_id)
                else:
                    # Juste mettre à jour l'affichage des prix (lecture seule)
                    await update_prices_display(user_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                pass  # Silencieux en mode backup
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        raise

async def auto_reset_macro_filter(user_id: int):
    """Désactive le filtre macro après la fenêtre de blackout réglable (avant/après une annonce)"""
    conn = get_db()
    user = conn.execute("SELECT finnhub_key FROM users WHERE id=?", (user_id,)).fetchone()
    cfg = conn.execute("SELECT filter_macro, macro_blackout_before_min, macro_blackout_after_min FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    
    if not cfg or not cfg["filter_macro"]:
        return
    finnhub_key = user["finnhub_key"] if user and "finnhub_key" in user.keys() else None
    if not finnhub_key:
        return
    
    before_min = cfg["macro_blackout_before_min"] if "macro_blackout_before_min" in cfg.keys() and cfg["macro_blackout_before_min"] else 120
    after_min = cfg["macro_blackout_after_min"] if "macro_blackout_after_min" in cfg.keys() and cfg["macro_blackout_after_min"] else 60
    macro_data = await check_macro_calendar(user_id, finnhub_key)
    events = macro_data.get("events", [])
    # Si aucune annonce dans la fenêtre de blackout réglable, désactiver le filtre
    critical = [e for e in events if e["hours_left"]*60 <= before_min and e["hours_left"]*60 >= -after_min]
    if not critical:
        conn2 = get_db()
        conn2.execute("UPDATE bot_config SET filter_macro=0 WHERE user_id=?", (user_id,))
        conn2.commit()
        conn2.close()
        add_bot_log(user_id, "✅ Filtre macro désactivé automatiquement — fenêtre macro passée", "success")

async def update_prices_display(user_id: int):
    """Met à jour seulement le prix affiché — pas de fermeture — évite les locks DB"""
    if not ws_prices:
        return
    try:
        conn = get_db()
        trades = conn.execute(
            "SELECT id, coin, action, entry_price, size_usdc, leverage FROM paper_trades WHERE user_id=? AND status='OPEN'",
            (user_id,)
        ).fetchall()
        for trade in trades:
            trade = dict(trade)
            cur = ws_prices.get(trade["coin"])
            if not cur:
                continue
            pnl_dir = 1 if trade["action"] == "LONG" else -1
            pnl = (cur - trade["entry_price"]) / trade["entry_price"] * trade["size_usdc"] * trade["leverage"] * pnl_dir
            conn.execute("UPDATE paper_trades SET current_price=?, pnl=?, pnl_pct=? WHERE id=?",
                (cur, round(pnl,2), round(pnl/trade["size_usdc"]*100,2), trade["id"]))
        conn.commit()
        conn.close()
    except:
        pass

async def update_open_positions(user_id: int):
    """Filet de secours (toutes les 5s, uniquement si le WebSocket est déconnecté) :
    appelle EXACTEMENT la même fonction que le WebSocket et scan_markets — manage_open_trade —
    pour que le Trailing Profit / Max Loss continue à s'appliquer même pendant une coupure WS.
    Aucune logique de fermeture propre ici : un seul cerveau, plusieurs déclencheurs."""
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
    else:
        async with httpx.AsyncClient() as client:
            prices = await fetch_all_metas(client)
    for trade in paper_trades:
        trade = dict(trade)
        cur = prices.get(trade["coin"])
        if not cur:
            continue
        result = manage_open_trade(user_id, trade, cur, conn)
        if result:
            await finalize_closed_trade(user_id, trade, result["pnl"], conn)
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
    ai_mode_paper: Optional[str] = None
    resume_now: Optional[bool] = None
    pause_now: Optional[bool] = None
    loss_streak_size: Optional[int] = None
    pause_hours: Optional[float] = None
    base_confidence: Optional[float] = None
    conf_step1: Optional[float] = None
    conf_step2: Optional[float] = None
    conf_step3: Optional[float] = None
    rsi_oversold: Optional[float] = None
    rsi_overbought: Optional[float] = None
    volume_spike_mult: Optional[float] = None
    btc_trend_threshold: Optional[float] = None
    max_same_direction_neutral: Optional[int] = None
    max_same_direction_trend: Optional[int] = None
    trailing_activation_mult: Optional[float] = None
    trailing_gap_usd: Optional[float] = None
    qp_lock_trigger_usd: Optional[float] = None
    quick_profit_pct: Optional[float] = None
    max_loss_pct: Optional[float] = None
    trailing_gap_pct: Optional[float] = None
    qp_lock_trigger_pct: Optional[float] = None
    rsi_period: Optional[int] = None
    macd_fast: Optional[int] = None
    macd_slow: Optional[int] = None
    macd_signal: Optional[int] = None
    bb_period: Optional[int] = None
    bb_stddev: Optional[float] = None
    atr_period: Optional[int] = None
    hours_creuses_start: Optional[int] = None
    hours_creuses_end: Optional[int] = None
    macro_blackout_before_min: Optional[int] = None
    macro_blackout_after_min: Optional[int] = None
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
    user = conn.execute("SELECT email, wallet, api_key, finnhub_key, hl_api_key, hl_wallet, sendgrid_key, alert_email FROM users WHERE id=?", (user_id,)).fetchone()
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
        "has_sendgrid_key": bool(user["sendgrid_key"]) if "sendgrid_key" in user.keys() else False,
        "alert_email": user["alert_email"] if "alert_email" in user.keys() else "",
        "ws_connected": ws_connected,
        "active_coins": json.loads(config["active_coins"]),
        "is_running": bool(config["is_running"]),
        "trading_mode": config["trading_mode"] or "paper",
        "ai_mode_paper": config["ai_mode_paper"] if "ai_mode_paper" in config.keys() and config["ai_mode_paper"] else "ai",
        "pause_until": config["pause_until"] if "pause_until" in config.keys() else None,
        "loss_streak_size": config["loss_streak_size"] if "loss_streak_size" in config.keys() and config["loss_streak_size"] else 3,
        "pause_hours": config["pause_hours"] if "pause_hours" in config.keys() and config["pause_hours"] else 2.0,
        "base_confidence": config["base_confidence"] if "base_confidence" in config.keys() and config["base_confidence"] else 60,
        "conf_step1": config["conf_step1"] if "conf_step1" in config.keys() and config["conf_step1"] else 72,
        "conf_step2": config["conf_step2"] if "conf_step2" in config.keys() and config["conf_step2"] else 82,
        "conf_step3": config["conf_step3"] if "conf_step3" in config.keys() and config["conf_step3"] else 90,
        "rsi_oversold": config["rsi_oversold"] if "rsi_oversold" in config.keys() and config["rsi_oversold"] else 35,
        "rsi_overbought": config["rsi_overbought"] if "rsi_overbought" in config.keys() and config["rsi_overbought"] else 65,
        "volume_spike_mult": config["volume_spike_mult"] if "volume_spike_mult" in config.keys() and config["volume_spike_mult"] else 1.5,
        "btc_trend_threshold": config["btc_trend_threshold"] if "btc_trend_threshold" in config.keys() and config["btc_trend_threshold"] else 2.0,
        "max_same_direction_neutral": config["max_same_direction_neutral"] if "max_same_direction_neutral" in config.keys() and config["max_same_direction_neutral"] else 2,
        "max_same_direction_trend": config["max_same_direction_trend"] if "max_same_direction_trend" in config.keys() and config["max_same_direction_trend"] else 3,
        "trailing_activation_mult": config["trailing_activation_mult"] if "trailing_activation_mult" in config.keys() and config["trailing_activation_mult"] else 1.0,
        "trailing_gap_usd": config["trailing_gap_usd"] if "trailing_gap_usd" in config.keys() and config["trailing_gap_usd"] else 0.3,
        "qp_lock_trigger_usd": config["qp_lock_trigger_usd"] if "qp_lock_trigger_usd" in config.keys() and config["qp_lock_trigger_usd"] else 1.5,
        "quick_profit_pct": config["quick_profit_pct"] if "quick_profit_pct" in config.keys() and config["quick_profit_pct"] else 0.46,
        "max_loss_pct": config["max_loss_pct"] if "max_loss_pct" in config.keys() and config["max_loss_pct"] else 0.31,
        "trailing_gap_pct": config["trailing_gap_pct"] if "trailing_gap_pct" in config.keys() and config["trailing_gap_pct"] else 0.42,
        "qp_lock_trigger_pct": config["qp_lock_trigger_pct"] if "qp_lock_trigger_pct" in config.keys() and config["qp_lock_trigger_pct"] else 0.63,
        "rsi_period": config["rsi_period"] if "rsi_period" in config.keys() and config["rsi_period"] else 14,
        "macd_fast": config["macd_fast"] if "macd_fast" in config.keys() and config["macd_fast"] else 12,
        "macd_slow": config["macd_slow"] if "macd_slow" in config.keys() and config["macd_slow"] else 26,
        "macd_signal": config["macd_signal"] if "macd_signal" in config.keys() and config["macd_signal"] else 9,
        "bb_period": config["bb_period"] if "bb_period" in config.keys() and config["bb_period"] else 20,
        "bb_stddev": config["bb_stddev"] if "bb_stddev" in config.keys() and config["bb_stddev"] else 2,
        "atr_period": config["atr_period"] if "atr_period" in config.keys() and config["atr_period"] else 14,
        "hours_creuses_start": config["hours_creuses_start"] if "hours_creuses_start" in config.keys() and config["hours_creuses_start"] is not None else 21,
        "hours_creuses_end": config["hours_creuses_end"] if "hours_creuses_end" in config.keys() and config["hours_creuses_end"] is not None else 23,
        "macro_blackout_before_min": config["macro_blackout_before_min"] if "macro_blackout_before_min" in config.keys() and config["macro_blackout_before_min"] else 120,
        "macro_blackout_after_min": config["macro_blackout_after_min"] if "macro_blackout_after_min" in config.keys() and config["macro_blackout_after_min"] else 60,
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
    if req.ai_mode_paper is not None:
        conn.execute("UPDATE bot_config SET ai_mode_paper=? WHERE user_id=?",
                    (req.ai_mode_paper, user_id))
    if req.resume_now:
        conn.execute("UPDATE bot_config SET pause_until=NULL WHERE user_id=?", (user_id,))
        add_bot_log(user_id, "▶️ Pause levée manuellement", "info")
    if req.pause_now:
        cfg_p = conn.execute("SELECT pause_hours FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
        pause_h = cfg_p["pause_hours"] if cfg_p and "pause_hours" in cfg_p.keys() and cfg_p["pause_hours"] else 2.0
        resume_at = datetime.utcnow() + timedelta(hours=pause_h)
        conn.execute("UPDATE bot_config SET pause_until=? WHERE user_id=?", (resume_at.isoformat(), user_id))
        add_bot_log(user_id, f"⏸️ Pause générale déclenchée manuellement jusqu'à {resume_at.strftime('%H:%M')} UTC", "warning")
    if req.loss_streak_size is not None:
        conn.execute("UPDATE bot_config SET loss_streak_size=? WHERE user_id=?", (req.loss_streak_size, user_id))
    if req.pause_hours is not None:
        conn.execute("UPDATE bot_config SET pause_hours=? WHERE user_id=?", (req.pause_hours, user_id))
    if req.base_confidence is not None:
        conn.execute("UPDATE bot_config SET base_confidence=? WHERE user_id=?", (req.base_confidence, user_id))
    if req.conf_step1 is not None:
        conn.execute("UPDATE bot_config SET conf_step1=? WHERE user_id=?", (req.conf_step1, user_id))
    if req.conf_step2 is not None:
        conn.execute("UPDATE bot_config SET conf_step2=? WHERE user_id=?", (req.conf_step2, user_id))
    if req.conf_step3 is not None:
        conn.execute("UPDATE bot_config SET conf_step3=? WHERE user_id=?", (req.conf_step3, user_id))
    if req.rsi_oversold is not None:
        conn.execute("UPDATE bot_config SET rsi_oversold=? WHERE user_id=?", (req.rsi_oversold, user_id))
    if req.rsi_overbought is not None:
        conn.execute("UPDATE bot_config SET rsi_overbought=? WHERE user_id=?", (req.rsi_overbought, user_id))
    if req.volume_spike_mult is not None:
        conn.execute("UPDATE bot_config SET volume_spike_mult=? WHERE user_id=?", (req.volume_spike_mult, user_id))
    if req.btc_trend_threshold is not None:
        conn.execute("UPDATE bot_config SET btc_trend_threshold=? WHERE user_id=?", (req.btc_trend_threshold, user_id))
    if req.max_same_direction_neutral is not None:
        conn.execute("UPDATE bot_config SET max_same_direction_neutral=? WHERE user_id=?", (req.max_same_direction_neutral, user_id))
    if req.max_same_direction_trend is not None:
        conn.execute("UPDATE bot_config SET max_same_direction_trend=? WHERE user_id=?", (req.max_same_direction_trend, user_id))
    if req.trailing_activation_mult is not None:
        conn.execute("UPDATE bot_config SET trailing_activation_mult=? WHERE user_id=?", (req.trailing_activation_mult, user_id))
    if req.trailing_gap_usd is not None:
        conn.execute("UPDATE bot_config SET trailing_gap_usd=? WHERE user_id=?", (req.trailing_gap_usd, user_id))
    if req.qp_lock_trigger_usd is not None:
        conn.execute("UPDATE bot_config SET qp_lock_trigger_usd=? WHERE user_id=?", (req.qp_lock_trigger_usd, user_id))
    if req.quick_profit_pct is not None:
        conn.execute("UPDATE bot_config SET quick_profit_pct=? WHERE user_id=?", (req.quick_profit_pct, user_id))
    if req.max_loss_pct is not None:
        conn.execute("UPDATE bot_config SET max_loss_pct=? WHERE user_id=?", (req.max_loss_pct, user_id))
    if req.trailing_gap_pct is not None:
        conn.execute("UPDATE bot_config SET trailing_gap_pct=? WHERE user_id=?", (req.trailing_gap_pct, user_id))
    if req.qp_lock_trigger_pct is not None:
        conn.execute("UPDATE bot_config SET qp_lock_trigger_pct=? WHERE user_id=?", (req.qp_lock_trigger_pct, user_id))
    if req.rsi_period is not None:
        conn.execute("UPDATE bot_config SET rsi_period=? WHERE user_id=?", (req.rsi_period, user_id))
    if req.macd_fast is not None:
        conn.execute("UPDATE bot_config SET macd_fast=? WHERE user_id=?", (req.macd_fast, user_id))
    if req.macd_slow is not None:
        conn.execute("UPDATE bot_config SET macd_slow=? WHERE user_id=?", (req.macd_slow, user_id))
    if req.macd_signal is not None:
        conn.execute("UPDATE bot_config SET macd_signal=? WHERE user_id=?", (req.macd_signal, user_id))
    if req.bb_period is not None:
        conn.execute("UPDATE bot_config SET bb_period=? WHERE user_id=?", (req.bb_period, user_id))
    if req.bb_stddev is not None:
        conn.execute("UPDATE bot_config SET bb_stddev=? WHERE user_id=?", (req.bb_stddev, user_id))
    if req.atr_period is not None:
        conn.execute("UPDATE bot_config SET atr_period=? WHERE user_id=?", (req.atr_period, user_id))
    if req.hours_creuses_start is not None:
        conn.execute("UPDATE bot_config SET hours_creuses_start=? WHERE user_id=?", (req.hours_creuses_start, user_id))
    if req.hours_creuses_end is not None:
        conn.execute("UPDATE bot_config SET hours_creuses_end=? WHERE user_id=?", (req.hours_creuses_end, user_id))
    if req.macro_blackout_before_min is not None:
        conn.execute("UPDATE bot_config SET macro_blackout_before_min=? WHERE user_id=?", (req.macro_blackout_before_min, user_id))
    if req.macro_blackout_after_min is not None:
        conn.execute("UPDATE bot_config SET macro_blackout_after_min=? WHERE user_id=?", (req.macro_blackout_after_min, user_id))
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

@app.put("/api/config/sendgrid")
async def save_sendgrid_config(req: dict, user_id: int = Depends(get_current_user)):
    conn = get_db()
    if req.get("sendgrid_key"):
        conn.execute("UPDATE users SET sendgrid_key=? WHERE id=?", (req["sendgrid_key"].strip(), user_id))
    if req.get("alert_email"):
        conn.execute("UPDATE users SET alert_email=? WHERE id=?", (req["alert_email"].strip(), user_id))
    conn.commit()
    conn.close()
    # Test email
    await send_alert_email(user_id, "Configuration OK", 
        "HyperBot AI est configuré pour vous envoyer des alertes.\nVous recevrez des notifications en cas de problème critique.")
    add_bot_log(user_id, "📧 Configuration SendGrid sauvegardée — email de test envoyé", "success")
    return {"message": "Configuration SendGrid sauvegardée"}

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
    """Signaux EN COURS D'EXÉCUTION uniquement — liés à un trade Paper encore OPEN"""
    conn = get_db()
    rows = conn.execute("""
        SELECT s.*, pt.status as trade_status, pt.pnl as trade_pnl, pt.pnl_pct as trade_pnl_pct,
               pt.current_price as trade_current_price, pt.opened_at as trade_opened_at
        FROM signals s
        JOIN paper_trades pt ON pt.signal_id = s.id
        WHERE s.user_id=? AND pt.status='OPEN'
        ORDER BY s.created_at DESC LIMIT ?
    """, (user_id, limit)).fetchall()
    conn.close()
    signals = [dict(r) for r in rows]
    return {"signals": signals, "total": len(signals)}

@app.get("/api/signals/history")
def get_signals_history(limit: int = 50, user_id: int = Depends(get_current_user)):
    """HISTORIQUE — uniquement les signaux qui ont été tradés ET fermés"""
    conn = get_db()
    rows = conn.execute("""
        SELECT s.*, pt.status as trade_status, pt.pnl as trade_pnl, pt.pnl_pct as trade_pnl_pct,
               pt.close_reason as trade_close_reason, pt.opened_at as trade_opened_at,
               pt.closed_at as trade_closed_at
        FROM signals s
        JOIN paper_trades pt ON pt.signal_id = s.id
        WHERE s.user_id=? AND pt.status='CLOSED'
        ORDER BY pt.closed_at DESC LIMIT ?
    """, (user_id, limit)).fetchall()
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

@app.get("/api/coins/paused")
def get_paused_coins(user_id: int = Depends(get_current_user)):
    """Liste des actifs actuellement en pause automatique (3 pertes consécutives)"""
    conn = get_db()
    rows = conn.execute(
        "SELECT coin, paused_until, reason FROM coin_pause WHERE user_id=? AND paused_until > ?",
        (user_id, datetime.utcnow().isoformat())
    ).fetchall()
    conn.close()
    return {"paused": [dict(r) for r in rows]}

@app.get("/api/stats/ai-usage")
def get_ai_usage(user_id: int = Depends(get_current_user)):
    """Suivi du coût réel des appels IA — aujourd'hui et cumul total"""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    conn = get_db()
    today_row = conn.execute(
        "SELECT calls, input_tokens, output_tokens FROM ai_usage WHERE user_id=? AND date=?",
        (user_id, today)
    ).fetchone()
    total_row = conn.execute(
        "SELECT SUM(calls) as calls, SUM(input_tokens) as input_tokens, SUM(output_tokens) as output_tokens FROM ai_usage WHERE user_id=?",
        (user_id,)
    ).fetchone()
    conn.close()

    def to_cost(row):
        if not row or not row["calls"]:
            return {"calls": 0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0}
        cost = (row["input_tokens"] or 0) / 1_000_000 * AI_PRICE_INPUT_PER_M + \
               (row["output_tokens"] or 0) / 1_000_000 * AI_PRICE_OUTPUT_PER_M
        return {
            "calls": row["calls"] or 0,
            "input_tokens": row["input_tokens"] or 0,
            "output_tokens": row["output_tokens"] or 0,
            "cost_usd": round(cost, 4)
        }

    return {"today": to_cost(today_row), "total": to_cost(total_row)}

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
    if trade["is_live"]:
        user_row = conn.execute("SELECT hl_wallet FROM users WHERE id=?", (user_id,)).fetchone()
        account_address = user_row["hl_wallet"] if user_row and "hl_wallet" in user_row.keys() else None
        try:
            hl_close_position(account_address, trade["coin"], trade["hl_sl_oid"])
        except Exception as e:
            conn.close()
            raise HTTPException(status_code=500, detail=f"Échec de fermeture réelle sur Hyperliquid — vérifiez manuellement sur l'exchange ! ({e})")
    conn.execute("""
        UPDATE paper_trades SET status='CLOSED', current_price=?, pnl=?, pnl_pct=?,
        closed_at=?, close_reason=? WHERE id=?
    """, (cur_price, round(pnl,2), round(pnl/trade["size_usdc"]*100,2),
          datetime.utcnow().isoformat(), req.reason, req.trade_id))
    if not trade["is_live"]:
        conn.execute("UPDATE paper_portfolio SET balance=balance+?+? WHERE user_id=?",
                    (trade["size_usdc"], round(pnl,2), user_id))
    conn.commit()
    conn.close()
    return {"message": f"Trade fermé avec PnL: {round(pnl,2)} USDC"}

@app.post("/api/paper/reset")
def reset_paper_portfolio(user_id: int = Depends(get_current_user)):
    conn = get_db()
    open_live = conn.execute("SELECT COUNT(*) FROM paper_trades WHERE user_id=? AND status='OPEN' AND is_live=1", (user_id,)).fetchone()[0]
    if open_live > 0:
        conn.close()
        raise HTTPException(status_code=400, detail=f"{open_live} trade(s) LIVE encore ouvert(s) sur Hyperliquid — fermez-les d'abord (le reset supprimerait leur suivi sans toucher à l'exchange)")
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
    """Rafraîchit uniquement l'affichage (prix courant / PnL) des positions ouvertes.
    NE FERME AUCUN TRADE — la fermeture est gérée exclusivement par manage_open_trade."""
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
        conn.execute("UPDATE paper_trades SET current_price=?, pnl=?, pnl_pct=? WHERE id=?",
                    (cur, round(pnl,2), round(pnl/trade["size_usdc"]*100,2), trade["id"]))
    conn.commit()
    conn.close()
    return {"message": "Trades mis à jour"}

# ── REINITIALISATION COMPLETE ────────────────────────────────
@app.post("/api/reset-all")
def reset_all(user_id: int = Depends(get_current_user)):
    conn = get_db()
    open_live = conn.execute("SELECT COUNT(*) FROM paper_trades WHERE user_id=? AND status='OPEN' AND is_live=1", (user_id,)).fetchone()[0]
    if open_live > 0:
        conn.close()
        raise HTTPException(status_code=400, detail=f"{open_live} trade(s) LIVE encore ouvert(s) sur Hyperliquid — fermez-les d'abord")
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
@app.get("/api/sessions/debug")
def debug_sessions():
    conn = get_db()
    sessions = conn.execute(
        "SELECT session_date, length(session_date) as len, total_trades, net_pnl FROM trading_sessions ORDER BY session_date DESC"
    ).fetchall()
    trades = conn.execute(
        """SELECT COALESCE(session_date, substr(opened_at,1,10), substr(closed_at,1,10)) as day,
           COUNT(*) as total, SUM(pnl) as net
           FROM paper_trades WHERE status='CLOSED'
           GROUP BY day ORDER BY day DESC LIMIT 10"""
    ).fetchall()
    # Trades sans session_date valide
    orphans = conn.execute(
        """SELECT id, coin, action, pnl, session_date, substr(opened_at,1,10) as open_day,
           substr(closed_at,1,10) as close_day
           FROM paper_trades WHERE status='CLOSED' 
           AND (session_date IS NULL OR session_date='')
           ORDER BY closed_at DESC LIMIT 20"""
    ).fetchall()
    conn.close()
    conn.close()
    # Nouvelle connexion pour les stats portfolio
    conn2 = get_db()
    portfolio = conn2.execute("SELECT balance, initial_balance FROM paper_portfolio LIMIT 1").fetchone()
    total_pnl = conn2.execute("SELECT SUM(pnl + COALESCE(tp1_pnl,0)) as total, COUNT(*) as cnt FROM paper_trades WHERE status='CLOSED'").fetchone()
    conn2.close()
    
    bal = portfolio["balance"] if portfolio else 0
    ini = portfolio["initial_balance"] if portfolio else 1000
    pnl = round(total_pnl["total"] or 0, 2)
    
    return {
        "sessions_table": [dict(s) for s in sessions],
        "trades_by_date": [dict(t) for t in trades],
        "balance": bal,
        "initial_balance": ini,
        "real_gain": round(bal - ini, 2),
        "sum_pnl_closed": pnl,
        "unexplained": round((bal - ini) - pnl, 2),
        "total_closed_trades": total_pnl["cnt"]
    }

@app.post("/api/sessions/cleanup")
def cleanup_sessions(user_id: int = Depends(get_current_user)):
    """Supprime toutes les sessions invalides et reconstruit"""
    conn = get_db()
    # Supprimer sessions avec dates invalides
    conn.execute("""DELETE FROM trading_sessions 
        WHERE user_id=? AND (
            length(session_date) != 10 
            OR session_date NOT LIKE '____-__-__'
            OR CAST(substr(session_date,9,2) AS INTEGER) > 31
            OR CAST(substr(session_date,6,2) AS INTEGER) > 12
        )""", (user_id,))
    deleted = conn.execute("SELECT changes()").fetchone()[0]
    # Aussi forcer correction des session_date dans les trades
    conn.execute("""UPDATE paper_trades 
        SET session_date = CASE
            WHEN opened_at LIKE '____-__-__%' THEN substr(opened_at, 1, 10)
            WHEN closed_at LIKE '____-__-__%' THEN substr(closed_at, 1, 10)
            ELSE strftime('%Y-%m-%d', 'now')
        END
        WHERE user_id=? AND (session_date IS NULL OR length(session_date) != 10 
            OR CAST(substr(session_date,9,2) AS INTEGER) > 31)""", (user_id,))
    conn.commit()
    # Corriger session_date dans les trades
    conn.execute("""UPDATE paper_trades 
        SET session_date = CASE
            WHEN opened_at LIKE '____-__-__%' THEN substr(opened_at, 1, 10)
            WHEN closed_at LIKE '____-__-__%' THEN substr(closed_at, 1, 10)
            ELSE strftime('%Y-%m-%d', 'now')
        END
        WHERE user_id=? AND (session_date IS NULL OR length(session_date) != 10)""", (user_id,))
    conn.commit()
    conn.close()
    return {"message": f"{deleted} sessions invalides supprimées — rechargez Sessions"}

@app.post("/api/ws/reconnect")
async def reconnect_websocket(user_id: int = Depends(get_current_user)):
    """Force la reconnexion du WebSocket Hyperliquid"""
    global ws_connected
    ws_connected = False  # Force la déconnexion
    asyncio.create_task(connect_hyperliquid_ws())
    add_bot_log(user_id, "🔌 Reconnexion WebSocket forcée manuellement", "info")
    return {"message": "Reconnexion WebSocket lancée"}

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
            SUM(pnl + COALESCE(tp1_pnl,0)) as net
        FROM paper_trades
        WHERE user_id=? AND status='CLOSED'
    """, (user_id,)).fetchone()
    
    # Trades ouverts PnL
    open_pnl = conn.execute("""
        SELECT SUM(pnl) as open_pnl, COUNT(*) as open_count, SUM(size_usdc) as open_margin
        FROM paper_trades WHERE user_id=? AND status='OPEN'
    """, (user_id,)).fetchone()
    
    # Stats par jour (7 derniers) basé sur date de FERMETURE = performance cash du jour
    daily = conn.execute("""
        SELECT date(pt.closed_at) as day,
            COUNT(*) as total,
            SUM(CASE WHEN pt.pnl > 0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN pt.pnl <= 0 THEN 1 ELSE 0 END) as losses,
            SUM(pt.pnl) as net,
            1000 as capital_start
        FROM paper_trades pt
        WHERE pt.user_id=? AND pt.status='CLOSED' AND pt.closed_at IS NOT NULL
        GROUP BY date(pt.closed_at)
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
    # Supprimer les signaux des actifs desactives — jamais ceux liés à un trade (ex: PAXG opportuniste)
    if active_coins:
        placeholders = ",".join("?" * len(active_coins))
        conn.execute(f"""DELETE FROM signals WHERE user_id=? AND coin NOT IN ({placeholders})
            AND id NOT IN (SELECT signal_id FROM paper_trades WHERE signal_id IS NOT NULL)""",
            [user_id] + active_coins)
        inactive_deleted = conn.execute("SELECT changes()").fetchone()[0]
    else:
        inactive_deleted = 0
    # Supprimer les doublons — garder seulement le signal le plus récent par coin+action, jamais un signal tradé
    conn.execute("""
        DELETE FROM signals WHERE id NOT IN (
            SELECT MAX(id) FROM signals
            WHERE user_id=?
            GROUP BY coin, action, date(created_at)
        ) AND user_id=?
        AND id NOT IN (SELECT signal_id FROM paper_trades WHERE signal_id IS NOT NULL)
    """, (user_id, user_id))
    deleted = conn.execute("SELECT changes()").fetchone()[0]
    # Supprimer aussi les signaux de plus de 24h — jamais ceux liés à un trade
    conn.execute("""DELETE FROM signals WHERE user_id=? AND created_at < datetime('now', '-24 hours')
        AND id NOT IN (SELECT signal_id FROM paper_trades WHERE signal_id IS NOT NULL)""", (user_id,))
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
