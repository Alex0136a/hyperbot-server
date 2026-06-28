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
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            wallet TEXT DEFAULT '',
            api_key TEXT DEFAULT '',
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
            active_coins TEXT DEFAULT '["BTC","ETH","SOL","ARB","AVAX","LINK","OP","INJ","TIA","BNB","HYPE","PAXG"]',
            is_running INTEGER DEFAULT 0,
            trading_mode TEXT DEFAULT 'paper',
            max_position_usdc REAL DEFAULT 50.0,
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
async def analyze_with_ai(client, coin, tech, ob, price, api_key):
    prompt = f"""You are an elite quantitative trading analyst for crypto perpetual futures on Hyperliquid.
Analyze {coin}/USDC and provide a precise trading decision.
PRICE: ${price}
RSI: {tech.get('rsi','N/A')} | MACD Bull: {tech.get('macd_bull','N/A')} | Bear: {tech.get('macd_bear','N/A')}
EMA20: {tech.get('ema20','N/A')} | EMA50: {tech.get('ema50','N/A')} | EMA200: {tech.get('ema200','N/A')}
BB Upper: {tech.get('bb_upper','N/A')} | Lower: {tech.get('bb_lower','N/A')}
ATR: {tech.get('atr','N/A')} | VWAP: {tech.get('vwap','N/A')}
Volume: {tech.get('volume_trend','N/A')}
CONSTRAINTS: Leverage x2-x5, position 5-15% portfolio, min R/R 2:1
Respond ONLY with JSON (no markdown):
{{"action":"LONG"|"SHORT"|"WAIT","confidence":0-100,"entry":number,"stopLoss":number,"takeProfit1":number,"takeProfit2":number,"leverage":2-5,"positionSize":5-15,"reasoning":"2 sentences in French","keySignals":["s1","s2","s3"],"riskReward":number,"timeframe":"court-terme"|"moyen-terme"}}"""

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
        data = r.json()
        text = "".join(b.get("text","") for b in data.get("content",[]))
        return json.loads(text.replace("```json","").replace("```","").strip())
    except:
        return None

# ── MOTEUR DE SCAN ───────────────────────────────────────────
scanning_tasks = {}

async def scan_markets(user_id: int):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    config = conn.execute("SELECT * FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
    conn.close()

    if not user or not config:
        return

    active_coins = json.loads(config["active_coins"])
    api_key = user["api_key"]

    async with httpx.AsyncClient() as client:
        # Fetch prices
        prices = await fetch_all_metas(client)

        # Update prices in DB
        conn = get_db()
        for coin, price in prices.items():
            conn.execute("INSERT OR REPLACE INTO prices (coin, price, updated_at) VALUES (?,?,?)",
                        (coin, price, datetime.utcnow().isoformat()))
        conn.commit()
        conn.close()

        # Analyze each coin
        for coin in active_coins:
            if coin not in prices:
                continue

            price = prices[coin]
            candles_raw = await fetch_candles(client, coin)
            if not candles_raw or len(candles_raw) < 50:
                continue

            candles = [{"h":float(c["h"]),"l":float(c["l"]),"c":float(c["c"]),"v":float(c["v"])} for c in candles_raw]
            closes = [c["c"] for c in candles]
            vols = [c["v"] for c in candles]

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
            }

            # Pre-filter
            has_signal = (rsi and (rsi < 35 or rsi > 65)) or (macd and (macd["crossBull"] or macd["crossBear"])) or vol_cur > vol_avg*1.5 or True

            if not has_signal or not api_key:
                continue

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
                print(f"{coin}: Signal recent, ignore")
                continue

            ai = await analyze_with_ai(client, coin, tech, None, price, api_key)
            if not ai or ai.get("action") == "WAIT" or ai.get("confidence", 0) < 55:
                continue

            # Ignorer signaux contradictoires
            if last_action and last_action["action"] != ai.get("action") and last_action["action"] != "WAIT":
                print(f"{coin}: Contradictoire {last_action['action']} vs {ai.get('action')}, ignore")
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
                size = cfg["max_position_usdc"] or 50.0
                if portfolio and open_count < max_trades and portfolio["balance"] >= size:
                    entry_price = ai.get("entry") or price
                    conn.execute("""
                        INSERT INTO paper_trades (user_id, coin, action, entry_price, current_price,
                        size_usdc, leverage, stop_loss, take_profit1, take_profit2, signal_id)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """, (user_id, coin, ai["action"], entry_price, price, size,
                           ai.get("leverage") or 1, ai.get("stopLoss"),
                           ai.get("takeProfit1"), ai.get("takeProfit2"), sig_id))
                    conn.execute("UPDATE paper_portfolio SET balance=balance-? WHERE user_id=?", (size, user_id))
                    print(f"PAPER TRADE AUTO: {ai['action']} {coin} @  | Taille: {size} USDC")

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
            if trade["stop_loss"]:
                if trade["action"] == "LONG" and cur <= trade["stop_loss"]: close_reason = "STOP_LOSS"
                elif trade["action"] == "SHORT" and cur >= trade["stop_loss"]: close_reason = "STOP_LOSS"
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
                print(f"Trade {trade['coin']} ferme automatiquement: {close_reason} | PnL: {round(pnl,2)} USDC")
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

async def run_bot_loop(user_id: int):
    while True:
        conn = get_db()
        config = conn.execute("SELECT is_running FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
        conn.close()
        if not config or not config["is_running"]:
            break
        await scan_markets(user_id)
        await asyncio.sleep(60)

# ── FASTAPI APP ──────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
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
    user = conn.execute("SELECT email, wallet, api_key FROM users WHERE id=?", (user_id,)).fetchone()
    config = conn.execute("SELECT * FROM bot_config WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return {
        "email": user["email"],
        "wallet": user["wallet"],
        "has_api_key": bool(user["api_key"]),
        "active_coins": json.loads(config["active_coins"]),
        "is_running": bool(config["is_running"]),
        "trading_mode": config["trading_mode"] or "paper",
        "max_position_usdc": config["max_position_usdc"] or 50.0,
        "max_open_trades": config["max_open_trades"] or 5,
        "last_scan": config["last_scan"],
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
        conn.execute("UPDATE bot_config SET trading_mode=? WHERE user_id=?",
                    (req.trading_mode, user_id))
    if req.max_position_usdc is not None:
        conn.execute("UPDATE bot_config SET max_position_usdc=? WHERE user_id=?",
                    (req.max_position_usdc, user_id))
    if req.max_open_trades is not None:
        conn.execute("UPDATE bot_config SET max_open_trades=? WHERE user_id=?",
                    (req.max_open_trades, user_id))
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
    if user_id not in scanning_tasks or scanning_tasks[user_id].done():
        task = asyncio.create_task(run_bot_loop(user_id))
        scanning_tasks[user_id] = task
    return {"message": "Bot démarré"}

@app.post("/api/bot/stop")
def stop_bot(user_id: int = Depends(get_current_user)):
    conn = get_db()
    conn.execute("UPDATE bot_config SET is_running=0 WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()
    return {"message": "Bot arrêté"}

@app.get("/api/signals")
def get_signals(limit: int = 50, user_id: int = Depends(get_current_user)):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM signals WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
        (user_id, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/prices")
def get_prices():
    conn = get_db()
    rows = conn.execute("SELECT coin, price FROM prices").fetchall()
    conn.close()
    return {r["coin"]: r["price"] for r in rows}

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
    return {
        "balance": portfolio["balance"],
        "initial_balance": portfolio["initial_balance"],
        "total_pnl": round(total_pnl, 2),
        "total_pnl_pct": round((portfolio["balance"] + total_pnl - portfolio["initial_balance"]) / portfolio["initial_balance"] * 100, 2),
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
    conn.execute("UPDATE paper_portfolio SET balance=1000.0, initial_balance=1000.0 WHERE user_id=?", (user_id,))
    conn.execute("DELETE FROM paper_trades WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()
    return {"message": "Portefeuille réinitialisé à 1000 USDC"}

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
        if trade["stop_loss"]:
            if trade["action"] == "LONG" and cur <= trade["stop_loss"]: close_reason = "STOP_LOSS"
            elif trade["action"] == "SHORT" and cur >= trade["stop_loss"]: close_reason = "STOP_LOSS"
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

# ── NETTOYAGE ────────────────────────────────────────────────
@app.post("/api/cleanup")
def cleanup_signals(user_id: int = Depends(get_current_user)):
    conn = get_db()
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
