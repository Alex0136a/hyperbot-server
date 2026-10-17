"""
api.py — Backend FastAPI pour HyperBot Web (déploiement GitHub + Railway).

Sert :
  - l API JSON consommée par index.html (voir contrat dans le fichier HTML)
  - le fichier index.html lui-meme sur "/"

Demarrage local :
    pip install -r requirements.txt
    uvicorn api:app --host 0.0.0.0 --port 8000

Variables d environnement (voir README.md pour la liste complete) :
    HYPERBOT_DATA_DIR        dossier de donnees persistantes (DB + logs + capital)
    HYPERBOT_SECRET_KEY      cle secrete pour signer les tokens de session
    HYPERBOT_PRIVATE_KEY / HYPERBOT_WALLET_ADDRESS / HYPERBOT_FINNHUB_API_KEY
"""
import os
import sys
import json
import queue
import threading
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Header, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, Response
from pydantic import BaseModel

# v3.2 — FIX CRITIQUE : les imports locaux (db, auth, bot_engine) DOIVENT se
# faire AVANT tout changement de repertoire courant (os.chdir). Auparavant,
# le chdir vers HYPERBOT_DATA_DIR (ex: /data) se faisait avant ces imports :
# Python resolvait alors "import db" par rapport au NOUVEAU repertoire
# courant (/data) au lieu du dossier contenant le code (/app), provoquant
# un crash systematique ("ModuleNotFoundError: No module named 'db'") des
# qu un Volume etait monte et HYPERBOT_DATA_DIR defini.
import db
import auth
import bot_engine as be
import replay_research as rr   # v4.347 — rejeu historique des entrees (lecture seule)
import asset_cards as ac       # v4.349 — fiche actif (lecture seule)
# v4.274 — FIX : importe ICI, avec les autres modules, AVANT le os.chdir()
# vers le dossier de donnees ci-dessous. Importe apres (v4.273), Python le
# cherchait dans le Volume -> "ModuleNotFoundError: No module named
# 'manual_trading'" et le serveur ne demarrait plus.
import manual_trading
import push_alerts             # v4.376 — alertes telephone

# ── Dossier de donnees persistantes (a monter en Volume sur Railway) ─────
# Fait APRES les imports ci-dessus : seuls les FICHIERS ecrits a l execution
# (base SQLite, capital, logs, session) doivent utiliser ce dossier — pas la
# resolution des modules Python, deja faite a ce stade.
_DATA_DIR = os.environ.get("HYPERBOT_DATA_DIR", ".")
_DATA_DIR_CONFIGURED = "HYPERBOT_DATA_DIR" in os.environ
os.makedirs(_DATA_DIR, exist_ok=True)
# v4.274 — garde-fou : le dossier du code reste importable apres le chdir
# (uvicorn ajoute "." RELATIF a sys.path, qui designerait sinon le Volume).
_CODE_DIR = os.path.dirname(os.path.abspath(__file__))
if _CODE_DIR not in sys.path:
    sys.path.insert(0, _CODE_DIR)
os.chdir(_DATA_DIR)

# ─────────────────────────────────────────────────────────────────────────
#  INITIALISATION
# ─────────────────────────────────────────────────────────────────────────
db.init_db()
# v4.244 — SUR DEMANDE EXPLICITE : purge le journal permanent au
# demarrage (rythme naturel de redeploiement de ce bot, suffisant pour
# eviter une croissance illimitee sans necessiter de tache planifiee
# separee).
try:
    db.purge_log_history()
except Exception as e:
    print(f"[LOG-HISTORY] Echec purge au demarrage : {e}")

# Nos 6 symboles reellement supportes (voir bot_engine.CONFIG["SYMBOLS"]).
# L interface propose 30 cryptos (ALL_COINS) — on ne peut en activer que
# parmi ce sous-ensemble reellement tradable par ce bot.
SUPPORTED_TICKERS = [be.ticker_from_slot_key(s) for s in be.CONFIG["SYMBOLS"]]

cfg = dict(be.CONFIG)

# v3.2 : fallback via variable d environnement pour ACTIVE_COINS — permet de
# fixer les actifs actifs sans dependre d un Volume Railway (comme pour les
# cles Hyperliquid/Finnhub). Une eventuelle valeur enregistree en base (via
# l interface web, necessite un Volume) reste prioritaire si presente.
def _norm_ticker(t: str) -> str:
    """v4.264 — normalise un ticker SANS casser les marches HIP-3 : "btc" ->
    "BTC", mais "xyz:eur"/"XYZ:EUR" -> "xyz:EUR" (prefixe DEX en minuscules,
    comme dans toute la configuration). Un simple .upper() produisait
    "XYZ:EUR", qui ne correspondait plus jamais a rien."""
    t = (t or "").strip()
    if ":" in t:
        dex, coin = t.split(":", 1)
        return f"{dex.lower()}:{coin.upper()}"
    return t.upper()


_env_active_coins = os.environ.get("HYPERBOT_ACTIVE_COINS", "").strip()
if _env_active_coins:
    cfg["ACTIVE_COINS"] = [_norm_ticker(c) for c in _env_active_coins.split(",") if c.strip()]

# v4.269 — FIX BUG CRITIQUE : le profil (SWING/SCALP) etait applique APRES
# les reglages enregistres par l utilisateur, et ecrasait donc a CHAQUE
# redemarrage tous les reglages avances qu il contient (RSI, TTP, flux, SL…) :
# un reglage modifie dans l interface revenait silencieusement a sa valeur
# par defaut au deploiement suivant. Ordre corrige : profil d abord, puis les
# reglages de l utilisateur par-dessus.
_saved_overrides = db.get_all_config_overrides()
be.apply_profile(cfg, _saved_overrides.get("PROFILE", cfg.get("PROFILE", "swing")))
for k, v in _saved_overrides.items():
    cfg[k] = v

if db.get_meta("initial_balance") is None:
    db.set_meta("initial_balance", str(cfg["CAPITAL_USD"]))
if db.get_meta("reset_at") is None:
    db.set_meta("reset_at", db.now_iso())

# ── Diagnostic de persistance ────────────────────────────────────────────
# BOOT_COUNT est stocke en base : s il repart TOUJOURS a 1 apres chaque
# redeploiement Railway (au lieu de s incrementer 1, 2, 3...), c est la
# preuve que les donnees ne persistent pas — HYPERBOT_DATA_DIR ne pointe
# pas vers un Volume monte. Visible dans /health et dans les logs de
# demarrage.
BOOT_COUNT = int(db.get_meta("boot_count", "0")) + 1
db.set_meta("boot_count", str(BOOT_COUNT))
_startup_warnings = []
if not _DATA_DIR_CONFIGURED:
    _startup_warnings.append(
        "HYPERBOT_DATA_DIR n est pas definie — les donnees (base, capital, cle "
        "API) NE PERSISTERONT PAS entre deux redeploiements Railway. Cree un "
        "Volume (Settings > Volumes), monte-le par exemple sur /data, et "
        "definis HYPERBOT_DATA_DIR=/data."
    )
if BOOT_COUNT == 1:
    _startup_warnings.append(
        "Premier demarrage detecte pour cette base de donnees (boot_count=1). "
        "Si ce nombre repart TOUJOURS a 1 apres un redeploiement, le Volume "
        "n est pas correctement monte — voir /health."
    )

event_queue = queue.Queue()
bot = be.BotEngine(cfg, event_queue)
# v4.273 — trading manuel (doit exister AVANT le demarrage du moteur : la
# reprise des positions tient compte des positions manuelles)
bot.manual = manual_trading.ManualTrading(bot)
# v4.376 — alertes sur telephone (Web Push). Sans `cryptography`, elles sont simplement indisponibles.
push = push_alerts.PushAlerts(_DATA_DIR, log=print, closed_trades_since=lambda since: db.get_all_closed_trades(since=since))
push.ticker_from_slot = be.ticker_from_slot_key
bot.push = push
push.start()

log_buffer = deque(maxlen=3000)
_state_lock = threading.Lock()

# v3.2 — FIX : l interface (index.html) attend un champ "message" (pas "msg")
# et des niveaux "success"/"warning"/"error" (tout le reste s affiche en
# gris) — alors que le bot (bot_engine.py) emet des niveaux "ok"/"warn"/
# "error"/"win"/"loss"/"info"/"signal"/"dim". Sans cette traduction, TOUS
# les logs s affichaient sans aucun texte visible (mauvais nom de champ) et
# sans les bonnes couleurs.
_LEVEL_MAP = {"ok": "success", "win": "success", "warn": "warning", "loss": "error", "error": "error"}


def _push_log(level_raw: str, msg: str):
    now = datetime.now(timezone.utc).isoformat()
    log_buffer.append({
        "time": now,
        "level": _LEVEL_MAP.get(level_raw, "info"),
        "message": msg,
    })
    # v4.244 — SUR DEMANDE EXPLICITE : persiste EGALEMENT les niveaux
    # significatifs (pas le bruit "info"/"dim"/"signal" repete a chaque
    # cycle) dans le journal permanent, consultable au-dela du buffer en
    # memoire (3000 lignes, quelques heures seulement).
    if level_raw in ("warn", "error", "ok", "win", "loss"):
        try:
            db.append_log_history(now, level_raw, msg)
        except Exception as e:
            print(f"[LOG-HISTORY] Echec ecriture journal permanent : {e}")


for _w in _startup_warnings:
    _push_log("warn", _w)


def _compute_protected_trade_ids():
    """IDs exacts (pas les coins) des trades en base correspondant a des
    positions REELLEMENT ouvertes en memoire du bot en ce moment — utilise
    a la fois par le nettoyage manuel et le balayage automatique au
    demarrage."""
    protected_ids = []
    # v4.264 — FIX BUG CRITIQUE : ne parcourait que bot.states — les
    # positions Accumulation (bot.accum_states) n etaient jamais protegees,
    # leur ligne etait supprimee par le nettoyage au demarrage, puis la
    # position etait reclassee "forex" au redeploiement suivant.
    for states in (bot.states, bot.accum_states):
        for slot_key, s in states.items():
            if not s.position:
                continue
            tid = db.get_open_trade_id_by_uid(s.position.get("trade_uid"))
            if not tid:
                ticker = be.ticker_from_slot_key(slot_key)
                action = "LONG" if s.position["type"] == "long" else "SHORT"
                tid = db.get_open_trade_id_by_coin_action(ticker, action, s.position.get("strategy"))
            if tid:
                protected_ids.append(tid)
    # v4.273 — positions manuelles ouvertes
    for item in list(bot.manual.items.values()):
        if item.get("status") in ("open", "opening", "closing"):
            tid = db.get_open_trade_id_by_uid(item.get("trade_uid"))
            if tid:
                protected_ids.append(tid)
    return protected_ids


def _consume_events():
    """Tourne en tache de fond : lit la queue du BotEngine et persiste les
    evenements pertinents (logs en memoire, trades en base)."""
    while True:
        try:
            ev = event_queue.get(timeout=1)
        except queue.Empty:
            continue
        etype, data = ev.get("type"), ev.get("data") or {}
        try:
            if etype == "log":
                _push_log(data.get("level", "info"), data.get("msg", ""))
            elif etype == "ws_event":
                db.insert_ws_event(data.get("kind", "unknown"), data.get("message", ""))
            elif etype == "trade_opened":
                # v4.264 — deja ecrit de facon synchrone par bot_engine (trace
                # indexee par trade_uid) ; sinon repli : insertion/completion.
                if not data.get("persisted"):
                    db.upsert_open_trade(data)
            elif etype == "trade":
                ticker = be.ticker_from_slot_key(data.get("symbol", ""))
                action = "LONG" if data.get("type") == "long" else "SHORT"
                # v4.264 — correspondance EXACTE par identifiant de trade,
                # repli sur coin/sens/strategie pour les trades anciens.
                trade_id = db.get_open_trade_id_by_uid(data.get("trade_uid"))
                if not trade_id:
                    trade_id = db.get_open_trade_id_by_coin_action(ticker, action, data.get("strategy"))
                if trade_id:
                    db.close_trade(trade_id, data.get("exit"), data.get("pnl"), data.get("reason"), peak_pnl=data.get("peak_pnl_usd"), peak_pnl_pct=data.get("peak_pnl_pct"), fees_paid=data.get("fees_paid"), exit_price_source=data.get("exit_price_source"), mtf_trend_intact_at_close=data.get("mtf_trend_intact_at_close"),
                                   pnl_min_pct=data.get("pnl_min_pct"), pnl_min_after_touch_pct=data.get("pnl_min_after_touch_pct"),
                                   peak_at_min_after_pct=data.get("peak_at_min_after_pct"), touch_delay_sec=data.get("touch_delay_sec"),
                                   path_complete=data.get("path_complete"))
                else:
                    # v3.2 — diagnostic : auparavant, si aucune ligne ouverte
                    # ne correspondait (coin/action), la fermeture etait
                    # perdue EN SILENCE (le trade restait "ouvert" en base
                    # pour toujours, jamais comptabilise dans le Bilan, meme
                    # si la position etait bien fermee en memoire).
                    # v4.220 — SUR DEMANDE EXPLICITE, FIX BUG CRITIQUE : ne
                    # se contente plus de logger — cree un enregistrement de
                    # secours directement, pour ne plus jamais perdre le
                    # PnL/statistiques de cette fermeture, meme sans pouvoir
                    # retrouver la ligne d ouverture d origine (confidence,
                    # levier, SL/TP initiaux resteront vides pour cette
                    # ligne specifiquement, mais le PnL est preserve).
                    # v4.313 — SUR DEMANDE EXPLICITE : la ligne "ouverte"
                    # d origine, elle, reste orpheline en base pour toujours
                    # (jamais fermee) — confirme par un cas reel (plusieurs
                    # orphelins Spot-Accum/Funding/Forex). Journalise ici
                    # TOUT ce qui pourrait expliquer l echec de correspondance
                    # (trade_uid recherche, et l etat REEL de toutes les
                    # lignes encore ouvertes pour ce coin+action, toutes
                    # strategies confondues) — la PROCHAINE occurrence donnera
                    # la preuve exacte au lieu de deviner.
                    debug_rows = db.debug_open_rows_for_coin_action(ticker, action)
                    msg = (f"[event_consumer] ATTENTION : aucun trade ouvert trouve en base pour {ticker}/{action} "
                           f"(symbol brut={data.get('symbol')!r}, type brut={data.get('type')!r}, "
                           f"trade_uid recherche={data.get('trade_uid')!r}, strategy recherchee={data.get('strategy')!r}) "
                           f"— lignes encore ouvertes pour ce coin/action (toutes strategies) : {debug_rows} "
                           f"— creation d un enregistrement de secours.")
                    print(msg)
                    _push_log("error", msg)
                    db.insert_orphaned_closed_trade(
                        ticker, action, data.get("entry"), data.get("exit"), data.get("pnl"),
                        data.get("reason"), strategy=data.get("strategy"), trade_mode=data.get("trade_mode"),
                        peak_pnl=data.get("peak_pnl_usd"), peak_pnl_pct=data.get("peak_pnl_pct"),
                        fees_paid=data.get("fees_paid"),
                    )
            elif etype == "startup_ready":
                # v3.2 — "programme balai" automatique au demarrage : a ce
                # stade, la reconciliation Hyperliquid (mode live) ou la
                # restauration des positions paper (voir bot_engine.py) est
                # DEJA terminee — bot.states reflete donc l etat REEL exact.
                # Tout trade "ouvert" en base qui ne correspond a AUCUNE
                # position reelle a cet instant precis est forcement un
                # orphelin (pas besoin d attendre 24h de delai comme pour le
                # nettoyage manuel en cours de session, ou l incertitude est
                # plus grande) — nettoyage immediat pour un tableau de bord
                # propre des le demarrage.
                try:
                    protected_ids = _compute_protected_trade_ids()
                    dup, stale = db.cleanup_signals(stale_hours=0, protected_ids=protected_ids)
                    if dup or stale:
                        msg = f"🧹 Nettoyage automatique au demarrage : {dup} doublon(s) + {stale} orphelin(s) supprime(s)."
                        print(f"[AUDIT] {msg}")
                        _push_log("ok", msg)
                except Exception as e:
                    print(f"[event_consumer] Erreur nettoyage automatique au demarrage : {e}")
            elif etype == "active_coins_auto_added":
                # v3.2 — un actif inactif vient d etre auto-active suite a une
                # opportunite de tres forte confiance (voir
                # _gate_active_or_auto_activate dans bot_engine.py). On
                # persiste la nouvelle liste pour qu elle survive aux
                # redemarrages, exactement comme un changement manuel.
                new_list = data.get("active_coins")
                if new_list:
                    db.set_config_override("ACTIVE_COINS", new_list)
                    print(f"[AUDIT] ACTIVE_COINS auto-etendu suite a une opportunite forte sur {data.get('ticker')} : {new_list}")
        except Exception as e:
            print(f"[event_consumer] Erreur traitement evenement {etype}: {e}")
        push.on_event(etype, data)      # v4.376 — alertes (apres l ecriture en base : la perte du jour inclut ce trade)


threading.Thread(target=_consume_events, daemon=True).start()

app = FastAPI(title="HyperBot API")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.on_event("shutdown")
def _on_shutdown():
    # Best-effort : accumule le temps de fonctionnement si le process
    # s arrete proprement (Railway envoie SIGTERM avant un redeploiement).
    # Ne couvre pas un arret brutal (crash, kill -9) — dans ce cas, la
    # derniere fraction de temps depuis "running_since" ne sera comptee
    # qu au prochain calcul via _get_running_seconds si "running_since"
    # est encore renseigne (le calcul inclut deja la session en cours).
    # v4.14 — trading_enabled (pas running, qui reste toujours actif tant
    # que le process tourne desormais) : ce compteur mesure le temps de
    # TRADING actif, pas la duree de vie du process/moteur.
    if bot.trading_enabled:
        _mark_running_stop_and_accumulate()


# ─────────────────────────────────────────────────────────────────────────
#  TEMPS DE FONCTIONNEMENT REEL (exclut les periodes d arret)
# ─────────────────────────────────────────────────────────────────────────
# total_running_seconds (persiste en base) + running_since (horodatage du
# demarrage en cours, tant que le bot tourne). La duree affichee au client
# ne compte donc que le temps ou le bot a reellement tourne, jamais les
# periodes ou il etait arrete — contrairement a un simple "temps ecoule
# depuis le dernier reset".
def _mark_running_start():
    db.set_meta("running_since", db.now_iso())


def _mark_running_stop_and_accumulate():
    since = db.get_meta("running_since")
    if not since:
        return
    try:
        started = datetime.fromisoformat(since)
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        total = float(db.get_meta("total_running_seconds", "0")) + max(elapsed, 0)
        db.set_meta("total_running_seconds", str(total))
    except Exception as e:
        print(f"[uptime] Erreur calcul temps de fonctionnement: {e}")
    finally:
        db.set_meta("running_since", "")


def _get_running_seconds() -> float:
    """Temps de fonctionnement cumule, y compris la session en cours si le
    bot tourne actuellement (sans attendre un arret pour la comptabiliser)."""
    total = float(db.get_meta("total_running_seconds", "0"))
    since = db.get_meta("running_since")
    if since:
        try:
            started = datetime.fromisoformat(since)
            total += max((datetime.now(timezone.utc) - started).total_seconds(), 0)
        except Exception:
            pass
    return total


# ─────────────────────────────────────────────────────────────────────────
#  AUTO-DEMARRAGE PERSISTANT (independant de la page web)
# ─────────────────────────────────────────────────────────────────────────
# Le bot tourne cote serveur (dans ce process), pas dans le navigateur : une
# fois lance, il continue de tourner meme navigateur ferme. Pour qu il
# redemarre TOUT SEUL apres un redeploiement/redemarrage Railway (sans
# intervention manuelle sur la page web), on persiste un etat souhaite
# ("running"/"stopped") en base, mis a jour par /api/bot/start et
# /api/bot/stop. Au boot du process, on relit cet etat :
#   - "running" (valeur par defaut, y compris au tout premier deploiement)
#     -> demarrage automatique si la cle/wallet sont configures
#   - "stopped" (l utilisateur a explicitement clique ARRETER) -> reste
#     arrete tant qu il ne clique pas DEMARRER, meme apres un redeploiement.
def _auto_start_if_desired():
    # v4.14 — SUR DEMANDE EXPLICITE : le MOTEUR (collecte de prix,
    # indicateurs, WebSocket, gestion des positions ouvertes) doit tourner
    # en continu des que le process demarre, INDEPENDAMMENT du dernier etat
    # "running"/"stopped" choisi par l utilisateur — cet etat persiste ne
    # controle desormais plus que l ouverture de NOUVEAUX trades.
    if not cfg.get("PRIVATE_KEY") or not cfg.get("WALLET_ADDRESS"):
        _push_log("warn", "Moteur non demarre : cle API / wallet Hyperliquid non configures. Configurez-les puis redemarrez le service.")
        return
    bot.start_engine()
    _push_log("ok", "Moteur demarre (collecte de prix/indicateurs, WebSocket) — actif en continu.")

    desired = db.get_meta("bot_desired_state", "running")
    if desired != "running":
        _push_log("info", "Trading non active automatiquement (dernier etat : ARRETE manuellement). Le moteur tourne, mais aucun nouveau trade ne s ouvrira tant que DEMARRER n est pas reclique.")
        return
    bot.start()
    _mark_running_start()
    _push_log("ok", "Trading active automatiquement (etat persistant : en cours d execution).")


_auto_start_if_desired()


# ─────────────────────────────────────────────────────────────────────────
#  AUTHENTIFICATION
# ─────────────────────────────────────────────────────────────────────────
class AuthBody(BaseModel):
    email: str
    password: str


def require_user(authorization: Optional[str] = Header(None)) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "Authentification requise")
    token = authorization.split(" ", 1)[1]
    email = auth.decode_token(token)
    if not email:
        raise HTTPException(401, "Token invalide ou expire")
    # v4.264 — le compte doit toujours exister (un token d un compte supprime
    # ne donne plus acces au bot).
    if not db.get_user_by_email(email):
        raise HTTPException(401, "Compte inconnu")
    return email


@app.post("/api/register")
def register(body: AuthBody):
    # Un seul compte proprietaire pour ce bot — l inscription se ferme
    # d elle-meme des qu un premier compte existe (evite qu un tiers
    # s inscrive et prenne le controle du bot si l URL fuite).
    if db.user_count() > 0:
        raise HTTPException(403, "Inscription fermee — un compte existe deja sur cette instance")
    if len(body.password) < 8:
        raise HTTPException(400, "Mot de passe trop court (8 caracteres minimum)")
    if db.get_user_by_email(body.email):
        raise HTTPException(400, "Ce compte existe deja")
    db.create_user(body.email, auth.hash_password(body.password))
    return {"ok": True}


@app.post("/api/login")
def login(body: AuthBody):
    user = db.get_user_by_email(body.email)
    if not user or not auth.verify_password(body.password, user["password_hash"]):
        raise HTTPException(401, "Email ou mot de passe incorrect")
    token = auth.create_token(body.email)
    return {"token": token, "email": body.email}


@app.post("/api/logout")
def logout(email: str = Depends(require_user)):
    # JWT sans etat : rien a invalider cote serveur, le client oublie le token.
    return {"ok": True}


# ─────────────────────────────────────────────────────────────────────────
#  HELPERS DE MAPPING (bot_engine <-> contrat API)
# ─────────────────────────────────────────────────────────────────────────
def _mask(secret: Optional[str]) -> str:
    if not secret:
        return ""
    return "****" + secret[-4:] if len(secret) > 4 else "****"


def _public_config() -> Dict[str, Any]:
    last_times = [s.last_price_time for s in bot.states.values() if s.last_price_time]
    return {
        "trading_mode": cfg.get("MODE", "paper"),
        "fees_in_stats": bool(cfg.get("FEES_IN_STATS", 1)),                       # v4.355
        "fee_round_trip_pct": float(cfg.get("FEE_ROUND_TRIP_PCT", 0.09)),
        "profile": cfg.get("PROFILE", "swing"),
        "position_pct": cfg.get("POSITION_SIZE_PCT"),
        # v4.10 — moteur ASYMETRIQUE : SL en % de E (perte $ plafonnee,
        # independante du levier), TP en % de mouvement de prix (gain $
        # amplifie par le levier). max_loss_usd/quick_profit_usd = valeurs $
        # informatives a titre indicatif (capital courant, levier x1 pour le TP).
        # Utiliser sl_pct_of_e / ttp_arm1_price_pct pour les valeurs reelles.
        "max_loss_usd": round(cfg["CAPITAL_USD"] * cfg["POSITION_SIZE_PCT"] / 100 * cfg.get("SL_PCT_OF_E", 1.0) / 100, 4),
        "quick_profit_usd": round(cfg["CAPITAL_USD"] * cfg["POSITION_SIZE_PCT"] / 100 * cfg.get("TTP_ARM1_PRICE_PCT", 1.0) / 100, 4),
        "sl_pct_of_e": cfg.get("SL_PCT_OF_E", 1.0),
        "ttp_arm1_price_pct": cfg.get("TTP_ARM1_PRICE_PCT", 1.0),
        "ttp_lock1_price_pct": cfg.get("TTP_LOCK1_PRICE_PCT", 0.8),
        "ttp_arm2_price_pct": cfg.get("TTP_ARM2_PRICE_PCT", 1.3),
        "ttp_trail_gap_price_pct": cfg.get("TTP_TRAIL_GAP_PRICE_PCT", 0.3),
        "max_open_trades": cfg.get("MAX_OPEN_TRADES", 15),
        "auto_activate_confidence_pct": cfg.get("AUTO_ACTIVATE_CONFIDENCE_PCT", 80.0),
        "active_coins": cfg.get("ACTIVE_COINS") or SUPPORTED_TICKERS,
        "manual_exclude_coins": cfg.get("MANUAL_EXCLUDE_COINS", []),
        "supported_coins": SUPPORTED_TICKERS,
        "wallet": cfg.get("WALLET_ADDRESS", ""),
        "api_key": _mask(cfg.get("PRIVATE_KEY", "")),
        "finnhub_key": _mask(cfg.get("FINNHUB_API_KEY", "")),
        "filter_hours": cfg.get("CRYPTO_OFFPEAK_ENABLED", True),
        "filter_weekend": bool(cfg.get("FOREX_SYMBOLS")),
        "filter_macro": cfg.get("CPI_BLACKOUT_ENABLED", True),
        "accumulation_enabled": cfg.get("ACCUMULATION_ENABLED", False),
        "accumulation_require_trend_confirm": cfg.get("ACCUMULATION_REQUIRE_TREND_CONFIRM", False),
        "accumulation_max_trades": cfg.get("ACCUMULATION_MAX_TRADES", 3),
        "accumulation_proximity_pct": cfg.get("ACCUMULATION_PROXIMITY_PCT", 1.0),
        "sl_ttp_adaptive_enabled": cfg.get("SL_TTP_ADAPTIVE_ENABLED", False),
        "funding_mode_enabled": cfg.get("FUNDING_MODE_ENABLED", False),
        "funding_mode_live_allowed": cfg.get("FUNDING_MODE_LIVE_ALLOWED", False),
        "strategy_mode_override": cfg.get("STRATEGY_MODE_OVERRIDE", {
            "forex": None, "accumulation": None, "funding_contrarian": None, "spot_accumulation": None,
        }),
        "strategy_trading_enabled": cfg.get("STRATEGY_TRADING_ENABLED", {
            "forex": True, "accumulation": True, "funding_contrarian": True, "spot_accumulation": True,
        }),
        "require_sr_ema200_separation": cfg.get("REQUIRE_SR_EMA200_SEPARATION", True),
        "unified_require_sr_amplitude": cfg.get("UNIFIED_REQUIRE_SR_AMPLITUDE", False),
        "unified_simplified_mode": cfg.get("UNIFIED_SIMPLIFIED_MODE", True),
        "unified_full_simplified_mode": cfg.get("UNIFIED_FULL_SIMPLIFIED_MODE", True),
        "ttp_trend_hold_filter_enabled": cfg.get("TTP_TREND_HOLD_FILTER_ENABLED", True),
        "unified_require_adx_confirm": cfg.get("UNIFIED_REQUIRE_ADX_CONFIRM", False),
        "ttp_dynamic_from_arm1": cfg.get("TTP_DYNAMIC_FROM_ARM1", True),
        "spot_accum_enabled": cfg.get("SPOT_ACCUM_ENABLED", False),
        "spot_accum_sl_enabled": cfg.get("SPOT_ACCUM_SL_ENABLED", False),
        "spot_accum_require_adx_confirm": cfg.get("SPOT_ACCUM_REQUIRE_ADX_CONFIRM", False),
        "accumulation_reversal_exit_enabled": cfg.get("ACCUMULATION_REVERSAL_EXIT_ENABLED", True),
        "spot_accum_hard_sl_enabled": cfg.get("SPOT_ACCUM_HARD_SL_ENABLED", True),
        "accumulation_active_coins": cfg.get("ACCUMULATION_ACTIVE_COINS"),
        "funding_active_coins": cfg.get("FUNDING_ACTIVE_COINS"),
        "spot_accum_active_coins": cfg.get("SPOT_ACCUM_ACTIVE_COINS"),
        "ai_continuous": db.get_config_override("ai_continuous", False),
        "running": bot.trading_enabled,
        "is_running": bot.trading_enabled,
        "engine_running": bot.running,  # v4.14 — moteur (collecte/WS), toujours actif independamment du trading
        "started_at": db.get_meta("running_since") or None,
        "last_scan": max(last_times).isoformat() if last_times else None,
        "ws_connected": bot.info is not None,
        "ws_healthy": bot._is_ws_healthy() if bot.info is not None else False,
        "offpeak_hour_start": cfg.get("CRYPTO_OFFPEAK_HOUR_START_UTC", 21),
        "offpeak_hour_end": cfg.get("CRYPTO_OFFPEAK_HOUR_END_UTC", 23),
        "hyperliquid_configured": bool(cfg.get("PRIVATE_KEY") and cfg.get("WALLET_ADDRESS")),
    }


def _apply_and_persist(key: str, value):
    cfg[key] = value
    db.set_config_override(key, value)


def _analyze_confidence_calibration(min_samples: int = 15):
    """v4.2 — Calibration du score de confiance a partir des resultats REELS
    des trades clotures, plutot que de se fier a des poids choisis a la
    main dans le code. Pour chaque indicateur present dans
    confidence_breakdown (macd, bollinger, volume, ema200, ema_mid,
    momentum, consec, breakout), compare le taux de reussite des trades ou
    il etait confirme vs ceux ou il ne l etait pas :
      - Si l ecart (lift) est positif et mesure sur un echantillon
        suffisant dans les deux groupes (>= min_samples), l indicateur a
        vraiment un pouvoir predictif sur CE bot / CES marches -> son poids
        est augmente proportionnellement.
      - Si l ecart est nul/negatif, l indicateur n apporte rien de mesurable
        ici -> son poids diminue (mais ne tombe jamais a zero strict, pour
        eviter de l exclure definitivement sur un echantillon qui pourrait
        encore etre bruite).
      - Si l echantillon est insuffisant dans un des deux groupes, on ne
        touche PAS a son poids actuel (mieux vaut garder le defaut que de
        calibrer sur trop peu de donnees) et on le signale clairement.
    Le total de points redistribue entre indicateurs calibrables est
    preserve, pour que les seuils de confiance en % (65%, 75%, 85%...)
    gardent le meme ordre de grandeur apres calibration.
    """
    trades = db.get_all_closed_trades()
    rows = []
    for t in trades:
        bd = t.get("confidence_breakdown")
        pnl = t.get("pnl")
        if not bd or pnl is None:
            continue
        try:
            bd = json.loads(bd)
        except Exception:
            continue
        rows.append({"breakdown": bd, "win": pnl > 0})

    total = len(rows)
    overall_wins = sum(1 for r in rows if r["win"])
    overall_win_rate = round(overall_wins / total * 100, 1) if total else 0.0

    current_weights = cfg.get("CONFIDENCE_WEIGHTS", {})
    all_keys = set(current_weights.keys())
    for r in rows:
        all_keys |= set(r["breakdown"].keys())

    per_indicator = {}
    for key in sorted(all_keys):
        true_rows  = [r for r in rows if r["breakdown"].get(key) is True]
        false_rows = [r for r in rows if r["breakdown"].get(key) is False]
        n_true, n_false = len(true_rows), len(false_rows)
        wr_true  = round(sum(1 for r in true_rows if r["win"]) / n_true * 100, 1) if n_true else None
        wr_false = round(sum(1 for r in false_rows if r["win"]) / n_false * 100, 1) if n_false else None
        enough_data = n_true >= min_samples and n_false >= min_samples
        lift = round(wr_true - wr_false, 1) if enough_data else None
        per_indicator[key] = {
            "current_weight": current_weights.get(key, 0),
            "n_true": n_true, "n_false": n_false,
            "win_rate_true": wr_true, "win_rate_false": wr_false,
            "lift_pts": lift, "enough_data": enough_data,
        }

    calibratable = [k for k, v in per_indicator.items() if v["enough_data"]]
    new_weights = dict(current_weights)
    if calibratable:
        pool = sum(current_weights.get(k, 0) for k in calibratable)
        raw = {k: max(per_indicator[k]["lift_pts"], 0) + 0.5 for k in calibratable}
        raw_total = sum(raw.values())
        for k in calibratable:
            new_weights[k] = round(raw[k] / raw_total * pool, 1) if raw_total > 0 else current_weights.get(k, 0)
    for k, v in per_indicator.items():
        v["suggested_weight"] = new_weights.get(k, v["current_weight"])

    return {
        "total_closed_trades": len(trades),
        "trades_with_breakdown": total,
        "overall_win_rate": overall_win_rate,
        "min_samples_required": min_samples,
        "indicators": per_indicator,
        "current_weights": current_weights,
        "suggested_weights": new_weights,
        "ready_to_calibrate": len(calibratable) > 0,
    }


def _analyze_confidence_by_asset(min_trades: int = 5):
    """v4.4 — Probabilite de reussite REELLE par actif, calculee a partir de
    l historique des trades clotures (pas une estimation theorique). Pour
    chaque actif ayant deja des trades clotures : nombre de trades, taux de
    reussite, PnL net, confiance moyenne a l entree. min_trades est
    ajustable dynamiquement (parametre de requete) : plus il est bas, plus
    d actifs apparaissent tot, mais avec un echantillon moins fiable."""
    trades = db.get_all_closed_trades()
    by_coin = {}
    for t in trades:
        pnl = t.get("pnl")
        coin = t.get("coin")
        if pnl is None or not coin:
            continue
        by_coin.setdefault(coin, []).append(t)

    results = []
    for coin, rows in by_coin.items():
        n = len(rows)
        wins = [r for r in rows if (r.get("pnl") or 0) > 0]
        win_rate = round(len(wins) / n * 100, 1) if n else 0.0
        net = round(sum(r.get("pnl") or 0 for r in rows), 2)
        confs = [r.get("confidence") for r in rows if r.get("confidence") is not None]
        avg_conf = round(sum(confs) / len(confs), 1) if confs else None
        results.append({
            "coin": coin,
            "n_trades": n,
            "win_rate": win_rate,
            "net_pnl": net,
            "avg_confidence": avg_conf,
            "enough_data": n >= min_trades,
        })

    results.sort(key=lambda r: (r["enough_data"], r["win_rate"]), reverse=True)
    return {
        "min_trades_required": min_trades,
        "total_closed_trades": len(trades),
        "assets": results,
    }


def _open_positions() -> List[Dict[str, Any]]:
    out = []
    # v4.121 — SUR DEMANDE EXPLICITE : Accumulation a desormais son PROPRE
    # emplacement (bot.accum_states), sans quoi ses positions n apparaitraient
    # JAMAIS dans l interface (affichage "Trades ouverts" de tous les
    # onglets, alimente par cette fonction). Combine les deux sources —
    # slot_key identique entre les deux dicts, sans risque de collision
    # dans la liste de sortie (chaque position devient une entree separee).
    combined_states = list(bot.states.items()) + list(bot.accum_states.items())
    for slot_key, state in combined_states:
        pos = state.position
        if not pos:
            continue
        try:
            ticker = be.ticker_from_slot_key(slot_key)
            price = state.current_price or pos["entry"]
            if pos["type"] == "long":
                raw_price_move_pct = (price - pos["entry"]) / pos["entry"] * 100
            else:
                raw_price_move_pct = (pos["entry"] - price) / pos["entry"] * 100
            # v4.189 — historique : bot_engine.py compare ses SEUILS internes
            # (SL, TTP) au mouvement de prix BRUT, jamais leverage-ajuste —
            # le % affiche doit rester BRUT pour correspondre a la decision
            # reelle du bot.
            # v4.229 — tentative d unifier sur un pnl_pct amplifie par le
            # levier, cote bot_engine.py ET ici — ANNULEE en urgence (v4.231)
            # suite a un effet retroactif dangereux sur les positions deja
            # ouvertes au redeploiement (fermetures en cascade cote bot,
            # non repercutees cote Hyperliquid). bot_engine.py est revenu au
            # % BRUT pour ses seuils internes — cet affichage doit donc
            # rester coherent avec CETTE decision reelle, pas avec le ROE%
            # d Hyperliquid (le montant $ ci-dessous, lui, reste correct et
            # leverage-ajuste — seul le % redevient brut).
            # v4.325 — pnl_pct reste BRUT ici (comparable aux seuils internes) ; l interface
            # l affiche en % de marge (x levier) comme Hyperliquid, avec le % de prix a cote.
            leverage_for_pnl = pos.get("leverage", 1)
            pnl_pct = raw_price_move_pct
            pnl = pos["size"] * leverage_for_pnl * pnl_pct / 100

            # opened_at est stocke par bot_engine.py au format "%d/%m/%Y %H:%M:%S"
            # (francais, sans fuseau) — converti en ISO pour que new Date(...) le
            # parse correctement cote navigateur (ambigu sinon selon le moteur JS).
            # v3.2 — FIX : sans le "+00:00" explicite, le navigateur interprete
            # cette heure comme une heure LOCALE (pas UTC), decalant l affichage
            # de plusieurs heures selon le fuseau du visiteur (durees et heures
            # d ouverture incoherentes, ex: "0min" alors que l heure affichee
            # semblait ancienne).
            opened_at_iso = None
            try:
                opened_at_iso = datetime.strptime(pos["opened_at"], "%d/%m/%Y %H:%M:%S").replace(tzinfo=timezone.utc).isoformat()
            except Exception:
                pass

            # Complements (leverage, take_profit1/2) recuperes depuis la ligne DB
            # ouverte correspondante — calcules une seule fois a l entree, voir
            # bot_engine.py (emit "trade_opened").
            action = "LONG" if pos["type"] == "long" else "SHORT"
            leverage = cfg.get("LEVERAGE", 1)
            tp1 = tp2 = None
            try:
                # v4.159 — FIX BUG CRITIQUE : transmet desormais la
                # strategie exacte de CETTE position pour eviter de
                # recuperer par erreur le levier/TP d une position d un
                # AUTRE mode sur le meme coin+action (ex: Normal ET
                # Accumulation shorts simultanes sur le meme actif).
                trade_id = db.get_open_trade_id_by_coin_action(ticker, action, pos.get("strategy"))
                if trade_id:
                    rows = db.get_trades(limit=1000)
                    match = next((r for r in rows if r["id"] == trade_id), None)
                    if match:
                        leverage = match.get("leverage") or leverage
                        tp1 = match.get("take_profit1")
                        tp2 = match.get("take_profit2")
            except Exception as e:
                print(f"[_open_positions] Erreur enrichissement DB pour {ticker}: {e}")

            # v4.15 — Pic de PnL latent atteint jusqu ici pendant la vie du
            # trade (trailing principal ou protection anticipee, selon
            # lequel est actif) — visible dans le panneau "Trades ouverts".
            # v4.188 — FIX BUG CRITIQUE : Spot-Accum a son PROPRE traqueur de
            # pic (state.spot_accum_peak_pnl_pct), separe et JAMAIS mis a
            # jour via peak_pnl_usd/tier0_peak_pnl_usd (utilises par les
            # AUTRES modes uniquement) — donnait un pic errone/perime
            # (parfois INFERIEUR au PnL actuel, incoherent), confirme par
            # une position reelle observee (pic affiche +0.20% alors que le
            # PnL courant etait deja a +0.77%).
            # v4.241 — SUR DEMANDE EXPLICITE, FIX BUG CRITIQUE : ces 4
            # strategies partagent desormais TOUTES le meme mecanisme de
            # sortie unifie, qui met a jour EXCLUSIVEMENT
            # state.spot_accum_peak_pnl_pct — les autres champs (tier0/
            # tier1/absolu, lus dans la branche "else" ci-dessous) ne sont
            # plus jamais mis a jour pour Accumulation/Forex/Funding
            # depuis l unification, risquant d y afficher un pic obsolete
            # ou absent.
            # v4.301 — FIX BUG D AFFICHAGE : une position "top-down" (engine
            # == "mtf") a sa PROPRE gestion de sortie (_manage_mtf, SL/TP sur
            # la structure, breakeven a +1R, suiveur au-dela de +2R) —
            # totalement independante de state.spot_accum_armed / 
            # state.spot_accum_peak_pnl_pct, qui ne sont JAMAIS mis a jour
            # pour ce moteur (voir bot_engine.py, _manage_position_impl
            # retourne vers _manage_mtf avant d atteindre ce mecanisme).
            # Sans ce cas separe, le tableau de bord affichait a tort
            # "Trailing : pas encore arme" en boucle sur des trades top-down
            # deja passes en breakeven/suiveur depuis longtemps — confirme
            # par un cas reel (SL deja remonte au prix d entree +1R, pic en
            # cours depuis des heures, badge pourtant reste "pas encore
            # arme"). Utilise desormais le vrai pic (v4.301, voir
            # _manage_mtf) et l etat reel de breakeven/suiveur de CE trade.
            if pos.get("engine") == "mtf":
                peak_pnl_usd = (
                    state.peak_pnl_usd if state.peak_pnl_usd is not None
                    else state.absolute_peak_pnl_usd
                )
                pos_size = pos.get("size", 0)
                pos_leverage = pos.get("leverage", 1)
                peak_pnl_pct = (
                    round(peak_pnl_usd / (pos_size * pos_leverage) * 100, 3)
                    if peak_pnl_usd is not None and pos_size and pos_leverage
                    else None
                )
            elif pos.get("strategy") in ("spot_accumulation", "accumulation", "forex", "funding_contrarian"):
                peak_pnl_pct = round(state.spot_accum_peak_pnl_pct, 3) if state.spot_accum_peak_pnl_pct is not None else None
                # v4.231 — ROLLBACK URGENT de v4.229 (voir bot_engine.py) :
                # spot_accum_peak_pnl_pct est de nouveau un % BRUT — la
                # multiplication par le levier redevient necessaire ici.
                peak_pnl_usd = (
                    round(pos.get("size", 0) * pos.get("leverage", 1) * peak_pnl_pct / 100, 4)
                    if peak_pnl_pct is not None else None
                )
            else:
                # v4.18 — priorite tier1 > tier0 > pic absolu (des le 1er cycle en
                # profit, meme sous 0.5%) — voir close_position pour la meme logique.
                peak_pnl_usd = (
                    state.peak_pnl_usd if state.peak_pnl_usd is not None
                    else state.tier0_peak_pnl_usd if state.tier0_peak_pnl_usd is not None
                    else state.absolute_peak_pnl_usd
                )
                # v4.231 — ROLLBACK URGENT de v4.229 : peak_pnl_usd reste en
                # dollars reels (incluant le levier) — diviser par
                # (size*leverage) redonne le % BRUT, coherent avec le
                # pnl_pct BRUT restaure dans bot_engine.py.
                pos_size = pos.get("size", 0)
                pos_leverage = pos.get("leverage", 1)
                peak_pnl_pct = (
                    round(peak_pnl_usd / (pos_size * pos_leverage) * 100, 3)
                    if peak_pnl_usd is not None and pos_size and pos_leverage
                    else None
                )

            out.append({
                "id": slot_key,
                "coin": ticker,
                "action": action,
                "entry_price": pos["entry"],
                "current_price": price,
                "size": pos["size"],
                "size_usdc": round(pos["size"], 2),
                "leverage": leverage,
                "stop_loss": pos["sl"],
                "take_profit1": tp1,
                "take_profit2": tp2,
                "opened_at": opened_at_iso,
                "pnl": round(pnl, 4),
                "pnl_pct": round(pnl_pct, 3),
                "roe_pct": round(pnl_pct * (leverage_for_pnl or 1), 3),  # v4.324 : % de MARGE (mouvement de prix x levier), affichage seul
                # v4.293 — rapprochement avec Hyperliquid (positions live)
                "swing": bool(pos.get("mtf_swing")),     # v4.359 : position du profil Swing
                "pnl_hyperliquid": pos.get("hl_unrealized_pnl"),
                "hl_sync_age_sec": round(time.time() - pos["hl_sync_ts"]) if pos.get("hl_sync_ts") else None,
                "fees_est": round(pos["size"] * pos.get("leverage", 1) * 0.0009, 4),
                "pnl_net_est": round(pnl - pos["size"] * pos.get("leverage", 1) * 0.0009, 4),
                "peak_pnl": round(peak_pnl_usd, 4) if peak_pnl_usd is not None else None,
                "peak_pnl_pct": peak_pnl_pct,
                # v4.61 — SUR DEMANDE EXPLICITE : diagnostic precis du
                # mecanisme de trailing REELLEMENT actif sur ce trade, pour
                # eviter de devoir deviner ou chercher dans les logs bruts
                # (cas observe : pic affiche de 1.32% sans sortie declenchee
                # malgre un trailing dynamique cense s armer a 1%).
                "tp_stage": state.tp_stage,  # 0=aucun, 1=arme (tier1)
                "effective_mode": pos.get("effective_mode", "paper"),  # v4.90 — mode reel de CE trade
                # v4.301 — champs dedies au moteur top-down : distincts de
                # spot_accum_armed (jamais mis a jour pour ce moteur, voir
                # plus haut) — reflete l etat REEL de _manage_mtf sur ce
                # trade precis (SL deja remonte a l entree ? suiveur actif ?).
                "mtf_engine": pos.get("engine") == "mtf",
                "mtf_be_done": pos.get("mtf_be_done", False),
                "mtf_target": pos.get("tp"),
                # v4.336 — SL dynamique Forex : niveau courant, pic et etat (affichage)
                "mtf_lock_fraction": cfg.get("MTF_LOCK_FRACTION", 0.5),  # v4.340
                "dsl_on": bool(pos.get("strategy") == "forex" and cfg.get("FOREX_DYNAMIC_SL", 1)),
                "dsl_sl": pos.get("dsl_sl"),
                "dsl_peak": pos.get("dsl_peak"),
                "dsl_arm_pct": cfg.get("FOREX_TTP_ARM_PCT"),
                "spot_accum_armed": state.spot_accum_armed,  # v4.62 — FIX : Spot-Accum a son propre armement, separe de tp_stage
                "spot_accum_arm_pct_used": (cfg.get("FOREX_TTP_ARM_PCT") if pos.get("strategy") == "forex" else cfg.get("SPOT_ACCUM_TTP_ARM_PCT")),  # v4.63 — seuil REELLEMENT lu, pour verifier sans deviner
                "spot_accum_peak_pnl_pct_internal": round(state.spot_accum_peak_pnl_pct, 3) if state.spot_accum_peak_pnl_pct is not None else None,
                "tier0_armed": state.tier0_armed,
                # v4.241 — SUR DEMANDE EXPLICITE, FIX BUG D AFFICHAGE : ces
                # 4 strategies partagent desormais TOUTES le meme mecanisme
                # de sortie unifie (state.spot_accum_armed), pas seulement
                # "spot_accumulation" — l ancienne condition affichait a
                # tort "aucun tier arme" pour Accumulation/Forex/Funding,
                # meme quand un pic avait reellement arme le trailing
                # (confirme par un cas reel : PENDLE short, pic +0.84%,
                # jamais montre comme arme).
                "peak_source": "spot_accum" if pos.get("strategy") in ("spot_accumulation", "accumulation", "forex", "funding_contrarian") else ("tier1" if state.peak_pnl_usd is not None else ("tier0" if state.tier0_peak_pnl_usd is not None else "absolu (aucun tier arme)")),
                # v4.243 — SUR DEMANDE EXPLICITE : tracabilite explicite du
                # mecanisme ayant valide cette entree (flirt, cassure
                # fraiche, volume, cassure ratee, etoile filante, pression
                # directionnelle, signal standard) — memorise a l ouverture,
                # jamais recalcule apres coup.
                "entry_mechanism": pos.get("entry_mechanism", "Non enregistré (position antérieure à ce suivi)"),
                "computed_exit_threshold_pct": (
                    round(peak_pnl_pct - cfg.get("TTP_DYNAMIC_TRAIL_GAP_PCT", 0.5), 3)
                    if state.tp_stage == 1 and peak_pnl_pct is not None and cfg.get("TTP_DYNAMIC_FROM_ARM1", True)
                    else None
                ),
                # v4.30 — seuils SL/TTP REELLEMENT appliques a CE trade (fixes
                # ou adaptatifs a l ATR, figes a l ouverture) — voir bot_engine.py
                # _finalize_open, stockes sur la position elle-meme.
                "sl_pct_used": pos.get("sl_pct_of_e"),
                "ttp_arm1_pct_used": pos.get("ttp_arm1_pct"),
                "ttp_lock1_pct_used": pos.get("ttp_lock1_pct"),
                "ttp_arm2_pct_used": pos.get("ttp_arm2_pct"),
                "ttp_gap_pct_used": pos.get("ttp_gap_pct"),
                "adaptive_sl_ttp": pos.get("adaptive_sl_ttp", False),
                "strategy": pos.get("strategy", "forex"),  # v4.34 — FIX : jamais expose ici avant
                "target_price": pos.get("target_price"),  # v4.47 — objectif Spot-Accum (modifiable)
                "trailing_arm_price": pos.get("trailing_arm_price"),  # v4.49 — seuil structurel d'armement du trailing (modifiable)
            })
        except Exception as e:
            print(f"[_open_positions] Erreur sur la position {slot_key}, ignoree pour cette reponse: {e}")
            continue
    return out


def _trade_row_to_signal(row: Dict[str, Any]) -> Dict[str, Any]:
    # v4.185 — historique : calculait le % amplifie par le levier.
    # v4.250 — SUR DEMANDE EXPLICITE, FIX BUG CRITIQUE : jamais inclus dans
    # le rollback d urgence (v4.231, voir bot_engine.py) qui est revenu au
    # % BRUT pour les positions OUVERTES — creant une incoherence : le
    # meme declenchement (plafond immediat a 0.5% brut) s affichait
    # correctement en position ouverte, mais AMPLIFIE PAR LE LEVIER une
    # fois passe en historique (confirme par un cas reel : plusieurs
    # "STOP LOSS (plafond immediat)" Spot-Accum affichant -2.5% a -3.5%
    # au lieu des ~0.5% reellement declenches, sur des trades a levier
    # x5). Retire la multiplication, coherent avec le % BRUT partout.
    exit_pnl_pct = None
    try:
        entry_p = row["entry_price"]
        exit_p = row["exit_price"]
        if entry_p and exit_p:
            raw_move_pct = ((exit_p - entry_p) / entry_p * 100) if row["action"] == "LONG" else ((entry_p - exit_p) / entry_p * 100)
            exit_pnl_pct = round(raw_move_pct, 3)
    except (TypeError, KeyError, ZeroDivisionError):
        pass
    return {
        "id": row["id"],
        "coin": row["coin"],
        "action": row["action"],
        "confidence": row["confidence"],
        "leverage": row["leverage"],
        "position_size": row["position_size_pct"],
        "risk_reward": row["risk_reward"],
        "timeframe": row["timeframe"],
        "entry": row["entry_price"],
        "price": row["entry_price"],
        "rsi": row["rsi"] if "rsi" in row.keys() else None,
        "entry_reasons": row["entry_reasons"] if "entry_reasons" in row.keys() else None,
        "stop_loss": row["stop_loss"],
        "take_profit1": row["take_profit1"],
        "take_profit2": row["take_profit2"],
        "created_at": row["created_at"],
        "closed_at": row["closed_at"],
        "exit_price": row["exit_price"],
        "pnl": row["pnl"],
        "pnl_pct": exit_pnl_pct,
        "roe_pct": (round(exit_pnl_pct * (row["leverage"] or 1), 3) if exit_pnl_pct is not None else None),  # v4.324 : % de marge, affichage seul
        "reason": row["reason"],
        "strategy": row["strategy"] if "strategy" in row.keys() else "forex",
        "pnl_net": (round(_row_pnl(dict(row)), 4) if row["pnl"] is not None else None),       # v4.358 : net de frais (si FEES_IN_STATS)
        "fee_est": (round(_trade_fee_usd(dict(row)), 4) if row["pnl"] is not None else None),
        "peak_pnl": row["peak_pnl"] if "peak_pnl" in row.keys() else None,
        "peak_pnl_pct": row["peak_pnl_pct"] if "peak_pnl_pct" in row.keys() else None,
        "size_usd": row["size_usd"] if "size_usd" in row.keys() else None,
        "sl_pct_used": row["sl_pct_used"] if "sl_pct_used" in row.keys() else None,
        "ttp_arm1_pct_used": row["ttp_arm1_pct_used"] if "ttp_arm1_pct_used" in row.keys() else None,
        "adaptive_sl_ttp": bool(row["adaptive_sl_ttp"]) if "adaptive_sl_ttp" in row.keys() and row["adaptive_sl_ttp"] is not None else False,
        # v4.294 — resultat REEL Hyperliquid (trades live)
        "trade_mode": row["trade_mode"] if "trade_mode" in row.keys() else None,
        "pnl_real_hl": row["pnl_real_hl"] if "pnl_real_hl" in row.keys() else None,
        "fees_real": row["fees_real"] if "fees_real" in row.keys() else None,
        "pnl_net_real": (row["pnl_real_hl"] - row["fees_real"])
                        if "pnl_real_hl" in row.keys() and row["pnl_real_hl"] is not None and row["fees_real"] is not None else None,
        "fills_status": row["fills_status"] if "fills_status" in row.keys() else None,
    }


# ─────────────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────────────
class ConfigBody(BaseModel):
    trading_mode: Optional[str] = None
    position_pct: Optional[float] = None
    # v4.0 — champs legacy en $, encore acceptes pour compat avec l interface
    # actuelle : convertis a la volee en % de mouvement de prix (voir put_config).
    max_loss_usd: Optional[float] = None
    quick_profit_usd: Optional[float] = None
    # v4.9 — nouveaux champs natifs, en % de MOUVEMENT DE PRIX (a privilegier)
    sl_pct_of_e: Optional[float] = None
    ttp_arm1_price_pct: Optional[float] = None
    ttp_lock1_price_pct: Optional[float] = None
    ttp_arm2_price_pct: Optional[float] = None
    ttp_trail_gap_price_pct: Optional[float] = None
    max_open_trades: Optional[int] = None
    auto_activate_confidence_pct: Optional[float] = None
    wallet: Optional[str] = None
    api_key: Optional[str] = None
    active_coins: Optional[List[str]] = None


# v3.2 — Reglages avances : whitelist de parametres de STRATEGIE surs a
# exposer/editer librement depuis l interface (indicateurs, filtres,
# session, confiance...). Volontairement separee des reglages "structurels"
# (SYMBOLS, cles API, wallet...) qui ont deja leurs propres formulaires
# dedies et securises — pas question de les rendre modifiables via un
# simple champ numerique generique.
ADVANCED_SETTINGS = {
    "RSI_PERIOD":              {"label": "RSI - Periode",                     "default": 14},
    # v4.266 — flux de transactions (Spot-Accum / Accumulation)
    "ENTRY_FLOW_CONFIRM_MIN_PRESSURE": {"label": "Flux - confirmation exigee a l entree (0 = desactivee, ex: 0.1)", "default": 0.0},
    "ENTRY_FLOW_CONTRADICTION_THRESHOLD": {"label": "Flux - veto si pression contraire au-dela de", "default": 0.3},
    "TRADE_FLOW_WINDOW_SEC":   {"label": "Flux - fenetre d analyse (secondes)", "default": 180},
    # v4.268 — TTP Funding
    "FUNDING_TTP_ARM_PCT":       {"label": "Funding - TTP armement (% de prix)", "default": 0.5},
    # v4.351 — filtre de qualite du marche de Funding (pose en v4.317, jusqu ici modifiable seulement dans le code)
    "FUNDING_MAX_ACTIVITY_RATIO": {"label": "Funding - plafond d activite du marche (x l habitude ; 0 = AUCUN plafond). Au-dela, l entree est refusee ('marche trop actif')", "default": 1.1},
    "FUNDING_MAX_SPREAD_PCT":     {"label": "Funding - plafond de spread (% du prix ; 0 = AUCUN plafond). Au-dela, l entree est refusee ('spread trop large')", "default": 0.02},
    # v4.276 — regime de marche et plages horaires
    "MARKET_REGIME_FILTER_ENABLED": {"label": "Regime de marche - filtre actif (1 = oui, 0 = non)", "default": 0},
    "MARKET_REGIME_BREADTH_PCT": {"label": "Regime de marche - % minimal d actifs dans le meme sens", "default": 60},
    "SITUATION_RULES_ENABLED":         {"label": "Situations de marche fond 1h x court terme 5 min (1) ou regime global (0)", "default": 1},
    "ENTRY_ENGINE_MTF":                {"label": "Moteur TOP-DOWN multi-unites de temps pour Spot-Accum / Accumulation (1/0)", "default": 1},
    "MTF_USE_DAILY":                   {"label": "Top-down - unite majeure Daily (1) ou H4 (0)", "default": 0},
    "MTF_USE_H1":                      {"label": "Top-down - unite du signal H1 (1) ou M15 (0)", "default": 0},
    "MTF_RISK_PCT":                    {"label": "Top-down - risque par trade (% du capital)", "default": 0.5},
    "MTF_MIN_RR":                      {"label": "Top-down - rapport gain/risque minimal", "default": 1.5},
    "MTF_MAX_NOTIONAL_USD":            {"label": "Top-down - notionnel maximal par trade ($)", "default": 30.0},
    "MTF_MAX_RISK_PCT":                {"label": "Top-down - distance maximale au SL (% du prix)", "default": 4.0},
    # v4.348 — profil d activite (voir _PROFILES)
    # v4.352 — profil Swing (tendance Daily, signal sur figures H4, entree a la cloture)
    # v4.355 — frais integres aux statistiques et a la performance
    "FEES_IN_STATS":               {"label": "Frais dans les statistiques : 1 = performance NETTE de frais (bilan, onglet P/L, performance par mode, win rate) ; 0 = brute. La base garde toujours le PnL brut", "default": 1},
    "FEE_ROUND_TRIP_PCT":          {"label": "Frais aller-retour estimes (% du notionnel) pour les trades sans frais reels. 0,09 = 2 x 0,045 % (ordres au marche)", "default": 0.09},
    # v4.361 — Swing Forex (sous-mode, PAPER uniquement)
    "FXSWING_ENABLED":          {"label": "Swing Forex - active (1) ou eteint (0). PAPER uniquement ; meme logique que le Swing crypto (tendance et zones Daily, signal H4)", "default": 0},
    "FXSWING_RISK_PCT":         {"label": "Swing Forex - perte au stop = N % du capital", "default": 0.5},
    "FXSWING_MAX_NOTIONAL_USD": {"label": "Swing Forex - notionnel maximal par trade ($, paper)", "default": 60.0},
    "FXSWING_LEVERAGE":         {"label": "Swing Forex - levier simule", "default": 3},
    "FXSWING_MAX_TRADES":       {"label": "Swing Forex - swings simultanes au plus", "default": 2},
    "FXSWING_MIN_RISK_PCT":     {"label": "Swing Forex - stop minimal (% du prix) : en dessous, frais et spread mangent le gain", "default": 0.25},
    "FXSWING_MAX_RISK_PCT":     {"label": "Swing Forex - stop maximal (% du prix)", "default": 2.0},
    "FXSWING_MIN_RR":           {"label": "Swing Forex - gain/risque brut minimal", "default": 2.0},
    "FXSWING_MIN_NET_RR":       {"label": "Swing Forex - gain/risque NET de frais et spread minimal", "default": 1.2},
    "FXSWING_SIGNAL_MAX_AGE_SEC": {"label": "Swing Forex - entree dans les N secondes qui suivent la cloture H4", "default": 1800},
    "FXSWING_MAX_SPREAD_PCT":   {"label": "Swing Forex - spread maximal a l entree (%)", "default": 0.05},
    "FXSWING_MAX_HOLD_DAYS":    {"label": "Swing Forex - duree maximale d une position (jours)", "default": 10},
    # v4.362 — Swing Support (crypto, PAPER uniquement)
    "SUPSW_ENABLED":            {"label": "Swing Support · ACTIVATION — 1 = module allumé, 0 = éteint (paper uniquement : retest de support ou balayage de liquidité en tendance Daily haussière)", "default": 0},
    "SUPSW_SL_PCT":             {"label": "Swing Support · STOP FIXE (si « selon le levier » = 0) — distance du stop en % du prix ; sert aussi de plafond quand « selon le levier » = 1", "default": 5.0},
    "SUPSW_TP_PCT":             {"label": "Swing Support · OBJECTIF FIXE (si « selon le levier » = 0) — distance de l'objectif en % du prix", "default": 2.0},
    "SUPSW_RISK_PCT":           {"label": "Swing Support · TAILLE — perte au stop = N % du capital (détermine la taille de la position)", "default": 1.0},
    "SUPSW_MAX_NOTIONAL_USD":   {"label": "Swing Support · TAILLE — notionnel maximal par trade en $ (taille × levier, paper)", "default": 60.0},
    "SUPSW_MAX_TRADES":         {"label": "Swing Support · LIMITE — nombre maximal de positions Swing Support ouvertes en même temps", "default": 3},
    "SUPSW_WINDOW":             {"label": "Swing Support · SIGNAL — nombre de bougies 1 h examinées pour détecter le retest ou le balayage", "default": 5},
    "SUPSW_SIGNAL_MAX_AGE_SEC": {"label": "Swing Support · SIGNAL — délai d'entrée maximal en secondes après la clôture de la bougie 1 h du signal", "default": 900},
    "SUPSW_TRAIL_ON":           {"label": "Swing Support · LAISSER COURIR — 1 = à l'objectif on ne ferme pas : le stop remonte pour verrouiller un gain puis suit le plus haut ; 0 = sortie fixe à l'objectif", "default": 1},
    "SUPSW_TRAIL_LOCK_PCT":     {"label": "Swing Support · LAISSER COURIR — gain verrouillé dès l'objectif atteint (stop remonté à +N % au-dessus de l'entrée)", "default": 0.5},
    "SUPSW_TRAIL_PCT":          {"label": "Swing Support · LAISSER COURIR — le stop suit le plus haut atteint à N % en dessous", "default": 0.5},
    "MTF_PCT_LOCK_ON":          {"label": "Top-down (Spot-Accum, Accumulation, Swing) · VERROU DE GAIN EN % — 1 = dès que le gain atteint le seuil ci-dessous, le stop garantit une part du pic (indépendant du R)", "default": 1},
    "MTF_PCT_LOCK_ARM_PCT":     {"label": "Top-down · VERROU DE GAIN EN % — seuil d'armement : meilleur gain atteint, en % du prix", "default": 1.0},
    "MTF_PCT_LOCK_FRACTION":    {"label": "Top-down · VERROU DE GAIN EN % — part du pic garantie (0,6 = 60 %)", "default": 0.6},
    "MTF_PCT_LOCK_BIG_PCT":     {"label": "Top-down · VERROU DE GAIN EN % — au-delà de ce pic (% du prix), la part garantie est plus élevée", "default": 3.0},
    "MTF_PCT_LOCK_BIG_FRACTION": {"label": "Top-down · VERROU DE GAIN EN % — part du pic garantie au-delà du pic ci-dessus", "default": 0.75},
    "MTF_PCT_LOCK_FLOOR_PCT":   {"label": "Top-down · VERROU DE GAIN EN % — le stop est toujours au moins à +N % de l'entrée (frais couverts)", "default": 0.15},
    "SUPSW_BTC_GATE_ON":        {"label": "Swing Support · PORTE BTC — 1 = pas d'entrée quand BTC est en baisse (variation 24 h sous le minimum ci-dessous, ou tendance H4 non conforme au mode)", "default": 1},
    "SUPSW_BTC24_MIN":          {"label": "Swing Support · PORTE BTC — variation minimale de BTC sur 24 h en % pour accepter l'entrée (0 = BTC au moins stable)", "default": 0.0},
    "SUPSW_BTC_H4_MODE":        {"label": "Swing Support · PORTE BTC — tendance H4 de BTC : 0 = ignorée, 1 = refus si baissière, 2 = haussière exigée", "default": 2},
    "SUPSW_RECENT_ON":          {"label": "Swing Support · DERNIÈRES HEURES — 1 = pas d'entrée pendant une chute en cours (actif ou BTC)", "default": 1},
    "SUPSW_RECENT_HOURS":       {"label": "Swing Support · DERNIÈRES HEURES — fenêtre examinée en heures", "default": 4},
    "SUPSW_RECENT_ASSET_MIN_PCT": {"label": "Swing Support · DERNIÈRES HEURES — refus si l'actif a perdu plus de N % sur la fenêtre", "default": 1.5},
    "SUPSW_RECENT_BTC_MIN_PCT": {"label": "Swing Support · DERNIÈRES HEURES — refus si BTC a perdu plus de N % sur la fenêtre", "default": 0.5},
    "SUPSW_RES_FILTER_ON":      {"label": "Swing Support · RÉSISTANCE — 1 = pas d'entrée si une résistance Daily est trop proche au-dessus du prix (retournement probable)", "default": 1},
    "SUPSW_RES_MIN_PCT":        {"label": "Swing Support · RÉSISTANCE — distance minimale en % du prix entre le prix et le bas de la résistance Daily pour accepter l'entrée", "default": 3.0},
    "SUPSW_RIDE_ON":            {"label": "Swing Support · VISER LA RÉSISTANCE — 1 = l'objectif est la résistance Daily moins un tampon (au lieu de l'objectif fixe) ; le gain est verrouillé dès l'objectif fixe", "default": 1},
    "SUPSW_RES_BUFFER_PCT":     {"label": "Swing Support · VISER LA RÉSISTANCE — tampon sous la résistance en % du prix", "default": 0.3},
    "SUPSW_RIDE_MAX_PCT":       {"label": "Swing Support · VISER LA RÉSISTANCE — objectif maximal en % du prix", "default": 6.0},
    "SUPSW_RIDE_TRAIL_PCT":     {"label": "Swing Support · VISER LA RÉSISTANCE — distance du stop suiveur sous le plus haut pendant la montée (large, pour ne pas être sorti par le bruit)", "default": 1.0},
    "SUPSW_REENTRY_ON":         {"label": "Swing Support · RÉENTRÉE APRÈS GAIN — 1 = après un trade gagnant, on reprend si le prix continue de monter, sinon on attend la fin de la baisse près du support", "default": 1},
    "SUPSW_REENTRY_HOURS":      {"label": "Swing Support · RÉENTRÉE APRÈS GAIN — la règle s'applique pendant N heures après la sortie gagnante", "default": 12},
    "SUPSW_REENTRY_TOL_PCT":    {"label": "Swing Support · RÉENTRÉE APRÈS GAIN — le prix doit être au moins à (prix de sortie − N %) pour reprendre tout de suite", "default": 0.2},
    "SUPSW_MAX_HOLD_DAYS":      {"label": "Swing Support · DURÉE — durée maximale d'une position en jours", "default": 5},
    "SUPSW_EXIT_BUFFER_ATR":    {"label": "Swing Support · SORTIE ANTICIPÉE — tampon : support cassé = clôture 1 h sous le niveau moins N × ATR 1 h", "default": 0.3},
    "SUPSW_EXIT_FLOW":          {"label": "Swing Support · SORTIE ANTICIPÉE — flux de transactions : sortie si le flux est inférieur à N (en plus du support cassé)", "default": -0.4},
    "SUPSW_EXIT_LEVEL":         {"label": "Swing Support · SORTIE ANTICIPÉE — niveau surveillé : 1 = plus bas du retest (borné par les 2 réglages suivants), 0 = bas de la zone Daily", "default": 1},
    "SUPSW_EXIT_MIN_PCT":       {"label": "Swing Support · SORTIE ANTICIPÉE — le niveau surveillé est au moins à N % sous le prix d'entrée", "default": 0.5},
    "SUPSW_EXIT_MAX_PCT":       {"label": "Swing Support · SORTIE ANTICIPÉE — le niveau surveillé est au plus à N % sous le prix d'entrée (toujours maintenu au-dessus du stop)", "default": 3.0},
    "SUPSW_EXIT_REQUIRE_H4_END": {"label": "Swing Support · SORTIE ANTICIPÉE — 1 = exige aussi que la tendance H4 ne soit plus haussière pour sortir, 0 = seulement enregistrée", "default": 0},
    "SUPSW_LEVERAGE":           {"label": "Swing Support · LEVIER — levier utilisé par les trades Swing Support (paper, 1 à 10)", "default": 3},
    "SUPSW_LEVER_SCALE":        {"label": "Swing Support · STOP ET OBJECTIF SELON LE LEVIER — 1 = le stop et l'objectif sont calculés d'après le levier (réglages « SELON LE LEVIER » ci-dessous), 0 = stop et objectif fixes (réglages « FIXE »)", "default": 0},
    "SUPSW_RESERVE_ASSETS":     {"label": "Swing Support · ACTIFS RÉSERVÉS — 1 = les actifs de Swing Support (BTC, ETH, HYPE, TAO, SUI) sont ignorés par Tendance, Accumulation et Spot-Accum pour ne pas se bloquer entre modes, 0 = tous les modes se partagent les actifs", "default": 1},
    "SUPSW_MARGIN_RISK_PCT":    {"label": "Swing Support · SELON LE LEVIER — perte visée au stop en % de la marge ; stop en % du prix = N ÷ levier (plafonné au stop fixe, plancher ci-dessous)", "default": 7.5},
    "SUPSW_MIN_SL_PCT":         {"label": "Swing Support · SELON LE LEVIER — distance minimale du stop en % du prix (évite un stop trop serré aux leviers élevés)", "default": 1.5},
    "SUPSW_TP_LEV1":            {"label": "Swing Support · SELON LE LEVIER — objectif en % du prix quand le levier est ×1", "default": 2.0},
    "SUPSW_TP_LEV2":            {"label": "Swing Support · SELON LE LEVIER — objectif en % du prix quand le levier est ×2", "default": 1.5},
    "SUPSW_TP_LEV3":            {"label": "Swing Support · SELON LE LEVIER — objectif en % du prix quand le levier est ×3 ou plus", "default": 1.0},
    "TRENDF_ENABLED":           {"label": "Tendance - actif (1) ou eteint (0). PAPER uniquement ; entree quand Daily ET H4 confirment, on reste tant que la tendance tient", "default": 0},
    "TRENDF_ALLOW_SHORT":       {"label": "Tendance - autoriser les SHORT quand la tendance confirmee est baissiere (1 = oui)", "default": 1},
    "TRENDF_RISK_PCT":          {"label": "Tendance - perte au stop initial = N % du capital (dimensionne la taille)", "default": 1.0},
    "TRENDF_MAX_NOTIONAL_USD":  {"label": "Tendance - notionnel maximal par trade ($, paper)", "default": 60.0},
    "TRENDF_LEVERAGE":          {"label": "Tendance - levier (paper)", "default": 3},
    "TRENDF_MAX_TRADES":        {"label": "Tendance - positions simultanees au plus", "default": 3},
    "TRENDF_STOP_ATR":          {"label": "Tendance - distance du stop suiveur (multiple de l ATR H4)", "default": 3.0},
    "TRENDF_MIN_STOP_PCT":      {"label": "Tendance - distance minimale du stop (% du prix)", "default": 0.8},
    "TRENDF_MAX_STOP_PCT":      {"label": "Tendance - distance maximale du stop (% du prix)", "default": 6.0},
    "TRENDF_ENTRY_MODE":        {"label": "Tendance - 0 = entree des que la tendance est confirmee (momentum 1 h) ; 1 = exige en plus une cassure du plus haut/bas des N dernieres heures", "default": 0},
    "TRENDF_BREAK_BARS":        {"label": "Tendance - mode cassure : nombre de bougies 1 h de reference", "default": 12},
    "TRENDF_MIN_FLOW":          {"label": "Tendance - flux de transactions minimal dans le sens du trade (0 a 1)", "default": 0.2},
    "TRENDF_MAX_EXT_ATR":       {"label": "Tendance - refuse si le prix est a plus de N ATR Daily de sa moyenne (0 = pas de limite, mesure seulement)", "default": 0.0},
    "TRENDF_MAX_SPREAD_PCT":    {"label": "Tendance - spread maximal (%)", "default": 0.05},
    "TRENDF_MAX_HOLD_DAYS":     {"label": "Tendance - duree maximale d une position (jours)", "default": 30},
    "TRENDF_COOLDOWN_SEC":      {"label": "Tendance - pause sur un actif apres une sortie (secondes)", "default": 14400},
    "TRENDF_EXIT_H4_REVERSE":   {"label": "Tendance - sortir aussi quand la tendance H4 se retourne (1 = oui ; 0 = seulement Daily perdue / stop)", "default": 1},
    # v4.353 — scalp Forex (sous-mode du Forex, PAPER uniquement)
    "FOREX_SCALP_ENABLED":         {"label": "Scalp Forex - active (1) ou eteint (0). PAPER uniquement ; le mode Forex n est pas modifie", "default": 0},
    "FOREX_SCALP_NOTIONAL_USD":    {"label": "Scalp Forex - notionnel par trade ($, paper)", "default": 30.0},
    "FOREX_SCALP_LEVERAGE":        {"label": "Scalp Forex - levier simule", "default": 3},
    "FOREX_SCALP_MAX_TRADES":      {"label": "Scalp Forex - scalps simultanes au plus", "default": 2},
    "FOREX_SCALP_MAX_HOLD_MIN":    {"label": "Scalp Forex - sortie au temps (minutes)", "default": 20},
    "FOREX_SCALP_COOLDOWN_SEC":    {"label": "Scalp Forex - pause sur un actif apres une perte (secondes)", "default": 300},
    "FOREX_SCALP_DAILY_LOSS_USD":  {"label": "Scalp Forex - perte paper journaliere au-dela de laquelle le scalp s arrete ($)", "default": 1.0},
    "FOREX_SCALP_FEE_PCT":         {"label": "Scalp Forex - frais aller-retour supposes (% du notionnel ; 0,089 = ordres au marche, ~0,03 = ordres limite)", "default": 0.089},
    "FOREX_SCALP_SLIPPAGE_PCT":    {"label": "Scalp Forex - glissement defavorable par jambe (% du prix, en plus du demi-spread)", "default": 0.005},
    "FOREX_SCALP_MIN_NET_RR":      {"label": "Scalp Forex - gain/risque NET de frais et spread minimal pour prendre un setup", "default": 0.3},
    "FOREX_SCALP_MIN_TARGET_PCT":  {"label": "Scalp Forex - objectif minimal (% du prix)", "default": 0.15},
    "FOREX_SCALP_MIN_RISK_PCT":    {"label": "Scalp Forex - stop minimal (% du prix)", "default": 0.06},
    "FOREX_SCALP_MAX_RISK_PCT":    {"label": "Scalp Forex - stop maximal (% du prix)", "default": 0.30},
    "FOREX_SCALP_MAX_SIGNAL_AGE_SEC": {"label": "Scalp Forex - le rejet 5 min doit dater de moins de N secondes", "default": 150},
    "FOREX_SCALP_REQUIRE_1M":      {"label": "Scalp Forex - exiger la confirmation de la bougie 1 min (1/0)", "default": 1},
    "FUNDING_MAX_ANNUAL_PCT":      {"label": "Funding · PLAFOND D'ENTREE - pas d'entree si le funding annualise depasse ce niveau (%, 0 = sans plafond)", "default": 40.0},
    "FUNDING_EXCLUDE_WEAK_COINS":  {"label": "Funding · ACTIFS EXCLUS - ne plus ouvrir de Funding sur GMX et RENDER, resultats negatifs (1 = oui, 0 = non)", "default": 1},
    "FUNDING_STALE_ENABLED":       {"label": "Funding · SANS PROGRES - sortir un trade qui n'a jamais pris de gain apres un delai (1 = oui, 0 = non)", "default": 1},
    "FUNDING_STALE_MIN":           {"label": "Funding · SANS PROGRES - delai avant de verifier (minutes)", "default": 20},
    "FUNDING_STALE_PEAK_PCT":      {"label": "Funding · SANS PROGRES - sort si le pic de gain est reste sous ce seuil (% du prix)", "default": 0.15},
    "FOREX_SCALP_STALE_ENABLED":   {"label": "Scalp Forex · SANS PROGRES - sortir sans attendre le temps max si le trade n'a jamais pris de gain (1 = oui, 0 = non)", "default": 1},
    "FOREX_SCALP_STALE_MIN":       {"label": "Scalp Forex · SANS PROGRES - delai avant de verifier (minutes)", "default": 10},
    "FOREX_SCALP_STALE_PEAK_PCT":  {"label": "Scalp Forex · SANS PROGRES - sort si le pic de gain est reste sous ce seuil (% du prix)", "default": 0.05},
    "FOREX_SCALP_EARLYFAIL_ENABLED": {"label": "Scalp Forex · ECHEC PRECOCE - couper avant le stop complet un trade qui n'a jamais pris de gain (1 = oui, 0 = non)", "default": 1},
    "FOREX_SCALP_EARLYFAIL_RISK_FRAC": {"label": "Scalp Forex · ECHEC PRECOCE - sort quand la perte atteint cette fraction du stop (0,5 = la moitie)", "default": 0.5},
    "FOREX_SCALP_EARLYFAIL_PEAK_PCT": {"label": "Scalp Forex · ECHEC PRECOCE - uniquement si le pic de gain est reste sous ce seuil (% du prix)", "default": 0.03},
    "MTF_ZONE_MAX_WIDTH_ATR":          {"label": "Top-down - largeur maximale d une zone support/resistance (en ATR de l unite majeure ; 0 = sans limite). Evite les zones de 40-50 % du prix sur les longues consolidations ; 1,5 conseille en Swing", "default": 0.0},
    "MTF_USE_H4_LOWER":                {"label": "Top-down - signal sur bougies H4 (1) ou selon M15/H1 (0). Profil Swing : 1 avec la tendance Daily", "default": 0},
    "MTF_SIGNAL_MAX_AGE_SEC":          {"label": "Top-down - entree seulement dans les N secondes qui suivent la cloture de la bougie du signal (0 = sans limite)", "default": 0},
    "MTF_DOUBLE_PATTERN_ENABLED":      {"label": "Top-down - ajouter double creux / double sommet aux motifs de retournement (1/0)", "default": 0},
    "MTF_TREND_PATH_ENABLED":          {"label": "Top-down - chemin d entree 'tendance' par mesure de mouvement (1/0)", "default": 1},
    "MTF_REQUIRE_FLOW_CONFIRM":        {"label": "Top-down - exiger que le flux de transactions ne soit pas hostile (1/0)", "default": 1},
    "MTF_MIN_FLOW_PRESSURE":           {"label": "Top-down - seuil du flux de transactions (de -1 a +1, mesure sur 3 min). 0 = le flux doit simplement ne pas etre hostile ; -0,4 = refus seulement si le flux est NETTEMENT contraire (veto) ; +0,2 = flux favorable exige", "default": 0.0},
    "MTF_REQUIRE_REAL_TARGET_ZONE":    {"label": "Top-down - exiger un objectif reel, une zone opposee distincte (1 = oui, 0 = entrer sans objectif fixe : sortie par breakeven, verrou et suiveur)", "default": 1},
    "MTF_REQUIRE_H1_STRUCTURE":        {"label": "Top-down - exiger la structure H1 (EMA20 H1 qui repart ou prix repasse au-dessus / en dessous) (1/0)", "default": 1},
    "SPOT_ACCUM_TREND_MODE":           {"label": "Spot-Accum - tendance H4 exigee a l entree (0 = stricte ou repli, 1 = tant que la structure n est pas opposee, 2 = aucune)", "default": 0},
    "ACCUMULATION_TREND_MODE":         {"label": "Accumulation - tendance H4 exigee a l entree (0 = stricte ou repli, 1 = tant que la structure n est pas opposee, 2 = aucune)", "default": 0},
    "MTF_ZONE_TOLERANCE_ATR":          {"label": "Top-down - marge autour d une zone (x ATR majeur)", "default": 0.25},
    "MTF_ZONE_LOOKBACK":               {"label": "Top-down - bougies majeures examinees pour les zones", "default": 120},
    "ENTRY_ENGINE_SIMPLE":             {"label": "Moteur d entree simple Spot-Accum / Accumulation (1) ou ancienne chaine de conditions (0)", "default": 1},
    "SIMPLE_ENGINE_DYNAMIC_LEVERAGE":  {"label": "Moteur simple - levier dynamique 2-5x sur les entrees pres d un niveau (1/0)", "default": 0},
    "FUNDING_MODE_LIVE_ALLOWED":       {"label": "Funding - AUTORISER LE LIVE (verrou de securite : 0 = toujours simule en paper)", "default": 0},
    "LIVE_MIN_NOTIONAL_BUMP":          {"label": "Live - relever les trades sous 10 $ au minimum Hyperliquid (1/0)", "default": 1},
    "SITUATION_ALLOW_COUNTERTREND":    {"label": "Autoriser les trades a contre-tendance de fond (1 = oui, 0 = non)", "default": 0},
    "CONTINUATION_ENABLED":            {"label": "Voie continuation (entree sans proximite d un niveau) (1/0)", "default": 1},
    "CONTINUATION_PAPER_ONLY":         {"label": "Voie continuation en paper seulement, meme si le mode est live (1/0)", "default": 1},
    "CONTINUATION_MIN_FLOW":           {"label": "Continuation - flux minimal (2 lectures)", "default": 0.3},
    "CONTINUATION_MIN_ACTIVITY":       {"label": "Continuation - activite minimale (x habitude)", "default": 1.0},
    "CONTINUATION_MAX_EXTENSION_ATR":  {"label": "Continuation - ecart maximal a l EMA (en ATR 5 min)", "default": 1.5},
    "CONTINUATION_MIN_ROOM_X_SL":      {"label": "Continuation - marge minimale avant le niveau oppose (x le SL)", "default": 2.0},
    "SITUATION_REVERSAL_MIN_FLOW":     {"label": "Fin de repli / fin de rebond - flux minimal dans le sens du trade", "default": 0.2},
    "SITUATION_COUNTERTREND_MIN_FLOW": {"label": "Contre-tendance - flux minimal dans le sens du trade", "default": 0.3},
    "SITUATION_MIN_ROOM_PCT":          {"label": "Contre-tendance - marge minimale avant le niveau 1h oppose (%)", "default": 1.0},
    "COUNTERTREND_TTP_MULT":           {"label": "Contre-tendance - facteur du trailing (0,6 = arme et repli a 60 %)", "default": 0.6},
    "ANTI_RANGE_RELATIVE_ENABLED": {"label": "Anti-range relatif a l actif (1) ou seuils absolus (0)", "default": 1},
    "ANTI_RANGE_REL_MULT":         {"label": "Anti-range relatif - part de l amplitude habituelle exigee", "default": 0.6},
    "MARKET_QUALITY_MIN_VOL_RATIO":      {"label": "Qualite - volatilite minimale (x habitude, 0 = off)", "default": 0.0},
    "MARKET_QUALITY_MIN_ACTIVITY_RATIO": {"label": "Qualite - activite minimale (x habitude, 0 = off)", "default": 0.0},
    "SPOT_ACCUM_MIN_FLOW_CONVICTION":    {"label": "Spot-Accum - conviction minimale du flux |pression| (0 = off)", "default": 0.0},
    "ACCUMULATION_MIN_FLOW_CONVICTION":  {"label": "Accumulation - conviction minimale du flux |pression| (0 = off)", "default": 0.0},
    "FUNDING_MIN_FLOW_CONVICTION":       {"label": "Funding - conviction minimale du flux |pression| (0 = off)", "default": 0.0},
    "FOREX_MIN_FLOW_CONVICTION":         {"label": "Forex - conviction minimale du flux |pression| (0 = off)", "default": 0.0},
    "MARKET_QUALITY_MAX_SPREAD_PCT":     {"label": "Qualite - spread maximal a l entree (% du prix, 0 = off)", "default": 0.0},
    "SPOT_ACCUM_BYPASS_REQUIRE_TREND": {"label": "Spot-Accum - cassures/rebonds exigent la tendance EMA200 (1/0)", "default": 1},
    "SPOT_ACCUM_RISING_SUPPORT_ENABLED": {"label": "Spot-Accum - achat sur repli (support ascendant) (1/0)", "default": 1},
    "SPOT_ACCUM_FRESH_BREAKOUT_COUNTER_TREND":   {"label": "Spot-Accum - cassure fraiche autorisee contre l EMA de tendance (1/0)", "default": 0},
    "ACCUMULATION_FRESH_BREAKOUT_COUNTER_TREND": {"label": "Accumulation - cassure fraiche autorisee contre l EMA de tendance (1/0)", "default": 0},
    "FRESH_BREAKOUT_COUNTER_TREND_MIN_FLOW":     {"label": "Cassure fraiche contre-tendance - flux minimal exige", "default": 0.2},
    "SPOT_ACCUM_PIVOT_CANDLES": {"label": "Spot-Accum - creux : bougies 1h de chaque cote", "default": 2},
    "SPOT_ACCUM_BYPASS_FLOW_VETO":     {"label": "Spot-Accum - cassures/rebonds soumis au veto du flux (1/0)", "default": 1},
    "ACCUMULATION_BYPASS_REQUIRE_TREND": {"label": "Accumulation - cassures/rejets exigent la tendance EMA200 (1/0)", "default": 1},
    "ACCUMULATION_BYPASS_FLOW_VETO":     {"label": "Accumulation - cassures/rejets soumis au veto du flux (1/0)", "default": 1},
    "SPOT_ACCUM_TRADE_HOUR_START_UTC":   {"label": "Spot-Accum - debut plage horaire (h UTC)", "default": 0},
    "SPOT_ACCUM_TRADE_HOUR_END_UTC":     {"label": "Spot-Accum - fin plage horaire (h UTC, 24 = toujours)", "default": 24},
    "ACCUMULATION_TRADE_HOUR_START_UTC": {"label": "Accumulation - debut plage horaire (h UTC)", "default": 0},
    "ACCUMULATION_TRADE_HOUR_END_UTC":   {"label": "Accumulation - fin plage horaire (h UTC, 24 = toujours)", "default": 24},
    "FUNDING_TRADE_HOUR_START_UTC":      {"label": "Funding - debut plage horaire (h UTC)", "default": 0},
    "FUNDING_TRADE_HOUR_END_UTC":        {"label": "Funding - fin plage horaire (h UTC, 24 = toujours)", "default": 24},
    # v4.269 — reglages par mode (vide = herite du reglage general)
    "SPOT_ACCUM_SL_CAP_PCT":     {"label": "Spot-Accum - SL plafond (% de prix, vide = 0,5)", "default": None},
    "SPOT_ACCUM_SL_FLOW_THRESHOLD": {"label": "Spot-Accum - patience SL : pression acheteuse minimale", "default": 0.2},
    "SPOT_ACCUM_SL_FLOW_MAX_PCT":   {"label": "Spot-Accum - patience SL : perte maximale toleree (%)", "default": 1.0},
    "SPOT_ACCUM_SL_FLOW_MAX_WAIT_SEC": {"label": "Spot-Accum - patience SL : duree maximale (s, 0 = desactivee)", "default": 900},
    "SPOT_ACCUM_SL_PATIENCE_REQUIRE_TREND": {"label": "Spot-Accum - patience SL : exiger la tendance EMA200 (1 = oui, 0 = non)", "default": 1},
    "ACCUMULATION_SL_CAP_PCT":   {"label": "Accumulation - SL plafond (% de prix, vide = 0,5)", "default": None},
    "SPOT_ACCUM_ENTRY_FLOW_CONFIRM_MIN": {"label": "Spot-Accum - confirmation flux a l entree (vide = reglage general)", "default": None},
    "ACCUMULATION_ENTRY_FLOW_CONFIRM_MIN": {"label": "Accumulation - confirmation flux a l entree (vide = reglage general)", "default": None},
    "ACCUMULATION_LOSS_COOLDOWN_SEC": {"label": "Accumulation - delai apres perte sur un actif (s, 0 = aucun)", "default": 3600},
    "SPOT_ACCUM_LOSS_COOLDOWN_SEC":   {"label": "Spot-Accum - delai apres perte sur un actif (s, 0 = aucun)", "default": 0},
    "FUNDING_LOSS_COOLDOWN_SEC":      {"label": "Funding - delai apres perte sur un actif (s, 0 = aucun)", "default": 0},
    "ACCUMULATION_MAX_ENTRIES_PER_WINDOW": {"label": "Accumulation - entrees max par fenetre (0 = illimite)", "default": 3},
    "SPOT_ACCUM_MAX_ENTRIES_PER_WINDOW":   {"label": "Spot-Accum - entrees max par fenetre (0 = illimite)", "default": 0},
    "ENTRY_BURST_WINDOW_SEC":    {"label": "Fenetre de comptage des entrees (s)", "default": 600},
    "FUNDING_TTP_TOLERANCE_PCT": {"label": "Funding - TTP repli normal (%)", "default": 0.5},
    "FUNDING_TTP_FLOW_MAX_TOLERANCE_PCT": {"label": "Funding - TTP repli si flux favorable (patience, %)", "default": 0.9},
    "FUNDING_TTP_FLOW_FAST_TOLERANCE_PCT": {"label": "Funding - TTP repli si flux contraire (%)", "default": 0.25},
    "FUNDING_TTP_FLOW_THRESHOLD": {"label": "Funding - TTP seuil de flux favorable/contraire", "default": 0.2},
    "FUNDING_TTP_MIN_LOCK_PCT":  {"label": "Funding - TTP plancher de gain une fois arme (%)", "default": 0.3},
    "RSI_OVERSOLD":            {"label": "RSI - Seuil survente",              "default": 32},
    "RSI_OVERBOUGHT":          {"label": "RSI - Seuil surachat",              "default": 68},
    "RSI_EXTREME_LOW":         {"label": "RSI - Zone survente extreme (no SHORT sous)",  "default": 15},
    "RSI_EXTREME_HIGH":        {"label": "RSI - Zone surachat extreme (no LONG au-dessus)", "default": 85},
    "EMA_SHORT":               {"label": "EMA courte - periode",              "default": 12},
    "EMA_LONG":                {"label": "EMA longue - periode",              "default": 26},
    "EMA_MID_PERIOD":          {"label": "EMA intermediaire - periode (cycles, ~33min a 10s/cycle)", "default": 200},
    "MACD_FAST":               {"label": "MACD - rapide",                    "default": 12},
    "MACD_SLOW":               {"label": "MACD - lent",                      "default": 26},
    "MACD_SIGNAL":             {"label": "MACD - signal",                    "default": 9},
    "BB_PERIOD":               {"label": "Bollinger - periode",              "default": 20},
    "BB_STD":                  {"label": "Bollinger - ecart-type",           "default": 2.0},
    "ATR_PERIOD":              {"label": "ATR - periode (cycles)",           "default": 21},
    "ATR_MIN_PCT":             {"label": "ATR - seuil min % (filtre marche calme)", "default": 0.015},
    "ADX_PERIOD":              {"label": "ADX - periode",                    "default": 14},
    "ADX_TREND_THRESHOLD":     {"label": "ADX - seuil Trend/Reversal",       "default": 25.0},
    "ACCUMULATION_ADX_TREND_THRESHOLD": {"label": "Accumulation - seuil ADX dedie (distinct du mode normal)", "default": 20.0},
    "ACCUMULATION_MIN_ABOVE_SUPPORT_PCT": {"label": "Accumulation - minimum fenetre proximite (% de l'amplitude, dedie)", "default": 5.0},
    "ACCUMULATION_MAX_ABOVE_SUPPORT_PCT": {"label": "Accumulation - maximum fenetre proximite (% de l'amplitude, dedie)", "default": 15.0},
    "ACCUMULATION_ANTI_RANGE_MIN_PCT": {"label": "Accumulation - mouvement minimum requis pour eviter le range (%)", "default": 2.0},
    "ACCUMULATION_MIN_AMPLITUDE_TO_ATR_RATIO": {"label": "Accumulation - amplitude S/R minimale, en multiple de l'ATR", "default": 3.0},
    "ACCUMULATION_ANTI_RANGE_LOOKBACK": {"label": "Accumulation - echantillons pour le detecteur de range (200 = 6h40, aligne sur EMA200)", "default": 200},
    "SPOT_ACCUM_ANTI_RANGE_MIN_PCT": {"label": "Spot-Accum - mouvement minimum requis pour eviter le range (%)", "default": 2.0},
    "SPOT_ACCUM_ANTI_RANGE_LOOKBACK": {"label": "Spot-Accum - echantillons pour le detecteur de range (200 = 6h40, aligne sur EMA200)", "default": 200},
    "SPOT_ACCUM_TREND_STABILITY_CYCLES":   {"label": "Spot-Accum - stabilité tendance requise avant entrée (cycles ~10s)", "default": 24},
    "FOREX_TREND_STABILITY_CYCLES":       {"label": "Forex - stabilité tendance requise avant entrée (cycles ~10s)", "default": 12},
    "FOREX_ANTI_RANGE_MIN_PCT":  {"label": "Forex - mouvement minimal pour ne pas etre en range (%)", "default": 0.25},
    "FOREX_TTP_ARM_PCT":         {"label": "Forex - armement du trailing (% de mouvement de PRIX)", "default": 0.15},
    "FOREX_DYNAMIC_SL":          {"label": "Forex - stop loss dynamique (1 = oui, 0 = non)", "default": 1},
    "FOREX_DSL_FEE_PAD_PCT":     {"label": "Forex - SL dynamique : plancher apres armement = entree + frais (%)", "default": 0.10},
    "FOREX_STALE_ENABLED":       {"label": "Forex · SANS PROGRES - sortir un trade qui n'a jamais pris de gain apres un delai (1 = oui, 0 = non)", "default": 1},
    "FOREX_STALE_MIN":           {"label": "Forex · SANS PROGRES - delai avant de verifier (minutes)", "default": 90},
    "MANUAL_DEFAULT_SWING":      {"label": "Trading manuel - ouvrir par defaut avec les reglages du mode Swing Support (1 = oui, 0 = reglages libres)", "default": 1},
    "ASSET_BT_DAYS":             {"label": "Resume d actif - profondeur de l historique rejoue pour choisir la pre-programmation (jours, 30 a 180)", "default": 90},
    "FOREX_STALE_PEAK_PCT":      {"label": "Forex · SANS PROGRES - sort si le pic de gain est reste sous ce seuil (% du prix)", "default": 0.15},
    "FOREX_DSL_TRAIL_FRACTION":  {"label": "Forex - SL dynamique : distance du suiveur (fraction du gain maximal)", "default": 0.40},
    "FOREX_TTP_FLOW_REVERSAL_MIN_PEAK_PCT": {"label": "Forex - pic minimal pour sortie sur retournement de flux (%)", "default": 0.12},
    "FOREX_ENTRY_MIN_PCT":       {"label": "Forex - entree : minimum (% de l'amplitude S/R)", "default": 5.0},
    "FOREX_ENTRY_MAX_PCT":       {"label": "Forex - entree : maximum (% de l'amplitude S/R)", "default": 30.0},
    "FOREX_TREND_HOLD_FILTER_ENABLED": {"label": "Forex - garder la position tant que la tendance tient (1 = oui, 0 = non)", "default": 0},
    "FOREX_LONG_TERM_MOMENTUM_MIN_CHANGE_PCT": {"label": "Forex - momentum long terme minimal (%)", "default": 0.4},
    "ACCUMULATION_TREND_STABILITY_CYCLES": {"label": "Accumulation - stabilité tendance requise avant entrée (cycles ~10s)", "default": 24},
    "SR_PERIOD":               {"label": "Support/Resistance - periode (cycles, repli seulement)", "default": 50},
    "SR_PERIOD_CANDLES":       {"label": "Support/Resistance - periode (bougies ~2min, ex: 100=~3h20)", "default": 100},
    "SL_PCT_OF_E":                {"label": "Stop Loss (% de E)", "default": 1.0},
    "EXCHANGE_SAFETY_SL_MULT":    {"label": "SL Hyperliquid - multiple du Stop Loss bot", "default": 2.0},
    "TTP_ARM1_PRICE_PCT":          {"label": "TTP - 1er seuil d'armement (% de mouvement de prix)", "default": 1.0},
    "TTP_LOCK1_PRICE_PCT":         {"label": "TTP - seuil de sortie initial (% de mouvement de prix)", "default": 0.8},
    "TTP_ARM2_PRICE_PCT":          {"label": "TTP - 2e seuil, active le trailing continu (% de mouvement de prix)", "default": 1.3},
    "TTP_TRAIL_GAP_PRICE_PCT":     {"label": "TTP - marge de repli continue sous le pic (% de mouvement de prix)", "default": 0.3},
    "TTP_DYNAMIC_TRAIL_GAP_PCT":   {"label": "TTP - marge du trailing dynamique dès l'armement (%)", "default": 0.5},
    "ACCUMULATION_MAX_TRADES":     {"label": "Accumulation - trades simultanes max", "default": 3},
    "ACCUMULATION_PROXIMITY_PCT":  {"label": "Accumulation - proximite support/resistance (%)", "default": 1.0},
    "SL_ATR_MULTIPLIER":       {"label": "SL/TTP adaptatif - multiplicateur ATR", "default": 2.0},
    "SL_PCT_MIN":              {"label": "SL/TTP adaptatif - plancher de securite (%)", "default": 0.3},
    "SL_MIN_PRICE_MOVE_PCT":   {"label": "SL - mouvement de prix minimum garanti, compense par le levier (%)", "default": 0.3},
    "TTP_MIN_ARM_PCT_FLOOR":   {"label": "TTP - plancher strict d'armement, anti trades microscopiques (%)", "default": 0.3},
    "TTP_MAX_GIVEBACK_PCT_OF_PEAK": {"label": "TTP - plafond de redonnage maximum du pic, même tendance intacte (%)", "default": 20.0},
    "TTP_MAX_GIVEBACK_MIN_PEAK_PCT": {"label": "TTP - pic minimum requis avant que le plafond de redonnage s'applique (%)", "default": 1.0},
    "TTP_SMALL_PEAK_GIVEBACK_PCT": {"label": "TTP - plafond de redonnage pour petits pics, si repli soutenu (%)", "default": 50.0},
    "TTP_SMALL_PEAK_GIVEBACK_MIN_CYCLES": {"label": "TTP - cycles consecutifs de repli requis avant fermeture (petits pics)", "default": 5},
    "TTP_MAX_GIVEBACK_PCT_SMALL_PEAK": {"label": "TTP - plafond de redonnage pour petits pics, sous le seuil ci-dessus (%)", "default": 50.0},
    "TIER0_ARM_RATIO_OF_SL":   {"label": "TTP - ratio fixe tier0 armement / SL", "default": 0.5},
    "TIER0_GAP_RATIO_OF_SL":   {"label": "TTP - ratio fixe tier0 marge de repli / SL", "default": 0.42},
    "TIER0_REARM_HYSTERESIS_PCT": {"label": "TTP - marge d'hysteresis anti-oscillation tier1->tier0 (%)", "default": 15.0},
    "SL_PCT_MAX":              {"label": "SL/TTP adaptatif - plafond de securite (%)", "default": 3.0},
    "FUNDING_ANNUAL_THRESHOLD_PCT": {"label": "Funding Contrarian - seuil annualise (%)", "default": 25.0},
    "FUNDING_MODE_MAX_TRADES":      {"label": "Funding Contrarian - trades simultanes max", "default": 3},
    "FUNDING_REFRESH_SEC":          {"label": "Funding Contrarian - frequence de rafraichissement (s)", "default": 300},
    "SR_EMA200_PROXIMITY_PCT":      {"label": "Separation S/R vs EMA200 - proximite (%)", "default": 0.5},
    "UNIFIED_MIN_ABOVE_SUPPORT_PCT":   {"label": "Base commune - minimum au-dessus du niveau (% de l'amplitude S/R)", "default": 5.0},
    "UNIFIED_MAX_ABOVE_SUPPORT_PCT":   {"label": "Base commune - maximum au-dessus du niveau (% de l'amplitude S/R)", "default": 10.0},
    "UNIFIED_MIN_SR_AMPLITUDE_PCT":    {"label": "Base commune - amplitude minimale fourchette S/R (%)", "default": 4.0},
    "SPOT_ACCUM_MAX_TRADES":            {"label": "Spot-Accum - trades simultanes max", "default": 3},
    "SPOT_ACCUM_MIN_ABOVE_SUPPORT_PCT": {"label": "Spot-Accum - minimum au-dessus du support (% de l'amplitude S/R)", "default": 5.0},
    "SPOT_ACCUM_MIN_SR_AMPLITUDE_PCT": {"label": "Spot-Accum - amplitude minimale fourchette S/R (%)", "default": 2.0},
    "SPOT_ACCUM_MAX_ABOVE_SUPPORT_PCT": {"label": "Spot-Accum - maximum au-dessus du support (% de l'amplitude S/R)", "default": 10.0},
    "SPOT_ACCUM_TTP_ARM_PCT":           {"label": "Spot-Accum - TTP armement (% de PnL)", "default": 1.0},
    "SPOT_ACCUM_TTP_TOLERANCE_PCT":     {"label": "Spot-Accum - TTP marge de repli depuis le pic (%)", "default": 0.5},
    "SPOT_ACCUM_TARGET_SR_PCT":         {"label": "Spot-Accum - objectif (% distance support-resistance)", "default": 80.0},
    "SPOT_ACCUM_TRAILING_ARM_SR_PCT":   {"label": "Spot-Accum - armement trailing (% distance support-résistance)", "default": 70.0},
    "SPOT_ACCUM_SL_PCT_OF_PNL":         {"label": "Spot-Accum - SL simple (% du PnL)", "default": 1.5},
    "SPOT_ACCUM_HARD_SL_PCT":           {"label": "Spot-Accum - plafond dur (% du PnL, ferme sans condition)", "default": 5.0},
    "SPOT_ACCUM_REVERSAL_CONFIRM_CYCLES":    {"label": "Spot-Accum - retournement confirmé (cycles, ~10s chacun)", "default": 180},
    "SPOT_ACCUM_REVERSAL_MIN_EMA_MATURITY":  {"label": "Spot-Accum - maturité EMA200 minimale (bougies)", "default": 100},
    "ACCUMULATION_REVERSAL_CONFIRM_CYCLES":    {"label": "Accumulation - retournement confirmé (cycles, ~10s chacun)", "default": 180},
    "ACCUMULATION_REVERSAL_MIN_EMA_MATURITY":  {"label": "Accumulation - maturité EMA200 minimale (bougies)", "default": 100},
    "VOLUME_MIN_RATIO":        {"label": "Volume - ratio minimum vs moyenne","default": 1.2},
    "MOMENTUM_PERIOD":         {"label": "Momentum - periode (cycles)",      "default": 4},
    "MOMENTUM_THRESHOLD_PCT":  {"label": "Momentum - seuil %",               "default": 0.20},
    "CONFIDENCE_MIN_PCT":      {"label": "Confiance - seuil minimum %",      "default": 65.0},
    "CONFIDENCE_STEP_PCT":     {"label": "Confiance - pas d ajustement %",   "default": 5.0},
    "CONFIDENCE_MAX_PCT":      {"label": "Confiance - plafond dynamique %",  "default": 87.0},
    "CONFIDENCE_RESET_HOURS":  {"label": "Confiance - decroissance auto apres (h)", "default": 2.0},
    "AUTO_ACTIVATE_CONFIDENCE_PCT": {"label": "Auto-activation - seuil %",   "default": 80.0},
    "CRYPTO_OFFPEAK_HOUR_START_UTC": {"label": "Heures creuses - debut (UTC)", "default": 21},
    "CRYPTO_OFFPEAK_HOUR_END_UTC":   {"label": "Heures creuses - fin (UTC)",   "default": 23},
    "CPI_BLACKOUT_BEFORE_MIN": {"label": "Blackout CPI - minutes avant",     "default": 15},
    "CPI_BLACKOUT_AFTER_MIN":  {"label": "Blackout CPI - minutes apres",     "default": 30},

    # v4.42 — SUR DEMANDE EXPLICITE : jeu de seuils SL/TTP DEDIE au mode
    # Accumulation (LONG et SHORT confondus). default=None : Accumulation
    # continue de retomber sur les reglages du mode normal tant que rien
    # n est explicitement defini ici — ne rien toucher = aucun changement.
    "ACCUMULATION_SL_PCT_OF_E":             {"label": "Accumulation - SL (% de E)", "default": None},
    "ACCUMULATION_TTP_ARM1_PRICE_PCT":      {"label": "Accumulation - TTP armement (% de prix)", "default": None},
    "ACCUMULATION_TTP_LOCK1_PRICE_PCT":     {"label": "Accumulation - TTP verrou initial (% de prix)", "default": None},
    "ACCUMULATION_TTP_ARM2_PRICE_PCT":      {"label": "Accumulation - TTP palier 2 (% de prix)", "default": None},
    "ACCUMULATION_TTP_TRAIL_GAP_PRICE_PCT": {"label": "Accumulation - TTP marge de repli (% de prix)", "default": None},
    "ACCUMULATION_SL_ATR_MULTIPLIER":       {"label": "Accumulation - multiplicateur ATR (mode adaptatif)", "default": None},
    "FUNDING_SL_PCT_OF_E":             {"label": "Funding - SL (% de E)", "default": None},
    "FUNDING_TTP_ARM1_PRICE_PCT":      {"label": "Funding - TTP armement (% de prix)", "default": None},
    "FUNDING_TTP_LOCK1_PRICE_PCT":     {"label": "Funding - TTP verrou initial (% de prix)", "default": None},
    "FUNDING_TTP_ARM2_PRICE_PCT":      {"label": "Funding - TTP palier 2 (% de prix)", "default": None},
    "FUNDING_TTP_TRAIL_GAP_PRICE_PCT": {"label": "Funding - TTP marge de repli (% de prix)", "default": None},
    "ACCUMULATION_TTP_DYNAMIC_TRAIL_GAP_PCT": {"label": "Accumulation - marge du trailing dynamique dès l'armement (%)", "default": None},
    "FUNDING_TTP_DYNAMIC_TRAIL_GAP_PCT": {"label": "Funding - marge du trailing dynamique dès l'armement (%)", "default": None},
    "FUNDING_SL_ATR_MULTIPLIER":       {"label": "Funding - multiplicateur ATR (mode adaptatif)", "default": None},
}


@app.get("/api/config/advanced")
def get_advanced_config(email: str = Depends(require_user)):
    return {
        key: {"value": cfg.get(key, meta["default"]), "label": meta["label"], "default": meta["default"]}
        for key, meta in ADVANCED_SETTINGS.items()
    }


# v4.264 — FIX BUG CRITIQUE : le corps de requete type les valeurs en float,
# donc RSI_PERIOD=14 devenait 14.0 — et `prices[-(period + 1):]` leve alors
# TypeError (slice indices must be integers) : modifier une periode depuis
# l interface cassait l analyse de TOUS les actifs. Les reglages entiers
# (periodes, cycles, compteurs, heures) sont desormais convertis en int et
# bornes ; None n est accepte que pour les reglages "herite" (defaut None).
_RSI_FLOAT_THRESHOLDS = {"RSI_OVERSOLD", "RSI_OVERBOUGHT", "RSI_EXTREME_LOW", "RSI_EXTREME_HIGH"}
_ZERO_ALLOWED_INT_KEYS = {"FUNDING_EXCLUDE_WEAK_COINS","FUNDING_STALE_ENABLED","FOREX_STALE_ENABLED","MANUAL_DEFAULT_SWING","FOREX_SCALP_STALE_ENABLED","FOREX_SCALP_EARLYFAIL_ENABLED","FXSWING_ENABLED","SUPSW_LEVER_SCALE","SUPSW_RESERVE_ASSETS","TRENDF_ENABLED","TRENDF_ALLOW_SHORT","TRENDF_ENTRY_MODE","TRENDF_EXIT_H4_REVERSE","TRENDF_COOLDOWN_SEC","SUPSW_ENABLED","SUPSW_EXIT_REQUIRE_H4_END","SUPSW_EXIT_LEVEL","FEES_IN_STATS","FOREX_SCALP_ENABLED", "FOREX_SCALP_REQUIRE_1M", "FOREX_SCALP_COOLDOWN_SEC","MTF_USE_H4_LOWER", "MTF_SIGNAL_MAX_AGE_SEC", "MTF_DOUBLE_PATTERN_ENABLED", "MTF_TREND_PATH_ENABLED", "MTF_REQUIRE_FLOW_CONFIRM", "MTF_REQUIRE_REAL_TARGET_ZONE", "MTF_REQUIRE_H1_STRUCTURE", "SPOT_ACCUM_TREND_MODE", "ACCUMULATION_TREND_MODE",
                          "CRYPTO_OFFPEAK_HOUR_START_UTC", "CRYPTO_OFFPEAK_HOUR_END_UTC",
                          "CPI_BLACKOUT_BEFORE_MIN", "CPI_BLACKOUT_AFTER_MIN",
                          "ACCUMULATION_LOSS_COOLDOWN_SEC", "SPOT_ACCUM_LOSS_COOLDOWN_SEC", "FUNDING_LOSS_COOLDOWN_SEC",
                          "ACCUMULATION_MAX_ENTRIES_PER_WINDOW", "SPOT_ACCUM_MAX_ENTRIES_PER_WINDOW",
                          "SPOT_ACCUM_SL_FLOW_MAX_WAIT_SEC", "SPOT_ACCUM_SL_PATIENCE_REQUIRE_TREND",
                          "MARKET_REGIME_FILTER_ENABLED", "SPOT_ACCUM_TRADE_HOUR_START_UTC", "ANTI_RANGE_RELATIVE_ENABLED", "SITUATION_RULES_ENABLED", "SITUATION_ALLOW_COUNTERTREND", "CONTINUATION_ENABLED", "CONTINUATION_PAPER_ONLY", "ENTRY_ENGINE_SIMPLE", "SIMPLE_ENGINE_DYNAMIC_LEVERAGE", "FUNDING_MODE_LIVE_ALLOWED", "LIVE_MIN_NOTIONAL_BUMP",
                          "ENTRY_ENGINE_MTF", "MTF_USE_DAILY", "MTF_USE_H1",
                          "SPOT_ACCUM_BYPASS_REQUIRE_TREND", "SPOT_ACCUM_BYPASS_FLOW_VETO", "SPOT_ACCUM_RISING_SUPPORT_ENABLED",
                          "SPOT_ACCUM_FRESH_BREAKOUT_COUNTER_TREND", "ACCUMULATION_FRESH_BREAKOUT_COUNTER_TREND",
                          "ACCUMULATION_BYPASS_REQUIRE_TREND", "ACCUMULATION_BYPASS_FLOW_VETO", "ACCUMULATION_TRADE_HOUR_START_UTC",
                          "FUNDING_TRADE_HOUR_START_UTC", "SPOT_ACCUM_TRADE_HOUR_END_UTC", "ACCUMULATION_TRADE_HOUR_END_UTC",
                          "FUNDING_TRADE_HOUR_END_UTC"}


def _is_int_setting(key: str) -> bool:
    default = ADVANCED_SETTINGS.get(key, {}).get("default")
    return isinstance(default, int) and not isinstance(default, bool) and key not in _RSI_FLOAT_THRESHOLDS


def _coerce_advanced_value(key: str, value):
    """Retourne (True, valeur_propre) ou (False, raison)."""
    default = ADVANCED_SETTINGS[key]["default"]
    if value is None:
        return (True, None) if default is None else (False, "valeur vide refusee pour ce reglage")
    if value != value or value in (float("inf"), float("-inf")):
        return False, "valeur invalide"
    if (key.startswith("ENTRY_FLOW_") or key.endswith("_FLOW_THRESHOLD") or key.endswith("_ENTRY_FLOW_CONFIRM_MIN") or key == "FRESH_BREAKOUT_COUNTER_TREND_MIN_FLOW") and not 0 <= value <= 1:
        return False, "doit etre entre 0 et 1 (pression de -1 a +1)"
    if key.endswith("_MIN_FLOW_CONVICTION") and not 0 <= value <= 1:
        return False, "entre 0 et 1"
    if key in ("SITUATION_RULES_ENABLED", "SITUATION_ALLOW_COUNTERTREND", "CONTINUATION_ENABLED", "CONTINUATION_PAPER_ONLY", "ENTRY_ENGINE_SIMPLE", "SIMPLE_ENGINE_DYNAMIC_LEVERAGE", "FUNDING_MODE_LIVE_ALLOWED", "LIVE_MIN_NOTIONAL_BUMP", "CONTINUATION_ENABLED", "CONTINUATION_PAPER_ONLY", "ENTRY_ENGINE_MTF", "MTF_USE_DAILY", "MTF_USE_H1") and value not in (0, 1):
        return False, "1 (oui) ou 0 (non)"
    if key in ("SITUATION_REVERSAL_MIN_FLOW", "SITUATION_COUNTERTREND_MIN_FLOW") and not 0 <= value <= 1:
        return False, "entre 0 et 1"
    if key == "COUNTERTREND_TTP_MULT" and not 0.2 <= value <= 1:
        return False, "entre 0,2 et 1"
    if key == "MARKET_QUALITY_MAX_SPREAD_PCT" and not 0 <= value <= 5:
        return False, "entre 0 et 5 %"
    if key in ("MARKET_QUALITY_MIN_VOL_RATIO", "MARKET_QUALITY_MIN_ACTIVITY_RATIO") and not 0 <= value <= 5:
        return False, "entre 0 et 5"
    if key == "ANTI_RANGE_REL_MULT" and not 0.1 <= value <= 3:
        return False, "entre 0,1 et 3"
    if (key == "MARKET_REGIME_FILTER_ENABLED" or key == "ANTI_RANGE_RELATIVE_ENABLED" or "_BYPASS_" in key or key == "SPOT_ACCUM_RISING_SUPPORT_ENABLED" or key.endswith("_FRESH_BREAKOUT_COUNTER_TREND")) and value not in (0, 1):
        return False, "1 (oui) ou 0 (non)"
    if key.endswith("_TRADE_HOUR_START_UTC") and not 0 <= value <= 23:
        return False, "heure entre 0 et 23"
    if key.endswith("_TRADE_HOUR_END_UTC") and not 0 <= value <= 24:
        return False, "heure entre 0 et 24"
    if key == "MARKET_REGIME_BREADTH_PCT" and not 50 <= value <= 100:
        return False, "entre 50 et 100 %"
    if key == "SPOT_ACCUM_SL_PATIENCE_REQUIRE_TREND" and value not in (0, 1):
        return False, "1 (oui) ou 0 (non)"
    if key in ("MTF_REQUIRE_REAL_TARGET_ZONE", "MTF_REQUIRE_H1_STRUCTURE", "MTF_USE_H4_LOWER", "MTF_DOUBLE_PATTERN_ENABLED",
               "MTF_TREND_PATH_ENABLED", "MTF_REQUIRE_FLOW_CONFIRM") and value not in (0, 1):
        return False, "1 (oui) ou 0 (non)"
    if key == "FEES_IN_STATS" and value not in (0, 1):
        return False, "1 (frais inclus) ou 0 (brut)"
    if key == "FEE_ROUND_TRIP_PCT" and not 0 <= value <= 1:
        return False, "entre 0 et 1 % du notionnel"
    if key == "FXSWING_ENABLED" and value not in (0, 1):
        return False, "1 (actif) ou 0 (eteint)"
    if key == "FXSWING_RISK_PCT" and not 0.05 <= value <= 3:
        return False, "entre 0,05 et 3 % du capital"
    if key == "FXSWING_MAX_NOTIONAL_USD" and not 5 <= value <= 300:
        return False, "entre 5 $ et 300 $"
    if key == "FXSWING_LEVERAGE" and not 1 <= value <= 10:
        return False, "entre 1 et 10"
    if key == "FXSWING_MAX_TRADES" and not 1 <= value <= 5:
        return False, "entre 1 et 5"
    if key in ("FXSWING_MIN_RISK_PCT", "FXSWING_MAX_RISK_PCT", "FXSWING_MAX_SPREAD_PCT") and not 0.01 <= value <= 10:
        return False, "entre 0,01 et 10 %"
    if key in ("FXSWING_MIN_RR", "FXSWING_MIN_NET_RR") and not 0.5 <= value <= 10:
        return False, "entre 0,5 et 10"
    if key == "FXSWING_MAX_HOLD_DAYS" and not 1 <= value <= 60:
        return False, "entre 1 et 60 jours"
    if key in ("SUPSW_EXIT_MIN_PCT", "SUPSW_EXIT_MAX_PCT") and not 0.1 <= value <= 10:
        return False, "entre 0,1 et 10 %"
    if key == "SUPSW_LEVERAGE" and not 1 <= value <= 10:
        return False, "entre 1 et 10"
    if key in ("SUPSW_LEVER_SCALE", "SUPSW_RESERVE_ASSETS") and value not in (0, 1):
        return False, "1 (oui) ou 0 (non)"
    if key == "SUPSW_MARGIN_RISK_PCT" and not 1 <= value <= 30:
        return False, "entre 1 et 30 % de la marge"
    if key == "SUPSW_MIN_SL_PCT" and not 0.3 <= value <= 10:
        return False, "entre 0,3 et 10 %"
    if key in ("SUPSW_TP_LEV1", "SUPSW_TP_LEV2", "SUPSW_TP_LEV3",
               "SUPSW_TRAIL_ON", "SUPSW_TRAIL_LOCK_PCT", "SUPSW_TRAIL_PCT", "SUPSW_REENTRY_ON", "SUPSW_REENTRY_HOURS", "SUPSW_REENTRY_TOL_PCT",
               "SUPSW_BTC_GATE_ON", "SUPSW_BTC24_MIN", "SUPSW_BTC_H4_MODE", "SUPSW_RECENT_ON", "SUPSW_RECENT_HOURS", "SUPSW_RECENT_ASSET_MIN_PCT", "SUPSW_RECENT_BTC_MIN_PCT",
               "SUPSW_RES_FILTER_ON", "SUPSW_RES_MIN_PCT", "SUPSW_RIDE_ON", "SUPSW_RES_BUFFER_PCT", "SUPSW_RIDE_MAX_PCT", "SUPSW_RIDE_TRAIL_PCT") and not 0.3 <= value <= 15:
        return False, "entre 0,3 et 15 %"
    if key in ("SUPSW_ENABLED", "SUPSW_EXIT_REQUIRE_H4_END", "SUPSW_EXIT_LEVEL") and value not in (0, 1):
        return False, "1 (oui) ou 0 (non)"
    if key == "SUPSW_SL_PCT" and not 0.5 <= value <= 15:
        return False, "entre 0,5 et 15 %"
    if key == "SUPSW_TP_PCT" and not 0.3 <= value <= 15:
        return False, "entre 0,3 et 15 %"
    if key == "SUPSW_RISK_PCT" and not 0.05 <= value <= 3:
        return False, "entre 0,05 et 3 % du capital"
    if key == "SUPSW_MAX_NOTIONAL_USD" and not 5 <= value <= 300:
        return False, "entre 5 $ et 300 $"
    if key == "SUPSW_MAX_TRADES" and not 1 <= value <= 5:
        return False, "entre 1 et 5"
    if key == "SUPSW_WINDOW" and not 3 <= value <= 12:
        return False, "entre 3 et 12 bougies"
    if key == "SUPSW_SIGNAL_MAX_AGE_SEC" and not 60 <= value <= 3600:
        return False, "entre 60 et 3600 secondes"
    if key == "SUPSW_MAX_HOLD_DAYS" and not 1 <= value <= 30:
        return False, "entre 1 et 30 jours"
    if key == "SUPSW_EXIT_BUFFER_ATR" and not 0 <= value <= 3:
        return False, "entre 0 et 3"
    if key == "SUPSW_EXIT_FLOW" and not -1 <= value <= 1:
        return False, "entre -1 et 1"
    if key in ("TRENDF_ENABLED", "TRENDF_ALLOW_SHORT", "TRENDF_ENTRY_MODE", "TRENDF_EXIT_H4_REVERSE") and value not in (0, 1):
        return False, "1 (oui) ou 0 (non)"
    if key == "TRENDF_RISK_PCT" and not 0.05 <= value <= 3:
        return False, "entre 0,05 et 3 % du capital"
    if key == "TRENDF_MAX_NOTIONAL_USD" and not 5 <= value <= 300:
        return False, "entre 5 $ et 300 $"
    if key == "TRENDF_LEVERAGE" and not 1 <= value <= 10:
        return False, "entre 1 et 10"
    if key == "TRENDF_MAX_TRADES" and not 1 <= value <= 8:
        return False, "entre 1 et 8"
    if key == "TRENDF_STOP_ATR" and not 0.5 <= value <= 10:
        return False, "entre 0,5 et 10 ATR"
    if key in ("TRENDF_MIN_STOP_PCT", "TRENDF_MAX_STOP_PCT") and not 0.1 <= value <= 20:
        return False, "entre 0,1 et 20 %"
    if key == "TRENDF_BREAK_BARS" and not 3 <= value <= 48:
        return False, "entre 3 et 48 bougies"
    if key == "TRENDF_MIN_FLOW" and not 0 <= value <= 1:
        return False, "entre 0 et 1"
    if key == "TRENDF_MAX_EXT_ATR" and not 0 <= value <= 20:
        return False, "entre 0 et 20 (0 = pas de limite)"
    if key == "TRENDF_MAX_SPREAD_PCT" and not 0.005 <= value <= 1:
        return False, "entre 0,005 et 1 %"
    if key == "TRENDF_MAX_HOLD_DAYS" and not 1 <= value <= 120:
        return False, "entre 1 et 120 jours"
    if key == "TRENDF_COOLDOWN_SEC" and not 0 <= value <= 172800:
        return False, "entre 0 et 172800 secondes"
    if key in ("FOREX_SCALP_ENABLED", "FOREX_SCALP_REQUIRE_1M", "FOREX_SCALP_STALE_ENABLED", "FOREX_SCALP_EARLYFAIL_ENABLED") and value not in (0, 1):
        return False, "1 (oui) ou 0 (non)"
    if key in ("FUNDING_EXCLUDE_WEAK_COINS", "FUNDING_STALE_ENABLED", "FOREX_STALE_ENABLED") and value not in (0, 1):
        return False, "1 (oui) ou 0 (non)"
    if key == "FUNDING_MAX_ANNUAL_PCT" and not 0 <= value <= 1000:
        return False, "entre 0 (sans plafond) et 1000 %"
    if key == "MANUAL_DEFAULT_SWING" and value not in (0, 1):
        return False, "0 ou 1"
    if key == "ASSET_BT_DAYS" and not 30 <= value <= 180:
        return False, "entre 30 et 180 jours"
    if key == "FOREX_STALE_MIN" and not 10 <= value <= 720:
        return False, "entre 10 et 720 minutes"
    if key == "FOREX_STALE_PEAK_PCT" and not 0 <= value <= 1:
        return False, "entre 0 et 1 % du prix"
    if key == "FUNDING_STALE_MIN" and not 5 <= value <= 240:
        return False, "entre 5 et 240 minutes"
    if key == "FUNDING_STALE_PEAK_PCT" and not 0 <= value <= 1:
        return False, "entre 0 et 1 % du prix"
    if key == "FOREX_SCALP_STALE_MIN" and not 2 <= value <= 60:
        return False, "entre 2 et 60 minutes"
    if key in ("FOREX_SCALP_STALE_PEAK_PCT", "FOREX_SCALP_EARLYFAIL_PEAK_PCT") and not 0 <= value <= 1:
        return False, "entre 0 et 1 % du prix"
    if key == "FOREX_SCALP_EARLYFAIL_RISK_FRAC" and not 0.1 <= value <= 1:
        return False, "entre 0,1 et 1"
    if key == "FOREX_SCALP_FEE_PCT" and not 0 <= value <= 1:
        return False, "entre 0 et 1 % du notionnel"
    if key == "FOREX_SCALP_NOTIONAL_USD" and not 5 <= value <= 200:
        return False, "entre 5 $ et 200 $"
    if key == "FOREX_SCALP_LEVERAGE" and not 1 <= value <= 10:
        return False, "entre 1 et 10"
    if key == "FOREX_SCALP_MAX_TRADES" and not 1 <= value <= 5:
        return False, "entre 1 et 5"
    if key == "MTF_ZONE_MAX_WIDTH_ATR" and not 0 <= value <= 10:
        return False, "entre 0 (sans limite) et 10 ATR"
    if key == "MTF_MIN_FLOW_PRESSURE" and not -1 <= value <= 1:
        return False, "entre -1 et +1"
    if key == "MTF_SIGNAL_MAX_AGE_SEC" and not 0 <= value <= 86400:
        return False, "entre 0 (sans limite) et 86 400 secondes"
    if key == "MTF_MAX_RISK_PCT" and not 0.5 <= value <= 20:
        return False, "entre 0,5 % et 20 %"
    if key.endswith("_TREND_MODE") and value not in (0, 1, 2):
        return False, "0, 1 ou 2"
    if key == "FUNDING_MAX_ACTIVITY_RATIO" and not 0 <= value <= 20:
        return False, "entre 0 (aucun plafond) et 20"
    if key == "FUNDING_MAX_SPREAD_PCT" and not 0 <= value <= 5:
        return False, "entre 0 (aucun plafond) et 5 %"
    if key == "MTF_MIN_RR" and not 0.5 <= value <= 5:
        return False, "entre 0,5 et 5"
    if key == "MTF_RISK_PCT" and not 0.05 <= value <= 2:
        return False, "entre 0,05 % et 2 % du capital"
    if key.endswith("_SL_CAP_PCT") and not 0.2 <= value <= 5:
        return False, "doit etre entre 0,2 et 5 %"
    if _is_int_setting(key):
        value = int(round(value))
        if key.endswith("_UTC") and not key.endswith("_END_UTC") and not 0 <= value <= 23:
            return False, "heure hors plage 0-23"
        if value < (0 if key in _ZERO_ALLOWED_INT_KEYS else 1):
            return False, "doit etre >= 1" if key not in _ZERO_ALLOWED_INT_KEYS else "doit etre >= 0"
    return True, value


def _normalize_loaded_advanced_settings():
    """Corrige les valeurs deja enregistrees en base (floats 14.0 ecrits par
    les versions precedentes) au chargement."""
    for key in ADVANCED_SETTINGS:
        if key in cfg and cfg[key] is not None and _is_int_setting(key):
            try:
                cfg[key] = int(round(float(cfg[key])))
            except (TypeError, ValueError):
                cfg[key] = ADVANCED_SETTINGS[key]["default"]


_normalize_loaded_advanced_settings()


class AdvancedConfigBody(BaseModel):
    # v4.42 — Optional[float] (pas juste float) : autorise l envoi explicite
    # de None pour REINITIALISER un reglage Accumulation dedie et revenir a
    # l heritage du mode normal (voir ACCUMULATION_SL_PCT_OF_E etc.).
    values: Dict[str, Optional[float]]


@app.put("/api/config/advanced")
def put_advanced_config(body: AdvancedConfigBody, email: str = Depends(require_user)):
    applied, ignored = {}, []
    for key, value in body.values.items():
        if key not in ADVANCED_SETTINGS:
            ignored.append(key)
            continue
        ok, clean = _coerce_advanced_value(key, value)
        if not ok:
            ignored.append(f"{key} ({clean})")
            continue
        _apply_and_persist(key, clean)
        applied[key] = clean
    return {"ok": True, "applied": applied, "ignored": ignored}


@app.get("/api/config")
def get_config(email: str = Depends(require_user)):
    return _public_config()


@app.put("/api/config")
def put_config(body: ConfigBody, email: str = Depends(require_user)):
    if body.trading_mode is not None:
        if bot.trading_enabled:
            raise HTTPException(400, "Arretez le bot avant de changer de mode (paper/live)")
        if body.trading_mode not in ("paper", "live"):
            raise HTTPException(400, "trading_mode doit etre 'paper' ou 'live'")
        _apply_and_persist("MODE", body.trading_mode)

    if body.position_pct is not None:
        _apply_and_persist("POSITION_SIZE_PCT", body.position_pct)

    # v4.10 — moteur ASYMETRIQUE : SL en % de E (perte $ plafonnee), TP en %
    # de mouvement de prix (gain $ amplifie par le levier). Les champs $
    # legacy (max_loss_usd, quick_profit_usd) restent acceptes pour compat
    # avec l interface actuelle : convertis a la volee via une estimation de
    # E a levier x1 (capital/POSITION_SIZE_PCT courants).
    e_estimate = cfg["CAPITAL_USD"] * cfg["POSITION_SIZE_PCT"] / 100

    if body.max_loss_usd is not None:
        if e_estimate > 0:
            _apply_and_persist("SL_PCT_OF_E", body.max_loss_usd / e_estimate * 100)
        else:
            raise HTTPException(400, "Capital/position_pct invalides pour convertir max_loss_usd en %")

    if body.quick_profit_usd is not None:
        if e_estimate > 0:
            _apply_and_persist("TTP_ARM1_PRICE_PCT", body.quick_profit_usd / e_estimate * 100)
        else:
            raise HTTPException(400, "Capital/position_pct invalides pour convertir quick_profit_usd en %")

    if body.sl_pct_of_e is not None:
        _apply_and_persist("SL_PCT_OF_E", body.sl_pct_of_e)

    if body.ttp_arm1_price_pct is not None:
        _apply_and_persist("TTP_ARM1_PRICE_PCT", body.ttp_arm1_price_pct)

    if body.ttp_lock1_price_pct is not None:
        _apply_and_persist("TTP_LOCK1_PRICE_PCT", body.ttp_lock1_price_pct)

    if body.ttp_arm2_price_pct is not None:
        _apply_and_persist("TTP_ARM2_PRICE_PCT", body.ttp_arm2_price_pct)

    if body.ttp_trail_gap_price_pct is not None:
        _apply_and_persist("TTP_TRAIL_GAP_PRICE_PCT", body.ttp_trail_gap_price_pct)

    if body.max_open_trades is not None:
        clamped = max(1, min(body.max_open_trades, len(SUPPORTED_TICKERS)))
        _apply_and_persist("MAX_OPEN_TRADES", clamped)

    if body.auto_activate_confidence_pct is not None:
        clamped_conf = max(50.0, min(body.auto_activate_confidence_pct, 100.0))
        _apply_and_persist("AUTO_ACTIVATE_CONFIDENCE_PCT", clamped_conf)

    # v3.2 — FIX : ignore une chaine vide plutot que d ecraser un wallet deja
    # enregistre — un formulaire n envoyant pas de wallet ne doit jamais
    # pouvoir effacer celui deja configure (defense en profondeur, en plus
    # du fix cote interface qui ne l envoie plus vide).
    if body.wallet:
        _apply_and_persist("WALLET_ADDRESS", be._clean_hex_secret(body.wallet))

    if body.api_key and not body.api_key.startswith("****"):
        _apply_and_persist("PRIVATE_KEY", be._clean_hex_secret(body.api_key))

    if body.active_coins is not None:
        valid = [c for c in body.active_coins if c in SUPPORTED_TICKERS]
        ignored = [c for c in body.active_coins if c not in SUPPORTED_TICKERS]
        old_active = set(cfg.get("ACTIVE_COINS") or SUPPORTED_TICKERS)
        new_active = set(valid)
        removed = old_active - new_active  # actifs que l utilisateur vient de desactiver
        added    = new_active - old_active  # actifs que l utilisateur vient de reactiver
        # v4.3 — une desactivation manuelle depuis l onglet Marches est une
        # exclusion EXPLICITE : elle doit tenir meme si la confiance de cet
        # actif remonte tres haut ensuite (voir _gate_active_or_auto_activate
        # dans bot_engine.py). Une reactivation manuelle leve cette exclusion.
        manual_exclude = set(cfg.get("MANUAL_EXCLUDE_COINS", []))
        manual_exclude |= removed
        manual_exclude -= added
        _apply_and_persist("ACTIVE_COINS", valid)
        _apply_and_persist("MANUAL_EXCLUDE_COINS", sorted(manual_exclude))
        if removed:
            _push_log("warn", f"Actifs desactives manuellement (ne seront plus jamais auto-reactives) : {', '.join(sorted(removed))}")
        if ignored:
            _push_log("warn", f"Actifs ignores (non supportes par ce bot) : {', '.join(ignored)}")

    return _public_config()


class HyperliquidBody(BaseModel):
    # v3.2 — FIX : l interface (index.html) envoie ces champs sous les noms
    # "hl_wallet" / "hl_api_key" a cet endpoint precis (un autre formulaire,
    # sur /api/config, utilise "wallet"/"api_key" — les deux sont donc geres
    # ici par securite). Le mismatch precedent faisait que la requete
    # "reussissait" (200 OK) sans rien enregistrer reellement.
    wallet: Optional[str] = None
    api_key: Optional[str] = None
    hl_wallet: Optional[str] = None
    hl_api_key: Optional[str] = None


@app.put("/api/config/hyperliquid")
def put_hyperliquid(body: HyperliquidBody, email: str = Depends(require_user)):
    wallet = body.wallet or body.hl_wallet
    api_key = body.api_key or body.hl_api_key
    if wallet:
        _apply_and_persist("WALLET_ADDRESS", be._clean_hex_secret(wallet))
    if api_key and not api_key.startswith("****"):
        _apply_and_persist("PRIVATE_KEY", be._clean_hex_secret(api_key))
    return {"ok": True, "note": "Prend effet au prochain demarrage du bot (arret puis demarrage)."}


class FinnhubBody(BaseModel):
    finnhub_key: str


@app.put("/api/config/finnhub")
def put_finnhub(body: FinnhubBody, email: str = Depends(require_user)):
    _apply_and_persist("FINNHUB_API_KEY", body.finnhub_key)
    return {"ok": True}


MODE_ACTIVE_COINS_KEY = {
    "accumulation": "ACCUMULATION_ACTIVE_COINS",
    "funding_contrarian": "FUNDING_ACTIVE_COINS",
    "spot_accumulation": "SPOT_ACCUM_ACTIVE_COINS",
}


class ModeCoinBody(BaseModel):
    mode: str
    ticker: str
    active: bool


@app.put("/api/config/mode-coins")
def put_mode_coins(body: ModeCoinBody, email: str = Depends(require_user)):
    """v4.44 — SUR DEMANDE EXPLICITE : bascule un actif ON/OFF pour un mode
    PRECIS (Accumulation/Funding/Spot-Accum), en un clic, independamment de
    la liste globale (Marches). Au premier reglage pour un mode, initialise
    sa liste dediee a partir de la liste globale ACTIVE_COINS actuelle
    (pour ne pas desactiver silencieusement tout le reste d un coup)."""
    key = MODE_ACTIVE_COINS_KEY.get(body.mode)
    if not key:
        raise HTTPException(400, f"Mode inconnu : {body.mode}")
    current = cfg.get(key)
    if current is None:
        current = list(cfg.get("ACTIVE_COINS") or [])
    else:
        current = list(current)
    ticker = _norm_ticker(body.ticker)
    if body.active and ticker not in current:
        current.append(ticker)
    elif not body.active and ticker in current:
        current.remove(ticker)
    _apply_and_persist(key, current)
    return {"ok": True, "mode": body.mode, "active_coins": current}


class StrategyModeBody(BaseModel):
    strategy: str  # "forex" | "accumulation" | "funding_contrarian" | "spot_accumulation"
    value: Optional[str] = None  # "paper" | "live" | None (suit le mode global)


@app.put("/api/config/strategy-mode")
def put_strategy_mode(body: StrategyModeBody, email: str = Depends(require_user)):
    """v4.87 — SUR DEMANDE EXPLICITE : bascule un mode precis entre paper et
    live, independamment du mode global du bot et des 3 autres modes.
    value=None retire la personnalisation (retombe sur le mode global)."""
    valid_strategies = ("forex", "accumulation", "funding_contrarian", "spot_accumulation")
    if body.strategy not in valid_strategies:
        raise HTTPException(400, f"Mode inconnu : {body.strategy}")
    if body.value is not None and body.value not in ("paper", "live"):
        raise HTTPException(400, "value doit etre 'paper', 'live', ou absent")
    current = cfg.get("STRATEGY_MODE_OVERRIDE") or {}
    current = dict(current)
    current[body.strategy] = body.value
    _apply_and_persist("STRATEGY_MODE_OVERRIDE", current)
    return {"ok": True, "strategy": body.strategy, "value": body.value}


class StrategyTradingEnabledBody(BaseModel):
    strategy: str  # "forex" | "accumulation" | "funding_contrarian" | "spot_accumulation"
    enabled: bool


@app.put("/api/config/strategy-trading-enabled")
def put_strategy_trading_enabled(body: StrategyTradingEnabledBody, email: str = Depends(require_user)):
    """v4.106 — SUR DEMANDE EXPLICITE : Marche/Arret INDEPENDANT par mode —
    enabled=False bloque UNIQUEMENT l ouverture de nouveaux trades pour ce
    mode precis ; les positions deja ouvertes de ce mode continuent d etre
    gerees normalement (SL/TTP/retournement) jusqu a leur fermeture."""
    valid_strategies = ("forex", "accumulation", "funding_contrarian", "spot_accumulation")
    if body.strategy not in valid_strategies:
        raise HTTPException(400, f"Mode inconnu : {body.strategy}")
    current = cfg.get("STRATEGY_TRADING_ENABLED") or {}
    current = dict(current)
    current[body.strategy] = body.enabled
    _apply_and_persist("STRATEGY_TRADING_ENABLED", current)
    _push_log("ok" if body.enabled else "warn", f"{'▶️' if body.enabled else '⏸️'} Mode {body.strategy} {'redemarre' if body.enabled else 'arrete'} — {'nouveaux trades autorises' if body.enabled else 'nouveaux trades bloques, positions ouvertes gerees normalement'}.")
    return {"ok": True, "strategy": body.strategy, "enabled": body.enabled}


class StrategyGoLiveBody(BaseModel):
    strategy: str  # "forex" | "accumulation" | "funding_contrarian" | "spot_accumulation"


@app.post("/api/config/strategy-go-live")
def post_strategy_go_live(body: StrategyGoLiveBody, email: str = Depends(require_user)):
    """v4.88 — SUR DEMANDE EXPLICITE : bascule un mode precis en LIVE, en
    une seule action combinee (le frontend demande deja confirmation avant
    d appeler cet endpoint, vu son caractere irreversible) :
      1) Ferme TOUTES les positions actuellement ouvertes pour ce mode
      2) Efface l historique (trades fermes) de ce mode uniquement
      3) Si des identifiants Hyperliquid sont configures, synchronise le
         capital reel du compte (remplace le capital local — attention,
         ce capital est PARTAGE entre tous les modes, meme ceux restes en
         paper, car le bot n a qu un seul pot de capital)
      4) Force enfin ce mode sur "live" via STRATEGY_MODE_OVERRIDE
    """
    valid_strategies = ("forex", "accumulation", "funding_contrarian", "spot_accumulation")
    if body.strategy not in valid_strategies:
        raise HTTPException(400, f"Mode inconnu : {body.strategy}")

    # v4.298 — FIX : la bascule en live ne ferme plus que les positions PAPER
    # de ce mode et N EFFACE PLUS AUCUN historique (les statistiques live
    # sont de toute facon calculees sur les seuls trades live). Avant, chaque
    # aller-retour paper/live effacait les trades REELS du mode, et leurs
    # pertes disparaissaient des bilans.
    closed_count = bot._close_all_trades_for_strategy(body.strategy, paper_only=True)
    deleted_count = 0
    if db.get_meta(f"live_since_{body.strategy}") is None:
        db.set_meta(f"live_since_{body.strategy}", db.now_iso())

    capital_synced = False
    new_capital = None
    if bot.info is not None and cfg.get("WALLET_ADDRESS"):
        real_balance = be.sync_capital_from_hyperliquid(bot.info, cfg["WALLET_ADDRESS"])
        if real_balance is not None and real_balance > 0:
            # v4.89 — FIX : alimente le pot LIVE separe, plus jamais le
            # capital paper partage — les deux ne se melangent plus.
            bot.live_capital_base = real_balance
            if db.get_meta("live_initial_balance") is None:  # v4.298 — capital de depart du live, fige une fois
                db.set_meta("live_initial_balance", str(real_balance))
            capital_synced = True
            new_capital = real_balance
            _push_log("ok", f"💰 Capital LIVE synchronise depuis Hyperliquid suite au passage de {body.strategy} en live : ${real_balance:.2f} (capital paper des autres modes inchange)")

    current = cfg.get("STRATEGY_MODE_OVERRIDE") or {}
    current = dict(current)
    current[body.strategy] = "live"
    _apply_and_persist("STRATEGY_MODE_OVERRIDE", current)

    _push_log("warn", f"🔴 Mode {body.strategy} bascule en LIVE — {closed_count} position(s) paper fermee(s), historique conserve.")

    return {
        "ok": True,
        "strategy": body.strategy,
        "closed_positions": closed_count,
        "deleted_history": deleted_count,
        "capital_synced": capital_synced,
        "new_capital": new_capital,
    }


@app.post("/api/config/strategy-clear-only")
def post_strategy_clear_only(body: StrategyGoLiveBody, email: str = Depends(require_user)):
    """v4.165 — SUR DEMANDE EXPLICITE : version SANS le passage en live de
    strategy-go-live — ferme toutes les positions ouvertes pour ce mode et
    efface son historique, mais NE TOUCHE PAS a son statut paper/live
    (contrairement a strategy-go-live, qui force le passage en live comme
    etape finale). Utile pour repartir sur une base propre (ex: mode
    Normal reoriente vers un nouvel univers d actifs) sans forcer d
    engagement en argent reel."""
    valid_strategies = ("forex", "accumulation", "funding_contrarian", "spot_accumulation")
    if body.strategy not in valid_strategies:
        raise HTTPException(400, f"Mode inconnu : {body.strategy}")

    closed_count = bot._close_all_trades_for_strategy(body.strategy)
    deleted_count = db.clear_trades_by_strategy(body.strategy, keep_live=True)  # v4.298 : jamais les trades live

    _push_log("info", f"🧹 Nettoyage {body.strategy} — {closed_count} position(s) fermee(s), {deleted_count} trade(s) PAPER efface(s) (historique live conserve). Statut paper/live inchange.")

    return {
        "ok": True,
        "strategy": body.strategy,
        "closed_positions": closed_count,
        "deleted_history": deleted_count,
    }


class ModeCoinResetBody(BaseModel):
    mode: str


@app.put("/api/config/mode-coins/reset")
def reset_mode_coins(body: ModeCoinResetBody, email: str = Depends(require_user)):
    """v4.52 — SUR DEMANDE EXPLICITE : reinitialise la liste d actifs d un
    mode PRECIS pour qu elle revienne a heriter de la liste globale
    (Marches) — corrige le cas ou un mode etait reste bloque sur une liste
    personnalisee figee (ex: 5 actifs), sans plus jamais suivre les
    changements faits dans Marches, faute d un moyen de revenir en arriere."""
    key = MODE_ACTIVE_COINS_KEY.get(body.mode)
    if not key:
        raise HTTPException(400, f"Mode inconnu : {body.mode}")
    _apply_and_persist(key, None)
    return {"ok": True, "mode": body.mode, "active_coins": None}


class FiltersBody(BaseModel):
    filter_hours: Optional[bool] = None
    filter_weekend: Optional[bool] = None
    filter_macro: Optional[bool] = None
    accumulation_enabled: Optional[bool] = None
    accumulation_require_trend_confirm: Optional[bool] = None
    sl_ttp_adaptive_enabled: Optional[bool] = None
    funding_mode_enabled: Optional[bool] = None
    funding_mode_live_allowed: Optional[bool] = None
    require_sr_ema200_separation: Optional[bool] = None
    unified_simplified_mode: Optional[bool] = None
    unified_full_simplified_mode: Optional[bool] = None
    unified_require_sr_amplitude: Optional[bool] = None
    ttp_trend_hold_filter_enabled: Optional[bool] = None
    unified_require_adx_confirm: Optional[bool] = None
    ttp_dynamic_from_arm1: Optional[bool] = None
    spot_accum_enabled: Optional[bool] = None
    spot_accum_sl_enabled: Optional[bool] = None
    spot_accum_require_adx_confirm: Optional[bool] = None
    accumulation_reversal_exit_enabled: Optional[bool] = None
    spot_accum_hard_sl_enabled: Optional[bool] = None


@app.put("/api/config/filters")
def put_filters(body: FiltersBody, email: str = Depends(require_user)):
    # filter_hours  -> heures creuses crypto (CRYPTO_OFFPEAK_ENABLED)
    # filter_weekend-> fermeture Forex sur PAXG (FOREX_SYMBOLS)
    # filter_macro  -> blackout CPI Finnhub (CPI_BLACKOUT_ENABLED)
    if body.filter_hours is not None:
        _apply_and_persist("CRYPTO_OFFPEAK_ENABLED", body.filter_hours)
    if body.filter_weekend is not None:
        _apply_and_persist("FOREX_SYMBOLS", ["PAXG"] if body.filter_weekend else [])
    if body.filter_macro is not None:
        _apply_and_persist("CPI_BLACKOUT_ENABLED", body.filter_macro)
    # v4.8 — mode Accumulation (LONG pres du support / SHORT pres de la
    # resistance, independant de la logique RSI/tendance normale)
    if body.accumulation_enabled is not None:
        _apply_and_persist("ACCUMULATION_ENABLED", body.accumulation_enabled)
    if body.accumulation_require_trend_confirm is not None:
        _apply_and_persist("ACCUMULATION_REQUIRE_TREND_CONFIRM", body.accumulation_require_trend_confirm)
    # v4.24 — SL/TTP adaptatifs a l ATR reel (unique/global : un seul
    # multiplicateur pour tous les actifs, mais le resultat differe par
    # actif car chacun a sa propre ATR au moment de l entree — voir
    # SL_ATR_MULTIPLIER/SL_PCT_MIN/SL_PCT_MAX dans Parametres avances).
    if body.sl_ttp_adaptive_enabled is not None:
        _apply_and_persist("SL_TTP_ADAPTIVE_ENABLED", body.sl_ttp_adaptive_enabled)
    # v4.33 — mode Funding Contrarian : source de signal differente
    # (desequilibre de position via le funding rate, pas RSI/MACD/EMA).
    # funding_mode_live_allowed reste False par defaut : meme active, ce
    # mode simule ses trades (paper) tant que ce deuxieme interrupteur n est
    # pas leve manuellement, quel que soit le mode global du bot — protege
    # un capital de trading deja fragilise pendant la phase de validation.
    if body.funding_mode_enabled is not None:
        _apply_and_persist("FUNDING_MODE_ENABLED", body.funding_mode_enabled)
    if body.funding_mode_live_allowed is not None:
        _apply_and_persist("FUNDING_MODE_LIVE_ALLOWED", body.funding_mode_live_allowed)
    # v4.37 — bloque un LONG/SHORT si le support/resistance est proche ET du
    # mauvais cote de l EMA200 (marche en range pur, sans separation nette
    # de sa moyenne longue) — DESACTIVE par defaut.
    if body.require_sr_ema200_separation is not None:
        _apply_and_persist("REQUIRE_SR_EMA200_SEPARATION", body.require_sr_ema200_separation)
    # v4.58 — mode SIMPLIFIE : 3 conditions communes (tendance+ADX,
    # proximite 1-5%, amplitude S/R) remplacent la complexite empilee sur le
    # mode normal, et s ajoutent a Accumulation. ACTIF par defaut.
    if body.unified_simplified_mode is not None:
        _apply_and_persist("UNIFIED_SIMPLIFIED_MODE", body.unified_simplified_mode)
    if body.unified_full_simplified_mode is not None:
        _apply_and_persist("UNIFIED_FULL_SIMPLIFIED_MODE", body.unified_full_simplified_mode)
    if body.unified_require_sr_amplitude is not None:
        _apply_and_persist("UNIFIED_REQUIRE_SR_AMPLITUDE", body.unified_require_sr_amplitude)
    if body.ttp_trend_hold_filter_enabled is not None:
        _apply_and_persist("TTP_TREND_HOLD_FILTER_ENABLED", body.ttp_trend_hold_filter_enabled)
    if body.unified_require_adx_confirm is not None:
        _apply_and_persist("UNIFIED_REQUIRE_ADX_CONFIRM", body.unified_require_adx_confirm)
    if body.ttp_dynamic_from_arm1 is not None:
        _apply_and_persist("TTP_DYNAMIC_FROM_ARM1", body.ttp_dynamic_from_arm1)
    # v4.43 — mode Spot-Accumulation (achat d actif esprit spot, aucun SL
    # par defaut, levier toujours x1). spot_accum_sl_enabled ajoute un SL
    # optionnel en % du PnL (voir SPOT_ACCUM_SL_PCT_OF_PNL dans Parametres avances).
    if body.spot_accum_enabled is not None:
        _apply_and_persist("SPOT_ACCUM_ENABLED", body.spot_accum_enabled)
    if body.spot_accum_sl_enabled is not None:
        _apply_and_persist("SPOT_ACCUM_SL_ENABLED", body.spot_accum_sl_enabled)
    if body.spot_accum_require_adx_confirm is not None:
        _apply_and_persist("SPOT_ACCUM_REQUIRE_ADX_CONFIRM", body.spot_accum_require_adx_confirm)
    if body.accumulation_reversal_exit_enabled is not None:
        _apply_and_persist("ACCUMULATION_REVERSAL_EXIT_ENABLED", body.accumulation_reversal_exit_enabled)
    if body.spot_accum_hard_sl_enabled is not None:
        _apply_and_persist("SPOT_ACCUM_HARD_SL_ENABLED", body.spot_accum_hard_sl_enabled)
    return {"ok": True}


class AiContinuousBody(BaseModel):
    enabled: bool


@app.put("/api/config/ai-continuous")
def put_ai_continuous(body: AiContinuousBody, email: str = Depends(require_user)):
    # Aucun equivalent fonctionnel dans ce bot (pas de couche IA generative
    # de signaux) — stocke pour compatibilite avec l interface, sans effet.
    db.set_config_override("ai_continuous", body.enabled)
    return {"ok": True, "note": "Reserve — sans effet sur ce bot (pas de moteur IA continu)."}


# ─────────────────────────────────────────────────────────────────────────
#  CONTROLE DU BOT
# ─────────────────────────────────────────────────────────────────────────
@app.post("/api/bot/start")
def bot_start(email: str = Depends(require_user)):
    if bot.trading_enabled:
        raise HTTPException(400, "Le trading tourne deja")
    # v3.2 : la cle API + le wallet Hyperliquid sont obligatoires (paper ET
    # live) — on le verifie ici pour repondre immediatement plutot que de
    # laisser le thread du bot echouer silencieusement en arriere-plan.
    if not cfg.get("PRIVATE_KEY") or not cfg.get("WALLET_ADDRESS"):
        raise HTTPException(
            400,
            "Cle API et wallet Hyperliquid obligatoires (paper et live). "
            "Configurez-les via /api/config/hyperliquid ou les variables "
            "d environnement HYPERBOT_PRIVATE_KEY / HYPERBOT_WALLET_ADDRESS."
        )
    bot.start()
    _mark_running_start()
    db.set_meta("bot_desired_state", "running")
    return {"ok": True}


@app.post("/api/bot/stop")
def bot_stop(email: str = Depends(require_user)):
    bot.stop()
    _mark_running_stop_and_accumulate()
    # Persiste explicitement l intention d arret : le TRADING ne redemarrera
    # pas tout seul apres un redeploiement/redemarrage Railway tant que
    # quelqu un n aura pas reclique sur DEMARRER (voir _auto_start_if_desired).
    # v4.14 — le MOTEUR (collecte/WebSocket), lui, redemarre toujours
    # automatiquement des que le process reboote, meme si le trading reste
    # arrete — seule la persistance de nouveaux trades depend de cet etat.
    db.set_meta("bot_desired_state", "stopped")
    return {"ok": True}


@app.post("/api/bot/force-recollect")
def force_recollect(email: str = Depends(require_user)):
    """Force une collecte d indicateurs entierement fraiche pour tous les
    actifs, sans attendre un redeploiement — utile si un probleme est
    suspecte sur les indicateurs restaures (reprise rapide < 90s). Ne touche
    ni aux positions ouvertes, ni au capital, ni a l historique."""
    bot.force_fresh_collection()
    return {"ok": True, "message": "Collecte fraiche forcee pour tous les actifs — les indicateurs vont se reconstruire progressivement."}


@app.post("/api/bot/reset-confidence")
def reset_confidence(email: str = Depends(require_user)):
    """Reinitialisation CIBLEE des seuils de confiance dynamiques par actif —
    ne touche ni aux positions ouvertes, ni au capital, ni a l historique.
    Utile quand de nombreux actifs sont "au frigo" (seuil eleve suite a des
    pertes), sans avoir a attendre la decroissance automatique ni a faire
    une reinitialisation complete destructrice."""
    bot.reset_confidence_penalties()
    return {"ok": True, "message": "Toutes les penalites de confiance ont ete reinitialisees — chaque actif repart au seuil de base."}


@app.get("/api/confidence/calibration")
def get_confidence_calibration(email: str = Depends(require_user)):
    """v4.2 — Analyse (sans rien modifier) le pouvoir predictif reel de
    chaque indicateur du score de confiance, a partir de l historique des
    trades clotures. Permet de voir AVANT d appliquer si les poids actuels
    sont corrects, sur-estimes ou sous-estimes par rapport aux resultats
    reellement observes sur ce compte."""
    return _analyze_confidence_calibration()


@app.post("/api/confidence/calibration/apply")
def apply_confidence_calibration(email: str = Depends(require_user)):
    """v4.2 — Recalcule ET applique les poids de CONFIDENCE_WEIGHTS a partir
    de l historique reel. Refuse si aucun indicateur n a assez de donnees
    (evite de calibrer sur du bruit). Les indicateurs sans assez de donnees
    gardent leur poids actuel inchange ; seuls ceux avec un echantillon
    suffisant (>= min_samples dans les deux groupes) sont ajustes."""
    print(f"[AUDIT] /api/confidence/calibration/apply appele par {email} a {datetime.now(timezone.utc).isoformat()}")
    analysis = _analyze_confidence_calibration()
    if not analysis["ready_to_calibrate"]:
        raise HTTPException(
            400,
            f"Pas assez de donnees pour calibrer (minimum {analysis['min_samples_required']} trades "
            f"dans chaque groupe 'confirme'/'non confirme' par indicateur). "
            f"Trades clotures avec detail disponible : {analysis['trades_with_breakdown']}."
        )
    _apply_and_persist("CONFIDENCE_WEIGHTS", analysis["suggested_weights"])
    return {"ok": True, "applied_weights": analysis["suggested_weights"], "analysis": analysis}


@app.get("/api/confidence/by-asset")
def get_confidence_by_asset(min_trades: int = 5, email: str = Depends(require_user)):
    """v4.4 — Probabilite de reussite reelle par actif, a partir de
    l historique des trades clotures. min_trades ajustable dynamiquement
    depuis l interface (bouton dans l onglet Historique)."""
    return _analyze_confidence_by_asset(min_trades=min_trades)


@app.get("/api/ws/events")
def get_ws_events(email: str = Depends(require_user)):
    """v4.6 — Journal des evenements WebSocket (connexion, deconnexion,
    echec/succes de reconnexion) des 7 derniers jours, persiste en base
    (survit aux redemarrages, contrairement au log en memoire)."""
    events = db.get_ws_events(days=7)
    return {
        "events": events,
        "currently_healthy": bot._is_ws_healthy() if bot.info is not None else False,
        "currently_connected": bot.info is not None,
    }


@app.get("/api/bot/logs")
def bot_logs(persistent: bool = Query(False), limit: int = Query(200), search: str = Query(None), email: str = Depends(require_user)):
    if persistent:
        # Lit la fin du fichier de log sur disque (persiste entre redemarrages
        # si HYPERBOT_DATA_DIR pointe vers un Volume Railway).
        # v3.2 — FIX : le fichier contient des lignes texte brutes
        # ("YYYY-MM-DD HH:MM:SS [LEVEL   ] message"), mais l interface attend
        # des objets {time, level, message} comme pour les logs en direct —
        # sans ce parsing, les logs persistants s affichaient vides.
        # v3.2 — FIX #2 : la limite par defaut (200 lignes) etait bien trop
        # basse compte tenu du volume de log genere (jusqu a 30 actifs
        # values a chaque cycle de 10s) — elle ne couvrait parfois que 2-3
        # minutes reelles, alors que le fichier lui-meme garde 24h. Un
        # parametre "search" permet desormais de filtrer par mot-cle (ex: un
        # ticker precis) AVANT d appliquer la limite, pour retrouver un
        # evenement precis n importe ou dans la fenetre de 24h — pas
        # seulement dans les dernieres minutes.
        import re
        pattern = re.compile(r"^(\S+ \S+) \[(\w+)\s*\] (.*)$")
        try:
            with open(be.LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
                all_lines = f.readlines()
            if search:
                all_lines = [l for l in all_lines if search.lower() in l.lower()]
            lines = all_lines[-limit:]
            parsed = []
            for line in lines:
                line = line.rstrip("\n")
                m = pattern.match(line)
                if m:
                    time_str, level_str, msg = m.groups()
                    parsed.append({
                        "time": time_str,
                        "level": _LEVEL_MAP.get(level_str.lower(), "info"),
                        "message": msg,
                    })
                else:
                    parsed.append({"time": "", "level": "info", "message": line})
            return {"logs": parsed}
        except FileNotFoundError:
            return {"logs": []}
    return {"logs": list(log_buffer)[-limit:]}


@app.get("/api/bot/logs/history")
def bot_logs_history(limit: int = Query(200), before_id: int = Query(None), level: str = Query(None), email: str = Depends(require_user)):
    """v4.244 — SUR DEMANDE EXPLICITE : journal PERSISTANT en base de
    donnees (survit aux redemarrages, retention 30 jours) — distinct du
    buffer en memoire (quelques heures) et du fichier 24h existant
    (?persistent=true sur /api/bot/logs). Pagination via before_id (id du
    plus ancien deja recu, pour charger la page precedente/plus ancienne).
    Filtre optionnel par niveau (warn/error/ok/win/loss)."""
    try:
        rows = db.query_log_history(limit=min(limit, 500), before_id=before_id, level=level)
        return {"logs": rows, "has_more": len(rows) == min(limit, 500)}
    except Exception as e:
        return {"logs": [], "has_more": False, "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────
#  DONNEES DE MARCHE / POSITIONS / SIGNAUX
# ─────────────────────────────────────────────────────────────────────────
@app.get("/api/prices")
def get_prices(email: str = Depends(require_user)):
    # v3.2 : priorite au cache WebSocket complet (bot.all_mids, alimente par
    # le flux allMids — couvre potentiellement TOUS les actifs Hyperliquid,
    # pas seulement ceux tradés par ce bot). Complete avec les prix suivis
    # individuellement (state.current_price) pour nos symboles, au cas ou
    # le WebSocket ne serait pas encore actif (repli cycle REST).
    prices = {}
    try:
        for ticker, raw in (bot.all_mids or {}).items():
            try:
                prices[ticker] = float(raw)
            except (TypeError, ValueError):
                continue
    except Exception:
        pass
    for k, s in bot.states.items():
        ticker = be.ticker_from_slot_key(k)
        if s.current_price and ticker not in prices:
            prices[ticker] = s.current_price
    return {"prices": prices}


@app.get("/api/volatility")
def get_volatility(email: str = Depends(require_user)):
    """Classement en direct de la volatilite (ATR%) des actifs suivis — aide
    a identifier ou le mouvement de prix est le plus fort a l instant present
    (donc le potentiel de capture le plus eleve avec les seuils actuels).
    Inclut aussi l ADX (force de la tendance) et le mode detecte
    (trend/reversal) pour chaque actif."""
    active_coins = cfg.get("ACTIVE_COINS") or SUPPORTED_TICKERS
    adx_threshold = cfg.get("ADX_TREND_THRESHOLD", 25.0)
    manual_modes = cfg.get("SYMBOL_RSI_MODE", {})
    rows = []
    for slot_key, state in bot.states.items():
        ticker = be.ticker_from_slot_key(slot_key)
        if state.current_atr_pct is None:
            continue
        adx = state.current_adx
        if manual_modes.get(ticker):
            mode = manual_modes[ticker]
        elif adx is not None:
            mode = "trend" if adx >= adx_threshold else "reversal"
        else:
            # v3.2 — FIX : le bot bascule sur "trend" par defaut si l ADX
            # n est pas encore calculable (collecte insuffisante) — l API
            # doit refleter EXACTEMENT ce meme comportement, sinon
            # l interface affichait aucun badge, donnant l impression
            # trompeuse d un actif "non tradable" alors qu il l est deja.
            mode = "trend"
        rows.append({
            "coin": ticker,
            "atr_pct": round(state.current_atr_pct, 4),
            "adx": round(adx, 1) if adx is not None else None,
            "mode": mode,
            "price": state.current_price,
            "active": ticker in active_coins,
            "has_position": bool(state.position),
        })
    rows.sort(key=lambda r: r["atr_pct"], reverse=True)
    return {"ranking": rows, "updated_at": datetime.now(timezone.utc).isoformat()}


@app.get("/api/positions")
def get_positions(email: str = Depends(require_user)):
    return {"positions": _open_positions()}


@app.get("/api/indicators/{ticker}")
def get_indicator_history(ticker: str, email: str = Depends(require_user)):
    """v4.21 — Historique des indicateurs (RSI, MACD, EMA200, ATR,
    support/resistance) d un actif, pour affichage en graphe cote
    interface. Cherche parmi TOUS les slots (BTC_0, BTC_1...) portant ce
    ticker et retourne celui qui a le plus de donnees (le plus actif)."""
    ticker = _norm_ticker(ticker)
    best_state = None
    best_len = -1
    for slot_key, state in bot.states.items():
        if be.ticker_from_slot_key(slot_key) == ticker:
            n = len(state.indicator_history)
            if n > best_len:
                best_len = n
                best_state = state
    if best_state is None:
        raise HTTPException(404, f"Actif inconnu ou non suivi : {ticker}")
    return {"ticker": ticker, "history": list(best_state.indicator_history)}


@app.get("/api/atr-summary")
def get_atr_summary(email: str = Depends(require_user)):
    """v4.38 — SUR DEMANDE EXPLICITE : resume de l ATR (vrai calcul haut/bas
    de Wilder, avec repli sur l ancien calcul cloture-a-cloture si pas
    encore assez de bougies accumulees) de TOUS les actifs suivis, a la
    demande — pour recalibrer ATR_MIN_PCT sur des donnees reelles plutot
    que de scanner des dizaines de lignes eparpillees dans les logs."""
    cfg = bot.cfg
    atr_period = cfg.get("ATR_PERIOD", 14)
    results = []
    for slot_key, state in bot.states.items():
        ticker = be.ticker_from_slot_key(slot_key)
        atr_pct_val = None
        source = None
        if len(state.candle_history) >= atr_period + 1:
            _, atr_pct_val = be.calc_true_range_atr(list(state.candle_history), atr_period)
            source = "haut/bas (vrai)"
        if atr_pct_val is None and len(state.price_history) >= atr_period + 1:
            _, atr_pct_val = be.calc_atr(list(state.price_history), atr_period)
            source = "cloture-a-cloture (repli)"
        results.append({
            "ticker": ticker,
            "atr_pct": round(atr_pct_val, 4) if atr_pct_val is not None else None,
            "source": source,
            "candles_collected": len(state.candle_history),
            "active": ticker in cfg.get("ACTIVE_COINS", []),
        })
    results.sort(key=lambda r: (r["atr_pct"] is None, -(r["atr_pct"] or 0)))
    return {"atr_period": atr_period, "results": results}


@app.get("/api/trend-summary")
def get_trend_summary(email: str = Depends(require_user)):
    """v4.59 — SUR DEMANDE EXPLICITE : classe chaque actif suivi en
    "haussier franc" / "baissier franc" / "indecis", a la demande — meme
    logique que la base commune des 3 modes (_unified_trend_confirmed) :
    EMA200 pour la direction, ADX >= seuil pour la force (une direction
    sans force n est pas consideree franche)."""
    cfg = bot.cfg
    adx_period = cfg.get("ADX_PERIOD", 14)
    adx_threshold = cfg.get("ADX_TREND_THRESHOLD", 25.0)
    results = []
    for slot_key, state in bot.states.items():
        ticker = be.ticker_from_slot_key(slot_key)
        price = state.current_price
        prices = list(state.price_history)
        ema200 = be.calc_ema(list(state.mtf_prices), 200) if len(state.mtf_prices) >= 5 else None
        adx = be.calc_adx(prices, adx_period) if len(prices) >= adx_period + 1 else None
        if price is None or ema200 is None or adx is None:
            label = "donnees insuffisantes"
        else:
            strong = adx >= adx_threshold
            if price > ema200 and strong:
                label = "haussier franc"
            elif price < ema200 and strong:
                label = "baissier franc"
            else:
                label = "indecis"
        results.append({
            "ticker": ticker,
            "price": price,
            "ema200": round(ema200, 6) if ema200 is not None else None,
            "adx": round(adx, 1) if adx is not None else None,
            "adx_threshold": adx_threshold,
            "label": label,
            "active": ticker in (cfg.get("ACTIVE_COINS") or []),
        })
    order = {"haussier franc": 0, "baissier franc": 1, "indecis": 2, "donnees insuffisantes": 3}
    results.sort(key=lambda r: (order.get(r["label"], 9), r["ticker"]))
    return {"adx_threshold": adx_threshold, "results": results, "refreshed_at": time.time()}


@app.get("/api/strategy-performance/{strategy}")
def get_strategy_performance(strategy: str, email: str = Depends(require_user)):
    """v4.43 — SUR DEMANDE EXPLICITE : performance d un MODE precis
    (normal/accumulation/funding_contrarian/spot_accumulation), calculee a
    la demande depuis l historique reel en base — pour le bouton
    "Performance" de chaque sous-onglet de l onglet Paper Trading."""
    all_closed = db.get_all_closed_trades(since=db.get_meta("stats_reset_at"))  # v4.306 — point de reinitialisation
    filtered = [t for t in all_closed if (t.get("strategy") or "forex") == strategy]
    # v4.355 — net de frais si FEES_IN_STATS : gagnant / perdant jugees APRES frais (un +0,02 $ qui coute 0,03 $ de
    # frais est une perte), sommes nettes ; le brut reste disponible (net_pnl_gross).
    wins = [t for t in filtered if _row_pnl(t) > 0]
    losses = [t for t in filtered if _row_pnl(t) <= 0]
    total_pnl = sum(_row_pnl(t) for t in filtered)
    win_pnl = sum(_row_pnl(t) for t in wins)
    loss_pnl = sum(_row_pnl(t) for t in losses)
    gross_total = sum((t.get("pnl") or 0) for t in filtered)
    # v4.186 — SUR DEMANDE EXPLICITE : total des frais REELS estimes payes
    # a Hyperliquid pour ce mode — uniquement les trades LIVE ont un frais
    # non-None (voir bot_engine.py close_position), le paper n en a jamais.
    total_fees = sum((t.get("fees_paid") or 0) for t in filtered)
    open_count = sum(1 for st in bot.states.values() if st.position and (st.position.get("strategy") or "forex") == strategy)
    # v4.121 — SUR DEMANDE EXPLICITE : Accumulation a desormais son PROPRE
    # emplacement (bot.accum_states), jamais compte ci-dessus.
    if strategy == "accumulation":
        open_count += sum(1 for st in bot.accum_states.values() if st.position)
    # v4.350 — SUR DEMANDE EXPLICITE (le panneau paraissait fige) : periode couverte, heure de calcul et net
    # apres frais estimes. Rappel : seuls les trades fermes depuis le point de reinitialisation des
    # statistiques sont comptes ("since"), pas "depuis le debut".
    closed_times = [t.get("closed_at") for t in filtered if t.get("closed_at")]
    fee_pct = float(cfg.get("FEE_ROUND_TRIP_PCT", _ROUND_TRIP_FEE_RATE * 100))
    fees_est = sum(_trade_fee_usd(t) for t in filtered)
    manual_summary = None
    if strategy in _MANUAL_DIR_BY_MODE:
        want = _MANUAL_DIR_BY_MODE[strategy]
        mt = [t for t in all_closed if (t.get("strategy") or "") == "manual" and t.get("action") == want]
        mnet = [_row_pnl(t) for t in mt]
        mopen = 0
        try:
            mopen = sum(1 for it in bot.manual.items.values()
                        if it.get("status") in ("open", "opening", "closing") and it.get("direction") == want.lower())
        except Exception:
            pass
        manual_summary = {"direction": want, "n": len(mt), "wins": sum(1 for x in mnet if x > 0), "net": round(sum(mnet), 4),
                          "gross": round(sum((t.get("pnl") or 0) for t in mt), 4), "fees": round(sum(_trade_fee_usd(t) for t in mt), 4),
                          "open": mopen}
    return {
        "manual": manual_summary,
        "fees_on": _fees_on(), "net_pnl_gross": round(gross_total, 4),
        "since": db.get_meta("stats_reset_at"),
        "first_closed_at": min(closed_times) if closed_times else None,
        "last_closed_at": max(closed_times) if closed_times else None,
        "fees_estimated": round(fees_est, 4), "fee_pct": round(fee_pct, 3),
        "net_after_fees_est": round(gross_total - fees_est, 4),
        "strategy": strategy,
        "total_trades": len(filtered),
        "open_trades": open_count,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(filtered) * 100, 1) if filtered else 0,
        "net_pnl": round(total_pnl, 4),
        "win_pnl": round(win_pnl, 4),
        "loss_pnl": round(loss_pnl, 4),
        "total_fees_paid": round(total_fees, 4),
        "refreshed_at": time.time(),
    }


@app.get("/api/entry-diagnostics")
def get_entry_diagnostics_all(email: str = Depends(require_user)):
    """v4.40 — SUR DEMANDE EXPLICITE, suite a un ecart de 13h sans aucun
    trade jamais explique faute de logs disponibles : instantane de l etat
    de TOUTES les portes d entree (RSI, MACD, amplitude, niveaux, fraicheur,
    confirmation post-trade, separation EMA200...) pour TOUS les actifs
    suivis, a la demande — capture a chaque cycle cote bot_engine, donc
    toujours a jour au moment de l appel, quelle que soit la retention des
    logs. Vue d ensemble compacte : pour le detail complet d un actif, voir
    /api/entry-diagnostics/{ticker}."""
    results = []
    for slot_key, state in bot.states.items():
        ticker = be.ticker_from_slot_key(slot_key)
        snap = state.last_gate_snapshot or {}
        has_position = state.position is not None
        # v4.40 — resume compact : identifie le PREMIER obstacle qui bloque
        # chaque sens, pour un coup d oeil rapide sans lire les 20 champs.
        blocker_long = None
        if has_position:
            blocker_long = "position deja ouverte"
        elif not snap:
            blocker_long = "pas encore de donnees"
        # v4.134 — FIX BUG CRITIQUE : en mode simplifie (unified_mode_active),
        # la VRAIE decision n utilise plus du tout rsi_buy/ema_bull/trend_up
        # individuellement — seulement long_level_ok_final (tendance+ADX+
        # proximite combines). Ce gate obsolete bloquait AVANT MEME d
        # atteindre la logique correcte du mode simplifie (deja codee plus
        # bas, mais jamais atteinte) — expliquant "signal de base non
        # reuni" en quasi-permanence, meme quand le signal simplifie etait
        # en realite tout pres de qualifier.
        elif not snap.get("unified_mode_active") and not (snap.get("rsi_buy") and snap.get("ema_bull") and snap.get("trend_up")):
            blocker_long = "signal de base non reuni (RSI/EMA/tendance)"
        elif snap.get("long_signal_stale"):
            blocker_long = "signal pas encore renouvele (fraicheur)"
        elif not snap.get("long_level_ok_final"):
            # v4.58 — SUR DEMANDE EXPLICITE : raisons COHERENTES avec le mode
            # actif — le mode simplifie (base commune) n a plus rien a voir
            # avec MACD/amplitude ATR/separation EMA200/confirmation post-trade.
            if snap.get("unified_mode_active"):
                if not snap.get("unified_trend_confirmed_long"):
                    blocker_long = "tendance/ADX pas assez forte"
                elif not snap.get("unified_proximity_long_ok"):
                    blocker_long = "hors fenetre 1-5% du support (et pas de cassure)"
                elif not snap.get("unified_amplitude_ok"):
                    blocker_long = "fourchette S/R trop etroite"
                else:
                    blocker_long = "base commune non reunie (raison indeterminee)"
            elif not snap.get("direction_confirmed_long"):
                blocker_long = "MACD ne confirme pas"
            elif not snap.get("amplitude_coherent"):
                blocker_long = "amplitude ATR incoherente"
            elif not snap.get("sr_ema_long_ok"):
                blocker_long = "support trop proche de l EMA200"
            elif snap.get("post_win_confirm_long"):
                blocker_long = f"confirmation post-trade en attente ({snap.get('confirm_count_long',0)}/18 cycles, {snap.get('post_win_wait_long',0)}/180 max)"
            else:
                blocker_long = "niveau (support/resistance) non respecte"
        else:
            blocker_long = None  # rien ne bloque, devrait trader au prochain signal
        blocker_short = None
        if has_position:
            blocker_short = "position deja ouverte"
        elif not snap:
            blocker_short = "pas encore de donnees"
        # v4.134 — FIX BUG CRITIQUE : meme correctif que blocker_long.
        elif not snap.get("unified_mode_active") and not (snap.get("rsi_sell") and snap.get("ema_bear") and snap.get("trend_down")):
            blocker_short = "signal de base non reuni (RSI/EMA/tendance)"
        elif snap.get("short_signal_stale"):
            blocker_short = "signal pas encore renouvele (fraicheur)"
        elif not snap.get("short_level_ok_final"):
            if snap.get("unified_mode_active"):
                if not snap.get("unified_trend_confirmed_short"):
                    blocker_short = "tendance/ADX pas assez forte"
                elif not snap.get("unified_proximity_short_ok"):
                    blocker_short = "hors fenetre 1-5% de la resistance (et pas de cassure)"
                elif not snap.get("unified_amplitude_ok"):
                    blocker_short = "fourchette S/R trop etroite"
                else:
                    blocker_short = "base commune non reunie (raison indeterminee)"
            elif not snap.get("direction_confirmed_short"):
                blocker_short = "MACD ne confirme pas"
            elif not snap.get("amplitude_coherent"):
                blocker_short = "amplitude ATR incoherente"
            elif not snap.get("sr_ema_short_ok"):
                blocker_short = "resistance trop proche de l EMA200"
            elif snap.get("post_win_confirm_short"):
                blocker_short = f"confirmation post-trade en attente ({snap.get('confirm_count_short',0)}/18 cycles, {snap.get('post_win_wait_short',0)}/180 max)"
            else:
                blocker_short = "niveau (support/resistance) non respecte"
        else:
            blocker_short = None
        # v4.55 — SUR DEMANDE EXPLICITE : le diagnostic ne couvrait jusqu ici
        # que le mode normal — ajoute Spot-Accumulation, dont la logique
        # d entree est completement differente (LONG uniquement, fenetre de
        # distance au support, amplitude S/R, confirmation ADX...).
        spot_snap = state.spot_accum_gate_snapshot or {}
        if has_position and state.position.get("strategy") == "spot_accumulation":
            blocker_spot_accum = "position deja ouverte"
        elif not spot_snap:
            blocker_spot_accum = "pas encore de donnees"
        else:
            blocker_spot_accum = spot_snap.get("blocker", "pas encore de donnees")
        # v4.80 — SUR DEMANDE EXPLICITE : meme diagnostic pour Accumulation,
        # qui n en avait aucun jusqu ici.
        # v4.121 — FIX : Accumulation a desormais son PROPRE emplacement
        # (bot.accum_states), independant de bot.states — lit depuis le bon
        # endroit, verifie sa PROPRE position (pas celle du mode normal).
        accum_state = bot.accum_states.get(slot_key)
        accum_snap = (accum_state.accumulation_gate_snapshot if accum_state else None) or {}
        if accum_state and accum_state.position is not None:
            blocker_accumulation = "position deja ouverte"
            blocker_accumulation_long = blocker_accumulation
            blocker_accumulation_short = blocker_accumulation
        elif not accum_snap:
            blocker_accumulation = "pas encore de donnees"
            blocker_accumulation_long = blocker_accumulation
            blocker_accumulation_short = blocker_accumulation
        else:
            blocker_accumulation = accum_snap.get("blocker", "pas encore de donnees")
            # v4.136 — SUR DEMANDE EXPLICITE : expose desormais separement
            # LONG et SHORT pour Accumulation (comme le mode normal),
            # au lieu d un seul champ combine qui masquait si un seul des
            # deux sens etait en realite bloque.
            blocker_accumulation_long = accum_snap.get("blocker_long", blocker_accumulation)
            blocker_accumulation_short = accum_snap.get("blocker_short", blocker_accumulation)
        # v4.272 — les lignes LONG/SHORT sont celles du mode FOREX : il ne
        # trade que FOREX_MODE_SYMBOLS (devises xyz + PAXG). Pour une crypto,
        # "aucun obstacle" etait trompeur — et pour une devise, le filtre
        # anti-range (blocage le plus frequent) n apparaissait pas.
        if ticker not in cfg.get("FOREX_MODE_SYMBOLS", []) and not bot._forex_crypto_eligible(ticker):
            if cfg.get("FOREX_CRYPTO_ENABLED", 0):
                _sc = getattr(bot, "_asset_win_rates_all", {}).get(ticker)
                _why = (f"score {_sc['win_rate']:.0f}% sur {_sc['n']} trades < {cfg.get('FOREX_CRYPTO_MIN_WIN_RATE', 55.0)}%"
                        if _sc else f"score non etabli (moins de {cfg.get('FOREX_CRYPTO_MIN_TRADES', 10)} trades)")
                blocker_long = blocker_short = f"non concerne (crypto non eligible au mode Forex : {_why})"
            else:
                blocker_long = blocker_short = "non concerne (crypto : le mode Forex ne trade que les devises et PAXG)"
        elif not has_position and (snap.get("forex_ranging") or snap.get("quality_block")):
            range_txt = snap.get("quality_block") or snap.get("forex_range_text") or "marche en range"
            blocker_long = blocker_long or range_txt
            blocker_short = blocker_short or range_txt
        # v4.264 — raison EXACTE d un blocage anticipe (collecte, forex
        # ferme, chauffe, prix indisponible, horaires...) au lieu de "pas
        # encore de donnees" — cas permanent des actifs forex jusqu ici.
        blocked_reason = snap.get("blocked_reason")
        if blocked_reason and not has_position and ticker in cfg.get("FOREX_MODE_SYMBOLS", []):
            blocker_long = blocker_short = blocked_reason
        is_forex_row = ticker in cfg.get("FOREX_MODE_SYMBOLS", []) and ticker.startswith("xyz:")
        if is_forex_row:
            # Les marches forex HIP-3 sont reserves au mode Forex : les autres
            # modes ne les evaluent jamais (isolation v4.163).
            blocker_spot_accum = "non concerne (actif forex, mode Forex uniquement)"
            blocker_accumulation = blocker_accumulation_long = blocker_accumulation_short = blocker_spot_accum
        elif blocked_reason:
            if blocker_spot_accum == "pas encore de donnees":
                blocker_spot_accum = blocked_reason
            if blocker_accumulation == "pas encore de donnees":
                blocker_accumulation = blocker_accumulation_long = blocker_accumulation_short = blocked_reason
        try:  # v4.281 — qualite du marche (relative aux habitudes de l actif)
            quality = bot.market_quality(ticker, state)
        except Exception:
            quality = None
        _sr = getattr(state, "sr_display", None) or {}
        levels = {  # v4.283 — valeurs exactes, verifiables sur le graphique Hyperliquid
            "trend_ema": getattr(state, "trend_ema_value", None),
            "trend_ema_source": getattr(state, "trend_ema_source", None),
            "support": _sr.get("support"), "resistance": _sr.get("resistance"), "sr_source": _sr.get("source"),
            "price": (lambda v: float(v) if v else state.current_price)((bot.all_mids or {}).get(ticker)),
            # v4.286 — niveaux de STRUCTURE 1h
            "support_1h": bot._structural_support(state), "resistance_1h": bot._structural_resistance(state),
        }
        try:
            _px = None
            try:
                _px = float((bot.all_mids or {}).get(ticker) or 0) or None
            except (TypeError, ValueError):
                pass
            situation = bot.situation(state, _px) if ":" not in ticker else None
        except Exception:
            situation = None
        _fs = getattr(state, "funding_gate_snapshot", None) or {}
        if not cfg.get("FUNDING_MODE_ENABLED", False):
            blocker_funding = "mode desactive"
        elif not _fs:
            blocker_funding = snap.get("blocked_reason") or "pas encore evalue"
        elif _fs.get("blocker"):
            blocker_funding = _fs["blocker"]
        else:
            blocker_funding = f"signal {(_fs.get('direction') or '').upper()} ({_fs.get('annual_pct')}%/an) — candidat genere"
        mtf_line = None
        if ":" not in ticker and cfg.get("ENTRY_ENGINE_MTF", 1):
            try:  # v4.299 — lecture top-down (bougies en cache uniquement : diagnostic instantane)
                _p = float((bot.all_mids or {}).get(ticker) or 0) or state.current_price
                mv = bot.mtf_view(ticker, _p, cache_only=True) if _p else None
                if mv and mv.get("ok"):
                    M = {"4h": "H4", "1d": "Daily"}.get(mv["major_tf"], mv["major_tf"])
                    where = ("prix DANS la zone de support" if mv["in_support"] else
                             "prix DANS la zone de resistance" if mv["in_resistance"] else "prix entre deux zones")
                    # v4.307 — FIX BUG CONFIRME : quand le prix est a l
                    # interieur d une zone UNIQUE, mtf.nearest_zones()
                    # renvoie cette meme zone comme support ET resistance
                    # (les deux conditions sont vraies a la fois) — l
                    # affichage montrait alors "support X · resistance X"
                    # avec les MEMES bornes, laissant croire a 2 zones
                    # distinctes alors qu il n y en a qu une. Confirme sur 8
                    # actifs reels (AAVE, BTC, CRV, ETH, INJ, PENDLE, SUSHI,
                    # WIF). Ce cas a aussi un impact reel sur le calcul de l
                    # objectif (voir bot_engine._mtf_entry, meme version) —
                    # ici, seul l affichage est corrige.
                    if mv["support"] is mv["resistance"] and mv["support"] is not None:
                        mtf_line = (f"tendance {M} {mv['trend']} · zone unique {M} {bot._zone_txt(mv['support'])} "
                                    f"(support ET resistance, aucune zone opposee distincte) · {where}")
                    else:
                        mtf_line = (f"tendance {M} {mv['trend']} · support {M} {bot._zone_txt(mv['support'])} · "
                                    f"resistance {M} {bot._zone_txt(mv['resistance'])} · {where}")
                    # v4.341 — SUR DEMANDE EXPLICITE : signale des bougies H4 ecartees du reel
                    if not mv.get("data_fresh", True):
                        mtf_line += (f" · ⚠️ DONNEES {M} PERIMEES (derniere bougie cloturee : "
                                     f"{datetime.fromtimestamp(mv['last_closed_t'] / 1000, timezone.utc).strftime('%d/%m %H:%M')} UTC)")
                elif mv:
                    mtf_line = mv.get("why")
            except Exception as e_m:
                mtf_line = f"indisponible ({e_m})"
        results.append({
            "mtf": mtf_line,
            "blocker_funding": blocker_funding,
            "situation": situation,
            "levels": levels,
            "quality": quality,
            "blocked_reason": blocked_reason,
            "ticker": ticker,
            "has_position": has_position,
            "snapshot_age_sec": round(time.time() - snap["ts"], 1) if snap.get("ts") else None,
            "blocker_long": blocker_long,
            "blocker_short": blocker_short,
            "scalp": _scalp_diag_line(ticker, state),   # v4.353
            "fxswing": _fxswing_diag_line(ticker, state),   # v4.361
            "supsw": _supsw_diag_line(ticker, state),       # v4.362
            "trendf": _trendf_diag_line(ticker, state),     # v4.365
            "blocker_spot_accum": blocker_spot_accum,
            "spot_accum_detail": spot_snap if spot_snap else None,
            "blocker_accumulation": blocker_accumulation,
            "blocker_accumulation_long": blocker_accumulation_long,
            "blocker_accumulation_short": blocker_accumulation_short,
            "accumulation_detail": accum_snap if accum_snap else None,
            # v4.255 — SUR DEMANDE EXPLICITE : expose directement le flux de
            # transactions calcule en interne — auparavant jamais visible
            # nulle part, empechant toute verification concrete que ce
            # mecanisme fonctionne reellement (donnees recuperees, calcul
            # correct) plutot que de rester une simple affirmation
            # theorique. None = pas encore de lecture disponible pour cet
            # actif a cet instant (historique insuffisant ou echec API).
            # v4.261 — SUR DEMANDE EXPLICITE, FIX BUG D AFFICHAGE : lisait
            # auparavant "entry_flow_pressure" (snap), calcule UNIQUEMENT
            # quand la logique d entree ATTEINT cette verification precise
            # — si un actif est bloque PLUS TOT (tendance, proximite...),
            # ce calcul n est jamais execute, affichant a tort "pas encore
            # de donnees" meme quand le mecanisme fonctionne normalement.
            # Lit desormais depuis state.trade_flow_history, alimente en
            # CONTINU par _maybe_refresh_trade_flow (tourne a chaque cycle,
            # independamment des conditions d entree) — la VRAIE source
            # utilisee par la pression soutenue et la sortie acceleree.
            # v4.266 — derniere lecture seulement si elle date de moins de 5 min
            "trade_flow_pressure": (round(list(state.trade_flow_history)[-1], 3)
                                    if getattr(state, "trade_flow_history", None)
                                    and getattr(state, "trade_flow_ts_history", None)
                                    and time.time() - list(state.trade_flow_ts_history)[-1] < 300
                                    else None),
            "trend_persistence_confirmed": snap.get("trend_persistence_confirmed") or (accum_snap.get("trend_persistence_confirmed") if accum_snap else None) or (spot_snap.get("trend_persistence_confirmed") if spot_snap else None),
        })
    results.sort(key=lambda r: r["ticker"])
    _rg = bot.market_regime()
    return {"results": results, "market_regime": {"regime": _rg.get("regime"), "detail": _rg.get("detail"),
                                                  "filter_enabled": bool(cfg.get("MARKET_REGIME_FILTER_ENABLED", 1))}}


# ─────────────────────────────────────────────────────────────────────────
#  v4.276 — REGIME DE MARCHE + STATISTIQUES PAR TRANCHE HORAIRE
# ─────────────────────────────────────────────────────────────────────────
@app.get("/api/market-regime")
def get_market_regime(email: str = Depends(require_user)):
    r = dict(bot.market_regime())
    r["filter_enabled"] = bool(cfg.get("MARKET_REGIME_FILTER_ENABLED", 1))
    r["blocks"] = {"haussier": "Accumulation (shorts) bloque", "baissier": "Spot-Accum (achats) bloque"}.get(r["regime"], "aucun blocage")
    return r


@app.get("/api/stats/hours")
def stats_by_hour(days: int = Query(14, ge=1, le=90), email: str = Depends(require_user)):
    """Resultats par mode et par tranche de 4 h UTC (trades fermes)."""
    labels = {"spot_accumulation": "Spot-Accum", "accumulation": "Accumulation", "funding_contrarian": "Funding", "forex": "Forex", "manual": "Manuel"}
    days_cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    reset_at = db.get_meta("stats_reset_at")  # v4.306 — point de reinitialisation des statistiques (onglet Historique)
    out = {}
    for t in db.get_all_closed_trades(since=reset_at):
        if not t.get("closed_at") or (t.get("created_at") or "") < days_cutoff or t.get("pnl") is None:
            continue
        try:
            h = datetime.fromisoformat(t["created_at"]).astimezone(timezone.utc).hour
        except (TypeError, ValueError):
            continue
        mode = labels.get(t.get("strategy") or "forex", t.get("strategy"))
        block = f"{h // 4 * 4:02d}-{h // 4 * 4 + 4:02d}"
        fees = _trade_fee_usd(t)                       # v4.355 : meme calcul des frais partout
        b = out.setdefault(mode, {}).setdefault(block, {"n": 0, "wins": 0, "pnl": 0.0, "net": 0.0})
        b["n"] += 1
        b["wins"] += 1 if t["pnl"] > 0 else 0
        b["pnl"] += t["pnl"]
        b["net"] += t["pnl"] - fees
    for mode in out.values():
        for b in mode.values():
            b["win_rate"] = round(b["wins"] / b["n"] * 100, 1)
            b["pnl"], b["net"] = round(b["pnl"], 3), round(b["net"], 3)
    return {"days": days, "blocks_utc": ["00-04", "04-08", "08-12", "12-16", "16-20", "20-24"], "modes": out}


# ─────────────────────────────────────────────────────────────────────────
#  v4.292 — CONTROLE DE SYNCHRONISATION BOT <-> HYPERLIQUID
# ─────────────────────────────────────────────────────────────────────────
@app.get("/api/live-sync")
def live_sync_report(email: str = Depends(require_user)):
    rep = getattr(bot, "live_sync_report", None)
    return rep or {"ts": None, "rows": [], "ok": None, "info": "premier controle dans les 2 minutes suivant le demarrage"}


@app.post("/api/live-sync/run")
def live_sync_run(email: str = Depends(require_user)):
    if bot.info is None or not cfg.get("WALLET_ADDRESS"):
        raise HTTPException(400, "Connexion Hyperliquid indisponible")
    return bot.run_live_sync()


# ─────────────────────────────────────────────────────────────────────────
#  v4.273 — TRADING MANUEL
# ─────────────────────────────────────────────────────────────────────────
class ManualOrderBody(BaseModel):
    strategy: str
    ticker: str
    direction: str
    market: str = "perp"
    mode: str = "paper"
    execution: str = "now"
    notional_usd: float
    leverage: int = 1
    sl_pct: float
    tp_pct: Optional[float] = None
    ttp_arm_pct: Optional[float] = None
    ttp_trail_pct: Optional[float] = None
    trigger_price: Optional[float] = None
    trigger_time: Optional[float] = None
    expiry_hours: Optional[float] = None
    require_valid: bool = True
    confirm_live: bool = False
    # v4.343 — ordre libre : verrou progressif du SL + criteres de tendance (choix manuels)
    lock_arm_pct: Optional[float] = None
    lock_fraction: Optional[float] = None
    trend_entry: Optional[str] = None     # None | "haussiere" | "baissiere" | "neutre"
    trend_exit: Optional[str] = None      # None | "opposee" | "perdue"
    tp_run: Optional[bool] = None         # v4.395 : laisser courir au TP (gain verrouille puis stop suiveur)
    tp_run_lock_pct: Optional[float] = None
    tp_run_trail_pct: Optional[float] = None
    tp1_pct: Optional[float] = None       # v4.404 : TP1 (cloture partielle), le TP devient le TP2
    tp1_close_pct: Optional[float] = None
    tp1_be: Optional[bool] = None


class ManualModifyBody(BaseModel):
    sl_price: Optional[float] = None
    tp_price: Optional[float] = None
    clear_tp: bool = False
    ttp_arm_pct: Optional[float] = None
    ttp_trail_pct: Optional[float] = None
    clear_ttp: bool = False
    lock_arm_pct: Optional[float] = None    # v4.343
    lock_fraction: Optional[float] = None
    clear_lock: bool = False
    trend_exit: Optional[str] = None
    clear_trend_exit: bool = False
    tp_run: Optional[bool] = None            # v4.395 : laisser courir au TP
    tp_run_lock_pct: Optional[float] = None
    tp_run_trail_pct: Optional[float] = None
    # v4.400 : modification d'un ordre PROGRAMME (pourcentages, comme a la creation)
    sl_pct: Optional[float] = None
    tp_pct: Optional[float] = None
    notional_usd: Optional[float] = None
    leverage: Optional[int] = None
    trigger_price: Optional[float] = None
    trigger_time: Optional[float] = None
    expiry_hours: Optional[float] = None
    direction: Optional[str] = None          # v4.401 : ordre programme, changer le sens ("long"/"short")
    mode: Optional[str] = None               # v4.401 : ordre programme, "paper"/"live"
    confirm_live: bool = False
    execution: Optional[str] = None          # v4.402 : critere d'entree d'un ordre programme
    trend_entry: Optional[str] = None        # "" = aucune condition de tendance
    require_valid: Optional[bool] = None
    tp1_price: Optional[float] = None        # v4.404 : position ouverte
    tp1_pct: Optional[float] = None          # v4.404 : ordre programme
    clear_tp1: bool = False
    tp1_close_pct: Optional[float] = None
    tp1_be: Optional[bool] = None


def _manual_call(fn, *args):
    try:
        return fn(*args)
    except manual_trading.ManualError as e:
        raise HTTPException(400, str(e))


@app.get("/api/manual/state")
def manual_state(email: str = Depends(require_user)):
    return bot.manual.snapshot()


@app.get("/api/manual/leverage-check")
def manual_leverage_check(email: str = Depends(require_user)):
    """v4.414 — diagnostic du levier reel (ce que renvoie Hyperliquid) + alignement immediat du suivi."""
    return bot.manual.leverage_check()


@app.get("/api/manual/proposal")
def manual_proposal(strategy: str, ticker: str, direction: str, market: str = "perp",
                    email: str = Depends(require_user)):
    return _manual_call(bot.manual.propose, strategy, _norm_ticker(ticker), direction, market)


@app.post("/api/manual/orders")
def manual_submit(body: ManualOrderBody, email: str = Depends(require_user)):
    params = body.model_dump()
    params["ticker"] = _norm_ticker(params["ticker"])
    item = _manual_call(bot.manual.submit, params)
    if item.get("status") == "failed":
        raise HTTPException(400, f"Ouverture impossible : {item.get('error')}")
    return item


@app.delete("/api/manual/orders/{item_id}")
def manual_cancel(item_id: int, email: str = Depends(require_user)):
    return _manual_call(bot.manual.cancel, item_id)


@app.put("/api/manual/positions/{item_id}")
def manual_modify(item_id: int, body: ManualModifyBody, email: str = Depends(require_user)):
    changes = {}
    if body.sl_price:
        changes["sl_price"] = body.sl_price
    if body.clear_tp:
        changes["tp_price"] = None
        changes["tp_pct"] = None
    elif body.tp_price:
        changes["tp_price"] = body.tp_price
    if not body.clear_tp and body.tp_pct:
        changes["tp_pct"] = body.tp_pct
    for _k in ("sl_pct", "notional_usd", "leverage", "trigger_price", "trigger_time", "expiry_hours", "direction", "mode"):
        if getattr(body, _k) is not None:
            changes[_k] = getattr(body, _k)
    if body.clear_ttp:
        changes["ttp_arm_pct"] = changes["ttp_trail_pct"] = None
    else:
        if body.ttp_arm_pct:
            changes["ttp_arm_pct"] = body.ttp_arm_pct
        if body.ttp_trail_pct:
            changes["ttp_trail_pct"] = body.ttp_trail_pct
    # v4.343 — verrou progressif + regle de sortie sur la tendance
    if body.clear_lock:
        changes["lock_arm_pct"] = changes["lock_fraction"] = None
    else:
        if body.lock_arm_pct:
            changes["lock_arm_pct"] = body.lock_arm_pct
        if body.lock_fraction:
            changes["lock_fraction"] = body.lock_fraction
    if body.confirm_live:
        changes["confirm_live"] = True
    if body.clear_tp1:
        changes["tp1_price"] = None
        changes["tp1_pct"] = None
    else:
        if body.tp1_price:
            changes["tp1_price"] = body.tp1_price
        if body.tp1_pct:
            changes["tp1_pct"] = body.tp1_pct
    if body.tp1_close_pct is not None:
        changes["tp1_close_pct"] = body.tp1_close_pct
    if body.tp1_be is not None:
        changes["tp1_be"] = body.tp1_be
    if body.execution:
        changes["execution"] = body.execution
    if body.trend_entry is not None:
        changes["trend_entry"] = body.trend_entry
    if body.require_valid is not None:
        changes["require_valid"] = body.require_valid
    if body.tp_run is not None:
        changes["tp_run"] = body.tp_run
    if body.tp_run_lock_pct is not None:
        changes["tp_run_lock_pct"] = body.tp_run_lock_pct
    if body.tp_run_trail_pct is not None:
        changes["tp_run_trail_pct"] = body.tp_run_trail_pct
    if body.clear_trend_exit:
        changes["trend_exit"] = None
    elif body.trend_exit:
        changes["trend_exit"] = body.trend_exit
    return _manual_call(bot.manual.modify, item_id, changes)


@app.get("/api/manual/trend")
def manual_trend(ticker: str, email: str = Depends(require_user)):
    """v4.343 — tendance de fond H4 d un actif (affichee sur le ticket d ordre libre)."""
    return bot.manual.trend_info(_norm_ticker(ticker))


@app.get("/api/asset/summary")
def asset_summary_ep(ticker: str, email: str = Depends(require_user)):
    """v4.406 — resume instantane d un actif (tendance, supports / resistances H1-H4-D1, conseil). Lecture seule."""
    t = _norm_ticker(ticker)
    if not t:
        raise HTTPException(status_code=400, detail="actif manquant")
    try:
        return bot.manual.asset_summary(t)
    except Exception as e:
        return {"ok": False, "why": f"{type(e).__name__}: {e}"}


@app.post("/api/manual/positions/{item_id}/close")
def manual_close(item_id: int, email: str = Depends(require_user)):
    return _manual_call(bot.manual.close, item_id)


# ─────────────────────────────────────────────────────────────────────────
#  v4.265 — EXPORT CSV DU SUIVI DES TRADES SPOT-ACCUM / ACCUMULATION
# ─────────────────────────────────────────────────────────────────────────
_EXPORT_STRATEGY_LABEL = {"spot_accumulation": "Spot-Accum", "accumulation": "Accumulation", "funding_contrarian": "Funding", "manual": "Manuel", "forex_scalp": "Forex scalp", "forex_swing": "Forex swing", "swing_support": "Swing Support", "trend_follow": "Tendance"}
_ROUND_TRIP_FEE_RATE = 0.0009  # 2 x 0,045 % (taker) — estimation quand les frais reels manquent


# ───────────────── v4.355 — FRAIS INTEGRES AUX STATISTIQUES (calcul unique, partout pareil) ─────────────────
# La base garde le PnL BRUT de chaque trade (piste d audit, exports). Les frais sont retires a l AGREGATION :
#   * trade LIVE avec frais reels connus (fees_real, issus des fills Hyperliquid) -> ces frais ;
#   * sinon frais estimes du bot a la fermeture (fees_paid, live) ;
#   * sinon (paper) notionnel x frais aller-retour : FEE_ROUND_TRIP_PCT (0,09 %), ou FOREX_SCALP_FEE_PCT
#     pour le scalp (qui modelise ses propres frais) ;
#   notionnel = marge x levier. Un seul interrupteur : FEES_IN_STATS.
def _fees_on():
    return bool(cfg.get("FEES_IN_STATS", 1))


def _fee_pct_for(t):
    if (t.get("strategy") or "") == "forex_scalp":
        return float(cfg.get("FOREX_SCALP_FEE_PCT", 0.089))
    return float(cfg.get("FEE_ROUND_TRIP_PCT", _ROUND_TRIP_FEE_RATE * 100))


def _trade_fee_usd(t):
    if t.get("fees_real") is not None:
        return float(t["fees_real"])
    if t.get("fees_paid") is not None:
        return float(t["fees_paid"])
    notional = (t.get("size_usd") or 0) * (t.get("leverage") or 1)
    return notional * _fee_pct_for(t) / 100


def _row_pnl(t):
    """PnL utilise par les statistiques : net de frais si FEES_IN_STATS, sinon brut."""
    g = t.get("pnl") or 0.0
    return g - _trade_fee_usd(t) if _fees_on() else g


def _fees_all_closed():
    """Frais cumules de TOUS les trades fermes (pour ajuster capital et solde affiches)."""
    try:
        return sum(_trade_fee_usd(t) for t in db.get_all_closed_trades())
    except Exception:
        return 0.0



def _resolve_tz(tz_name):
    """v4.269 — fuseau horaire de l utilisateur (transmis par le navigateur),
    UTC si inconnu ou invalide."""
    if tz_name:
        try:
            from zoneinfo import ZoneInfo
            return ZoneInfo(tz_name), tz_name
        except Exception:
            pass
    return timezone.utc, "UTC"


def _local_time(iso_str, tz):
    if not iso_str:
        return ""
    try:
        return datetime.fromisoformat(iso_str).astimezone(tz).strftime("%d/%m/%Y %H:%M:%S")
    except (TypeError, ValueError):
        return ""


_SIM_LABEL = {"entree": "revenu a l entree", "sl": "SL touche", "aucun": "ni l un ni l autre (2 h)"}


def _fr(v, nd=4):
    """Nombre au format Excel francais (virgule decimale), vide si inconnu."""
    if v is None:
        return ""
    try:
        return f"{float(v):.{nd}f}".replace(".", ",")
    except (TypeError, ValueError):
        return ""


def _export_row(t, tz=timezone.utc):
    sign = 1 if t.get("action") == "LONG" else -1
    entry, exit_p = t.get("entry_price"), t.get("exit_price")
    lev = t.get("leverage") or 1
    E = t.get("size_usd")
    notional = E * lev if E else None
    try:
        opened = datetime.fromisoformat(t["created_at"])
        closed = datetime.fromisoformat(t["closed_at"]) if t.get("closed_at") else None
        duration = round((closed - opened).total_seconds() / 60, 1) if closed else None
    except (TypeError, ValueError, KeyError):
        duration = None
    move_pct = sign * (exit_p - entry) / entry * 100 if entry and exit_p else None
    fees_est = notional * _ROUND_TRIP_FEE_RATE if notional else None
    is_live = t.get("trade_mode") == "live"
    fees_used = t.get("fees_real") if t.get("fees_real") is not None else fees_est
    if is_live and t.get("pnl_real_hl") is not None and t.get("fees_real") is not None:
        pnl_net, net_src = t["pnl_real_hl"] - t["fees_real"], "reel Hyperliquid"
    elif t.get("pnl") is not None and fees_est is not None:
        pnl_net, net_src = t["pnl"] - fees_est, ("estime" if is_live else "estime (si live)")
    else:
        pnl_net, net_src = None, ""

    def after(p):
        return sign * (p - exit_p) / exit_p * 100 if p and exit_p else None
    best_60 = None
    back_to_entry = ""
    if exit_p and t.get("high_60m") is not None and t.get("low_60m") is not None:
        best_60 = (t["high_60m"] - exit_p) / exit_p * 100 if sign > 0 else (exit_p - t["low_60m"]) / exit_p * 100
        if entry and move_pct is not None and move_pct < 0:  # utile pour les pertes (SL trop serre ?)
            back_to_entry = "oui" if (t["high_60m"] >= entry if sign > 0 else t["low_60m"] <= entry) else "non"
    best_from_entry = None
    if entry and t.get("high_60m") is not None and t.get("low_60m") is not None:
        best_from_entry = (t["high_60m"] - entry) / entry * 100 if sign > 0 else (entry - t["low_60m"]) / entry * 100
    return [
        t.get("id"), _EXPORT_STRATEGY_LABEL.get(t.get("strategy"), t.get("strategy") or ""),
        t.get("coin", ""), t.get("action", ""), t.get("trade_mode") or "",
        _local_time(t.get("created_at"), tz), _local_time(t.get("closed_at"), tz), _fr(duration, 1),
        t.get("reason") or ("ouvert" if not t.get("closed_at") else ""),
        _fr(t.get("confidence"), 1), _fr(lev, 0), _fr(E, 2), _fr(notional, 2),
        _fr(entry, 6), _fr(exit_p, 6), t.get("exit_price_source") or "",
        _fr(move_pct, 3), _fr(t.get("peak_pnl_pct"), 3), _fr(t.get("pnl"), 4),
        _fr(fees_used, 4), "reels" if t.get("fees_real") is not None else (("estimes" if is_live else "estimes (si live)") if fees_used is not None else ""),
        _fr(t.get("pnl_real_hl"), 4), _fr(pnl_net, 4), net_src,
        _fr(t.get("price_after_30m"), 6), _fr(after(t.get("price_after_30m")), 3),
        _fr(t.get("price_after_60m"), 6), _fr(after(t.get("price_after_60m")), 3),
        _fr(best_60, 3), back_to_entry,
        _SIM_LABEL.get(t.get("sim_sl_075"), ""), _SIM_LABEL.get(t.get("sim_sl_100"), ""), _SIM_LABEL.get(t.get("sim_sl_150"), ""),
        _SIM_LABEL.get(t.get("sim_sl_200"), ""), _fr(t.get("sl_back_min"), 1), _fr(t.get("sl_mae_pct"), 3), _fr(t.get("sl_mark_120_pct"), 3),
        t.get("followup_status") or ("en attente" if t.get("closed_at") else ""),
        (t.get("entry_reasons") or "").replace(";", ","),
        _fr(t.get("vol_ratio"), 2), _fr(t.get("activity_ratio"), 2), _fr(t.get("flow_at_entry"), 2), _fr(t.get("spread_at_entry"), 4),
        t.get("trade_uid") or "",
        {1: "oui", 0: "non"}.get(t.get("mtf_trend_intact_at_close"), ""),  # v4.310
        # v4.342 — chemin du PnL pendant le trade + mouvement MAX depuis l ENTREE jusqu a 1 h apres la sortie
        _fr(t.get("pnl_min_pct"), 3), _fr(t.get("pnl_min_after_touch_pct"), 3), _fr(t.get("peak_at_min_after_pct"), 3),
        _fr(t.get("touch_delay_sec"), 0), {1: "oui", 0: "non (partiel)"}.get(t.get("path_complete"), ""),
        _fr(best_from_entry, 3),
    ]


_EXPORT_HEADER = [
    "id", "mode", "actif", "sens", "paper/live", "ouverture (Paris)", "fermeture (Paris)", "duree (min)",
    "motif de sortie", "confiance %", "levier", "marge E ($)", "notionnel ($)",
    "prix entree", "prix sortie", "source prix sortie", "mouvement de prix %", "pic %", "PnL brut ($)",
    "frais ($)", "type frais", "PnL Hyperliquid hors frais ($)", "PnL net ($)", "source PnL net",
    "prix +30 min", "evolution +30 min % (dans le sens du trade)",
    "prix +60 min", "evolution +60 min % (dans le sens du trade)",
    "meilleur mouvement dans l heure suivant la sortie %", "trade perdant : prix revenu a l entree dans l heure",
    "si SL a 0,75 % : issue", "si SL a 1 % : issue", "si SL a 1,5 % : issue",
    "si SL a 2 % : issue", "apres SL : minutes avant retour a l entree", "apres SL : pire recul depuis l entree avant retour %",
    "apres SL : prix 2 h apres vs entree %",
    "statut du suivi", "raisons d entree",
    "volatilite a l entree (x habitude)", "activite a l entree (x habitude)", "flux a l entree", "spread a l entree %",
    "identifiant trade",
    "tendance H4 encore intacte a la sortie (top-down)",  # v4.310
    "PnL minimal pendant le trade %", "PnL minimal APRES avoir touche +0,5 %", "pic atteint au moment de ce minimum %",
    "delai jusqu a +0,5 % (s)", "suivi complet du trade",
    "mouvement max depuis l ENTREE jusqu a 1 h apres la sortie % (dans le sens du trade)",  # v4.342
]


# ─────────────────────────────────────────────────────────────────────────
#  v4.290 — ANALYSE STATISTIQUE DES STOP LOSS (elargir le SL ? patienter ?)
# ─────────────────────────────────────────────────────────────────────────
_SL_LEVELS = (("0,75 %", "sim_sl_075", 0.75), ("1 %", "sim_sl_100", 1.0), ("1,5 %", "sim_sl_150", 1.5), ("2 %", "sim_sl_200", 2.0))


def _pctl(values, q):
    if not values:
        return None
    v = sorted(values)
    return v[min(len(v) - 1, int(round(q * (len(v) - 1))))]


def _sl_analysis(days=None):
    rows = [t for t in db.get_trades_for_export(days=days)
            if t.get("closed_at") and (t.get("reason") or "").upper().startswith(("STOP LOSS", "SL "))
            and (t.get("sim2_status") or "").startswith("ok")]
    groups = {}
    for t in rows:
        groups.setdefault(_EXPORT_STRATEGY_LABEL.get(t.get("strategy"), t.get("strategy")), []).append(t)
    if len(groups) > 1:
        groups["Tous les modes"] = rows
    out = []
    for mode, ts in groups.items():
        n = len(ts)
        backs = [t["sl_back_min"] for t in ts if t.get("sl_back_min") is not None]
        maes_back = [t["sl_mae_pct"] for t in ts if t.get("sl_back_min") is not None and t.get("sl_mae_pct") is not None]
        actual = []
        for t in ts:
            sign = 1 if t.get("action") == "LONG" else -1
            if t.get("entry_price") and t.get("exit_price"):
                actual.append(sign * (t["exit_price"] - t["entry_price"]) / t["entry_price"] * 100)
        actual_total = sum(actual)
        levels = []
        for label, col, pct in _SL_LEVELS:
            back = sum(1 for t in ts if t.get(col) == "entree")
            hit = sum(1 for t in ts if t.get(col) == "sl")
            none = [t for t in ts if t.get(col) == "aucun"]
            simulated = -pct * hit + sum(max(t.get("sl_mark_120_pct") or 0.0, -pct) for t in none)
            levels.append({"sl": label, "revenu_entree": back, "sl_touche": hit, "ni_l_un_ni_l_autre": len(none),
                           "pct_revenu_entree": round(back / n * 100, 1) if n else None,
                           "resultat_simule_pts": round(simulated, 2), "resultat_reel_pts": round(actual_total, 2),
                           "gain_estime_pts": round(simulated - actual_total, 2)})
        out.append({
            "mode": mode, "nb_sl": n,
            "retour_entree_30min_pct": round(sum(1 for b in backs if b <= 30) / n * 100, 1) if n else None,
            "retour_entree_60min_pct": round(sum(1 for b in backs if b <= 60) / n * 100, 1) if n else None,
            "retour_entree_120min_pct": round(len(backs) / n * 100, 1) if n else None,
            "delai_median_retour_min": _pctl(backs, 0.5),
            "delai_p75_retour_min": _pctl(backs, 0.75),
            "recul_median_avant_retour_pct": _pctl(maes_back, 0.5),
            "recul_p75_avant_retour_pct": _pctl(maes_back, 0.75),
            "recul_p90_avant_retour_pct": _pctl(maes_back, 0.9),
            "niveaux": levels,
        })
    return out


@app.get("/api/stats/sl-analysis")
def sl_analysis(days: Optional[int] = Query(None, ge=1, le=3650), email: str = Depends(require_user)):
    return {"days": days, "modes": _sl_analysis(days)}


@app.get("/api/export/sl-analysis.csv")
def export_sl_analysis(days: Optional[int] = Query(None, ge=1, le=3650), email: str = Depends(require_user)):
    import csv
    import io
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\r\n")
    w.writerow(["mode", "nb SL analyses", "revenu a l entree en 30 min %", "en 60 min %", "en 2 h %",
                "delai median de retour (min)", "delai 75e centile (min)",
                "pire recul median avant retour %", "75e centile %", "90e centile %",
                "SL simule", "revenu a l entree d abord", "SL simule touche", "ni l un ni l autre",
                "% revenu a l entree", "resultat reel des SL (pts de %)", "resultat simule (pts de %)", "gain estime (pts de %)"])
    for m in _sl_analysis(days):
        for lv in m["niveaux"]:
            w.writerow([m["mode"], m["nb_sl"], _fr(m["retour_entree_30min_pct"], 1), _fr(m["retour_entree_60min_pct"], 1),
                        _fr(m["retour_entree_120min_pct"], 1), _fr(m["delai_median_retour_min"], 1), _fr(m["delai_p75_retour_min"], 1),
                        _fr(m["recul_median_avant_retour_pct"], 3), _fr(m["recul_p75_avant_retour_pct"], 3), _fr(m["recul_p90_avant_retour_pct"], 3),
                        lv["sl"], lv["revenu_entree"], lv["sl_touche"], lv["ni_l_un_ni_l_autre"], _fr(lv["pct_revenu_entree"], 1),
                        _fr(lv["resultat_reel_pts"], 2), _fr(lv["resultat_simule_pts"], 2), _fr(lv["gain_estime_pts"], 2)])
    stamp = datetime.now().strftime("%Y-%m-%d_%Hh%M")
    return Response(content="\ufeff" + buf.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="analyse_stop_loss_{stamp}.csv"'})


@app.get("/api/export/accumulation-trades.csv")
def export_accumulation_trades(days: Optional[int] = Query(None, ge=1, le=3650),
                               tz: Optional[str] = Query(None, max_length=64),
                               email: str = Depends(require_user)):
    """v4.265 — Suivi complet des trades Spot-Accum, Accumulation, Funding et (v4.323) Forex, au format
    CSV lisible directement par Excel (separateur ;, virgule decimale).
    Les colonnes +30/+60 min se remplissent automatiquement environ une
    heure apres chaque fermeture (bougies Hyperliquid) ; pour le live, frais
    et PnL reels proviennent des remplissages Hyperliquid."""
    import csv
    import io
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\r\n")
    tzinfo, tz_label = _resolve_tz(tz)
    header = [h.replace("(Paris)", f"({tz_label})") for h in _EXPORT_HEADER]
    w.writerow(header)
    for t in db.get_trades_for_export(days=days):
        w.writerow(_export_row(t, tzinfo))
    stamp = datetime.now(tzinfo).strftime("%Y-%m-%d_%Hh%M")
    return Response(content="\ufeff" + buf.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="suivi_trades_{stamp}.csv"'})


@app.get("/api/stats/streaks")
def get_streaks(strategy: str = Query(..., pattern="^(funding_contrarian|spot_accumulation|accumulation|forex)$"),
                email: str = Depends(require_user)):
    """v4.316/v4.318 — SUR DEMANDE EXPLICITE : detecte la plus longue serie de
    gains consecutifs et la plus longue serie de pertes consecutives, et
    compare les conditions moyennes a l entree entre les deux — flux,
    volatilite/activite relatives, spread, score de confiance, heure UTC,
    repartition par actif. v4.318 : generalise a n importe laquelle des 3
    strategies top-down/funding (auparavant reserve a Funding Contrarian
    seul) — meme logique, un seul endpoint parametre par `strategy`."""
    trades = [t for t in db.get_all_closed_trades() if t.get("strategy") == strategy and t.get("pnl") is not None]
    trades.sort(key=lambda t: t.get("closed_at") or t.get("created_at") or "")

    streaks = []
    current = None
    for t in trades:
        is_win = _row_pnl(t) > 0          # v4.355 : gagnant = gagnant APRES frais (si FEES_IN_STATS)
        if current and current["is_win"] == is_win:
            current["trades"].append(t)
        else:
            current = {"is_win": is_win, "trades": [t]}
            streaks.append(current)
    win_streaks = [s for s in streaks if s["is_win"]]
    loss_streaks = [s for s in streaks if not s["is_win"]]
    longest_win = max(win_streaks, key=lambda s: len(s["trades"])) if win_streaks else None
    longest_loss = max(loss_streaks, key=lambda s: len(s["trades"])) if loss_streaks else None

    def summarize(streak):
        if not streak:
            return None
        rows = streak["trades"]

        # v4.326 — SUR DEMANDE EXPLICITE : nombre de trades REELLEMENT utilises
        # pour chaque moyenne (les colonnes d entree — flux, volatilite,
        # activite, spread — sont vides pour les trades ouverts avant leur
        # enregistrement : une moyenne calculee sur 1 trade sur 5 ne doit pas
        # se lire comme une moyenne de la serie entiere).
        sample_n = {}

        def avg(key):
            vals = [r.get(key) for r in rows if r.get(key) is not None]
            sample_n[key] = len(vals)
            return round(sum(vals) / len(vals), 3) if vals else None

        hour_counts = {}
        for r in rows:
            try:
                h = datetime.fromisoformat(r["created_at"]).hour
                hour_counts[h] = hour_counts.get(h, 0) + 1
            except (TypeError, ValueError, KeyError):
                pass
        coin_counts = {}
        for r in rows:
            c = r.get("coin")
            if c:
                coin_counts[c] = coin_counts.get(c, 0) + 1
        reason_counts = {}
        for r in rows:
            rs = r.get("reason") or "?"
            reason_counts[rs] = reason_counts.get(rs, 0) + 1
        return {
            "length": len(rows),
            "start": rows[0].get("created_at"),
            "end": rows[-1].get("closed_at"),
            "total_pnl": round(sum(_row_pnl(r) for r in rows), 4),
            "avg_confidence": avg("confidence"),
            "avg_flow_at_entry": avg("flow_at_entry"),
            "avg_vol_ratio": avg("vol_ratio"),
            "avg_activity_ratio": avg("activity_ratio"),
            "avg_spread_at_entry": avg("spread_at_entry"),
            "avg_peak_pnl_pct": avg("peak_pnl_pct"),
            "sample_n": sample_n,  # v4.326 : {colonne: nb de trades avec une valeur}
            "hour_distribution_utc": dict(sorted(hour_counts.items())),
            "coin_distribution": dict(sorted(coin_counts.items(), key=lambda x: -x[1])),
            "reason_distribution": reason_counts,
            "trades": [{"id": r.get("id"), "coin": r.get("coin"), "action": r.get("action"), "pnl": r.get("pnl"), "pnl_net": round(_row_pnl(r), 4),
                       "reason": r.get("reason"), "created_at": r.get("created_at"), "closed_at": r.get("closed_at")}
                      for r in rows],
        }

    return {
        "strategy": strategy,
        "total_trades_closed": len(trades),
        "n_win_streaks": len(win_streaks),
        "n_loss_streaks": len(loss_streaks),
        "longest_win_streak": summarize(longest_win),
        "longest_loss_streak": summarize(longest_loss),
    }


@app.get("/api/stats/entry-funnel")
def get_entry_funnel(days: int = Query(7, ge=1, le=30), email: str = Depends(require_user)):
    """v4.330 — SUR DEMANDE EXPLICITE : entonnoir d entree du top-down, par jour
    UTC. Chaque compte = un actif sur une fenetre de 15 min ou l etape a ete
    atteinte au moins une fois (alimente par bot_engine._funnel_note, lecture
    seule). Etapes : eval, tendance, zone, signal, signal_flux (zone + signal
    M15 + flux favorable ou non mesure), h1, candidat ; et nt_* = memes
    alignements mesures quand la tendance H4 bloquait."""
    today = datetime.now(timezone.utc).date()
    out = {}
    for m in ("spot_accumulation", "accumulation"):
        rows = []
        for i in range(days):
            d = (today - timedelta(days=i)).isoformat()
            raw = db.get_meta(f"funnel:{d}:{m}")
            try:
                cnt = json.loads(raw) if raw else {}
            except (TypeError, ValueError):
                cnt = {}
            try:
                ass = json.loads(db.get_meta(f"funnel_assets:{d}:{m}") or "{}")
            except (TypeError, ValueError):
                ass = {}
            try:
                rrs = json.loads(db.get_meta(f"funnel_rr:{d}:{m}") or "[]")
            except (TypeError, ValueError):
                rrs = []
            rows.append({"day": d, "counts": cnt, "assets": ass, "rr": rrs})
        out[m] = rows
    return {"days": days, "modes": out}


@app.get("/api/stats/blocked-signals")
def get_blocked_signals(days: int = Query(7, ge=1, le=30), email: str = Depends(require_user)):
    """v4.338 — SUR DEMANDE EXPLICITE : que devient un signal BLOQUE par un filtre ?
    Pour chaque signal complet (zone + signal M15) arrete par un filtre, on regarde
    2 h plus tard (bougies M15) si le prix a touche +1R avant le SL, le SL d abord,
    l objectif, ou ni l un ni l autre — avec SL/objectif hypothetiques identiques
    aux vrais trades. La ligne "candidat" (trades reellement pris) sert de reference.
    Alimente par bot_engine._blocked_note / _blocked_followups_if_due (lecture seule)."""
    today = datetime.now(timezone.utc).date()
    out = {}
    for m in ("spot_accumulation", "accumulation", "funding_contrarian"):   # v4.346 : + Funding
        gates = {}
        for i in range(days):
            d = (today - timedelta(days=i)).isoformat()
            try:
                agg = json.loads(db.get_meta(f"blocked_agg:{d}:{m}") or "{}")
            except (TypeError, ValueError):
                agg = {}
            for g, v in agg.items():
                t = gates.setdefault(g, {"n": 0, "sl": 0, "r1": 0, "tp": 0, "none": 0, "amb": 0,
                                         "mfe": [], "mae": [], "fin": [], "assets": {}})
                for k in ("n", "sl", "r1", "tp", "none", "amb"):
                    t[k] += v.get(k, 0)
                for k in ("mfe", "mae", "fin"):
                    t[k] += v.get(k, [])
                for e in v.get("ev", []):
                    t["assets"].setdefault(e["t"], 0)
                    t["assets"][e["t"]] += 1

        def med(a):
            a = sorted(a)
            return round(a[len(a) // 2], 3) if a else None

        out[m] = {g: {"n": t["n"], "sl": t["sl"], "r1": t["r1"], "tp": t["tp"], "none": t["none"], "amb": t["amb"],
                      "mfe_med": med(t["mfe"]), "mae_med": med(t["mae"]), "fin_med": med(t["fin"]),
                      "assets": sorted(t["assets"], key=lambda a: -t["assets"][a])}
                  for g, t in gates.items()}
    try:
        pending = len(json.loads(db.get_meta("blocked_pending") or "[]"))
    except (TypeError, ValueError):
        pending = 0
    return {"days": days, "modes": out, "pending": pending}


# ───────────────── v4.345 — TABLEAU DE BORD D OBJECTIF (lecture seule) ─────────────────
# Sur demande : l objectif est d avoir plus de trades POSITIFS, au sens du resultat net. La part de
# trades positifs necessaire pour que la moyenne apres frais soit nulle depend de ce que gagnent les
# gagnants, de ce que perdent les perdants et des frais : necessaires = (perte + frais) / (gain + perte).
# Toutes les valeurs sont des mouvements de PRIX en % (sans levier), comme dans l export de suivi.
def _objective_group_key(t):
    strat = t.get("strategy") or ""
    label = _EXPORT_STRATEGY_LABEL.get(strat, strat or "?")
    if strat in ("spot_accumulation", "accumulation"):
        if (t.get("timeframe") or "") == "SWING_D1H4":          # v4.359 : trades du profil Swing, a part
            return f"{label} (swing)"
        r = (t.get("reason") or "").lower()
        er = (t.get("entry_reasons") or "").lower()
        td = ("top-down" in r) or ("zone opposee" in r) or ("top-down" in er)
        return f"{label} ({'top-down' if td else 'ancien moteur'})"
    return label


def _objective_stats(trades, fee_pct, takeoff_pct):
    import statistics as _st

    def med(a):
        return round(_st.median(a), 3) if a else None

    recs = []
    for t in trades:
        entry, ex = t.get("entry_price"), t.get("exit_price")
        if not entry or not ex or not t.get("closed_at"):
            continue
        sign = 1 if t.get("action") == "LONG" else -1
        notional = (t.get("size_usd") or 0) * (t.get("leverage") or 1)
        fees = _trade_fee_usd(t)
        def after(p):
            return sign * (p - ex) / ex * 100 if p else None
        recs.append({
            "g": _objective_group_key(t), "pct": sign * (ex - entry) / entry * 100,
            "peak": t.get("peak_pnl_pct") or 0.0, "usd": t.get("pnl") or 0.0, "fees": fees,
            "reason": (t.get("reason") or "?").strip(), "e30": after(t.get("price_after_30m")),
            "e60": after(t.get("price_after_60m")),
        })

    def stats(rs, with_reasons=True):
        n = len(rs)
        wins = [r for r in rs if r["pct"] > 0]
        losses = [r for r in rs if r["pct"] <= 0]
        out = {"n": n, "wins": len(wins), "win_rate": round(len(wins) / n * 100, 1),
               "mean_pct": round(sum(r["pct"] for r in rs) / n, 3), "small": n < 30}
        aw = sum(r["pct"] for r in wins) / len(wins) if wins else None
        al = -sum(r["pct"] for r in losses) / len(losses) if losses else None
        out["avg_win"] = round(aw, 3) if aw is not None else None
        out["avg_loss"] = round(-al, 3) if al is not None else None
        if aw is not None and al is not None and (aw + al) > 0:
            needed = (al + fee_pct) / (aw + al) * 100
            out["needed"] = round(needed, 1)
            out["unreachable"] = needed > 100     # les frais depassent le gain moyen : aucun taux de reussite ne suffit
            out["gap"] = round(out["win_rate"] - needed, 1)
            out["value_per_point"] = round((aw + al) / 100, 4)
        else:
            out["needed"] = out["gap"] = out["value_per_point"] = None
        out["net_pct"] = round(out["mean_pct"] - fee_pct, 3)
        out["takeoff"] = round(sum(1 for r in rs if r["peak"] >= takeoff_pct) / n * 100, 1)
        out["dead_losers"] = (round(sum(1 for r in losses if r["peak"] < takeoff_pct) / len(losses) * 100, 1) if losses else None)
        cap = [min(r["pct"] / r["peak"], 1.0) for r in wins if r["peak"] > 0.2]
        out["capture"] = round(_st.median(cap) * 100, 0) if cap else None
        e30 = [r["e30"] for r in wins if r["e30"] is not None]
        e60 = [r["e60"] for r in wins if r["e60"] is not None]
        out["after30"], out["after60"] = med(e30), med(e60)
        out["after60_up"] = round(sum(1 for e in e60 if e > 0) / len(e60) * 100, 0) if e60 else None
        out["after_n"] = len(e60)
        out["usd_gross"] = round(sum(r["usd"] for r in rs), 3)
        out["usd_net"] = round(sum(r["usd"] - r["fees"] for r in rs), 3)
        if with_reasons:
            by = {}
            for r in rs:
                by.setdefault(r["reason"], []).append(r)
            rows = [{"reason": k, "n": len(v), "mean_pct": round(sum(x["pct"] for x in v) / len(v), 3),
                     "usd": round(sum(x["usd"] for x in v), 3)} for k, v in by.items()]
            out["reasons"] = sorted(rows, key=lambda x: -abs(x["usd"]))[:8]
        return out

    groups = {}
    for r in recs:
        groups.setdefault(r["g"], []).append(r)
    res = [{"group": g, **stats(rs)} for g, rs in sorted(groups.items(), key=lambda kv: -len(kv[1]))]
    total = {"group": "Tous modes", **stats(recs, with_reasons=False)} if recs else None
    return res, total


@app.get("/api/stats/objective")
def get_objective(days: int = Query(7, ge=0, le=365), where: str = Query("all", pattern="^(all|paper|live)$"),
                  takeoff: float = Query(0.5, ge=0.05, le=5.0), email: str = Depends(require_user)):
    """v4.345 — tableau de bord d objectif : par mode, la part de trades positifs comparee a celle
    necessaire pour un resultat net nul, le taux de decollage (pic >= seuil), la part du pic conservee
    et l evolution du prix apres la sortie d un gagnant. Lecture seule, independant du point de
    reinitialisation des statistiques (la periode est choisie explicitement). days=0 : tout."""
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat() if days else None
    trades = db.get_all_closed_trades(since=since)
    if where != "all":
        trades = [t for t in trades if (t.get("trade_mode") or "paper") == where]
    fee_pct = float(cfg.get("FEE_ROUND_TRIP_PCT", _ROUND_TRIP_FEE_RATE * 100))      # v4.355 : un seul reglage de frais
    fee_src = f"reglage Frais aller-retour ({fee_pct} %) ; frais reels des trades live quand ils sont connus"
    groups, total = _objective_stats(trades, fee_pct, takeoff)
    return {"days": days, "where": where, "takeoff": takeoff, "fee_pct": fee_pct, "fee_source": fee_src,
            "groups": groups, "total": total}


# ───────────────── v4.347 — REJEU HISTORIQUE DES ENTREES (lecture seule) ─────────────────
_replay_job = rr.ReplayJob()


def _replay_fee_pct():
    """Frais aller-retour : mediane des frais reels des trades live si >= 20, sinon estimation."""
    fee = _ROUND_TRIP_FEE_RATE * 100
    try:
        real = [t["fees_real"] / ((t.get("size_usd") or 0) * (t.get("leverage") or 1)) * 100
                for t in db.get_all_closed_trades()
                if t.get("fees_real") is not None and (t.get("size_usd") or 0) > 0 and t.get("trade_mode") == "live"]
        if len(real) >= 20:
            real.sort()
            fee = round(real[len(real) // 2], 4)
    except Exception:
        pass
    return fee


@app.post("/api/research/replay/start")
def replay_start(days: int = Query(30, ge=7, le=40), email: str = Depends(require_user)):
    """Lance le rejeu historique en arriere-plan (un seul a la fois). Ne passe aucun ordre, ne modifie
    aucun reglage. Telecharge des bougies publiques a debit bride (~6 minutes pour 29 actifs)."""
    assets = sorted({be.ticker_from_slot_key(s) for s in bot.cfg.get("SYMBOLS", [])})
    assets = [a for a in assets if not a.startswith("xyz:")]

    def done(res):
        try:
            db.set_meta("replay_last", json.dumps({"saved_at": time.time(), "result": res}))
        except Exception as e:
            print(f"[REJEU] sauvegarde impossible : {e}")

    started = _replay_job.start(bot.info, assets, days, _replay_fee_pct(), on_done=done)
    return {"started": started, "assets": len(assets), "days": days}


@app.post("/api/research/replay/cancel")
def replay_cancel(email: str = Depends(require_user)):
    _replay_job.cancel()
    return {"ok": True}


@app.get("/api/research/replay")
def replay_status(email: str = Depends(require_user)):
    """Etat du rejeu en cours, ou dernier resultat sauvegarde (survit a un redemarrage)."""
    st = _replay_job.status(with_result=True)
    if st["state"] == "idle":
        try:
            saved = json.loads(db.get_meta("replay_last") or "null")
        except (TypeError, ValueError):
            saved = None
        if saved:
            return {"state": "saved", "saved_at": saved.get("saved_at"), "result": saved.get("result"), "progress": st["progress"]}
    return st


# ───────────────── v4.348 — PROFIL D ACTIVITE (Spot-Accum / Accumulation) ─────────────────
# Sur demande : une configuration en un clic qui fait trader ces deux modes davantage, avec une taille
# reduite, SANS passer par de nouvelles statistiques. Les reglages precedents sont sauvegardes pour un retour
# en un clic. Ne concerne que le top-down (Spot-Accum / Accumulation) ; Funding et Forex ne sont pas touches.
_PROFILES = {
    "actif": {
        "label": "Actif prudent",
        "values": {
            "MTF_REQUIRE_REAL_TARGET_ZONE": 0,   # entrer meme sans zone opposee distincte : objectif synthetique a MTF_MIN_RR x le risque, + breakeven et verrou
            "MTF_RISK_PCT": 0.3,                 # 0,5 -> 0,3 % du capital risque par trade (plus de trades, plus petits)
            "SPOT_ACCUM_TREND_MODE": 1,          # longs aussi en tendance neutre (refus si structure H4 opposee)
            "ACCUMULATION_TREND_MODE": 2,        # shorts sur resistance H4 meme en tendance haussiere
        },
    },
}


_PROFILES["scalp"] = {
    "label": "Scalp Forex (paper)",
    "free": ["FOREX_SCALP_FEE_PCT", "FOREX_SCALP_MIN_NET_RR", "FOREX_SCALP_NOTIONAL_USD", "FOREX_SCALP_MAX_TRADES"],
    "values": {
        "FOREX_SCALP_ENABLED": 1,
        "FOREX_SCALP_NOTIONAL_USD": 30.0,
        "FOREX_SCALP_MAX_TRADES": 2,
        "FOREX_SCALP_MAX_HOLD_MIN": 20,
        "FOREX_SCALP_FEE_PCT": 0.089,
        "FOREX_SCALP_SLIPPAGE_PCT": 0.005,
        "FOREX_SCALP_MIN_NET_RR": 0.3,
        "FOREX_SCALP_MIN_TARGET_PCT": 0.15,
        "FOREX_SCALP_DAILY_LOSS_USD": 1.0,
    },
}
_PROFILES["swing"] = {
    "label": "Swing (tendance Daily, figures H4)",
    "free": ["MTF_REQUIRE_FLOW_CONFIRM", "MTF_MIN_FLOW_PRESSURE", "MTF_ZONE_MAX_WIDTH_ATR"],   # reglables a la main sans que le profil passe a "non applique"
    "values": {
        "MTF_USE_DAILY": 1,                    # tendance de fond ET zones sur bougies JOURNALIERES
        "MTF_USE_H4_LOWER": 1,                 # signal de retournement sur bougies H4
        "MTF_DOUBLE_PATTERN_ENABLED": 1,       # + double creux / double sommet
        "MTF_SIGNAL_MAX_AGE_SEC": 1800,        # entree dans les 30 min qui suivent la cloture H4 (prise de position chirurgicale)
        "MTF_TREND_PATH_ENABLED": 0,           # pas de chemin "mesure de mouvement" 1h
        "MTF_REQUIRE_H1_STRUCTURE": 0,         # la structure H1 n a plus de sens a cette echelle
        "MTF_REQUIRE_FLOW_CONFIRM": 1,         # flux de 3 min : VETO doux seulement (voir MTF_MIN_FLOW_PRESSURE)
        "MTF_MIN_FLOW_PRESSURE": -0.4,         # refus seulement si le flux est nettement contraire ; donnee absente = on laisse passer
        "MTF_REQUIRE_REAL_TARGET_ZONE": 1,     # objectif = vraie zone Daily opposee, sinon pas de trade
        "MTF_MIN_RR": 2.0,                     # gain/risque d au moins 2
        "MTF_MAX_RISK_PCT": 10.0,              # stop jusqu a 10 % du prix (stops larges, positions petites)
        "MTF_RISK_PCT": 0.5,                   # 0,5 % du capital risque par trade
        "SPOT_ACCUM_MAX_TRADES": 3,
        "ACCUMULATION_MAX_TRADES": 2,
        "SPOT_ACCUM_TREND_MODE": 0,            # long : tendance Daily haussiere ou repli
        "ACCUMULATION_TREND_MODE": 0,          # short : tendance Daily baissiere ou rebond
    },
}


def _profile_current(name):
    return {k: cfg.get(k, ADVANCED_SETTINGS[k]["default"]) for k in _PROFILES[name]["values"]}


def _profile_active(name):
    cur = _profile_current(name)
    free = set(_PROFILES[name].get("free", []))
    return all(abs(float(cur[k]) - float(v)) < 1e-9 for k, v in _PROFILES[name]["values"].items() if k not in free)


@app.get("/api/config/profile")
def get_profile(name: str = Query("actif", pattern="^(actif|swing|scalp)$"), email: str = Depends(require_user)):
    prof = _PROFILES[name]
    try:
        prev = json.loads(db.get_meta(f"profile_prev:{name}") or "null")
    except (TypeError, ValueError):
        prev = None
    # v4.348 — passage au reel : etat des deux modes et capacite reelle du compte (marge au levier x1)
    live_equity = getattr(bot, "live_equity_real", None)
    cap = float(cfg.get("MTF_MAX_NOTIONAL_USD", 30.0))
    risk = float(cfg.get("MTF_RISK_PCT", 0.5))
    typical_stop = 2.0            # % du prix : ordre de grandeur des stops top-down observes (1,9 a 2,8 %)
    typical = min(cap, live_equity * risk / typical_stop) if live_equity else None
    live = {"equity": live_equity, "cap": cap, "risk_pct": risk, "typical_stop_pct": typical_stop,
            "typical_notional": round(typical, 1) if typical else None,
            "positions_by_margin": int(live_equity // typical) if (live_equity and typical) else None,
            "hl_configured": bool(cfg.get("WALLET_ADDRESS") and cfg.get("PRIVATE_KEY")),
            "funding_live": bool(bot._effective_mode("funding_contrarian") == "live" and cfg.get("FUNDING_MODE_LIVE_ALLOWED"))}
    modes = {sname: bot._effective_mode(sname) for sname in ("spot_accumulation", "accumulation")}
    return {"name": name, "label": prof["label"], "values": prof["values"], "current": _profile_current(name),
            "active": _profile_active(name), "previous": prev,
            "labels": {k: ADVANCED_SETTINGS[k]["label"] for k in prof["values"]},
            "modes": modes, "live": live}


@app.post("/api/config/profile/apply")
def apply_profile(name: str = Query("actif", pattern="^(actif|swing|scalp)$"), email: str = Depends(require_user)):
    prof = _PROFILES[name]
    if not _profile_active(name):
        db.set_meta(f"profile_prev:{name}", json.dumps({"saved_at": time.time(), "values": _profile_current(name)}))
    applied = {}
    for k, v in prof["values"].items():
        ok, clean = _coerce_advanced_value(k, v)
        if not ok:
            raise HTTPException(status_code=400, detail=f"{k} : {clean}")
        _apply_and_persist(k, clean)
        applied[k] = clean
    return {"ok": True, "applied": applied}


@app.post("/api/config/profile/restore")
def restore_profile(name: str = Query("actif", pattern="^(actif|swing|scalp)$"), email: str = Depends(require_user)):
    try:
        prev = json.loads(db.get_meta(f"profile_prev:{name}") or "null")
    except (TypeError, ValueError):
        prev = None
    vals = (prev or {}).get("values") or {k: ADVANCED_SETTINGS[k]["default"] for k in _PROFILES[name]["values"]}
    restored = {}
    for k, v in vals.items():
        if k not in ADVANCED_SETTINGS:
            continue
        ok, clean = _coerce_advanced_value(k, v)
        if ok:
            _apply_and_persist(k, clean)
            restored[k] = clean
    return {"ok": True, "restored": restored, "from_saved": bool(prev)}


# ───────────────── v4.349 — ENREGISTREUR DE FLUX + FICHE ACTIF (lecture seule) ─────────────────
@app.get("/api/research/flow/status")
def flow_status_ep(email: str = Depends(require_user)):
    """Etat de l enregistreur de flux acheteur/vendeur (tranches de 5 minutes par actif)."""
    now = time.time()
    st = db.flow_status(int(now * 1000))
    rec = getattr(bot, "flow_recorder", None)
    live = None
    if rec is not None:
        live = {"trades_seen": rec.stats["trades"], "dupes": rec.stats["dupes"], "bad": rec.stats["bad"],
                "open_buckets": len(rec.cur), "pending_rows": len(rec.closed)}
    return {"enabled": bool(bot.cfg.get("FLOW_RECORDER_ENABLED", 1)), "ws_alive": (now - getattr(bot, "_ws_trades_last_any", 0)) < 90,
            "keep_days": bot.cfg.get("FLOW_RECORDER_KEEP_DAYS", 120), "big_usd": bot.cfg.get("FLOW_RECORDER_BIG_TRADE_USD", 5000.0),
            "live": live, "db": st, "now_ms": int(now * 1000)}


@app.get("/api/research/flow/export.csv")
def flow_export_ep(days: int = Query(7, ge=1, le=120), what: str = Query("bars", pattern="^(bars|alive)$"),
                   email: str = Depends(require_user)):
    """Export CSV des tranches de flux (what=bars) ou des tranches ou l enregistreur etait actif (what=alive)."""
    import csv
    import io
    since = int((time.time() - days * 86400) * 1000)
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\r\n")
    fr = lambda v, d=2: "" if v is None else (str(round(v, d)).replace(".", ",") if isinstance(v, float) else v)
    iso = lambda ms: datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
    if what == "alive":
        w.writerow(["debut tranche (UTC)", "t0 (ms)"])
        with db._lock, db._connect() as conn:
            for r in conn.execute("SELECT t0 FROM flow_5m_alive WHERE t0 >= ? ORDER BY t0", (since,)).fetchall():
                w.writerow([iso(r["t0"]), r["t0"]])
    else:
        w.writerow(["actif", "debut tranche (UTC)", "t0 (ms)", "nb achats", "nb ventes", "achats ($)", "ventes ($)", "gros achats ($)",
                    "gros ventes ($)", "plus grosse transaction ($)", "prix ouverture", "prix haut", "prix bas", "prix cloture",
                    "desequilibre (achats-ventes)/(achats+ventes)"])
        for r in db.flow_export_rows(since):
            tot = (r[4] or 0) + (r[5] or 0)
            w.writerow([r[0], iso(r[1]), r[1], r[2], r[3], fr(r[4]), fr(r[5]), fr(r[6]), fr(r[7]), fr(r[8]),
                        fr(r[9], 8), fr(r[10], 8), fr(r[11], 8), fr(r[12], 8), fr(((r[4] or 0) - (r[5] or 0)) / tot, 4) if tot else ""])
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%Hh%M")
    return Response(content="\ufeff" + buf.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="flux_{what}_{stamp}.csv"'})


_cards_job = ac.CardsJob()


def _asset_bot_history():
    """Resultats reels du bot par actif (tous modes, toute la base) : taille de l echantillon incluse."""
    out = {}
    for t in db.get_all_closed_trades():
        coin, entry, ex = t.get("coin"), t.get("entry_price"), t.get("exit_price")
        if not coin or not entry or not ex:
            continue
        sign = 1 if t.get("action") == "LONG" else -1
        pct = sign * (ex - entry) / entry * 100
        e = out.setdefault(coin, {"n": 0, "wins": 0, "usd": 0.0, "sum_pct": 0.0, "take": 0, "groups": {}, "last": None})
        e["n"] += 1
        e["wins"] += 1 if pct > 0 else 0
        e["usd"] += t.get("pnl") or 0.0
        e["fees"] = e.get("fees", 0.0) + _trade_fee_usd(t)          # v4.355
        e["sum_pct"] += pct
        e["take"] += 1 if (t.get("peak_pnl_pct") or 0) >= 0.5 else 0
        g = e["groups"].setdefault(_objective_group_key(t), {"n": 0, "wins": 0, "usd": 0.0})
        g["n"] += 1
        g["wins"] += 1 if pct > 0 else 0
        g["usd"] += t.get("pnl") or 0.0
        ca = t.get("closed_at")
        if ca and (e["last"] is None or ca > e["last"]):
            e["last"] = ca
    res = {}
    for coin, e in out.items():
        n = e["n"]
        res[coin] = {"n": n, "win_rate": round(e["wins"] / n * 100, 0), "usd": round(e["usd"], 2),
                     "usd_net": round(e["usd"] - e["fees"], 2), "fees": round(e["fees"], 2), "fees_on": _fees_on(),
                     "mean_pct": round(e["sum_pct"] / n, 3), "takeoff": round(e["take"] / n * 100, 0), "small": n < 30,
                     "last": e["last"], "groups": sorted(({"group": g, "n": v["n"], "win_rate": round(v["wins"] / v["n"] * 100, 0),
                                                           "usd": round(v["usd"], 2)} for g, v in e["groups"].items()),
                                                         key=lambda x: -x["n"])[:3]}
    return res


@app.post("/api/research/cards/start")
def cards_start(days: int = Query(30, ge=14, le=40), email: str = Depends(require_user)):
    """Calcule les fiches actifs a partir des bougies (arriere-plan, ~6 minutes pour 29 actifs, debit bride)."""
    assets = sorted({be.ticker_from_slot_key(s) for s in bot.cfg.get("SYMBOLS", [])})
    assets = [a for a in assets if not a.startswith("xyz:")]

    def done(res):
        try:
            db.set_meta("cards_last", json.dumps({"saved_at": time.time(), "result": res}))
        except Exception as e:
            print(f"[FICHES] sauvegarde impossible : {e}")

    return {"started": _cards_job.start(bot.info, assets, days, on_done=done), "assets": len(assets), "days": days}


@app.post("/api/research/cards/cancel")
def cards_cancel(email: str = Depends(require_user)):
    _cards_job.cancel()
    return {"ok": True}


@app.get("/api/research/cards")
def cards_status(email: str = Depends(require_user)):
    """Etat du calcul, derniere fiche enregistree, et resultats du bot par actif (toujours a jour)."""
    st = _cards_job.status(with_result=True)
    out = {"state": st["state"], "progress": st["progress"], "error": st["error"], "history": _asset_bot_history()}
    if st["state"] == "done":
        out["result"] = st["result"]
    elif st["state"] == "idle":
        try:
            saved = json.loads(db.get_meta("cards_last") or "null")
        except (TypeError, ValueError):
            saved = None
        if saved:
            out.update({"state": "saved", "saved_at": saved.get("saved_at"), "result": saved.get("result")})
    return out


# ───────────────── v4.353 — SCALP FOREX : diagnostic, etat, statistiques du journal, export ─────────────────
def _scalp_diag_line(ticker, state):
    """Ligne du diagnostic par actif (None si le scalp n est pas concerne)."""
    try:
        if not cfg.get("FOREX_SCALP_ENABLED", 0) or ticker not in cfg.get("FOREX_SCALP_SYMBOLS", ["xyz:EUR", "xyz:JPY", "xyz:KRW", "PAXG"]):
            return None
        sn = getattr(state, "scalp_snapshot", None)
        if not sn:
            return "pas encore evalue"
        parts = [sn.get("blocker") or "setup valide ce cycle (voir le journal)"]
        if sn.get("bias"):
            parts.append(f"biais 15 min {sn['bias']}")
        if sn.get("levels"):
            parts.append("niveaux proches : " + " ; ".join(f"{p:g} {k} (w{w})" for p, k, w in sn["levels"]))
        return " · ".join(parts)
    except Exception as e:
        return f"indisponible ({e})"


def _supsw_diag_line(ticker, state):
    """Ligne du diagnostic par actif pour le Swing Support (None si non concerne)."""
    try:
        if not cfg.get("SUPSW_ENABLED", 0) or ticker not in cfg.get("SUPSW_SYMBOLS", ["BTC", "ETH", "HYPE", "TAO", "SUI"]):
            return None
        sn = getattr(state, "supsw_snapshot", None)
        if not sn:
            return "pas encore evalue"
        parts = [sn.get("blocker") or "setup valide ce cycle (voir le journal)"]
        if sn.get("trend"):
            parts.append(f"tendance Daily {sn['trend']}")
        if sn.get("support"):
            parts.append(f"support {sn['support']}")
        return " · ".join(parts)
    except Exception as e:
        return f"indisponible ({e})"


def _trendf_diag_line(ticker, state):
    """Ligne du diagnostic par actif pour le module Tendance (None si non concerne)."""
    try:
        if not cfg.get("TRENDF_ENABLED", 0) or ticker not in cfg.get("TRENDF_SYMBOLS", ["BTC", "ETH", "HYPE", "TAO", "SUI", "PAXG", "xyz:EUR", "xyz:JPY"]):
            return None
        sn = getattr(state, "trendf_snapshot", None)
        if not sn:
            return "pas encore evalue"
        parts = [sn.get("blocker") or "tendance confirmee, entree evaluee ce cycle (voir le journal)"]
        if sn.get("flow") is not None:
            parts.append(f"flux {sn['flow']:+.2f}")
        if sn.get("ext") is not None:
            parts.append(f"retard {sn['ext']:.1f} ATR Daily")
        return " · ".join(parts)
    except Exception as e:
        return f"indisponible ({e})"


def _fxswing_diag_line(ticker, state):
    """Ligne du diagnostic par actif pour le Swing Forex (None si non concerne)."""
    try:
        if not cfg.get("FXSWING_ENABLED", 0) or ticker not in cfg.get("FXSWING_SYMBOLS", ["xyz:EUR", "xyz:JPY", "xyz:KRW", "xyz:DXY", "PAXG"]):
            return None
        sn = getattr(state, "fxswing_snapshot", None)
        if not sn:
            return "pas encore evalue"
        parts = [sn.get("blocker") or "setup valide ce cycle (voir le journal)"]
        if sn.get("trend"):
            parts.append(f"tendance Daily {sn['trend']}")
        if sn.get("support") or sn.get("resistance"):
            parts.append(f"support {sn.get('support') or 'aucun'} / resistance {sn.get('resistance') or 'aucune'}")
        return " · ".join(parts)
    except Exception as e:
        return f"indisponible ({e})"


def _scalp_stats(events, fee_default=0.089):
    """Agregats du journal : par decision / setup / type de niveau / biais / heure, avec l issue mesuree 60 min plus tard
    et le gain net ESTIME (frais et spread deduits) si le setup avait ete pris."""
    import statistics as _st

    def est(e):
        """gain net estime (en % du prix) d un setup evalue : objectif -> +gain, stop -> -risque, sinon rendement a 15 min."""
        if not e.get("evaluated") or e.get("outcome") is None or e.get("risk_pct") is None:
            return None
        cost = e.get("cost_pct") if e.get("cost_pct") is not None else fee_default
        if e["outcome"] == "tp":
            g = e.get("tp_pct") or 0.0
        elif e["outcome"] == "sl":
            g = -(e.get("risk_pct") or 0.0)
        else:
            g = e.get("fin15") if e.get("fin15") is not None else 0.0
        return g - cost

    def agg(rows):
        ev = [r for r in rows if r.get("evaluated") and r.get("outcome")]
        n_ev = len(ev)
        ests = [x for x in (est(r) for r in ev) if x is not None]
        return {"n": len(rows), "evaluated": n_ev,
                "tp_pct": round(sum(1 for r in ev if r["outcome"] == "tp") / n_ev * 100, 1) if n_ev else None,
                "sl_pct": round(sum(1 for r in ev if r["outcome"] == "sl") / n_ev * 100, 1) if n_ev else None,
                "none_pct": round(sum(1 for r in ev if r["outcome"] == "none") / n_ev * 100, 1) if n_ev else None,
                "mfe15": round(_st.median([r["mfe15"] for r in ev if r.get("mfe15") is not None]), 3) if ev and any(r.get("mfe15") is not None for r in ev) else None,
                "mae15": round(_st.median([r["mae15"] for r in ev if r.get("mae15") is not None]), 3) if ev and any(r.get("mae15") is not None for r in ev) else None,
                "net_est": round(sum(ests) / len(ests), 4) if ests else None, "net_n": len(ests)}

    def group(keyfn):
        d = {}
        for r in events:
            d.setdefault(keyfn(r), []).append(r)
        return sorted(({"key": k, **agg(v)} for k, v in d.items()), key=lambda x: -x["n"])

    def dec(r):
        d = r.get("decision") or "?"
        return "pris" if d == "pris" else (d.replace("filtre : ", "")[:46])
    return {"total": agg(events),
            "by_decision": group(dec),
            "by_setup": group(lambda r: r.get("setup") or "?"),
            "by_level": group(lambda r: (r.get("level_kinds") or "?").split("+")[0].replace(" x", " x")[:18]),
            "by_bias": group(lambda r: r.get("bias") or "?"),
            "by_side": group(lambda r: r.get("side") or "?"),
            "by_hour": group(lambda r: f"{(r.get('hour_utc') or 0) // 6 * 6:02d}-{(r.get('hour_utc') or 0) // 6 * 6 + 6:02d} h UTC"),
            "by_asset": group(lambda r: r.get("asset") or "?")}


@app.get("/api/scalp/status")
def scalp_status(email: str = Depends(require_user)):
    """Etat du scalp : reglages, positions ouvertes, resultats du jour et du total, derniers setups."""
    open_pos = []
    for sk, st in list(bot.states.items()):
        pos = st.position
        if pos and pos.get("strategy") == "forex_scalp":
            tk = be.ticker_from_slot_key(sk)
            px = float((bot.all_mids or {}).get(tk) or 0)
            sc = pos.get("scalp") or {}
            gain = ((px - pos["entry"]) if pos["type"] == "long" else (pos["entry"] - px)) / pos["entry"] * 100 if px else None
            open_pos.append({"asset": tk, "side": pos["type"], "entry": pos["entry"], "sl": pos["sl"], "tp": pos["tp"], "price": px,
                             "gain_pct": round(gain, 3) if gain is not None else None, "setup": sc.get("setup"),
                             "age_min": round((time.time() - sc.get("opened_ts", time.time())) / 60, 1), "be_done": bool(sc.get("be_done"))})
    trades = [t for t in db.get_all_closed_trades() if t.get("strategy") == "forex_scalp"]
    day0 = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    def summ(ts):
        w = [t for t in ts if (t.get("pnl") or 0) > 0]
        return {"n": len(ts), "wins": len(w), "net": round(sum((t.get("pnl") or 0) for t in ts), 3)}
    fees = sum(_trade_fee_usd(t) for t in trades)          # v4.355 : memes frais que partout (scalp : FOREX_SCALP_FEE_PCT)
    since = int((time.time() - 3 * 86400) * 1000)
    recent = db.scalp_events_since(since, limit=60)
    keys = ("FOREX_SCALP_ENABLED", "FOREX_SCALP_NOTIONAL_USD", "FOREX_SCALP_MAX_TRADES", "FOREX_SCALP_MAX_HOLD_MIN", "FOREX_SCALP_FEE_PCT",
            "FOREX_SCALP_SLIPPAGE_PCT", "FOREX_SCALP_MIN_NET_RR", "FOREX_SCALP_MIN_TARGET_PCT", "FOREX_SCALP_DAILY_LOSS_USD")
    return {"enabled": bool(cfg.get("FOREX_SCALP_ENABLED", 0)), "forex_open": be.is_forex_open(),
            "symbols": cfg.get("FOREX_SCALP_SYMBOLS", ["xyz:EUR", "xyz:JPY", "xyz:KRW", "PAXG"]),
            "settings": {k: cfg.get(k, ADVANCED_SETTINGS[k]["default"]) for k in keys},
            "open": open_pos, "today": summ([t for t in trades if (t.get("closed_at") or "") >= day0]), "total": summ(trades),
            "fees_estimated": round(fees, 3), "net_after_fees_est": round(sum((t.get("pnl") or 0) for t in trades) - fees, 3),
            "recent": [{k: e.get(k) for k in ("ts", "asset", "side", "setup", "level_kinds", "bias", "decision", "risk_pct", "tp_pct", "net_rr", "outcome", "evaluated")} for e in recent]}


@app.get("/api/scalp/stats")
def scalp_stats_ep(days: int = Query(7, ge=1, le=60), email: str = Depends(require_user)):
    """Statistiques du journal : ce que valent les setups pris ET filtres, par decision, setup, niveau, biais, heure, actif."""
    ev = db.scalp_events_since(int((time.time() - days * 86400) * 1000), limit=20000)
    return {"days": days, **_scalp_stats(ev, cfg.get("FOREX_SCALP_FEE_PCT", 0.089))}


@app.get("/api/scalp/journal/export.csv")
def scalp_export(days: int = Query(7, ge=1, le=60), email: str = Depends(require_user)):
    import csv
    import io
    ev = list(reversed(db.scalp_events_since(int((time.time() - days * 86400) * 1000), limit=50000)))
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\r\n")
    cols = ["ts", "asset", "side", "setup", "level_kinds", "level_price", "level_w", "price", "spread_pct", "atr5_pct", "bias", "flow", "hour_utc",
            "decision", "sl", "tp", "risk_pct", "tp_pct", "rr", "net_rr", "cost_pct", "hypothetical", "trade_uid", "evaluated", "outcome",
            "hit_min", "mfe5", "mae5", "mfe15", "mae15", "mfe60", "mae60", "fin15", "fin60"]
    w.writerow(["heure (UTC)"] + cols[1:])
    for e in ev:
        row = [datetime.fromtimestamp(e["ts"] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")]
        for c in cols[1:]:
            v = e.get(c)
            row.append("" if v is None else (str(round(v, 6)).replace(".", ",") if isinstance(v, float) else v))
        w.writerow(row)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%Hh%M")
    return Response(content="\ufeff" + buf.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="scalp_journal_{stamp}.csv"'})


# ───────────────── v4.359 — SUIVI DU SWING : positions, trades fermes, journal des signaux ─────────────────
_SWING_TAG = "SWING_D1H4"


def _swing_is_active():
    return bool(cfg.get("MTF_USE_DAILY", 0) and cfg.get("MTF_USE_H4_LOWER", 0))


@app.get("/api/swing/status")
def swing_status(email: str = Depends(require_user)):
    """Etat du Swing : reglages, positions ouvertes (en R), trades fermes (net de frais, en R), signaux recents."""
    open_pos = []
    for pool in (bot.states, bot.accum_states):
        for sk, st in list(pool.items()):
            pos = st.position
            if not (pos and pos.get("mtf_swing")):
                continue
            tk = be.ticker_from_slot_key(sk)
            px = float((bot.all_mids or {}).get(tk) or 0) or pos["entry"]
            sgn = 1 if pos["type"] == "long" else -1
            r_units = pos.get("mtf_r") or 0
            gain = sgn * (px - pos["entry"])
            info = pos.get("swing_info") or {}
            opened_ts = None
            try:        # opened_at est ecrit en heure LOCALE du serveur au format jj/mm/aaaa hh:mm:ss
                opened_ts = datetime.strptime(pos.get("opened_at", ""), "%d/%m/%Y %H:%M:%S").timestamp()
            except Exception:
                pass
            open_pos.append({
                "asset": tk, "side": pos["type"], "fx": pos.get("strategy") == "forex_swing",
                "mode": "Swing Forex" if pos.get("strategy") == "forex_swing" else ("Spot-Accum" if pos["type"] == "long" else "Accumulation"),
                "entry": pos["entry"], "price": px, "sl": pos["sl"], "tp": pos.get("tp"),
                "gain_pct": round(gain / pos["entry"] * 100, 3), "r_now": round(gain / r_units, 2) if r_units else None,
                "risk_pct": info.get("risk_pct"), "rr": info.get("rr"), "pattern": info.get("pattern"),
                "zone": info.get("zone"), "touches": info.get("touches"), "trend": info.get("trend"),
                "be_done": bool(pos.get("mtf_be_done")), "sl_pushed": bool(pos.get("mtf_sl_pushed")), "effective_mode": pos.get("effective_mode"),
                "age_h": round((time.time() - opened_ts) / 3600, 1) if opened_ts else None})
    rows = [t for t in db.get_all_closed_trades() if (t.get("timeframe") or "") == _SWING_TAG]
    closed = []
    for t in rows:
        entry, ex = t.get("entry_price"), t.get("exit_price")
        sgn = 1 if t.get("action") == "LONG" else -1
        mv = sgn * (ex - entry) / entry * 100 if entry and ex else None
        risk = t.get("sl_pct_used")
        hours = None
        try:
            hours = round((datetime.fromisoformat(t["closed_at"]) - datetime.fromisoformat(t["created_at"])).total_seconds() / 3600, 1)
        except Exception:
            pass
        closed.append({"id": t["id"], "asset": t.get("coin"), "fx": (t.get("strategy") or "") == "forex_swing",
                       "side": (t.get("action") or "").lower(), "reason": t.get("reason"),
                       "opened": t.get("created_at"), "closed": t.get("closed_at"), "hours": hours, "pnl": t.get("pnl"),
                       "pnl_net": round(_row_pnl(t), 4), "fees": round(_trade_fee_usd(t), 4), "move_pct": round(mv, 3) if mv is not None else None,
                       "risk_pct": risk, "r": round(mv / risk, 2) if (mv is not None and risk) else None,
                       "peak_pct": t.get("peak_pnl_pct"), "mode": t.get("trade_mode")})
    closed.sort(key=lambda x: x["closed"] or "", reverse=True)

    def perf(ts):
        n = len(ts)
        wins = [x for x in ts if (x["pnl_net"] or 0) > 0]
        rs = [x["r"] for x in ts if x["r"] is not None]
        return {"n": n, "wins": len(wins), "win_rate": round(len(wins) / n * 100, 1) if n else None,
                "net": round(sum(x["pnl_net"] or 0 for x in ts), 3), "gross": round(sum(x["pnl"] or 0 for x in ts), 3),
                "fees": round(sum(x["fees"] or 0 for x in ts), 3), "avg_r": round(sum(rs) / len(rs), 2) if rs else None,
                "gains": round(sum(x["pnl_net"] for x in wins), 3),
                "losses": round(sum((x["pnl_net"] or 0) for x in ts if (x["pnl_net"] or 0) <= 0), 3)}
    fx_assets = []
    for sk, st in list(bot.states.items()):
        tk = be.ticker_from_slot_key(sk)
        if tk in cfg.get("FXSWING_SYMBOLS", ["xyz:EUR", "xyz:JPY", "xyz:KRW", "xyz:DXY", "PAXG"]):
            sn = getattr(st, "fxswing_snapshot", None) or {}
            fx_assets.append({"asset": tk, "blocker": sn.get("blocker") if sn else None, "evaluated": bool(sn),
                              "age_s": round(time.time() - sn["ts"], 0) if sn.get("ts") else None, "trend": sn.get("trend"),
                              "support": sn.get("support"), "resistance": sn.get("resistance"),
                              "open": bool(st.position and st.position.get("strategy") == "forex_swing")})
    fx_keys = ("FXSWING_RISK_PCT", "FXSWING_MAX_NOTIONAL_USD", "FXSWING_MAX_TRADES", "FXSWING_MIN_RR", "FXSWING_MIN_NET_RR",
               "FXSWING_MIN_RISK_PCT", "FXSWING_MAX_RISK_PCT", "FXSWING_MAX_HOLD_DAYS", "FXSWING_SIGNAL_MAX_AGE_SEC")
    recent = db.swing_events_since(int((time.time() - 5 * 86400) * 1000), limit=80)
    keys = ("MTF_USE_DAILY", "MTF_USE_H4_LOWER", "MTF_SIGNAL_MAX_AGE_SEC", "MTF_MIN_RR", "MTF_MAX_RISK_PCT", "MTF_RISK_PCT",
            "MTF_REQUIRE_REAL_TARGET_ZONE", "MTF_REQUIRE_FLOW_CONFIRM", "MTF_MIN_FLOW_PRESSURE", "MTF_ZONE_MAX_WIDTH_ATR",
            "SPOT_ACCUM_MAX_TRADES", "ACCUMULATION_MAX_TRADES", "SPOT_ACCUM_TREND_MODE", "ACCUMULATION_TREND_MODE")
    return {"fx_enabled": bool(cfg.get("FXSWING_ENABLED", 0)), "fx_assets": fx_assets,
            "fx_settings": {k: cfg.get(k, ADVANCED_SETTINGS[k]["default"]) for k in fx_keys if k in ADVANCED_SETTINGS},
            "perf_fx": perf([x for x in closed if x.get("fx")]), "perf_crypto": perf([x for x in closed if not x.get("fx")]),
            "active": _swing_is_active(), "fees_on": _fees_on(), "fee_pct": float(cfg.get("FEE_ROUND_TRIP_PCT", 0.09)),
            "settings": {k: cfg.get(k, ADVANCED_SETTINGS[k]["default"]) for k in keys if k in ADVANCED_SETTINGS},
            "open": open_pos, "closed": closed[:100], "perf": perf(closed),
            "perf_long": perf([x for x in closed if x["side"] == "long"]), "perf_short": perf([x for x in closed if x["side"] == "short"]),
            "recent": [{k: e.get(k) for k in ("ts", "signal_t", "asset", "side", "pattern", "trend", "zone_low", "zone_high", "decision", "rr",
                                               "risk_pct", "reward_pct", "outcome", "evaluated", "r_est", "age_min", "hypothetical")} for e in recent]}


# ───────────────── v4.362 — SWING SUPPORT (crypto, paper) : suivi, statistiques, export ─────────────────
_SUPSW_KEYS = ("SUPSW_SL_PCT", "SUPSW_TP_PCT", "SUPSW_RISK_PCT", "SUPSW_MAX_NOTIONAL_USD", "SUPSW_MAX_TRADES", "SUPSW_WINDOW",
               "SUPSW_SIGNAL_MAX_AGE_SEC", "SUPSW_MAX_HOLD_DAYS", "SUPSW_EXIT_BUFFER_ATR", "SUPSW_EXIT_FLOW", "SUPSW_EXIT_REQUIRE_H4_END",
               "SUPSW_EXIT_LEVEL", "SUPSW_EXIT_MIN_PCT", "SUPSW_EXIT_MAX_PCT", "SUPSW_LEVERAGE", "SUPSW_LEVER_SCALE", "SUPSW_RESERVE_ASSETS", "SUPSW_MARGIN_RISK_PCT", "SUPSW_MIN_SL_PCT",
               "SUPSW_TP_LEV1", "SUPSW_TP_LEV2", "SUPSW_TP_LEV3")


def _supsw_est(e, fee):
    """Resultat estime (% du prix, net de frais) d un signal mesure : objectif -> +gain, stop -> -risque, sinon rendement final."""
    if not e.get("evaluated") or e.get("outcome") is None:
        return None
    if e["outcome"] == "tp":
        g = e.get("reward_pct") or 0.0
    elif e["outcome"] == "sl":
        g = -(e.get("risk_pct") or 0.0)
    else:
        g = e.get("fin72") if e.get("fin72") is not None else (e.get("fin24") if e.get("fin24") is not None else 0.0)
    return g - fee


def _supsw_stats(events, fee, sl_pct, tp_pct):
    def agg(rows):
        ev = [r for r in rows if r.get("evaluated") and r.get("outcome")]
        n_ev = len(ev)
        ests = [x for x in (_supsw_est(r, fee) for r in ev) if x is not None]
        return {"n": len(rows), "evaluated": n_ev,
                "tp_pct": round(sum(1 for r in ev if r["outcome"] == "tp") / n_ev * 100, 0) if n_ev else None,
                "sl_pct": round(sum(1 for r in ev if r["outcome"] == "sl") / n_ev * 100, 0) if n_ev else None,
                "none_pct": round(sum(1 for r in ev if r["outcome"] == "none") / n_ev * 100, 0) if n_ev else None,
                "net_est": round(sum(ests) / len(ests), 3) if ests else None, "net_n": len(ests)}

    def group(rows, keyfn):
        d = {}
        for r in rows:
            d.setdefault(keyfn(r), []).append(r)
        return sorted(({"key": k, **agg(v)} for k, v in d.items()), key=lambda x: -x["n"])

    def fbucket(r):
        f = r.get("funding_ann")
        return "?" if f is None else ("< 0 %" if f < 0 else ("0 a 15 %" if f < 15 else ("15 a 40 %" if f < 40 else "> 40 %")))

    def dec(r):
        d = r.get("decision") or "?"
        return d if d in ("pris", "candidat (non ouvert)") else d.replace("refus : ", "")[:46]
    # sorties anticipees : etaient-elles justifiees ?
    taken = [r for r in events if r.get("trade_uid") and r.get("exit_ts")]
    early = [r for r in taken if r.get("early_exit")]
    ev_early = [r for r in early if r.get("post_eval")]

    def mean(rows, k):
        v = [r[k] for r in rows if r.get(k) is not None]
        return round(sum(v) / len(v), 3) if v else None
    exits = {"taken": len(taken), "early_n": len(early), "early_evaluated": len(ev_early),
             "too_early_n": sum(1 for r in ev_early if r.get("too_early")),
             "after_tp": sum(1 for r in ev_early if r.get("after_outcome") == "tp"),
             "after_sl": sum(1 for r in ev_early if r.get("after_outcome") == "sl"),
             "after_none": sum(1 for r in ev_early if r.get("after_outcome") == "none"),
             "avg_pnl_pct_at_exit": mean(early, "pnl_pct"), "avg_fin1h": mean(ev_early, "fin1h"), "avg_fin4h": mean(ev_early, "fin4h"),
             "avg_fin24h": mean(ev_early, "fin24h"),
             "h4_intact_oui": sum(1 for r in early if r.get("h4_intact_exit") == "oui"),
             "h4_intact_non": sum(1 for r in early if r.get("h4_intact_exit") == "non"),
             "by_reason": group(taken, lambda r: r.get("exit_reason") or "?")}
    for x in exits["by_reason"]:
        rows = [r for r in taken if (r.get("exit_reason") or "?") == x["key"]]
        x["avg_pnl_pct"] = mean(rows, "pnl_pct")
        x["avg_pnl_net_pct"] = round(x["avg_pnl_pct"] - fee, 3) if x["avg_pnl_pct"] is not None else None
    held = [r for r in taken if r.get("pnl_pct") is not None]
    real = {"n": len(held), "wins": sum(1 for r in held if r["pnl_pct"] - fee > 0),
            "avg_net_pct": round(sum(r["pnl_pct"] - fee for r in held) / len(held), 3) if held else None}
    breakeven = round((sl_pct + fee) / (sl_pct + tp_pct) * 100, 1) if (sl_pct + tp_pct) else None
    return {"total": agg(events), "by_decision": group(events, dec),
            "by_setup": group(events, lambda r: r.get("setup_kind") or "?"), "by_asset": group(events, lambda r: r.get("asset") or "?"),
            "by_weekly": group(events, lambda r: r.get("weekly_trend") or "?"), "by_btc": group(events, lambda r: r.get("btc_trend") or "?"),
            "by_funding": group(events, fbucket), "by_trend": group(events, lambda r: r.get("trend") or "?"),
            "exits": exits, "real": real, "breakeven_tp_rate": breakeven}


@app.get("/api/supsw/status")
def supsw_status(email: str = Depends(require_user)):
    open_pos, assets = [], []
    for pool in (bot.states, bot.accum_states):
        for sk, st in list(pool.items()):
            pos = st.position
            if not (pos and pos.get("strategy") == "swing_support"):
                continue
            tk = be.ticker_from_slot_key(sk)
            px = float((bot.all_mids or {}).get(tk) or 0) or pos["entry"]
            sp = pos.get("supsw") or {}
            open_pos.append({"asset": tk, "entry": pos["entry"], "price": px, "sl": pos["sl"], "tp": pos.get("tp"),
                             "gain_pct": round((px - pos["entry"]) / pos["entry"] * 100, 3), "lev": pos.get("leverage") or 1, "setup": sp.get("kind"),
                             "setup_name": sp.get("setup"), "zone": [sp.get("zone_low"), sp.get("zone_high")], "inval": sp.get("inval"), "ctx": sp.get("ctx"),
                             "flow_entry": sp.get("flow_entry"), "broken_noflow": bool(sp.get("broken_noflow_ts")),
                             "age_h": round((time.time() - sp.get("opened_ts", time.time())) / 3600, 1)})
    syms = cfg.get("SUPSW_SYMBOLS", ["BTC", "ETH", "HYPE", "TAO", "SUI"])
    for sk, st in list(bot.states.items()):
        tk = be.ticker_from_slot_key(sk)
        if tk in syms:
            sn = getattr(st, "supsw_snapshot", None) or {}
            assets.append({"asset": tk, "blocker": sn.get("blocker"), "evaluated": bool(sn), "trend": sn.get("trend"), "support": sn.get("support"),
                           "setup": sn.get("setup"), "age_s": round(time.time() - sn["ts"], 0) if sn.get("ts") else None,
                           "open": bool(st.position and st.position.get("strategy") == "swing_support")})
    rows = [t for t in db.get_all_closed_trades() if (t.get("strategy") or "") == "swing_support"]
    closed = []
    for t in rows:
        entry, ex = t.get("entry_price"), t.get("exit_price")
        mv = (ex - entry) / entry * 100 if entry and ex else None
        closed.append({"id": t["id"], "asset": t.get("coin"), "reason": t.get("reason"), "opened": t.get("created_at"),
                       "closed": t.get("closed_at"), "pnl": t.get("pnl"), "pnl_net": round(_row_pnl(t), 4), "fees": round(_trade_fee_usd(t), 4),
                       "move_pct": round(mv, 3) if mv is not None else None, "side": t.get("action"), "peak_usd": t.get("peak_pnl"), "peak_pct": t.get("peak_pnl_pct"), "lev": t.get("leverage") or 1})
    closed.sort(key=lambda x: x["closed"] or "", reverse=True)
    n = len(closed)
    wins = [x for x in closed if (x["pnl_net"] or 0) > 0]
    perf = {"n": n, "wins": len(wins), "win_rate": round(len(wins) / n * 100, 1) if n else None,
            "net": round(sum(x["pnl_net"] or 0 for x in closed), 3), "gross": round(sum(x["pnl"] or 0 for x in closed), 3),
            "fees": round(sum(x["fees"] or 0 for x in closed), 3)}
    recent = db.supsw_events_since(int((time.time() - 4 * 86400) * 1000), limit=60)
    return {"enabled": bool(cfg.get("SUPSW_ENABLED", 0)), "symbols": syms, "assets": assets, "open": open_pos, "closed": closed[:60], "perf": perf,
            "fees_on": _fees_on(), "fee_pct": float(cfg.get("FEE_ROUND_TRIP_PCT", 0.09)),
            "settings": {k: cfg.get(k, ADVANCED_SETTINGS[k]["default"]) for k in _SUPSW_KEYS if k in ADVANCED_SETTINGS},
            "recent": [{k: e.get(k) for k in ("ts", "signal_t", "asset", "setup_kind", "decision", "trend", "weekly_trend", "btc_trend", "funding_ann",
                                               "flow", "outcome", "evaluated", "exit_reason", "early_exit", "pnl_pct", "too_early", "after_outcome",
                                               "fin1h", "fin4h", "fin24h", "h4_intact_exit")} for e in recent]}


@app.get("/api/supsw/stats")
def supsw_stats_ep(days: int = Query(30, ge=1, le=120), email: str = Depends(require_user)):
    """Ce que valent les signaux (pris ET refuses) mesures a 24 h / 72 h, par declencheur et par contexte (BTC, Weekly, funding), et la
    qualite des sorties anticipees (le prix aurait-il atteint l objectif / le stop ensuite ?)."""
    fee = float(cfg.get("FEE_ROUND_TRIP_PCT", 0.09))
    ev = db.supsw_events_since(int((time.time() - days * 86400) * 1000), limit=20000)
    last = next((e for e in ev if e.get("risk_pct") and e.get("reward_pct")), None)       # niveaux reellement utilises (peuvent dependre du levier)
    sl_u = float(last["risk_pct"]) if last else float(cfg.get("SUPSW_SL_PCT", 5.0))
    tp_u = float(last["reward_pct"]) if last else float(cfg.get("SUPSW_TP_PCT", 2.0))
    # v4.370 : alertes de degradation (mesure seule) — comparaison entre trades gagnants et perdants
    grp = {"gagnants": [], "perdants": []}
    for e in ev:
        if e.get("exit_ts") and e.get("alerts_max") is not None and e.get("pnl_pct") is not None:
            grp["gagnants" if (e.get("exit_reason") or "").endswith("OBJECTIF") else "perdants" if (e.get("pnl_pct") or 0) < 0 else "gagnants"].append(e)
    alerts = {}
    for k, rows in grp.items():
        if rows:
            alerts[k] = {"n": len(rows), "max_moyen": round(sum(r["alerts_max"] for r in rows) / len(rows), 2),
                         "a_la_sortie_moyen": round(sum((r.get("alerts_exit") or 0) for r in rows) / len(rows), 2),
                         "repartition_max": {str(i): sum(1 for r in rows if r["alerts_max"] == i) for i in range(0, 8)}}
    return {"days": days, "fee_pct": fee, **_supsw_stats(ev, fee, sl_u, tp_u), "alerts": alerts}


@app.get("/api/supsw/journal/export.csv")
def supsw_export(days: int = Query(60, ge=1, le=120), email: str = Depends(require_user)):
    import csv
    import io
    ev = list(reversed(db.supsw_events_since(int((time.time() - days * 86400) * 1000), limit=50000)))
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\r\n")
    cols = ["asset", "side", "setup", "setup_kind", "trend", "weekly_trend", "btc_trend", "funding_ann", "flow", "atr_pct", "hour_utc",
            "zone_low", "zone_high", "zone_touches", "held", "pierce_pct", "price", "entry_ref", "sl", "tp", "inval", "risk_pct", "reward_pct", "decision",
            "trade_uid", "evaluated", "outcome", "hit_h", "mfe24", "mae24", "mfe72", "mae72", "fin24", "fin72", "exit_ts", "exit_price",
            "exit_reason", "early_exit", "pnl_pct", "hold_h", "flow_exit", "h4_intact_exit", "d1_trend_exit", "post_eval", "fin1h", "fin4h", "fin24h",
            "after_outcome", "after_hit_h", "after_mfe", "after_mae", "too_early", "alerts_max", "alerts_exit", "alerts_json", "btc_chg24", "btc_h4", "h4_trend", "peak_pct", "trail_armed", "res_low", "res_dist_pct", "chg_recent", "btc_chg_recent"]
    w.writerow(["cloture du signal (UTC)"] + cols)
    for e in ev:
        row = [datetime.fromtimestamp(e["signal_t"] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")]
        for c in cols:
            v = e.get(c)
            if c == "exit_ts" and v:
                v = datetime.fromtimestamp(v / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
            row.append("" if v is None else (str(round(v, 6)).replace(".", ",") if isinstance(v, float) else v))
        w.writerow(row)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%Hh%M")
    return Response(content="﻿" + buf.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="swing_support_{stamp}.csv"'})


# ───────────────── v4.365 — TENDANCE CONFIRMEE (paper) : suivi, statistiques, export ─────────────────
_TRENDF_KEYS = ("TRENDF_ALLOW_SHORT", "TRENDF_RISK_PCT", "TRENDF_MAX_NOTIONAL_USD", "TRENDF_LEVERAGE", "TRENDF_MAX_TRADES", "TRENDF_STOP_ATR",
                "TRENDF_MIN_STOP_PCT", "TRENDF_MAX_STOP_PCT", "TRENDF_ENTRY_MODE", "TRENDF_BREAK_BARS", "TRENDF_MIN_FLOW", "TRENDF_MAX_EXT_ATR",
                "TRENDF_MAX_SPREAD_PCT", "TRENDF_MAX_HOLD_DAYS", "TRENDF_COOLDOWN_SEC", "TRENDF_EXIT_H4_REVERSE")


def _trendf_est(e, fee):
    """Resultat estime (% du prix, net de frais) d un signal mesure, sans objectif : stop initial touche -> -risque, sinon rendement 72 h (ou 24 h)."""
    if not e.get("evaluated") or e.get("outcome") is None:
        return None
    if e["outcome"] == "sl":
        g = -(e.get("risk_pct") or 0.0)
    else:
        g = e.get("fin72") if e.get("fin72") is not None else (e.get("fin24") if e.get("fin24") is not None else 0.0)
    return g - fee


def _trendf_stats(events, fee):
    def agg(rows):
        ev = [r for r in rows if r.get("evaluated") and r.get("outcome")]
        n_ev = len(ev)
        ests = [x for x in (_trendf_est(r, fee) for r in ev) if x is not None]
        return {"n": len(rows), "evaluated": n_ev,
                "sl_pct": round(sum(1 for r in ev if r["outcome"] == "sl") / n_ev * 100, 0) if n_ev else None,
                "net_est": round(sum(ests) / len(ests), 3) if ests else None, "net_n": len(ests)}

    def group(rows, keyfn):
        d = {}
        for r in rows:
            d.setdefault(keyfn(r), []).append(r)
        return sorted(({"key": k, **agg(v)} for k, v in d.items()), key=lambda x: -x["n"])

    def fbucket(r):
        f = r.get("funding_ann")
        return "?" if f is None else ("< 0 %" if f < 0 else ("0 a 15 %" if f < 15 else ("15 a 40 %" if f < 40 else "> 40 %")))

    def late(r):
        x = r.get("ext_atr")
        return "?" if x is None else ("< 1 ATR (debut de mouvement)" if x < 1 else ("1 a 2 ATR" if x < 2 else ("2 a 3 ATR" if x < 3 else "> 3 ATR (mouvement avance)")))

    def dec(r):
        d = r.get("decision") or "?"
        return d if d in ("pris", "candidat (non ouvert)") else d.replace("refus : ", "")[:46]

    def mean(rows, k):
        v = [r[k] for r in rows if r.get(k) is not None]
        return round(sum(v) / len(v), 3) if v else None
    taken = [r for r in events if r.get("trade_uid") and r.get("exit_ts")]
    ev_exit = [r for r in taken if r.get("post_eval")]
    exits = {"taken": len(taken), "evaluated": len(ev_exit), "avg_pnl_pct": mean(taken, "pnl_pct"), "avg_hold_h": mean(taken, "hold_h"),
             "avg_peak_pct": mean(taken, "peak_pct"), "avg_fin1h": mean(ev_exit, "fin1h"), "avg_fin4h": mean(ev_exit, "fin4h"),
             "avg_fin24h": mean(ev_exit, "fin24h"), "by_reason": group(taken, lambda r: r.get("exit_reason") or "?")}
    for x in exits["by_reason"]:
        rows = [r for r in taken if (r.get("exit_reason") or "?") == x["key"]]
        x["avg_pnl_pct"] = mean(rows, "pnl_pct")
        x["avg_pnl_net_pct"] = round(x["avg_pnl_pct"] - fee, 3) if x["avg_pnl_pct"] is not None else None
        x["avg_hold_h"] = mean(rows, "hold_h")
    held = [r for r in taken if r.get("pnl_pct") is not None]
    real = {"n": len(held), "wins": sum(1 for r in held if r["pnl_pct"] - fee > 0),
            "avg_net_pct": round(sum(r["pnl_pct"] - fee for r in held) / len(held), 3) if held else None}
    return {"total": agg(events), "by_decision": group(events, dec), "by_side": group(events, lambda r: r.get("side") or "?"),
            "by_asset": group(events, lambda r: r.get("asset") or "?"), "by_weekly": group(events, lambda r: r.get("weekly_trend") or "?"),
            "by_btc": group(events, lambda r: r.get("btc_trend") or "?"), "by_funding": group(events, fbucket),
            "by_late": group(events, late), "exits": exits, "real": real}


@app.get("/api/trend/status")
def trend_status(email: str = Depends(require_user)):
    open_pos, assets = [], []
    for pool in (bot.states, bot.accum_states):
        for sk, st in list(pool.items()):
            pos = st.position
            if not (pos and pos.get("strategy") == "trend_follow"):
                continue
            tk = be.ticker_from_slot_key(sk)
            px = float((bot.all_mids or {}).get(tk) or 0) or pos["entry"]
            tf = pos.get("trendf") or {}
            sgn = 1 if pos.get("type") == "long" else -1
            open_pos.append({"asset": tk, "side": pos.get("type"), "entry": pos["entry"], "price": px, "sl": pos["sl"], "sl0": tf.get("sl0"),
                             "gain_pct": round(sgn * (px - pos["entry"]) / pos["entry"] * 100, 3), "peak_pct": round(tf.get("peak_pct", 0.0), 3),
                             "moved": bool(tf.get("moved")), "d1": tf.get("d1"), "h4": tf.get("h4"), "ctx": tf.get("ctx"),
                             "flow_entry": tf.get("flow_entry"), "ext_atr": tf.get("ext_atr"),
                             "age_h": round((time.time() - tf.get("opened_ts", time.time())) / 3600, 1)})
    syms = cfg.get("TRENDF_SYMBOLS", ["BTC", "ETH", "HYPE", "TAO", "SUI", "PAXG", "xyz:EUR", "xyz:JPY"])
    for sk, st in list(bot.states.items()):
        tk = be.ticker_from_slot_key(sk)
        if tk in syms:
            sn = getattr(st, "trendf_snapshot", None) or {}
            assets.append({"asset": tk, "blocker": sn.get("blocker"), "evaluated": bool(sn), "d1": sn.get("d1"), "h4": sn.get("h4"),
                           "side": sn.get("side"), "age_s": round(time.time() - sn["ts"], 0) if sn.get("ts") else None,
                           "open": bool(st.position and st.position.get("strategy") == "trend_follow")})
    rows = [t for t in db.get_all_closed_trades() if (t.get("strategy") or "") == "trend_follow"]
    closed = []
    for t in rows:
        entry, ex = t.get("entry_price"), t.get("exit_price")
        mv = (ex - entry) / entry * 100 if entry and ex else None
        if mv is not None and str(t.get("action") or t.get("side") or "").upper().startswith("SHORT"):
            mv = -mv
        closed.append({"id": t["id"], "asset": t.get("coin"), "reason": t.get("reason"), "opened": t.get("created_at"),
                       "closed": t.get("closed_at"), "pnl": t.get("pnl"), "pnl_net": round(_row_pnl(t), 4), "fees": round(_trade_fee_usd(t), 4),
                       "move_pct": round(mv, 3) if mv is not None else None, "side": t.get("action"), "peak_usd": t.get("peak_pnl"), "peak_pct": t.get("peak_pnl_pct"), "lev": t.get("leverage") or 1})
    closed.sort(key=lambda x: x["closed"] or "", reverse=True)
    n = len(closed)
    wins = [x for x in closed if (x["pnl_net"] or 0) > 0]
    perf = {"n": n, "wins": len(wins), "win_rate": round(len(wins) / n * 100, 1) if n else None,
            "net": round(sum(x["pnl_net"] or 0 for x in closed), 3), "gross": round(sum(x["pnl"] or 0 for x in closed), 3),
            "fees": round(sum(x["fees"] or 0 for x in closed), 3)}
    return {"enabled": bool(cfg.get("TRENDF_ENABLED", 0)), "symbols": syms, "assets": assets, "open": open_pos, "closed": closed[:60], "perf": perf,
            "fees_on": _fees_on(), "fee_pct": float(cfg.get("FEE_ROUND_TRIP_PCT", 0.09)),
            "settings": {k: cfg.get(k, ADVANCED_SETTINGS[k]["default"]) for k in _TRENDF_KEYS if k in ADVANCED_SETTINGS}}


@app.get("/api/trend/stats")
def trend_stats_ep(days: int = Query(30, ge=1, le=120), email: str = Depends(require_user)):
    """Ce que valent les entrees (prises ET refusees) mesurees a 24 h / 72 h, selon le sens, le contexte et surtout leur RETARD ; qualite des sorties."""
    fee = float(cfg.get("FEE_ROUND_TRIP_PCT", 0.09))
    ev = db.trendf_events_since(int((time.time() - days * 86400) * 1000), limit=20000)
    return {"days": days, "fee_pct": fee, **_trendf_stats(ev, fee)}


@app.get("/api/trend/journal/export.csv")
def trend_export(days: int = Query(60, ge=1, le=120), email: str = Depends(require_user)):
    import csv
    import io
    ev = list(reversed(db.trendf_events_since(int((time.time() - days * 86400) * 1000), limit=50000)))
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\r\n")
    cols = ["asset", "side", "d1_trend", "h4_trend", "weekly_trend", "btc_trend", "funding_ann", "flow", "atr_pct", "ext_atr", "hour_utc", "price",
            "entry_ref", "sl", "risk_pct", "entry_mode", "decision", "trade_uid", "evaluated", "outcome", "hit_h", "mfe24", "mae24", "mfe72",
            "mae72", "fin24", "fin72", "exit_ts", "exit_price", "exit_reason", "pnl_pct", "hold_h", "peak_pct", "d1_trend_exit", "h4_trend_exit",
            "post_eval", "fin1h", "fin4h", "fin24h", "after_mfe", "after_mae"]
    w.writerow(["cloture du signal (UTC)"] + cols)
    for e in ev:
        row = [datetime.fromtimestamp(e["signal_t"] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")]
        for c in cols:
            v = e.get(c)
            if c == "exit_ts" and v:
                v = datetime.fromtimestamp(v / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
            row.append("" if v is None else (str(round(v, 6)).replace(".", ",") if isinstance(v, float) else v))
        w.writerow(row)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%Hh%M")
    return Response(content="﻿" + buf.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="tendance_{stamp}.csv"'})


def _swing_stats(events, fee_pct):
    import statistics as _st

    def agg(rows):
        ev = [r for r in rows if r.get("evaluated") and r.get("outcome")]
        n_ev = len(ev)
        rs = [r["r_est"] for r in ev if r.get("r_est") is not None]
        rn = [r["r_est"] - (fee_pct / r["risk_pct"]) for r in ev if r.get("r_est") is not None and r.get("risk_pct")]
        med = lambda k: round(_st.median([r[k] for r in ev if r.get(k) is not None]), 2) if any(r.get(k) is not None for r in ev) else None
        return {"n": len(rows), "evaluated": n_ev,
                "tp_pct": round(sum(1 for r in ev if r["outcome"] == "tp") / n_ev * 100, 0) if n_ev else None,
                "sl_pct": round(sum(1 for r in ev if r["outcome"] == "sl") / n_ev * 100, 0) if n_ev else None,
                "none_pct": round(sum(1 for r in ev if r["outcome"] == "none") / n_ev * 100, 0) if n_ev else None,
                "mfe72": med("mfe72"), "mae72": med("mae72"),
                "r_gross": round(sum(rs) / len(rs), 2) if rs else None, "r_net": round(sum(rn) / len(rn), 2) if rn else None, "r_n": len(rn)}

    def group(keyfn):
        d = {}
        for r in events:
            d.setdefault(keyfn(r), []).append(r)
        return sorted(({"key": k, **agg(v)} for k, v in d.items()), key=lambda x: -x["n"])

    def dec(r):
        d = r.get("decision") or "?"
        return d if d in ("pris", "candidat (non ouvert)") else d.replace("filtre : ", "")[:52]
    return {"total": agg(events), "by_decision": group(dec), "by_pattern": group(lambda r: (r.get("pattern") or "?").split(" (")[0][:30]),
            "by_side": group(lambda r: r.get("side") or "?"), "by_asset": group(lambda r: r.get("asset") or "?"),
            "by_trend": group(lambda r: r.get("trend") or "?"),
            "by_close": group(lambda r: f"cloture {((r.get('signal_t') or 0) // 3600000) % 24:02d}h UTC")}


@app.get("/api/swing/stats")
def swing_stats_ep(days: int = Query(14, ge=1, le=120), email: str = Depends(require_user)):
    """Ce que valent les signaux du Swing, pris ET filtres, mesures a 24 h / 72 h : issue, MFE/MAE et resultat en R net de frais."""
    ev = db.swing_events_since(int((time.time() - days * 86400) * 1000), limit=20000)
    return {"days": days, "fee_pct": float(cfg.get("FEE_ROUND_TRIP_PCT", 0.09)), **_swing_stats(ev, float(cfg.get("FEE_ROUND_TRIP_PCT", 0.09)))}


@app.get("/api/swing/journal/export.csv")
def swing_export(days: int = Query(30, ge=1, le=120), email: str = Depends(require_user)):
    import csv
    import io
    ev = list(reversed(db.swing_events_since(int((time.time() - days * 86400) * 1000), limit=50000)))
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\r\n")
    cols = ["asset", "side", "mode", "pattern", "trend", "zone_low", "zone_high", "zone_touches", "price", "entry_ref", "atr_pct", "flow",
            "hour_utc", "age_min", "decision", "sl", "tp", "risk_pct", "reward_pct", "rr", "hypothetical", "trade_uid", "evaluated",
            "outcome", "hit_h", "mfe24", "mae24", "mfe72", "mae72", "fin24", "fin72", "covered_h", "r_est"]
    w.writerow(["cloture du signal (UTC)"] + cols)
    for e in ev:
        row = [datetime.fromtimestamp(e["signal_t"] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")]
        for c in cols:
            v = e.get(c)
            row.append("" if v is None else (str(round(v, 6)).replace(".", ",") if isinstance(v, float) else v))
        w.writerow(row)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%Hh%M")
    return Response(content="\ufeff" + buf.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="swing_journal_{stamp}.csv"'})


@app.get("/api/stats/funding-streaks")
def get_funding_streaks(email: str = Depends(require_user)):
    """v4.316 — CONSERVE pour compatibilite (l URL existante continue de
    fonctionner) — redirige vers le nouvel endpoint generalise ci-dessus."""
    return get_streaks(strategy="funding_contrarian", email=email)


@app.get("/api/diagnostics/orphaned-trades")
def get_orphaned_trades(older_than_minutes: int = Query(20, ge=1, le=1440), email: str = Depends(require_user)):
    """v4.312 — SUR DEMANDE EXPLICITE : diagnostic pour des positions
    disparues du bot SANS AUCUNE trace (ni ouvertes, ni dans l historique).
    Croise les lignes marquees 'ouvertes' en base depuis plus de
    older_than_minutes avec les positions REELLEMENT ouvertes cote bot
    (bot.states + bot.accum_states) — ce qui ne correspond a aucune position
    reelle est une ligne ORPHELINE : la fermeture s est produite en memoire
    (voir _safe_close_position) mais son ecriture en base a echoue (pas de
    ligne 'open' correspondante a l instant de la fermeture, ou trade_uid
    desynchronise), la laissant coincee 'ouverte' en base pour toujours."""
    live_uids = set()
    live_coin_action_strategy = set()
    for pool in (bot.states, bot.accum_states):
        for st in pool.values():
            if st.position:
                uid = st.position.get("trade_uid")
                if uid:
                    live_uids.add(uid)
                ticker = be.ticker_from_slot_key(next((k for k, v in pool.items() if v is st), ""))
                action = "LONG" if st.position.get("type") == "long" else "SHORT"
                live_coin_action_strategy.add((ticker, action, st.position.get("strategy") or "forex"))
    stale = db.get_stale_open_trades(older_than_minutes=older_than_minutes)
    orphaned = []
    for t in stale:
        uid = t.get("trade_uid")
        key = (t.get("coin"), t.get("action"), t.get("strategy") or "forex")
        if uid and uid in live_uids:
            continue  # bien vivante cote bot, juste pas encore fermee — normal
        if not uid and key in live_coin_action_strategy:
            continue
        orphaned.append(t)
    return {
        "checked_older_than_minutes": older_than_minutes,
        "stale_open_in_db": len(stale),
        "orphaned_count": len(orphaned),
        "orphaned": orphaned,
    }



def get_entry_diagnostics_one(ticker: str, email: str = Depends(require_user)):
    """v4.40 — Detail COMPLET de l instantane des portes d entree pour UN
    actif precis (tous les champs bruts, pas juste le resume compact)."""
    ticker = _norm_ticker(ticker)
    best_state = None
    best_ts = -1
    for slot_key, state in bot.states.items():
        if be.ticker_from_slot_key(slot_key) == ticker:
            ts = (state.last_gate_snapshot or {}).get("ts", -1)
            if ts > best_ts:
                best_ts = ts
                best_state = state
    if best_state is None:
        raise HTTPException(404, f"Actif inconnu ou non suivi : {ticker}")
    return {
        "ticker": ticker,
        "has_position": best_state.position is not None,
        "position": best_state.position,
        "snapshot": best_state.last_gate_snapshot,
    }


# v4.358 — sens des trades manuels affiches dans chaque volet (Accumulation = shorts, Spot-Accum = longs)
_MANUAL_DIR_BY_MODE = {"accumulation": "SHORT", "spot_accumulation": "LONG"}


@app.get("/api/signals")
def get_signals(limit: int = Query(50), strategy: str = Query(None), include_manual: int = Query(0), email: str = Depends(require_user)):
    """v4.44 — SUR DEMANDE EXPLICITE : parametre 'strategy' optionnel pour
    filtrer l historique PAR MODE. Sans ce filtre, la limite de 50 est
    partagee entre TOUS les modes confondus — insuffisant pour un historique
    par mode complet (avec 4 modes actifs, chacun pourrait n avoir que
    quelques lignes visibles, voire aucune, meme avec beaucoup de trades
    reels). Avec le filtre, on interroge un bassin bien plus large AVANT de
    filtrer, pour que 'limit' s applique au nombre de trades de CE mode
    precis, pas au nombre de trades tous modes confondus.
    v4.114 — FIX BUG : order_by_close=True trie desormais par date de
    FERMETURE (plus recent en premier) — l ancien tri par ouverture (id)
    inversait l ordre reel des trades fermes (un trade ouvert tot mais
    ferme tard apparaissait avant un trade ouvert tard mais ferme vite)."""
    if strategy:
        raw = db.get_trades(limit=max(limit * 20, 2000), order_by_close=True)
        filtered = [r for r in raw if (r.get("strategy") or "forex") == strategy]
        # v4.358 — SUR DEMANDE EXPLICITE : les trades MANUELS apparaissent dans le volet du mode correspondant a
        # leur SENS : shorts -> Accumulation, longs -> Spot-Accum (strategie "manual", reconnaissables a leur badge).
        if include_manual and strategy in _MANUAL_DIR_BY_MODE:
            want = _MANUAL_DIR_BY_MODE[strategy]
            filtered = filtered + [r for r in raw if (r.get("strategy") or "") == "manual" and r.get("action") == want]
            filtered.sort(key=lambda r: (r.get("closed_at") or r.get("created_at") or ""), reverse=True)
        return {"signals": [_trade_row_to_signal(r) for r in filtered[:limit]]}
    return {"signals": [_trade_row_to_signal(r) for r in db.get_trades(limit=limit, order_by_close=True)]}


@app.get("/api/stats")
def get_stats(email: str = Depends(require_user)):
    reset_at = db.get_meta("stats_reset_at")  # v4.306 — point de reinitialisation des statistiques
    rows = db.get_trades(limit=100000, since=reset_at) if reset_at else db.get_trades(limit=100000)
    if not rows:
        return {"total": 0, "longs": 0, "shorts": 0, "avg_confidence": 0, "avg_rr": "--"}
    longs = sum(1 for r in rows if r["action"] == "LONG")
    shorts = sum(1 for r in rows if r["action"] == "SHORT")
    confs = [r["confidence"] for r in rows if r["confidence"] is not None]
    rrs = [r["risk_reward"] for r in rows if r["risk_reward"] is not None]
    return {
        "total": len(rows),
        "longs": longs,
        "shorts": shorts,
        "avg_confidence": round(sum(confs) / len(confs), 1) if confs else 0,
        "avg_rr": round(sum(rrs) / len(rrs), 2) if rrs else "--",
    }


def _manual_open_list():
    try:
        with bot.manual.lock:
            items = [bot.manual.public(i) for i in bot.manual.items.values() if i.get("status") in ("open", "opening", "closing")]
        keys = ("id", "ticker", "direction", "mode", "market", "leverage", "entry_price", "current_price", "move_pct", "pnl", "qty",
                "notional_usd", "sl_price", "tp_price", "opened_at", "strategy_label", "status", "ttp_arm_pct", "trade_uid", "peak_pct", "qty", "armed",
                "tp_run", "tp_run_armed", "tp_run_lock_pct", "tp_run_trail_pct", "tp1_price", "tp1_close_pct", "tp1_be", "tp1_done", "realized_pnl")
        return [{k: it.get(k) for k in keys} for it in sorted(items, key=lambda i: i.get("opened_at") or 0, reverse=True)]
    except Exception as e:
        print(f"[portfolio] trades manuels ignores : {type(e).__name__}: {e}")
        return []


@app.get("/api/paper/portfolio")
def paper_portfolio(email: str = Depends(require_user)):
    # v3.2 — FIX : l interface attend total_pnl/total_pnl_pct (le PnL NON
    # REALISE des positions actuellement ouvertes), alors que cette route ne
    # renvoyait que le PnL REALISE cumule (s.pnl, uniquement des trades deja
    # fermes) — d ou "PnL ouvert" et "Performance" bloques a 0.00 en
    # permanence, meme avec des positions ouvertes en profit/perte.
    open_positions = _open_positions()
    unrealized_pnl = round(sum(p["pnl"] for p in open_positions), 2)
    # v4.121 — SUR DEMANDE EXPLICITE : Accumulation a desormais son PROPRE
    # emplacement (bot.accum_states), jamais inclus ci-dessous auparavant.
    realized_pnl = sum(s.pnl for s in bot.states.values()) + sum(s.pnl for s in bot.accum_states.values())
    initial_balance = float(db.get_meta("initial_balance", cfg["CAPITAL_USD"])) or 1.0

    closed = db.get_all_closed_trades(since=db.get_meta("stats_reset_at"))  # v4.306 — point de reinitialisation
    wins = sum(1 for r in closed if _row_pnl(r) > 0)          # v4.355 : win rate APRES frais si FEES_IN_STATS
    win_rate = round(wins / len(closed) * 100, 1) if closed else 0
    fees_adj = _fees_all_closed() if _fees_on() else 0.0

    # v3.2 — FIX : "balance" (affiche "SOLDE VIRTUEL") ne deduisait pas les
    # montants deja engages dans les positions ouvertes — il affichait donc
    # le capital total, pas ce qu il reste reellement disponible pour de
    # nouveaux trades.
    engaged = sum(p["size"] for p in open_positions)
    return {
        "balance": round(bot.capital + realized_pnl - fees_adj - engaged, 2),
        "fees_on": _fees_on(), "fees_total": round(fees_adj, 2), "fee_pct": float(cfg.get("FEE_ROUND_TRIP_PCT", 0.09)),   # v4.355
        "open_trades": open_positions,
        "manual_open": _manual_open_list(),      # v4.383 : trades MANUELS ouverts (affiches dans « Tous » et dans le volet du mode, tag Manuel) ; hors des totaux ci-dessus
        "total_pnl": unrealized_pnl,
        "total_pnl_pct": round(unrealized_pnl / initial_balance * 100, 3),
        "win_rate": win_rate,
    }


# ─────────────────────────────────────────────────────────────────────────
#  v4.306 — SUR DEMANDE EXPLICITE : point de reinitialisation des
#  STATISTIQUES affichees dans l onglet Historique (win rate, PnL net,
#  Bilan, Performance par mode...), SANS RIEN SUPPRIMER en base — a la
#  difference de "REINITIALISATION COMPLETE" (/api/paper/reset) qui, elle,
#  efface reellement les signaux/trades/historique. Ici, tous les trades
#  restent en base et dans les exports CSV — seule la fenetre utilisee pour
#  calculer les statistiques affichees est deplacee a "a partir de
#  maintenant". Stocke dans meta['stats_reset_at'] (ISO UTC) ; None = pas de
#  reinitialisation active, comportement d origine (tout l historique).
# ─────────────────────────────────────────────────────────────────────────
@app.get("/api/stats/reset-point")
def get_stats_reset_point(email: str = Depends(require_user)):
    return {"stats_reset_at": db.get_meta("stats_reset_at")}


@app.post("/api/stats/reset-point")
def set_stats_reset_point(email: str = Depends(require_user)):
    now_iso = datetime.now(timezone.utc).isoformat()
    db.set_meta("stats_reset_at", now_iso)
    print(f"[AUDIT] /api/stats/reset-point (definir) appele par {email} a {now_iso}")
    return {"ok": True, "stats_reset_at": now_iso}


@app.delete("/api/stats/reset-point")
def clear_stats_reset_point(email: str = Depends(require_user)):
    db.set_meta("stats_reset_at", "")
    print(f"[AUDIT] /api/stats/reset-point (effacer) appele par {email} a {datetime.now(timezone.utc).isoformat()}")
    return {"ok": True, "stats_reset_at": None}


@app.post("/api/paper/reset")
def paper_reset(email: str = Depends(require_user)):
    print(f"[AUDIT] /api/paper/reset appele par {email} a {datetime.now(timezone.utc).isoformat()}")
    if bot.trading_enabled:
        raise HTTPException(400, "Arretez le bot avant de reinitialiser")
    db.clear_all_trades()
    # v4.14 — le moteur (cycle de gestion des positions) tourne desormais en
    # continu, y compris pendant un reset (seul trading_enabled est verifie
    # ci-dessus, pas l arret du moteur) : on protege cette mutation directe
    # de l etat avec le meme verrou que _manage_position, pour eviter toute
    # collision avec un cycle en cours au meme instant.
    with bot.lock:
        for state in bot.states.values():
            state.position = None
            state.pnl = 0.0
            state.trades = 0
            state.wins = 0
            state.closed_trades.clear()
        # v4.121 — SUR DEMANDE EXPLICITE : Accumulation a desormais son
        # PROPRE emplacement (bot.accum_states), jamais reinitialise
        # ci-dessus auparavant — les trades/PnL Accumulation auraient
        # survecu a une reinitialisation complete.
        for accum_state in bot.accum_states.values():
            accum_state.position = None
            accum_state.pnl = 0.0
            accum_state.trades = 0
            accum_state.wins = 0
            accum_state.closed_trades.clear()
    # v4.1 — FIX : repart du capital par defaut du CODE (be.CONFIG), pas de
    # cfg["CAPITAL_USD"] qui contient la derniere valeur PERSISTEE (chargee
    # au demarrage depuis hyperbot_capital_*.json) — sans ce fix, changer le
    # capital par defaut dans le code n avait plus aucun effet des qu un
    # fichier de capital existait deja sur le volume, et "reinitialiser"
    # ne faisait que re-sauvegarder cette meme valeur perimee.
    cfg["CAPITAL_USD"] = be.CONFIG["CAPITAL_USD"]
    bot.capital = cfg["CAPITAL_USD"]
    bot.sessions = 0
    bot.total_pnl_all = 0.0
    # v4.1 — un capital reinitialise demarre un lot neuf : l ancien E fige
    # (base sur l ancien capital) ne doit pas survivre a la reinitialisation.
    bot.batch_entry_sizes = {}  # v4.297 — un lot par pot
    bot.clear_all_persisted_files()
    be.save_capital(bot.capital, 0, 0.0)
    be.save_batch_entry_size(None)
    db.set_meta("reset_at", db.now_iso())
    db.set_meta("initial_balance", str(bot.capital))
    db.set_meta("total_running_seconds", "0")
    db.set_meta("running_since", "")
    return {"ok": True}


class PaperCloseBody(BaseModel):
    trade_id: str  # = slot_key (ex: "BTC_0")
    reason: str = "MANUEL"


@app.post("/api/paper/close")
def paper_close(body: PaperCloseBody, email: str = Depends(require_user)):
    # v4.121 — SUR DEMANDE EXPLICITE : Accumulation a desormais son PROPRE
    # emplacement (bot.accum_states) — repli si absent de bot.states.
    state = bot.states.get(body.trade_id)
    if not state or not state.position:
        state = bot.accum_states.get(body.trade_id)
    if not state or not state.position:
        raise HTTPException(404, "Aucune position ouverte pour cet identifiant")
    price = state.current_price or state.position["entry"]
    ticker = be.ticker_from_slot_key(body.trade_id)
    pos_snapshot = dict(state.position)  # avant fermeture — necessaire pour close_order (actifs spot)
    # v4.110 — FIX BUG CRITIQUE : utilisait cfg.get("MODE") (mode GLOBAL),
    # jamais le mode EFFECTIF par strategie — si le mode global est reste
    # "paper" alors qu UNE strategie precise (ex: Spot-Accum) a ete basculee
    # en live via le switch par mode, la fermeture reelle sur Hyperliquid
    # n etait JAMAIS tentee, alors que le suivi interne du bot marquait la
    # position comme fermee — position reelle abandonnee sans plus AUCUN
    # suivi (SL/TTP/retournement), un vrai risque de securite confirme.
    real_strategy = pos_snapshot.get("strategy", "forex")
    # v4.264 — mode REEL de la position (fige a son ouverture), et ordre
    # reel envoye AVANT de fermer le suivi interne : si Hyperliquid ne
    # confirme pas la fermeture, la position reste suivie par le bot au lieu
    # d etre abandonnee sans surveillance.
    effective_mode_close = bot._position_mode(pos_snapshot)
    close_order_ok = None
    if effective_mode_close == "live" and bot.exchange:
        close_order_ok = be.close_order(bot.exchange, body.trade_id, pos_snapshot, cfg)
        if not close_order_ok:
            _push_log("error", f"⚠️ [{ticker}] Fermeture manuelle : Hyperliquid n'a PAS confirme la fermeture — position conservee et toujours suivie par le bot. Reessayez ou verifiez sur Hyperliquid.")
            raise HTTPException(502, "Fermeture reelle non confirmee par Hyperliquid — position conservee et toujours suivie. Reessayez.")
    with _state_lock:
        if not state.position:
            raise HTTPException(409, "La position vient d'etre fermee par le bot entre-temps")
        exit_source = "simulation"
        if close_order_ok:
            real_exit = be.pop_last_close_fill(ticker)
            exit_source = "bot"
            if real_exit:
                price, exit_source = real_exit, "hyperliquid"
        pnl, win, trade = state.close_position(price, body.reason)
        trade["symbol"] = body.trade_id
        # v4.310 — meme diagnostic que pour une fermeture automatique (voir
        # bot_engine._safe_close_position) — coherence entre les deux voies
        # de fermeture pour un trade top-down ferme manuellement.
        if pos_snapshot.get("engine") == "mtf":
            try:
                mv = bot.mtf_view(ticker, price, cache_only=True)
                if mv and mv.get("ok"):
                    want = "haussiere" if trade["type"] == "long" else "baissiere"
                    trade["mtf_trend_at_close"] = mv["trend"]
                    trade["mtf_trend_intact_at_close"] = (mv["trend"] == want)
            except Exception:
                pass
        action = "LONG" if trade["type"] == "long" else "SHORT"
        trade_id = db.get_open_trade_id_by_uid(pos_snapshot.get("trade_uid")) or \
            db.get_open_trade_id_by_coin_action(ticker, action, real_strategy)
        if trade_id:
            db.close_trade(trade_id, trade["exit"], trade["pnl"], trade["reason"],
                           fees_paid=trade.get("fees_paid"), exit_price_source=exit_source,
                           mtf_trend_intact_at_close=trade.get("mtf_trend_intact_at_close"),
                           pnl_min_pct=trade.get("pnl_min_pct"), pnl_min_after_touch_pct=trade.get("pnl_min_after_touch_pct"),
                           peak_at_min_after_pct=trade.get("peak_at_min_after_pct"), touch_delay_sec=trade.get("touch_delay_sec"),
                           path_complete=trade.get("path_complete"))
        bot._save_open_positions()
    _push_log("warn", f"[{ticker}] Fermeture manuelle @ ${price:.2f} | PnL: {pnl:+.2f}$")
    return {"ok": True, "pnl": pnl, "real_close_confirmed": close_order_ok}


class SpotAccumTargetBody(BaseModel):
    trade_id: str  # = slot_key (ex: "BTC_0")
    target_price: Optional[float] = None  # None = retire l objectif (repli sur le trailing seul)


@app.put("/api/spot-accum/target")
def put_spot_accum_target(body: SpotAccumTargetBody, email: str = Depends(require_user)):
    """v4.47 — SUR DEMANDE EXPLICITE : permet de modifier l objectif (prix
    cible, 80% de la distance support-resistance par defaut) d une position
    Spot-Accumulation DEJA OUVERTE — utile si le marche a evolue depuis
    l entree et que l objectif initial ne semble plus pertinent. target_price
    a None retire l objectif fixe : la position ne sortira plus que via le
    trailing (3%/0.5%) ou le retournement confirme."""
    state = bot.states.get(body.trade_id)
    if not state or not state.position:
        raise HTTPException(404, "Aucune position ouverte pour cet identifiant")
    if state.position.get("strategy") != "spot_accumulation":
        raise HTTPException(400, "Cette position n'est pas en mode Spot-Accumulation")
    if body.target_price is not None and body.target_price <= (state.current_price or state.position["entry"]):
        raise HTTPException(400, "L'objectif doit être supérieur au prix actuel")
    state.position["target_price"] = body.target_price
    bot._save_open_positions()
    ticker = be.ticker_from_slot_key(body.trade_id)
    _push_log("info", f"[{ticker}] 🌱 Objectif Spot-Accum modifié manuellement : {'$'+str(body.target_price) if body.target_price else 'retiré (trailing seul)'}")
    return {"ok": True, "target_price": body.target_price}


class SpotAccumTrailingArmBody(BaseModel):
    trade_id: str
    trailing_arm_price: Optional[float] = None  # None = retire ce seuil, garde seulement le seuil de PnL


@app.put("/api/spot-accum/trailing-arm")
def put_spot_accum_trailing_arm(body: SpotAccumTrailingArmBody, email: str = Depends(require_user)):
    """v4.49 — SUR DEMANDE EXPLICITE : permet de modifier le seuil de PRIX
    (structurel, 70% de la distance support-resistance par defaut) qui arme
    le trailing pour une position Spot-Accumulation DEJA OUVERTE. S ADDITIONNE
    au seuil de PnL (SPOT_ACCUM_TTP_ARM_PCT) — arme des que l un des deux est
    atteint. None retire ce seuil : seul le PnL% arme alors le trailing."""
    state = bot.states.get(body.trade_id)
    if not state or not state.position:
        raise HTTPException(404, "Aucune position ouverte pour cet identifiant")
    if state.position.get("strategy") != "spot_accumulation":
        raise HTTPException(400, "Cette position n'est pas en mode Spot-Accumulation")
    if body.trailing_arm_price is not None and body.trailing_arm_price <= (state.current_price or state.position["entry"]):
        raise HTTPException(400, "Le seuil doit être supérieur au prix actuel")
    state.position["trailing_arm_price"] = body.trailing_arm_price
    bot._save_open_positions()
    ticker = be.ticker_from_slot_key(body.trade_id)
    _push_log("info", f"[{ticker}] 🌱 Seuil d'armement trailing Spot-Accum modifié : {'$'+str(body.trailing_arm_price) if body.trailing_arm_price else 'retiré (seuil PnL seul)'}")
    return {"ok": True, "trailing_arm_price": body.trailing_arm_price}


# ─────────────────────────────────────────────────────────────────────────
#  BILAN / STATISTIQUES / RAPPORT
# ─────────────────────────────────────────────────────────────────────────
def _day_key(iso_str: str) -> str:
    return datetime.fromisoformat(iso_str).astimezone(timezone.utc).strftime("%d/%m")


def _aggregate(rows: List[Dict[str, Any]], base: float = None) -> Dict[str, Any]:
    total = len(rows)
    pn = [(r, _row_pnl(r)) for r in rows]          # v4.355 : net de frais si FEES_IN_STATS
    wins = [r for r, p in pn if p > 0]
    losses = [r for r, p in pn if p <= 0]
    gains = round(sum(p for r, p in pn if p > 0), 2)
    pertes = round(sum(p for r, p in pn if p <= 0), 2)
    net = round(gains + pertes, 2)
    win_rate = round(len(wins) / total * 100, 1) if total else 0
    # v4.2 — % du net par rapport au capital initial (base), quand fourni.
    net_pct = round(net / base * 100, 2) if base else None
    fees = sum(_trade_fee_usd(r) for r in rows)
    gross = sum((r.get("pnl") or 0.0) for r in rows)
    return {
        "total": total, "wins": len(wins), "losses": len(losses),
        "gains": gains, "pertes": pertes, "net": net, "win_rate": win_rate,
        "net_pct": net_pct,
        "fees": round(fees, 2), "net_gross": round(gross, 2), "fees_on": _fees_on(),   # v4.355
    }


def _compute_daily(rows: List[Dict[str, Any]], days: int = 7, base: float = None) -> List[Dict[str, Any]]:
    """v3.2 — Jour calendaire UTC fixe (00h00-23h59:59) : chaque trade est
    attribue au jour ou il a ete OUVERT (created_at), pas ferme (closed_at).
    Un trade ouvert juste avant minuit et ferme apres compte donc pour la
    journee de son ouverture — coherent avec le decoupage en jours fixes
    demande, sans qu un trade a cheval sur minuit ne soit "perdu" ou compte
    deux fois."""
    today = datetime.now(timezone.utc).date()
    buckets = {}
    for i in range(days):
        d = today - timedelta(days=i)  # v3.2 — du plus recent (aujourd hui) au plus vieux
        buckets[d.strftime("%d/%m")] = []
    for r in rows:
        if not r["created_at"]:
            continue
        try:
            key = _day_key(r["created_at"])
        except Exception:
            continue
        if key in buckets:
            buckets[key].append(r)
    out = []
    for day, day_rows in buckets.items():
        agg = _aggregate(day_rows, base)
        out.append({"day": day, **agg})
    return out


def _compute_by_coin(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_coin: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_coin.setdefault(r["coin"], []).append(r)
    out = []
    for coin, coin_rows in by_coin.items():
        agg = _aggregate(coin_rows)
        wins = [r for r in coin_rows if _row_pnl(r) > 0]
        losses = [r for r in coin_rows if _row_pnl(r) <= 0]
        avg_gain = round(sum(_row_pnl(r) for r in wins) / len(wins), 2) if wins else 0
        avg_loss = round(sum(_row_pnl(r) for r in losses) / len(losses), 2) if losses else 0
        total_minutes = 0
        for r in coin_rows:
            try:
                opened = datetime.fromisoformat(r["created_at"])
                closed = datetime.fromisoformat(r["closed_at"]) if r["closed_at"] else opened
                total_minutes += int((closed - opened).total_seconds() / 60)
            except Exception:
                pass
        out.append({
            "coin": coin, **agg, "avg_gain": avg_gain, "avg_loss": avg_loss,
            "total_minutes": total_minutes,
        })
    out.sort(key=lambda c: c["net"], reverse=True)
    return out


@app.get("/api/report/daily-table")
def get_daily_table(email: str = Depends(require_user)):
    """Rapport journalier sur le jour calendaire UTC FIXE en cours
    (00h00:00 a 23h59:59) : gains, pertes, nombre de trades et performance
    par actif + une ligne total. v3.2 — attribution par date d OUVERTURE
    (created_at) : un trade ouvert aujourd hui mais ferme demain (ou plus
    tard) compte pour la journee de son ouverture, pas de sa fermeture —
    coherent avec le decoupage en jours fixes (plus de session glissante
    de 24h ni de blocage en fin de journee). Concu pour etre telecharge
    en tableau (CSV) depuis l interface."""
    closed = db.get_all_closed_trades(since=db.get_meta("stats_reset_at"))  # v4.306 — point de reinitialisation
    today_str = datetime.now(timezone.utc).strftime("%d/%m")
    day_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    recent = [r for r in closed if r["created_at"] and _day_key(r["created_at"]) == today_str]

    initial_balance = float(db.get_meta("initial_balance", cfg["CAPITAL_USD"])) or 1.0
    by_coin = _compute_by_coin(recent)
    for row in by_coin:
        row["performance_pct"] = round(row["net"] / initial_balance * 100, 3)

    total = _aggregate(recent)
    total["performance_pct"] = round(total["net"] / initial_balance * 100, 3)

    return {
        "period_start": day_start.isoformat(),
        "period_end": (day_start + timedelta(hours=23, minutes=59, seconds=59)).isoformat(),
        "by_coin": by_coin,
        "total": total,
    }


@app.get("/api/bilan")
def get_bilan(email: str = Depends(require_user)):
    closed = db.get_all_closed_trades(since=db.get_meta("stats_reset_at"))  # v4.306 — point de reinitialisation
    total_pnl_realized = sum(s.pnl for s in bot.states.values())  # realise cette session
    initial_balance = float(db.get_meta("initial_balance", cfg["CAPITAL_USD"]))
    open_positions = _open_positions()
    open_pnl = round(sum(p["pnl"] for p in open_positions), 2)
    # v3.2 — FIX : total_capital n incluait que le PnL REALISE (trades deja
    # fermes) — tant qu aucun trade n avait encore ete cloture dans la
    # session, "Total" et "Performance" restaient figes a 1000$/+0%, meme
    # avec des positions ouvertes clairement en profit/perte. On inclut
    # desormais aussi le PnL LATENT (mark-to-market), comme pour Paper
    # Trading — Capital total = valeur reelle actuelle du portefeuille.
    fees_all = _fees_all_closed()                       # v4.355 : frais cumules (reels si connus, sinon estimes)
    fees_adj = fees_all if _fees_on() else 0.0
    total_capital = bot.capital + total_pnl_realized + open_pnl - fees_adj
    performance_pct = round((total_capital - initial_balance) / initial_balance * 100, 2) if initial_balance else 0

    today_str = datetime.now(timezone.utc).strftime("%d/%m")
    # v3.2 — jour calendaire UTC fixe : attribution par date d OUVERTURE
    # (created_at), pas de fermeture — un trade ouvert aujourd hui mais
    # ferme demain compte pour aujourd hui.
    today_rows = [r for r in closed if r["created_at"] and _day_key(r["created_at"]) == today_str]

    return {
        "balance": round(bot.capital + total_pnl_realized - fees_adj - sum(p["size"] for p in open_positions), 2),
        "fees_total": round(fees_all, 2), "fees_on": _fees_on(), "fee_pct": float(cfg.get("FEE_ROUND_TRIP_PCT", 0.09)),   # v4.355
        "total_capital": round(total_capital, 2),
        "initial_balance": round(initial_balance, 2),
        "performance_pct": performance_pct,
        "open_pnl": open_pnl,
        "open_count": len(open_positions),
        "reset_at": db.get_meta("reset_at"),
        "running_seconds": round(_get_running_seconds()),
        "today": _aggregate(today_rows, initial_balance),
        "total": _aggregate(closed, initial_balance),
        "daily": _compute_daily(closed, days=7, base=initial_balance),
        "by_coin": _compute_by_coin(closed),
    }


@app.get("/api/hyperliquid/precision-check")
def get_precision_check(email: str = Depends(require_user)):
    """v4.101 — SUR DEMANDE EXPLICITE : verifie PROACTIVEMENT la precision
    reelle (szDecimals) de TOUS les actifs actuellement actifs, en une
    seule requete — au lieu de decouvrir les problemes de precision un par
    un via des echecs d ordres reels en production. Pour chaque actif,
    montre : szDecimals reel, un exemple de prix/taille formates avec la
    fonction du bot, et un avertissement si l actif semble absent des
    metadonnees Hyperliquid (cas ou un ordre echouerait a coup sur)."""
    if bot.info is None:
        raise HTTPException(503, "Connexion Hyperliquid non etablie — le bot doit etre demarre.")

    try:
        perp_map, spot_map = be._get_sz_decimals_map(bot.info)
    except Exception as e:
        raise HTTPException(503, f"Echec recuperation metadonnees Hyperliquid : {e}")

    active_coins = cfg.get("ACTIVE_COINS", [])
    results = []
    for ticker in active_coins:
        sz_dec = perp_map.get(ticker)
        row = {
            "ticker": ticker,
            "found_in_metadata": sz_dec is not None,
            "sz_decimals": sz_dec if sz_dec is not None else 4,
        }
        if sz_dec is None:
            row["warning"] = "Actif absent des metadonnees perp Hyperliquid — un ordre reel echouerait probablement (repli sur 4 decimales par defaut, potentiellement incorrect)."
        else:
            # Exemple concret avec un prix fictif typique, pour verifier que
            # le formatage produit un resultat coherent (pas de test reel
            # contre l API, juste une verification de calcul).
            sample_price = 1.23456789
            row["example_formatted_price"] = be.format_price_hl(sample_price, sz_dec, False)
            row["example_formatted_size"] = be.format_size_hl(1.23456789, sz_dec)
        results.append(row)

    missing = [r["ticker"] for r in results if not r["found_in_metadata"]]
    return {
        "total_active_coins": len(active_coins),
        "perp_assets_in_metadata": len(perp_map),
        "spot_assets_in_metadata": len(spot_map),
        "missing_from_metadata": missing,
        "assets": results,
    }


@app.get("/api/bilan-live")
def get_bilan_live(email: str = Depends(require_user)):
    """v4.90 — SUR DEMANDE EXPLICITE : bilan DEDIE au capital LIVE (reel,
    synchronise depuis Hyperliquid), completement separe du bilan paper
    ci-dessus — capital, PnL ouvert, performance, trades ouverts et win
    rate, tous calcules UNIQUEMENT a partir des trades reellement executes
    en live (trade_mode='live'), jamais melanges avec le paper."""
    closed_all = db.get_all_closed_trades()
    closed_live = [r for r in closed_all if r.get("trade_mode") == "live"]

    # v4.91 — SUR DEMANDE EXPLICITE : tente de recuperer le VRAI solde
    # Hyperliquid a CHAQUE consultation de cet onglet (pas seulement au
    # moment de basculer un mode en live) — pour reference, meme si aucun
    # mode n est encore live. Repli sur la derniere valeur connue
    # (live_capital_base) si la recuperation echoue (pas d identifiants
    # configures, ou API Hyperliquid indisponible).
    live_capital_base = getattr(bot, "live_capital_base", cfg.get("CAPITAL_USD", 0))
    hyperliquid_reachable = False
    hyperliquid_error = None
    if bot.info is None:
        hyperliquid_error = "Connexion Hyperliquid non etablie (bot.info absent) — verifiez que le bot a bien demarre."
    elif not cfg.get("WALLET_ADDRESS"):
        hyperliquid_error = "Adresse de wallet non configuree (WALLET_ADDRESS)."
    else:
        # v4.94 — FIX BUG CRITIQUE : documentation Hyperliquid confirmee par
        # recherche — "For API users, unified account and portfolio margin
        # shows all balances and holds in the spot clearinghouse state.
        # Individual perp dex user states are not meaningful." En mode
        # compte UNIFIE (celui de l utilisateur), marginSummary.accountValue
        # (endpoint perps) renvoie systematiquement $0, MEME avec des fonds
        # reels — le vrai solde se trouve dans le solde SPOT en USDC.
        # Interroge desormais LES DEUX endpoints et utilise le plus eleve
        # des deux (couvre a la fois les comptes Standard/Manual, ou les
        # fonds perps sont reellement separes, et les comptes Unified/
        # Portfolio Margin, ou le solde perps est toujours a 0 par design).
        try:
            perps_balance = 0.0
            perps_error = None
            try:
                raw_state = bot.info.user_state(cfg["WALLET_ADDRESS"])
                margin_summary = raw_state.get("marginSummary") if isinstance(raw_state, dict) else None
                if margin_summary and "accountValue" in margin_summary:
                    perps_balance = float(margin_summary["accountValue"])
                else:
                    perps_error = f"Reponse perps inattendue : {str(raw_state)[:200]}"
            except Exception as e:
                perps_error = f"{type(e).__name__}: {e}"

            spot_balance = 0.0
            spot_error = None
            try:
                spot_state = bot.info.spot_user_state(cfg["WALLET_ADDRESS"])
                balances = spot_state.get("balances", []) if isinstance(spot_state, dict) else []
                usdc_entry = next((b for b in balances if b.get("coin") == "USDC"), None)
                if usdc_entry:
                    spot_balance = float(usdc_entry.get("total", 0))
                else:
                    spot_error = f"Pas d'entree USDC dans le solde spot : {str(balances)[:200]}"
            except Exception as e:
                spot_error = f"{type(e).__name__}: {e}"

            fresh_balance = max(perps_balance, spot_balance)
            balance_source = "perps" if perps_balance >= spot_balance else "spot (compte probablement en mode Unifié)"

            if perps_error and spot_error:
                hyperliquid_error = f"Echec des deux endpoints — perps: {perps_error} | spot: {spot_error}"
            elif fresh_balance >= 0:
                live_capital_base = fresh_balance
                bot.live_capital_base = fresh_balance
                hyperliquid_reachable = True
                if fresh_balance == 0:
                    wallet_display = cfg["WALLET_ADDRESS"]
                    wallet_masked = f"{wallet_display[:6]}...{wallet_display[-4:]}" if len(wallet_display) > 12 else wallet_display
                    hyperliquid_error = f"Solde $0 sur les DEUX comptes (perps ET spot) pour l'adresse {wallet_masked} — verifiez que c'est bien la bonne adresse."
                else:
                    hyperliquid_error = f"Source du solde utilise : {balance_source} (perps: ${perps_balance:.2f}, spot USDC: ${spot_balance:.2f})"
        except Exception as e:
            import traceback
            hyperliquid_error = f"{type(e).__name__}: {e}"
            print(f"[BILAN-LIVE] Erreur complete :\n{traceback.format_exc()}")

    total_pnl_realized_live = sum(s.live_pnl for s in bot.states.values())

    open_positions = _open_positions()
    open_positions_live = [p for p in open_positions if p.get("effective_mode") == "live"]
    open_pnl_live = round(sum(p["pnl"] for p in open_positions_live), 2)

    total_capital_live = live_capital_base + total_pnl_realized_live + open_pnl_live
    performance_pct_live = round((total_capital_live - live_capital_base) / live_capital_base * 100, 2) if live_capital_base else 0

    stats_live = _aggregate(closed_live, live_capital_base)

    return {
        "hyperliquid_capital": round(live_capital_base, 2),
        "hyperliquid_reachable": hyperliquid_reachable,
        "hyperliquid_error": hyperliquid_error,
        "total_capital_live": round(total_capital_live, 2),
        "open_pnl": open_pnl_live,
        "open_count": len(open_positions_live),
        "performance_pct": performance_pct_live,
        "win_rate": stats_live["win_rate"],
        "total_trades": stats_live["total"],
        "wins": stats_live["wins"],
        "losses": stats_live["losses"],
        "net_realized": stats_live["net"],
        "open_positions": open_positions_live,
    }


@app.get("/api/stats/daily")
def get_stats_daily(email: str = Depends(require_user)):
    # v3.2 — FIX : l onglet Performance attend un OBJET structure precis
    # (summary/daily/wins/losses avec des noms de champs specifiques comme
    # total_wins_usdc, net_pnl, close_reason...), completement different de
    # ce que renvoyait cette route auparavant (une simple liste avec les
    # noms de _aggregate) — d ou la page blanche sans aucune donnee.
    closed = db.get_all_closed_trades(since=db.get_meta("stats_reset_at"))  # v4.306 — point de reinitialisation
    wins_rows   = [r for r in closed if _row_pnl(r) > 0]          # v4.355 : net de frais si FEES_IN_STATS
    losses_rows = [r for r in closed if _row_pnl(r) <= 0]
    total_wins_usdc = round(sum(_row_pnl(r) for r in wins_rows), 2)
    total_losses_usdc = round(sum(_row_pnl(r) for r in losses_rows), 2)
    win_rate = round(len(wins_rows) / len(closed) * 100, 1) if closed else 0

    today = datetime.now(timezone.utc).date()
    buckets = {}
    for i in range(7):
        d = today - timedelta(days=6 - i)
        buckets[d.strftime("%d/%m")] = []
    for r in closed:
        if not r["closed_at"]:
            continue
        try:
            key = _day_key(r["closed_at"])
        except Exception:
            continue
        if key in buckets:
            buckets[key].append(r)

    daily = []
    for day, day_rows in buckets.items():
        dw = [r for r in day_rows if _row_pnl(r) > 0]
        dl = [r for r in day_rows if _row_pnl(r) <= 0]
        daily.append({
            "day": day,
            "wins": len(dw),
            "total_wins_usdc": round(sum(_row_pnl(r) for r in dw), 2),
            "losses": len(dl),
            "total_losses_usdc": round(sum(_row_pnl(r) for r in dl), 2),
            "net_pnl": round(sum(_row_pnl(r) for r in day_rows), 2),
            "fees": round(sum(_trade_fee_usd(r) for r in day_rows), 2),
        })

    def _fmt(r):
        return {
            "action": r["action"], "coin": r["coin"],
            "close_reason": r["reason"] or "?",
            "pnl": r["pnl"], "pnl_net": round(_row_pnl(r), 4), "closed_at": r["closed_at"],
        }

    return {
        "summary": {
            "total_wins": len(wins_rows),
            "total_wins_usdc": total_wins_usdc,
            "total_losses": len(losses_rows),
            "total_losses_usdc": total_losses_usdc,
            "win_rate": win_rate,
        },
        "daily": daily,
        "wins": [_fmt(r) for r in sorted(wins_rows, key=lambda r: r["closed_at"] or "", reverse=True)],
        "losses": [_fmt(r) for r in sorted(losses_rows, key=lambda r: r["closed_at"] or "", reverse=True)],
    }


@app.post("/api/cleanup")
def cleanup(email: str = Depends(require_user)):
    print(f"[AUDIT] /api/cleanup appele par {email} a {datetime.now(timezone.utc).isoformat()}")
    # v3.2 — FIX : le bouton de l interface attend un champ "message" (via
    # alert(r.message)), jamais renvoye jusqu ici (d ou l impression que le
    # bouton "ne faisait rien"). Utilise desormais cleanup_signals, qui
    # cible specifiquement les doublons/orphelins "ouverts" (voir db.py) —
    # les trades reellement fermes (historique du Bilan) ne sont jamais
    # touches par ce nettoyage.
    # Retrouve l ID exact (pas juste le coin) de chaque position reellement
    # ouverte en memoire, pour proteger UNIQUEMENT cette ligne precise —
    # les eventuels AUTRES doublons du meme coin restent nettoyables.
    protected_ids = _compute_protected_trade_ids()
    duplicates, stale = db.cleanup_signals(stale_hours=24, protected_ids=protected_ids)
    old_closed = db.delete_trades_older_than(30)
    total = duplicates + stale + old_closed
    if total == 0:
        message = "Rien a nettoyer — aucun signal en double ou orphelin trouve."
    else:
        parts = []
        if duplicates:
            parts.append(f"{duplicates} doublon(s) ouvert(s)")
        if stale:
            parts.append(f"{stale} signal(aux) orphelin(s) (>24h sans fermeture)")
        if old_closed:
            parts.append(f"{old_closed} trade(s) ferme(s) de plus de 30 jours")
        message = "Nettoyage termine : " + ", ".join(parts) + "."
    return {"ok": True, "message": message, "duplicates": duplicates, "stale": stale, "old_closed": old_closed}


@app.post("/api/reset-all")
def reset_all(email: str = Depends(require_user)):
    print(f"[AUDIT] /api/reset-all appele par {email} a {datetime.now(timezone.utc).isoformat()}")
    if bot.trading_enabled:
        raise HTTPException(400, "Arretez le bot avant une reinitialisation complete")
    # v3.2 — FIX : preserve les identifiants Hyperliquid/Finnhub (wallet, cle
    # privee, cle Finnhub) avant de tout effacer, puis les restaure apres —
    # ce ne sont pas des "donnees de trading" a effacer par une remise a
    # zero du portefeuille/historique, ce sont des identifiants de connexion.
    # Un utilisateur ayant clique par erreur sur ce bouton (juste en dessous
    # du nettoyage des doublons) se retrouvait auparavant a devoir tout
    # ressaisir sans comprendre pourquoi.
    preserved_keys = ("PRIVATE_KEY", "WALLET_ADDRESS", "FINNHUB_API_KEY")
    preserved = {k: cfg.get(k) for k in preserved_keys if cfg.get(k)}
    db.clear_all_trades()
    # v4.65 — SUR DEMANDE EXPLICITE : "on ne touche pas aux reglages" — ce
    # bouton effacait AUSSI tous les reglages personnalises
    # (db.clear_config_overrides() + remise a zero complete de cfg), ce qui
    # contredit frontalement l intention exprimee. Retire : seules les
    # DONNEES DE TRADING (trades, positions, portefeuille) sont effacees
    # desormais, la configuration (SL/TTP par mode, ADX, listes d actifs,
    # tous les reglages avances) reste intacte.
    for state in bot.states.values():
        state.position = None
        state.pnl = 0.0
        state.trades = 0
        state.wins = 0
        state.closed_trades.clear()
    # v4.121 — SUR DEMANDE EXPLICITE : Accumulation a desormais son PROPRE
    # emplacement (bot.accum_states), jamais reinitialise ci-dessus
    # auparavant.
    for accum_state in bot.accum_states.values():
        accum_state.position = None
        accum_state.pnl = 0.0
        accum_state.trades = 0
        accum_state.wins = 0
        accum_state.closed_trades.clear()
    for k, v in preserved.items():
        cfg[k] = v
    bot.cfg = cfg
    bot.capital = cfg["CAPITAL_USD"]
    bot.sessions = 0
    bot.total_pnl_all = 0.0
    bot.batch_entry_sizes = {}  # v4.297 — un lot par pot
    bot.clear_all_persisted_files()
    be.save_capital(bot.capital, 0, 0.0)
    be.save_batch_entry_size(None)
    db.set_meta("reset_at", db.now_iso())
    db.set_meta("initial_balance", str(bot.capital))
    db.set_meta("total_running_seconds", "0")
    db.set_meta("running_since", "")
    log_buffer.clear()
    return {"ok": True, "message": "Reinitialisation complete effectuee : signaux, trades, historique et portefeuille remis a zero."}


# ─────────────────────────────────────────────────────────────────────────
#  FICHIERS STATIQUES (index.html)
# ─────────────────────────────────────────────────────────────────────────
_HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")


@app.get("/", response_class=HTMLResponse)
def index():
    with open(_HTML_PATH, "r", encoding="utf-8") as f:
        return f.read()


# ── v4.372 — application installable sur smartphone (PWA) : icones generees en code, aucun fichier a deposer en plus ──
_ICON_CACHE = {}


def _png_icon(size):
    """Icone PNG sans dependance : fond sombre + lettre H bleue. Cache en memoire."""
    if size in _ICON_CACHE:
        return _ICON_CACHE[size]
    import struct
    import zlib
    bg, fg = (10, 14, 26), (59, 130, 246)
    lo, hi = int(size * 0.28), int(size * 0.72)          # zone de la lettre
    bar = max(int(size * 0.11), 2)
    mid0, mid1 = int(size * 0.47), int(size * 0.47) + bar
    rows = []
    for y in range(size):
        row = bytearray([0])
        for x in range(size):
            in_h = lo <= y < hi and (lo <= x < lo + bar or hi - bar <= x < hi or (mid0 <= y < mid1 and lo <= x < hi))
            row += bytes(fg if in_h else bg)
        rows.append(bytes(row))

    def chunk(tag, data):
        c = struct.pack(">I", len(data)) + tag + data
        return c + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
    png = b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0)) \
        + chunk(b"IDAT", zlib.compress(b"".join(rows), 9)) + chunk(b"IEND", b"")
    _ICON_CACHE[size] = png
    return png


@app.get("/icon-192.png")
def pwa_icon_192():
    return Response(content=_png_icon(192), media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})


@app.get("/icon-512.png")
def pwa_icon_512():
    return Response(content=_png_icon(512), media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})


@app.get("/apple-touch-icon.png")
def pwa_icon_apple():
    return Response(content=_png_icon(180), media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})


@app.get("/manifest.webmanifest")
def pwa_manifest():
    return Response(content=json.dumps({
        "name": "HyperBot", "short_name": "HyperBot", "start_url": "/", "scope": "/", "display": "standalone",
        "background_color": "#0a0e1a", "theme_color": "#0a0e1a", "lang": "fr",
        "icons": [{"src": "/icon-192.png", "sizes": "192x192", "type": "image/png"},
                  {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png"}]}),
        media_type="application/manifest+json")


@app.get("/sw.js")
def pwa_service_worker():
    # Aucun cache : le service worker ne sert qu'a rendre l'application installable (Android). Toujours la version en ligne.
    js = ("self.addEventListener('install',function(){self.skipWaiting();});"
          "self.addEventListener('activate',function(e){e.waitUntil(self.clients.claim());});"
          "self.addEventListener('fetch',function(e){e.respondWith(fetch(e.request));});"
          # v4.376 — alertes : affichage de la notification recue, puis ouverture de l'application au toucher
          "self.addEventListener('push',function(e){var d={};try{d=e.data?e.data.json():{};}catch(x){d={title:'HyperBot',body:e.data?e.data.text():''};}"
          "e.waitUntil(self.registration.showNotification(d.title||'HyperBot',{body:d.body||'',icon:'/icon-192.png',badge:'/icon-192.png',tag:d.tag||undefined,renotify:!!d.tag,data:{url:d.url||'/'}}));});"
          "self.addEventListener('notificationclick',function(e){e.notification.close();var u=(e.notification.data&&e.notification.data.url)||'/';"
          "e.waitUntil(self.clients.matchAll({type:'window',includeUncontrolled:true}).then(function(cs){for(var i=0;i<cs.length;i++){if('focus' in cs[i]){try{cs[i].postMessage({tab:(u.indexOf('tab=')>=0?u.split('tab=')[1]:null)});}catch(x){}return cs[i].focus();}}"
          "return self.clients.openWindow(u);}));});")
    return Response(content=js, media_type="application/javascript", headers={"Cache-Control": "no-cache"})


class PushSubBody(BaseModel):
    subscription: Dict[str, Any]


class PushUnsubBody(BaseModel):
    endpoint: str


class PushPrefsBody(BaseModel):
    prefs: Dict[str, Any]


@app.get("/api/push/status")
def push_status(email: str = Depends(require_user)):
    return push.status()


@app.post("/api/push/subscribe")
def push_subscribe(body: PushSubBody, email: str = Depends(require_user)):
    if not push.available():
        raise HTTPException(status_code=503, detail="Alertes indisponibles : la bibliotheque `cryptography` est absente (voir requirements.txt).")
    try:
        push.add_sub(body.subscription)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return push.status()


@app.post("/api/push/unsubscribe")
def push_unsubscribe(body: PushUnsubBody, email: str = Depends(require_user)):
    push.remove_sub(body.endpoint)
    return push.status()


@app.put("/api/push/prefs")
def push_prefs(body: PushPrefsBody, email: str = Depends(require_user)):
    try:
        push.set_prefs(body.prefs)
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    return push.status()


class PushHistDeleteBody(BaseModel):
    ids: List[int] = []
    everything: bool = False


@app.post("/api/push/history/delete")
def push_history_delete(body: PushHistDeleteBody, email: str = Depends(require_user)):
    n = push.delete_history(body.ids, body.everything)
    items = push.history(50)
    for it in items:
        o = it.get("opp")
        it["busy"] = bool(o and bot.manual.ticker_busy(o.get("ticker")))
        try:      # v4.386 : prix actuel, pour juger si l opportunite vaut encore la peine
            it["now_price"] = float((bot.all_mids or {}).get(o.get("ticker"))) if o and o.get("ticker") in (bot.all_mids or {}) else None
        except Exception:
            it["now_price"] = None
    return {"deleted": n, "items": items}


@app.get("/api/push/history")
def push_history(limit: int = Query(50, ge=1, le=100), email: str = Depends(require_user)):
    items = push.history(limit)
    for it in items:      # v4.382 : le bouton « Preparer le trade » disparait si une position est deja ouverte sur l actif
        o = it.get("opp")
        it["busy"] = bool(o and bot.manual.ticker_busy(o.get("ticker")))
        try:      # v4.386 : prix actuel, pour juger si l opportunite vaut encore la peine
            it["now_price"] = float((bot.all_mids or {}).get(o.get("ticker"))) if o and o.get("ticker") in (bot.all_mids or {}) else None
        except Exception:
            it["now_price"] = None
    return {"items": items}


@app.post("/api/push/test")
def push_test(email: str = Depends(require_user)):
    if not push.status()["devices"]:
        raise HTTPException(status_code=400, detail="Aucun appareil abonne : active d'abord les alertes sur cet appareil.")
    push.send_test()
    return {"ok": True}


@app.get("/health")
def health():
    return {
        "ok": True,
        "bot_running": bot.running,  # v4.14 — moteur (collecte/WS), toujours actif independamment du trading
        "trading_enabled": bot.trading_enabled,
        "version": be.BOT_VERSION,
        "build": be.BOT_BUILD,
        "data_dir": _DATA_DIR,
        "data_dir_configured": _DATA_DIR_CONFIGURED,
        "boot_count": BOOT_COUNT,
        "persistence_note": (
            "boot_count doit augmenter (1, 2, 3...) a chaque redeploiement. "
            "S il repart toujours a 1, le Volume Railway n est pas monte "
            "correctement (voir HYPERBOT_DATA_DIR)."
        ),
    }
