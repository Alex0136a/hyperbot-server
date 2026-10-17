"""
db.py — Persistance SQLite pour HyperBot Web.

Toutes les données (utilisateurs, trades fermés, réglages personnalisés)
vivent dans un seul fichier SQLite. Pour survivre aux redéploiements sur
Railway, ce fichier doit se trouver sur un Volume monté (voir README.md) —
sinon il est remis à zéro à chaque nouveau déploiement, comme le reste du
système de fichiers du conteneur.

Aucune dépendance externe : uniquement la bibliothèque standard (sqlite3).
"""
import sqlite3
import os
import json
import threading
from datetime import datetime, timezone, timedelta

DB_PATH = os.environ.get("HYPERBOT_DB_PATH", "hyperbot.db")

_lock = threading.Lock()  # sqlite3 + threads : on sérialise les écritures


class _ClosingConnection(sqlite3.Connection):
    """v4.264 — `with sqlite3.connect(...) as conn` valide la transaction
    mais NE FERME PAS la connexion : chaque appel en laissait une ouverte
    jusqu au ramasse-miettes. Cette sous-classe valide (ou annule) puis
    ferme reellement la connexion en sortie de bloc."""
    def __exit__(self, exc_type, exc, tb):
        try:
            return super().__exit__(exc_type, exc, tb)
        finally:
            self.close()


def _connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=15, factory=_ClosingConnection)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _lock, _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        # v4.244 — SUR DEMANDE EXPLICITE : journal PERSISTANT (survit aux
        # redemarrages, contrairement au log_buffer en memoire limite a
        # 3000 lignes/quelques heures) — permet une vraie consultation
        # historique depuis l interface, sans dependre des logs Railway.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS log_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_log_history_ts ON log_history(ts)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coin TEXT NOT NULL,
                action TEXT NOT NULL,
                confidence REAL,
                leverage INTEGER,
                position_size_pct REAL,
                risk_reward REAL,
                timeframe TEXT,
                entry_price REAL NOT NULL,
                stop_loss REAL,
                take_profit1 REAL,
                take_profit2 REAL,
                exit_price REAL,
                pnl REAL,
                reason TEXT,
                created_at TEXT NOT NULL,
                closed_at TEXT,
                rsi REAL,
                entry_reasons TEXT,
                confidence_breakdown TEXT,
                strategy TEXT,
                peak_pnl REAL,
                peak_pnl_pct REAL,
                size_usd REAL,
                sl_pct_used REAL,
                ttp_arm1_pct_used REAL,
                adaptive_sl_ttp INTEGER,
                trade_mode TEXT
            )
        """)
        # v4.264 — FIX : ces tables etaient creees APRES les migrations qui
        # les lisent — une base NEUVE (premier deploiement, nouveau Volume)
        # faisait planter init_db ("no such table: config_overrides").
        conn.execute("""
            CREATE TABLE IF NOT EXISTS config_overrides (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        # v4.349 — enregistreur de flux : tranches de 5 minutes par actif + battement de coeur
        conn.execute("""
            CREATE TABLE IF NOT EXISTS flow_5m (
                ticker TEXT NOT NULL, t0 INTEGER NOT NULL,
                n_buy INTEGER, n_sell INTEGER, buy_usd REAL, sell_usd REAL,
                big_buy_usd REAL, big_sell_usd REAL, max_trade_usd REAL,
                px_open REAL, px_high REAL, px_low REAL, px_close REAL,
                PRIMARY KEY (ticker, t0)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_flow_5m_t0 ON flow_5m (t0)")
        conn.execute("CREATE TABLE IF NOT EXISTS flow_5m_alive (t0 INTEGER PRIMARY KEY)")
        # v4.353 — journal du SCALP Forex : chaque setup detecte (pris OU filtre) + son issue 60 min plus tard
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scalp_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, asset TEXT, side TEXT, setup TEXT,
                level_kinds TEXT, level_price REAL, level_w INTEGER, price REAL, spread_pct REAL, atr5_pct REAL,
                bias TEXT, flow REAL, hour_utc INTEGER, decision TEXT, sl REAL, tp REAL, risk_pct REAL, tp_pct REAL,
                rr REAL, net_rr REAL, cost_pct REAL, hypothetical INTEGER DEFAULT 0, trade_uid TEXT,
                evaluated INTEGER DEFAULT 0, outcome TEXT, hit_min REAL, mfe5 REAL, mae5 REAL, mfe15 REAL, mae15 REAL,
                mfe60 REAL, mae60 REAL, fin15 REAL, fin60 REAL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scalp_events_ts ON scalp_events (ts)")
        # v4.359 — journal du SWING : chaque signal H4 en zone Daily (pris ou filtre) + son issue 24 h / 72 h plus tard
        conn.execute("""
            CREATE TABLE IF NOT EXISTS swing_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, signal_t INTEGER NOT NULL,
                asset TEXT, side TEXT, mode TEXT, pattern TEXT, trend TEXT,
                zone_low REAL, zone_high REAL, zone_touches INTEGER, price REAL, entry_ref REAL, atr_pct REAL,
                flow REAL, hour_utc INTEGER, age_min REAL, decision TEXT, prio INTEGER DEFAULT 1,
                sl REAL, tp REAL, risk_pct REAL, reward_pct REAL, rr REAL, hypothetical INTEGER DEFAULT 0, trade_uid TEXT,
                evaluated INTEGER DEFAULT 0, outcome TEXT, hit_h REAL, mfe24 REAL, mae24 REAL, mfe72 REAL, mae72 REAL,
                fin24 REAL, fin72 REAL, covered_h REAL, r_est REAL,
                UNIQUE (asset, side, signal_t)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_swing_events_ts ON swing_events (ts)")
        # v4.362 — journal du SWING SUPPORT (paper) : une ligne par signal 1 h (pris ou non) ; si pris, la sortie et ce qui s est
        # passe ensuite (1 h / 4 h / 24 h, et l objectif ou le stop aurait-il ete touche : la sortie anticipee etait-elle trop tot ?)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS supsw_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, signal_t INTEGER NOT NULL,
                asset TEXT, side TEXT, setup TEXT, setup_kind TEXT, trend TEXT, weekly_trend TEXT, btc_trend TEXT,
                funding_ann REAL, flow REAL, atr_pct REAL, hour_utc INTEGER, zone_low REAL, zone_high REAL, zone_touches INTEGER,
                held INTEGER, pierce_pct REAL, price REAL, entry_ref REAL, sl REAL, tp REAL, risk_pct REAL, reward_pct REAL,
                decision TEXT, prio INTEGER DEFAULT 1, trade_uid TEXT,
                evaluated INTEGER DEFAULT 0, outcome TEXT, hit_h REAL, mfe24 REAL, mae24 REAL, mfe72 REAL, mae72 REAL,
                fin24 REAL, fin72 REAL, covered_h REAL, inval REAL,
                exit_ts INTEGER, exit_price REAL, exit_reason TEXT, early_exit INTEGER, pnl_pct REAL, hold_h REAL,
                flow_exit REAL, h4_intact_exit TEXT, d1_trend_exit TEXT,
                post_eval INTEGER DEFAULT 0, fin1h REAL, fin4h REAL, fin24h REAL, after_outcome TEXT, after_hit_h REAL,
                after_mfe REAL, after_mae REAL, too_early INTEGER,
                UNIQUE (asset, signal_t)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_supsw_events_ts ON supsw_events (ts)")
        # v4.365 — journal du module TENDANCE CONFIRMEE (paper) : entrees (prises ou refusees), sorties, retard de l entree
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trendf_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, signal_t INTEGER NOT NULL,
                asset TEXT, side TEXT, d1_trend TEXT, h4_trend TEXT, weekly_trend TEXT, btc_trend TEXT, funding_ann REAL, flow REAL,
                atr_pct REAL, ext_atr REAL, hour_utc INTEGER, price REAL, entry_ref REAL, sl REAL, risk_pct REAL, entry_mode INTEGER,
                decision TEXT, prio INTEGER DEFAULT 1, trade_uid TEXT,
                evaluated INTEGER DEFAULT 0, outcome TEXT, hit_h REAL, mfe24 REAL, mae24 REAL, mfe72 REAL, mae72 REAL,
                fin24 REAL, fin72 REAL, covered_h REAL,
                exit_ts INTEGER, exit_price REAL, exit_reason TEXT, pnl_pct REAL, hold_h REAL, peak_pct REAL,
                d1_trend_exit TEXT, h4_trend_exit TEXT,
                post_eval INTEGER DEFAULT 0, fin1h REAL, fin4h REAL, fin24h REAL, after_mfe REAL, after_mae REAL,
                UNIQUE (asset, side, signal_t)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trendf_events_ts ON trendf_events (ts)")
        if "inval" not in [r[1] for r in conn.execute("PRAGMA table_info(supsw_events)").fetchall()]:
            conn.execute("ALTER TABLE supsw_events ADD COLUMN inval REAL")      # v4.363 : niveau d invalidation (sortie anticipee)
        # v4.370 : alertes de degradation mesurees en cours de trade (aucune sortie n en depend)
        _sup_cols = [r[1] for r in conn.execute("PRAGMA table_info(supsw_events)").fetchall()]
        for _c, _t in (("alerts_max", "INTEGER"), ("alerts_exit", "INTEGER"), ("alerts_json", "TEXT"),
                   ("btc_chg24", "REAL"), ("btc_h4", "TEXT"), ("h4_trend", "TEXT"),
                   ("peak_pct", "REAL"), ("trail_armed", "INTEGER"),
                   ("res_low", "REAL"), ("res_dist_pct", "REAL"),
                   ("chg_recent", "REAL"), ("btc_chg_recent", "REAL")):      # v4.387 : contexte de marche
            if _c not in _sup_cols:
                conn.execute(f"ALTER TABLE supsw_events ADD COLUMN {_c} {_t}")
        # Migration : ajoute la colonne rsi si la table trades existait deja
        # (CREATE TABLE IF NOT EXISTS n ajoute pas les colonnes manquantes a
        # une table deja creee par une version anterieure du code).
        existing_cols = [r[1] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]
        if "rsi" not in existing_cols:
            conn.execute("ALTER TABLE trades ADD COLUMN rsi REAL")
        if "entry_reasons" not in existing_cols:
            conn.execute("ALTER TABLE trades ADD COLUMN entry_reasons TEXT")
        if "confidence_breakdown" not in existing_cols:
            # v4.2 — detail brut (JSON) de quel indicateur etait confirme a
            # l entree de CE trade, necessaire pour calibrer statistiquement
            # les poids de CONFIDENCE_WEIGHTS a partir des resultats reels
            # (voir api.py, module de calibration de la confiance).
            conn.execute("ALTER TABLE trades ADD COLUMN confidence_breakdown TEXT")
        if "strategy" not in existing_cols:
            # v4.8 — "normal" ou "accumulation", pour differencier les trades
            # du mode Accumulation (achat pres du support / vente pres de la
            # resistance) des trades normaux (RSI/tendance) dans l historique.
            conn.execute("ALTER TABLE trades ADD COLUMN strategy TEXT")
        if "peak_pnl" not in existing_cols:
            # v4.15 — Pic de PnL latent ($) atteint pendant la vie du trade,
            # avant sa fermeture — permet de voir combien de gain a ete
            # "rendu" entre le sommet et la sortie reelle (SL ou TTP).
            conn.execute("ALTER TABLE trades ADD COLUMN peak_pnl REAL")
        if "peak_pnl_pct" not in existing_cols:
            # v4.17 — meme pic, en % de mouvement de prix (prefere a l
            # affichage en $, plus comparable d un trade a l autre quel que
            # soit la taille ou le levier utilises).
            conn.execute("ALTER TABLE trades ADD COLUMN peak_pnl_pct REAL")
        if "size_usd" not in existing_cols:
            # v4.28 — taille reelle en $ (E) de CE trade precis, pour
            # affichage dans l historique (varie d un lot a l autre selon
            # le capital courant au moment de l ouverture).
            conn.execute("ALTER TABLE trades ADD COLUMN size_usd REAL")
        if "sl_pct_used" not in existing_cols:
            # v4.30 — seuils SL/TTP REELLEMENT appliques a CE trade (fixes ou
            # adaptatifs a l ATR, figes a l ouverture) — pour verifier a
            # posteriori quelle valeur a ete utilisee, notamment en mode adaptatif.
            conn.execute("ALTER TABLE trades ADD COLUMN sl_pct_used REAL")
        if "ttp_arm1_pct_used" not in existing_cols:
            conn.execute("ALTER TABLE trades ADD COLUMN ttp_arm1_pct_used REAL")
        if "adaptive_sl_ttp" not in existing_cols:
            conn.execute("ALTER TABLE trades ADD COLUMN adaptive_sl_ttp INTEGER")
        if "trade_mode" not in existing_cols:
            # v4.90 — SUR DEMANDE EXPLICITE : "paper" ou "live", mode REEL de
            # CE trade precis au moment de sa fermeture — permet de calculer
            # des statistiques (win rate, performance) separees par mode
            # reel, jamais melangees entre capital virtuel et capital reel.
            conn.execute("ALTER TABLE trades ADD COLUMN trade_mode TEXT")
        # v4.264 — INDEXATION DES TRADES A L OUVERTURE : identifiant unique
        # (aussi envoye a Hyperliquid comme cloid), emplacement du bot et
        # statut du cycle de vie (pending -> open -> ferme via closed_at).
        existing_cols = [r[1] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]
        if "trade_uid" not in existing_cols:
            conn.execute("ALTER TABLE trades ADD COLUMN trade_uid TEXT")
        if "slot_key" not in existing_cols:
            conn.execute("ALTER TABLE trades ADD COLUMN slot_key TEXT")
        if "is_accum_slot" not in existing_cols:
            conn.execute("ALTER TABLE trades ADD COLUMN is_accum_slot INTEGER")
        if "status" not in existing_cols:
            conn.execute("ALTER TABLE trades ADD COLUMN status TEXT")
        if "mtf_trend_intact_at_close" not in existing_cols:
            # v4.310 — SUR DEMANDE EXPLICITE : pour un trade top-down, la
            # tendance H4 D ORIGINE (celle qui justifiait l entree) est-elle
            # ENCORE dans le meme sens au moment precis de la fermeture ?
            # NULL = non applicable (trade non top-down) ou non determinable
            # (donnees H4 indisponibles) ; 1 = tendance encore intacte
            # (simple repli) ; 0 = tendance deja retournee (vrai changement
            # de regime). Objectif : distinguer objectivement les deux
            # hypotheses sur une serie de pertes SL/structure cassee.
            conn.execute("ALTER TABLE trades ADD COLUMN mtf_trend_intact_at_close INTEGER")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_uid ON trades(trade_uid) WHERE trade_uid IS NOT NULL")
        # v4.273 — TRADING MANUEL : ordres programmes et positions manuelles
        # (donnees completes en JSON, statut indexe).
        conn.execute("""
            CREATE TABLE IF NOT EXISTS manual_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT NOT NULL,
                data TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_manual_status ON manual_trades(status)")
        # v4.265 — SUIVI DETAILLE DES TRADES (export CSV) : source du prix de
        # sortie, frais et PnL reels Hyperliquid, et comportement du prix
        # 30/60 min APRES la sortie (le SL etait-il trop serre ? le TTP trop
        # precoce ?).
        for col, typ in (("exit_price_source", "TEXT"), ("fees_real", "REAL"), ("pnl_real_hl", "REAL"),
                         ("price_after_30m", "REAL"), ("price_after_60m", "REAL"),
                         ("high_60m", "REAL"), ("low_60m", "REAL"),
                         ("followup_status", "TEXT"), ("followup_at", "TEXT"),
                         # v4.269 — simulation minute par minute d un SL plus large
                         ("sim_sl_075", "TEXT"), ("sim_sl_100", "TEXT"), ("sim_sl_150", "TEXT"),
                         ("sim_status", "TEXT"),
                         # v4.281 — qualite du marche a l entree
                         ("vol_ratio", "REAL"), ("activity_ratio", "REAL"), ("flow_at_entry", "REAL"),
                         ("spread_at_entry", "REAL"),  # v4.282
                         # v4.290 — suivi apres SL sur 2 h
                         ("sim_sl_200", "TEXT"), ("sl_back_min", "REAL"), ("sl_mae_pct", "REAL"),
                         # v4.342 — chemin du PnL pendant le trade (Funding)
                         ("pnl_min_pct", "REAL"), ("pnl_min_after_touch_pct", "REAL"),
                         ("peak_at_min_after_pct", "REAL"), ("touch_delay_sec", "REAL"), ("path_complete", "INTEGER"),
                         ("sl_mark_120_pct", "REAL"), ("sim2_status", "TEXT")):
            if col not in existing_cols:
                conn.execute(f"ALTER TABLE trades ADD COLUMN {col} {typ}")
        if "fees_paid" not in existing_cols:
            # v4.186 — SUR DEMANDE EXPLICITE : frais REELS estimes payes a
            # Hyperliquid pour ce trade (ouverture + fermeture), uniquement
            # pour les trades LIVE — jamais pour le paper (simulation, pas
            # de frais reels).
            conn.execute("ALTER TABLE trades ADD COLUMN fees_paid REAL")
        # v4.171 — SUR DEMANDE EXPLICITE : renommage complet du mode "normal"
        # en "forex" (desormais dedie au forex/HIP-3) — migre les lignes
        # EXISTANTES en base, une seule fois (idempotent, sans effet si deja
        # applique). Les trades sans champ strategy (avant son introduction)
        # restent NULL ici — deja geres comme "forex" par defaut ailleurs
        # (clear_trades_by_strategy, api.py) sans necessiter de migration.
        conn.execute("UPDATE trades SET strategy = 'forex' WHERE strategy = 'normal'")
        # v4.171 — SUR DEMANDE EXPLICITE : migre aussi les reglages
        # PERSONNALISES deja enregistres (config_overrides) sous les anciens
        # noms NORMAL_* — sans ca, tout reglage que l utilisateur aurait
        # deja personnalise pour ce mode serait orphelin (le code cherche
        # desormais FOREX_*, ces lignes resteraient invisibles).
        key_renames = {
            "NORMAL_FOREX_SYMBOLS": "FOREX_MODE_SYMBOLS",
            "NORMAL_FOREX_ISOLATED_MARGIN": "FOREX_ISOLATED_MARGIN",
            "NORMAL_ANTI_RANGE_LOOKBACK": "FOREX_ANTI_RANGE_LOOKBACK",
            "NORMAL_ANTI_RANGE_MIN_PCT": "FOREX_ANTI_RANGE_MIN_PCT",
            "NORMAL_REQUIRE_ANTI_RANGE": "FOREX_REQUIRE_ANTI_RANGE",
            "NORMAL_TREND_STABILITY_CYCLES": "FOREX_TREND_STABILITY_CYCLES",
        }
        for old_key, new_key in key_renames.items():
            existing_new = conn.execute("SELECT 1 FROM config_overrides WHERE key=?", (new_key,)).fetchone()
            if not existing_new:
                conn.execute("UPDATE config_overrides SET key=? WHERE key=?", (new_key, old_key))
        # v4.171 (suite) — STRATEGY_TRADING_ENABLED et STRATEGY_MODE_OVERRIDE
        # sont des DICTIONNAIRES JSON persistes en UNE seule ligne, avec
        # "normal" comme cle INTERNE (ex: {"normal": false, ...}) — si l
        # utilisateur avait deja mis ce mode en pause, cette preference
        # serait sinon perdue/invisible (le code cherche desormais la cle
        # "forex" a l interieur de ce meme dictionnaire).
        for dict_key in ("STRATEGY_TRADING_ENABLED", "STRATEGY_MODE_OVERRIDE"):
            row = conn.execute("SELECT value FROM config_overrides WHERE key=?", (dict_key,)).fetchone()
            if row:
                try:
                    parsed = json.loads(row["value"])
                    if isinstance(parsed, dict) and "normal" in parsed and "forex" not in parsed:
                        parsed["forex"] = parsed.pop("normal")
                        conn.execute("UPDATE config_overrides SET value=? WHERE key=?", (json.dumps(parsed), dict_key))
                except (json.JSONDecodeError, TypeError):
                    pass
        # v4.184 — SUR DEMANDE EXPLICITE : force la mise a jour d un
        # reglage personnalise DEJA enregistre en base pour
        # SPOT_ACCUM_TTP_ARM_PCT — un ancien override (1.0, voire 3.0)
        # continuait de primer sur le nouveau defaut du code (0.4),
        # empechant le correctif de prendre effet malgre le redeploiement.
        # Supprime purement et simplement cette ligne : le defaut du code
        # (0.4) prendra alors le relais naturellement.
        conn.execute("DELETE FROM config_overrides WHERE key='SPOT_ACCUM_TTP_ARM_PCT' AND value IN ('1.0', '1', '3.0', '3')")
        # v4.201 — SUR DEMANDE EXPLICITE, FIX BUG CRITIQUE : un override
        # DEJA enregistre en base pour "SYMBOLS" (et "ACTIVE_COINS"),
        # datant d avant l ajout du forex cette session, continuait de
        # primer sur le nouveau defaut du code (qui inclut EUR/JPY/KRW/
        # DXY) — confirme par la ligne de demarrage montrant ces 4 tickers
        # absents malgre le code correctement mis a jour. Supprime ces deux
        # overrides s ils ne contiennent PAS "xyz:EUR" — le defaut du code
        # (avec forex inclus) prend alors le relais naturellement.
        # v4.208 — FIX BUG CRITIQUE : "FOREX_SYMBOLS" est en realite un
        # reglage PRE-EXISTANT et DIFFERENT (["PAXG"], delai de "chauffe"
        # du marche pour PAXG apres reouverture du forex traditionnel) —
        # la collision de nom avec ma nouvelle liste d isolation forex
        # (renommee "FOREX_MODE_SYMBOLS" pour eviter tout conflit futur)
        # expliquait le blocage persistant : le code lisait par erreur
        # cette valeur ["PAXG"] a la place de la vraie liste forex. Cible
        # desormais le bon nom, sans jamais toucher a "FOREX_SYMBOLS" (qui
        # garde sa valeur legitime pour la chauffe PAXG).
        conn.execute("DELETE FROM config_overrides WHERE key IN ('SYMBOLS', 'FOREX_MODE_SYMBOLS')")
        # ACTIVE_COINS, lui, reste modifiable par l utilisateur (Marches) —
        # au lieu de le supprimer entierement (ce qui effacerait ses choix
        # d activation/desactivation), on AJOUTE simplement les tickers
        # forex manquants a la liste EXISTANTE, sans toucher au reste.
        row_ac = conn.execute("SELECT value FROM config_overrides WHERE key='ACTIVE_COINS'").fetchone()
        if row_ac:
            try:
                parsed_ac = json.loads(row_ac["value"])
                if isinstance(parsed_ac, list):
                    missing_forex = [t for t in ("xyz:EUR", "xyz:JPY", "xyz:KRW", "xyz:DXY") if t not in parsed_ac]
                    if missing_forex:
                        parsed_ac.extend(missing_forex)
                        conn.execute("UPDATE config_overrides SET value=? WHERE key='ACTIVE_COINS'", (json.dumps(parsed_ac),))
            except (json.JSONDecodeError, TypeError):
                pass
        # v4.6 — Journal persistant des evenements WebSocket (connexion,
        # deconnexion, echec de reconnexion, retablissement). Independant du
        # log en memoire (limite a 3000 lignes, perdu au redemarrage) : ici
        # on garde specifiquement l historique de la connexion temps reel,
        # conserve 7 jours, pour pouvoir diagnostiquer des coupures passees
        # meme apres un redemarrage du serveur.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ws_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        conn.commit()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


# ── Journal persistant ───────────────────────────────────────────────────
def append_log_history(ts, level, message):
    """v4.244 — SUR DEMANDE EXPLICITE : enregistre une ligne dans le
    journal PERSISTANT (survit aux redemarrages)."""
    with _lock, _connect() as conn:
        conn.execute(
            "INSERT INTO log_history (ts, level, message) VALUES (?, ?, ?)",
            (ts, level, message),
        )
        conn.commit()


def query_log_history(limit=200, before_id=None, level=None):
    """v4.244 — SUR DEMANDE EXPLICITE : lit le journal persistant, du plus
    RECENT au plus ANCIEN, avec pagination (before_id = continuer avant
    cet id, pour "charger plus ancien") et filtre optionnel par niveau.
    Retourne une liste de dicts {id, ts, level, message}."""
    with _lock, _connect() as conn:
        query = "SELECT id, ts, level, message FROM log_history WHERE 1=1"
        params = []
        if before_id is not None:
            query += " AND id < ?"
            params.append(before_id)
        if level:
            query += " AND level = ?"
            params.append(level)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def purge_log_history(max_age_days=30, max_rows=500000):
    """v4.244 — SUR DEMANDE EXPLICITE : purge automatique — evite une
    croissance illimitee de la base. Supprime les lignes plus vieilles que
    max_age_days, ET plafonne le nombre total de lignes (garde les plus
    recentes) si max_rows est depasse malgre tout (rythme de log
    inhabituellement eleve)."""
    with _lock, _connect() as conn:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).isoformat()
        conn.execute("DELETE FROM log_history WHERE ts < ?", (cutoff,))
        count = conn.execute("SELECT COUNT(*) as c FROM log_history").fetchone()["c"]
        if count > max_rows:
            excess = count - max_rows
            conn.execute(
                "DELETE FROM log_history WHERE id IN (SELECT id FROM log_history ORDER BY id ASC LIMIT ?)",
                (excess,),
            )
        conn.commit()


# ── Utilisateurs ─────────────────────────────────────────────────────────
def create_user(email, password_hash):
    with _lock, _connect() as conn:
        conn.execute(
            "INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)",
            (email, password_hash, now_iso())
        )
        conn.commit()


def get_user_by_email(email):
    with _lock, _connect() as conn:
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        return dict(row) if row else None


def user_count():
    with _lock, _connect() as conn:
        return conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]


# ── Trades ───────────────────────────────────────────────────────────────
def insert_open_trade(coin, action, confidence, leverage, position_size_pct,
                       risk_reward, timeframe, entry_price, stop_loss,
                       take_profit1, take_profit2, rsi=None, entry_reasons=None,
                       confidence_breakdown=None, strategy=None, size_usd=None,
                       sl_pct_used=None, ttp_arm1_pct_used=None, adaptive_sl_ttp=None,
                       trade_mode=None):
    with _lock, _connect() as conn:
        cur = conn.execute("""
            INSERT INTO trades (coin, action, confidence, leverage, position_size_pct,
                                 risk_reward, timeframe, entry_price, stop_loss,
                                 take_profit1, take_profit2, rsi, entry_reasons,
                                 confidence_breakdown, strategy, size_usd,
                                 sl_pct_used, ttp_arm1_pct_used, adaptive_sl_ttp, trade_mode, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (coin, action, confidence, leverage, position_size_pct, risk_reward,
              timeframe, entry_price, stop_loss, take_profit1, take_profit2, rsi,
              entry_reasons, confidence_breakdown, strategy, size_usd,
              sl_pct_used, ttp_arm1_pct_used, int(bool(adaptive_sl_ttp)) if adaptive_sl_ttp is not None else None,
              trade_mode, now_iso()))
        conn.commit()
        return cur.lastrowid


# ── v4.264 — Indexation des trades par identifiant unique ───────────────
_OPEN_TRADE_FIELDS = (
    "confidence", "leverage", "position_size_pct", "risk_reward", "timeframe",
    "stop_loss", "take_profit1", "take_profit2", "rsi", "entry_reasons",
    "confidence_breakdown", "size_usd", "sl_pct_used", "ttp_arm1_pct_used",
    "vol_ratio", "activity_ratio", "flow_at_entry", "spread_at_entry",  # v4.281 / v4.282
)


def register_pending_trade(trade_uid, coin, action, strategy, slot_key, trade_mode,
                           entry_price, is_accum_slot=False):
    """Trace ecrite AVANT l envoi d un ordre reel : le trade est identifiable
    (mode source, heure) meme si le process est coupe juste apres."""
    with _lock, _connect() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO trades (trade_uid, coin, action, strategy, slot_key,
                                          is_accum_slot, trade_mode, entry_price, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        """, (trade_uid, coin, action, strategy, slot_key, int(bool(is_accum_slot)),
              trade_mode, entry_price, now_iso()))
        conn.commit()


def discard_pending_trade(trade_uid):
    """Supprime la trace d un ordre qui n a finalement pas ouvert de position."""
    with _lock, _connect() as conn:
        conn.execute("DELETE FROM trades WHERE trade_uid=? AND status='pending' AND closed_at IS NULL", (trade_uid,))
        conn.commit()


def upsert_open_trade(ev):
    """Complete la trace prealable (meme trade_uid) avec toutes les donnees d
    ouverture, ou cree la ligne si elle n existe pas (paper, anciens appels).
    Conserve l heure d ouverture d origine de la trace prealable."""
    uid = ev.get("trade_uid")
    abs_ = ev.get("adaptive_sl_ttp")
    adaptive = int(bool(abs_)) if abs_ is not None else None
    values = {k: ev.get(k) for k in _OPEN_TRADE_FIELDS}
    with _lock, _connect() as conn:
        row = conn.execute("SELECT id FROM trades WHERE trade_uid=?", (uid,)).fetchone() if uid else None
        if row:
            sets = ", ".join(f"{k}=?" for k in values) + ", entry_price=?, strategy=?, trade_mode=?, slot_key=?, is_accum_slot=?, adaptive_sl_ttp=?, status='open'"
            conn.execute(f"UPDATE trades SET {sets} WHERE id=?",
                         (*values.values(), ev.get("entry"), ev.get("strategy", "forex"), ev.get("trade_mode", "paper"),
                          ev.get("slot_key"), int(bool(ev.get("is_accum_slot"))), adaptive, row["id"]))
            conn.commit()
            return row["id"]
        cols = list(values) + ["coin", "action", "entry_price", "strategy", "trade_mode", "trade_uid",
                               "slot_key", "is_accum_slot", "adaptive_sl_ttp", "status", "created_at"]
        vals = list(values.values()) + [ev["coin"], ev["action"], ev.get("entry"), ev.get("strategy", "forex"),
                                         ev.get("trade_mode", "paper"), uid, ev.get("slot_key"),
                                         int(bool(ev.get("is_accum_slot"))), adaptive, "open", now_iso()]
        cur = conn.execute(f"INSERT INTO trades ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})", vals)
        conn.commit()
        return cur.lastrowid


def get_open_trade_id_by_uid(trade_uid):
    if not trade_uid:
        return None
    with _lock, _connect() as conn:
        row = conn.execute("SELECT id FROM trades WHERE trade_uid=? AND closed_at IS NULL", (trade_uid,)).fetchone()
        return row["id"] if row else None


def get_trade_by_uid(trade_uid):
    if not trade_uid:
        return None
    with _lock, _connect() as conn:
        row = conn.execute("SELECT * FROM trades WHERE trade_uid=?", (trade_uid,)).fetchone()
        return dict(row) if row else None


def list_pending_trades():
    """Traces prealables jamais confirmees (ordre envoye, process coupe ?)."""
    with _lock, _connect() as conn:
        rows = conn.execute(
            "SELECT trade_uid, coin, action FROM trades WHERE status='pending' AND closed_at IS NULL"
        ).fetchall()
        return [dict(r) for r in rows]


def find_open_trades_for_recovery(coin, action):
    """Lignes encore ouvertes (plus recentes d abord) pour une position
    reelle retrouvee sur Hyperliquid — sert a lui rendre son mode source,
    son heure d ouverture et son emplacement au redemarrage."""
    with _lock, _connect() as conn:
        rows = conn.execute("""
            SELECT id, trade_uid, strategy, created_at, slot_key, is_accum_slot, trade_mode,
                   confidence, leverage, size_usd, sl_pct_used, ttp_arm1_pct_used
            FROM trades WHERE coin=? AND action=? AND closed_at IS NULL
            ORDER BY (trade_uid IS NULL), id DESC
        """, (coin, action)).fetchall()
        return [dict(r) for r in rows]


def recent_open_paper_trades(since_iso, strategies):
    """v4.373 — trades PAPER encore ouverts en base (closed_at NULL) crees depuis since_iso, pour reconstruire des positions
    perdues au redemarrage (fichier de positions ecrase)."""
    if not strategies:
        return []
    marks = ", ".join("?" * len(strategies))
    with _lock, _connect() as conn:
        rows = conn.execute(f"""
            SELECT trade_uid, coin, action, strategy, created_at, slot_key, is_accum_slot, leverage, size_usd, entry_price,
                   stop_loss, take_profit1, confidence, sl_pct_used, ttp_arm1_pct_used
            FROM trades WHERE closed_at IS NULL AND status='open' AND COALESCE(trade_mode,'paper')='paper'
              AND trade_uid IS NOT NULL AND strategy IN ({marks}) AND created_at >= ? ORDER BY id
        """, (*strategies, since_iso)).fetchall()
        return [dict(r) for r in rows]


def supsw_event_by_uid(trade_uid):
    with _lock, _connect() as conn:
        row = conn.execute("SELECT * FROM supsw_events WHERE trade_uid=? ORDER BY id DESC LIMIT 1", (trade_uid,)).fetchone()
        return dict(row) if row else None


def insert_orphaned_closed_trade(coin, action, entry_price, exit_price, pnl, reason,
                                  strategy=None, trade_mode=None, peak_pnl=None,
                                  peak_pnl_pct=None, fees_paid=None):
    """v4.220 — SUR DEMANDE EXPLICITE, FIX BUG CRITIQUE : filet de recuperation
    quand get_open_trade_id_by_coin_action ne trouve AUCUNE ligne ouverte
    correspondante (confirme par un cas reel : fermeture SEI/LONG perdue en
    silence, la position etait bien fermee en memoire mais son historique
    a completement disparu). Cree directement un enregistrement DEJA FERME,
    avec les seules donnees disponibles a la fermeture (pas de confidence/
    levier/SL-TP d origine, inconnus a ce stade) — prefere un enregistrement
    incomplet a une perte totale de la donnee (PnL, statistiques)."""
    with _lock, _connect() as conn:
        now = now_iso()
        cur = conn.execute("""
            INSERT INTO trades (coin, action, entry_price, exit_price, pnl, reason,
                                 strategy, trade_mode, peak_pnl, peak_pnl_pct, fees_paid,
                                 created_at, closed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (coin, action, entry_price, exit_price, pnl, reason, strategy, trade_mode,
              peak_pnl, peak_pnl_pct, fees_paid, now, now))
        conn.commit()
        return cur.lastrowid


def fix_trade_exit_by_uid(trade_uid, exit_price, pnl, exit_price_source, fees_paid=None):
    """v4.411 — corrige le prix de sortie / PnL d un trade deja ferme quand le vrai remplissage Hyperliquid est retrouve."""
    with _lock, _connect() as conn:
        conn.execute("UPDATE trades SET exit_price=?, pnl=?, exit_price_source=?, fees_paid=COALESCE(?, fees_paid) WHERE trade_uid=?",
                     (exit_price, pnl, exit_price_source, fees_paid, trade_uid))
        conn.commit()


def close_trade(trade_id, exit_price, pnl, reason, peak_pnl=None, peak_pnl_pct=None, fees_paid=None,
                exit_price_source=None, mtf_trend_intact_at_close=None,
                pnl_min_pct=None, pnl_min_after_touch_pct=None, peak_at_min_after_pct=None,
                touch_delay_sec=None, path_complete=None):
    with _lock, _connect() as conn:
        conn.execute(
            "UPDATE trades SET exit_price=?, pnl=?, reason=?, closed_at=?, peak_pnl=?, peak_pnl_pct=?, fees_paid=?, "
            "exit_price_source=?, mtf_trend_intact_at_close=?, pnl_min_pct=?, pnl_min_after_touch_pct=?, "
            "peak_at_min_after_pct=?, touch_delay_sec=?, path_complete=?, status='closed' WHERE id=?",
            (exit_price, pnl, reason, now_iso(), peak_pnl, peak_pnl_pct, fees_paid, exit_price_source,
             mtf_trend_intact_at_close, pnl_min_pct, pnl_min_after_touch_pct, peak_at_min_after_pct,
             touch_delay_sec, path_complete, trade_id)
        )
        conn.commit()


# ── v4.265 — Suivi apres sortie + export ─────────────────────────────────
FOLLOWUP_STRATEGIES = ("spot_accumulation", "accumulation", "funding_contrarian", "forex", "manual", "forex_scalp", "forex_swing", "swing_support", "trend_follow")  # v4.269 : + Funding ; v4.273 : + Manuel ; v4.323 : + Forex (export CSV et suivi +30/+60 min)


def list_trades_needing_followup(min_age_minutes=62, max_age_days=16, limit=5):
    """Trades Spot-Accum / Accumulation fermes depuis plus d une heure dont le
    suivi (prix +30/+60 min, frais reels) n a pas encore ete fait."""
    now = datetime.now(timezone.utc)
    newest = (now - timedelta(minutes=min_age_minutes)).isoformat()
    oldest = (now - timedelta(days=max_age_days)).isoformat()
    with _lock, _connect() as conn:
        rows = conn.execute(f"""
            SELECT * FROM trades
            WHERE closed_at IS NOT NULL AND followup_status IS NULL
              AND strategy IN ({",".join("?" * len(FOLLOWUP_STRATEGIES))})
              AND closed_at <= ? AND closed_at >= ?
            ORDER BY closed_at DESC LIMIT ?
        """, (*FOLLOWUP_STRATEGIES, newest, oldest, limit)).fetchall()
        return [dict(r) for r in rows]


def list_trades_needing_sim(min_age_minutes=122, max_age_days=16, limit=5):
    """v4.269 — trades sortis par STOP LOSS dont la simulation "SL plus
    large" n a pas encore ete faite (y compris les trades deja suivis)."""
    now = datetime.now(timezone.utc)
    newest = (now - timedelta(minutes=min_age_minutes)).isoformat()
    oldest = (now - timedelta(days=max_age_days)).isoformat()
    with _lock, _connect() as conn:
        rows = conn.execute(f"""
            SELECT * FROM trades
            WHERE closed_at IS NOT NULL AND sim2_status IS NULL
              AND (reason LIKE 'STOP LOSS%' OR reason LIKE 'SL %')
              AND strategy IN ({",".join("?" * len(FOLLOWUP_STRATEGIES))})
              AND closed_at <= ? AND closed_at >= ?
            ORDER BY closed_at DESC LIMIT ?
        """, (*FOLLOWUP_STRATEGIES, newest, oldest, limit)).fetchall()
        return [dict(r) for r in rows]


def save_trade_followup(trade_id, fields):
    allowed = ("price_after_30m", "price_after_60m", "high_60m", "low_60m", "fees_real", "pnl_real_hl", "followup_status",
               "sim_sl_075", "sim_sl_100", "sim_sl_150", "sim_status",
               "sim_sl_200", "sl_back_min", "sl_mae_pct", "sl_mark_120_pct", "sim2_status")
    data = {k: v for k, v in fields.items() if k in allowed}
    data["followup_at"] = now_iso()
    with _lock, _connect() as conn:
        conn.execute(f"UPDATE trades SET {', '.join(f'{k}=?' for k in data)} WHERE id=?", (*data.values(), trade_id))
        conn.commit()


def get_trades_for_export(strategies=FOLLOWUP_STRATEGIES, days=None):
    params = list(strategies)
    where = f"strategy IN ({','.join('?' * len(strategies))})"
    if days:
        where += " AND created_at >= ?"
        params.append((datetime.now(timezone.utc) - timedelta(days=days)).isoformat())
    with _lock, _connect() as conn:
        rows = conn.execute(f"SELECT * FROM trades WHERE {where} AND status IS NOT 'pending' ORDER BY created_at DESC", params).fetchall()
        return [dict(r) for r in rows]


def get_open_trade_info(coin, action):
    """v4.233/257 — SUR DEMANDE EXPLICITE, FIX BUG CRITIQUE : retrouve la
    VRAIE strategie ET la VRAIE heure d ouverture d origine d une position
    via la base de donnees DURABLE (table trades, ligne encore ouverte :
    closed_at IS NULL), au lieu du fichier positions.json EPHEMERE —
    celui-ci est vide des qu une position a deja ete (a tort) fermee cote
    bot, rendant toute reconciliation basee dessus impossible. Confirme
    par 2 cas reels : (1) des positions Spot-Accum orphelines, reclassees
    "forex" par defaut faute de reference locale, gerees ensuite avec la
    MAUVAISE logique de sortie ; (2) l heure d ouverture reinitialisee a
    l heure de RECUPERATION (pas la vraie heure d origine) a chaque
    redeploiement necessitant une reconciliation, faussant la duree
    affichee — un vrai handicap pour le suivi. La base SQLite garde la
    trace du trade ORIGINAL (strategie ET horodatage) tant qu il n a
    jamais ete marque ferme — une source bien plus fiable pour ce cas
    precis. Retourne un dict {strategy, created_at} ou None si aucune
    correspondance."""
    with _lock, _connect() as conn:
        row = conn.execute("""
            SELECT strategy, created_at, trade_uid FROM trades
            WHERE coin=? AND action=? AND closed_at IS NULL
            ORDER BY (trade_uid IS NULL), created_at DESC LIMIT 1
        """, (coin, action)).fetchone()
        return dict(row) if row else None


def get_open_trade_strategy(coin, action):
    """Repli de compatibilite — voir get_open_trade_info (etendu avec
    la vraie heure d ouverture)."""
    info = get_open_trade_info(coin, action)
    return info["strategy"] if info else None


def get_open_trade_id_by_coin_action(coin, action, strategy=None):
    """Retrouve le dernier trade ouvert (non ferme) pour ce coin/action —
    utilise quand on ne connait pas l id (ouverture geree par bot_engine,
    pas par l API).
    v4.159 — FIX BUG CRITIQUE : sans le parametre strategy, cette
    recherche etait AMBIGUE des que 2 modes (ex: Normal ET Accumulation)
    avaient chacun une position ouverte sur le MEME coin+action —
    retournait potentiellement la MAUVAISE ligne (levier, TP d un AUTRE
    mode affiches a tort). Filtre desormais aussi par strategy quand
    fourni, pour cibler exactement la bonne ligne."""
    with _lock, _connect() as conn:
        if strategy is not None:
            row = conn.execute(
                "SELECT id FROM trades WHERE coin=? AND action=? AND strategy=? AND closed_at IS NULL ORDER BY id DESC LIMIT 1",
                (coin, action, strategy)
            ).fetchone()
            if row:
                return row["id"]
        # repli (strategy non fournie, ou aucune ligne avec cette strategy
        # exacte — trades anciens sans champ strategy renseigne) :
        # comportement d origine.
        row = conn.execute(
            "SELECT id FROM trades WHERE coin=? AND action=? AND closed_at IS NULL ORDER BY id DESC LIMIT 1",
            (coin, action)
        ).fetchone()
        return row["id"] if row else None


def debug_open_rows_for_coin_action(coin, action):
    """v4.313 — SUR DEMANDE EXPLICITE : diagnostic pour comprendre POURQUOI
    une fermeture ne retrouve aucune ligne ouverte correspondante (ni par
    trade_uid, ni par coin/action/strategy) — retourne TOUTES les lignes
    encore marquees ouvertes pour ce coin+action, toutes strategies
    confondues, avec leur trade_uid/strategy/created_at exacts. Permet de
    voir d un coup d oeil une desynchronisation (ex : strategy differente
    de celle attendue, trade_uid absent ou different) plutot que de deviner."""
    with _lock, _connect() as conn:
        rows = conn.execute(
            "SELECT id, trade_uid, strategy, created_at, entry_price FROM trades "
            "WHERE coin=? AND action=? AND closed_at IS NULL ORDER BY id DESC",
            (coin, action)
        ).fetchall()
        return [dict(r) for r in rows]


def get_trades(limit=50, only_closed=False, order_by_close=False, since=None):
    """v4.114 — SUR DEMANDE EXPLICITE : order_by_close=True trie par date de
    FERMETURE (closed_at DESC, plus recent en premier) au lieu de l ordre
    d OUVERTURE (id DESC, comportement d origine) — pour un historique de
    trades FERMES, trier par ouverture est incorrect : un trade ouvert tot
    mais ferme tard apparaissait avant un trade ouvert tard mais ferme vite,
    inversant l ordre reel de l historique. Comportement par defaut
    INCHANGE (order_by_close=False) pour ne rien casser des autres usages.
    v4.306 — SUR DEMANDE EXPLICITE : since (ISO UTC) ne renvoie que les
    trades FERMES a partir de cette date/heure — utilise par le bouton
    "reinitialiser les statistiques" de l onglet Historique (voir
    meta['stats_reset_at']). N affecte QUE les requetes qui le passent
    explicitement ; sans since, comportement inchange."""
    with _lock, _connect() as conn:
        q = "SELECT * FROM trades"
        conds = []
        if only_closed:
            conds.append("closed_at IS NOT NULL")
        if since:
            conds.append("closed_at IS NOT NULL AND closed_at >= ?")
        if conds:
            q += " WHERE " + " AND ".join(conds)
        if order_by_close:
            q += " ORDER BY closed_at DESC LIMIT ?"
        else:
            q += " ORDER BY id DESC LIMIT ?"
        params = ((since,) if since else ()) + (limit,)
        rows = conn.execute(q, params).fetchall()
        return [dict(r) for r in rows]


def get_all_closed_trades(since=None):
    """v4.306 — SUR DEMANDE EXPLICITE : voir get_trades ci-dessus, meme
    parametre since pour le point de reinitialisation des statistiques."""
    with _lock, _connect() as conn:
        q = "SELECT * FROM trades WHERE closed_at IS NOT NULL"
        params = ()
        if since:
            q += " AND closed_at >= ?"
            params = (since,)
        q += " ORDER BY closed_at ASC"
        rows = conn.execute(q, params).fetchall()
        return [dict(r) for r in rows]


def get_stale_open_trades(older_than_minutes=20):
    """v4.312 — SUR DEMANDE EXPLICITE : diagnostic pour une position disparue
    du bot SANS AUCUNE trace (ni ouverte, ni dans l historique) — hypothese :
    la fermeture s est bien produite EN MEMOIRE (le bot l a retiree), mais l
    ecriture en base de la clOture a echoue (aucune ligne 'open' en base a
    faire correspondre, ex : upsert_open_trade jamais appele avec succes a l
    ouverture) — laissant une ligne orpheline, TOUJOURS marquee ouverte en
    base indefiniment, invisible cote dashboard (qui lit l etat du bot, pas
    la base, pour les positions ouvertes) et invisible cote historique
    (jamais de closed_at). Retourne les lignes 'open' en base depuis plus de
    older_than_minutes — l appelant les croise avec les positions REELLEMENT
    ouvertes cote bot pour ne garder que les vraies orphelines."""
    with _lock, _connect() as conn:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=older_than_minutes)).isoformat()
        rows = conn.execute(
            "SELECT * FROM trades WHERE closed_at IS NULL AND created_at <= ? ORDER BY created_at DESC",
            (cutoff,)
        ).fetchall()
        return [dict(r) for r in rows]


def delete_trades_older_than(days):
    with _lock, _connect() as conn:
        cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
        rows = conn.execute("SELECT id, closed_at FROM trades WHERE closed_at IS NOT NULL").fetchall()
        to_delete = []
        for r in rows:
            try:
                ts = datetime.fromisoformat(r["closed_at"]).timestamp()
                if ts < cutoff:
                    to_delete.append(r["id"])
            except Exception:
                continue
        if to_delete:
            conn.executemany("DELETE FROM trades WHERE id=?", [(i,) for i in to_delete])
            conn.commit()
        return len(to_delete)


def cleanup_signals(stale_hours=24, protected_ids=None):
    """Nettoie la table trades en deux temps :
    1. DOUBLONS "ouverts" : un seul trade peut reellement etre ouvert a la
       fois par actif (voir architecture du bot) — si plusieurs lignes du
       meme coin ont closed_at IS NULL, ce sont forcement des orphelins
       (ex: positions paper perdues lors d un redeploiement avant le fix de
       persistance) ou des doublons. On ne garde que le plus recent par coin
       (sauf la ligne protegee, si elle existe pour ce coin — voir ci-dessous).
    2. ANCIENS : supprime tout trade (ouvert ou ferme) cree il y a plus de
       stale_hours heures et qui n a jamais ete cloture proprement (evite
       de perdre les vrais trades fermes recemment, utiles au Bilan) — seuls
       les trades RESTES OUVERTS trop longtemps sont vises ici, pas
       l historique des trades fermes normalement.

    protected_ids : iterable d IDs de lignes (pas de coins entiers !)
    correspondant EXACTEMENT aux positions reellement ouvertes en memoire du
    bot en ce moment (voir api.py, qui retrouve l id exact via
    get_open_trade_id_by_coin_action). Seules CES lignes precises sont
    exclues de toute suppression — les AUTRES doublons du meme coin restent
    nettoyables normalement (contrairement a une version precedente qui
    protegeait tout le coin, empechant par erreur le nettoyage des vrais
    orphelins a cote d une position legitime).
    Retourne (doublons_supprimes, anciens_supprimes).
    """
    protected = set(protected_ids or [])
    with _lock, _connect() as conn:
        # 1. Doublons "ouverts" par coin (la ligne protegee, si presente,
        #    est toujours gardee ; sinon on garde le plus recent)
        # v4.264 — FIX BUG CRITIQUE : regroupait par COIN seul, alors que
        # deux modes (ex: Accumulation + Spot-Accum/Funding) peuvent tenir le
        # meme coin en meme temps — la ligne du second etait supprimee, puis
        # au redeploiement suivant sa position etait reclassee "forex" faute
        # de trace. Ne considere desormais comme doublons que les lignes
        # SANS identifiant (anciennes) du meme coin + sens + strategie ; une
        # ligne indexee (trade_uid) n est jamais un doublon.
        open_rows = conn.execute(
            "SELECT id, coin, action, strategy, created_at FROM trades "
            "WHERE closed_at IS NULL AND trade_uid IS NULL ORDER BY id DESC"
        ).fetchall()
        by_coin = {}
        for r in open_rows:
            by_coin.setdefault((r["coin"], r["action"], r["strategy"] or "forex"), []).append(r["id"])
        dup_ids = []
        for coin, ids in by_coin.items():
            keep = next((i for i in ids if i in protected), ids[0])
            dup_ids.extend(i for i in ids if i != keep)
        if dup_ids:
            conn.executemany("DELETE FROM trades WHERE id=?", [(i,) for i in dup_ids])

        # 2. Trades restes "ouverts" trop longtemps (orphelins probables),
        #    hors ligne protegee
        cutoff = datetime.now(timezone.utc).timestamp() - stale_hours * 3600
        # v4.264 — au demarrage (stale_hours=0), une ligne INDEXEE non
        # protegee n est jamais supprimee : son absence en memoire peut
        # venir d une lecture Hyperliquid impossible a cet instant, pas
        # forcement d un orphelin. Elle reste visible pour verification.
        uid_filter = " AND trade_uid IS NULL" if stale_hours <= 0 else ""
        still_open = conn.execute(
            "SELECT id, coin, created_at FROM trades WHERE closed_at IS NULL" + uid_filter
        ).fetchall()
        stale_ids = []
        for r in still_open:
            if r["id"] in protected:
                continue
            try:
                ts = datetime.fromisoformat(r["created_at"]).timestamp()
                if ts < cutoff:
                    stale_ids.append(r["id"])
            except Exception:
                continue
        if stale_ids:
            conn.executemany("DELETE FROM trades WHERE id=?", [(i,) for i in stale_ids])

        conn.commit()
        print(f"[AUDIT] cleanup_signals() a {now_iso()} — doublons supprimes: {dup_ids} | orphelins supprimes: {stale_ids} | ids proteges: {sorted(protected)}")
        return len(dup_ids), len(stale_ids)


# ── Journal WebSocket (v4.6) ─────────────────────────────────────────────
def insert_ws_event(kind, message):
    """Enregistre un evenement WebSocket (deconnexion, reconnexion...) et
    purge au passage tout ce qui date de plus de 7 jours — pas besoin d une
    tache de nettoyage separee, la purge se fait naturellement a chaque
    nouvel evenement (les coupures WS sont rares, l overhead est nul)."""
    with _lock, _connect() as conn:
        conn.execute(
            "INSERT INTO ws_events (kind, message, created_at) VALUES (?, ?, ?)",
            (kind, message, now_iso())
        )
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        conn.execute("DELETE FROM ws_events WHERE created_at < ?", (cutoff,))
        conn.commit()


def get_ws_events(days=7):
    """Retourne les evenements WebSocket des N derniers jours, du plus
    recent au plus ancien."""
    with _lock, _connect() as conn:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = conn.execute(
            "SELECT * FROM ws_events WHERE created_at >= ? ORDER BY created_at DESC",
            (cutoff,)
        ).fetchall()
        return [dict(r) for r in rows]


def clear_all_trades():
    import traceback
    with _lock, _connect() as conn:
        count_before = conn.execute("SELECT COUNT(*) AS c FROM trades").fetchone()["c"]
        conn.execute("DELETE FROM trades")
        conn.commit()
    print(f"[AUDIT] clear_all_trades() appelee a {now_iso()} — {count_before} trade(s) supprime(s). Pile d appel :")
    print("".join(traceback.format_stack()[:-1]))


def clear_trades_by_strategy(strategy):
    """v4.88 — SUR DEMANDE EXPLICITE : efface UNIQUEMENT l historique d une
    strategie precise (utilise au moment de basculer un mode en live, pour
    repartir sur un historique propre pour ce mode-la sans toucher aux
    autres). Les trades sans champ strategy (anciens trades, avant son
    introduction, ou avant le renommage "normal"->"forex") sont consideres
    "forex"."""
    import traceback
    with _lock, _connect() as conn:
        if strategy == "forex":
            rows = conn.execute("SELECT COUNT(*) AS c FROM trades WHERE strategy = ? OR strategy IS NULL OR strategy = 'normal'", (strategy,)).fetchone()
            count_before = rows["c"]
            conn.execute("DELETE FROM trades WHERE strategy = ? OR strategy IS NULL OR strategy = 'normal'", (strategy,))
        else:
            rows = conn.execute("SELECT COUNT(*) AS c FROM trades WHERE strategy = ?", (strategy,)).fetchone()
            count_before = rows["c"]
            conn.execute("DELETE FROM trades WHERE strategy = ?", (strategy,))
        conn.commit()
    print(f"[AUDIT] clear_trades_by_strategy('{strategy}') appelee a {now_iso()} — {count_before} trade(s) supprime(s). Pile d appel :")
    print("".join(traceback.format_stack()[:-1]))
    return count_before


# ── Config persistante (survit aux redemarrages) ────────────────────────
def get_config_override(key, default=None):
    with _lock, _connect() as conn:
        row = conn.execute("SELECT value FROM config_overrides WHERE key=?", (key,)).fetchone()
        if row is None:
            return default
        try:
            return json.loads(row["value"])
        except Exception:
            return default


def set_config_override(key, value):
    with _lock, _connect() as conn:
        conn.execute(
            "INSERT INTO config_overrides (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value))
        )
        conn.commit()


def get_all_config_overrides():
    with _lock, _connect() as conn:
        rows = conn.execute("SELECT key, value FROM config_overrides").fetchall()
        out = {}
        for r in rows:
            try:
                out[r["key"]] = json.loads(r["value"])
            except Exception:
                pass
        return out


def clear_config_overrides():
    with _lock, _connect() as conn:
        conn.execute("DELETE FROM config_overrides")
        conn.commit()


# ── Meta (ex: date de premier demarrage, pour "reset_at") ───────────────
def get_meta(key, default=None):
    with _lock, _connect() as conn:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def set_meta(key, value):
    with _lock, _connect() as conn:
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value)
        )
        conn.commit()


# ── v4.273 — Trading manuel ──────────────────────────────────────────────
def manual_insert(status, data):
    with _lock, _connect() as conn:
        cur = conn.execute("INSERT INTO manual_trades (status, data, created_at, updated_at) VALUES (?, ?, ?, ?)",
                           (status, json.dumps(data), now_iso(), now_iso()))
        conn.commit()
        return cur.lastrowid


def manual_update(item_id, status, data):
    with _lock, _connect() as conn:
        conn.execute("UPDATE manual_trades SET status=?, data=?, updated_at=? WHERE id=?",
                     (status, json.dumps(data), now_iso(), item_id))
        conn.commit()


def manual_list(statuses):
    with _lock, _connect() as conn:
        rows = conn.execute(f"SELECT id, status, data FROM manual_trades WHERE status IN ({','.join('?' * len(statuses))}) ORDER BY id",
                            tuple(statuses)).fetchall()
        return [{"id": r["id"], "status": r["status"], "data": json.loads(r["data"])} for r in rows]


def manual_history(limit=50):
    with _lock, _connect() as conn:
        rows = conn.execute("SELECT id, status, data FROM manual_trades WHERE status IN ('closed','cancelled','expired','failed') "
                            "ORDER BY updated_at DESC LIMIT ?", (limit,)).fetchall()
        return [{"id": r["id"], "status": r["status"], "data": json.loads(r["data"])} for r in rows]


# ── v4.349 — Enregistreur de flux (lecture seule, aucun effet sur le trading) ──────────
def flow_insert_many(rows):
    """rows : tuples (ticker, t0, n_buy, n_sell, buy_usd, sell_usd, big_buy_usd, big_sell_usd, max_trade_usd,
    px_open, px_high, px_low, px_close). INSERT OR REPLACE : une tranche rejouee ne cree pas de doublon."""
    if not rows:
        return 0
    with _lock, _connect() as conn:
        conn.executemany("INSERT OR REPLACE INTO flow_5m VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        conn.commit()
    return len(rows)


def flow_alive_insert(t0s):
    if not t0s:
        return 0
    with _lock, _connect() as conn:
        conn.executemany("INSERT OR IGNORE INTO flow_5m_alive (t0) VALUES (?)", [(t,) for t in t0s])
        conn.commit()
    return len(t0s)


def flow_prune(older_than_ms):
    with _lock, _connect() as conn:
        a = conn.execute("DELETE FROM flow_5m WHERE t0 < ?", (older_than_ms,)).rowcount
        b = conn.execute("DELETE FROM flow_5m_alive WHERE t0 < ?", (older_than_ms,)).rowcount
        conn.commit()
    return a, b


def flow_status(now_ms):
    """Etat de l enregistrement : volumes, periode couverte, couverture par actif sur 24 h."""
    with _lock, _connect() as conn:
        tot = conn.execute("SELECT COUNT(*) AS n, MIN(t0) AS first, MAX(t0) AS last, COUNT(DISTINCT ticker) AS assets FROM flow_5m").fetchone()
        alive = conn.execute("SELECT COUNT(*) AS n, MIN(t0) AS first, MAX(t0) AS last FROM flow_5m_alive").fetchone()
        since = now_ms - 24 * 3600 * 1000
        per = conn.execute("SELECT ticker, COUNT(*) AS n, SUM(n_buy + n_sell) AS trades, SUM(buy_usd + sell_usd) AS usd "
                           "FROM flow_5m WHERE t0 >= ? GROUP BY ticker ORDER BY ticker", (since,)).fetchall()
        alive24 = conn.execute("SELECT COUNT(*) AS n FROM flow_5m_alive WHERE t0 >= ?", (since,)).fetchone()["n"]
        return {"rows": tot["n"], "first": tot["first"], "last": tot["last"], "assets": tot["assets"],
                "alive_buckets": alive["n"], "alive_first": alive["first"], "alive_last": alive["last"],
                "alive_24h": alive24, "per_asset_24h": [dict(r) for r in per]}


def flow_export_rows(since_ms, until_ms=None):
    q = "SELECT * FROM flow_5m WHERE t0 >= ?"
    params = [since_ms]
    if until_ms:
        q += " AND t0 < ?"
        params.append(until_ms)
    q += " ORDER BY t0, ticker"
    with _lock, _connect() as conn:
        return [tuple(r) for r in conn.execute(q, params).fetchall()]


# ── v4.353 — Journal du scalp Forex (lecture seule pour le trading : sert a AMELIORER le scalp) ──────────
_SCALP_EVENT_FIELDS = ("ts", "asset", "side", "setup", "level_kinds", "level_price", "level_w", "price", "spread_pct", "atr5_pct",
                       "bias", "flow", "hour_utc", "decision", "sl", "tp", "risk_pct", "tp_pct", "rr", "net_rr", "cost_pct",
                       "hypothetical", "trade_uid")


def scalp_event_insert(ev):
    vals = [ev.get(k) for k in _SCALP_EVENT_FIELDS]
    with _lock, _connect() as conn:
        cur = conn.execute(f"INSERT INTO scalp_events ({', '.join(_SCALP_EVENT_FIELDS)}) VALUES ({', '.join('?' * len(_SCALP_EVENT_FIELDS))})", vals)
        conn.commit()
        return cur.lastrowid


def scalp_events_pending(max_ts_ms, limit=60):
    with _lock, _connect() as conn:
        rows = conn.execute("SELECT * FROM scalp_events WHERE evaluated = 0 AND ts <= ? AND sl IS NOT NULL AND tp IS NOT NULL "
                            "ORDER BY ts LIMIT ?", (max_ts_ms, limit)).fetchall()
        return [dict(r) for r in rows]


def scalp_event_update(event_id, fields):
    if not fields:
        return
    keys = list(fields)
    with _lock, _connect() as conn:
        conn.execute(f"UPDATE scalp_events SET {', '.join(k + '=?' for k in keys)} WHERE id=?", [fields[k] for k in keys] + [event_id])
        conn.commit()


def scalp_events_since(since_ms, limit=5000):
    with _lock, _connect() as conn:
        rows = conn.execute("SELECT * FROM scalp_events WHERE ts >= ? ORDER BY ts DESC LIMIT ?", (since_ms, limit)).fetchall()
        return [dict(r) for r in rows]


def scalp_events_prune(older_than_ms):
    with _lock, _connect() as conn:
        n = conn.execute("DELETE FROM scalp_events WHERE ts < ?", (older_than_ms,)).rowcount
        conn.commit()
    return n


# ── v4.359 — Journal du Swing ──────────────────────────────────────────────────────────────
_SWING_FIELDS = ("ts", "signal_t", "asset", "side", "mode", "pattern", "trend", "zone_low", "zone_high", "zone_touches", "price",
                 "entry_ref", "atr_pct", "flow", "hour_utc", "age_min", "decision", "prio", "sl", "tp", "risk_pct", "reward_pct",
                 "rr", "hypothetical", "trade_uid")


def swing_event_upsert(ev):
    """Une ligne par (actif, sens, bougie du signal). Une decision de rang superieur (pris 3 > candidat 2 > filtre 1)
    remplace la precedente ; sinon la PREMIERE raison est conservee. Retourne l id."""
    with _lock, _connect() as conn:
        row = conn.execute("SELECT id, prio, evaluated FROM swing_events WHERE asset=? AND side=? AND signal_t=?",
                           (ev["asset"], ev["side"], ev["signal_t"])).fetchone()
        if row is None:
            cur = conn.execute(f"INSERT INTO swing_events ({', '.join(_SWING_FIELDS)}) VALUES ({', '.join('?' * len(_SWING_FIELDS))})",
                               [ev.get(k) for k in _SWING_FIELDS])
            conn.commit()
            return cur.lastrowid
        if (ev.get("prio") or 1) > (row["prio"] or 1) and not row["evaluated"]:
            keys = [k for k in _SWING_FIELDS if k not in ("ts", "signal_t", "asset", "side")]
            conn.execute(f"UPDATE swing_events SET {', '.join(k + '=?' for k in keys)} WHERE id=?", [ev.get(k) for k in keys] + [row["id"]])
            conn.commit()
        return row["id"]


def swing_event_mark_taken(asset, side, signal_t, trade_uid):
    with _lock, _connect() as conn:
        conn.execute("UPDATE swing_events SET decision='pris', prio=3, trade_uid=? WHERE asset=? AND side=? AND signal_t=?",
                     (trade_uid, asset, side, signal_t))
        conn.commit()


def swing_events_pending(now_ms, limit=40):
    """Evenements a mesurer : 24 h apres le signal (evaluated=0), puis 72 h (evaluated=1)."""
    with _lock, _connect() as conn:
        rows = conn.execute("SELECT * FROM swing_events WHERE sl IS NOT NULL AND tp IS NOT NULL AND ("
                            "(evaluated=0 AND signal_t <= ?) OR (evaluated=1 AND signal_t <= ?)) ORDER BY signal_t LIMIT ?",
                            (now_ms - 25 * 3_600_000, now_ms - 73 * 3_600_000, limit)).fetchall()
        return [dict(r) for r in rows]


def swing_event_update(event_id, fields):
    if not fields:
        return
    keys = list(fields)
    with _lock, _connect() as conn:
        conn.execute(f"UPDATE swing_events SET {', '.join(k + '=?' for k in keys)} WHERE id=?", [fields[k] for k in keys] + [event_id])
        conn.commit()


def swing_events_since(since_ms, limit=5000):
    with _lock, _connect() as conn:
        rows = conn.execute("SELECT * FROM swing_events WHERE ts >= ? ORDER BY ts DESC LIMIT ?", (since_ms, limit)).fetchall()
        return [dict(r) for r in rows]


def swing_events_prune(older_than_ms):
    with _lock, _connect() as conn:
        n = conn.execute("DELETE FROM swing_events WHERE ts < ?", (older_than_ms,)).rowcount
        conn.commit()
    return n


# ── v4.362 — Journal du SWING SUPPORT ───────────────────────────────────────────────────────
_SUPSW_FIELDS = ("ts", "signal_t", "asset", "side", "setup", "setup_kind", "trend", "weekly_trend", "btc_trend", "funding_ann", "flow",
                 "atr_pct", "hour_utc", "zone_low", "zone_high", "zone_touches", "held", "pierce_pct", "price", "entry_ref", "sl", "tp",
                 "risk_pct", "reward_pct", "decision", "prio", "trade_uid", "btc_chg24", "btc_h4", "h4_trend", "res_low", "res_dist_pct", "chg_recent", "btc_chg_recent")


def supsw_upsert(ev):
    """Une ligne par (actif, bougie 1 h du signal). Une decision de rang superieur (pris 3 > candidat 2 > refus 1) remplace
    la precedente tant que rien n est mesure ; sinon la PREMIERE raison est conservee. Retourne l id."""
    with _lock, _connect() as conn:
        row = conn.execute("SELECT id, prio, evaluated FROM supsw_events WHERE asset=? AND signal_t=?",
                           (ev["asset"], ev["signal_t"])).fetchone()
        if row is None:
            cur = conn.execute(f"INSERT INTO supsw_events ({', '.join(_SUPSW_FIELDS)}) VALUES ({', '.join('?' * len(_SUPSW_FIELDS))})",
                               [ev.get(k) for k in _SUPSW_FIELDS])
            conn.commit()
            return cur.lastrowid
        if (ev.get("prio") or 1) > (row["prio"] or 1) and not row["evaluated"]:
            keys = [k for k in _SUPSW_FIELDS if k not in ("ts", "signal_t", "asset")]
            conn.execute(f"UPDATE supsw_events SET {', '.join(k + '=?' for k in keys)} WHERE id=?", [ev.get(k) for k in keys] + [row["id"]])
            conn.commit()
        return row["id"]


def supsw_mark_taken(asset, signal_t, trade_uid):
    with _lock, _connect() as conn:
        conn.execute("UPDATE supsw_events SET decision='pris', prio=3, trade_uid=? WHERE asset=? AND signal_t=?",
                     (trade_uid, asset, signal_t))
        conn.commit()


def supsw_last_exit(asset):
    """v4.389 : derniere sortie journalisee d un actif (exit_ts ms, exit_price, pnl_pct) ou None."""
    with _lock, _connect() as conn:
        row = conn.execute("SELECT exit_ts, exit_price, pnl_pct FROM supsw_events WHERE asset=? AND exit_ts IS NOT NULL "
                           "ORDER BY exit_ts DESC LIMIT 1", (asset,)).fetchone()
    return dict(row) if row else None


def supsw_record_exit(trade_uid, fields):
    """Ecrit la sortie d un trade pris (raison, prix, contexte a la sortie)."""
    if not trade_uid or not fields:
        return
    keys = list(fields)
    with _lock, _connect() as conn:
        conn.execute(f"UPDATE supsw_events SET {', '.join(k + '=?' for k in keys)} WHERE trade_uid=?", [fields[k] for k in keys] + [trade_uid])
        conn.commit()


def supsw_pending_signals(now_ms, limit=40):
    """Signaux a mesurer sur bougies 1 h : 24 h apres (evaluated=0) puis 72 h (evaluated=1)."""
    with _lock, _connect() as conn:
        rows = conn.execute("SELECT * FROM supsw_events WHERE sl IS NOT NULL AND tp IS NOT NULL AND ("
                            "(evaluated=0 AND signal_t <= ?) OR (evaluated=1 AND signal_t <= ?)) ORDER BY signal_t LIMIT ?",
                            (now_ms - 25 * 3_600_000, now_ms - 73 * 3_600_000, limit)).fetchall()
        return [dict(r) for r in rows]


def supsw_pending_exits(now_ms, limit=40):
    """Trades pris et sortis, a mesurer apres la sortie : 25 h plus tard (post_eval=0) puis 73 h (post_eval=1)."""
    with _lock, _connect() as conn:
        rows = conn.execute("SELECT * FROM supsw_events WHERE exit_ts IS NOT NULL AND ("
                            "(post_eval=0 AND exit_ts <= ?) OR (post_eval=1 AND exit_ts <= ?)) ORDER BY exit_ts LIMIT ?",
                            (now_ms - 25 * 3_600_000, now_ms - 73 * 3_600_000, limit)).fetchall()
        return [dict(r) for r in rows]


def supsw_update(event_id, fields):
    if not fields:
        return
    keys = list(fields)
    with _lock, _connect() as conn:
        conn.execute(f"UPDATE supsw_events SET {', '.join(k + '=?' for k in keys)} WHERE id=?", [fields[k] for k in keys] + [event_id])
        conn.commit()


def supsw_events_since(since_ms, limit=5000):
    with _lock, _connect() as conn:
        rows = conn.execute("SELECT * FROM supsw_events WHERE ts >= ? ORDER BY ts DESC LIMIT ?", (since_ms, limit)).fetchall()
        return [dict(r) for r in rows]


def supsw_events_prune(older_than_ms):
    with _lock, _connect() as conn:
        n = conn.execute("DELETE FROM supsw_events WHERE ts < ?", (older_than_ms,)).rowcount
        conn.commit()
    return n


# ── v4.365 — Journal du module TENDANCE CONFIRMEE ───────────────────────────────────────────
_TRENDF_FIELDS = ("ts", "signal_t", "asset", "side", "d1_trend", "h4_trend", "weekly_trend", "btc_trend", "funding_ann", "flow", "atr_pct",
                  "ext_atr", "hour_utc", "price", "entry_ref", "sl", "risk_pct", "entry_mode", "decision", "prio", "trade_uid")


def trendf_upsert(ev):
    """Une ligne par (actif, sens, bougie 1 h du signal). Decision de rang superieur (pris 3 > candidat 2 > refus 1) remplace la precedente
    tant que rien n est mesure. Retourne l id."""
    with _lock, _connect() as conn:
        row = conn.execute("SELECT id, prio, evaluated FROM trendf_events WHERE asset=? AND side=? AND signal_t=?",
                           (ev["asset"], ev["side"], ev["signal_t"])).fetchone()
        if row is None:
            cur = conn.execute(f"INSERT INTO trendf_events ({', '.join(_TRENDF_FIELDS)}) VALUES ({', '.join('?' * len(_TRENDF_FIELDS))})",
                               [ev.get(k) for k in _TRENDF_FIELDS])
            conn.commit()
            return cur.lastrowid
        if (ev.get("prio") or 1) > (row["prio"] or 1) and not row["evaluated"]:
            keys = [k for k in _TRENDF_FIELDS if k not in ("ts", "signal_t", "asset", "side")]
            conn.execute(f"UPDATE trendf_events SET {', '.join(k + '=?' for k in keys)} WHERE id=?", [ev.get(k) for k in keys] + [row["id"]])
            conn.commit()
        return row["id"]


def trendf_mark_taken(asset, side, signal_t, trade_uid):
    with _lock, _connect() as conn:
        conn.execute("UPDATE trendf_events SET decision='pris', prio=3, trade_uid=? WHERE asset=? AND side=? AND signal_t=?",
                     (trade_uid, asset, side, signal_t))
        conn.commit()


def trendf_record_exit(trade_uid, fields):
    if not trade_uid or not fields:
        return
    keys = list(fields)
    with _lock, _connect() as conn:
        conn.execute(f"UPDATE trendf_events SET {', '.join(k + '=?' for k in keys)} WHERE trade_uid=?", [fields[k] for k in keys] + [trade_uid])
        conn.commit()


def trendf_pending_signals(now_ms, limit=40):
    with _lock, _connect() as conn:
        rows = conn.execute("SELECT * FROM trendf_events WHERE sl IS NOT NULL AND entry_ref IS NOT NULL AND ("
                            "(evaluated=0 AND signal_t <= ?) OR (evaluated=1 AND signal_t <= ?)) ORDER BY signal_t LIMIT ?",
                            (now_ms - 25 * 3_600_000, now_ms - 73 * 3_600_000, limit)).fetchall()
        return [dict(r) for r in rows]


def trendf_pending_exits(now_ms, limit=40):
    with _lock, _connect() as conn:
        rows = conn.execute("SELECT * FROM trendf_events WHERE exit_ts IS NOT NULL AND ("
                            "(post_eval=0 AND exit_ts <= ?) OR (post_eval=1 AND exit_ts <= ?)) ORDER BY exit_ts LIMIT ?",
                            (now_ms - 25 * 3_600_000, now_ms - 73 * 3_600_000, limit)).fetchall()
        return [dict(r) for r in rows]


def trendf_update(event_id, fields):
    if not fields:
        return
    keys = list(fields)
    with _lock, _connect() as conn:
        conn.execute(f"UPDATE trendf_events SET {', '.join(k + '=?' for k in keys)} WHERE id=?", [fields[k] for k in keys] + [event_id])
        conn.commit()


def trendf_events_since(since_ms, limit=5000):
    with _lock, _connect() as conn:
        rows = conn.execute("SELECT * FROM trendf_events WHERE ts >= ? ORDER BY ts DESC LIMIT ?", (since_ms, limit)).fetchall()
        return [dict(r) for r in rows]


def trendf_events_prune(older_than_ms):
    with _lock, _connect() as conn:
        n = conn.execute("DELETE FROM trendf_events WHERE ts < ?", (older_than_ms,)).rowcount
        conn.commit()
    return n
