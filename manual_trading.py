"""
manual_trading.py — v4.273 — TRADING MANUEL assiste par le bot.

Principe
--------
Le bot publie ses OPPORTUNITES (les candidats d entree qu il detecte a
chaque cycle, mode par mode). L utilisateur peut en choisir une TANT QU ELLE
EST VALABLE et uniquement DANS LE SENS PREVU PAR LE BOT. Le bot propose tous
les parametres (prix, taille, levier, SL, TP, TTP), tous modifiables.
L ordre peut etre execute tout de suite ou PROGRAMME (declenchement sur prix
ou a une heure donnee, avec expiration), en paper ou en live (confirmation
obligatoire), sur les perps ou en SPOT (achat uniquement, sans levier).

Une position manuelle est ensuite suivie EN TEMPS REEL par le bot selon SES
propres parametres (SL / TP / TTP choisis), independamment de la logique
des modes automatiques. Elle est enregistree dans l historique (strategie
"manual"), l export CSV, et survit aux redemarrages.

Regles de securite (Hyperliquid ne tient qu UNE position perp par actif) :
  - un ordre manuel LIVE perp est refuse si le bot (ou un autre ordre manuel)
    detient deja une position live sur cet actif, ou si une position existe
    deja sur Hyperliquid ;
  - reciproquement, le bot n ouvre pas de position live sur un actif tenu
    manuellement en live (voir BotEngine._finalize_open).
En perp live, le SL choisi est aussi pose en ORDRE NATIF sur Hyperliquid
(protection meme si le bot est arrete). En SPOT, la protection est geree par
le bot uniquement.
"""
import threading
import time
from datetime import datetime

import db
import bot_engine as be
import swing_support as sups     # v4.381 : niveaux Swing Support (ticket prepare depuis une alerte)

STRATEGY_LABELS = {
    "libre": "Ordre libre",
    "swing_support": "Swing Support",
    "forex": "Forex",
    "accumulation": "Accumulation",
    "spot_accumulation": "Spot-Accum",
    "funding_contrarian": "Funding",
}
FREE_LIKE = ("libre", "swing_support")     # tickets sans opportunite du bot a confirmer
OPEN_STATUSES = ("scheduled", "opening", "open", "closing")
TREND_FR = {"haussiere": "haussière", "baissiere": "baissière", "neutre": "neutre"}  # v4.343
FEE_ROUND_TRIP = be.FEE_RATE_TAKER_ESTIMATE * 2


class ManualError(Exception):
    """Erreur de validation ou d execution presentee telle quelle a l utilisateur."""


class ManualTrading:
    def __init__(self, bot):
        self.bot = bot
        self.lock = threading.RLock()
        self.opportunities = {}   # (strategy, ticker, direction) -> dict
        self.items = {}           # id -> dict (ordres programmes + positions ouvertes)
        self._busy = set()        # ids en cours d execution (ouverture/fermeture)
        self._fire_busy = set()   # v4.343 : ordres programmes en cours de declenchement (controle de tendance)
        self._push_busy = set()   # v4.343 : mises a jour du SL natif en cours (verrou progressif)
        self._trend_last = {}     # v4.343 : id -> derniere verification de la regle de sortie sur tendance
        self._last_persist = {}
        self._load()

    # ─────────────────────────────── outils ──────────────────────────────
    @property
    def cfg(self):
        return self.bot.cfg

    def _log(self, msg, level="info"):
        self.bot.emit("log", {"msg": f"✋ {msg}", "level": level})

    def _wallet(self):
        return self.cfg.get("WALLET_ADDRESS")

    def _load(self):
        for row in db.manual_list(OPEN_STATUSES):
            item = row["data"]
            item["id"] = row["id"]
            item["status"] = row["status"]
            if item["status"] in ("opening", "closing"):
                # coupure pendant une operation : on repart d un etat stable,
                # la reconciliation avec Hyperliquid tranchera
                item["status"] = "open" if item.get("entry_price") else "scheduled"
            self.items[item["id"]] = item

    def _save(self, item):
        data = {k: v for k, v in item.items() if k not in ("id", "status")}
        db.manual_update(item["id"], item["status"], data)
        self._last_persist[item["id"]] = time.time()

    # ─────────────────────────── opportunites ────────────────────────────
    def ticker_busy(self, ticker):
        """v4.382 : True si une position est deja ouverte sur cet actif (n importe quel mode du bot, paper ou live,
        ou un trade manuel). Une opportunite sur un actif occupe n est ni proposee ni annoncee."""
        try:
            for pool in (self.bot.states, self.bot.accum_states):
                for sk, st in list(pool.items()):
                    if st.position and be.ticker_from_slot_key(sk) == ticker:
                        return True
            return any(i.get("status") in ("opening", "open", "closing") and i.get("ticker") == ticker for i in list(self.items.values()))
        except Exception:
            return False

    def record_candidates(self, strategy, candidates):
        """Appele par le bot a chaque cycle avec ses candidats d entree
        (avant tout filtrage par plafond de positions)."""
        now = time.time()
        with self.lock:
            for c in candidates:
                if self.ticker_busy(c["ticker"]):
                    continue
                key = (strategy, c["ticker"], c["signal"])
                opp = self.opportunities.get(key)
                is_new = False
                if opp is None or now - opp["last_seen"] > self._ttl():
                    opp = {"strategy": strategy, "ticker": c["ticker"], "direction": c["signal"],
                           "first_seen": now}
                    self.opportunities[key] = opp
                    is_new = True
                opp.update({
                    "last_seen": now,
                    "price": c.get("price"),
                    "confidence": c.get("confidence"),
                    "support": c.get("support"),
                    "resistance": c.get("resistance"),
                    "reasons": [str(r) for r in (c.get("reasons") or [])][:8],
                })
                if is_new:      # v4.376 — alerte telephone (si activee dans Trading Manuel)
                    _push = getattr(self.bot, "push", None)
                    if _push is not None:
                        _push.on_opportunity(dict(opp))

    def _ttl(self):
        return self.cfg.get("MANUAL_OPPORTUNITY_TTL_SEC", 120)

    def get_opportunity(self, strategy, ticker, direction):
        opp = self.opportunities.get((strategy, ticker, direction))
        if opp and time.time() - opp["last_seen"] <= self._ttl():
            return opp
        return None

    def list_opportunities(self):
        now = time.time()
        with self.lock:
            for key in [k for k, o in self.opportunities.items() if now - o["last_seen"] > self._ttl() * 5]:
                del self.opportunities[key]
            out = []
            for o in self.opportunities.values():
                valid = now - o["last_seen"] <= self._ttl()
                if not valid or self.ticker_busy(o["ticker"]):     # v4.382 : plus proposee si une position vient de s ouvrir
                    continue
                out.append({**o, "strategy_label": STRATEGY_LABELS.get(o["strategy"], o["strategy"]),
                            "age_sec": round(now - o["first_seen"]),
                            "current_price": self.price(o["ticker"], "perp"),
                            "spot_available": self.spot_pair(o["ticker"]) is not None and o["direction"] == "long"})
            out.sort(key=lambda o: -(o.get("confidence") or 0))
            return out

    # ─────────────────────────── marches / prix ──────────────────────────
    def spot_pair(self, ticker):
        """Paire spot Hyperliquid correspondant a un ticker perp (ex: BTC ->
        UBTC/USDC, HYPE -> HYPE/USDC). None si aucun marche spot."""
        if not ticker or ":" in ticker:
            return None
        info = self.bot.info
        if info is None:
            return None
        names = getattr(info, "name_to_coin", {}) or {}
        for base in (ticker, "U" + ticker):
            pair = f"{base}/USDC"
            if pair in names:
                coin = names[pair]
                try:
                    szd = info.asset_to_sz_decimals[info.coin_to_asset[coin]]
                except (KeyError, AttributeError):
                    szd = 4
                return {"pair": pair, "coin": coin, "base": base, "sz_decimals": szd}
        return None

    def price(self, ticker, market="perp"):
        mids = getattr(self.bot, "all_mids", {}) or {}
        key = ticker
        if market == "spot":
            sp = self.spot_pair(ticker)
            if not sp:
                return None
            key = sp["coin"]
        raw = mids.get(key)
        try:
            p = float(raw)
            return p if p > 0 else None
        except (TypeError, ValueError):
            return None

    # ───────────────────────────── proposition ───────────────────────────
    # ─────────────────────── ORDRE LIBRE (v4.343) ────────────────────────
    def _propose_free(self, ticker, direction, market):
        """Proposition par defaut d un ORDRE LIBRE : n importe quel actif suivi (crypto ou
        Forex), sans opportunite du bot. Tout reste modifiable sur le ticket."""
        if direction not in ("long", "short"):
            raise ManualError("Sens invalide.")
        if market == "spot" and (direction != "long" or not self.spot_pair(ticker)):
            raise ManualError("Pas de marche spot disponible pour cet actif (le spot n autorise que l achat).")
        price = self.price(ticker, market)
        if not price:
            raise ManualError(f"Prix indisponible pour {ticker} : actif inconnu ou non suivi par le bot.")
        cfg = self.cfg
        lev = 1 if market == "spot" else int(cfg.get("MANUAL_DEFAULT_LEVERAGE", 1))
        sl_pct = float(cfg.get("MANUAL_DEFAULT_SL_PCT", 1.0))
        return {
            "strategy": "libre", "free": True, "ticker": ticker, "direction": direction, "market": market,
            "price": price, "leverage": lev, "notional_usd": round(float(cfg.get("MANUAL_DEFAULT_NOTIONAL_USD", 15.0)), 2),
            "margin_usd": round(float(cfg.get("MANUAL_DEFAULT_NOTIONAL_USD", 15.0)) / max(lev, 1), 2),
            "sl_pct": round(sl_pct, 3), "sl_basis": "SL par defaut (ordre libre)",
            "tp_pct": round(2 * sl_pct, 3), "tp_basis": "2 x le SL (a activer si souhaite)",
            "ttp_arm_pct": 1.0, "ttp_trail_pct": 0.5,
            "lock_arm_pct": round(sl_pct, 3), "lock_fraction": float(cfg.get("MANUAL_DEFAULT_LOCK_FRACTION", 0.5)),
            "trend": self.trend_info(ticker),
            "confidence": None, "reasons": [],
            "spot_available": self.spot_pair(ticker) is not None and direction == "long",
            "live_available": self.bot.exchange is not None and bool(self._wallet()),
            "min_notional_usd": 10.0,
        }

    def trend_info(self, ticker, use_cache=False):
        """Tendance de fond H4 d un actif (meme calcul que le top-down : EMA50/EMA200 sur bougies
        H4 cloturees + prix live), avec l etat de fraicheur des donnees."""
        price = self.price(ticker, "perp")
        if not price:
            return {"ok": False, "why": "prix indisponible"}
        try:
            mv = self.bot.mtf_view(ticker, price, cache_only=use_cache)
        except Exception as e:
            return {"ok": False, "why": f"{type(e).__name__}: {e}"}
        if not mv or not mv.get("ok"):
            return {"ok": False, "why": (mv or {}).get("why") or "historique H4 indisponible pour cet actif"}
        return {"ok": True, "trend": mv["trend"], "data_fresh": bool(mv.get("data_fresh", True)),
                "last_closed_t": mv.get("last_closed_t"), "major_tf": mv.get("major_tf")}

    # ─────────────────── v4.406 — RESUME INSTANTANE D UN ACTIF ───────────────────
    def asset_summary(self, ticker):
        """Lecture seule : etat d un actif en un coup d oeil (prix, variations, tendance / RSI / ATR /
        support / resistance en H1, H4 et D1) + conseil LONG / SHORT / ATTENDRE fonde sur des regles
        simples et explicites (indicatif, aucune garantie). Utilise les bougies Hyperliquid deja mises en
        cache par le moteur (rechargees une fois par bougie)."""
        import mtf_analysis as mtf
        price = self.price(ticker, "perp")
        if not price:
            return {"ok": False, "why": "prix indisponible (actif non suivi en perp ?)"}
        bot = self.bot
        spec = (("h1", "1h", 220), ("h4", "4h", 260), ("d1", "1d", 230))
        tfs, cands, zs = {}, {}, {}
        for key, tf, cnt in spec:
            try:
                c = bot._mtf_candles(ticker, tf, cnt)
            except Exception:
                c = []
            cands[key] = c
            if len(c) < 30:
                tfs[key] = {"ok": False, "why": f"historique {tf} insuffisant ({len(c)} bougies)"}
                continue
            series = c + [{**c[-1], "c": float(price)}]          # prix live = cloture provisoire
            tr, ef, es = mtf.trend(series)
            a = mtf.atr(c) or 0
            closes = [x["c"] for x in series]
            try:
                rsi = be.calc_rsi(closes)
            except Exception:
                rsi = None
            lb = 120 if key != "h1" else 150
            zones = mtf.find_zones(c, lookback=lb, max_width_atr=3)
            sup, res = mtf.nearest_zones(zones, price)
            zs[key] = zones
            tol = a * 0.25
            tfs[key] = {"ok": True, "trend": tr, "rsi": rsi, "atr": a, "atr_pct": (a / price * 100) if price else None,
                        "support": sup, "resistance": res,
                        "sup_dist_pct": ((price - sup["high"]) / price * 100) if sup else None,
                        "res_dist_pct": ((res["low"] - price) / price * 100) if res else None,
                        "in_support": bool(mtf.in_zone(price, sup, tol)), "in_resistance": bool(mtf.in_zone(price, res, tol)),
                        "fresh": True}
        def chg(key, n):
            c = cands.get(key) or []
            return ((price / c[-n]["c"] - 1) * 100) if len(c) >= n and c[-n]["c"] else None
        # extremes 24 h (H1) et 7 j (D1)
        h1 = cands.get("h1") or []
        d1 = cands.get("d1") or []
        hi24 = max([x["h"] for x in h1[-24:]] + [price]) if h1 else None
        lo24 = min([x["l"] for x in h1[-24:]] + [price]) if h1 else None
        hi7 = max([x["h"] for x in d1[-7:]] + [price]) if d1 else None
        lo7 = min([x["l"] for x in d1[-7:]] + [price]) if d1 else None
        out = {"ok": True, "ticker": ticker, "price": price, "ts": time.time(),
               "chg_1h": chg("h1", 1), "chg_4h": chg("h1", 4), "chg_24h": chg("h1", 24), "chg_7d": chg("d1", 7),
               "hi24": hi24, "lo24": lo24, "hi7": hi7, "lo7": lo7, "tf": tfs}
        # contexte : BTC (tendance H4) et funding
        btc = None
        if ticker != "BTC":
            try:
                bc = bot._mtf_candles("BTC", "4h", 260, cache_only=True)
                bp = self.price("BTC", "perp")
                if len(bc) >= 60 and bp:
                    btc = mtf.trend(bc + [{**bc[-1], "c": float(bp)}])[0]
            except Exception:
                btc = None
        out["btc_trend"] = btc
        try:
            fr = (getattr(bot, "funding_rates", None) or {}).get(ticker)
            out["funding_hourly_pct"] = (float(fr) * 100) if fr is not None else None
            out["funding_annual_pct"] = (float(fr) * 24 * 365 * 100) if fr is not None else None
        except Exception:
            out["funding_hourly_pct"] = out["funding_annual_pct"] = None
        out["advice"] = self._asset_advice(out)
        try:
            out["scenarios"] = self._asset_scenarios(out, zs.get("h4") or [], cands)
        except Exception:
            out["scenarios"] = []
        # v4.409 — historique propre a l actif : chaque situation rejouee sur ses bougies H4 passees
        try:
            bt = self._scenario_backtest(ticker)
        except Exception as e:
            bt = {"ok": False, "why": f"{type(e).__name__}: {e}"}
        out["backtest"] = {k: v for k, v in bt.items() if k != "stats"}
        for sc in out["scenarios"]:
            sc["hist"] = (bt.get("stats") or {}).get(sc["id"])
        out["best_scenario"] = self._pick_best_scenario(out["scenarios"], bt)
        return out

    # ───────── v4.409 — PRE-PROGRAMMATION : la situation au meilleur historique sur CET actif ─────────
    _BT_KINDS = ("bounce_support", "reject_resistance", "breakout_up", "breakout_down")
    _BT_ARM_BARS, _BT_HOLD_BARS, _BT_MIN_N = 18, 24, 8        # validite 72 h, tenue max 96 h, echantillon minimal
    _BT_FEE = 0.0009                                          # aller-retour (2 x 0,045 %)

    @staticmethod
    def _levels(kind, price, a, sup, res, zs):
        """Prix de declenchement / stop / objectif d une situation (meme regle pour le direct et l historique)."""
        if a <= 0:
            return None
        if kind == "bounce_support" and sup:
            trig = sup["high"] + 0.1 * a
            if trig >= price:
                return None
            tp = res["low"] if res and res["low"] > trig else trig + 2 * a
            side, ex, sl = "long", "price_below", sup["low"] - 0.15 * a
        elif kind == "reject_resistance" and res:
            trig = res["low"] - 0.1 * a
            if trig <= price:
                return None
            tp = sup["high"] if sup and sup["high"] < trig else trig - 2 * a
            side, ex, sl = "short", "price_above", res["high"] + 0.15 * a
        elif kind == "breakout_up" and res:
            trig = res["high"] + 0.1 * a
            nxt = [z for z in zs if z["low"] > res["high"] + 0.5 * a]
            tp = nxt[0]["low"] if nxt else trig + 2 * a
            side, ex, sl = "long", "price_above", res["low"] - 0.15 * a
        elif kind == "breakout_down" and sup:
            trig = sup["low"] - 0.1 * a
            nxt = [z for z in reversed(zs) if z["high"] < sup["low"] - 0.5 * a]
            tp = nxt[0]["high"] if nxt else trig - 2 * a
            side, ex, sl = "short", "price_below", sup["high"] + 0.15 * a
        else:
            return None
        risk, gain = abs(trig - sl), abs(tp - trig)
        if risk <= 0 or gain <= 0:
            return None
        return {"side": side, "ex": ex, "trig": trig, "sl": sl, "tp": tp, "risk": risk, "gain": gain}

    def _hist_h4(self, ticker, n=1000):
        end_ms = int(time.time() * 1000)
        sec = 14400
        raw = self.bot.info.post("/info", {"type": "candleSnapshot", "req": {
            "coin": ticker, "interval": "4h", "startTime": end_ms - (n + 1) * sec * 1000, "endTime": end_ms}})
        if not isinstance(raw, list):
            return []
        return [{"t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]), "l": float(c["l"]), "c": float(c["c"]),
                 "v": float(c.get("v", 0))} for c in raw if int(c.get("T", 0)) <= end_ms]

    def _scenario_backtest(self, ticker):
        """Rejoue chaque situation sur l historique H4 de l actif (jusqu a ~6 mois) : declenchement dans les 72 h,
        puis stop / objectif / sortie apres 96 h, frais aller-retour deduits. Un meme episode n est compte qu une fois.
        Resultat en multiples du risque (R). Cache par bougie H4."""
        import mtf_analysis as mtf
        cache = getattr(self, "_bt_cache", None)
        if cache is None:
            cache = self._bt_cache = {}
        try:
            days_cfg = max(30, min(180, int(self.cfg.get("ASSET_BT_DAYS", 90))))
        except Exception:
            days_cfg = 90
        bucket = int(time.time() // 14400)
        hit = cache.get((ticker, days_cfg))
        if hit and hit[0] == bucket:
            return hit[1]
        c = self._hist_h4(ticker, days_cfg * 6 + 150)           # + 150 bougies de chauffe pour les zones
        if len(c) < 220:
            res_ = {"ok": False, "why": f"historique H4 trop court ({len(c)} bougies)"}
            cache[(ticker, days_cfg)] = (bucket, res_)
            return res_
        i_start = max(120, len(c) - days_cfg * 6)               # on ne juge que la fenetre voulue
        i_half = len(c) - days_cfg * 3                          # seconde moitie = la plus recente
        # niveaux a chaque bougie cloturee (zones recalculees avec les seules donnees connues a ce moment)
        lv = {}
        for i in range(i_start, len(c) - 1):
            win = c[max(0, i - 149): i + 1]
            a = mtf.atr(win) or 0
            zones = mtf.find_zones(win, lookback=120, max_width_atr=3)
            price = c[i]["c"]
            sup, res = mtf.nearest_zones(zones, price)
            zs = sorted(zones, key=lambda z: z["low"])
            lv[i] = {k: self._levels(k, price, a, sup, res, zs) for k in self._BT_KINDS}
        stats = {}
        for kind in self._BT_KINDS:
            n_arm = 0
            rs, rrec, wins, losses, tos = [], [], 0, 0, 0
            i = i_start
            while i < len(c) - 1:
                L = (lv.get(i) or {}).get(kind)
                if not L:
                    i += 1
                    continue
                n_arm += 1
                trig, sl, tp, long = L["trig"], L["sl"], L["tp"], L["side"] == "long"
                below = L["ex"] == "price_below"
                j0 = None
                for j in range(i + 1, min(i + 1 + self._BT_ARM_BARS, len(c))):
                    if (c[j]["l"] <= trig) if below else (c[j]["h"] >= trig):
                        j0 = j
                        break
                if j0 is None:
                    i += 6
                    continue
                fee_r = self._BT_FEE * trig / L["risk"]
                r, end = None, j0
                if (c[j0]["l"] <= sl) if long else (c[j0]["h"] >= sl):       # stop touche dans la bougie de declenchement
                    r, losses = -1.0, losses + 1
                else:
                    last = min(j0 + self._BT_HOLD_BARS, len(c) - 1)
                    for j in range(j0 + 1, last + 1):
                        hit_sl = (c[j]["l"] <= sl) if long else (c[j]["h"] >= sl)
                        hit_tp = (c[j]["h"] >= tp) if long else (c[j]["l"] <= tp)
                        if hit_sl:                                            # stop prioritaire (hypothese prudente)
                            r, losses, end = -1.0, losses + 1, j
                            break
                        if hit_tp:
                            r, wins, end = L["gain"] / L["risk"], wins + 1, j
                            break
                    if r is None:
                        end = last
                        r = ((c[last]["c"] - trig) if long else (trig - c[last]["c"])) / L["risk"]
                        tos += 1
                rs.append(r - fee_r)
                if j0 >= i_half:
                    rrec.append(r - fee_r)
                i = max(i + 1, end + 1)
            n = len(rs)
            stats[kind] = {"armed": n_arm, "n": n, "wins": wins, "losses": losses, "timeouts": tos,
                           "p_tp": (wins / n) if n else None, "exp_r": (sum(rs) / n) if n else None,
                           "sum_r": sum(rs) if n else 0.0,
                           "n_recent": len(rrec), "exp_r_recent": (sum(rrec) / len(rrec)) if rrec else None}
        days = (c[-1]["t"] - c[i_start]["t"]) / 86400000.0
        res_ = {"ok": True, "bars": len(c), "days": round(days), "stats": stats}
        cache[(ticker, days_cfg)] = (bucket, res_)
        return res_

    @classmethod
    def _pick_best_scenario(cls, scenarios, bt):
        """Retient LA situation la plus favorable : esperance nette historique positive, echantillon suffisant,
        gain/risque actuel >= 1 ; classement par esperance ponderee par la taille de l echantillon."""
        best, best_score = None, None
        for sc in scenarios:
            h = sc.get("hist") or {}
            n, e = h.get("n") or 0, h.get("exp_r")
            er, nr = h.get("exp_r_recent"), h.get("n_recent") or 0
            recent_bad = bool(er is not None and nr >= 4 and er <= 0)      # la moitie la plus recente doit confirmer
            sc["eligible"] = bool(e is not None and n >= cls._BT_MIN_N and e > 0 and sc["rr"] >= 1.0 and not recent_bad)
            if not sc["eligible"]:
                sc["reject"] = ("défavorable sur la moitié la plus récente (" + format(er, "+.2f") + " R sur " + str(nr) + " cas) : le régime a peut-être changé") if (recent_bad and e is not None and e > 0 and n >= cls._BT_MIN_N) else ("échantillon trop faible (" + str(n) + " cas)") if n < cls._BT_MIN_N else \
                    ("historique défavorable (espérance nette " + format(e, "+.2f") + " R)") if (e is not None and e <= 0) else \
                    ("gain/risque actuel trop faible (" + format(sc["rr"], ".2f") + ")")
                continue
            score = e * n / (n + 10.0) + 0.02 * (len(sc.get("pros") or []) - len(sc.get("cons") or []))
            sc["score"] = round(score, 3)
            if best_score is None or score > best_score:
                best, best_score = sc["id"], score
        return best

    @staticmethod
    def _asset_scenarios(o, zones, cands=None):
        """Situations a attendre (a programmer en ordre sur prix) a partir des zones H4 : rebond sur support,
        rejet de resistance, cassure haussiere, cassure baissiere. Chaque scenario donne le prix de
        declenchement, un stop au-dela de la zone, un objectif sur la zone suivante et le gain/risque."""
        h4 = (o.get("tf") or {}).get("h4") or {}
        if not h4.get("ok"):
            return []
        price, a = o["price"], h4.get("atr") or 0
        if a <= 0:
            return []
        sup, res = h4.get("support"), h4.get("resistance")
        tr = h4.get("trend")
        d1 = ((o.get("tf") or {}).get("d1") or {}).get("trend")
        lean = (o.get("advice") or {}).get("lean")
        zs = sorted(zones or [], key=lambda z: z["low"])
        out = []
        def add(side, kind, label, wait_txt, trig, sl, tp, ex, aligned, note):
            risk, gain = abs(trig - sl), abs(tp - trig)
            if risk <= 0 or gain <= 0:
                return
            out.append({"id": kind, "side": side, "label": label, "wait": wait_txt, "execution": ex,
                        "trigger": trig, "trigger_dist_pct": (trig / price - 1) * 100,
                        "sl": sl, "tp": tp, "sl_pct": risk / trig * 100, "tp_pct": gain / trig * 100,
                        "rr": gain / risk, "aligned": bool(aligned), "note": note})
        # 1) LONG : rebond sur support (le prix doit redescendre vers la zone)
        if sup:
            trig = sup["high"] + 0.1 * a
            if trig < price:
                tp = res["low"] if res and res["low"] > trig else trig + 2 * a
                add("long", "bounce_support", "LONG sur rebond du support H4",
                    f"que le prix redescende vers le support {sup['low']:.6g}–{sup['high']:.6g}", trig, sup["low"] - 0.15 * a, tp,
                    "price_below", tr != "baissiere", "Pertinent tant que la tendance H4 n'est pas baissière." if tr != "baissiere" else "Contre la tendance H4 baissière : risque de cassure du support.")
        # 2) SHORT : rejet sous la resistance (le prix doit remonter vers la zone)
        if res:
            trig = res["low"] - 0.1 * a
            if trig > price:
                tp = sup["high"] if sup and sup["high"] < trig else trig - 2 * a
                add("short", "reject_resistance", "SHORT sur rejet de la résistance H4",
                    f"que le prix remonte vers la résistance {res['low']:.6g}–{res['high']:.6g}", trig, res["high"] + 0.15 * a, tp,
                    "price_above", tr != "haussiere", "Pertinent tant que la tendance H4 n'est pas haussière." if tr != "haussiere" else "Contre la tendance H4 haussière : risque de cassure de la résistance.")
        # 3) LONG : cassure de la resistance
        if res:
            trig = res["high"] + 0.1 * a
            nxt = [z for z in zs if z["low"] > res["high"] + 0.5 * a]
            tp = nxt[0]["low"] if nxt else trig + 2 * a
            add("long", "breakout_up", "LONG sur cassure de la résistance H4",
                f"que le prix dépasse {res['high']:.6g} (cassure)", trig, res["low"] - 0.15 * a, tp,
                "price_above", tr == "haussiere" or (tr == "neutre" and d1 != "baissiere"),
                "Cassure dans le sens de la tendance." if tr == "haussiere" else "Cassure sans tendance H4 haussière : risque de fausse cassure.")
        # 4) SHORT : cassure du support
        if sup:
            trig = sup["low"] - 0.1 * a
            nxt = [z for z in reversed(zs) if z["high"] < sup["low"] - 0.5 * a]
            tp = nxt[0]["high"] if nxt else trig - 2 * a
            add("short", "breakout_down", "SHORT sur cassure du support H4",
                f"que le prix passe sous {sup['low']:.6g} (cassure)", trig, sup["high"] + 0.15 * a, tp,
                "price_below", tr == "baissiere" or (tr == "neutre" and d1 != "haussiere"),
                "Cassure dans le sens de la tendance." if tr == "baissiere" else "Cassure sans tendance H4 baissière : risque de fausse cassure.")
        zmap = {"bounce_support": sup, "breakout_down": sup, "reject_resistance": res, "breakout_up": res}
        for sc in out:
            try:
                sc["pros"], sc["cons"] = ManualTrading._scenario_args(sc["id"], sc["side"], zmap.get(sc["id"]), o, cands or {})
            except Exception:
                sc["pros"], sc["cons"] = [], []
        out.sort(key=lambda x: (not x["aligned"], 0 if x["side"] == lean else 1, -x["rr"]))
        return out

    @staticmethod
    def _scenario_args(kind, side, z, o, cands):
        """Arguments POUR / CONTRE un scenario (regles usuelles d analyse technique, non validees statistiquement)."""
        import mtf_analysis as mtf
        tf = o.get("tf") or {}
        h4 = tf.get("h4") or {}
        a = h4.get("atr") or 0
        pros, cons = [], []
        P, C = pros.append, cons.append
        long = side == "long"
        bounce = kind in ("bounce_support", "reject_resistance")
        names = {"haussiere": "haussière", "baissiere": "baissière"}
        for key, lbl in (("h4", "H4"), ("d1", "D1")):
            t = (tf.get(key) or {}).get("trend") if (tf.get(key) or {}).get("ok") else None
            if t in names:
                if (t == "haussiere") == long:
                    P(f"Tendance {lbl} {names[t]} : dans le sens du trade")
                else:
                    C(f"Tendance {lbl} {names[t]} : à contre-courant")
        r = h4.get("rsi")
        if r is not None:
            if bounce and long:
                (P if r <= 40 else C if r >= 65 else (lambda x: None))(f"RSI H4 {r:.0f} : {'survente, rebond plausible' if r <= 40 else 'encore haut pour un rebond'}")
            elif bounce and not long:
                (P if r >= 60 else C if r <= 35 else (lambda x: None))(f"RSI H4 {r:.0f} : {'surachat, repli plausible' if r >= 60 else 'déjà bas pour un repli'}")
            elif long:
                (P if 50 <= r < 72 else C if (r >= 75 or r < 45) else (lambda x: None))(f"RSI H4 {r:.0f} : {'élan haussier sans excès' if 50 <= r < 72 else 'trop étiré, cassure essoufflée' if r >= 75 else 'pas d élan haussier'}")
            else:
                (P if 28 < r <= 50 else C if (r <= 25 or r > 55) else (lambda x: None))(f"RSI H4 {r:.0f} : {'élan baissier sans excès' if 28 < r <= 50 else 'déjà très bas, risque de rebond' if r <= 25 else 'pas d élan baissier'}")
        if z:
            tc = z.get("touches", 1)
            if bounce:
                if 2 <= tc <= 4:
                    P(f"Zone confirmée ({tc} contacts) et encore solide")
                elif tc <= 1:
                    C("Zone peu testée (1 contact) : fiabilité faible")
                else:
                    C(f"Zone très sollicitée ({tc} contacts) : elle s'use et finit souvent par céder")
            else:
                if tc >= 5:
                    P(f"Zone très testée ({tc} contacts) : elle s'use, la cassure devient plus probable")
            # confluence sur les autres unites de temps
            role = "support" if kind in ("bounce_support", "breakout_down") else "resistance"
            conf = []
            for key, lbl in (("h1", "H1"), ("d1", "D1")):
                zz = (tf.get(key) or {}).get(role) if (tf.get(key) or {}).get("ok") else None
                if zz and a and zz["low"] <= z["high"] + 0.5 * a and zz["high"] >= z["low"] - 0.5 * a:
                    conf.append(lbl)
            if conf:
                if bounce:
                    P(f"Même zone retrouvée en {' et '.join(conf)} : niveau confirmé sur plusieurs échelles")
                else:
                    C(f"Zone renforcée en {' et '.join(conf)} : plus difficile à franchir")
            # bougie de rejet (rebonds uniquement)
            c4 = (cands or {}).get("h4") or []
            if bounce and c4 and a:
                c = c4[-1]
                rng = c["h"] - c["l"]
                if rng > 0:
                    if long and (min(c["o"], c["c"]) - c["l"]) / rng >= 0.5 and c["l"] <= z["high"] + 0.3 * a:
                        P("Dernière bougie H4 : longue mèche basse près de la zone (rejet acheteur)")
                    if (not long) and (c["h"] - max(c["o"], c["c"])) / rng >= 0.5 and c["h"] >= z["low"] - 0.3 * a:
                        P("Dernière bougie H4 : longue mèche haute près de la zone (rejet vendeur)")
            # volume et compression (cassures)
            if not bounce and len(c4) >= 25:
                base = sum(x.get("v", 0) for x in c4[-21:-1]) / 20.0
                if base > 0:
                    vr = c4[-1].get("v", 0) / base
                    if vr >= 1.3:
                        P(f"Volume H4 récent ×{vr:.1f} la moyenne : participation en hausse")
                    elif vr <= 0.7:
                        C(f"Volume H4 récent faible (×{vr:.1f}) : cassure peu appuyée")
                a_short, a_long = mtf.atr(c4[-7:]) or 0, mtf.atr(c4[-31:]) or 0
                if a_long > 0:
                    cr = a_short / a_long
                    if cr < 0.85:
                        P("Volatilité en compression : une cassure sort souvent plus franche")
                    elif cr > 1.3:
                        C("Volatilité déjà élevée : une partie du mouvement est peut-être consommée")
        bt = o.get("btc_trend")
        if bt in names and o.get("ticker") != "BTC":
            if (bt == "haussiere") == long:
                P(f"BTC {names[bt]} (H4) : contexte favorable")
            else:
                C(f"BTC {names[bt]} (H4) : contexte défavorable")
        fa = o.get("funding_annual_pct")
        if fa is not None and abs(fa) >= 30:
            if (fa < 0) == long:
                P(f"Funding {fa:+.0f} %/an : la foule est de l'autre côté")
            else:
                C(f"Funding {fa:+.0f} %/an : la foule est déjà dans ce sens")
        return pros, cons

    @staticmethod
    def _asset_advice(o):
        tf = o["tf"]
        pts, why = 0.0, []
        def T(k):
            return (tf.get(k) or {}).get("trend") if (tf.get(k) or {}).get("ok") else None
        for key, w, lbl in (("h4", 2.0, "H4"), ("d1", 1.0, "D1"), ("h1", 1.0, "H1")):
            t = T(key)
            if t == "haussiere":
                pts += w; why.append((w, f"Tendance {lbl} haussière"))
            elif t == "baissiere":
                pts -= w; why.append((-w, f"Tendance {lbl} baissière"))
            elif t:
                why.append((0, f"Tendance {lbl} neutre"))
        h4 = tf.get("h4") or {}
        if h4.get("ok"):
            r = h4.get("rsi")
            if r is not None:
                if r >= 70:
                    pts -= 1; why.append((-1, f"RSI H4 {r:.0f} : surachat (risque de repli)"))
                elif r <= 30:
                    pts += 1; why.append((1, f"RSI H4 {r:.0f} : survente (rebond possible)"))
            if h4.get("in_support"):
                pts += 1; why.append((1, "Prix sur un support H4"))
            if h4.get("in_resistance"):
                pts -= 1; why.append((-1, "Prix sur une résistance H4"))
        h1 = tf.get("h1") or {}
        if h1.get("ok"):
            if h1.get("in_support"):
                pts += 0.5; why.append((0.5, "Prix sur un support H1"))
            if h1.get("in_resistance"):
                pts -= 0.5; why.append((-0.5, "Prix sur une résistance H1"))
        bt = o.get("btc_trend")
        if bt == "haussiere":
            pts += 0.5; why.append((0.5, "BTC haussier (H4)"))
        elif bt == "baissiere":
            pts -= 0.5; why.append((-0.5, "BTC baissier (H4)"))
        fa = o.get("funding_annual_pct")
        if fa is not None and abs(fa) >= 30:
            if fa > 0:
                pts -= 0.5; why.append((-0.5, f"Funding {fa:+.0f} %/an : les longs paient (foule longue)"))
            else:
                pts += 0.5; why.append((0.5, f"Funding {fa:+.0f} %/an : les shorts paient (foule short)"))
        SEUIL, RR_MIN = 3.0, 1.2          # score minimal ; gain/risque minimal d une entree immediate
        wait = []
        if pts >= SEUIL:
            side, label = "long", "LONG"
        elif pts <= -SEUIL:
            side, label = "short", "SHORT"
        else:
            side, label = None, "ATTENDRE"
            wait.append(f"Signaux insuffisants ou partagés (score {pts:+.1f}, il faut au moins ±{SEUIL:.0f}).")
        lean = "long" if pts > 0 else ("short" if pts < 0 else None)
        conf = "forte" if abs(pts) >= 4.5 else ("moyenne" if abs(pts) >= SEUIL else "faible")
        # niveaux de reference (structure H4) pour le sens penche
        plan = None
        side_plan = side or lean
        if side_plan and h4.get("ok"):
            price, a = o["price"], h4.get("atr") or 0
            sup, res = h4.get("support"), h4.get("resistance")
            if side_plan == "long":
                sl = (sup["low"] - 0.15 * a) if sup else price - 1.5 * a
                tp = res["low"] if res else price + 2 * a
            else:
                sl = (res["high"] + 0.15 * a) if res else price + 1.5 * a
                tp = sup["high"] if sup else price - 2 * a
            risk, gain = abs(price - sl), abs(tp - price)
            if risk > 0:
                plan = {"side": side_plan, "sl": sl, "tp": tp, "sl_pct": risk / price * 100, "tp_pct": gain / price * 100,
                        "rr": gain / risk}
        # entree immediate trop chere : bonne direction mais gain/risque insuffisant -> ATTENDRE un meilleur prix
        if side and plan and plan["rr"] < RR_MIN:
            wait.append(f"Direction {label}, mais entrer maintenant donne un gain/risque de {plan['rr']:.2f} (minimum {RR_MIN}) : mieux vaut attendre un meilleur prix.")
            side, label, conf = None, "ATTENDRE", "faible"
        elif not side and plan and plan["rr"] < RR_MIN and lean:
            wait.append(f"Entrer maintenant dans le sens {lean.upper()} donnerait un gain/risque de {plan['rr']:.2f} seulement.")
        why.sort(key=lambda x: -abs(x[0]))
        return {"wait": wait, "verdict": label, "side": side, "lean": lean, "score": round(pts, 1), "confidence": conf,
                "reasons": [t for _, t in why], "plan": plan,
                "note": "Conseil indicatif fondé sur des règles (tendance, RSI, zones, BTC, funding) — pas une garantie."}

    def _check_trend_entry(self, ticker, required):
        """Condition d entree sur la tendance H4 : leve ManualError si elle n est pas remplie
        (ou si la tendance est indisponible / perimee : on n ouvre jamais a l aveugle)."""
        info = self.trend_info(ticker)
        if not info["ok"]:
            raise ManualError(f"Tendance H4 indisponible pour {ticker} ({info['why']}) — ordre non ouvert (condition d entree demandee).")
        if not info["data_fresh"]:
            raise ManualError(f"Donnees H4 de {ticker} perimees — ordre non ouvert (condition d entree demandee).")
        if info["trend"] != required:
            raise ManualError(f"Tendance H4 de {ticker} : {TREND_FR.get(info['trend'], info['trend'])} "
                              f"(requis : {TREND_FR.get(required, required)}) — ordre non ouvert.")
        return info["trend"]

    def _check_trend_exit_not_immediate(self, ticker, direction, rule):
        """v4.344 — COHERENCE : si la regle de sortie sur la tendance se declencherait des le premier
        controle (ex. LONG avec "si la tendance devient opposee" alors que la tendance H4 est DEJA
        baissiere), la position s ouvrirait puis se fermerait aussitot : deux ordres, des frais, aucun
        sens. On refuse avec un message explicite. Sans effet si la tendance est indisponible/perimee
        (la regle ne pourrait de toute facon pas agir)."""
        info = self.trend_info(ticker)
        if not info["ok"] or not info["data_fresh"]:
            return
        want = "haussiere" if direction == "long" else "baissiere"
        opposite = "baissiere" if direction == "long" else "haussiere"
        tr = info["trend"]
        if (rule == "opposee" and tr == opposite) or (rule == "perdue" and tr != want):
            raise ManualError(
                f"Avec cette regle de sortie, la position se fermerait aussitot : la tendance H4 de {ticker} est "
                f"{TREND_FR.get(tr, tr)}, {'opposee' if tr == opposite else 'differente de celle attendue'} pour un "
                f"{direction.upper()}. Modifiez la regle de sortie sur la tendance (ou la condition d entree).")

    @staticmethod
    def _validate_lock_trend(p):
        """Validation des options v4.343 : verrou progressif et criteres de tendance."""
        la, lf = p.get("lock_arm_pct"), p.get("lock_fraction")
        if (la in (None, "", 0)) != (lf in (None, "", 0)):
            raise ManualError("Verrou progressif : renseignez a la fois le seuil de depart et la part garantie (ou aucun des deux).")
        if la not in (None, "", 0):
            la, lf = float(la), float(lf)
            if not 0.05 <= la <= 500:
                raise ManualError("Verrou progressif : seuil de depart entre 0,05 % et 500 %.")
            if not 0.05 <= lf <= 0.95:
                raise ManualError("Verrou progressif : part garantie entre 5 % et 95 % du gain maximal.")
            p["lock_arm_pct"], p["lock_fraction"] = la, lf
        else:
            p["lock_arm_pct"] = p["lock_fraction"] = None
        te = p.get("trend_entry") or None
        if te not in (None, "haussiere", "baissiere", "neutre"):
            raise ManualError("Condition d entree sur la tendance invalide.")
        tx = p.get("trend_exit") or None
        if tx not in (None, "opposee", "perdue"):
            raise ManualError("Regle de sortie sur la tendance invalide.")
        p["trend_entry"], p["trend_exit"] = te, tx

    def _validate_tp1(self, p, tp):
        """v4.404 : TP1 = cloture partielle a un premier objectif ; le TP existant devient le TP2 (le reste)."""
        t1 = p.get("tp1_pct")
        if t1 in (None, "", 0):
            p["tp1_pct"] = None
            return
        t1 = float(t1)
        if tp in (None, "", 0):
            raise ManualError("TP1 : un TP2 (take profit final) est necessaire.")
        if not 0.05 <= t1 < float(tp):
            raise ManualError("TP1 : doit etre compris entre 0,05 % et le TP2 (exclu).")
        cp = float(p.get("tp1_close_pct") or 50)
        if not 5 <= cp <= 95:
            raise ManualError("TP1 : la part a cloturer doit etre comprise entre 5 % et 95 %.")
        if p.get("mode") == "live" and p.get("market") == "spot":
            raise ManualError("TP1 (cloture partielle) n est pas disponible en spot LIVE.")
        p["tp1_pct"], p["tp1_close_pct"] = t1, cp
        p["tp1_be"] = bool(p.get("tp1_be", True))

    def _propose_swing_support(self, ticker, direction, market):
        """v4.381 : ticket prepare depuis une alerte Swing Support (n importe quel actif crypto). Memes stop / objectif / levier
        / taille que le mode Swing Support, tout reste modifiable ; aucune opportunite du bot n est requise."""
        if direction != "long":
            raise ManualError("Swing Support : achat (LONG) uniquement.")
        p = self._propose_free(ticker, direction, market)
        cfg = self.cfg
        lev = 1 if market == "spot" else max(int(cfg.get("SUPSW_LEVERAGE", 3)), 1)
        sl, tp = float(cfg.get("SUPSW_SL_PCT", 5.0)), float(cfg.get("SUPSW_TP_PCT", 2.0))
        if int(cfg.get("SUPSW_LEVER_SCALE", 0)) == 1:
            sl, tp = sups.scaled_levels(lev, sl, tp, float(cfg.get("SUPSW_MARGIN_RISK_PCT", 7.5)), float(cfg.get("SUPSW_MIN_SL_PCT", 1.5)),
                                        float(cfg.get("SUPSW_TP_LEV1", 2.0)), float(cfg.get("SUPSW_TP_LEV2", 1.5)), float(cfg.get("SUPSW_TP_LEV3", 1.0)))
        notional = max(min(float(cfg.get("CAPITAL_USD", 100.0)) * float(cfg.get("SUPSW_RISK_PCT", 1.0)) / sl, float(cfg.get("SUPSW_MAX_NOTIONAL_USD", 60.0))), 10.0)
        p.update({"strategy": "swing_support", "free": False, "no_ttp": True, "tp_run": True,
                  "tp_run_lock_pct": float(cfg.get("SUPSW_TRAIL_LOCK_PCT", 0.5)), "tp_run_trail_pct": float(cfg.get("SUPSW_TRAIL_PCT", 0.5)), "leverage": lev, "notional_usd": round(notional, 2),
                  "margin_usd": round(notional / lev, 2), "sl_pct": round(sl, 3), "sl_basis": "stop Swing Support (selon le levier)" if int(cfg.get("SUPSW_LEVER_SCALE", 0)) == 1 else "stop fixe Swing Support",
                  "tp_pct": round(tp, 3), "tp_basis": "objectif Swing Support",
                  "reasons": ["Ticket prepare depuis une alerte Swing Support : stop, objectif, levier et taille du mode (modifiables). "
                              "La sortie anticipee du mode ne s applique pas aux trades manuels."]})
        return p

    def propose(self, strategy, ticker, direction, market="perp"):
        if strategy == "libre":
            return self._propose_free(ticker, direction, market)
        if strategy == "swing_support":
            return self._propose_swing_support(ticker, direction, market)
        opp = self.get_opportunity(strategy, ticker, direction)
        if opp is None:
            raise ManualError("Cette opportunite n est pas (ou plus) valable dans ce sens.")
        if self.ticker_busy(ticker):
            raise ManualError(f"Une position est deja ouverte sur {ticker} : opportunite non proposee.")
        if market == "spot" and (direction != "long" or not self.spot_pair(ticker)):
            raise ManualError("Pas de marche spot disponible pour cet actif (le spot n autorise que l achat).")
        cfg = self.cfg
        price = self.price(ticker, market) or opp.get("price")
        if not price:
            raise ManualError("Prix indisponible pour le moment.")
        lev = 1 if market == "spot" else int(cfg.get("MANUAL_DEFAULT_LEVERAGE", 1))
        notional = float(cfg.get("MANUAL_DEFAULT_NOTIONAL_USD", 15.0))

        # SL : meme logique que le mode source
        if strategy == "spot_accumulation":
            sl_pct, sl_basis = (cfg.get("SPOT_ACCUM_SL_CAP_PCT") or cfg.get("STRUCTURAL_SL_HARD_CAP_PCT", 0.5)), "plafond SL Spot-Accum"
        elif strategy == "accumulation":
            sl_pct, sl_basis = (cfg.get("ACCUMULATION_SL_CAP_PCT") or cfg.get("STRUCTURAL_SL_HARD_CAP_PCT", 0.5)), "plafond SL Accumulation"
        else:
            sl_of_e = (cfg.get("FUNDING_SL_PCT_OF_E") if strategy == "funding_contrarian" else None) or cfg.get("SL_PCT_OF_E", 1.0)
            sl_pct = max(sl_of_e / max(lev, 1), cfg.get("SL_MIN_PRICE_MOVE_PCT", 0.3))
            sl_basis = f"SL {STRATEGY_LABELS.get(strategy, strategy)} ({sl_of_e}% de la marge / levier)"

        # TP : 80 % de la fourchette support/resistance si exploitable
        sup, res = opp.get("support"), opp.get("resistance")
        tp_pct, tp_basis = None, ""
        if sup and res and res > sup:
            target = sup + 0.8 * (res - sup) if direction == "long" else res - 0.8 * (res - sup)
            dist = (target - price) / price * 100 if direction == "long" else (price - target) / price * 100
            if dist >= max(sl_pct, 0.3):
                tp_pct, tp_basis = dist, "80 % de la fourchette support/resistance"
        if tp_pct is None:
            tp_pct, tp_basis = max(2 * sl_pct, cfg.get("TAKE_PROFIT_PCT", 1.5)), "2 x le SL (fourchette S/R inexploitable)"

        # TTP : reglages du mode source
        if strategy == "funding_contrarian":
            arm, trail = cfg.get("FUNDING_TTP_ARM_PCT", 1.0), cfg.get("FUNDING_TTP_TOLERANCE_PCT", 0.5)
        elif strategy == "accumulation":
            arm = cfg.get("ACCUMULATION_TTP_ARM1_PRICE_PCT") or cfg.get("TTP_ARM1_PRICE_PCT", 1.0)
            trail = cfg.get("ACCUMULATION_TTP_TRAIL_GAP_PRICE_PCT") or cfg.get("TTP_DYNAMIC_TRAIL_GAP_PCT", 0.5)
        else:
            arm, trail = cfg.get("TTP_ARM1_PRICE_PCT", 1.0), cfg.get("TTP_DYNAMIC_TRAIL_GAP_PCT", 0.5)

        return {
            "strategy": strategy, "ticker": ticker, "direction": direction, "market": market,
            "price": price, "leverage": lev, "notional_usd": round(notional, 2),
            "margin_usd": round(notional / max(lev, 1), 2),
            "sl_pct": round(sl_pct, 3), "sl_basis": sl_basis,
            "tp_pct": round(tp_pct, 3), "tp_basis": tp_basis,
            "ttp_arm_pct": arm, "ttp_trail_pct": trail,
            "confidence": opp.get("confidence"), "reasons": opp.get("reasons", []),
            "spot_available": self.spot_pair(ticker) is not None and direction == "long",
            "live_available": self.bot.exchange is not None and bool(self._wallet()),
            "min_notional_usd": 10.0,
        }

    # ───────────────────────────── validations ───────────────────────────
    def has_live_perp(self, ticker, exclude_id=None):
        return any(i["status"] in ("opening", "open", "closing") and i["mode"] == "live"
                   and i["market"] == "perp" and i["ticker"] == ticker
                   and i.get("id") != exclude_id for i in self.items.values())

    def live_perp_coins(self):
        return {i["ticker"] for i in self.items.values()
                if i["status"] in ("opening", "open", "closing") and i["mode"] == "live" and i["market"] == "perp"}

    def _bot_live_position_on(self, ticker):
        for pool in (self.bot.states, self.bot.accum_states):
            for slot, st in pool.items():
                if st.position and be.ticker_from_slot_key(slot) == ticker and self.bot._position_mode(st.position) == "live":
                    return True
        return False

    def _check_live_perp_allowed(self, ticker, exclude_id=None):
        """exclude_id (v4.344) : l ordre en cours d ouverture est deja dans self.items au statut "opening" ;
        sans l exclure, il se comptait LUI-MEME comme "position manuelle live deja ouverte" et aucun ordre
        live perp ne pouvait jamais s ouvrir (bug present depuis v4.273 : le controle est refait dans
        _open_impl, apres le passage au statut "opening")."""
        if self._bot_live_position_on(ticker):
            raise ManualError(f"Le bot detient deja une position LIVE sur {ticker} : Hyperliquid ne tient qu une position par actif (elles se fondraient et se fermeraient ensemble).")
        if self.has_live_perp(ticker, exclude_id=exclude_id):
            raise ManualError(f"Une position manuelle LIVE est deja ouverte sur {ticker}.")
        szi = be.get_exchange_position_szi(self.bot.info, self._wallet(), ticker)
        if szi is None:
            raise ManualError("Impossible de verifier les positions Hyperliquid pour le moment — reessayez.")
        if szi != 0:
            raise ManualError(f"Une position existe deja sur {ticker} dans votre compte Hyperliquid.")

    def _validate(self, p):
        for key in ("strategy", "ticker", "direction", "market", "mode", "execution"):
            if not p.get(key):
                raise ManualError(f"Champ manquant : {key}")
        if p["direction"] not in ("long", "short") or p["market"] not in ("perp", "spot") or p["mode"] not in ("paper", "live"):
            raise ManualError("Parametres invalides.")
        if p["strategy"] in FREE_LIKE:
            # v4.343 — ORDRE LIBRE : aucun besoin d une opportunite du bot ; l actif doit simplement etre suivi
            opp = {}
            p["require_valid"] = False
            if not self.price(p["ticker"], p["market"]):
                raise ManualError(f"Prix indisponible pour {p['ticker']} : actif inconnu ou non suivi par le bot.")
        else:
            opp = self.get_opportunity(p["strategy"], p["ticker"], p["direction"])
            if opp is None:
                raise ManualError("Cette opportunite n est plus valable (ou le sens ne correspond pas a celui prevu par le bot).")
        self._validate_lock_trend(p)
        if p["market"] == "spot":
            if p["direction"] != "long":
                raise ManualError("Le spot n autorise que l achat (LONG).")
            if not self.spot_pair(p["ticker"]):
                raise ManualError(f"Pas de marche spot pour {p['ticker']}.")
            p["leverage"] = 1
        lev = int(p.get("leverage") or 1)
        if not 1 <= lev <= int(self.cfg.get("MANUAL_MAX_LEVERAGE", 20)):
            raise ManualError(f"Levier hors limites (1 a {self.cfg.get('MANUAL_MAX_LEVERAGE', 20)}).")
        p["leverage"] = lev
        notional = float(p.get("notional_usd") or 0)
        if notional <= 0:
            raise ManualError("Taille invalide.")
        if p["mode"] == "live" and notional < 10:
            raise ManualError("Hyperliquid exige au moins 10 $ de notionnel par ordre.")
        sl = float(p.get("sl_pct") or 0)
        if not 0.05 <= sl <= 50:
            raise ManualError("Le stop loss doit etre compris entre 0,05 % et 50 % du prix d entree.")
        tp = p.get("tp_pct")
        if tp not in (None, "", 0) and not 0.05 <= float(tp) <= 500:
            raise ManualError("Take profit invalide (0,05 % a 500 %, ou vide).")
        if p.get("tp_run"):      # v4.395 : laisser courir au TP (verrou puis stop suiveur)
            if tp in (None, "", 0):
                raise ManualError("Laisser courir au TP : un take profit est necessaire.")
            lk, tr_ = float(p.get("tp_run_lock_pct") or 0), float(p.get("tp_run_trail_pct") or 0)
            if not (0 <= lk < float(tp)) or not 0.05 <= tr_ <= 50:
                raise ManualError("Laisser courir : le gain verrouille doit etre inferieur au TP (en %) et le suiveur compris entre 0,05 et 50 %.")
        self._validate_tp1(p, p.get("tp_pct"))
        arm, trail = float(p.get("ttp_arm_pct") or 0), float(p.get("ttp_trail_pct") or 0)
        if arm and not (0.05 <= arm <= 500 and 0.02 <= trail <= 100):
            raise ManualError("Trailing TP invalide (armement 0,05-500 %, repli 0,02-100 %).")
        if p["execution"] not in ("now", "price_above", "price_below", "time"):
            raise ManualError("Type d execution invalide.")
        if p["execution"] in ("price_above", "price_below") and not float(p.get("trigger_price") or 0) > 0:
            raise ManualError("Prix de declenchement manquant.")
        if p["execution"] == "time" and not float(p.get("trigger_time") or 0) > time.time():
            raise ManualError("L heure de declenchement doit etre dans le futur.")
        if p["mode"] == "live":
            if not p.get("confirm_live"):
                raise ManualError("Confirmation LIVE requise.")
            if self.bot.exchange is None or not self._wallet():
                raise ManualError("Connexion Hyperliquid indisponible : le live est impossible pour le moment.")
            if p["market"] == "perp":
                self._check_live_perp_allowed(p["ticker"])
        return opp

    # ───────────────────────────── soumission ────────────────────────────
    def submit(self, params):
        with self.lock:
            p = dict(params)
            opp = self._validate(p)
            if p["execution"] == "now" and p.get("trend_entry"):
                self._check_trend_entry(p["ticker"], p["trend_entry"])   # v4.343 : refuse avant toute creation
            if p["execution"] == "now" and p.get("trend_exit"):
                self._check_trend_exit_not_immediate(p["ticker"], p["direction"], p["trend_exit"])   # v4.344
            now = time.time()
            item = {
                "strategy_source": p["strategy"], "ticker": p["ticker"], "direction": p["direction"],
                "market": p["market"], "mode": p["mode"], "leverage": p["leverage"],
                "notional_usd": float(p["notional_usd"]),
                "sl_pct": float(p["sl_pct"]),
                "tp_pct": float(p["tp_pct"]) if p.get("tp_pct") not in (None, "", 0) else None,
                "ttp_arm_pct": float(p.get("ttp_arm_pct") or 0) or None,
                "ttp_trail_pct": float(p.get("ttp_trail_pct") or 0) or None,
                # v4.395 — laisser courir au TP : a l objectif, gain verrouille puis stop suiveur sous le plus haut
                "tp_run": bool(p.get("tp_run")) and p.get("tp_pct") not in (None, "", 0),
                "tp_run_lock_pct": float(p.get("tp_run_lock_pct") or 0), "tp_run_trail_pct": float(p.get("tp_run_trail_pct") or 0.5),
                "tp_run_armed": False,
                # v4.404 — TP1 : cloture partielle ; le reste continue vers le TP2 (tp_pct)
                "tp1_pct": float(p["tp1_pct"]) if p.get("tp1_pct") not in (None, "", 0) else None,
                "tp1_close_pct": float(p.get("tp1_close_pct") or 50), "tp1_be": bool(p.get("tp1_be", True)),
                "tp1_done": False, "realized_pnl": 0.0, "realized_fees": 0.0,
                # v4.343 — verrou progressif du SL + criteres de tendance (choix manuels)
                "lock_arm_pct": p.get("lock_arm_pct"), "lock_fraction": p.get("lock_fraction"), "lock_active": False,
                "trend_entry": p.get("trend_entry"), "trend_exit": p.get("trend_exit"),
                "execution": p["execution"],
                "trigger_price": float(p["trigger_price"]) if p.get("trigger_price") else None,
                "trigger_time": float(p["trigger_time"]) if p.get("trigger_time") else None,
                "expires_at": now + float(p.get("expiry_hours") or self.cfg.get("MANUAL_ORDER_EXPIRY_HOURS", 24)) * 3600,
                "require_valid": bool(p.get("require_valid", True)),
                "confidence": opp.get("confidence"), "reasons": opp.get("reasons", []),
                "created_at": now, "trade_uid": be.make_trade_uid("manual"),
            }
            status = "opening" if p["execution"] == "now" else "scheduled"
            item["id"] = db.manual_insert(status, item)
            item["status"] = status
            self.items[item["id"]] = item
        if p["execution"] == "now":
            self._open(item)
        else:
            self._log(f"Ordre programme #{item['id']} : {item['direction'].upper()} {item['ticker']} ({item['market']}, {item['mode']}) — {self._trigger_text(item)}", "info")
        return self.public(item)

    def _trigger_text(self, item):
        if item["execution"] == "price_above":
            return f"si le prix monte a {item['trigger_price']:.6g}"
        if item["execution"] == "price_below":
            return f"si le prix descend a {item['trigger_price']:.6g}"
        if item["execution"] == "time":
            return "a " + datetime.fromtimestamp(item["trigger_time"]).strftime("%d/%m %H:%M")
        return "immediat"

    # ─────────────────────────────── ouverture ───────────────────────────
    def _open(self, item):
        if item["id"] in self._busy:
            return
        self._busy.add(item["id"])
        try:
            self._open_impl(item)
        except Exception as e:
            item["status"], item["error"] = "failed", f"{type(e).__name__}: {e}"
            self._save(item)
            self._log(f"#{item['id']} {item['ticker']} : echec d ouverture — {item['error']}", "error")
        finally:
            self._busy.discard(item["id"])

    def _open_impl(self, item):
        ticker, market, is_long = item["ticker"], item["market"], item["direction"] == "long"
        price = self.price(ticker, market)
        if not price:
            raise ManualError("prix indisponible")
        item["status"] = "opening"
        self._save(item)
        if item["mode"] == "live":
            if market == "perp":
                self._check_live_perp_allowed(ticker, exclude_id=item["id"])
                entry, qty = self._open_live_perp(item, price, is_long)
            else:
                entry, qty = self._open_live_spot(item, price)
        else:
            entry = price
            qty = item["notional_usd"] / price
        item.update({
            "status": "open", "entry_price": entry, "qty": qty, "opened_at": time.time(),
            "notional_usd": qty * entry,
            "sl_price": entry * (1 - item["sl_pct"] / 100) if is_long else entry * (1 + item["sl_pct"] / 100),
            "tp_price": (entry * (1 + item["tp_pct"] / 100) if is_long else entry * (1 - item["tp_pct"] / 100)) if item["tp_pct"] else None,
            "tp1_price": (entry * (1 + item["tp1_pct"] / 100) if is_long else entry * (1 - item["tp1_pct"] / 100)) if item.get("tp1_pct") else None,
            "peak_pct": 0.0, "armed": False, "last_price": price, "lock_active": False,
            "entry_verified": bool(item["mode"] == "live" and market == "perp"),   # v4.344 : position constatee sur Hyperliquid
        })
        try:
            ti = self.trend_info(ticker, use_cache=True)
            item["trend_at_entry"] = ti.get("trend") if ti.get("ok") else None
        except Exception:
            item["trend_at_entry"] = None
        self._save(item)
        try:
            db.upsert_open_trade({
                "trade_uid": item["trade_uid"], "coin": ticker, "action": "LONG" if is_long else "SHORT",
                "entry": entry, "strategy": "manual", "trade_mode": item["mode"],
                "slot_key": f"MANUAL__{item['id']}", "leverage": item["leverage"],
                "size_usd": item["notional_usd"] / max(item["leverage"], 1), "confidence": item.get("confidence"),
                "stop_loss": item["sl_price"], "take_profit1": item["tp_price"],
                "entry_reasons": f"Manuel ({market.upper()}) sur opportunite {STRATEGY_LABELS.get(item['strategy_source'], item['strategy_source'])} | " + " | ".join(item.get("reasons", [])[:4]),
            })
        except Exception as e:
            print(f"[MANUEL] Ecriture historique #{item['id']} impossible : {e}")
        self._log(f"#{item['id']} OUVERT : {item['direction'].upper()} {ticker} {market.upper()} ({item['mode']}) @ {entry:.6g} | "
                  f"{item['notional_usd']:.2f} $ x{item['leverage']} | SL {item['sl_price']:.6g}"
                  + (f" | TP1 {item['tp1_price']:.6g} ({item['tp1_close_pct']:.0f} %)" if item.get("tp1_price") else "")
                  + (f" | TP {item['tp_price']:.6g}" if item["tp_price"] else "")
                  + (f" | TTP +{item['ttp_arm_pct']}%/-{item['ttp_trail_pct']}%" if item["ttp_arm_pct"] else "")
                  + (f" | verrou {round(item['lock_fraction'] * 100)}% des +{item['lock_arm_pct']}%" if item.get("lock_arm_pct") else "")
                  + (f" | tendance H4 a l entree : {TREND_FR.get(item.get('trend_at_entry'), '?')}" if item.get("trend_entry") or item.get("trend_exit") else ""), "signal")

    def _open_live_perp(self, item, price, is_long):
        ex, ticker, lev = self.bot.exchange, item["ticker"], item["leverage"]
        want = lev
        is_cross = not ticker.startswith("xyz:")
        try:
            resp = ex.update_leverage(lev, ticker, is_cross=is_cross)
        except Exception as e:
            raise ManualError(f"levier x{lev} refuse par Hyperliquid ({e})")
        # v4.412 : le SDK ne leve pas d exception quand Hyperliquid REFUSE le levier, il renvoie {"status": "err", ...}.
        # Avant, la refus passait inapercu : l ordre partait avec le levier deja en place sur Hyperliquid, mais le suivi
        # gardait le levier demande (marge, % de marge, PnL faux).
        if isinstance(resp, dict) and resp.get("status") not in (None, "ok"):
            why = str(resp.get("response") or resp)[:160]
            self._log(f"#{item['id']} {ticker} : levier x{want} REFUSE par Hyperliquid ({why})", "warn")
            item["leverage_note"] = f"levier x{want} refusé par Hyperliquid : {why}"
            mx = self._asset_max_leverage(ticker)
            if mx and mx < lev:
                try:
                    r2 = ex.update_leverage(mx, ticker, is_cross=is_cross)
                    if not (isinstance(r2, dict) and r2.get("status") not in (None, "ok")):
                        lev = mx
                        self._log(f"#{item['id']} {ticker} : levier ramene au maximum de l actif (x{mx})", "warn")
                except Exception:
                    pass
        sl_price = price * (1 - item["sl_pct"] / 100) if is_long else price * (1 + item["sl_pct"] / 100)
        margin = item["notional_usd"] / max(lev, 1)
        ok, err, fill = be.place_order(ex, ticker, is_long, margin, price, self.cfg, sl_price=sl_price,
                                       tp_price=None, leverage=lev, trade_uid=item["trade_uid"])
        if not ok:
            raise ManualError(err or "ordre refuse")
        ep = None
        for _ in range(4):
            ep = (be.fetch_exchange_positions(self.bot.info, self._wallet(), [ticker]) or {}).get(ticker)
            if ep and ep["szi"] != 0:
                break
            time.sleep(0.7)
        if not ep or ep["szi"] == 0:
            raise ManualError("ordre accepte mais aucune position constatee sur Hyperliquid")
        # v4.412 : le levier REEL de la position fait foi (marge, % de marge, affichage)
        real = ep.get("leverage")
        try:
            real = int(float(real)) if real else None
        except (TypeError, ValueError):
            real = None
        final = real or lev
        if final != want:
            item["leverage_requested"] = want
            item.setdefault("leverage_note", f"levier x{want} demandé, x{final} appliqué par Hyperliquid")
            self._log(f"#{item['id']} {ticker} : levier REEL x{final} (demande x{want}) — suivi mis a jour", "warn")
        item["leverage"] = final
        return (fill or ep["entry"] or price), abs(ep["szi"])

    def _asset_max_leverage(self, ticker):
        try:
            for u in (self.bot.info.meta() or {}).get("universe", []):
                if u.get("name") == ticker:
                    return int(u.get("maxLeverage") or 0) or None
        except Exception:
            pass
        return None

    def _spot_available(self, token):
        state = self.bot.info.spot_user_state(self._wallet()) or {}
        for b in state.get("balances", []):
            if b.get("coin") == token:
                return float(b.get("total", 0) or 0) - float(b.get("hold", 0) or 0)
        return 0.0

    def _open_live_spot(self, item, price):
        sp = self.spot_pair(item["ticker"])
        usdc = self._spot_available("USDC")
        if usdc < item["notional_usd"]:
            raise ManualError(f"USDC insuffisant cote SPOT ({usdc:.2f} $ disponibles, {item['notional_usd']:.2f} $ requis) — transferez des USDC du compte Perp vers le compte Spot.")
        before = self._spot_available(sp["base"])
        sz = be.format_size_hl(item["notional_usd"] / price, sp["sz_decimals"])
        result = self.bot.exchange.market_open(sp["pair"], True, sz)
        status = be._order_first_status(result) if result else None
        if not (result and result.get("status") == "ok" and status and "filled" in status):
            raise ManualError(f"achat spot refuse : {str(result)[:200]}")
        fill = float(status["filled"]["avgPx"])
        time.sleep(0.7)
        qty = self._spot_available(sp["base"]) - before  # quantite REELLEMENT recue (frais preleves en jetons)
        if qty <= 0:
            qty = float(status["filled"]["totalSz"])
        return fill, qty

    # ─────────────────────────────── fermeture ───────────────────────────
    def close(self, item_id, reason="MANUEL"):
        item = self.items.get(item_id)
        if not item or item["status"] != "open":
            raise ManualError("Position introuvable ou deja fermee.")
        self._close(item, reason, self.price(item["ticker"], item["market"]) or item.get("last_price"))
        if item["status"] != "closed":
            raise ManualError(item.get("error") or "Fermeture non confirmee — position conservee.")
        return self.public(item)

    def _close(self, item, reason, price):
        if item["id"] in self._busy:
            return
        self._busy.add(item["id"])
        try:
            self._close_impl(item, reason, price)
        except Exception as e:
            item["status"], item["error"] = "open", f"{type(e).__name__}: {e}"
            self._log(f"#{item['id']} {item['ticker']} : erreur de fermeture ({item['error']}) — position conservee", "error")
        finally:
            self._busy.discard(item["id"])

    def _close_impl(self, item, reason, price):
        if True:
            if not price:
                raise ManualError("prix indisponible")
            item["status"] = "closing"
            exit_price, source = price, "simulation"
            if item["mode"] == "live":
                if item["market"] == "perp":
                    pos = {"type": item["direction"], "entry": item["entry_price"],
                           "size": item["notional_usd"] / max(item["leverage"], 1), "leverage": item["leverage"]}
                    if not be.close_order(self.bot.exchange, item["ticker"], pos, self.cfg):
                        item["status"], item["error"] = "open", "fermeture reelle non confirmee par Hyperliquid"
                        self._log(f"#{item['id']} {item['ticker']} : fermeture NON confirmee — position conservee, nouvel essai au prochain signal", "error")
                        return
                    fill = be.pop_last_close_fill(item["ticker"])
                    exit_price, source = (fill, "hyperliquid") if fill else (price, "bot")
                    if not fill:
                        # v4.411 : position deja fermee sur Hyperliquid (stop natif declenche) -> le vrai prix de sortie
                        # est dans les remplissages du compte, pas besoin de l estimer
                        real = self._hl_exit_fill(item, time.time())
                        if real:
                            exit_price, source = real["px"], "hyperliquid"
                            item["fees_real"] = real["fee"]
                    self._cancel_native_orders(item["ticker"])
                else:
                    sp = self.spot_pair(item["ticker"])
                    sz = be.format_size_hl(self._spot_available(sp["base"]), sp["sz_decimals"])
                    result = self.bot.exchange.market_open(sp["pair"], False, sz) if sz > 0 else None
                    status = be._order_first_status(result) if result else None
                    if not (status and "filled" in status):
                        item["status"], item["error"] = "open", f"vente spot non executee : {str(result)[:150]}"
                        self._log(f"#{item['id']} {item['ticker']} spot : vente NON executee — position conservee", "error")
                        return
                    exit_price, source = float(status["filled"]["avgPx"]), "hyperliquid"
            sign = 1 if item["direction"] == "long" else -1
            move_pct = sign * (exit_price - item["entry_price"]) / item["entry_price"] * 100
            pnl = item["qty"] * (exit_price - item["entry_price"]) * sign + float(item.get("realized_pnl") or 0)    # v4.404 : + partie deja encaissee au TP1
            fees = item["notional_usd"] * FEE_ROUND_TRIP + float(item.get("realized_fees") or 0)
            item.update({"status": "closed", "closed_at": time.time(), "exit_price": exit_price,
                         "pnl": pnl, "fees_est": fees, "reason": reason, "exit_source": source, "error": None})
            self._save(item)
            self.items.pop(item["id"], None)
            try:
                tid = db.get_open_trade_id_by_uid(item["trade_uid"])
                if tid:
                    db.close_trade(tid, exit_price, pnl, reason, peak_pnl_pct=item.get("peak_pct"),
                                   fees_paid=fees if item["mode"] == "live" else None, exit_price_source=source)
            except Exception as e:
                print(f"[MANUEL] Cloture historique #{item['id']} impossible : {e}")
            self._log(f"#{item['id']} FERME ({reason}) : {item['ticker']} @ {exit_price:.6g} | {move_pct:+.2f}% | PnL {pnl:+.2f} $", "win" if pnl > 0 else "loss")

    def _hl_exit_fill(self, item, ref_ts, tries=3):
        """v4.411 — prix moyen REEL de la derniere sortie de ce trade, lu dans les remplissages Hyperliquid
        (dernier groupe de remplissages de cloture sur l actif, a 60 s pres). None si introuvable."""
        wallet = self._wallet()
        if not wallet or item.get("market") != "perp":
            return None
        opened = float(item.get("opened_at") or item.get("created_at") or ref_ts - 86400)
        for k in range(tries):
            try:
                fills = self.bot.info.user_fills_by_time(wallet, int(opened * 1000) - 5_000, int(ref_ts * 1000) + 120_000) or []
            except Exception:
                fills = []
            cl = [f for f in fills if f.get("coin") == item["ticker"] and str(f.get("dir", "")).startswith("Close")]
            if cl:
                last_t = max(int(f.get("time") or 0) for f in cl)
                grp = [f for f in cl if int(f.get("time") or 0) >= last_t - 60_000]
                sz = sum(abs(float(f.get("sz") or 0)) for f in grp)
                if sz > 0:
                    px = sum(float(f["px"]) * abs(float(f.get("sz") or 0)) for f in grp) / sz
                    return {"px": px, "fee": sum(float(f.get("fee") or 0) for f in grp),
                            "closed_pnl": sum(float(f.get("closedPnl") or 0) for f in grp), "ts": last_t / 1000.0}
            if k < tries - 1:
                time.sleep(1.0)
        return None

    def sync_leverage(self):
        """v4.413 — le levier REEL de chaque position manuelle live (lu sur Hyperliquid) fait foi : s il differe de celui
        suivi par le bot (levier refuse / plafonne a l ouverture, ou modifie a la main sur Hyperliquid), on aligne le
        suivi. Le notionnel ne change pas ; marge, % de marge et PnL en pourcentage sont recalcules. Toutes les 5 min."""
        items = [i for i in list(self.items.values()) if i.get("status") == "open" and i.get("mode") == "live" and i.get("market") == "perp"]
        if not items:
            return 0
        try:
            exch = be.fetch_exchange_positions(self.bot.info, self._wallet(), [i["ticker"] for i in items])
        except Exception:
            return 0
        if not exch:
            return 0
        n = 0
        for it in items:
            ep = exch.get(it["ticker"])
            if not ep or ep["szi"] == 0 or (ep["szi"] > 0) != (it["direction"] == "long"):
                continue
            try:
                real = int(float(ep.get("leverage") or 0))
            except (TypeError, ValueError):
                real = 0
            if real <= 0 or real == it.get("leverage"):
                continue
            old = it.get("leverage")
            it.setdefault("leverage_requested", old)
            it["leverage_note"] = f"levier suivi x{old} corrigé en x{real} d'après Hyperliquid"
            it["leverage"] = real
            n += 1
            self._log(f"#{it['id']} {it['ticker']} : levier reel Hyperliquid x{real} (suivi x{old}) — suivi aligne", "warn")
            self._save(it)
            try:
                if it.get("trade_uid"):
                    db.set_trade_leverage_by_uid(it["trade_uid"], real, it["notional_usd"] / max(real, 1))
            except Exception as e:
                print(f"[MANUEL] Alignement levier historique #{it['id']} impossible : {e}")
        return n

    def reconcile_exit_sources(self, max_age_days=14, limit=20):
        """v4.411 — trades manuels LIVE perp deja fermes avec une sortie « estimee par le bot » : si le vrai remplissage
        Hyperliquid existe (stop natif declenche avant que le bot ne ferme), on corrige prix de sortie, PnL et source.
        Appel espace (une fois par heure au plus)."""
        if time.time() - getattr(self, "_recon_exit_ts", 0) < 3600:
            return 0
        self._recon_exit_ts = time.time()
        fixed = 0
        try:
            rows = db.manual_history(200)
        except Exception:
            return 0
        done = 0
        for r in rows:
            it = r["data"]
            if (r["status"] != "closed" or it.get("mode") != "live" or it.get("market") != "perp"
                    or it.get("exit_source") != "bot" or not it.get("closed_at")
                    or time.time() - it["closed_at"] > max_age_days * 86400 or it.get("exit_fix_tried")):
                continue
            if done >= limit:
                break
            done += 1
            it["id"] = r["id"]
            real = self._hl_exit_fill(it, it["closed_at"], tries=1)
            it["exit_fix_tried"] = True
            if real:
                sign = 1 if it["direction"] == "long" else -1
                old = it.get("exit_price")
                it["exit_price"], it["exit_source"] = real["px"], "hyperliquid"
                it["fees_real"] = real["fee"]
                it["pnl"] = it["qty"] * (real["px"] - it["entry_price"]) * sign + float(it.get("realized_pnl") or 0)
                fixed += 1
                self._log(f"#{r['id']} {it['ticker']} : sortie corrigee d apres Hyperliquid ({old} -> {real['px']:.6g})", "dim")
                try:
                    if it.get("trade_uid"):
                        db.fix_trade_exit_by_uid(it["trade_uid"], real["px"], it["pnl"], "hyperliquid")
                except Exception as e:
                    print(f"[MANUEL] Correction historique #{r['id']} impossible : {e}")
            try:
                data = {k: v for k, v in it.items() if k not in ("id", "status")}
                db.manual_update(r["id"], "closed", data)
            except Exception as e:
                print(f"[MANUEL] Sauvegarde correction #{r['id']} impossible : {e}")
        return fixed

    def _partial_close(self, item, price):
        """v4.404 : TP1 atteint -> cloture partielle au marche, puis SL au seuil de rentabilite (option)."""
        if item["id"] in self._busy:
            return
        self._busy.add(item["id"])
        try:
            self._partial_close_impl(item, price)
        except Exception as e:
            item["error"] = f"TP1 : {type(e).__name__}: {e}"
            self._log(f"#{item['id']} {item['ticker']} : cloture partielle TP1 impossible ({e}) — position inchangee, nouvel essai dans 60 s", "error")
        finally:
            self._busy.discard(item["id"])

    def _partial_close_impl(self, item, price):
        sign = 1 if item["direction"] == "long" else -1
        entry = item["entry_price"]
        qty_part = item["qty"] * float(item.get("tp1_close_pct") or 50) / 100
        exit_price, source = price, "simulation"
        if item["mode"] == "live":
            if item["market"] != "perp":
                raise ManualError("TP1 non disponible en spot live")
            perp_map, _ = be._get_sz_decimals_map(self.bot.info)
            sz = be.format_size_hl(qty_part, perp_map.get(item["ticker"], 4))
            if not sz or sz <= 0:
                raise ManualError("taille de cloture partielle nulle")
            result = self.bot.exchange.market_close(item["ticker"], sz)
            status = be._order_first_status(result) if result else None
            if not (status and "filled" in status):
                raise ManualError(f"ordre non execute : {str(result)[:150]}")
            exit_price, source = float(status["filled"]["avgPx"]), "hyperliquid"
            qty_part = float(status["filled"].get("totalSz") or sz)
        pnl_part = qty_part * (exit_price - entry) * sign
        item["realized_pnl"] = float(item.get("realized_pnl") or 0) + pnl_part
        item["realized_fees"] = float(item.get("realized_fees") or 0) + qty_part * entry * FEE_ROUND_TRIP
        item["qty"] = max(item["qty"] - qty_part, 0.0)
        item["notional_usd"] = item["qty"] * entry
        item["tp1_done"], item["error"] = True, None
        be_txt = ""
        if item.get("tp1_be", True):
            be_sl = entry * (1 + sign * FEE_ROUND_TRIP)
            better = be_sl > item["sl_price"] if sign > 0 else be_sl < item["sl_price"]
            on_side = be_sl < price if sign > 0 else be_sl > price
            if better and on_side:
                item["sl_price"] = be_sl
                be_txt = f" | SL remonte au seuil de rentabilite {be_sl:.6g}"
        self._save(item)
        item["lock_pushed"] = None
        self._maybe_push_sl(item, item["sl_price"])        # live : la taille du SL natif change dans tous les cas
        self._log(f"#{item['id']} {item['ticker']} TP1 atteint @ {exit_price:.6g} ({source}) : {item['tp1_close_pct']:.0f} % cloture, PnL partiel {pnl_part:+.2f} $"
                  + be_txt + " — le reste continue vers le TP2", "win")

    def _cancel_native_orders(self, ticker):
        try:
            sl_oids, tp_oids = be._get_open_orders_by_type(self.bot.info, self._wallet(), ticker)
            oids = sl_oids + tp_oids
            if oids:
                self.bot.exchange.bulk_cancel([{"coin": ticker, "oid": o} for o in oids])
        except Exception as e:
            print(f"[MANUEL] Annulation ordres natifs {ticker} : {e}")

    # ─────────────────────────────── modifications ───────────────────────
    def modify(self, item_id, changes):
        with self.lock:
            item = self.items.get(item_id)
            if not item:
                raise ManualError("Element introuvable.")
            if item["status"] == "scheduled":
                # v4.400 : un ordre programme est entierement modifiable (memes regles qu a la creation)
                p = dict(item)
                new_dir = changes.get("direction") or item["direction"]
                new_mode = changes.get("mode") or item["mode"]
                if new_dir not in ("long", "short") or new_mode not in ("paper", "live"):
                    raise ManualError("Sens ou mode invalide.")
                if new_dir == "short" and item["market"] == "spot":
                    raise ManualError("Le spot n autorise que l achat (LONG).")
                if new_mode == "live" and item["mode"] != "live":
                    if not changes.get("confirm_live"):
                        raise ManualError("Confirmation LIVE requise.")
                    if self.bot.exchange is None or not self._wallet():
                        raise ManualError("Connexion Hyperliquid indisponible : le live est impossible pour le moment.")
                    if item["market"] == "perp":
                        self._check_live_perp_allowed(item["ticker"], exclude_id=item["id"])
                for k in ("sl_pct", "tp_pct", "ttp_arm_pct", "ttp_trail_pct", "trigger_price", "trigger_time", "notional_usd", "leverage",
                          "lock_arm_pct", "lock_fraction", "trend_entry", "trend_exit",
                          "tp_run", "tp_run_lock_pct", "tp_run_trail_pct", "tp1_pct", "tp1_close_pct", "tp1_be"):
                    if k in changes:
                        p[k] = changes[k]
                try:
                    p["sl_pct"] = float(p.get("sl_pct") or 0)
                    p["notional_usd"] = float(p.get("notional_usd") or 0)
                    p["leverage"] = 1 if item["market"] == "spot" else int(p.get("leverage") or 1)
                except (TypeError, ValueError):
                    raise ManualError("Valeur numerique invalide.")
                if not 0.05 <= p["sl_pct"] <= 50:
                    raise ManualError("Le stop loss doit etre compris entre 0,05 % et 50 % du prix d entree.")
                if not 1 <= p["leverage"] <= int(self.cfg.get("MANUAL_MAX_LEVERAGE", 20)):
                    raise ManualError(f"Levier hors limites (1 a {self.cfg.get('MANUAL_MAX_LEVERAGE', 20)}).")
                if p["notional_usd"] <= 0:
                    raise ManualError("Taille invalide.")
                if new_mode == "live" and p["notional_usd"] < 10:
                    raise ManualError("Hyperliquid exige au moins 10 $ de notionnel par ordre.")
                tp = p.get("tp_pct")
                tp = float(tp) if tp not in (None, "", 0) else None
                if tp is not None and not 0.05 <= tp <= 500:
                    raise ManualError("Take profit invalide (0,05 % a 500 %, ou vide).")
                p["tp_pct"] = tp
                p["mode"], p["market"] = new_mode, item["market"]
                self._validate_tp1(p, tp)      # v4.404
                arm, trail = float(p.get("ttp_arm_pct") or 0), float(p.get("ttp_trail_pct") or 0)
                if arm and not (0.05 <= arm <= 500 and 0.02 <= trail <= 100):
                    raise ManualError("Trailing TP invalide (armement 0,05-500 %, repli 0,02-100 %).")
                p["ttp_arm_pct"], p["ttp_trail_pct"] = (arm or None), (trail or None)
                p["tp_run"] = bool(p.get("tp_run"))
                if p["tp_run"]:
                    if tp is None:
                        raise ManualError("Laisser courir au TP : un take profit est necessaire.")
                    lk, tr_ = float(p.get("tp_run_lock_pct") or 0), float(p.get("tp_run_trail_pct") or 0)
                    if not (0 <= lk < tp) or not 0.05 <= tr_ <= 50:
                        raise ManualError("Laisser courir : le gain verrouille doit etre inferieur au TP (en %) et le suiveur compris entre 0,05 et 50 %.")
                    p["tp_run_lock_pct"], p["tp_run_trail_pct"] = lk, tr_
                new_ex = changes.get("execution") or item["execution"]
                if new_ex not in ("price_above", "price_below", "time"):
                    raise ManualError("Type de declenchement invalide.")
                if new_ex in ("price_above", "price_below"):
                    try:
                        _tp_ = float(p.get("trigger_price") or 0)
                    except (TypeError, ValueError):
                        _tp_ = 0
                    if not _tp_ > 0:
                        raise ManualError("Prix de declenchement invalide.")
                    p["trigger_price"], p["trigger_time"] = _tp_, None
                else:
                    if ("trigger_time" in changes or new_ex != item["execution"]) and not float(p.get("trigger_time") or 0) > time.time():
                        raise ManualError("L heure de declenchement doit etre dans le futur.")
                    p["trigger_time"], p["trigger_price"] = float(p["trigger_time"]), None
                probe = {k: p.get(k) for k in ("lock_arm_pct", "lock_fraction", "trend_entry", "trend_exit")}
                self._validate_lock_trend(probe)      # v4.343 : memes regles qu a la creation
                for k in ("sl_pct", "tp_pct", "ttp_arm_pct", "ttp_trail_pct", "notional_usd", "leverage", "tp_run",
                          "tp_run_lock_pct", "tp_run_trail_pct", "trigger_price", "trigger_time", "tp1_pct", "tp1_close_pct", "tp1_be"):
                    item[k] = p.get(k)
                item["execution"] = new_ex
                if "require_valid" in changes and changes["require_valid"] is not None:
                    item["require_valid"] = bool(changes["require_valid"])
                item.update(probe)
                if new_dir != item["direction"]:
                    # le sens ne correspond plus a l opportunite du bot : l ordre devient un ordre libre
                    item["direction"] = new_dir
                    if item.get("strategy_source") not in FREE_LIKE:
                        item["strategy_source"] = "libre"
                    item["require_valid"] = False
                item["mode"] = new_mode
                if changes.get("expiry_hours"):
                    h = float(changes["expiry_hours"])
                    if not 0.1 <= h <= 24 * 30:
                        raise ManualError("Expiration : entre 0,1 h et 30 jours.")
                    item["expires_at"] = time.time() + h * 3600
                self._save(item)
                self._log(f"Ordre programme #{item['id']} {item['ticker']} modifie — {item['direction'].upper()} {item['mode'].upper()} — {self._trigger_text(item)} | SL {item['sl_pct']}%"
                          + (f" | TP {item['tp_pct']}%" if item.get("tp_pct") else " | sans TP"), "info")
                return self.public(item)
            if item["status"] != "open":
                raise ManualError("Position non modifiable dans son etat actuel.")
            is_long = item["direction"] == "long"
            price = self.price(item["ticker"], item["market"]) or item["last_price"]
            if "sl_price" in changes and changes["sl_price"]:
                new_sl = float(changes["sl_price"])
                if (is_long and new_sl >= price) or (not is_long and new_sl <= price):
                    raise ManualError("Le SL doit rester du bon cote du prix actuel.")
                item["sl_price"] = new_sl
                if item["mode"] == "live" and item["market"] == "perp":
                    be.update_sl_on_hyperliquid(self.bot.exchange, self.bot.info, self._wallet(), item["ticker"],
                                                {"type": item["direction"], "entry": item["entry_price"], "size": item["notional_usd"]},
                                                new_sl, self.cfg)
            if "tp_price" in changes:
                tp = changes["tp_price"]
                if tp:
                    tp = float(tp)
                    if (is_long and tp <= price) or (not is_long and tp >= price):
                        raise ManualError("Le TP doit rester du bon cote du prix actuel.")
                item["tp_price"] = tp or None
            # v4.404 — TP1 (cloture partielle) d une position ouverte
            if any(k in changes for k in ("tp1_price", "tp1_close_pct", "tp1_be")):
                if item.get("tp1_done"):
                    raise ManualError("TP1 deja execute : plus modifiable.")
                if "tp1_close_pct" in changes and changes["tp1_close_pct"] is not None:
                    cp = float(changes["tp1_close_pct"])
                    if not 5 <= cp <= 95:
                        raise ManualError("TP1 : la part a cloturer doit etre comprise entre 5 % et 95 %.")
                    item["tp1_close_pct"] = cp
                if "tp1_be" in changes and changes["tp1_be"] is not None:
                    item["tp1_be"] = bool(changes["tp1_be"])
                if "tp1_price" in changes:
                    t1 = changes["tp1_price"]
                    if t1:
                        t1 = float(t1)
                        if item.get("market") == "spot" and item["mode"] == "live":
                            raise ManualError("TP1 (cloture partielle) n est pas disponible en spot LIVE.")
                        if not item.get("tp_price"):
                            raise ManualError("TP1 : un TP2 (take profit final) est necessaire.")
                        if (is_long and t1 <= price) or (not is_long and t1 >= price):
                            raise ManualError("Le TP1 doit rester du bon cote du prix actuel.")
                        item["tp1_price"] = t1
                        item.setdefault("tp1_close_pct", 50.0)
                    else:
                        item["tp1_price"] = None
            if item.get("tp1_price") and not item.get("tp1_done"):
                _t2 = item.get("tp_price")
                if not _t2 or (is_long and item["tp1_price"] >= _t2) or (not is_long and item["tp1_price"] <= _t2):
                    raise ManualError("Le TP1 doit etre situe entre le prix et le TP2.")
            for k in ("ttp_arm_pct", "ttp_trail_pct"):
                if k in changes:
                    item[k] = float(changes[k]) if changes[k] else None
            if any(k in changes for k in ("tp_run", "tp_run_lock_pct", "tp_run_trail_pct")):      # v4.395
                if "tp_run_lock_pct" in changes and changes["tp_run_lock_pct"] is not None:
                    item["tp_run_lock_pct"] = float(changes["tp_run_lock_pct"])
                if "tp_run_trail_pct" in changes and changes["tp_run_trail_pct"] is not None:
                    item["tp_run_trail_pct"] = float(changes["tp_run_trail_pct"])
                if "tp_run" in changes and changes["tp_run"] is not None:
                    if changes["tp_run"] and not item.get("tp_price"):
                        raise ManualError("Laisser courir au TP : ce trade n'a pas de take profit. Saisis d'abord un prix de TP, puis coche l'option.")
                    item["tp_run"] = bool(changes["tp_run"])
                if item.get("tp_run") and item.get("tp_price"):
                    _d = abs(item["tp_price"] - item["entry_price"]) / item["entry_price"] * 100
                    if not (0 <= float(item.get("tp_run_lock_pct") or 0) < _d) or not 0.05 <= float(item.get("tp_run_trail_pct") or 0) <= 50:
                        raise ManualError("Laisser courir : le gain verrouille doit etre inferieur a la distance du TP et le suiveur compris entre 0,05 et 50 %.")
            if "lock_arm_pct" in changes or "lock_fraction" in changes:   # v4.343
                probe = {"lock_arm_pct": changes.get("lock_arm_pct", item.get("lock_arm_pct")),
                         "lock_fraction": changes.get("lock_fraction", item.get("lock_fraction"))}
                self._validate_lock_trend(probe)
                item["lock_arm_pct"], item["lock_fraction"] = probe["lock_arm_pct"], probe["lock_fraction"]
            if "trend_exit" in changes:
                if changes["trend_exit"] not in (None, "", "opposee", "perdue"):
                    raise ManualError("Regle de sortie sur la tendance invalide.")
                item["trend_exit"] = changes["trend_exit"] or None
            self._save(item)
            self._log(f"#{item['id']} {item['ticker']} modifie : SL {item['sl_price']:.6g}"
                      + (f" | TP {item['tp_price']:.6g}" if item.get("tp_price") else " | sans TP")
                      + (f" | TTP +{item['ttp_arm_pct']}%/-{item['ttp_trail_pct']}%" if item.get("ttp_arm_pct") else " | sans TTP"), "info")
            return self.public(item)

    def cancel(self, item_id):
        with self.lock:
            item = self.items.get(item_id)
            if not item or item["status"] != "scheduled":
                raise ManualError("Ordre programme introuvable (deja execute, annule ou expire ?).")
            self._finish_order(item, "cancelled", "annule par l utilisateur")
            return {"ok": True}

    def _finish_order(self, item, status, why):
        item["status"], item["reason"] = status, why
        item["closed_at"] = time.time()
        self._save(item)
        self.items.pop(item["id"], None)
        self._log(f"Ordre programme #{item['id']} {item['ticker']} : {why}", "warn" if status != "cancelled" else "dim")

    # ─────────────────────────────── temps reel ──────────────────────────
    def on_tick(self):
        """Appele a chaque tick de prix WebSocket (et par le cycle en secours)."""
        if not self.items:
            return
        for item in list(self.items.values()):
            try:
                if item["status"] == "open":
                    self._manage(item)
                elif item["status"] == "scheduled":
                    self._check_trigger(item)
            except Exception as e:
                print(f"[MANUEL] Erreur suivi #{item.get('id')} : {type(e).__name__}: {e}")

    def on_cycle(self):
        if not self.bot._is_ws_healthy():
            self.on_tick()
        if time.time() - getattr(self, "_recon_exit_ts", 0) >= 3600:
            threading.Thread(target=lambda: self.reconcile_exit_sources(), daemon=True).start()
        if time.time() - getattr(self, "_lev_sync_ts", 0) >= 300:
            self._lev_sync_ts = time.time()
            threading.Thread(target=self.sync_leverage, daemon=True).start()
        now = time.time()
        for item in list(self.items.values()):
            if item["status"] == "scheduled" and now > item.get("expires_at", now + 1):
                self._finish_order(item, "expired", "expire sans declenchement")
            elif item["status"] == "open" and now - self._last_persist.get(item["id"], 0) > 30:
                self._save(item)  # persiste regulierement le pic du trailing

    def _check_trigger(self, item):
        if item["id"] in self._fire_busy:
            return
        price = self.price(item["ticker"], item["market"])
        if price is None:
            return
        ex = item["execution"]
        fire = ((ex == "price_above" and price >= item["trigger_price"])
                or (ex == "price_below" and price <= item["trigger_price"])
                or (ex == "time" and time.time() >= item["trigger_time"]))
        if not fire:
            return
        if (item.get("require_valid", True) and item.get("strategy_source") not in FREE_LIKE
                and not self.get_opportunity(item["strategy_source"], item["ticker"], item["direction"])):
            self._finish_order(item, "cancelled", "declenche mais l opportunite n est plus valable — ordre annule")
            return
        self._log(f"Ordre programme #{item['id']} declenche ({self._trigger_text(item)}) — ouverture", "signal")
        self._fire_busy.add(item["id"])
        threading.Thread(target=self._fire, args=(item,), daemon=True).start()

    def _fire(self, item):
        """v4.343 — declenchement : controle de tendance H4 (si demande) puis ouverture. Execute dans un
        thread : la lecture des bougies peut prendre une seconde, ce qui ne doit pas bloquer les prix."""
        try:
            if item.get("trend_entry"):
                try:
                    self._check_trend_entry(item["ticker"], item["trend_entry"])
                except ManualError as e:
                    self._finish_order(item, "cancelled", f"declenche mais condition de tendance non remplie : {e}")
                    return
            if item.get("trend_exit"):
                try:
                    self._check_trend_exit_not_immediate(item["ticker"], item["direction"], item["trend_exit"])
                except ManualError as e:
                    self._finish_order(item, "cancelled", f"declenche mais {e}")
                    return
            self._open(item)
        finally:
            self._fire_busy.discard(item["id"])

    def _manage(self, item):
        if item["id"] in self._busy:
            return
        price = self.price(item["ticker"], item["market"])
        if price is None:
            return
        item["last_price"] = price
        sign = 1 if item["direction"] == "long" else -1
        move = sign * (price - item["entry_price"]) / item["entry_price"] * 100
        if move > item.get("peak_pct", 0):
            item["peak_pct"] = move
        reason = None
        # v4.343 — VERROU PROGRESSIF : des que le gain maximal atteint le seuil, le SL monte pour garantir
        # une part (lock_fraction) de ce gain maximal. Il ne fait que MONTER (long) / DESCENDRE (short).
        la, lf = item.get("lock_arm_pct"), item.get("lock_fraction")
        if la and lf and item["peak_pct"] >= la:
            lock_pct = lf * item["peak_pct"]
            new_sl = item["entry_price"] * (1 + sign * lock_pct / 100)
            if (sign > 0 and new_sl > item["sl_price"]) or (sign < 0 and new_sl < item["sl_price"]):
                item["sl_price"] = new_sl
                if not item.get("lock_active"):
                    item["lock_active"] = True
                    self._log(f"#{item['id']} {item['ticker']} : verrou du SL actif a +{item['peak_pct']:.2f}% — SL {new_sl:.6g} (garantit {lock_pct:.2f}%)", "signal")
                    self._save(item)
                self._maybe_push_sl(item, new_sl)
        # v4.395 — LAISSER COURIR AU TP : a l objectif on ne ferme pas ; le SL monte (gain verrouille puis suit le plus haut)
        if item.get("tp_run") and item.get("tp_price"):
            _tp_hit = (sign > 0 and price >= item["tp_price"]) or (sign < 0 and price <= item["tp_price"])
            _trail = float(item.get("tp_run_trail_pct") or 0.5)
            if _tp_hit and not item.get("tp_run_armed"):
                item["tp_run_armed"], item["tp_run_peak_px"] = True, price
                _lk = item["entry_price"] * (1 + sign * float(item.get("tp_run_lock_pct") or 0) / 100)
                _tr = price * (1 - sign * _trail / 100)
                _new = max(item["sl_price"], _lk, _tr) if sign > 0 else min(item["sl_price"], _lk, _tr)
                item["sl_price"] = _new
                self._log(f"#{item['id']} {item['ticker']} : objectif atteint — on laisse courir, SL remonte a {_new:.6g}", "signal")
                self._save(item)
                self._maybe_push_sl(item, _new)
            elif item.get("tp_run_armed"):
                _pk = item.get("tp_run_peak_px") or price
                if (sign > 0 and price > _pk) or (sign < 0 and price < _pk):
                    item["tp_run_peak_px"] = price
                    _new = price * (1 - sign * _trail / 100)
                    if (sign > 0 and _new > item["sl_price"]) or (sign < 0 and _new < item["sl_price"]):
                        item["sl_price"] = _new
                        self._maybe_push_sl(item, _new)
                        if time.time() - item.get("tp_run_saved_ts", 0) > 60:
                            item["tp_run_saved_ts"] = time.time()
                            self._save(item)
        # v4.404 — TP1 : cloture partielle (si le SL n est pas touche dans le meme tick)
        if (item.get("tp1_price") and not item.get("tp1_done") and time.time() - item.get("tp1_try_ts", 0) > 60
                and ((sign > 0 and price >= item["tp1_price"]) or (sign < 0 and price <= item["tp1_price"]))
                and not ((sign > 0 and price <= item["sl_price"]) or (sign < 0 and price >= item["sl_price"]))):
            item["tp1_try_ts"] = time.time()
            threading.Thread(target=self._partial_close, args=(item, price), daemon=True).start()
            return
        if (sign > 0 and price <= item["sl_price"]) or (sign < 0 and price >= item["sl_price"]):
            reason = ("TRAILING TAKE PROFIT (manuel)" if item.get("tp_run_armed")
                      else "STOP DYNAMIQUE (manuel)" if item.get("lock_active")
                      else "STOP APRES TP1 (manuel)" if item.get("tp1_done") else "STOP LOSS (manuel)")
        elif item.get("tp_price") and not item.get("tp_run") and ((sign > 0 and price >= item["tp_price"]) or (sign < 0 and price <= item["tp_price"])):
            reason = "TAKE PROFIT (manuel)"
        elif item.get("ttp_arm_pct"):
            if not item.get("armed") and item["peak_pct"] >= item["ttp_arm_pct"]:
                item["armed"] = True
                self._log(f"#{item['id']} {item['ticker']} : trailing arme a +{item['peak_pct']:.2f}%", "signal")
                self._save(item)
            if item.get("armed") and move <= item["peak_pct"] - (item.get("ttp_trail_pct") or 0.5):
                reason = "TRAILING TAKE PROFIT (manuel)"
        if not reason and item.get("trend_exit"):
            reason = self._trend_exit_reason(item, price, sign)
        if reason:
            threading.Thread(target=self._close, args=(item, reason, price), daemon=True).start()

    def _trend_exit_reason(self, item, price, sign):
        """v4.343 — regle de sortie choisie sur la tendance H4 : "opposee" = la tendance devient l inverse
        de celle du trade ; "perdue" = elle n est plus celle du sens du trade. Lecture du cache (jamais
        de requete reseau ici, le bot rafraichit les bougies H4 des actifs tenus), verifiee au plus toutes
        les 5 s, et uniquement sur des donnees a jour."""
        now = time.time()
        if now - self._trend_last.get(item["id"], 0) < 5:
            return None
        self._trend_last[item["id"]] = now
        try:
            mv = self.bot.mtf_view(item["ticker"], price, cache_only=True)
        except Exception:
            return None
        if not mv or not mv.get("ok") or not mv.get("data_fresh", True):
            return None
        want = "haussiere" if sign > 0 else "baissiere"
        opposite = "baissiere" if sign > 0 else "haussiere"
        if item["trend_exit"] == "opposee" and mv["trend"] == opposite:
            return "TENDANCE RETOURNEE (manuel)"
        if item["trend_exit"] == "perdue" and mv["trend"] != want:
            return "TENDANCE PERDUE (manuel)"
        return None

    def _maybe_push_sl(self, item, new_sl):
        """Live perp : renvoie le SL a Hyperliquid (ordre natif) quand il a bouge d au moins
        MANUAL_LOCK_PUSH_MIN_STEP_PCT depuis le dernier envoi — dans un thread, sans bloquer les prix."""
        if item["mode"] != "live" or item["market"] != "perp" or self.bot.exchange is None:
            return
        pushed = item.get("lock_pushed")
        step = self.cfg.get("MANUAL_LOCK_PUSH_MIN_STEP_PCT", 0.05)
        if pushed is not None and abs(new_sl - pushed) / item["entry_price"] * 100 < step:
            return
        iid = item["id"]
        if iid in self._push_busy:
            return
        self._push_busy.add(iid)

        def run():
            try:
                be.update_sl_on_hyperliquid(self.bot.exchange, self.bot.info, self._wallet(), item["ticker"],
                                            {"type": item["direction"], "entry": item["entry_price"], "size": item["notional_usd"]},
                                            new_sl, self.cfg)
                item["lock_pushed"] = new_sl
            except Exception as e:
                print(f"[MANUEL] Mise a jour du SL natif #{iid} impossible : {e}")
            finally:
                self._push_busy.discard(iid)
        threading.Thread(target=run, daemon=True).start()

    # ─────────────────────────── reprise au demarrage ────────────────────
    def reconcile(self, exch_positions):
        """Positions manuelles LIVE perp absentes d Hyperliquid au demarrage :
        fermees pendant la coupure (SL natif le plus souvent)."""
        for item in list(self.items.values()):
            if item["status"] != "open" or item["mode"] != "live" or item["market"] != "perp":
                continue
            ep = exch_positions.get(item["ticker"])
            if ep and (ep["szi"] > 0) == (item["direction"] == "long"):
                continue
            exit_px = item.get("sl_price") or item["entry_price"]
            try:
                for f in self.bot.info.user_fills(self._wallet()) or []:
                    if f.get("coin") == item["ticker"] and str(f.get("dir", "")).startswith("Close"):
                        exit_px = float(f["px"])
                        break
            except Exception:
                pass
            item["mode_close_note"] = "fermee pendant la coupure"
            item_mode = item["mode"]
            item["mode"] = "paper"  # pas d ordre reel a envoyer : deja fermee
            self._close(item, "FERMEE PENDANT COUPURE (manuel)", exit_px)
            item["mode"] = item_mode

    def adopt_orphan(self, coin, ep, uid, opened_ts=None):
        """Position LIVE identifiee comme manuelle (cloid) mais inconnue de la
        base locale (base perdue) : reprise avec un SL de secours."""
        direction = "long" if ep["szi"] > 0 else "short"
        entry = ep["entry"]
        sl_pct = self.cfg.get("RECOVERY_RESCUE_SL_PCT", 15.0)
        item = {"strategy_source": "manual", "ticker": coin, "direction": direction, "market": "perp",
                "mode": "live", "leverage": ep.get("leverage") or 1, "notional_usd": abs(ep["szi"]) * entry,
                "sl_pct": sl_pct, "tp_pct": None, "ttp_arm_pct": None, "ttp_trail_pct": None,
                "execution": "now", "created_at": opened_ts or time.time(), "opened_at": opened_ts or time.time(),
                "trade_uid": uid, "entry_price": entry, "qty": abs(ep["szi"]),
                "sl_price": entry * (1 - sl_pct / 100) if direction == "long" else entry * (1 + sl_pct / 100),
                "tp_price": None, "peak_pct": 0.0, "armed": False, "reasons": ["reprise apres perte de la base locale"]}
        item["id"] = db.manual_insert("open", item)
        item["status"] = "open"
        self.items[item["id"]] = item
        self._log(f"#{item['id']} Position manuelle LIVE {direction.upper()} {coin} retrouvee sur Hyperliquid — suivie avec un SL de secours a {sl_pct}%. Verifiez-la.", "error")

    # ─────────────────────────────── affichage ───────────────────────────
    def public(self, item):
        out = {k: v for k, v in item.items()}
        out["strategy_label"] = STRATEGY_LABELS.get(item.get("strategy_source"), item.get("strategy_source"))
        if item.get("status") == "open":
            price = self.price(item["ticker"], item["market"]) or item.get("last_price")
            sign = 1 if item["direction"] == "long" else -1
            if price:
                out["current_price"] = price
                out["move_pct"] = sign * (price - item["entry_price"]) / item["entry_price"] * 100
                out["pnl"] = item["qty"] * (price - item["entry_price"]) * sign + float(item.get("realized_pnl") or 0)    # v4.404 : inclut le TP1 deja encaisse
        elif item.get("status") == "scheduled":
            out["current_price"] = self.price(item["ticker"], item["market"])
            out["opportunity_valid"] = (True if item.get("strategy_source") in FREE_LIKE
                                        else self.get_opportunity(item["strategy_source"], item["ticker"], item["direction"]) is not None)
        return out

    def snapshot(self):
        with self.lock:
            items = [self.public(i) for i in self.items.values()]
        history = []
        for row in db.manual_history(50):
            d = row["data"]
            d.update({"id": row["id"], "status": row["status"]})
            d["strategy_label"] = STRATEGY_LABELS.get(d.get("strategy_source"), d.get("strategy_source"))
            history.append(d)
        w = self._wallet() or ""
        return {
            "wallet_short": (w[:6] + "…" + w[-4:]) if len(w) > 12 else (w or None),   # v4.344 : a comparer a l adresse affichee sur Hyperliquid
            "opportunities": self.list_opportunities(),
            "scheduled": [i for i in items if i["status"] == "scheduled"],
            "positions": [i for i in items if i["status"] in ("open", "opening", "closing")],
            "history": history,
            "live_available": self.bot.exchange is not None and bool(self._wallet()),
            "opportunity_ttl_sec": self._ttl(),
        }
