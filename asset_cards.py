"""v4.349 — FICHE ACTIF (LECTURE SEULE).

Sur demande : connaitre la "personnalite" de chaque actif a partir de son historique et de sa force
relative, sans attendre. Tout se calcule sur des bougies publiques (15 minutes, 30 jours ; H4 pour la
tendance). Les resultats du bot sur l actif sont ajoutes cote API (base de donnees).

Chaque mesure est accompagnee de la taille de son echantillon : un nombre sans sa taille ne se lit pas.
"""
import math
import statistics
import threading
import time

import replay_research as rr

BAR_MS = 900_000


def _pct(a, b):
    return (a / b - 1) * 100 if b else 0.0


def hourly_closes(m15):
    """Cloture de chaque heure complete (4 bougies de 15 minutes) : {debut d heure (ms): cloture}."""
    out, cnt = {}, {}
    for c in m15:
        h = c["t"] // 3_600_000 * 3_600_000
        cnt[h] = cnt.get(h, 0) + 1
        out[h] = c["c"]
    return {h: v for h, v in out.items() if cnt[h] == 4}


def _corr_beta(x, y):
    n = len(x)
    if n < 30:
        return None, None
    mx, my = sum(x) / n, sum(y) / n
    sxx = sum((a - mx) ** 2 for a in x)
    syy = sum((b - my) ** 2 for b in y)
    sxy = sum((a - mx) * (b - my) for a, b in zip(x, y))
    if sxx <= 0 or syy <= 0:
        return None, None
    return sxy / math.sqrt(sxx * syy), sxy / sxx


def _autocorr1(r):
    n = len(r)
    if n < 30:
        return None
    m = sum(r) / n
    den = sum((v - m) ** 2 for v in r)
    if den <= 0:
        return None
    return sum((r[i] - m) * (r[i + 1] - m) for i in range(n - 1)) / den


def _events(closes, thr, up, cooldown=8):
    """Rendement des 2 heures SUIVANT chaque mouvement >= thr % (hausse) ou <= -thr % (baisse) sur 2 heures.
    Evenements espaces d au moins `cooldown` bougies (ils ne sont pas independants sinon)."""
    out, last = [], -999
    for i in range(8, len(closes) - 8):
        mv = _pct(closes[i], closes[i - 8])
        if (up and mv >= thr) or ((not up) and mv <= -thr):
            if i - last >= cooldown:
                out.append(_pct(closes[i + 8], closes[i]))
                last = i
    return out


def compute_card(coin, m15, h4, btc_h1=None):
    """Fiche d un actif. btc_h1 : {debut d heure: cloture} de BTC (pour la force relative et le beta)."""
    if len(m15) < 700 or len(h4) < 230:
        return {"coin": coin, "error": f"historique insuffisant (M15 {len(m15)}, H4 {len(h4)})"}
    closes = [c["c"] for c in m15]
    last = closes[-1]
    n14 = min(len(m15), 14 * 96)
    rng = [(c["h"] - c["l"]) / c["c"] * 100 for c in m15[-n14:]]
    # amplitude quotidienne (jours UTC) et volume en dollars
    days = {}
    for c in m15:
        d = c["t"] // 86_400_000
        e = days.setdefault(d, {"o": c["o"], "h": c["h"], "l": c["l"], "usd": 0.0, "n": 0})
        e["h"] = max(e["h"], c["h"])
        e["l"] = min(e["l"], c["l"])
        e["usd"] += c["v"] * c["c"]
        e["n"] += 1
    full = [(d, e) for d, e in sorted(days.items()) if e["n"] == 96]
    daily_range = [(e["h"] - e["l"]) / e["o"] * 100 for _d, e in full[-30:]]
    vol_usd = [e["usd"] for _d, e in full[-14:]]
    # force relative et beta (rendements horaires alignes)
    h1 = hourly_closes(m15)
    hs = sorted(h1)
    ret_h = {hs[i]: _pct(h1[hs[i]], h1[hs[i - 1]]) for i in range(1, len(hs)) if hs[i] - hs[i - 1] == 3_600_000}
    rel7 = rel30 = corr = beta = None
    if btc_h1:
        bh = sorted(btc_h1)
        b_ret = {bh[i]: _pct(btc_h1[bh[i]], btc_h1[bh[i - 1]]) for i in range(1, len(bh)) if bh[i] - bh[i - 1] == 3_600_000}
        common = sorted(set(ret_h) & set(b_ret))[-30 * 24:]
        corr, beta = _corr_beta([b_ret[t] for t in common], [ret_h[t] for t in common])
        if btc_h1 and bh:
            def ago(series, keys, hours):
                k = [t for t in keys if t <= keys[-1] - hours * 3_600_000]
                return series[k[-1]] if k else None
            a7, a30 = ago(h1, hs, 168), ago(h1, hs, 720)
            b7, b30 = ago(btc_h1, bh, 168), ago(btc_h1, bh, 720)
            if a7 and b7:
                rel7 = _pct(h1[hs[-1]], a7) - _pct(btc_h1[bh[-1]], b7)
            if a30 and b30:
                rel30 = _pct(h1[hs[-1]], a30) - _pct(btc_h1[bh[-1]], b30)
    ac = _autocorr1([ret_h[t] for t in sorted(ret_h)])
    r8 = [_pct(closes[i], closes[i - 8]) for i in range(8, len(closes))]
    abs8 = sorted(abs(v) for v in r8)
    rally = _events(closes, 1.0, True)
    drop = _events(closes, 1.0, False)
    ctx = rr.build_h4_context(h4[-260:])
    c_last = next((c for c in reversed(ctx) if c), None)
    trend = rr.TREND_LABELS[rr.classify_trend(c_last, last)] if c_last else None
    ret7 = _pct(closes[-1], closes[-1 - 672]) if len(closes) > 673 else None
    ret30 = _pct(closes[-1], closes[0]) if len(m15) >= 2880 else None

    def ev(v, up):
        if len(v) < 5:
            return {"n": len(v), "median": None, "mean": None, "against": None}
        # "contre" = part des cas ou le prix est revenu en sens inverse (baisse apres hausse / hausse apres baisse)
        against = sum(1 for x in v if (x < 0 if up else x > 0)) / len(v) * 100
        return {"n": len(v), "median": round(statistics.median(v), 3), "mean": round(sum(v) / len(v), 3),
                "against": round(against, 0)}

    return {
        "coin": coin, "bars": len(m15), "last": last,
        "range15_med": round(statistics.median(rng), 3),
        "daily_range_med": round(statistics.median(daily_range), 2) if daily_range else None,
        "vol_usd_24h": round(sum(vol_usd) / len(vol_usd)) if vol_usd else None,
        "ret_7d": round(ret7, 2) if ret7 is not None else None, "ret_30d": round(ret30, 2) if ret30 is not None else None,
        "rel_7d": round(rel7, 2) if rel7 is not None else None, "rel_30d": round(rel30, 2) if rel30 is not None else None,
        "corr_btc": round(corr, 2) if corr is not None else None, "beta_btc": round(beta, 2) if beta is not None else None,
        "ac1_1h": round(ac, 3) if ac is not None else None, "ac1_se": round(1 / math.sqrt(max(len(ret_h), 1)), 3),
        "after_rally": ev(rally, True), "after_drop": ev(drop, False),
        "worst_2h": round(min(r8), 2), "p99_2h": round(abs8[int(0.99 * (len(abs8) - 1))], 2),
        "trend_h4": trend,
    }


def run_cards(info, assets, days=30, progress=None, sleeps=(9.0, 3.0), cancel=None, now_ms=None, retry_wait=20.0):
    """Telecharge les bougies (debit bride) et calcule les fiches. BTC d abord (reference pour le beta)."""
    now_ms = int(now_ms or time.time() * 1000)
    days = max(14, min(int(days), 40))
    order = sorted(assets, key=lambda a: (a != "BTC", a))
    cards, skipped, btc_h1 = [], [], None
    for idx, coin in enumerate(order):
        if cancel is not None and cancel():
            skipped.append((coin, "annule"))
            break
        if progress:
            progress(idx, len(order), coin)
        try:
            m15 = rr.fetch_with_retry(info, coin, "15m", days + 2, now_ms, wait=retry_wait)
            time.sleep(sleeps[0])
            h4 = rr.fetch_with_retry(info, coin, "4h", days + 50, now_ms, wait=retry_wait)
            time.sleep(sleeps[1])
            if coin == "BTC":
                btc_h1 = hourly_closes(m15)
            card = compute_card(coin, m15, h4, btc_h1 if coin != "BTC" else None)
            if card.get("error"):
                skipped.append((coin, card["error"]))
            else:
                if coin == "BTC":
                    card.update({"corr_btc": 1.0, "beta_btc": 1.0, "rel_7d": 0.0, "rel_30d": 0.0})
                cards.append(card)
        except Exception as e:
            skipped.append((coin, f"{type(e).__name__}: {e}"))
    if progress:
        progress(len(order), len(order), "terminé")
    return {"meta": {"days": days, "end_ms": now_ms, "assets_ok": [c["coin"] for c in cards], "assets_skipped": skipped},
            "cards": cards}


class CardsJob:
    """Calcul des fiches dans un thread ; un seul a la fois."""

    def __init__(self):
        self.lock = threading.Lock()
        self.state, self.result, self.error = "idle", None, None
        self.progress = {"done": 0, "total": 0, "current": ""}
        self.started_at = self.finished_at = None
        self._cancel = False

    def start(self, info, assets, days, on_done=None, sleeps=(9.0, 3.0), retry_wait=20.0):
        with self.lock:
            if self.state == "running":
                return False
            self.state, self.result, self.error = "running", None, None
            self.started_at, self.finished_at, self._cancel = time.time(), None, False
            self.progress = {"done": 0, "total": len(assets), "current": ""}

        def prog(i, total, coin):
            self.progress = {"done": i, "total": total, "current": coin}

        def work():
            try:
                res = run_cards(info, assets, days, progress=prog, sleeps=sleeps, cancel=lambda: self._cancel, retry_wait=retry_wait)
                self.result, self.state = res, "done"
                if on_done:
                    on_done(res)
            except Exception as e:
                self.error, self.state = f"{type(e).__name__}: {e}", "error"
            finally:
                self.finished_at = time.time()

        threading.Thread(target=work, daemon=True).start()
        return True

    def cancel(self):
        self._cancel = True

    def status(self, with_result=True):
        out = {"state": self.state, "progress": self.progress, "error": self.error,
               "started_at": self.started_at, "finished_at": self.finished_at}
        if with_result and self.state == "done":
            out["result"] = self.result
        return out
