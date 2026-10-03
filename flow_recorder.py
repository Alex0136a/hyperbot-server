"""v4.349 — ENREGISTREUR DE FLUX (LECTURE SEULE).

Sur demande : apprendre, actif par actif, de la lecture des achats et des ventes qui construisent les
signaux. Le bot recevait deja chaque transaction en direct (sens agressif, taille, prix, heure) mais n en
gardait que 10 minutes en memoire, sans prix, et rien n etait conserve : aucune donnee pour apprendre, et
Hyperliquid ne fournit pas l historique des transactions. Ce module agrege le flux par tranches de 5
minutes et par actif ; la base de donnees conserve les tranches.

Par tranche et par actif :
  n_buy / n_sell        nombre de transactions ou l ACHETEUR / le VENDEUR a pris l initiative (side B / A)
  buy_usd / sell_usd    montants correspondants (taille x prix)
  big_buy_usd / big_sell_usd   part venant de transactions >= big_usd (gros ordres)
  max_trade_usd         plus grosse transaction
  px_open/high/low/close  prix des transactions

Une tranche absente peut signifier "aucune transaction" OU "enregistreur arrete" : une table de
battement de coeur (alive) dit pour quelles tranches l enregistreur etait actif.
Aucun effet sur les decisions du bot : l appel est protege par try/except cote moteur.
"""
import threading
import time

BUCKET_MS = 300_000


class FlowRecorder:
    def __init__(self, big_usd=5000.0, bucket_ms=BUCKET_MS):
        self.big_usd = float(big_usd)
        self.bucket_ms = int(bucket_ms)
        self.cur = {}                # coin -> tranche en cours
        self.closed = []             # tranches terminees, en attente d ecriture
        self.seen = {}               # coin -> (ensemble des tid, ordre) pour eviter les doublons apres une reconnexion
        self.alive = []              # debuts de tranche pour lesquels le flux etait actif
        self._alive_marked = -1      # derniere tranche globale marquee vivante
        self.lock = threading.Lock()
        self.stats = {"trades": 0, "dupes": 0, "bad": 0}

    def _new(self, coin, t0):
        return {"coin": coin, "t0": t0, "n_buy": 0, "n_sell": 0, "buy_usd": 0.0, "sell_usd": 0.0,
                "big_buy_usd": 0.0, "big_sell_usd": 0.0, "max_trade_usd": 0.0,
                "px_open": None, "px_high": None, "px_low": None, "px_close": None, "_t_last": 0}

    def _row(self, b):
        return (b["coin"], b["t0"], b["n_buy"], b["n_sell"], round(b["buy_usd"], 2), round(b["sell_usd"], 2),
                round(b["big_buy_usd"], 2), round(b["big_sell_usd"], 2), round(b["max_trade_usd"], 2),
                b["px_open"], b["px_high"], b["px_low"], b["px_close"])

    def on_trades(self, data, now_ms=None):
        """data : liste de transactions du canal 'trades' ({coin, side, px, sz, time, tid})."""
        now_ms = int(now_ms or time.time() * 1000)
        with self.lock:
            for t in data:
                try:
                    coin = t.get("coin")
                    side = t.get("side")
                    if not coin or side not in ("B", "A"):
                        self.stats["bad"] += 1
                        continue
                    px, sz = float(t["px"]), float(t["sz"])
                    t_ms = int(t.get("time") or now_ms)
                except (KeyError, TypeError, ValueError):
                    self.stats["bad"] += 1
                    continue
                if px <= 0 or sz <= 0:
                    self.stats["bad"] += 1
                    continue
                tid = t.get("tid")
                if tid is not None:
                    seen = self.seen.get(coin)
                    if seen is None:
                        seen = self.seen[coin] = (set(), [])
                    if tid in seen[0]:
                        self.stats["dupes"] += 1
                        continue
                    seen[0].add(tid)
                    seen[1].append(tid)
                    if len(seen[1]) > 6000:                       # memoire bornee
                        for old in seen[1][:2000]:
                            seen[0].discard(old)
                        del seen[1][:2000]
                t0 = t_ms // self.bucket_ms * self.bucket_ms
                b = self.cur.get(coin)
                if b is not None and t0 > b["t0"]:                # la tranche precedente est terminee
                    self.closed.append(self._row(b))
                    b = None
                if b is None:
                    b = self.cur[coin] = self._new(coin, t0)
                elif t0 < b["t0"]:
                    # transaction en retard d une tranche deja close : ignoree (rare, effet negligeable)
                    self.stats["bad"] += 1
                    continue
                usd = px * sz
                if side == "B":
                    b["n_buy"] += 1
                    b["buy_usd"] += usd
                    if usd >= self.big_usd:
                        b["big_buy_usd"] += usd
                else:
                    b["n_sell"] += 1
                    b["sell_usd"] += usd
                    if usd >= self.big_usd:
                        b["big_sell_usd"] += usd
                if usd > b["max_trade_usd"]:
                    b["max_trade_usd"] = usd
                if b["px_open"] is None:
                    b["px_open"] = px
                b["px_high"] = px if b["px_high"] is None else max(b["px_high"], px)
                b["px_low"] = px if b["px_low"] is None else min(b["px_low"], px)
                b["px_close"] = px
                b["_t_last"] = t_ms
                self.stats["trades"] += 1

    def close_old(self, now_ms=None, stream_alive=True, grace_ms=15_000):
        """Cloture les tranches dont l heure est passee (meme sans nouvelle transaction) et marque
        vivantes les tranches globales ecoulees tant que le flux etait actif."""
        now_ms = int(now_ms or time.time() * 1000)
        with self.lock:
            for coin in list(self.cur):
                b = self.cur[coin]
                if b["t0"] + self.bucket_ms + grace_ms <= now_ms:
                    self.closed.append(self._row(b))
                    del self.cur[coin]
            last_done = (now_ms - grace_ms) // self.bucket_ms * self.bucket_ms - self.bucket_ms
            if stream_alive and last_done > self._alive_marked:
                start = self._alive_marked + self.bucket_ms if self._alive_marked >= 0 else last_done
                # une panne ou un redemarrage laisse des trous : seules les tranches recentes sont marquees
                start = max(start, last_done - 12 * self.bucket_ms)
                t = start
                while t <= last_done:
                    self.alive.append(t)
                    t += self.bucket_ms
                self._alive_marked = last_done
            elif not stream_alive:
                self._alive_marked = max(self._alive_marked, last_done)   # pas de marquage pendant une panne

    def drain(self, now_ms=None, stream_alive=True):
        """Retourne (lignes de tranches terminees, tranches vivantes) a ecrire, et les vide."""
        self.close_old(now_ms, stream_alive)
        with self.lock:
            rows, alive = self.closed, self.alive
            self.closed, self.alive = [], []
        return rows, alive
