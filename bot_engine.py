"""
╔═══════════════════════════════════════════════════════╗
║       HyperBot — Pro Edition                          ║
║  RSI + EMA + MACD + BB + Volume + Trailing SL         ║
║  Plage horaire | Multi-Crypto | Dashboard temps réel  ║
║  Perp (BTC, SOL, PAXG) — Long + Short sur tous       ║
╚═══════════════════════════════════════════════════════╝

INSTALLATION :
    pip install hyperliquid-python-sdk eth-account

LANCEMENT :
    python hyperbot_dashboard.py

NOTES :
    - BTC, SOL : marchés perpétuels (long + short)
    - PAXG     : perpétuel or sur Hyperliquid (index 187, levier max x10)
                 Tracker du prix de l or (1 PAXG = 1 once d or)
                 Long ET Short disponibles — remplace XAUT spot
"""

import time
import threading
import json
import db
from flow_recorder import FlowRecorder   # v4.349 — enregistreur de flux (lecture seule)
import scalp_forex as scalp               # v4.353 — scalp Forex (sous-mode du Forex, paper)
import swing_journal as swj               # v4.359 — journal du Swing (lecture seule)
import swing_support as sups              # v4.362 — Swing Support (crypto, paper)
import trend_follow as trf                # v4.365 — Tendance confirmee (module separe, paper)
import mtf_analysis as mtf  # v4.299 — analyse multi-unites de temps
from datetime import datetime
from collections import deque
import queue

# ─────────────────────────────────────────────
#  VERSION
# ─────────────────────────────────────────────
# Incrementer a chaque modification importante
# Visible dans le header du dashboard pour identifier
# exactement quelle version tourne sans ambiguite
BOT_VERSION = "4.413"
BOT_BUILD   = "2026-10-14-a"  # incremente a chaque correctif — visible dans les logs
                               # pour confirmer sans ambiguite quelle version tourne
# Historique :
# 4.299 — Moteur TOP-DOWN multi-unites de temps (H4/Daily -> M15/H1) pour
#        Spot-Accum et Accumulation : tendance + zones cles, signal de bougie,
#        SL sur la structure, objectif zone opposee, taille sur le risque.
# 4.298 — FIX : la bascule en live n efface plus l historique et ne ferme
#        plus les positions reelles ; "Nettoyer" ne supprime plus jamais les
#        trades live (seul l historique paper est efface).
# 4.297 — FIX dimensionnement : un lot par pot (paper / live) au lieu d un
#        lot partage ; positions comptees par pot (Accumulation comprise) ;
#        capital live = valeur reelle du compte Hyperliquid.
# 4.296 — Funding : deverrouillage du live reglable dans l interface ; trades
#        live sous 10 $ de notionnel releves au minimum Hyperliquid.
# 4.295 — FIX RISQUE : le moteur simple activait le levier dynamique 2-5x sur
#        toutes les entrees pres d un niveau (notionnel x2 a x10) ; levier 1
#        par defaut, levier dynamique optionnel.
# 4.294 — Trades live fermes : frais et PnL reels Hyperliquid releves dans la
#        minute et affiches dans l historique a cote du PnL du bot.
# 4.293 — PnL des positions live : quantite reelle Hyperliquid, PnL latent
#        Hyperliquid et frais estimes affiches (ecart bot / Hyperliquid).
# 4.292 — Controle de synchronisation bot <-> Hyperliquid toutes les 2 min
#        (positions fantomes / orphelines, sens, taille, entree, SL natif).
# 4.291 — FIX : prix courant mis a jour pour TOUS les actifs par le WebSocket
#        (il restait fige hors position : diagnostic et largeur du regime).
# 4.290 — Suivi apres SL sur 2 h (delai de retour a l entree, pire recul,
#        simulation SL 0,75/1/1,5/2 %) + rapport statistique telechargeable.
# 4.289 — NETTOYAGE : moteur d entree simple pour Spot-Accum et Accumulation
#        (garde-fous + 1 signal + 1 confirmation) ; diagnostic Funding.
# 4.288 — Voie "continuation" (entree en tendance saine sans proximite d un
#        niveau, sous 5 conditions), simulee en paper meme si le mode est live.
# 4.287 — Trades a contre-tendance desactives (achat en rebond baissier, short
#        en repli haussier, cassure fraiche contre l EMA de tendance).
# 4.286 — SITUATIONS DE MARCHE (fond 1h x court terme 5 min) : regles
#        explicites pour Spot-Accum et Accumulation (hausse saine, repli dans
#        une hausse, baisse saine, rebond dans une baisse), niveaux 1h ou 5 min
#        choisis selon la situation, trailing resserre en contre-tendance ;
#        resistance descendante 1h ; FTM retire.
# 4.285 — FIX : l etoile filante detectee avant l entree fermait les LONG
#        quelques secondes apres l ouverture ; Spot-Accum n achete plus
#        pendant qu une etoile filante est en cours de confirmation.
# 4.284 — Bougies 5 min et 1h recues en TEMPS REEL par WebSocket : chaque
#        bougie est prise en compte a l instant de sa cloture ; REST reduit
#        au chargement initial, a la resynchronisation (6 h) et au repli.
# 4.283 — Tendance (EMA80 5 min) et support/resistance calcules sur les
#        VRAIES bougies 5 min Hyperliquid (verifiables sur le graphique),
#        bougies rafraichies a chaque cloture ; valeurs affichees au diagnostic.
# 4.282 — Spread (carnet d ordres) ajoute a la qualite du marche : affiche,
#        enregistre a chaque entree, filtre optionnel (desactive).
# 4.281 — Anti-range RELATIF a chaque actif (remplace le seuil absolu de 2 %) ;
#        mesures de qualite du marche (volatilite, activite, flux) affichees
#        au diagnostic et enregistrees a chaque entree ; filtres associes
#        prets mais desactives en attendant le calibrage.
# 4.280 — Regime de marche : largeur calculee sur l EMA200 1h de chaque actif
#        (meme unite de temps que BTC) ; plus de regime "confirme" sur BTC
#        seul quand la largeur n est pas mesurable.
# 4.279 — Cassure fraiche : peut de nouveau capter un retournement contre
#        l EMA200 (flux franc + regime non oppose) ; FIX contradictions :
#        l anti-range annulait les voies "cassure fraiche" et "volume en
#        consolidation" (Accumulation : voie volume totalement morte), double
#        critere de proximite sur la voie volume.
# 4.278 — Spot-Accum : achat sur repli (support ascendant = dernier creux plus
#        haut en 1h) ; voie d entree indiquee dans les raisons (Spot-Accum et
#        Accumulation) pour l analyse de l export CSV.
# 4.277 — FIX : les voies d entree par contournement (cassure, volume, fausse
#        cassure, tendance persistante, etoile filante) ne permettent plus
#        d entrer CONTRE la tendance EMA200 et passent par le veto du flux
#        (Spot-Accum et Accumulation).
# 4.276 — Regime de marche global (BTC 1h EMA50/EMA200 + largeur de marche) :
#        Spot-Accum bloque en baissier confirme, Accumulation en haussier
#        confirme ; plages horaires d entree par mode ; stats par heure.
# 4.275 — FIX : plafond SL propre a chaque mode dans la gestion commune
#        Spot-Accum/Accumulation/Forex ; patience SL par le flux reservee a
#        Spot-Accum (elle s appliquait aussi a Accumulation et Forex).
# 4.274 (build 2026-09-24-c) — FIX : import de manual_trading avant le
#        changement de dossier (le serveur ne demarrait plus avec un Volume).
# 4.273 (build 2026-09-24-b) — Onglet TRADING MANUEL : opportunites du bot,
#        parametres proposes et modifiables, execution immediate ou
#        programmee, paper/live, perps/spot (voir manual_trading.py).
# 4.272 (build 2026-09-24-a) — Mode Forex : seuils anti-range (2 % -> 0,25 %)
#        et momentum long terme (2 % -> 0,4 %) recalibres pour les devises
#        (le mode ne pouvait jamais entrer) ; diagnostic Forex explicite.
# 4.271 (build 2026-09-23-g) — Patience du SL Spot-Accum : exige aussi une
#        tendance de fond intacte (prix au-dessus de l EMA200 pour un long).
# 4.270 (build 2026-09-23-f) — Spot-Accum : patience du SL pilotee par le
#        flux (attente si acheteurs dominants, dans la limite de -1 % et
#        15 min) ; motifs de sortie distincts pour mesurer son effet.
# 4.269 (build 2026-09-23-e) — Simulation "SL plus large" (0,75/1/1,5 %)
#        dans le suivi ; Funding ajoute au suivi/export ; plafond de SL,
#        confirmation par le flux, delai apres perte et limite de rafales
#        d entrees reglables PAR MODE (Accumulation : delai 1 h apres perte,
#        3 entrees max / 10 min).
# 4.268 (build 2026-09-23-d) — TTP Funding : armement a +1 % (au lieu de
#        1,5 %), patience pilotee par le flux de transactions (tolerance
#        elargie si flux favorable, reduite si contraire), plancher de gain.
# 4.267 (build 2026-09-23-c) — Funding Contrarian : le pic de trailing du
#        trade precedent n est plus herite a l ouverture (fermetures
#        immediates a ~0 %).
# 4.266 (build 2026-09-23-b) — Flux de transactions fiabilise : reabonnement
#        apres reconnexion WebSocket, fenetre de temps fixe horodatee, repli
#        REST si le flux est mort, pression "soutenue" uniquement sur des
#        transactions nouvelles ; confirmation optionnelle du flux a l entree
#        (ENTRY_FLOW_CONFIRM_MIN_PRESSURE, 0 = desactivee).
# 4.265 (build 2026-09-23-a) — PnL de sortie calcule sur le prix REEL
#        d execution Hyperliquid ; suivi apres sortie (+30/+60 min, plus
#        haut/bas de l heure) et frais/PnL reels pour Spot-Accum et
#        Accumulation ; export CSV depuis l onglet Historique.
# 4.264 (build 2026-09-22-a) — Fermeture reelle verifiee partout (ordre
#        d abord, suivi ferme seulement si Hyperliquid confirme) ; SDK
#        multi-DEX (forex xyz) ; indexation des trades a l ouverture
#        (trade_uid = cloid, mode source + heure) et reprise unifiee au
#        redemarrage ; diagnostic d entree toujours renseigne ; reglages
#        avances entiers valides ; cle JWT obligatoire. NB : BOT_VERSION
#        etait reste fige a "3.1" alors que le code etait en v4.263.
# 3.1 (build 2026-07-04-d) — FIX CRITIQUE : NameError sur 'rsi_mode' utilise
#        avant d etre defini dans le message du filtre ATR — plantait
#        silencieusement le traitement d un actif des que le marche etait
#        juge trop calme (cause probable du blocage "collecte bloquee" —
#        le symbole disparaissait du log sans trace car _process_with_timeout
#        n avait pas de except pour capturer/loguer l exception). Egalement
#        fix : les exceptions dans _process sont desormais capturees et
#        loguees explicitement (message + traceback) au lieu d etre avalees.
# 3.1 (build 2026-07-04-c) — Timeout sur le fetch prix (get_prices), sur le
#        fetch CPI Finnhub, ET filet generique par symbole (_process_with_timeout,
#        12s) — protege contre tout gel du cycle, quelle qu en soit la cause
# 3.1 (build 2026-07-04-b) — WebSocket temps reel (allMids) pour Max Loss/SL/
#        Trailing TP, alarme visible ON/OFF, timeout sur get_prices
# 3.1 (build initial) —
# 3.1 — Fix bug cle API (check placeholder incorrect qui ne se declenchait jamais)
#        Fix bug session fantome (sauvegarde capital/session meme sans demarrage)
#        Levier (LEVERAGE) desormais reellement applique sur Hyperliquid
#        Cle privee/wallet chargeables depuis variables d environnement (securite)
#        Nouveau moteur de risque en dollars : Max Loss -0.75$ gere par le bot,
#        SL 1.5% conserve uniquement comme filet de securite sur Hyperliquid
#        Trailing Take Profit 2 etages : Quick Profit arme a +1$ (sortie si retour a 1$),
#        puis trailing illimite a partir de +1.5$ tant que le profit progresse
#        Score de confiance (0-100%) sur chaque signal — entree seulement si >= 65%
#        Confiance minimale dynamique par actif : +5% apres chaque perte, -5% apres chaque gain
# 1.0 — Version initiale (RSI + EMA + Trailing SL/TP)
# 1.1 — Ajout PAXG EMA 20/50, paliers corriges
# 1.2 — 6 slots ON/OFF, boutons SL/TP/FERMER manuels
# 1.3 — Filtre ATR par symbole, pivot EMA, 2 cycles PAXG
# 1.4 — Support/Resistance scalp (50 cycles)
# 1.5 — Persistance session, reprise auto, uptime
# 1.6 — ATR affiche sur cartes, PnL Session
# 1.7 — Filtre Momentum instantane (4 cycles, tous actifs)
# 1.8 — BTC SL/TP resseres 1.0%/2.0%, Trailing delta 0.8%
# 1.9 — Paliers BTC specifiques (0.35/0.7/1.0/1.5%)
# 2.0 — Fix repertoire travail (chdir au demarrage)
# 2.1 — RSI mode tendance BTC (>50=LONG, <50=SHORT)
# 2.2 — RSI mode tendance HYPE (meme logique que BTC)
# 2.3 — RSI mode tendance ETH + SOL + BNB (PAXG garde retournement)
# 2.4 — Logs enrichis : RSI+mode+ATR dans chaque ouverture et blocage
# 2.5 — Sauvegarde logs dans fichier (hyperbot_log_swing/scalp.txt) avec rotation 5MB
# 2.6 — Dashboard : max 5 messages de position visibles (6eme efface le plus ancien)
# 2.7 — Fichier log : epuration automatique 7 jours (declenchee si >2MB)
# 2.8 — Momentum BTC swing releve a 0.20% (etait 0.15%) — plus selectif SHORT
# 2.9 — EMA intermediaire BTC+ETH (EMA50 swing/EMA60 scalp) filtre tendance 25-50 min
# 2.9b— Log : retention reduite a 24h (etait 7 jours), epuration si >500KB
# 3.0 — TP ramene a 1.5% sur tous les actifs, EMA50/60 etendue a SOL/BNB/HYPE
#        TSL delta 0.6% (etait 1.2%), TTP step 0.8% (etait 1.5%)
#        SOL : MACD+BB obligatoires supprimes (bloquaient tous les trades)

# ─────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────
CONFIG = {
    "PRIVATE_KEY":        "",
    "WALLET_ADDRESS":     "",


    # Cryptos à trader — 6 slots disponibles
    # Modifiables depuis le dashboard via les boutons ACTIF 1 a 6
    # v3.2 — liste etendue a 30 actifs (Hyperliquid propose 300+ perpetuels,
    # ces 30 sont les plus liquides/suivis). Les 6 premiers (BTC, PAXG, ETH,
    # SOL, BNB, HYPE) beneficient d un reglage fin par symbole (voir
    # SYMBOL_RSI_MODE, ATR_MIN_PCT_BY_SYMBOL, etc. plus bas) — les 24 autres
    # utilisent les reglages globaux par defaut (pas de tuning specifique).
    # ACTIVE_COINS (pilotable depuis l interface web) permet de n en activer
    # qu une partie a la fois ; MAX_OPEN_TRADES limite le nombre de positions
    # simultanees quel que soit le nombre d actifs actifs.
    "SYMBOLS":            ["BTC", "PAXG", "ETH", "SOL", "BNB", "HYPE",
                           "ARB", "AVAX", "LINK", "OP", "INJ", "TIA", "TAO",
                           "WIF", "JUP", "PENDLE", "EIGEN", "RENDER", "SUI",
                           "APT", "SEI", "DOGE", "XRP", "NEAR", "AAVE",  # v4.286 : FTM retire (plus de donnees sur Hyperliquid)
                           "UNI", "CRV", "SUSHI", "GMX", "POL",
                           # v4.163 — SUR DEMANDE EXPLICITE : tickers forex
                           # (HIP-3, namespace "xyz:") dedies au mode Normal
                           # — cote deja implicitement contre USD chacun.
                           # Isolation de mode geree dans _process (voir
                           # FOREX_SYMBOLS ci-dessous).
                           "xyz:EUR", "xyz:JPY", "xyz:KRW", "xyz:DXY"],

    # v4.163 — SUR DEMANDE EXPLICITE : liste de reference pour identifier
    # les tickers forex (namespace "xyz:") — Normal est le SEUL mode a les
    # trader ; les autres modes (Accumulation/Funding/Spot-Accum) les
    # ignorent completement, et inversement Normal ignore desormais les
    # cryptos (voir isolation dans _process).
    "FOREX_MODE_SYMBOLS": ["xyz:EUR", "xyz:JPY", "xyz:KRW", "xyz:DXY", "PAXG"],
    # v4.331 — SUR DEMANDE EXPLICITE : le mode Forex (stratégie "Normal") peut
    # AUSSI trader quelques cryptos ayant un bon score, en paper comme en live
    # (le mode paper/live reste celui du mode Forex), avec le levier dynamique
    # habituel de ce mode (_compute_prudent_leverage). Score = taux de reussite
    # historique REEL de l actif (tous modes confondus, hors manuel). Aucune
    # regle horaire forex ne s applique a ces cryptos (marche 24/7), marge
    # croisee comme les autres cryptos.
    "FOREX_CRYPTO_ENABLED": 0,           # v4.339 : DESACTIVE sur demande — le mode Forex ne trade a nouveau que ses actifs Forex (1 = reactive la fonction ci-dessous)
    "FOREX_CRYPTO_SYMBOLS": [],          # cryptos TOUJOURS eligibles (liste manuelle), en plus du score
    "FOREX_CRYPTO_MIN_TRADES": 10,       # echantillon minimal avant de fier le score
    "FOREX_CRYPTO_MIN_WIN_RATE": 55.0,   # taux de reussite minimal (%)
    "FOREX_CRYPTO_MAX_LEVERAGE": 5,      # plafond de levier pour ces cryptos (le levier reste dynamique en dessous)
    # v4.333 — SUR DEMANDE EXPLICITE : reglages PROPRES AU MODE FOREX. Les seuils
    # herites du crypto (armement 0.4 %, flux >= 0.3 % de pic) etaient rarement
    # atteints par des devises (pics observes 0.01 a 0.5 %) : le trailing ne
    # s armait presque jamais. Valeurs en % de MOUVEMENT DE PRIX (pas de marge).
    "FOREX_TTP_ARM_PCT": 0.15,                       # armement du trailing (crypto : SPOT_ACCUM_TTP_ARM_PCT = 0.4)
    "FOREX_TTP_FLOW_REVERSAL_MIN_PEAK_PCT": 0.12,    # pic minimal pour la sortie sur retournement de flux (crypto : 0.3)
    "FOREX_ENTRY_MIN_PCT": 5.0,                      # fenetre d entree : % de l amplitude S/R au-dessus du support (long) / sous la resistance (short)
    "FOREX_ENTRY_MAX_PCT": 30.0,                     # (avant : 5 a 10 %, herite du crypto)
    # v4.336 — SUR DEMANDE EXPLICITE : STOP LOSS DYNAMIQUE pour Forex (remplace le
    # plancher pur). Le SL MONTE (jamais ne descend) au fur et a mesure que le
    # gain progresse : breakeven + frais des l armement, puis suiveur a distance
    # max(marge ATR, fraction du gain maximal). Valeurs en % de mouvement de PRIX.
    "FOREX_DYNAMIC_SL": 1,
    "FOREX_DSL_FEE_PAD_PCT": 0.10,        # plancher apres armement : entree + frais aller-retour (~0.09 % en taker)
    "FOREX_STALE_ENABLED": 1,             # v4.399 : sortie "sans progres" Forex (1/0)
    "FOREX_STALE_MIN": 90,                # ... apres N minutes
    "FOREX_STALE_PEAK_PCT": 0.15,         # ... si le pic de gain est reste sous N % du prix
    "ASSET_BT_DAYS": 90,                  # v4.410 : profondeur (jours) de l historique rejoue pour la pre-programmation
    "FOREX_DSL_TRAIL_FRACTION": 0.40,     # distance du suiveur >= 40 % du gain maximal (jamais plus serre que la marge ATR)
    "FOREX_DSL_FALLBACK_TOL_PCT": 0.08,   # marge de repli si l ATR n est pas encore disponible
    "FOREX_DSL_EXCHANGE_MIN_STEP_PCT": 0.03,  # live : ne met a jour le SL natif Hyperliquid que si le SL a bouge d au moins ce %
    "FOREX_PURE_FLOOR": 0,                # v4.335 (plancher pur) : desactive tant que le SL dynamique est actif                           # v4.335 : 1 = plancher pur (pic - marge ATR), sans condition de bougie/flux, comme Funding
    "FOREX_TREND_HOLD_FILTER_ENABLED": 0,            # 0 = plus de "tendance encore intacte -> position maintenue" pour Forex
    # v4.163 — marge ISOLEE obligatoire pour les marches HIP-3 (contrairement
    # aux cryptos, en marge croisee) — voir application dans le passage d
    # ordre et l ajustement de levier.
    "FOREX_ISOLATED_MARGIN": True,

    # v3.2 — Nouvelle approche : les 30 marches sont TOUS eligibles par
    # defaut (le bot pioche librement parmi eux selon le score de
    # confiance de chacun, pas de presélection restrictive) — la limite
    # reelle est desormais MAX_OPEN_TRADES (15 par defaut). L onglet
    # Marchés reste disponible pour EXCLURE manuellement un actif si
    # besoin, mais ce n est plus une liste a activer un par un.
    # v4.3 — PAXG desactive par defaut (reste dans SYMBOLS/ALL_COINS : simple
    # exclusion, reactivable en un clic depuis l onglet Marches sans toucher
    # au code). POL (ex-MATIC — Polygon a migre son ticker vers POL en 2024,
    # "MATIC" n existe plus sur Hyperliquid) ajoute a la place.
    # v4.6 — TAO, TIA, SUI desactives par defaut : analyse sur 1048 trades
    # reels (07/08/2026) montrant un deficit statistiquement significatif et
    # robuste sur un large echantillon par actif :
    #   TAO  : 77 trades, 28.6% de reussite, -4.63$ net
    #   TIA  : 105 trades, 37.1% de reussite, -4.09$ net
    #   SUI  : 53 trades, 32.1% de reussite, -3.55$ net
    # A eux trois : -12.27$, plus de la moitie de la perte nette totale
    # (-24.04$) sur cette periode. Reactivables en un clic depuis l onglet
    # Marches si une analyse ulterieure montre une amelioration.
    "ACTIVE_COINS":       ["BTC", "ETH", "SOL", "BNB", "HYPE",
                           "ARB", "AVAX", "LINK", "OP", "INJ",
                           "WIF", "JUP", "PENDLE", "EIGEN", "RENDER",
                           "APT", "SEI", "DOGE", "XRP", "NEAR", "AAVE",  # v4.286 : FTM retire (plus de donnees sur Hyperliquid)
                           "UNI", "CRV", "SUSHI", "GMX", "POL",
                           # v4.164 — FIX : oublies lors de l implementation
                           # initiale, empechant tout traitement reel malgre
                           # leur presence dans SYMBOLS.
                           "xyz:EUR", "xyz:JPY", "xyz:KRW", "xyz:DXY"],
    "MAX_OPEN_TRADES":    5,

    # v4.9 — Cooldown de reentree DANS LE MEME SENS apres la fermeture d un
    # trade sur un actif : hypothese que repartir tout de suite dans la
    # meme direction (apres un SL notamment) capture souvent du bruit plutot
    # qu un vrai signal frais. Un signal dans le sens OPPOSE (retournement)
    # n est jamais concerne — seule la repetition immediate du meme pari est
    # freinee. S applique aux deux strategies (normal et Accumulation).
    "REENTRY_COOLDOWN_SEC": 900,  # 15 minutes

    # v4.3 — Actifs EXPLICITEMENT desactives par l utilisateur depuis l onglet
    # Marches (ex: un actif perdant a repetition). Contrairement a une simple
    # absence de ACTIVE_COINS, un actif ici ne peut JAMAIS etre auto-active
    # par l opportunite forte (_gate_active_or_auto_activate) — seule une
    # reactivation manuelle depuis l interface peut l en retirer. PAXG n est
    # PAS ici par defaut (simple exclusion "douce", auto-activable).
    "MANUAL_EXCLUDE_COINS": [],

    # ── Mode ACCUMULATION (v4.8) ─────────────────────────────────────────
    # Strategie INDEPENDANTE de la logique RSI/tendance habituelle : entre
    # un LONG quand le prix est proche du SUPPORT recent, un SHORT quand il
    # est proche de la RESISTANCE recente — logique de rebond/rejet, plutot
    # que de suivi de tendance. Fonctionne EN PLUS des signaux normaux (pas
    # a leur place), avec son propre plafond de trades simultanes
    # (ACCUMULATION_MAX_TRADES, separe de MAX_OPEN_TRADES). Meme moteur de
    # sortie que le bot normal : SL/TP/TTP et calcul du levier prudent
    # identiques (voir _manage_position_impl, _compute_prudent_leverage).
    # Chaque trade issu de ce mode est marque "strategy": "accumulation"
    # (logs, evenements, historique) pour rester bien distinct des trades
    # normaux.
    "ACCUMULATION_ENABLED":              False,
    "ACCUMULATION_MAX_TRADES":           4,     # plafond de trades Accumulation simultanes, independant de MAX_OPEN_TRADES
    "ACCUMULATION_PROXIMITY_PCT":        1.0,   # "proche" du support/resistance = a moins de ce % de distance

    # v4.42 — SUR DEMANDE EXPLICITE : jeu de seuils SL/TTP DEDIE au mode
    # Accumulation (LONG et SHORT confondus, un seul jeu pour les deux
    # sens), separe du mode normal. Laisser a None = aucun changement de
    # comportement (Accumulation continue de retomber sur les reglages du
    # mode normal, comme avant cette fonctionnalite) — ne definir une valeur
    # ici QUE pour ecarter deliberement Accumulation du mode normal.
    "ACCUMULATION_SL_PCT_OF_E":            None,
    "ACCUMULATION_TTP_ARM1_PRICE_PCT":     None,
    "ACCUMULATION_TTP_LOCK1_PRICE_PCT":    None,
    "ACCUMULATION_TTP_ARM2_PRICE_PCT":     None,
    "ACCUMULATION_TTP_TRAIL_GAP_PRICE_PCT": None,
    "ACCUMULATION_SL_ATR_MULTIPLIER":      None,  # utilise seulement si SL_TTP_ADAPTIVE_ENABLED est actif

    # v4.45 — SUR DEMANDE EXPLICITE : meme mecanisme que Accumulation,
    # applique a Funding Contrarian — pour que les 4 modes (normal,
    # accumulation, funding, spot-accum) soient tous reglables
    # independamment. None = herite du mode normal, aucun changement de
    # comportement tant que rien n est personnalise.
    "FUNDING_SL_PCT_OF_E":             None,
    "FUNDING_TTP_ARM1_PRICE_PCT":      None,
    "FUNDING_TTP_LOCK1_PRICE_PCT":     None,
    "FUNDING_TTP_ARM2_PRICE_PCT":      None,
    "FUNDING_TTP_TRAIL_GAP_PRICE_PCT": None,
    "FUNDING_SL_ATR_MULTIPLIER":       None,

    # v4.46 — SUR DEMANDE EXPLICITE : taille par trade (% du capital)
    # INDEPENDANTE par mode — jusqu ici, tous les modes partageaient le meme
    # batch_entry_size fige (calcule une seule fois depuis POSITION_SIZE_PCT
    # global). None = herite du comportement global habituel (aucun
    # changement tant que rien n est personnalise).
    "ACCUMULATION_POSITION_SIZE_PCT": None,
    "FUNDING_POSITION_SIZE_PCT": None,
    "SPOT_ACCUM_POSITION_SIZE_PCT": None,
    # Confirmation de tendance optionnelle : si activee, un LONG pres du
    # support n est accepte QUE si la tendance de fond (EMA200) est deja
    # haussiere (achat du repli dans une tendance, pas un pari de
    # retournement pur) — et inversement pour un SHORT pres de la
    # resistance. Desactivee par defaut : logique de rebond/rejet pure,
    # independante de la tendance de fond.
    "ACCUMULATION_REQUIRE_TREND_CONFIRM": False,

    # v4.19 — SUR DEMANDE EXPLICITE : "respect des niveaux" FUSIONNE dans la
    # logique d entree PRINCIPALE (pas seulement le mode Accumulation, qui
    # reste une strategie a part). Un trade normal ne se declenche desormais
    # QUE s il a une vraie raison structurelle d exister : soit un rebond
    # pres d un support/resistance recent, soit une cassure nette de ce
    # niveau — jamais plus "RSI+EMA d accord au milieu de la fourchette,
    # sans aucun rapport avec la structure du marche". Reduit mecaniquement
    # le nombre de trades, chacun restant structurellement justifie.
    "REQUIRE_LEVEL_RESPECT":      True,
    "ENTRY_LEVEL_PROXIMITY_PCT":  1.0,   # v4.209 — repli uniquement si l ATR n est pas encore disponible (voir ENTRY_ATR_PROXIMITY_MULTIPLIER, desormais utilise en priorite)
    # v4.209 — SUR DEMANDE EXPLICITE : proximite S/R desormais relative a l
    # ATR (volatilite reelle de l actif) plutot qu un % fixe identique pour
    # tous — "proche" = a moins de ce multiple de l ATR du niveau.
    "ENTRY_ATR_PROXIMITY_MULTIPLIER": 1.0,

    # v4.20 — SUR DEMANDE EXPLICITE, suite a un lot de trades fouettes par le
    # bruit apres la fusion du respect des niveaux (pics minuscules 0.01% a
    # 0.66% avant SL) : deux gardes-fous supplementaires, cumulatifs.
    # 1) Confirmation de direction : le MACD doit confirmer RSI+EMA pour
    #    TOUS les actifs (generalise SYMBOL_REQUIRE_MACD_BB a tout le monde).
    "REQUIRE_DIRECTION_CONFIRM":   True,
    # 2) Coherence d amplitude : l ATR recent doit rester dans une fourchette
    #    coherente avec le SL configure — ni trop calme (TTP inatteignable),
    #    ni trop agite (le bruit seul suffit a toucher le SL).
    "REQUIRE_AMPLITUDE_COHERENCE": True,
    "MIN_AMPLITUDE_TO_SL_RATIO":   0.5,   # ATR minimum = 50% du SL configure
    "MAX_AMPLITUDE_TO_SL_RATIO":   2.5,   # ATR maximum = 250% du SL configure

    # v4.37 — SUR DEMANDE EXPLICITE, DESACTIVE PAR DEFAUT (switch separe,
    # a activer volontairement une fois qu on aura des donnees sur les
    # garde-fous Accumulation deja en place) : bloque un LONG si le support
    # est proche ET en dessous de l EMA200 (marche sans separation nette par
    # rapport a sa moyenne longue — signe d un range sans vraie tendance),
    # et un SHORT si la resistance est proche ET au dessus de l EMA200.
    # Applique au mode normal ET a Accumulation.
    "REQUIRE_SR_EMA200_SEPARATION": True,
    "SR_EMA200_PROXIMITY_PCT": 0.5,  # "proche" de l EMA200 = a moins de ce % d ecart

    # v4.25 — Confirmation renforcee apres un gain (voir _process) : nombre
    # de cycles CONSECUTIFS ou toutes les conditions d entree doivent rester
    # vraies avant d autoriser une reouverture dans le MEME sens qu un trade
    # qui vient de gagner. Chaque cycle dure CYCLE_INTERVAL secondes (10s
    # par defaut) — 18 cycles = ~3 minutes de confirmation soutenue,
    # suffisant pour filtrer un simple recroisement furtif sans pour autant
    # rater une vraie continuation.
    "POST_WIN_CONFIRM_CYCLES": 18,
    # v4.26 — Delai maximum d attente (en cycles) avant de basculer sur un
    # second indicateur (Bollinger) plutot que de rester bloque
    # indefiniment si le signal n est jamais soutenu 18 cycles d affilee.
    # 180 cycles = ~30 min a 10s/cycle — au-dela, l actif retrouve la main
    # (avec ou sans confirmation Bollinger), jamais bloque plus longtemps.
    "POST_WIN_MAX_WAIT_CYCLES": 180,

    # v4.33 — SUR DEMANDE EXPLICITE : mode "Funding Contrarian", source de
    # signal FONDAMENTALEMENT DIFFERENTE de RSI/MACD/EMA (des indicateurs
    # deja integres dans les prix par des acteurs plus rapides). Le funding
    # rate reflete un vrai desequilibre de position entre traders a effet de
    # levier (pas un motif de prix) : quand il est extreme, ca signale un
    # positionnement sur-leverage dans un sens — logique contrarian (SHORT
    # si funding tres positif = trop de LONG en levier, LONG si tres negatif).
    # AUCUNE garantie que cet avantage soit reel sur cette plateforme/ces
    # actifs — a valider par les resultats, pas suppose. SECURITE EXPLICITE :
    # reste cantonne au paper trading tant que FUNDING_MODE_LIVE_ALLOWED
    # n est pas active manuellement, meme si le bot est en mode live —
    # protege un capital de trading deja fragilise pendant la phase de test.
    "FUNDING_MODE_ENABLED": False,
    "FUNDING_MODE_LIVE_ALLOWED": 0,  # reste paper-only tant que non deverrouille explicitement (v4.296 : reglable dans l interface)
    # v4.87 — SUR DEMANDE EXPLICITE : chaque mode peut desormais basculer
    # INDEPENDAMMENT entre paper et live. None = suit le mode global du bot
    # (aucun changement de comportement tant que rien n est personnalise).
    # "paper" ou "live" = force cette valeur pour ce mode precis, quel que
    # soit le mode global. Le garde-fou FUNDING_MODE_LIVE_ALLOWED ci-dessus
    # reste actif EN PLUS pour funding_contrarian, jamais retire.
    "STRATEGY_MODE_OVERRIDE": {
        "forex": None,
        "accumulation": None,
        "funding_contrarian": None,
        "spot_accumulation": None,
    },
    # v4.106 — SUR DEMANDE EXPLICITE : bouton Marche/Arret INDEPENDANT par
    # mode — True = ouvre normalement de nouveaux trades pour ce mode,
    # False = bloque UNIQUEMENT les nouvelles ouvertures (les positions
    # deja ouvertes de ce mode continuent normalement jusqu a fermeture).
    "STRATEGY_TRADING_ENABLED": {
        "forex": True,
        "accumulation": True,
        "funding_contrarian": True,
        "spot_accumulation": True,
    },
    "FUNDING_ANNUAL_THRESHOLD_PCT": 25.0,  # funding annualise au-dela duquel le positionnement est juge "extreme"
    # v4.249 — SUR DEMANDE EXPLICITE : sortie dediee quand le taux revient
    # DANS la fourchette normale (these d entree resolue) — ratio du seuil
    # d entree en dessous duquel on considere le taux "normalise" (0.4 =
    # 10% si le seuil d entree est 25%, nettement en dessous, pas juste
    # a peine repasse sous le seuil d entree).
    "FUNDING_EXIT_ON_RATE_NORMALIZED": True,
    "FUNDING_EXIT_NORMALIZE_RATIO": 0.4,
    # v4.256 — SUR DEMANDE EXPLICITE : retire le TTP classique (mouvement
    # de prix) pour Funding — seule la normalisation du taux (ci-dessus)
    # doit declencher une sortie en profit, coherent avec sa these
    # d entree. Le SL classique/structurel reste actif normalement.
    "FUNDING_SKIP_CLASSIC_TTP": True,
    # v4.258 — SUR DEMANDE EXPLICITE : TTP dedie, simple et fixe pour
    # Funding — arme a ce % de pic, tolere ce % de repli avant de fermer.
    "FUNDING_TTP_ARM_PCT": 0.5,               # v4.342 SUR DEMANDE EXPLICITE : 1.0 -> 0.5 (11 perdants sur 106 trades avaient un pic de 0.5 a <1 %, tous sans protection) ; v4.268 : 1.5 -> 1.0
    "FUNDING_PNL_TRACK_TOUCH_PCT": 0.5,       # v4.342 : seuil "touche" du suivi du chemin du PnL (lecture seule)
    "FUNDING_TTP_TOLERANCE_PCT": 0.5,
    # v4.268 — PATIENCE DU TTP FUNDING PILOTEE PAR LE FLUX DE TRANSACTIONS :
    # au moment ou le repli atteint la tolerance, le flux decide.
    #  - flux toujours FAVORABLE au trade (vendeurs dominants pour un short,
    #    acheteurs pour un long) -> patience : tolerance elargie ;
    #  - flux CONTRAIRE -> sortie anticipee (tolerance reduite) ;
    #  - flux neutre ou indisponible -> tolerance normale.
    # Un PLANCHER garantit qu un trade arme ne revient jamais sous ce gain.
    "FUNDING_TTP_FLOW_ENABLED": True,
    "FUNDING_TTP_FLOW_THRESHOLD": 0.2,        # |pression| minimale pour juger le flux favorable/contraire
    "FUNDING_TTP_FLOW_MAX_TOLERANCE_PCT": 0.9, # tolerance maximale accordee par la patience
    "FUNDING_TTP_FLOW_FAST_TOLERANCE_PCT": 0.25, # tolerance si le flux se retourne contre le trade
    "FUNDING_TTP_MIN_LOCK_PCT": 0.3,          # plancher de gain (% de prix) une fois le TTP arme
    # Confirmation d entree par flux — rejette si le flux contredit
    # clairement la these (le retournement attendu ne montre aucun signe
    # naissant), reduisant le hasard d une entree basee sur le taux seul.
    "FUNDING_ENTRY_FLOW_CONFIRM_ENABLED": True,
    "FUNDING_ENTRY_FLOW_CONTRADICTION_THRESHOLD": 0.3,
    "FUNDING_MODE_MAX_TRADES": 3,          # plafond de trades simultanes, independant des autres modes
    "FUNDING_REFRESH_SEC": 300,            # frequence de rafraichissement du funding (5 min, evite de spammer l API)

    # v4.43 — SUR DEMANDE EXPLICITE : mode "Spot-Accumulation" — achat
    # d actif dans l esprit spot ("tant que l actif existe on peut esperer
    # une hausse ou garder ses actifs"). DESACTIVE par defaut. Particularites
    # par rapport a tous les autres modes :
    #   - AUCUN Stop Loss par defaut (une position perdante reste ouverte
    #     indefiniment, jamais fermee de force sur une perte) — un
    #     interrupteur separe (SPOT_ACCUM_SL_ENABLED) permet d en ajouter un,
    #     exprime en % du PnL (pas % de E comme les autres modes).
    #   - Toujours LEVIER x1 (jamais de levier prudent calcule) — coherent
    #     avec l esprit spot et l absence de SL (pas de risque de liquidation).
    #   - Entree LONG uniquement, avec la tendance generale haussiere
    #     (EMA200) ET a au moins SPOT_ACCUM_MIN_ABOVE_SUPPORT_PCT au-dessus
    #     du support (pas pres du support comme Accumulation — ici on
    #     achete une tendance deja engagee, pas un rebond).
    #   - Sortie : TTP arme une fois le PnL >= SPOT_ACCUM_TTP_ARM_PCT (3%),
    #     puis trailing avec une marge de SPOT_ACCUM_TTP_TOLERANCE_PCT
    #     (0.5%) depuis le pic. Objectif complementaire : si le prix
    #     atteint SPOT_ACCUM_TARGET_SR_PCT (80%) de la distance
    #     support-resistance mesuree a l entree, fermeture immediate
    #     (objectif atteint), meme si le trailing n a pas encore suivi.
    # INTERPRETATION A CONFIRMER : la formulation "TTP a 3% avec tolerance
    # de 0.5%" a ete comprise comme un armement a 3% puis un trailing avec
    # 0.5% de marge de repli depuis le pic — a corriger si l intention etait
    # differente (ex: fenetre d armement 2.5%-3.5% plutot qu un trailing).
    "SPOT_ACCUM_ENABLED": True,
    "SPOT_ACCUM_MAX_TRADES": 8,
    # v4.108 — FIX BUG CRITIQUE : desormais en % de l AMPLITUDE (comme le
    # seuil structurel du trailing, 70% de l amplitude), pas du prix du
    # support — recalibre a 5-10% de l amplitude (au lieu de 1-5% du prix).
    "SPOT_ACCUM_MIN_ABOVE_SUPPORT_PCT": 5.0,
    # v4.50 — SUR DEMANDE EXPLICITE : plafond ajoute (n existait pas avant)
    # — l entree doit rester dans une fenetre serree pres du support, pas
    # n importe ou jusqu a la resistance.
    "SPOT_ACCUM_MAX_ABOVE_SUPPORT_PCT": 10.0,
    # v4.53 — SUR DEMANDE EXPLICITE : la fourchette support-resistance doit
    # avoir une amplitude minimale (support=100 -> resistance >= 103 pour 3%).
    # v4.127 — SUR DEMANDE EXPLICITE : neutralise par defaut (False),
    # remplace par le detecteur de range direct (ANTI_RANGE ci-dessous).
    "SPOT_ACCUM_REQUIRE_SR_AMPLITUDE": False,
    "SPOT_ACCUM_MIN_SR_AMPLITUDE_PCT": 2.0,
    # v4.53 — SUR DEMANDE EXPLICITE : confirmation ADX de la tendance
    # (reutilise ADX_TREND_THRESHOLD, 25 par defaut) — "tendance haussiere
    # claire", pas juste prix > EMA200 franchi de justesse.
    "SPOT_ACCUM_REQUIRE_ADX_CONFIRM": False,
    # v4.127 — SUR DEMANDE EXPLICITE : meme detecteur de range direct que
    # pour Accumulation.
    "SPOT_ACCUM_ANTI_RANGE_MIN_PCT": 2.0,
    # v4.130 — SUR DEMANDE EXPLICITE : 300 echantillons = 10h de recul (10
    # bougies 1h) — coherent avec la lecture d une tendance sur plusieurs
    # bougies, au lieu d 1h (l equivalent d une seule bougie), juge
    # incoherent.
    # v4.131 — SUR DEMANDE EXPLICITE : aligne sur la periode reelle de l
    # EMA200 (200 echantillons = 6h40), qui reste un vrai "EMA200" standard
    # — les deux systemes partagent deja la meme source (state.mtf_prices),
    # ils atteignent desormais leur pleine maturite au meme moment.
    "SPOT_ACCUM_ANTI_RANGE_LOOKBACK": 80,
    "SPOT_ACCUM_TTP_ARM_PCT": 0.4,             # v4.180 SUR DEMANDE EXPLICITE : abaisse de 1.0% a 0.4% — des pics de 0.64-0.89% observes n armaient jamais le trailing (sous l ancien seuil de 1.0%), laissant le prix redonner integralement jusqu en perte sans aucune protection
    "SPOT_ACCUM_TTP_TOLERANCE_PCT": 0.4,       # v4.181 SUR DEMANDE EXPLICITE : reduit de 0.5% a 0.4%, marge de repli depuis le pic, une fois arme
    "SPOT_ACCUM_TARGET_SR_PCT": 80.0,          # objectif = ce % de la distance support-resistance (mesuree a l entree)
    # v4.161 — SUR DEMANDE EXPLICITE : desactive par defaut — la fermeture
    # forcee a l objectif empechait de laisser courir un trade au-dela de
    # 80% de la distance S/R initiale, meme quand la tendance restait
    # forte. Le trailing (TTP) seul gere desormais la sortie, capturant
    # potentiellement bien plus de hausse si le mouvement continue.
    "SPOT_ACCUM_TARGET_EXIT_ENABLED": False,
    # v4.49 — SUR DEMANDE EXPLICITE : second seuil de declenchement du
    # trailing (s ajoute a SPOT_ACCUM_TTP_ARM_PCT, arme des que l un des
    # deux est atteint) — base sur la structure du marche, pas un % de PnL.
    "SPOT_ACCUM_TRAILING_ARM_SR_PCT": 70.0,    # = support + ce % de la distance support-resistance
    "SPOT_ACCUM_SL_ENABLED": False,            # AUCUN SL par defaut — interrupteur explicite pour en ajouter un
    "SPOT_ACCUM_SL_PCT_OF_PNL": 1.5,            # SL conditionnel (ne ferme QUE si retournement instantane en cours)
    # v4.72 — SUR DEMANDE EXPLICITE : marge de distance minimale pour le
    # retournement INSTANTANE (SL conditionnel) — filtre le bruit pur (prix
    # techniquement de l autre cote de l EMA200 mais par un ecart
    # insignifiant), sans imposer de delai de confirmation.
    "SPOT_ACCUM_INSTANT_REVERSAL_MARGIN_PCT": 0.15,
    # v4.70 — SUR DEMANDE EXPLICITE : plafond dur en dernier recours — ferme
    # QUOI QU IL ARRIVE au-dela de ce seuil, sans condition de retournement.
    # ACTIF par defaut (contrairement au SL conditionnel ci-dessus).
    "SPOT_ACCUM_HARD_SL_ENABLED": True,
    "SPOT_ACCUM_HARD_SL_PCT": 5.0,
    # v4.73 — TP sur retournement retire (redondant avec le TTP, voir plus bas).
    # v4.47/v4.54 — SUR DEMANDE EXPLICITE : fermeture si un retournement de
    # tendance est CONFIRME (prix sous l EMA200 de facon soutenue) — activee
    # par defaut. v4.54 corrige DEUX axes : la duree (l EMA200 ne bouge que
    # toutes les ~2 min, 3 min de confirmation etait trop court pour
    # representer un vrai changement de tendance) ET la qualite des donnees
    # (n accepte le signal que si l EMA200 est assez mature ET la collecte
    # saine — pas de coupure recente).
    "SPOT_ACCUM_REVERSAL_EXIT_ENABLED": True,
    "SPOT_ACCUM_REVERSAL_CONFIRM_CYCLES": 1080,      # v4.172 SUR DEMANDE EXPLICITE : 3h a 10s/cycle (etait 30 = 5min) — Spot-Accum vise a TENIR une tendance sur la duree ("tant que l actif existe"), un exit aussi rapide que celui d Accumulation (5min, juge separement trop lent AVANT correctif) allait a l encontre de cette philosophie patiente — 92% des fermetures via ce motif, pic moyen de 0.20% seulement observes.
    # v4.177 — SUR DEMANDE EXPLICITE : plafond de perte tolere PENDANT
    # l attente de confirmation de retournement (3h ci-dessus) — protege
    # le capital d une erosion progressive sans remettre en cause la
    # patience elle-meme.
    # v4.177 (annule) — plafond de perte pendant l attente retire sur
    # demande explicite : le vrai levier d action est le levier x1
    # (ci-dessous), pas un seuil % supplementaire.
    "SPOT_ACCUM_REVERSAL_MIN_EMA_MATURITY": 100,    # bougies mtf minimum (sur 200 max) pour faire confiance a l EMA200

    # v4.64 — SUR DEMANDE EXPLICITE : meme mecanisme de retournement
    # confirme que Spot-Accumulation, applique a Accumulation (LONG ET
    # SHORT, contrairement a Spot-Accum qui est LONG uniquement) — sortie
    # possible BIEN AVANT le SL, des qu un vrai changement de tendance est
    # confirme (pas un simple creux passager, voir garde-fous qualite des
    # donnees). ACTIF par defaut.
    "ACCUMULATION_REVERSAL_EXIT_ENABLED": True,
    "ACCUMULATION_REVERSAL_CONFIRM_CYCLES": 30,      # v4.157 SUR DEMANDE EXPLICITE : 5 min a 10s/cycle (etait 180 = ~30 min, aligne sur le meme correctif applique a Spot-Accum)
    "ACCUMULATION_REVERSAL_MIN_EMA_MATURITY": 100,

    # v4.44 — SUR DEMANDE EXPLICITE : liste d actifs DEDIEE par mode
    # (independante de la liste globale ACTIVE_COINS geree dans Marches).
    # None = herite de la liste globale (aucun changement de comportement
    # tant que rien n est personnalise) — voir _gate_active_or_auto_activate.
    "ACCUMULATION_ACTIVE_COINS": None,
    "FUNDING_ACTIVE_COINS": None,
    "SPOT_ACCUM_ACTIVE_COINS": None,

    # v4.24 — SL/TTP ADAPTATIFS a l ATR reel (optionnel, DESACTIVE par
    # defaut — activation explicite requise). Au lieu de seuils fixes,
    # chaque trade calcule son propre SL a partir de l ATR au moment de
    # l entree (SL = ATR% x SL_ATR_MULTIPLIER, borne entre SL_PCT_MIN et
    # SL_PCT_MAX pour eviter les cas extremes). Les seuils TTP (arm1, lock1,
    # arm2, gap) sont ensuite recalcules pour CONSERVER LES MEMES
    # PROPORTIONS que les reglages fixes actuels (TTP_ARM1_PRICE_PCT etc.),
    # juste mis a l echelle du SL adaptatif. Toujours en % — un marche
    # volatil obtient des seuils plus larges, un marche calme des seuils
    # plus serres, mais toujours proportionnellement coherents entre eux.
    "SL_TTP_ADAPTIVE_ENABLED": False,
    "SL_ATR_MULTIPLIER":       2.0,   # SL = ATR% x ce multiplicateur — v4.137 SUR DEMANDE EXPLICITE : 1.0x juge trop serre (frequents SL prematures malgre des signaux corrects), double a 2.0x pour laisser plus de place au trade
    "SL_PCT_MIN":              0.3,   # plancher de securite (evite un SL quasi nul si ATR tres faible)
    # v4.112 — SUR DEMANDE EXPLICITE : mouvement de PRIX minimum garanti
    # avant que le SL ne puisse se declencher, quel que soit le levier —
    # compense sl_pct_of_e a la hausse si le levier > x1 et que le plancher
    # adaptatif rendrait le mouvement de prix requis trop petit (bruit).
    "SL_MIN_PRICE_MOVE_PCT":   0.3,
    # v4.66 — SUR DEMANDE EXPLICITE : plancher STRICT et DIRECT sur les
    # seuils d armement (tier0/tier1), independant du ratio de mise a
    # l echelle — evite les trades microscopiques sur les actifs a tres
    # faible ATR. tier0 utilise la moitie de cette valeur (ratio ~0.5).
    "TTP_MIN_ARM_PCT_FLOOR":   0.3,
    # v4.68 — SUR DEMANDE EXPLICITE : ratios FIXES du tier0 par rapport au SL
    # adaptatif — remplacent un calcul de ratio fragile (derive de 2 valeurs
    # independantes qui pouvaient diverger fortement entre modes, ex:
    # Funding avec un SL tres bas). 0.5 et 0.42 correspondent aux reglages
    # de base historiques (TTP_TIER0_ARM/GAP_PRICE_PCT face a SL_PCT_OF_E=1.0%).
    "TIER0_ARM_RATIO_OF_SL":  0.5,
    "TIER0_GAP_RATIO_OF_SL":  0.42,
    # v4.120 — SUR DEMANDE EXPLICITE : marge d hysteresis pour la
    # reactivation defensive tier1->tier0 — evite l oscillation quand le
    # prix hesite pres du seuil d armement (observe sur WIF).
    "TIER0_REARM_HYSTERESIS_PCT": 15.0,
    "SL_PCT_MAX":              3.0,   # plafond de securite (evite un SL demesure si ATR tres eleve)

    # Tous les symboles sont des perpétuels — SPOT_SYMBOLS vide
    # PAXG remplace XAUT spot : index 187 sur Hyperliquid, levier max x10
    # Ticker direct "PAXG" dans l API (pas de @XXX)
    "SPOT_SYMBOLS":       [],
    "SPOT_TICKER_MAP":    {},
    "SPOT_SL_ASSET_MAP":  {},

    # Stop Loss / Take Profit specifiques par symbole — v3.2 : generalises,
    # plus aucune exception par symbole (voir PROFILE_SWING/PROFILE_SCALP,
    # qui ecrasent de toute facon ces valeurs a chaque demarrage/reset via
    # apply_profile). Utilise STOP_LOSS_PCT/TAKE_PROFIT_PCT globaux pour tous.
    "SYMBOL_SL_PCT":      {},
    "SYMBOL_TP_PCT":      {},

    # RSI specifique par symbole — v3.2 : generalise, plus d exception.
    "SYMBOL_RSI_OVERSOLD":   {},
    "SYMBOL_RSI_OVERBOUGHT": {},

    # Symboles pour lesquels MACD + BB sont OBLIGATOIRES pour entrer (pas juste optionnels)
    "SYMBOL_REQUIRE_MACD_BB": [],

    # Symboles pour lesquels l EMA200 est OBLIGATOIRE — v3.2 : generalise,
    # plus aucun symbole n a cette contrainte particuliere.
    "SYMBOL_REQUIRE_EMA200": [],

    "CAPITAL_USD":        100,
    "POSITION_SIZE_PCT":  20,              # 20% du capital par trade — E fige par lot (voir MAX_OPEN_TRADES)
    "LEVERAGE":           1,

    # RSI — seuils elargis pour signaux plus forts et moins de faux positifs
    "RSI_PERIOD":         14,
    "RSI_OVERSOLD":       32,              # etait 38 : entre uniquement sur vraie survente
    "RSI_OVERBOUGHT":     68,              # etait 62 : entre uniquement sur vrai surachat

    # EMA — periodes plus longues pour reduire les faux croisements (whipsaws)
    "EMA_SHORT":          12,             # etait 8
    "EMA_LONG":           26,             # etait 21

    # MACD — inchange, deja bien calibre
    "MACD_FAST":          12,
    "MACD_SLOW":          26,
    "MACD_SIGNAL":        9,

    # Bollinger Bands — std reduit pour que les bandes soient utiles
    "BB_PERIOD":          20,             # etait 14 : periode standard
    "BB_STD":             2.0,            # etait 2.5 : bandes plus proches = filtre actif

    # Volume : ratio minimum vs moyenne (1.0 = desactive)
    "VOLUME_MIN_RATIO":   1.2,            # etait 1.5 : moins restrictif

    # Gestion du risque — SL elargi pour laisser le trade respirer
    "STOP_LOSS_PCT":      1.5,            # etait 0.8 : evite les SL sur simple bruit
    "TAKE_PROFIT_PCT":    3.0,            # etait 2.0 : ratio RR 1:2 maintenu

    # Trailing Stop Loss — delta elargi pour ne pas couper les trades gagnants
    "TRAILING_STOP":      True,
    "TRAILING_DELTA_PCT": 1.2,   # delta global, applique desormais a tous uniformement

    # v3.2 : plus de delta specifique par symbole — generalise a tous.
    "SYMBOL_TRAILING_DELTA_PCT": {},

    # Seuil minimum de deplacement du Trailing SL avant synchronisation sur Hyperliquid.
    # Evite les appels API inutiles sur de micro-mouvements de prix.
    # Exemple : 0.1 = le SL doit avoir bouge d au moins 0.1% pour etre envoye a Hyperliquid.
    "TRAILING_SL_MIN_MOVE_PCT": 0.1,

    # Buffer de securite sur les ordres SL/TP poses sur Hyperliquid.
    # Meme avec le mark price, un micro-ecart residuel peut exister.
    # Ce buffer decale legerement les niveaux pour eviter les sorties inattendues.
    # Exemple : 0.05% sur un SL a $60000 long = SL pose a $59970 au lieu de $60000
    "MARK_PRICE_BUFFER_PCT": 0.05,
    # Apres la reouverture du marche Forex (lundi matin, fin de pause nocturne),
    # le bot observe PAXG pendant cette duree avant de prendre des positions.
    # Permet au spread de se normaliser et aux indicateurs de se recaler.
    # S applique aussi bien aux longs qu aux shorts sur PAXG.
    "FOREX_WARMUP_MINUTES": 15,
    "FOREX_SYMBOLS":        ["PAXG"],  # symboles soumis a la chauffe Forex

    # ── Trailing Take Profit ──────────────────────────────────────────────────
    # Quand le prix atteint le TP initial, au lieu de fermer la position,
    # le bot deplace le TP plus haut (step) et attend un retournement de tendance
    # confirme par au moins 2 signaux sur 3 avant de sortir.
    "TRAILING_TP":             True,
    "TRAILING_TP_STEP_PCT":    1.5,
    # Seuils de retournement pour la sortie Trailing TP :
    "TRAILING_TP_RSI_EXIT":    55,
    "TRAILING_TP_MIN_SIGNALS": 2,

    # SL protecteur des gains — quand le Trailing TP se deplace vers un nouveau sommet,
    # le SL remonte a ce pourcentage du TP precedent (calcule sur le gain, pas le prix brut).
    # 97% = on preserve 97% du gain acquis au moment ou le dernier TP etait atteint.
    # Garantit qu on ne peut jamais reperdre ce qui a ete gagne une fois le TP initial touche.
    "TRAILING_TP_PROTECT_PCT": 0.97,

    # Plage horaire Paris (0 et 24 = 24h/24) — appliquee en mode paper ET live
    "TRADE_HOUR_START":   0,
    "TRADE_HOUR_END":     24,

    "MODE":               "paper",
    "CYCLE_INTERVAL":     10,
    # Duree (s) sans tick WebSocket recu au-dela de laquelle le cycle reprend
    # la main sur la surveillance des positions ouvertes (filet de secours si
    # le WebSocket se deconnecte silencieusement). Voir _on_ws_allmids et
    # _maybe_manage_position_via_cycle.
    "WS_STALE_AFTER_SEC": 20,
    # Delai max (s) tolere pour un appel reseau de recuperation des prix
    # (get_prices). Au-dela, l appel est ABANDONNE (thread daemon laisse
    # tourner en arriere-plan) plutot que de geler tout le cycle indefiniment
    # en cas de coupure reseau. Voir _get_prices_with_timeout.
    "PRICE_FETCH_TIMEOUT_SEC": 10,
    # Delai max (s) tolere pour le traitement complet d un symbole (_process).
    # Filet de secours generique : si N IMPORTE QUELLE partie du traitement se
    # bloque un jour (appel reseau cache, I/O disque, etc.), ce symbole est
    # simplement ignore pour ce cycle au lieu de geler tout le bot.
    "PROCESS_TIMEOUT_SEC": 12,

    # Profil actif au demarrage : "swing" ou "scalp"
    "PROFILE":            "swing",

    # ── Moteur de risque en % DE E (v4.0) ────────────────────────────────────
    # E = taille de l entree (POSITION_SIZE_PCT % du capital), AVANT levier.
    # Le SL % "legacy" ci-dessus (STOP_LOSS_PCT / SYMBOL_SL_PCT) n est plus
    # utilise pour la gestion normale des sorties : il sert desormais
    # UNIQUEMENT a poser un ordre de securite fixe sur Hyperliquid (filet de
    # secours si le bot est deconnecte / en retard). La gestion normale se
    # fait entierement en % de E :
    # v4.10 — RETOUR au SL en % de E (perte $ PLAFONNEE, independante du
    # levier) — sur clarification explicite : "ce qui change avec le levier
    # ce n est pas le montant de la perte, c est la distance de prix
    # necessaire pour l atteindre". Le levier reduit le mouvement de prix
    # requis pour toucher le SL (E=20$, SL=1% -> perte 0.20$ a x1 COMME a x3,
    # mais il faut un mouvement de 1% a x1 contre seulement 0.33% a x3). Le
    # TP, lui, reste en % de mouvement de prix pur (inchange, amplifie par
    # le levier) — SEUL le SL est plafonne en $ ainsi, sur demande explicite.
    "SL_PCT_OF_E":            1.0,   # Stop Loss = -1.0% de E -> perte $ plafonnee, quel que soit le levier
    # v4.149 — SUR DEMANDE EXPLICITE : "patience" avant fermeture SL,
    # applicable a TOUS les modes (fonction partagee) — exige que le prix
    # reste au-dela du seuil pendant ce nombre de cycles CONSECUTIFS
    # (defaut 10, ~100s) avant de fermer reellement. Filtre les meches
    # breves (bruit) sans affecter un vrai effondrement soutenu.
    "SL_PATIENCE_CYCLES":     10,
    # v4.179 — SUR DEMANDE EXPLICITE : plafond de duree maximale (toutes
    # strategies sauf Spot-Accum, philosophie patiente explicitement
    # exclue) — ferme uniquement si le PnL est neutre ou positif, jamais
    # force une perte.
    "MAX_HOLD_DURATION_ENABLED": True,
    "MAX_HOLD_DURATION_HOURS": 12,
    # v4.182 — SUR DEMANDE EXPLICITE : sortie anticipee des qu une bougie
    # confirme un retournement, une fois le trailing arme — n attend plus
    # forcement le seuil de repli complet. Applique a tier0/tier1
    # (Normal/Accumulation/Funding) et Spot-Accum.
    "EARLY_REVERSAL_EXIT_ENABLED": True,
    # v4.213 — SUR DEMANDE EXPLICITE : filet de securite inconditionnel
    # (Accumulation/Spot-Accum) — ferme si le repli depuis le pic atteint
    # ce seuil, MEME SANS confirmation de couleur de bougie (protege contre
    # une degradation progressive du pic sur des bougies ambigues).
    "TTP_UNCONDITIONAL_GIVEBACK_PCT": 1.0,
    # v4.216 — SUR DEMANDE EXPLICITE : le filet inconditionnel ci-dessus ne
    # s active que si le pic a atteint au moins ce seuil — priorite au
    # trailing normal (couleur de bougie) pour les pics plus modestes.
    "TTP_UNCONDITIONAL_GIVEBACK_MIN_PEAK_PCT": 2.0,
    # v4.230 — SUR DEMANDE EXPLICITE : tolerance TTP relative a l ATR
    # (volatilite reelle) plutot qu un % fixe, et declencheur de VITESSE de
    # repli (independant de l ampleur absolue) — reagit a un retournement
    # rapide meme sous le seuil du filet inconditionnel ci-dessus.
    "TTP_ATR_TOLERANCE_MULTIPLIER": 1.0,
    "TTP_VELOCITY_WINDOW_SEC": 60,
    "TTP_VELOCITY_GIVEBACK_PCT": 0.6,
    # v4.203 — SUR DEMANDE EXPLICITE : confirmation d entree par tendance
    # dynamique (point de depart + retournement confirme sur 3 bougies 1h)
    # et MACD 1h — Accumulation (short) et Spot-Accum (long) uniquement.
    "DYNAMIC_TREND_CONFIRM_ENABLED": True,
    # v4.225 — SUR DEMANDE EXPLICITE : bonus de confiance (jamais bloquant)
    # quand la tendance dynamique + MACD 1h confirment — remplace l ancien
    # blocage dur, qui causait une regression (Spot-Accum moins reactif
    # sur de vraies hausses crypto, la tendance dynamique horaire pouvant
    # rester temporairement dans le mauvais sens meme au sein d une
    # tendance de fond deja favorable).
    "DYNAMIC_TREND_CONFIRM_BONUS": 5.0,
    # v4.193 — SUR DEMANDE EXPLICITE : filet de securite immediat pour le SL
    # structurel — ferme sans attendre la confirmation complete (patience +
    # couleur de bougie) des que la perte atteint ce plafond, evitant une
    # derive prolongee (perte de -6% observee sur un cas reel avant ce
    # correctif). Applique a tous les modes utilisant le SL structurel.
    "STRUCTURAL_SL_HARD_CAP_PCT": 0.5,
    # v4.154 — SUR DEMANDE EXPLICITE : meme principe de patience, applique
    # au TTP, COUPLE a une confirmation par changement de couleur de
    # bougie — les deux synchronisees sur les memes donnees temps reel
    # (candle_history, alimente par le WebSocket). 5 cycles par defaut
    # (un peu plus court que le SL, car le TTP protege deja un gain).
    "TTP_PATIENCE_CYCLES":    5,
    "EXCHANGE_SAFETY_SL_MULT": 2.0,  # SL pose sur Hyperliquid = ce multiple du SL bot (filet de securite uniquement)
    # v4.235 — SUR DEMANDE EXPLICITE : marge du SL de SECOURS pose sur
    # Hyperliquid uniquement si aucun SL n est deja detecte lors d une
    # recuperation de position (voir ensure_sl_on_hyperliquid) — un vrai
    # filet de catastrophe, pas le SL de gestion quotidienne.
    "RECOVERY_RESCUE_SL_PCT": 15.0,
    # v4.237 — SUR DEMANDE EXPLICITE : marge minimale de confiance qu un
    # nouveau signal doit depasser pour renverser une position existante
    # opposee (Accumulation vs Spot-Accum sur le meme actif) — sinon mis en
    # attente plutot que de fermer systematiquement l existant.
    "CONFLICT_RESOLUTION_MIN_CONFIDENCE_MARGIN": 5.0,

    # Trailing Take Profit (TTP), en % de MOUVEMENT DE PRIX REEL (v4.7) :
    #   - v4.7 — SUR DEMANDE EXPLICITE : contrairement au SL (reste en % de
    #     E, le levier y reduit le mouvement de prix necessaire donc plafonne
    #     la perte $), le TP ne doit PAS etre plafonne par le levier — le
    #     levier doit au contraire pouvoir AMPLIFIER librement le gain
    #     obtenu. Ces seuils sont donc de vrais % de mouvement de PRIX,
    #     identiques quel que soit le levier applique sur ce trade.
    #   - Arme des que le prix bouge de TTP_ARM1_PRICE_PCT (defaut 1.2%)
    #     dans le sens du trade. Seuil de sortie fixe a TTP_LOCK1_PRICE_PCT
    #     (defaut 1.0%) tant que le PIC de mouvement n a pas rejoint
    #     TTP_ARM2_PRICE_PCT.
    #   - Des que le pic de mouvement atteint TTP_ARM2_PRICE_PCT (defaut
    #     1.5%), le seuil de sortie devient pic - TTP_TRAIL_GAP_PRICE_PCT
    #     (defaut 0.3%) et continue de suivre le pic a l infini (trailing
    #     pur, sans plafond).
    "TTP_ARM1_PRICE_PCT":      1.0,
    "TTP_LOCK1_PRICE_PCT":     0.8,
    "TTP_ARM2_PRICE_PCT":      1.3,
    "TTP_TRAIL_GAP_PRICE_PCT": 0.3,
    # v4.60 — SUR DEMANDE EXPLICITE : des l armement (tier 1), le trailing
    # devient IMMEDIATEMENT dynamique (sortie = pic - marge fixe, mis a jour
    # a chaque nouveau pic) au lieu d attendre un second palier (arm2).
    # Applique a Normal/Accumulation/Funding — PAS Spot-Accumulation (deja
    # dynamique par nature). ACTIF par defaut ; repasser a False pour
    # retrouver l ancien comportement (verrou fixe a lock1 avant arm2).
    "TTP_DYNAMIC_FROM_ARM1": True,
    "TTP_DYNAMIC_TRAIL_GAP_PCT": 0.5,
    # v4.77 — SUR DEMANDE EXPLICITE : avant de fermer via le trailing, verifie
    # si la tendance de fond (EMA200) tient toujours — si oui, un simple
    # repli de la marge (0.5% par defaut) depuis le pic ne suffit pas a
    # justifier une sortie, la position est maintenue. ACTIF par defaut.
    # Applique a Normal/Accumulation/Funding (pas Spot-Accum, deja separe).
    "TTP_TREND_HOLD_FILTER_ENABLED": True,
    # v4.84 — SUR DEMANDE EXPLICITE : plafond de redonnage maximum, en % du
    # pic, applique MEME si la tendance est intacte — sans ca, un trade
    # pouvait redonner l essentiel de son pic (observe : LINK 74%, DOGE
    # 57.6%) sans jamais fermer tant que le prix restait du bon cote de
    # l EMA200. Ferme quand meme si le PnL retombe sous 20% du pic (peak *
    # 0.80), peu importe la tendance. Applique aux 3 mecanismes qui
    # partagent le filtre de tendance (tier0, tier1, Spot-Accum).
    "TTP_MAX_GIVEBACK_PCT_OF_PEAK": 20.0,
    # v4.85 — SUR DEMANDE EXPLICITE : le plafond de redonnage ci-dessus ne
    # s applique qu au-dela de ce pic minimum — sur un petit pic, ce plafond
    # serait trop serre et irait a l encontre de la protection de tendance.
    "TTP_MAX_GIVEBACK_MIN_PEAK_PCT": 1.0,
    # v4.119 — SUR DEMANDE EXPLICITE : pour les PETITS pics (< seuil
    # ci-dessus), plafond de redonnage a 50%, mais SEULEMENT si le repli
    # est SOUTENU sur plusieurs cycles consecutifs (evite de fermer sur un
    # simple aller-retour ponctuel/bruit, distinct du plafond instantane
    # des gros pics).
    "TTP_SMALL_PEAK_GIVEBACK_PCT": 50.0,
    "TTP_SMALL_PEAK_GIVEBACK_MIN_CYCLES": 5,
    # v4.86 — SUR DEMANDE EXPLICITE : SECOND plafond, plus large, applique
    # LUI sous le pic minimum ci-dessus — sans ca, un petit gain pouvait
    # techniquement repasser en perte et rester ouvert indefiniment tant
    # que la tendance ne cassait pas franchement.
    "TTP_MAX_GIVEBACK_PCT_SMALL_PEAK": 50.0,
    # v4.67 — SUR DEMANDE EXPLICITE : surcharges par mode du gap dynamique
    # (None = herite du reglage global ci-dessus, aucun changement de
    # comportement tant que rien n est personnalise).
    "ACCUMULATION_TTP_DYNAMIC_TRAIL_GAP_PCT": None,
    "FUNDING_TTP_DYNAMIC_TRAIL_GAP_PCT": None,

    # v4.11 — Protection anticipee ("tier 0"), sur demande explicite : les
    # trades n atteignant JAMAIS TTP_ARM1_PRICE_PCT (1.0% par defaut)
    # n avaient jusqu ici AUCUNE protection — un trade monte a +0.99% pouvait
    # rendre tout son gain (et plus, jusqu au SL) sans jamais rien capturer.
    # Des que le prix atteint TTP_TIER0_ARM_PRICE_PCT (0.5%), un trailing
    # s arme avec une marge plus large (TTP_TIER0_GAP_PRICE_PCT, 0.42%) —
    # ex: pic a 0.99% -> sortie a 0.57%, capture plus de la moitie du pic
    # au lieu de zero. Ce tier 0 se desactive des que le prix atteint le
    # seuil d armement principal (arm1, 1.0%) — le trailing principal, plus
    # fin, prend alors le relai. Il peut en theorie se reactiver si le prix
    # repasse sous ce seuil de 0.5% pendant que tier 1 est actif — dans les
    # faits, avec TTP_LOCK1_PRICE_PCT (0.8%) superieur a ce seuil, tier 1
    # ferme toujours le trade avant que ca puisse arriver (protection
    # simplement redondante avec les reglages actuels, utile si reconfigures).
    "TTP_TIER0_ARM_PRICE_PCT": 0.5,
    "TTP_TIER0_GAP_PRICE_PCT": 0.42,

    # ── Score de confiance (0-100%) — filtre final avant toute entree ───────
    # Poids relatifs des confirmations optionnelles disponibles pour un signal.
    # Le score est ramene sur 100% du poids REELLEMENT disponible pour ce
    # cycle/symbole (ex: si EMA200 n est pas calculable, son poids est retire
    # du total plutot que compte comme un echec).
    # Valeurs par defaut raisonnables — a ajuster selon les resultats observes.
    "CONFIDENCE_WEIGHTS": {
        "macd":      15,   # MACD aligne avec la direction du signal
        "bollinger": 15,   # Prix du bon cote de la bande de Bollinger
        "volume":    10,   # Volume superieur a la moyenne recente
        "ema200":    15,   # Alignement avec la tendance longue (EMA200)
        "ema_mid":   10,   # Alignement avec la tendance intermediaire (25-50 min)
        "momentum":  10,   # Momentum instantane franchement dans le sens du signal
        "consec":    10,   # Cycles consecutifs au-dela du minimum requis (conviction)
        "breakout":  15,   # v3.2 — Casse une resistance/support recent (comportement de trader)
    },
    "CONFIDENCE_MIN_PCT":  65.0,  # Seuil minimum pour prendre un trade
    "CONFIDENCE_STEP_PCT": 5.0,   # Ajustement du seuil par actif a chaque perte/gain
    "CONFIDENCE_MAX_PCT":  87.0,  # Plafond du seuil dynamique (evite de bloquer un actif a vie)
    # v3.2 — Decroissance automatique : si un actif reste penalise sans avoir
    # eu l occasion de regagner sa confiance (perte, puis plus aucun signal
    # qualifiant faute d atteindre le seuil releve) pendant ce delai, son
    # seuil redescend automatiquement a la base — evite un blocage permanent.
    # 0 ou negatif = decroissance desactivee (comportement d avant).
    "CONFIDENCE_RESET_HOURS": 2.0,

    # v3.2 — Auto-activation d un actif INACTIF (pas dans ACTIVE_COINS) si
    # une opportunite exceptionnelle est detectee dessus (confiance >= ce
    # seuil). Permet de profiter d une belle opportunite sur l un des 30
    # marches suivis sans devoir l activer manuellement a l avance.
    "AUTO_ACTIVATE_CONFIDENCE_PCT": 80.0,

    # v3.2 — Prudence live : session de trading limitee a 23h45 sur chaque
    # periode de 24h. Passe ce delai, plus aucune NOUVELLE entree n est
    # ouverte tant que TOUTES les positions de la session ne sont pas
    # fermees (normalement ou manuellement) — une fois toutes fermees, une
    # nouvelle session de 24h redemarre immediatement. Les positions deja
    # ouvertes continuent d etre gerees normalement (SL/Quick Profit/
    # Trailing) pendant cette periode de blocage.
    # v3.2 — SESSION_MAX_HOURS retire : le decoupage se fait desormais sur
    # le jour calendaire UTC fixe (00h00-23h59:59), sans aucun blocage des
    # nouvelles entrees en fin de journee. Voir api.py pour l attribution
    # des statistiques par jour d OUVERTURE (pas de fermeture).

    # v3.2 — Zones RSI extremes : evite d entrer a contre-sens dans une zone
    # de retournement violent probable (survente/surachat extreme). Ne
    # bloque QUE les nouvelles entrees dans le sens "continuation" quand le
    # RSI est deja tres extreme.
    "RSI_EXTREME_LOW": 15,   # ne pas SHORT si RSI < ce seuil (survente extreme)
    "RSI_EXTREME_HIGH": 85,  # ne pas LONG si RSI > ce seuil (surachat extreme)

    # ── Heures creuses crypto — nouvelles entrees suspendues (PAXG exclu, ─────
    #    deja gere par FOREX_SYMBOLS/is_forex_open) ───────────────────────────
    # Fenetre par defaut : 02h-06h UTC, periode de liquidite generalement la
    # plus faible sur les marches crypto. A ajuster si besoin.
    "CRYPTO_OFFPEAK_HOUR_START_UTC": 21,
    "CRYPTO_OFFPEAK_HOUR_END_UTC":   23,

    # ── Blackout CPI (annonces US) via calendrier economique Finnhub ─────────
    # Cle chargeable depuis la variable d environnement HYPERBOT_FINNHUB_API_KEY.
    # Bloque uniquement les NOUVELLES entrees crypto autour de l heure de
    # publication du CPI (le PAXG est deja couvert par la fermeture Forex).
    "FINNHUB_API_KEY":         "",
    "CPI_BLACKOUT_BEFORE_MIN": 15,   # minutes avant l annonce
    "CPI_BLACKOUT_AFTER_MIN":  30,   # minutes apres l annonce
    "CPI_CACHE_REFRESH_HOURS": 12,   # frequence de rafraichissement du calendrier
}

# ─────────────────────────────────────────────
#  SECURITE — CLE PRIVEE / WALLET DEPUIS L ENVIRONNEMENT
# ─────────────────────────────────────────────
# Priorite aux variables d environnement HYPERBOT_PRIVATE_KEY /
# HYPERBOT_WALLET_ADDRESS pour eviter de stocker un secret en clair dans ce
# fichier (risque si le fichier est partage, versionne ou sauvegarde dans le
# cloud). Si absentes, on retombe sur les valeurs codees en dur ci-dessus
# (deconseille en usage reel).
# Exemple avant lancement (Windows PowerShell) :
#   $env:HYPERBOT_PRIVATE_KEY   = "0x..."
#   $env:HYPERBOT_WALLET_ADDRESS = "0x..."
# Exemple avant lancement (Linux / macOS) :
#   export HYPERBOT_PRIVATE_KEY=0x...
#   export HYPERBOT_WALLET_ADDRESS=0x...
import os as _os_env

def _clean_hex_secret(value):
    """v4.4 — Nettoie une cle privee / adresse wallet collee depuis
    l environnement ou l interface : espaces/tabulations/retours a la ligne
    en trop (tres frequent lors d un copier-coller dans les variables
    Railway) et guillemets englobants accidentels. Ne touche PAS au
    contenu hexadecimal lui-meme."""
    if not value:
        return value
    v = value.strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
        v = v[1:-1].strip()
    return v

CONFIG["PRIVATE_KEY"]    = _clean_hex_secret(_os_env.environ.get("HYPERBOT_PRIVATE_KEY", CONFIG["PRIVATE_KEY"]))
CONFIG["WALLET_ADDRESS"] = _clean_hex_secret(_os_env.environ.get("HYPERBOT_WALLET_ADDRESS", CONFIG["WALLET_ADDRESS"]))
CONFIG["FINNHUB_API_KEY"] = _os_env.environ.get("HYPERBOT_FINNHUB_API_KEY", CONFIG["FINNHUB_API_KEY"])

# ─────────────────────────────────────────────
#  PROFILS SWING / SCALP
# ─────────────────────────────────────────────
PROFILE_SWING = {
    "PROFILE":                  "swing",
    # v3.2 — REGLES GENERALISEES A TOUS LES CRYPTOS : plus aucun traitement
    # special par symbole (RSI, EMA, ATR, pivot, EMA200, cycles consecutifs).
    # Tous les actifs (BTC, ETH, SOL, BNB, HYPE, PAXG et les 24 autres)
    # utilisent exactement les memes seuils globaux ci-dessous.
    # SEULE EXCEPTION : PAXG (l or) suit en plus les heures de fermeture du
    # Forex (voir FOREX_SYMBOLS dans CONFIG, gere independamment de ce profil)
    # — c est la seule difference de traitement qui subsiste pour l or.
    "RSI_OVERSOLD":             32,
    "RSI_OVERBOUGHT":           68,
    "SYMBOL_RSI_OVERSOLD":      {},
    "SYMBOL_RSI_OVERBOUGHT":    {},
    # Mode RSI unique pour tous : "trend" (entre dans le sens du momentum,
    # RSI>50=LONG, RSI<50=SHORT) — auparavant reserve a BTC/ETH/SOL/BNB/HYPE,
    # desormais le comportement par defaut pour tout le monde (voir le
    # fallback "trend" dans _process, plus "reversal").
    "SYMBOL_RSI_MODE":          {},
    "EMA_SHORT":                12,
    "EMA_LONG":                 26,
    "SYMBOL_EMA_SHORT":         {},
    "SYMBOL_EMA_LONG":          {},
    # EMA intermediaire — filtre de tendance 25-50 min, applique desormais a
    # TOUS les actifs de la meme facon via EMA_MID_PERIOD (plus de dict par
    # symbole). v3.2 — recalibre pour representer une VRAIE fenetre de
    # 25-50 min avec CYCLE_INTERVAL=10s (200 cycles x 10s = ~33 min,
    # milieu de la fourchette). La collecte initiale plus longue qui en
    # decoule est compensee par la reprise rapide (persistance <10 min).
    "EMA_MID_PERIOD":           200,
    "SYMBOL_EMA_MID":           {},
    "STOP_LOSS_PCT":            1.5,
    "TAKE_PROFIT_PCT":          1.5,
    "SYMBOL_SL_PCT":            {},
    "SYMBOL_TP_PCT":            {},
    "TRAILING_STOP":            True,
    "TRAILING_DELTA_PCT":       0.6,
    "SYMBOL_TRAILING_DELTA_PCT":{},
    "TRAILING_TP":              True,
    "TRAILING_TP_STEP_PCT":     0.8,
    "TRAILING_TP_RSI_EXIT":     55,
    "TRAILING_TP_MIN_SIGNALS":  2,
    "TRAILING_TP_PROTECT_PCT":  0.97,
    "TRAILING_SL_MIN_MOVE_PCT": 0.1,
    "SYMBOL_REQUIRE_MACD_BB":   [],
    "SYMBOL_REQUIRE_EMA200":    [],
    "PIVOT_CONFIRM_SYMBOLS":    [],
    "CONSEC_CONFIRM_SYMBOLS":   {},
    "VOLUME_MIN_RATIO":         1.2,
    "FOREX_WARMUP_MINUTES":     15,
    # Filtre ATR en swing — bloque les entrees sur marche trop calme.
    # Seuil global unique pour tous les actifs.
    # v4.57 — SUR DEMANDE EXPLICITE : DESACTIVE — ce filtre s executait
    # AVANT les appels a Accumulation/Spot-Accumulation dans _process,
    # donc un blocage bloquait TOUS les modes simultanement, pas
    # seulement le mode normal (confirme : BTC bloque ici pendant une
    # vraie tendance baissiere lente de -4.3% sur 12h, empechant aussi
    # Accumulation SHORT de s evaluer). Fait desormais doublon avec
    # REQUIRE_AMPLITUDE_COHERENCE, plus rigoureux (relatif au SL de
    # chaque trade, pas un seuil fixe jamais recalibre pour le nouveau
    # calcul haut/bas).
    "ATR_FILTER":               False,
    "ATR_PERIOD":               21,   # v3.2 — recalibre (14->21) pour preserver ~3min30 reelles avec le cycle a 10s (etait calibre pour 15s)
    "ATR_MIN_PCT":              0.015,
    "ATR_MIN_PCT_BY_SYMBOL":    {},

    # v3.2 — Detection automatique du mode Trend/Reversal via l ADX (force
    # de la tendance), par actif, a chaque cycle. ADX >= seuil -> mode
    # "trend" (suivi de tendance). ADX < seuil -> mode "reversal" (parie
    # sur un retournement en marche sans direction nette). SYMBOL_RSI_MODE
    # reste disponible pour forcer manuellement un mode fixe sur un actif
    # precis, en priorite sur cette detection automatique.
    "ADX_PERIOD":               14,
    "ADX_TREND_THRESHOLD":      25.0,
    # v4.115 — SUR DEMANDE EXPLICITE : seuil ADX DEDIE a Accumulation,
    # distinct de ADX_TREND_THRESHOLD ci-dessus (mode normal, inchange a
    # 25) — observe qu aucun actif sur un lot de 30 ne depassait jamais 25,
    # bloquant Accumulation en continu. Abaisse a 20 specifiquement pour ce
    # mode, sans affecter la qualite du mode normal recemment recalibree.
    "ACCUMULATION_ADX_TREND_THRESHOLD": 20.0,
    # v4.132 — SUR DEMANDE EXPLICITE : S/R sur 24h (720 bougies ~2min),
    # distinct de la fenetre plus courte du mode normal (SR_PERIOD_CANDLES,
    # ~3h20 par defaut) — repli automatique sur le S/R partage tant que
    # candle_history n a pas encore 720 bougies accumulees.
    "ACCUMULATION_SR_PERIOD_CANDLES": 720,
    # v4.133 — SUR DEMANDE EXPLICITE : amplitude de reference (pour la
    # fenetre de proximite 5-10%) basee sur 4h (120 bougies ~2min),
    # distincte du S/R structurel sur 24h ci-dessus.
    "ACCUMULATION_AMPLITUDE_PERIOD_CANDLES": 120,
    # v4.146 — SUR DEMANDE EXPLICITE : exige que l amplitude S/R (24h) soit
    # au moins ce multiple de l ATR (volatilite recente) — garantit une
    # vraie zone structurelle, pas juste du bruit de marche amplifie.
    "ACCUMULATION_REQUIRE_AMPLITUDE_VS_ATR": True,
    "ACCUMULATION_MIN_AMPLITUDE_TO_ATR_RATIO": 3.0,
    # v4.147 — SUR DEMANDE EXPLICITE : periode ATR dediee a ce filtre (4h =
    # 120 bougies ~2min), plus longue que le defaut (14 = 28min) pour ne
    # pas etre faussee par un pic de volatilite ponctuel et recent.
    "ACCUMULATION_AMPLITUDE_ATR_PERIOD": 120,
    # v4.135 — SUR DEMANDE EXPLICITE : confirmation de tendance longue duree
    # (alternative a l EMA200 court terme) — capture les mouvements lents
    # en "escalier" sur plusieurs heures. 180 bougies ~2min = 6h de recul,
    # 2% de changement net minimum pour confirmer.
    "LONG_TERM_MOMENTUM_LOOKBACK_CANDLES": 180,
    "LONG_TERM_MOMENTUM_MIN_CHANGE_PCT": 2.0,
    # v4.148 — SUR DEMANDE EXPLICITE : detecteur de cassure fraiche pour
    # Accumulation — 60 bougies ~2min = 2h de recul pour identifier un
    # nouveau plus haut/plus bas. Confirme la tendance ET contourne la
    # proximite S/R IMMEDIATEMENT des detection, pour une prise de
    # position des la confirmation du mouvement.
    # v4.217 — SUR DEMANDE EXPLICITE : elargi de 30 a 60 (1h -> 2h) — donne
    # plus de chances de capturer une vraie transition accumulation ->
    # mouvement, plutot que de se limiter a la derniere heure seulement.
    "ACCUMULATION_BREAKOUT_LOOKBACK_CANDLES": 60,
    # v4.156 — SUR DEMANDE EXPLICITE : les bougies precedant une cassure
    # doivent former une vraie consolidation (mouvement <= ce %) — evite de
    # declencher sur une simple poursuite de tendance deja fluide.
    # v4.217 — SUR DEMANDE EXPLICITE : assoupli (3.0->4.5% et 0.5->0.65) —
    # renforce le chemin "cassure fraiche" pour mieux capturer la
    # transition accumulation -> mouvement, sans exiger une consolidation
    # parfaitement plate.
    "BREAKOUT_MAX_PRIOR_CONSOLIDATION_PCT": 4.5,
    "BREAKOUT_MAX_CONSOLIDATION_DIRECTIONALITY": 0.65,
    # v4.140 — SUR DEMANDE EXPLICITE : fenetre de tendance "fraiche" pour
    # Accumulation — entre le minimum de cycles requis (12) et cette valeur,
    # la fenetre de proximite S/R est completement ignoree, pour capturer
    # le debut d un retournement plutot que d attendre un rejet a un niveau
    # precis (pertinent seulement pour une tendance deja mature).
    "ACCUMULATION_FRESH_TREND_MAX_CYCLES": 36,
    # v4.117 — SUR DEMANDE EXPLICITE : fenetre de proximite DEDIEE a
    # Accumulation, distincte de UNIFIED_MIN/MAX_ABOVE_SUPPORT_PCT (mode
    # normal, inchange a 5-10%) — elargie a 5-20% suite a l observation que
    # cette fenetre etait devenue le principal facteur bloquant
    # Accumulation une fois l ADX et l amplitude minimale traites.
    # v4.118 — SUR DEMANDE EXPLICITE : revert a 5-10% (identique au mode
    # normal) — l elargissement a 5-20% n etait pas la vraie cause du
    # blocage (confirme : INJ a qualifie "aucun obstacle particulier" avec
    # la fenetre a 5-10%, prouvant qu elle fonctionne aussi pour
    # Accumulation). Le veritable facteur limitant est la RARETE naturelle
    # de la combinaison simultanee de toutes les conditions, pas la largeur
    # de cette fenetre precise. Structure dediee conservee (permet de
    # differencier a nouveau facilement si besoin plus tard).
    "ACCUMULATION_MIN_ABOVE_SUPPORT_PCT": 5.0,
    "ACCUMULATION_MAX_ABOVE_SUPPORT_PCT": 15.0,
    # v4.127 — SUR DEMANDE EXPLICITE : detecteur de range DIRECT — bloque
    # l entree si le prix a bouge de moins de X% sur les N derniers
    # echantillons mtf_prices (~2min chacun). Remplace l amplitude S/R
    # (desactivee) comme outil anti-range, en plus de la fenetre de
    # proximite (conservee, inchangee).
    "ACCUMULATION_ANTI_RANGE_MIN_PCT": 2.0,
    # v4.218 — SUR DEMANDE EXPLICITE : bonus de confiance si le VRAI volume
    # (pas la volatilite proxy) confirme un etat d accumulation genuine
    # pendant une phase de range detectee.
    "ACCUMULATION_VOLUME_CONFIRM_BONUS": 5.0,
    # v4.219 — SUR DEMANDE EXPLICITE : chemin d entree DEDIE, base
    # uniquement sur une forte confirmation de volume pendant une
    # consolidation deja detectee — permet d entrer PENDANT l accumulation,
    # sans attendre une tendance ou une cassure de prix. Applique a
    # Accumulation ET Spot-Accum (miroir). Volontairement plus strict
    # (1.5x) que le bonus de confiance simple (1.15x), puisqu il permet ici
    # de CONTOURNER des exigences, pas seulement de renforcer un score.
    "ACCUMULATION_VOLUME_ENTRY_ENABLED": True,
    "ACCUMULATION_VOLUME_ENTRY_MIN_RATIO": 1.5,
    # v4.221 — SUR DEMANDE EXPLICITE : detection de cassure RATEE (fausse
    # cassure) — signal fort et INDEPENDANT du RSI/MACD, bypass ces
    # exigences quand detecte. Empeche un marche en tendance forte
    # (MACD structurellement dans un seul sens) de bloquer indefiniment ce
    # signal local pourtant clair.
    "FAILED_BREAKOUT_DETECTION_ENABLED": True,
    "FAILED_BREAKOUT_LOOKBACK_CANDLES": 20,
    # v4.221/222/223/224 — SUR DEMANDE EXPLICITE : detection de cassure
    # RATEE (fausse cassure), signal fort et INDEPENDANT du RSI/MACD.
    # 3 garde-fous OBLIGATOIRES (magnitude, recence, anti-repetition) —
    # voir _detect_failed_breakout pour le raisonnement complet sur
    # pourquoi couleur de bougie est retiree (redondante) et volume traite
    # en alternative plutot qu en 4e condition bloquante (evite que 5+
    # garde-fous cumulatifs ne s alignent jamais).
    "FAILED_BREAKOUT_MIN_MAGNITUDE_PCT": 0.3,
    "FAILED_BREAKOUT_RECENCY_CANDLES": 8,
    "FAILED_BREAKOUT_COOLDOWN_SEC": 1800,
    "FAILED_BREAKOUT_STRONG_MAGNITUDE_MULTIPLIER": 2.5,
    "FAILED_BREAKOUT_REQUIRE_VOLUME": True,
    "FAILED_BREAKOUT_VOLUME_MIN_RATIO": 1.3,
    # v4.227 — SUR DEMANDE EXPLICITE : detection d etoile filante (motif de
    # bougie japonaise, signal baissier) — entree short (Forex/Accumulation,
    # bypass EMA200/ADX si confirmee) et sortie de position longue
    # existante (Forex/Spot-Accum), une fois confirmee sur la duree ci-dessous.
    "SHOOTING_STAR_DETECTION_ENABLED": True,
    "SHOOTING_STAR_MIN_UPPER_WICK_RATIO": 2.0,
    "SHOOTING_STAR_MAX_LOWER_WICK_RATIO": 0.3,
    "SHOOTING_STAR_CONFIRM_MINUTES": 30,
    # v4.240 — SUR DEMANDE EXPLICITE : pression directionnelle soutenue,
    # basee sur le VRAI flux de transactions Hyperliquid (recentTrades) —
    # capture une tendance DEJA engagee, loin de tout support/resistance
    # (Forex, Accumulation, Spot-Accum — PAS Funding, retour a la moyenne).
    "TREND_PERSISTENCE_ENABLED": True,
    "TRADE_FLOW_SAMPLE_SIZE": 100,
    "TREND_PERSISTENCE_MIN_SAMPLES": 6,
    "TREND_PERSISTENCE_PRESSURE_THRESHOLD": 0.15,
    # v4.242 — SUR DEMANDE EXPLICITE : 2 garde-fous ajoutes pour reduire le
    # risque de faux signaux — volume notionnel minimal (evite les
    # marches creux) et correlation avec le mouvement de prix reel (evite
    # une "pression" sans reaction reelle du marche).
    "TRADE_FLOW_MIN_NOTIONAL_USD": 5000.0,
    # v4.266 — fenetre de temps FIXE du flux de transactions (avant : les 200
    # dernieres transactions, quelle que soit leur anciennete — quelques
    # secondes sur BTC, parfois 30 min+ sur un altcoin peu actif).
    "TRADE_FLOW_WINDOW_SEC": 180,
    # v4.266 — flux WebSocket juge MORT si AUCUNE transaction (tous actifs
    # confondus) n est recue depuis ce delai -> repli REST automatique.
    "TRADE_FLOW_WS_DEAD_SEC": 120,
    # v4.266 — CONFIRMATION par le flux a l entree (Spot-Accum / Accumulation,
    # entree pres du support/de la resistance) : 0 = desactivee (seul le veto
    # ENTRY_FLOW_CONTRADICTION_THRESHOLD s applique, comportement d origine).
    # > 0 = exige une pression AU MOINS egale dans le sens du trade (ex: 0.1 :
    # achat net >= +0.1 pour Spot-Accum, vente nette <= -0.1 pour Accumulation).
    "ENTRY_FLOW_CONFIRM_MIN_PRESSURE": 0.0,
    # v4.269 — confirmation par le flux PROPRE a chaque mode (None = herite
    # de ENTRY_FLOW_CONFIRM_MIN_PRESSURE ci-dessus).
    "SPOT_ACCUM_ENTRY_FLOW_CONFIRM_MIN": None,
    "ACCUMULATION_ENTRY_FLOW_CONFIRM_MIN": None,
    # v4.269 — plafond de SL immediat PROPRE a chaque mode (None = herite de
    # STRUCTURAL_SL_HARD_CAP_PCT, 0,5 %). Voir la simulation "SL x %" de l
    # export CSV avant de l elargir.
    "SPOT_ACCUM_SL_CAP_PCT": None,
    # v4.270 — patience du SL Spot-Accum pilotee par le flux (voir gestion)
    "SPOT_ACCUM_SL_FLOW_PATIENCE_ENABLED": True,
    "SPOT_ACCUM_SL_PATIENCE_REQUIRE_TREND": 1,  # v4.271 (1 = oui, 0 = non) — exige aussi prix du bon cote de l EMA200
    "SPOT_ACCUM_SL_FLOW_THRESHOLD": 0.2,      # pression acheteuse minimale pour attendre (long)
    "SPOT_ACCUM_SL_FLOW_MAX_PCT": 1.0,        # perte maximale toleree pendant l attente (% de prix)
    "SPOT_ACCUM_SL_FLOW_MAX_WAIT_SEC": 900,   # duree maximale de l attente
    "ACCUMULATION_SL_CAP_PCT": None,
    # v4.269 — delai de re-entree sur le MEME actif apres une PERTE (s ; 0 =
    # desactive). Donnees 18-23/09 : les shorts Accumulation re-ouverts dans
    # l heure suivant une perte sur le meme actif gagnent 28 % du temps
    # (contre 39 %).
    "ACCUMULATION_LOSS_COOLDOWN_SEC": 3600,
    "SPOT_ACCUM_LOSS_COOLDOWN_SEC": 0,
    "FUNDING_LOSS_COOLDOWN_SEC": 0,
    # v4.269 — nombre maximal de NOUVELLES entrees d un meme mode sur une
    # fenetre glissante (0 = illimite). Donnees : la 4e entree Accumulation
    # et au-dela dans une meme fenetre de 10 min gagne 27 % du temps.
    "ACCUMULATION_MAX_ENTRIES_PER_WINDOW": 3,
    "SPOT_ACCUM_MAX_ENTRIES_PER_WINDOW": 0,
    "ENTRY_BURST_WINDOW_SEC": 600,
    # v4.276 — REGIME DE MARCHE (filtre global, sur demande explicite) :
    # Spot-Accum (achats) bloque en marche BAISSIER confirme, Accumulation
    # (shorts) bloque en marche HAUSSIER confirme. "Confirme" = l actif de
    # reference (BTC) est du meme cote de son EMA200 sur l unite de temps
    # choisie, avec EMA50 du meme cote, ET une majorite des actifs suivis
    # (MARKET_REGIME_BREADTH_PCT) est du meme cote de sa propre EMA200.
    "MARKET_REGIME_FILTER_ENABLED": 0,   # v4.327 : SUR DEMANDE EXPLICITE, plus de blocage global par regime de marche — chaque mode juge la tendance de SON actif
    # v4.281 — ANTI-RANGE RELATIF A L ACTIF : mouvement minimal exige =
    # amplitude horaire mediane de l actif (7 j) x ANTI_RANGE_REL_MULT x
    # racine(duree de la fenetre en heures). Les seuils absolus (*_ANTI_RANGE_MIN_PCT)
    # ne servent plus que de repli tant que l habitude n est pas connue.
    # v4.283 — EMA de tendance sur les vraies bougies 5 min (80 x 5 min =
    # ~6 h 40, meme horizon que l ancienne EMA200 sur points de 2 min)
    "TREND_EMA_PERIOD_5M": 80,
    # v4.284 — bougies en temps reel par WebSocket (1 = oui, 0 = REST seul)
    "CANDLE_WS_ENABLED": 1,
    "CANDLE_REST_RESYNC_SEC": 21600,   # resynchronisation de securite par REST (6 h)
    "ANTI_RANGE_RELATIVE_ENABLED": 1,
    "ANTI_RANGE_REL_MULT": 0.6,
    # v4.281 — QUALITE DU MARCHE (0 = desactive, en attente de calibrage)
    "MARKET_QUALITY_MIN_VOL_RATIO": 0.0,        # ex. 0.5 : pas d entree si l amplitude < 50 % de l habitude
    "MARKET_QUALITY_MIN_ACTIVITY_RATIO": 0.0,   # ex. 0.3 : pas d entree si le volume < 30 % de l habitude
    "SPOT_ACCUM_MIN_FLOW_CONVICTION": 0.0,      # ex. 0.1 : pas d entree si |pression du flux| < 0.1
    "ACCUMULATION_MIN_FLOW_CONVICTION": 0.0,
    "FUNDING_MIN_FLOW_CONVICTION": 0.0,
    "FOREX_MIN_FLOW_CONVICTION": 0.0,
    # v4.282 — spread maximal accepte a l entree (% du prix, 0 = desactive)
    "MARKET_QUALITY_MAX_SPREAD_PCT": 0.0,
    "MARKET_QUALITY_SPREAD_CACHE_SEC": 30,
    # v4.277 — les entrees par cassure/rebond/tendance persistante doivent
    # respecter le SENS de la tendance (EMA200) et le veto du flux (1 = oui)
    "SPOT_ACCUM_BYPASS_REQUIRE_TREND": 1,
    # v4.279 — la cassure fraiche peut entrer contre l EMA200 si le flux
    # confirme franchement et que le regime n est pas oppose
    "SPOT_ACCUM_FRESH_BREAKOUT_COUNTER_TREND": 0,   # v4.287 : desactivee avec les autres contre-tendances
    "ACCUMULATION_FRESH_BREAKOUT_COUNTER_TREND": 0,
    "FRESH_BREAKOUT_COUNTER_TREND_MIN_FLOW": 0.2,
    # v4.278 — achat sur repli : support ascendant (dernier creux plus haut, 1h)
    "SPOT_ACCUM_RISING_SUPPORT_ENABLED": 1,
    "SPOT_ACCUM_PIVOT_CANDLES": 2,              # bougies 1h de chaque cote pour valider un creux
    "SPOT_ACCUM_PIVOT_LOOKBACK_CANDLES": 72,    # historique 1h examine (3 jours)
    "SPOT_ACCUM_BYPASS_FLOW_VETO": 1,
    "ACCUMULATION_BYPASS_REQUIRE_TREND": 1,
    "ACCUMULATION_BYPASS_FLOW_VETO": 1,
    "MARKET_REGIME_REF_TICKER": "BTC",
    "MARKET_REGIME_TIMEFRAME": "1h",
    "MARKET_REGIME_BREADTH_PCT": 60,
    "MARKET_REGIME_REFRESH_SEC": 300,
    "MARKET_REGIME_MIN_DIST_PCT": 0.2,
    "MARKET_REGIME_MIN_ASSETS": 8,           # v4.280 — nombre minimal d actifs mesures pour la largeur de marche
    # v4.276 — PLAGE HORAIRE d entree PAR MODE (heures UTC, debut inclus,
    # fin exclue, passage de minuit gere ; 0-24 = toujours).
    "SPOT_ACCUM_TRADE_HOUR_START_UTC": 0,
    "SPOT_ACCUM_TRADE_HOUR_END_UTC": 24,
    "ACCUMULATION_TRADE_HOUR_START_UTC": 0,
    "ACCUMULATION_TRADE_HOUR_END_UTC": 24,
    "FUNDING_TRADE_HOUR_START_UTC": 0,
    "FUNDING_TRADE_HOUR_END_UTC": 24,
    # v4.286 — SITUATIONS DE MARCHE (fond 1h x court terme 5 min)
    # v4.289 — moteur d entree : "simple" (garde-fous + 1 signal + 1
    # confirmation) ou "legacy" (ancienne chaine de conditions)
    # v4.299 — MOTEUR "TOP-DOWN" MULTI-UNITES DE TEMPS (Spot-Accum = achats,
    # Accumulation = ventes) : tendance et zones cles sur l unite MAJEURE,
    # signal de bougie sur l unite INFERIEURE, SL au-dela de la zone, objectif
    # sur la zone opposee, taille calculee sur le risque.
    "ENTRY_ENGINE_MTF": 1,
    "MTF_MAJOR_TF": "4h",              # "4h" ou "1d"
    "MTF_LOWER_TF": "15m",             # "15m" ou "1h"
    "MTF_USE_DAILY": 0,                # 1 = unite majeure Daily au lieu de H4
    "MTF_USE_H1": 0,                   # 1 = unite inferieure H1 au lieu de M15
    "MTF_ZONE_MAX_WIDTH_ATR": 0,       # v4.357 : largeur maximale d une zone en ATR de l unite majeure (0 = sans limite, comportement d origine)
    "MTF_USE_H4_LOWER": 0,             # v4.352 : 1 = signal sur bougies H4 (profil Swing, avec MTF_USE_DAILY=1)
    "MTF_SIGNAL_MAX_AGE_SEC": 0,       # v4.352 : > 0 = entree seulement dans les N s qui suivent la cloture de la bougie du signal
    "MTF_DOUBLE_PATTERN_ENABLED": 0,   # v4.352 : 1 = ajoute double creux / double sommet aux motifs de bougie
    "MTF_ZONE_LOOKBACK": 120,          # bougies majeures examinees pour les zones
    "MTF_ZONE_TOLERANCE_ATR": 0.25,    # marge autour d une zone (x ATR majeur)
    "MTF_MIN_RR": 1.5,                 # rapport gain/risque minimal
    "MTF_RISK_PCT": 0.5,               # risque par trade, % du capital
    "MTF_MAX_NOTIONAL_USD": 30.0,      # notionnel maximal par trade
    "MTF_MAX_RISK_PCT": 4.0,           # distance maximale au SL (% du prix)
    "MTF_BREAKEVEN_AT_R": 1.0,         # SL remonte au prix d entree a +1R
    "MTF_TRAIL_FROM_R": 2.0,           # au-dela de +2R, SL suiveur a 1R du plus haut
    # v4.304 — SUR DEMANDE EXPLICITE : deux confirmations SUPPLEMENTAIRES,
    # exigees en plus du signal de bougie M15/H1 existant, pour filtrer les
    # faux rebonds observes sur le lot du 26/09 (7 trades, 7 SL, tous des
    # LONG altcoins pris dans un marche global baissier) :
    #   1) STRUCTURE H1 : la bougie de retournement (M15 ou H1 selon
    #      MTF_LOWER_TF) ne suffit plus seule — exige aussi que l EMA courte
    #      H1 (MTF_H1_STRUCTURE_EMA_PERIOD) soit orientee dans le sens du
    #      trade, preuve d un vrai changement de structure au-dela d une
    #      seule bougie.
    #   2) FLUX DE TRANSACTIONS : exige que la pression achat/vente reelle
    #      (_compute_trade_flow_pressure, deja utilisee ailleurs pour la
    #      tolerance du trailing) ne soit pas hostile au sens du trade —
    #      filtre les rebonds sur bougie haussiere alors que le flux reel
    #      reste vendeur (piege classique en marche baissier).
    "MTF_REQUIRE_H1_STRUCTURE": 1,      # 1 = exige la confirmation de structure H1
    "MTF_H1_STRUCTURE_EMA_PERIOD": 20,  # periode de l EMA H1 dont la pente est verifiee
    "MTF_REQUIRE_FLOW_CONFIRM": 1,      # 1 = exige la confirmation du flux de transactions
    "SPOT_ACCUM_TREND_MODE": 0,          # v4.348 : tendance H4 exigee pour l ENTREE — 0 = stricte ou repli (defaut), 1 = tant que la structure n est pas OPPOSEE, 2 = aucune
    "ACCUMULATION_TREND_MODE": 0,        # v4.348 : idem pour Accumulation (short) — 2 = vendre les hausses a une resistance H4 meme en tendance haussiere
    "MTF_FLOW_MISSING_BLOCKS": 0,       # v4.328 : 1 = bloque quand le flux est indisponible (ancien comportement) ; 0 = laisse passer (score neutre)
    "MTF_MIN_FLOW_PRESSURE": 0.0,       # seuil minimal (0 = juste "pas hostile" ; 0.15-0.3 = plus strict)
    "MTF_REQUIRE_REAL_TARGET_ZONE": 1,  # v4.308 — 1 = bloque l entree quand il n y a pas de zone opposee distincte (pas d objectif synthetique)
    "MTF_CLOSE_ON_ZONE_BREAK": 1,       # v4.309 — 1 = ferme des que le prix casse la zone d entree (avant meme le SL classique)
    "MTF_CLOSE_ON_TREND_REVERSAL": 1,   # v4.311 — 1 = ferme des que la tendance H4 d origine se retourne completement
    # v4.321 — SUR DEMANDE EXPLICITE : chemin d entree "tendance" (support
    # ascendant / resistance descendante + objectif par mesure de
    # mouvement), pour capter une tendance qui s eloigne de toute zone H4 —
    # OU distinct du chemin "zone" classique, pas une condition en plus.
    # ── v4.353 — SCALP FOREX (sous-mode du Forex, PAPER uniquement ; le mode Forex actuel n est pas modifie) ──
    "FOREX_SCALP_ENABLED": 0,             # 1 = active le scalp (eteint par defaut)
    "FOREX_SCALP_SYMBOLS": ["xyz:EUR", "xyz:JPY", "xyz:KRW", "PAXG"],
    "FOREX_SCALP_NOTIONAL_USD": 30.0,     # notionnel par trade (paper)
    "FOREX_SCALP_LEVERAGE": 3,            # levier simule (marge = notionnel / levier)
    "FOREX_SCALP_MAX_TRADES": 2,          # scalps simultanes au plus
    "FOREX_SCALP_MAX_HOLD_MIN": 20,       # sortie au temps
    "FOREX_SCALP_COOLDOWN_SEC": 300,      # pause sur un actif apres une perte
    "FOREX_SCALP_DAILY_LOSS_USD": 1.0,    # perte paper journaliere au-dela de laquelle le scalp s arrete pour la journee
    "FOREX_SCALP_FEE_PCT": 0.089,         # frais aller-retour supposes (mettre ~0.03 pour modeliser des ordres limite)
    "FOREX_SCALP_SLIPPAGE_PCT": 0.005,    # glissement defavorable par jambe (en plus du demi-spread)
    "FOREX_SCALP_MIN_NET_RR": 0.3,        # gain/risque NET de frais et spread minimal
    "FOREX_SCALP_MIN_TARGET_PCT": 0.15,   # objectif minimal (% du prix)
    "FOREX_SCALP_MIN_RISK_PCT": 0.06,     # stop minimal (% du prix)
    "FOREX_SCALP_MAX_RISK_PCT": 0.30,     # stop maximal (% du prix)
    "FOREX_SCALP_RR": 1.2,                # gain/risque de l objectif synthetique
    "FOREX_SCALP_SL_ATR": 0.5,            # marge du stop au-dela du niveau (x ATR 5 min)
    "FOREX_SCALP_TOUCH_ATR": 0.6,         # tolerance de contact avec un niveau (x ATR 5 min)
    "FOREX_SCALP_MAX_SIGNAL_AGE_SEC": 150,   # le rejet 5 min doit dater de moins de N secondes
    "FOREX_SCALP_REQUIRE_1M": 1,          # exige la confirmation de la bougie 1 min
    "FOREX_SCALP_COUNTER_BIAS_MIN_W": 2,  # rejet contre le biais 15 min : niveau de confluence >= N
    "FOREX_SCALP_BE_R": 0.6,              # breakeven net de frais apres N x R
    "FOREX_SCALP_FLOW_EXIT": 0.5,         # sortie si le flux se retourne au-dela de +/- N
    "FOREX_SCALP_FLOW_EXIT_MIN_GAIN_R": 0.3,
    "FOREX_SCALP_STALE_ENABLED": 1,       # v4.371 : sortie "sans progres" (1/0)
    "FOREX_SCALP_STALE_MIN": 10,          # ... apres N minutes
    "FOREX_SCALP_STALE_PEAK_PCT": 0.05,   # ... si le pic de gain est reste sous N % du prix
    "FOREX_SCALP_EARLYFAIL_ENABLED": 1,   # v4.371 : sortie "echec precoce" (1/0)
    "FOREX_SCALP_EARLYFAIL_RISK_FRAC": 0.5,  # ... quand la perte atteint cette fraction du stop
    "FOREX_SCALP_EARLYFAIL_PEAK_PCT": 0.03,  # ... et que le pic de gain est reste sous N % du prix
    # ── v4.361 — SWING FOREX (sous-mode, PAPER uniquement ; eteint par defaut) ──
    "FXSWING_ENABLED": 0,
    "FXSWING_SYMBOLS": ["xyz:EUR", "xyz:JPY", "xyz:KRW", "xyz:DXY", "PAXG"],
    "FXSWING_RISK_PCT": 0.5,              # perte au stop = N % du capital
    "FXSWING_MAX_NOTIONAL_USD": 60.0,     # notionnel maximal par trade (paper)
    "FXSWING_LEVERAGE": 3,
    "FXSWING_MAX_TRADES": 2,
    "FXSWING_MIN_RISK_PCT": 0.25,         # stop minimal (% du prix) : en dessous, frais et spread le mangent
    "FXSWING_MAX_RISK_PCT": 2.0,
    "FXSWING_MIN_RR": 2.0,                # gain/risque brut minimal
    "FXSWING_MIN_NET_RR": 1.2,            # gain/risque NET de frais et spread minimal
    "FXSWING_SIGNAL_MAX_AGE_SEC": 1800,   # entree dans les N secondes qui suivent la cloture H4
    "FXSWING_MAX_SPREAD_PCT": 0.05,
    "FXSWING_MAX_HOLD_DAYS": 10,
    "FXSWING_COOLDOWN_SEC": 14400,
    "FXSWING_DOUBLE_PATTERN": 1,
    "FXSWING_FLOW_VETO": 1,
    # ── v4.362 — SWING SUPPORT (crypto, PAPER uniquement ; eteint par defaut) ──
    # Hypothese : tendance Daily haussiere + support (retest tenu sur les dernieres bougies 1 h, ou balayage de liquidite) -> objectif
    # proche ; stop large = simple filet ; la protection est la SORTIE ANTICIPEE (cloture 1 h sous la zone + flux hostile).
    # BTC / Weekly / funding sont ENREGISTRES a l entree mais ne filtrent rien. Aucun breakeven : on mesure l hypothese pure.
    # ── v4.365 — TENDANCE CONFIRMEE (module separe, PAPER uniquement ; eteint par defaut) ──
    # Entree quand Daily ET H4 sont dans le meme sens (+ momentum 1 h et flux), longs et shorts ; on reste tant que la tendance tient
    # (stop suiveur ATR H4, sortie quand la tendance cesse). Le retard de l entree est enregistre (ATR Daily) pour mesurer s il coute.
    "TRENDF_ENABLED": 0,
    "TRENDF_SYMBOLS": ["BTC", "ETH", "HYPE", "TAO", "SUI", "PAXG", "xyz:EUR", "xyz:JPY"],
    "TRENDF_ALLOW_SHORT": 1,
    "TRENDF_RISK_PCT": 1.0,               # perte au stop initial = N % du capital (dimensionne la taille)
    "TRENDF_MAX_NOTIONAL_USD": 60.0,
    "TRENDF_LEVERAGE": 3,
    "TRENDF_MAX_TRADES": 3,
    "TRENDF_STOP_ATR": 3.0,               # distance du stop suiveur = N x ATR H4
    "TRENDF_MIN_STOP_PCT": 0.8,
    "TRENDF_MAX_STOP_PCT": 6.0,
    "TRENDF_ENTRY_MODE": 0,               # 0 = entre des que la tendance est confirmee (momentum + flux) ; 1 = exige en plus une cassure
    "TRENDF_BREAK_BARS": 12,              # mode 1 : cassure du plus haut/bas des N dernieres heures
    "TRENDF_MIN_FLOW": 0.2,               # flux de transactions minimal dans le sens du trade (absent = on laisse passer)
    "TRENDF_MAX_EXT_ATR": 0.0,            # 0 = pas de limite ; sinon refuse si le prix est a plus de N ATR Daily de sa moyenne (entree tardive)
    "TRENDF_MAX_SPREAD_PCT": 0.05,
    "TRENDF_MAX_HOLD_DAYS": 30,
    "TRENDF_COOLDOWN_SEC": 14400,
    "TRENDF_EXIT_H4_REVERSE": 1,          # 1 = sort aussi quand la tendance H4 se retourne franchement
    "SUPSW_ENABLED": 0,
    "SUPSW_SYMBOLS": ["BTC", "ETH", "HYPE", "TAO", "SUI"],
    "SUPSW_SL_PCT": 5.0,                  # stop catastrophe (% du prix)
    "SUPSW_TP_PCT": 2.0,                  # objectif (% du prix)
    "SUPSW_RISK_PCT": 1.0,                # perte au stop plein = N % du capital (dimensionne la taille)
    "SUPSW_MAX_NOTIONAL_USD": 60.0,
    "SUPSW_LEVERAGE": 3,
    "SUPSW_MAX_TRADES": 3,
    "SUPSW_WINDOW": 5,                    # nombre de bougies 1 h examinees
    "SUPSW_SIGNAL_MAX_AGE_SEC": 900,      # entree dans les N secondes qui suivent la cloture de la bougie 1 h du signal
    "SUPSW_MAX_SPREAD_PCT": 0.05,
    "SUPSW_MAX_HOLD_DAYS": 5,
    "SUPSW_COOLDOWN_SEC": 14400,
    "SUPSW_TRAIL_ON": 1,                  # v4.389 : 1 = a l objectif on ne ferme pas : on verrouille un gain et on laisse courir (trailing)
    "SUPSW_TRAIL_LOCK_PCT": 0.5,          # ... le stop passe immediatement a +N % au-dessus de l entree (gain verrouille)
    "SUPSW_TRAIL_PCT": 0.5,               # ... puis suit le plus haut atteint a N % en dessous
    "SUPSW_BTC_GATE_ON": 1,               # v4.392 : porte de regime : pas d entree si BTC est en baisse (24 h / tendance H4)
    "SUPSW_BTC24_MIN": 0.0,               # ... refuse si la variation de BTC sur 24 h est inferieure a N % (0 = BTC doit etre au moins stable)
    "SUPSW_BTC_H4_MODE": 2,               # ... tendance H4 de BTC : 0 = ignoree, 1 = refuse si baissiere, 2 = exige haussiere
    "SUPSW_RECENT_ON": 1,                 # v4.392 : controle des dernieres heures : pas d entree pendant une chute en cours
    "SUPSW_RECENT_HOURS": 4,              # ... fenetre en heures
    "SUPSW_RECENT_ASSET_MIN_PCT": 1.5,    # ... refuse si l actif a perdu plus de N % sur la fenetre
    "SUPSW_RECENT_BTC_MIN_PCT": 0.5,      # ... refuse si BTC a perdu plus de N % sur la fenetre
    "SUPSW_RES_FILTER_ON": 1,             # v4.391 : 1 = pas d entree si une resistance Daily est trop proche au-dessus (retournement probable)
    "SUPSW_RES_MIN_PCT": 3.0,             # ... distance minimale (% du prix) entre le prix et le bas de la resistance Daily
    "SUPSW_RIDE_ON": 1,                   # v4.391 : 1 = on vise la resistance : objectif = resistance - tampon (au lieu de l objectif fixe)
    "SUPSW_RES_BUFFER_PCT": 0.3,          # ... tampon sous la resistance (% du prix)
    "SUPSW_RIDE_MAX_PCT": 6.0,            # ... objectif maximal (% du prix)
    "SUPSW_RIDE_TRAIL_PCT": 1.0,          # ... pendant la montee vers la resistance, le stop suit le plus haut a N % (large, pour ne pas etre sorti par le bruit)
    "SUPSW_REENTRY_ON": 1,                # v4.389 : apres un trade gagnant sur un actif, controle avant de reprendre
    "SUPSW_REENTRY_HOURS": 12,            # ... valable N heures apres la sortie gagnante
    "SUPSW_REENTRY_TOL_PCT": 0.2,         # ... prix >= prix de sortie - N % = ca continue de monter : on reprend ; sinon on attend la fin de la baisse
    "SUPSW_EXIT_BUFFER_ATR": 0.3,         # cloture 1 h sous (bas de zone - N x ATR 1 h) = support casse
    "SUPSW_EXIT_FLOW": -0.4,              # ... ET flux de transactions inferieur a N
    "SUPSW_EXIT_LEVEL": 1,                # v4.363 : 1 = niveau de sortie = plus bas du retest (borne ci-dessous) ; 0 = bas de la zone Daily (ancien)
    "SUPSW_EXIT_MIN_PCT": 0.5,            # le niveau de sortie est au moins a N % sous l entree (evite le declenchement hypersensible)
    "SUPSW_EXIT_MAX_PCT": 3.0,            # ... et au plus a N % sous l entree (toujours atteignable avant le stop)
    "SUPSW_EXIT_REQUIRE_H4_END": 0,       # 1 = exige aussi que la tendance H4 ne soit plus haussiere (sinon : seulement enregistre)
    "SUPSW_LEVER_SCALE": 0,               # v4.366 : 1 = stop et objectif dependent du levier (stop = SUPSW_MARGIN_RISK_PCT / levier, objectif par palier)
    "SUPSW_MARGIN_RISK_PCT": 7.5,         # perte au stop visee en % de la marge (stop prix = ce chiffre / levier, plafonne a SUPSW_SL_PCT)
    "SUPSW_MIN_SL_PCT": 1.5,              # plancher du stop (% du prix) pour ne pas etre sorti par le bruit aux leviers eleves
    "SUPSW_TP_LEV1": 2.0,                 # objectif (% du prix) a levier x1
    "SUPSW_TP_LEV2": 1.5,                 # ... a levier x2
    "SUPSW_TP_LEV3": 1.0,                 # ... a levier x3 et plus
    "FLOW_RECORDER_ENABLED": 1,           # v4.349 : 1 = enregistre le flux acheteur/vendeur par tranche de 5 min et par actif (lecture seule)
    "FLOW_RECORDER_BIG_TRADE_USD": 5000.0, # v4.349 : seuil d une "grosse" transaction
    "FLOW_RECORDER_KEEP_DAYS": 120,       # v4.349 : duree de conservation
    "FUNDING_MAX_ANNUAL_PCT": 40.0,       # v4.374 : pas d entree si |funding| annualise depasse N % (0 = sans plafond). Constat : tranche 40-60 % = -0,55 $ sur 34 trades (depuis le 28/09)
    "FUNDING_EXCLUDE_WEAK_COINS": 1,      # v4.374 : 1 = n ouvre plus de Funding sur FUNDING_WEAK_COINS (test)
    "FUNDING_WEAK_COINS": ["GMX", "RENDER"],
    "FUNDING_STALE_ENABLED": 1,           # v4.374 : sortie "sans progres" (1/0)
    "FUNDING_STALE_MIN": 20,              # ... apres N minutes
    "FUNDING_STALE_PEAK_PCT": 0.15,       # ... si le pic de gain est reste sous N % du prix
    "FUNDING_SHADOW_TRACKING": 1,         # v4.346 : 1 = suit ce que deviennent les entrees Funding bloquees par le filtre qualite (lecture seule)
    "FUNDING_SHADOW_SL_PCT": 0.7,         # v4.346 : stop hypothetique (% du prix) pour mesurer "+1R avant SL" (approximation du stop Funding)
    "BLOCKED_FOLLOWUP_ENABLED": 1,       # v4.338 : 1 = suit ce que devient un signal BLOQUE (lecture seule, aucun effet sur les entrees)
    "BLOCKED_FOLLOWUP_HORIZON_MIN": 120, # duree d observation apres le blocage (minutes)
    "ENTRY_FUNNEL_TRACKING": 1,          # v4.330 : 1 = compte, par fenetre de 15 min, les etapes atteintes par le top-down (lecture seule)
    "MTF_ENTRY_ALLOW_PULLBACK_TREND": 1, # v4.329 : 1 = accepte aussi repli/rebond avec structure EMA50/EMA200 intacte (pas seulement prix > EMA50 > EMA200)
    "MTF_PCT_LOCK_ON": 1,                # v4.393 : verrou de gain en % du prix (independant du R) : une fois le pic atteint, le SL garantit une part du pic
    "MTF_PCT_LOCK_ARM_PCT": 1.0,         # ... arme des que le meilleur gain atteint N % du prix (le verrou en R n arme qu a +1R, soit +3 % avec un stop large)
    "MTF_PCT_LOCK_FRACTION": 0.6,        # ... part du pic garantie (0.6 = 60 %)
    "MTF_PCT_LOCK_BIG_PCT": 3.0,         # ... au-dela de ce pic (%), part garantie plus elevee
    "MTF_PCT_LOCK_BIG_FRACTION": 0.75,
    "MTF_PCT_LOCK_FLOOR_PCT": 0.15,      # ... le SL est toujours au moins a +N % de l entree (frais couverts)
    "MTF_LOCK_FRACTION": 0.5,            # v4.340 : apres +1R, le SL garantit cette fraction du gain maximal (0 = desactive)
    "MTF_SL_PUSH_MIN_STEP_PCT": 0.05,    # v4.340 : live — ecart minimal (% du prix d entree) avant de renvoyer le SL a Hyperliquid
    "MTF_DATA_GRACE_SEC": 90,            # v4.341 : tolerance apres une limite de periode avant de declarer les bougies PERIMEES
    "MTF_BLOCK_ON_STALE_DATA": 1,        # v4.341 : 1 = aucune entree top-down sur des bougies H4 perimees
    "MTF_FETCH_RETRY_SEC": 20,           # v4.341 : delai avant de recharger des bougies en retard / en echec
    "MTF_FETCH_MAX_TRIES": 3,            # v4.341 : essais par periode avant d accepter les donnees en l etat
    "MTF_TREND_LIVE_PRICE": 1,           # v4.327 : 1 = tendance de fond calculee avec le prix live (WebSocket) ; 0 = derniere cloture H4 seule
    "MTF_TREND_PATH_ENABLED": 1,
    "MTF_TREND_ZONE_TOLERANCE_ATR": 0.25,  # tolerance (x ATR majeur) autour du support ascendant / resistance descendante
    # v4.314 — SUR DEMANDE EXPLICITE : score composite de priorisation des
    # candidats top-down ("comportement plus large", pas seulement l instant
    # T) — remplace le simple min(70+5xRR, 95) par une pondération de 5
    # facteurs : R:R (40%), force du flux (20%), force de la zone / nombre
    # de contacts (15%), performance historique REELLE de cet actif precis
    # (15%), qualite du marche (10%). Utilise pour CLASSER les candidats
    # quand plusieurs sont en concurrence pour un nombre limite d emplacements
    # (SPOT_ACCUM_MAX_TRADES / ACCUMULATION_MAX_TRADES) — le mieux note
    # gagne, au lieu d un ordre base sur le seul R:R.
    "MTF_SCORE_WEIGHT_RR": 0.40,
    "MTF_SCORE_WEIGHT_FLOW": 0.20,
    "MTF_SCORE_WEIGHT_ZONE": 0.15,
    "MTF_SCORE_WEIGHT_HISTORY": 0.15,
    "MTF_SCORE_WEIGHT_QUALITY": 0.10,
    "ASSET_WIN_RATE_REFRESH_SEC": 900,   # rafraichissement du cache de performance historique par actif (15 min)
    "ASSET_WIN_RATE_MIN_TRADES": 5,      # echantillon minimal avant de faire confiance au taux de reussite d un actif
    # v4.317 — SUR DEMANDE EXPLICITE : filtre de qualite du marche ACTIVE
    # pour Funding Contrarian, calibre depuis l analyse reelle de la plus
    # longue serie perdante (11 trades, -0.49 USDC, spread moyen 0.024% et
    # activite moyenne 1.19x, contre 0.014%/0.99x sur la serie gagnante).
    # Spot-Accum/Accumulation restent SANS filtre de qualite pour l instant
    # (pas encore leur propre analyse) — voir MARKET_QUALITY_MAX_SPREAD_PCT/
    # MARKET_QUALITY_MAX_ACTIVITY_RATIO (globaux, a 0/desactives) si vous
    # voulez un jour un seuil commun a tous les modes.
    "FUNDING_MAX_SPREAD_PCT": 0.02,
    "FUNDING_MAX_ACTIVITY_RATIO": 1.1,
    "ENTRY_ENGINE_SIMPLE": 1,
    "SIMPLE_ENGINE_DYNAMIC_LEVERAGE": 0,
    # v4.296 — live : releve les trades sous le minimum Hyperliquid (10 $)
    "LIVE_MIN_NOTIONAL_BUMP": 1,
    "LIVE_MIN_NOTIONAL_TARGET_USD": 10.5,   # v4.295 — 1 = levier dynamique 2-5x sur les entrees pres d un niveau
    "SITUATION_RULES_ENABLED": 1,          # 0 = ancien comportement (regime global)
    # v4.287 — SUR DEMANDE EXPLICITE : trades a CONTRE-TENDANCE (achat dans un
    # rebond baissier, short dans un repli haussier) desactives tant que les
    # donnees n ont pas montre qu ils gagnent (1 = reactiver)
    "SITUATION_ALLOW_COUNTERTREND": 0,
    # v4.287 — VOIE "CONTINUATION" : entree en tendance saine sans proximite
    # d un niveau, si flux franc confirme + marche actif + pas d exces + place
    # pour gagner + bougie dans le sens du trade. En PAPER seulement tant que
    # CONTINUATION_PAPER_ONLY = 1 (meme si le mode est en live).
    "CONTINUATION_ENABLED": 1,
    "SL_FOLLOWUP_WINDOW_MIN": 120,
    # v4.292 — controle de synchronisation bot <-> Hyperliquid (positions live)
    "LIVE_SYNC_INTERVAL_SEC": 120,
    "LIVE_SYNC_AUTO_FIX": 1,
    "LIVE_SYNC_SIZE_TOLERANCE": 0.05,
    "LIVE_SYNC_CHECK_NATIVE_SL": 1,   # v4.290 — suivi du prix apres chaque SL (etude du SL et de la patience)
    "CONTINUATION_PAPER_ONLY": 1,
    "CONTINUATION_MIN_FLOW": 0.3,
    "CONTINUATION_MIN_ACTIVITY": 1.0,
    "CONTINUATION_MAX_EXTENSION_ATR": 1.5,
    "CONTINUATION_MIN_ROOM_X_SL": 2.0,
    "SITUATION_REVERSAL_MIN_FLOW": 0.2,    # "fin de repli / fin de rebond" : flux minimal dans le sens du trade
    "SITUATION_COUNTERTREND_MIN_FLOW": 0.3,  # trade contre la tendance de fond : flux minimal
    "SITUATION_MIN_ROOM_PCT": 1.0,         # contre-tendance : marge minimale avant le niveau 1h oppose
    "COUNTERTREND_TTP_MULT": 0.6,          # contre-tendance : trailing arme a 60 % et repli toleré a 60 %
    # v4.273 — TRADING MANUEL
    "MANUAL_OPPORTUNITY_TTL_SEC": 120,     # une opportunite reste valable tant qu elle est revue dans ce delai
    "MANUAL_DEFAULT_NOTIONAL_USD": 15.0,   # taille proposee (notionnel) — minimum Hyperliquid 10 $
    "MANUAL_DEFAULT_LEVERAGE": 1,
    "MANUAL_MAX_LEVERAGE": 20,
    "MANUAL_DEFAULT_SL_PCT": 1.0,          # v4.343 : SL propose pour un ORDRE LIBRE (% du prix)
    "MANUAL_DEFAULT_LOCK_FRACTION": 0.5,   # v4.343 : part du gain maximal garantie par le verrou progressif
    "MANUAL_LOCK_PUSH_MIN_STEP_PCT": 0.05, # v4.343 : live — ecart minimal avant de renvoyer le SL a Hyperliquid
    "MANUAL_ORDER_EXPIRY_HOURS": 24,       # expiration par defaut d un ordre programme
    "TREND_PERSISTENCE_MIN_PRICE_MOVE_PCT": 0.1,
    # v4.246 — SUR DEMANDE EXPLICITE : confirmation IMMEDIATE (pas soutenue
    # dans le temps, contrairement a TREND_PERSISTENCE ci-dessus) par le
    # flux de transactions reel, pour l entree "flirt classique" — rejette
    # l entree si le flux contredit CLAIREMENT la direction attendue,
    # evite d entrer sur une simple bougie rouge/verte ponctuelle sans
    # vraie conviction du marche derriere.
    "ENTRY_FLOW_CONFIRM_ENABLED": True,
    "ENTRY_FLOW_CONTRADICTION_THRESHOLD": 0.3,
    # v4.247 — SUR DEMANDE EXPLICITE : confirmation par flux de
    # transactions reel, ajoutee a la cassure ratee (renforce/remplace le
    # volume par bougie) et a l etoile filante (n avait aucune
    # confirmation de ce type auparavant).
    "FAILED_BREAKOUT_FLOW_THRESHOLD": 0.15,
    "SHOOTING_STAR_FLOW_CONFIRM_ENABLED": True,
    "SHOOTING_STAR_FLOW_THRESHOLD": 0.1,
    # v4.248 — SUR DEMANDE EXPLICITE : bonus de confiance MAXIMAL
    # (module par l intensite 0.0-1.0 du signal) quand une cassure ratee
    # valide l entree — privilegie la force du signal lui-meme plutot que
    # la tendance externe (en retard sur ce type d evenement).
    "FAILED_BREAKOUT_MAX_CONFIDENCE_BONUS": 10.0,
    # v4.251 — SUR DEMANDE EXPLICITE : cooldown avant de retenter un trade
    # dont le notionnel projete est sous le minimum Hyperliquid (capital
    # probablement engage ailleurs) — evite les tentatives repetees
    # inutiles a chaque cycle, tant que rien n a change entre-temps.
    "INSUFFICIENT_NOTIONAL_COOLDOWN_SEC": 180,
    # v4.254 — SUR DEMANDE EXPLICITE : integration du flux de transactions
    # dans le TTP, sur 3 axes — (1) sortie ACCELEREE sur retournement
    # brutal, independant de la cloture de bougie (repond a un cas reel :
    # bougie verte devenant etoile filante EN COURS, rendant le profit
    # avant meme la cloture) ; (2) le flux peut ouvrir la porte de
    # retournement en alternative a la couleur de bougie ; (3) prolonge la
    # tolerance si le flux confirme toujours la direction.
    "TTP_FLOW_REVERSAL_EXIT_ENABLED": True,
    "TTP_FLOW_REVERSAL_THRESHOLD": -0.4,
    "TTP_FLOW_REVERSAL_MIN_PEAK_PCT": 0.3,
    "TTP_FLOW_GATE_THRESHOLD": 0.15,
    "TTP_FLOW_EXTEND_TOLERANCE_ENABLED": True,
    "TTP_FLOW_EXTEND_THRESHOLD": 0.2,
    "TTP_FLOW_EXTEND_MULTIPLIER": 1.5,
    # v4.130 — SUR DEMANDE EXPLICITE : meme raisonnement que Spot-Accum.
    # v4.131 — SUR DEMANDE EXPLICITE : meme alignement que Spot-Accum.
    "ACCUMULATION_ANTI_RANGE_LOOKBACK": 12,
    # v4.151 — SUR DEMANDE EXPLICITE : quand le marche est en range (voir
    # ci-dessus) ET que le prix est dans la fenetre de proximite normale,
    # trade DIRECTEMENT la fourchette (achat pres du support, vente pres
    # de la resistance) sans exiger de confirmation de tendance — retour a
    # l intention d origine d Accumulation, en complement (pas en
    # remplacement) des mecanismes de capture de tendance/cassure.
    "ACCUMULATION_TRADE_THE_RANGE": True,
    # v4.75 — SUR DEMANDE EXPLICITE : la tendance doit etre STABLE depuis ce
    # nombre de cycles consecutifs (~10s/cycle, 24 = ~4 min) avant d etre
    # consideree valide a l entree — evite d entrer juste avant/pendant un
    # retournement deja amorce (observe : pics de 0.14-0.48% suivis d un SL
    # sur Spot-Accum). N affecte PAS le mode normal.
    "SPOT_ACCUM_TREND_STABILITY_CYCLES": 12,
    "ACCUMULATION_TREND_STABILITY_CYCLES": 12,
    "FOREX_TREND_STABILITY_CYCLES": 12,
    # v4.58 — SUR DEMANDE EXPLICITE : 3 conditions de BASE PARTAGEES par les
    # 3 modes (normal, Accumulation, Spot-Accumulation) — remplacent une
    # grande partie de la complexite empilee ces dernieres iterations
    # (fraicheur du signal, ancien respect des niveaux, MACD separe,
    # ancienne coherence d amplitude, separation EMA200, confirmation
    # post-trade) pour le mode NORMAL. Accumulation garde en plus sa logique
    # de cassure (breakout) et sa confirmation post-trade existante.
    "UNIFIED_REQUIRE_ADX_CONFIRM":     False,
    "UNIFIED_SIMPLIFIED_MODE":         True,   # v4.58 — base commune remplace l ancienne complexite (mode normal)
    # v4.76 — SUR DEMANDE EXPLICITE : simplification COMPLETE du mode normal
    # (retire RSI/EMA croisee/fraicheur de la decision finale, ne garde que
    # les 3 conditions communes + stabilite) — alignement total avec
    # Accumulation/Spot-Accumulation.
    "UNIFIED_FULL_SIMPLIFIED_MODE":    True,
    # v4.155 — SUR DEMANDE EXPLICITE : bloqueur anti-range pour le mode
    # normal (aucun outil dedie pour bien trader un range, contrairement a
    # Accumulation) — 200 echantillons = 6h40 (aligne sur EMA200/ADX).
    "FOREX_REQUIRE_ANTI_RANGE": True,
    # v4.272 — FIX BUG CRITIQUE : 2,0 % etait calibre pour les CRYPTOS. Une
    # paire de devises (EUR/USD, USD/JPY...) bouge typiquement de 0,1 a 0,4 %
    # en 2 h 30 (30 bougies 5 min) : avec 2 %, le marche etait juge "en
    # range" en permanence et le mode Forex ne pouvait JAMAIS entrer — et ce
    # blocage n apparaissait meme pas dans le diagnostic.
    "FOREX_ANTI_RANGE_MIN_PCT": 0.25,
    # v4.272 — meme probleme pour le repli "momentum long terme" de la
    # confirmation de tendance (2 % sur 180 bougies, calibre crypto).
    "FOREX_LONG_TERM_MOMENTUM_MIN_CHANGE_PCT": 0.4,
    "FOREX_ANTI_RANGE_LOOKBACK": 30,
    # v4.155 — SUR DEMANDE EXPLICITE : meme protection pour Funding.
    "FUNDING_REQUIRE_ANTI_RANGE": True,
    "FUNDING_ANTI_RANGE_MIN_PCT": 2.0,
    "FUNDING_ANTI_RANGE_LOOKBACK": 30,
    # v4.108 — FIX BUG CRITIQUE : desormais exprime en % de l AMPLITUDE
    # (support-resistance), plus du prix du support — coherent avec le
    # seuil structurel du trailing (70% de l amplitude). Recalibre a 5-10%
    # (au lieu de 1-5%, qui deviendrait ridicule en % d amplitude).
    "UNIFIED_MIN_ABOVE_SUPPORT_PCT":   5.0,
    "UNIFIED_MAX_ABOVE_SUPPORT_PCT":   10.0,
    # v4.113 — SUR DEMANDE EXPLICITE : mis en pause temporairement (False)
    # en attendant un mecanisme plus robuste base sur les touches multiples
    # du support/resistance — la valeur ci-dessous reste prete a l emploi
    # des que reactive.
    "UNIFIED_REQUIRE_SR_AMPLITUDE": False,
    "UNIFIED_MIN_SR_AMPLITUDE_PCT":    4.0,
    # v4.32 — marge d hysteresis autour du seuil ci-dessus : le mode ne
    # bascule que si l ADX depasse clairement le seuil (+marge pour "trend",
    # -marge pour "reversal") — dans la zone ambigue entre les deux, le
    # dernier mode retenu est conserve, pour eviter un flip-flop trend/
    # reversal a chaque cycle sur un actif dont l ADX oscille pres du seuil.
    "ADX_HYSTERESIS_MARGIN":    3.0,

    # ── SL par paliers de gains (swing uniquement) ───────────────────────────
    "SL_LOCK_ENABLED":          True,
    "SL_LOCK_STEPS": [
        (0.5, 0.0),   # +0.5% → breakeven
        (1.0, 0.9),   # +1.0% → +0.9%
        (1.5, 1.3),   # +1.5% → +1.3%
        (2.0, 1.8),   # +2.0% → +1.8%
    ],
    "SL_LOCK_STEPS_BY_SYMBOL": {},

    # Momentum Instantane — "ce qui se passe MAINTENANT" prevaut sur les EMA
    # Si le prix a bouge de +/-0.20% sur les 4 derniers cycles (2 min) dans
    # le sens OPPOSE au signal EMA/RSI, l entree est bloquee. Seuil unique
    # pour tous les actifs.
    "MOMENTUM_PERIOD":          4,
    "MOMENTUM_THRESHOLD_PCT":   0.20,
}

PROFILE_SCALP = {
    "PROFILE":                  "scalp",
    # RSI compromis swing/scalp — BTC entre plus facilement
    "RSI_OVERSOLD":             40,
    "RSI_OVERBOUGHT":           60,
    "SYMBOL_RSI_OVERSOLD":      {},
    "SYMBOL_RSI_OVERBOUGHT":    {},
    "SYMBOL_RSI_MODE":          {},
    # EMA plus courtes pour etre plus reactif
    "EMA_SHORT":                8,
    "EMA_LONG":                 21,
    "SYMBOL_EMA_SHORT":         {},
    "SYMBOL_EMA_LONG":          {},
    # EMA intermediaire scalp — fenetre plus courte (30 min), uniforme pour tous
    # v3.2 — recalibre pour ~30 min reelles avec CYCLE_INTERVAL=10s
    "EMA_MID_PERIOD":           180,
    "SYMBOL_EMA_MID":           {},
    # SL et TP serres
    "STOP_LOSS_PCT":            0.4,
    "TAKE_PROFIT_PCT":          0.8,
    "SYMBOL_SL_PCT":            {},
    "SYMBOL_TP_PCT":            {},
    # Trailing serre
    "TRAILING_STOP":            True,
    "TRAILING_DELTA_PCT":       0.3,
    "SYMBOL_TRAILING_DELTA_PCT":{},
    "TRAILING_TP":              True,
    "TRAILING_TP_STEP_PCT":     0.4,
    "TRAILING_TP_RSI_EXIT":     52,
    "TRAILING_TP_MIN_SIGNALS":  2,
    "TRAILING_TP_PROTECT_PCT":  0.97,
    "TRAILING_SL_MIN_MOVE_PCT": 0.05,
    "SYMBOL_REQUIRE_EMA200":    [],
    "SYMBOL_REQUIRE_MACD_BB":   [],
    "PIVOT_CONFIRM_SYMBOLS":    [],
    "CONSEC_CONFIRM_SYMBOLS":   {},
    "VOLUME_MIN_RATIO":         1.0,
    "FOREX_WARMUP_MINUTES":     5,
    # Filtre ATR — seuil global unique pour tous les actifs
    # v4.57 — SUR DEMANDE EXPLICITE : DESACTIVE, meme raison que le profil swing.
    "ATR_FILTER":               False,
    "ATR_PERIOD":               21,   # v3.2 — recalibre (14->21) pour preserver la fenetre reelle originale avec le cycle a 10s
    "ATR_MIN_PCT":              0.02,
    "ATR_MIN_PCT_BY_SYMBOL":    {},
    "ATR_EXCLUDE_SYMBOLS":      [],     # plus d exclusion

    # Support/Resistance — confirmation de breakout (SCALP uniquement)
    # LONG  : prix doit CASSER au-dessus de la resistance des 50 derniers cycles (25 min)
    # SHORT : prix doit CASSER en-dessous du support des 50 derniers cycles
    # Filtre les faux signaux RSI/EMA en exigeant un vrai mouvement directionnel
    "SR_PERIOD":                50,
    # v3.2 — Filtre marge Support/Resistance : bloque une entree si le
    # support/resistance recent (50 cycles) est trop proche pour laisser la
    # place a un Quick Profit avant de s y heurter. Actif par defaut pour
    # tous les profils (avant : reserve au breakout scalp uniquement).
    "SR_MIN_ROOM_FILTER":       True,

    # Momentum Instantane — "ce qui se passe MAINTENANT" prevaut sur les EMA
    # Si le prix a bouge de +/-0.10% sur les 4 derniers cycles (2 min) dans
    # le sens OPPOSE au signal EMA/RSI, l entree est bloquee.
    # Seuil plus bas qu en swing car cycles plus courts et mouvements rapides.
    "MOMENTUM_PERIOD":          4,
    "MOMENTUM_THRESHOLD_PCT":   0.10,
}

def apply_profile(cfg, profile_name):
    """Applique un profil SWING ou SCALP sur le cfg actif.
    Preserve les parametres fixes (cles API, symboles, capital, mode).
    """
    profile = PROFILE_SWING if profile_name == "swing" else PROFILE_SCALP
    for k, v in profile.items():
        cfg[k] = v
    cfg["PROFILE"] = profile_name

# ─────────────────────────────────────────────
#  INDICATEURS TECHNIQUES
# ─────────────────────────────────────────────
def calc_ema(prices, period):
    """v4.124 — FIX BUG CRITIQUE : exigeait auparavant len(prices) >= period
    AVANT de retourner quoi que ce soit — pour period=200 (EMA200,
    echantillonne ~toutes les 2 min), ca signifiait ~6h40 d attente avant le
    tout premier resultat non-None, quel que soit le seuil externe applique
    par l appelant (celui-ci n a jamais eu d effet reel). Calcule desormais
    une estimation DES que 2+ points sont disponibles — graine simple
    (moyenne des points disponibles) qui s affine progressivement vers une
    vraie EMA a mesure que les donnees s accumulent, au lieu de rester
    bloque a None pendant des heures."""
    if len(prices) < 2:
        return None
    effective_period = min(period, len(prices))
    k = 2 / (effective_period + 1)
    ema = sum(prices[:effective_period]) / effective_period
    for p in prices[effective_period:]:
        ema = p * k + ema * (1 - k)
    return ema

def calc_adx(prices, period=14):
    """Average Directional Index (approxime) — mesure la FORCE d une
    tendance, independamment de sa direction. Contrairement a l ATR (qui
    mesure l amplitude du mouvement), l ADX mesure si ce mouvement est
    DIRECTIONNEL (tendance nette) ou erratique (range/oscillation).
    - ADX eleve (~25+)  : vraie tendance en cours -> mode "trend" adapte
      (suivre le mouvement, RSI>50 achete, RSI<50 vend).
    - ADX faible (~20-) : marche en range, sans direction nette -> mode
      "reversal" adapte (RSI survente achete, RSI surachat vend — parier
      sur l oscillation plutot que sur une tendance qui ne vient pas).
    Comme calc_atr, utilise les variations de close (pas de high/low
    disponibles) — une approximation fidele dans l esprit de l ADX
    classique, pas une implementation Wilder exacte.
    Retourne une valeur 0-100, ou None si donnees insuffisantes.
    """
    if len(prices) < period * 2 + 1:
        return None
    diffs = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    plus_dm  = [d if d > 0 else 0 for d in diffs]
    minus_dm = [-d if d < 0 else 0 for d in diffs]
    tr       = [abs(d) for d in diffs]

    dx_values = []
    for end in range(period, len(diffs) + 1):
        window_tr = sum(tr[end-period:end])
        if window_tr <= 0:
            continue
        window_plus  = sum(plus_dm[end-period:end])
        window_minus = sum(minus_dm[end-period:end])
        plus_di  = 100 * window_plus / window_tr
        minus_di = 100 * window_minus / window_tr
        di_sum = plus_di + minus_di
        if di_sum <= 0:
            continue
        dx_values.append(100 * abs(plus_di - minus_di) / di_sum)

    if not dx_values:
        return None
    return sum(dx_values[-period:]) / min(period, len(dx_values))


def calc_rsi(prices, period=14):
    if len(prices) < period + 1:
        return None
    recent = prices[-(period + 1):]
    gains = losses = 0
    for i in range(1, len(recent)):
        d = recent[i] - recent[i - 1]
        if d > 0:
            gains += d
        else:
            losses -= d
    if losses == 0:
        return 100.0
    return 100 - (100 / (1 + gains / losses))

def calc_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal:
        return None, None
    macd_series = []
    for i in range(slow - 1, len(prices)):
        ef = calc_ema(prices[:i+1], fast)
        es = calc_ema(prices[:i+1], slow)
        if ef and es:
            macd_series.append(ef - es)
    if len(macd_series) < signal:
        return None, None
    macd_line = macd_series[-1]
    signal_line = calc_ema(macd_series, signal)
    return macd_line, signal_line

def calc_bollinger(prices, period=20, std_mult=2.0):
    if len(prices) < period:
        return None, None, None
    recent = prices[-period:]
    mid = sum(recent) / period
    variance = sum((p - mid) ** 2 for p in recent) / period
    std = variance ** 0.5
    return mid + std_mult * std, mid, mid - std_mult * std


def calc_atr(prices, period=14):
    """Average True Range — mesure la volatilite reelle du marche.
    Un ATR% faible = marche en range, risque de faux signaux.
    Un ATR% eleve = marche directionnel, bonne opportunite de scalping.
    Utilise les variations de close (pas de high/low disponibles).
    Retourne (atr_abs, atr_pct) ou (None, None) si insuffisant.
    v4.36 — CONSERVEE pour compatibilite (utilisee sur price_history, le
    flux brut par cycle de 10s) — voir calc_true_range_atr ci-dessous pour
    le calcul PREFERE, base sur de vraies bougies haut/bas/cloture, plus
    representatif de la volatilite reelle qu une simple variation cloture-a-
    cloture sur un intervalle aussi court.
    """
    if len(prices) < period + 1:
        return None, None
    tr_list = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    atr = sum(tr_list[-period:]) / period
    atr_pct = (atr / prices[-1]) * 100 if prices[-1] > 0 else 0
    return atr, atr_pct


def calc_true_range_atr(candles, period=14):
    """v4.36 — SUR DEMANDE EXPLICITE : vrai calcul d ATR (methode Wilder),
    a partir de VRAIES bougies (haut, bas, cloture) construites en direct
    depuis le flux WebSocket (voir SymbolState.candle_history) — plus
    representatif de la volatilite reelle que calc_atr (qui ne voit que des
    points de prix isoles espaces de 10s, structurellement quasi-nul).

    True Range = max(haut-bas, |haut-cloture_precedente|, |bas-cloture_precedente|)
    ATR = moyenne des True Range sur la periode.

    candles : liste de tuples (high, low, close), la plus recente en dernier.
    Retourne (atr_abs, atr_pct) ou (None, None) si insuffisant.
    """
    if len(candles) < period + 1:
        return None, None
    tr_list = []
    for i in range(1, len(candles)):
        high, low, _ = candles[i]
        prev_close = candles[i-1][2]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_list.append(tr)
    atr = sum(tr_list[-period:]) / period
    last_close = candles[-1][2]
    atr_pct = (atr / last_close) * 100 if last_close > 0 else 0
    return atr, atr_pct


def calc_avg_candle_fluctuation(candles, period=14):
    """v4.41 — SUR DEMANDE EXPLICITE : fluctuation moyenne A L INTERIEUR de
    chaque bougie (haut-bas, en % de la cloture), INDEPENDAMMENT de sa
    couleur finale (verte ou rouge) — different de l ATR, qui mesure
    l amplitude ENTRE bougies consecutives (True Range). Sert a mesurer le
    "bruit interne" propre a chaque actif : une tendance n est jamais
    lineaire, une meme bougie peut osciller plusieurs fois de sens avant de
    clore dans une direction. Purement informatif — n influence aucune
    decision de trading, juste un outil de calibrage manuel des seuils
    SL/TTP/ATR par actif.
    Retourne le % moyen (haut-bas)/cloture sur les N dernieres bougies, ou
    None si pas assez de bougies.
    """
    if len(candles) < period:
        return None
    recent = candles[-period:]
    fluctuations = []
    for high, low, close in recent:
        if close > 0:
            fluctuations.append((high - low) / close * 100)
    if not fluctuations:
        return None
    return sum(fluctuations) / len(fluctuations)


def calc_support_resistance(prices, period=50):
    """Calcule le support et la resistance recents.
    Resistance = plus haut local sur la periode (hors prix courant)
    Support    = plus bas local sur la periode (hors prix courant)
    Utilise pour confirmer les breakouts en scalping :
    - LONG valide si le prix CASSE au-dessus de la resistance recente
    - SHORT valide si le prix CASSE en-dessous du support recent
    Retourne (support, resistance) ou (None, None) si insuffisant.
    """
    if len(prices) < period + 1:
        return None, None
    # Exclure le prix courant (dernier element) pour eviter l auto-validation
    window = prices[-(period+1):-1]
    support    = min(window)
    resistance = max(window)
    return support, resistance

def calc_support_resistance_from_candles(candles, period=100):
    """v4.78 — SUR DEMANDE EXPLICITE : support/resistance calcules a partir
    des VRAIES bougies (haut/bas), pas des prix bruts echantillonnes au
    rythme du cycle (~10s) — l ancien calcul (calc_support_resistance
    ci-dessus) ne couvrait qu environ 8 minutes avec ses reglages par
    defaut, bien trop court pour representer des niveaux structurels
    reels (confirme visuellement : un vrai support/resistance se lit sur
    des heures, pas des minutes). Les bougies sont echantillonnees toutes
    les MTF_CANDLE_SEC (2 min par defaut), donc period=100 couvre environ
    3h20 — period=200 (maximum disponible) couvre environ 6h40.
    Retourne (support, resistance) ou (None, None) si insuffisant.
    """
    if len(candles) < period:
        return None, None
    window = list(candles)[-period:]
    highs = [c[0] for c in window]
    lows  = [c[1] for c in window]
    resistance = max(highs)
    support    = min(lows)
    return support, resistance

# ─────────────────────────────────────────────
#  CONNEXION HYPERLIQUID
# ─────────────────────────────────────────────
_SZ_DECIMALS_CACHE = {"perp": None, "spot": None, "fetched_at": 0}

def _get_sz_decimals_map(info):
    """v4.97 — SUR DEMANDE EXPLICITE : Hyperliquid exige une precision de
    taille (szDecimals) DIFFERENTE PAR ACTIF (de 0 a 8 selon l actif) — un
    arrondi fixe applique a tous les actifs (l ancien comportement) produit
    des ordres rejetes des que l actif exige une precision differente de
    celle codee en dur. Recupere et met en cache les vraies valeurs depuis
    l API (universe[].szDecimals pour les perps, tokens[].szDecimals pour
    le spot), rafraichi toutes les 10 minutes (les specs d un actif
    changent tres rarement, pas besoin de requeter a chaque ordre)."""
    import time as _time
    now = _time.time()
    if _SZ_DECIMALS_CACHE["perp"] is not None and (now - _SZ_DECIMALS_CACHE["fetched_at"]) < 600:
        return _SZ_DECIMALS_CACHE["perp"], _SZ_DECIMALS_CACHE["spot"]
    perp_map, spot_map = {}, {}
    try:
        meta = info.meta()
        for asset in meta.get("universe", []):
            perp_map[asset["name"]] = asset.get("szDecimals", 4)
    except Exception as e:
        print(f"[SZDECIMALS] Echec recuperation meta perp : {e}")
    try:
        spot_meta = info.spot_meta()
        for token in spot_meta.get("tokens", []):
            spot_map[token["name"]] = token.get("szDecimals", 4)
    except Exception as e:
        print(f"[SZDECIMALS] Echec recuperation meta spot : {e}")
    _SZ_DECIMALS_CACHE["perp"] = perp_map
    _SZ_DECIMALS_CACHE["spot"] = spot_map
    _SZ_DECIMALS_CACHE["fetched_at"] = now
    return perp_map, spot_map

def format_size_hl(size, sz_decimals):
    """Tronque (jamais arrondi vers le haut) a la precision exacte exigee
    par Hyperliquid pour cet actif — un arrondi standard pourrait produire
    une taille legerement SUPERIEURE a ce qui est reellement disponible/
    autorise."""
    factor = 10 ** sz_decimals
    import math
    return math.floor(size * factor) / factor

def format_price_hl(price, sz_decimals, is_spot=False):
    """v4.100 — FIX BUG CRITIQUE : la version precedente (v4.98) omettait
    volontairement la regle des "5 chiffres significatifs", jugee a tort
    non-bloquante suite a un exemple ambigu de la doc (97000.5 valide pour
    BTC) — mais des echecs reels confirmes ("Price must be divisible by
    tick size") sur SOL/INJ prouvent que cette regle EST bien appliquee et
    peut etre PLUS restrictive que la seule limite de decimales. Applique
    desormais les DEUX contraintes documentees (5 chiffres significatifs
    ET max decimales par rapport a szDecimals), en gardant la plus stricte
    des deux. Troncature (jamais un arrondi vers le haut), coherente avec
    les implementations officielles du SDK.
    """
    if price == int(price):
        return price
    from decimal import Decimal, ROUND_DOWN
    max_decimals = max((8 if is_spot else 6) - sz_decimals, 0)
    d = Decimal(str(price))
    # Contrainte 1 : 5 chiffres significatifs
    exponent = d.adjusted()
    sig_fig_exp = exponent - 4  # 5 chiffres significatifs -> garde jusqu a cette position
    # Contrainte 2 : max_decimals par rapport a szDecimals
    max_dec_exp = -max_decimals
    # La plus stricte des deux = l exposant le PLUS GRAND (arrondit le plus tot)
    final_exp = max(sig_fig_exp, max_dec_exp)
    quantizer = Decimal(1).scaleb(final_exp)
    return float(d.quantize(quantizer, rounding=ROUND_DOWN))

def connect_hyperliquid(private_key, wallet_address):
    """Retourne (info, exchange, error_detail). error_detail est None en cas
    de succes, sinon un message texte precis (type + message de l exception)
    — evite d avaler silencieusement la vraie cause d un echec de connexion
    (mauvais format de cle, dependance manquante, probleme reseau, etc.)."""
    private_key = _clean_hex_secret(private_key)
    wallet_address = _clean_hex_secret(wallet_address)
    try:
        from hyperliquid.info import Info
        from hyperliquid.exchange import Exchange
        from hyperliquid.utils import constants
        import eth_account
        account = eth_account.Account.from_key(private_key)
        # v3.1 : skip_ws=False active la connexion WebSocket du SDK, necessaire
        # pour s abonner au flux temps reel (allMids) utilise par la
        # surveillance Max Loss / Trailing TP en direct (voir _on_ws_allmids).
        # v4.264 — FIX BUG CRITIQUE : sans perp_dexs, le SDK ne connait PAS
        # les actifs HIP-3 ("xyz:EUR"...) — tout ordre, fermeture ou levier
        # forex levait une KeyError avalee silencieusement. Repli sans le
        # DEX xyz si l initialisation multi-DEX echoue (le reste du bot
        # continue de fonctionner, le forex live sera alors indisponible).
        try:
            info = Info(constants.MAINNET_API_URL, skip_ws=False, perp_dexs=HL_PERP_DEXS)
        except Exception as e_dex:
            print(f"[connect_hyperliquid] Init multi-DEX {HL_PERP_DEXS} impossible ({e_dex}) — repli DEX natif seul (forex live indisponible).")
            info = Info(constants.MAINNET_API_URL, skip_ws=False)
        # v4.99 — FIX BUG CRITIQUE : wallet_address est un compte NORMAL,
        # pas un vault Hyperliquid (fonctionnalite distincte, pools de fonds
        # partages) — le passer en tant que vault_address causait un rejet
        # systematique de TOUS les ordres reels ("Vault not registered"),
        # meme apres correction du bug limit_px. account_address est le bon
        # parametre pour "trader au nom de cette adresse", que la cle privee
        # signataire soit celle du compte principal ou celle d un "agent"
        # (wallet API separe autorise a trader pour ce compte).
        try:
            exchange = Exchange(account, constants.MAINNET_API_URL, account_address=wallet_address, perp_dexs=HL_PERP_DEXS)
        except Exception as e_dex:
            print(f"[connect_hyperliquid] Exchange multi-DEX impossible ({e_dex}) — repli DEX natif seul.")
            exchange = Exchange(account, constants.MAINNET_API_URL, account_address=wallet_address)
        return info, exchange, None
    except Exception as e:
        import traceback
        detail = f"{type(e).__name__}: {e}"
        if "non-hexadecimal digit" in str(e).lower():
            # v4.4 — cause la plus frequente en pratique : un caractere
            # invisible (espace, retour a la ligne, guillemet) reste colle
            # a la cle malgre le nettoyage ci-dessus, ou la valeur collee
            # n est tout simplement pas une cle privee hexadecimale valide
            # (ex: phrase mnemonique au lieu de la cle, cle tronquee lors du
            # copier-coller, ou variable Railway mal renseignee).
            detail += (" — verifiez que HYPERBOT_PRIVATE_KEY contient bien la cle "
                       "privee hexadecimale complete (64 caracteres apres le '0x' "
                       "eventuel), sans espace ni guillemet, et pas une phrase de "
                       "recuperation (seed phrase).")
        print(f"[connect_hyperliquid] {detail}")
        print(traceback.format_exc())
        return None, None, detail

def sync_capital_from_hyperliquid(info, wallet_address):
    """Lit le solde réel USDC depuis Hyperliquid et le retourne.
    Retourne None en cas d'echec pour ne pas ecraser le capital local.
    """
    try:
        state = info.user_state(wallet_address)
        real_balance = float(state["marginSummary"]["accountValue"])
        print(f"[CAPITAL] Solde reel Hyperliquid : ${real_balance:.2f}")
        return real_balance
    except Exception as e:
        print(f"[CAPITAL] Impossible de lire le solde Hyperliquid : {e}")
        return None

# ─────────────────────────────────────────────────────────────────────────
#  v4.264 — OUTILS MULTI-DEX, FERMETURE VERIFIEE, IDENTIFIANT DE TRADE
# ─────────────────────────────────────────────────────────────────────────
# DEX interroges : "" = DEX natif Hyperliquid, "xyz" = DEX HIP-3 du forex.
HL_PERP_DEXS = ["", "xyz"]


def _dex_of(ticker):
    """"xyz:EUR" -> "xyz" ; "BTC" -> "" (DEX natif)."""
    return ticker.split(":", 1)[0] if ticker and ":" in ticker else ""


def exchange_wallet_address(exchange):
    """Adresse du compte reellement trade par cet Exchange SDK."""
    return (getattr(exchange, "vault_address", None)
            or getattr(exchange, "account_address", None)
            or exchange.wallet.address)


def fetch_exchange_positions(info, wallet_address, tickers=None):
    """Positions perp REELLES sur TOUS les DEX utiles (natif + xyz).
    Retourne {coin: {"szi", "entry", "leverage"}} — ou None si AU MOINS UNE
    requete a echoue : l appelant ne doit alors JAMAIS conclure qu une
    position est fermee (absence de donnee != absence de position)."""
    dexes = {""}
    for t in (tickers or []):
        dexes.add(_dex_of(t))
    result = {}
    for dex in sorted(dexes):
        try:
            state = info.user_state(wallet_address, dex) if dex else info.user_state(wallet_address)
        except Exception as e:
            print(f"[EXCH-POS] Lecture positions DEX '{dex or 'natif'}' impossible : {e}")
            return None
        for item in (state or {}).get("assetPositions", []):
            p = item.get("position", {}) or {}
            coin = p.get("coin", "")
            try:
                szi = float(p.get("szi", 0) or 0)
                entry = float(p.get("entryPx", 0) or 0)
            except (TypeError, ValueError):
                continue
            if coin and szi != 0:
                lev = (p.get("leverage") or {}).get("value")
                def _f(key):
                    try:
                        return float(p.get(key)) if p.get(key) is not None else None
                    except (TypeError, ValueError):
                        return None
                result[coin] = {"szi": szi, "entry": entry, "leverage": lev,
                                # v4.292 — infos supplementaires pour le controle de synchronisation
                                "liquidation_px": _f("liquidationPx"), "unrealized_pnl": _f("unrealizedPnl"),
                                "position_value": _f("positionValue")}
    return result


def get_exchange_position_szi(info, wallet_address, ticker):
    """szi reel d un seul actif : 0.0 si aucune position, None si inconnu."""
    positions = fetch_exchange_positions(info, wallet_address, [ticker])
    if positions is None:
        return None
    return positions.get(ticker, {}).get("szi", 0.0)


def _order_first_status(result):
    """Premier statut d une reponse d ordre Hyperliquid (dict) ou None."""
    try:
        statuses = result.get("response", {}).get("data", {}).get("statuses", [])
        return statuses[0] if statuses and isinstance(statuses[0], dict) else None
    except AttributeError:
        return None


# Codes de strategie encodes dans l identifiant client (cloid) des ordres
# d entree — permet de retrouver le MODE SOURCE d une position directement
# depuis l historique Hyperliquid, meme si la base locale etait perdue.
TRADE_UID_MAGIC = "4842"  # "HB"
STRATEGY_CODES = {"forex": "01", "accumulation": "02", "spot_accumulation": "03", "funding_contrarian": "04", "manual": "05", "forex_scalp": "06", "forex_swing": "07", "swing_support": "08", "trend_follow": "09"}
STRATEGY_FROM_CODE = {v: k for k, v in STRATEGY_CODES.items()}


def make_trade_uid(strategy):
    """Identifiant unique d un trade, cree AVANT l envoi de l ordre, au
    format cloid Hyperliquid (0x + 32 hex) : magic(4) + strategie(2) +
    horodatage ms(12) + aleatoire(14)."""
    import secrets as _secrets
    code = STRATEGY_CODES.get(strategy, "00")
    ts_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    return "0x" + TRADE_UID_MAGIC + code + f"{ts_ms:012x}" + _secrets.token_hex(7)


def decode_trade_uid(uid):
    """Retourne {"strategy", "opened_ts"} si uid est un identifiant HyperBot, sinon None."""
    try:
        raw = str(uid).lower()
        if not raw.startswith("0x") or len(raw) != 34 or raw[2:6] != TRADE_UID_MAGIC:
            return None
        strategy = STRATEGY_FROM_CODE.get(raw[6:8])
        opened_ts = int(raw[8:20], 16) / 1000.0
        return {"strategy": strategy, "opened_ts": opened_ts}
    except (ValueError, TypeError):
        return None


def find_trade_uid_in_history(info, wallet_address, coin, is_long):
    """Dernier recours au redemarrage (base locale muette) : cherche dans l
    historique d ordres Hyperliquid le plus recent ordre d ENTREE HyperBot
    (cloid reconnaissable) sur ce coin et dans ce sens. None si introuvable."""
    try:
        history = info.historical_orders(wallet_address) or []
    except Exception as e:
        print(f"[RECOVER] historique d ordres indisponible : {e}")
        return None
    best = None
    for h in history:
        order = h.get("order", h) if isinstance(h, dict) else {}
        if order.get("coin") != coin or order.get("reduceOnly"):
            continue
        side_long = order.get("side") == "B"
        if side_long != bool(is_long):
            continue
        cloid = order.get("cloid")
        if not cloid or decode_trade_uid(cloid) is None:
            continue
        ts = order.get("timestamp", 0) or 0
        if best is None or ts > best[0]:
            best = (ts, cloid)
    return best[1] if best else None


def recover_open_positions(info, wallet_address, symbols, cfg):
    """Recupere les positions ouvertes sur Hyperliquid apres un crash.
    Retourne un dict {symbol: position_dict} compatible avec SymbolState.
    """
    recovered = {}
    try:
        # v4.264 — interroge TOUS les DEX (natif + xyz) : les positions forex
        # HIP-3 n etaient jamais recuperees (user_state sans parametre dex).
        exch_positions = fetch_exchange_positions(info, wallet_address, symbols) or {}
        for coin, ep in exch_positions.items():
            szi  = ep["szi"]      # positif = long, negatif = short
            entry = ep["entry"]
            if coin not in symbols or szi == 0 or entry == 0:
                print(f"[RECOVER-DIAG] {coin} IGNORE — coin_in_symbols={coin in symbols} | szi={szi} | entry={entry}")
                continue
            print(f"[RECOVER-DIAG] {coin} RETENU — szi={szi} | entry={entry}")
            direction = "long" if szi > 0 else "short"
            size_usd  = abs(szi) * entry
            # v4.235 — SUR DEMANDE EXPLICITE, FIX BUG CRITIQUE : ce SL est
            # un FILET DE SECOURS "catastrophe" (place sur Hyperliquid
            # UNIQUEMENT si aucun SL n est deja detecte, voir
            # ensure_sl_on_hyperliquid) — PAS le SL de gestion normale
            # (gere en interne une fois le suivi repris). L ancienne
            # formule (STOP_LOSS_PCT generique, ~1.5%) etait bien trop
            # serree pour ce role, confirme par un cas reel : une position
            # Spot-Accum recuperee (marge normale attendue ~10%) a ete
            # fermee prematurement par CE SL de secours a 1.5%, sans lien
            # avec la logique de sortie du bot. Utilise desormais une
            # marge large et fixe, coherente avec un VRAI filet de
            # catastrophe plutot qu un SL de gestion quotidienne.
            sl_pct = cfg.get("RECOVERY_RESCUE_SL_PCT", 15.0)
            tp_pct = cfg.get("SYMBOL_TP_PCT", {}).get(coin, cfg["TAKE_PROFIT_PCT"])
            sl_p = entry * (1 - sl_pct/100) if direction == "long" else entry * (1 + sl_pct/100)
            tp_p = entry * (1 + tp_pct/100) if direction == "long" else entry * (1 - tp_pct/100)
            recovered[coin] = {
                "type":  direction,
                "entry": entry,
                "sl":    sl_p,
                "tp":    tp_p,
                "size":  size_usd,
                "peak":  entry,
                # v4.245 — SUR DEMANDE EXPLICITE : sans heure d ouverture
                # reelle connue (position recuperee), utilise l heure de
                # RECUPERATION comme repli raisonnable — evite l affichage
                # "OUVERT --"/"DUREE --" et permet au plafond de duree
                # maximale de fonctionner normalement a partir de
                # maintenant (mieux qu une duree indefiniment inconnue).
                "opened_at": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            }
            print(f"[RECOVER] {coin} {direction.upper()} @ ${entry:.2f} | SL ${sl_p:.2f} | TP ${tp_p:.2f}")
    except Exception as e:
        print(f"[RECOVER] Erreur recuperation positions : {e}")
    return recovered

def reconcile_closed_positions(info, wallet_address, saved_positions, cfg):
    """Au redemarrage en mode live, compare les positions sauvegardees localement
    avec ce qu Hyperliquid retourne. Si une position n existe plus sur la bourse,
    c est qu elle a ete fermee pendant la deconnexion (SL ou TP touche).
    Retourne une liste de trades reconstitues pour mise a jour du capital et historique.
    v4.226 — SUR DEMANDE EXPLICITE : retourne AUSSI un dict {coin: vrai prix
    d entree} pour les positions ENCORE ouvertes, permettant de corriger le
    prix d entree enregistre localement s il divergeait du vrai prix
    confirme par Hyperliquid (meme correctif que pour les nouvelles
    ouvertures, applique retroactivement aux positions deja en cours avant
    un redeploiement)."""
    ghost_trades = []
    real_entry_prices = {}
    if not saved_positions:
        return ghost_trades, real_entry_prices
    try:
        # v4.264 — FIX BUG CRITIQUE : ne lisait que le DEX natif — toute
        # position forex (xyz:) LIVE sauvegardee etait donc declaree "fermee
        # pendant la deconnexion" (faux trade fantome, PnL invente) alors
        # qu elle etait toujours ouverte. Si la lecture echoue, on ne
        # conclut RIEN (aucune position declaree fermee).
        saved_tickers = [ticker_from_slot_key(k.replace("ACCUM__", "", 1)) for k in saved_positions]
        exch_positions = fetch_exchange_positions(info, wallet_address, saved_tickers)
        if exch_positions is None:
            print("[RECONCILE] Positions Hyperliquid illisibles — aucune fermeture deduite par prudence.")
            return ghost_trades, real_entry_prices
        open_coins = set(exch_positions)
        for coin, ep in exch_positions.items():
            real_entry_prices[coin] = ep["entry"]

        # Recuperer l historique recent des fills pour connaitre le prix de cloture reel
        try:
            fills = info.user_fills(wallet_address)
        except Exception:
            fills = []

        fill_map = {}  # coin -> dernier fill de cloture
        for f in fills:
            coin = f.get("coin", "")
            if f.get("dir", "") in ("Close Long", "Close Short") or f.get("reduceOnly", False):
                if coin not in fill_map:
                    fill_map[coin] = f  # on prend le plus recent

        for slot_key_raw, pos in saved_positions.items():
            # v4.126 — FIX BUG CRITIQUE (2 niveaux) : 1) comparait la cle de
            # sauvegarde COMPLETE (ex: "NEAR_10", voire "ACCUM__NEAR_10")
            # directement contre les tickers BRUTS d Hyperliquid (ex:
            # "NEAR") — ces deux formats ne pouvaient JAMAIS correspondre,
            # ce qui signifie que TOUTE position sauvegardee etait
            # systematiquement traitee comme "fermee pendant la
            # deconnexion" (faux "ghost trade"), meme si elle etait encore
            # bel et bien ouverte. 2) le code appelant refaisait la MEME
            # erreur de comparaison en sens inverse, empechant de toute
            # facon la reintegration correcte. Extrait maintenant le VRAI
            # ticker et detecte le prefixe ACCUM__ pour un routage correct.
            is_accum = slot_key_raw.startswith("ACCUM__")
            slot_key = slot_key_raw[len("ACCUM__"):] if is_accum else slot_key_raw
            coin = ticker_from_slot_key(slot_key)
            if coin in open_coins:
                continue  # position encore ouverte, rien a faire

            # v4.253 — SUR DEMANDE EXPLICITE, FIX BUG CRITIQUE : cette
            # reconciliation compare UNIQUEMENT contre les positions
            # REELLES sur Hyperliquid — une position PAPER n y a jamais
            # existe, par design, donc son absence ne signifie PAS qu elle
            # a ete "fermee via SL/TP Hyperliquid" (aucun ordre reel n a
            # jamais ete place). Sans ce filtre, TOUTE position paper
            # sauvegardee etait a tort traitee comme "fermee pendant la
            # deconnexion" avec un motif "SL/TP HYPERLIQUID" trompeur,
            # confirme par un cas reel (trades Accumulation PAPER affichant
            # ce motif). Ignore desormais completement les positions paper
            # dans cette reconciliation — gerees normalement par ailleurs
            # (aucune synchronisation exchange necessaire pour elles).
            if pos.get("effective_mode", "paper") != "live":
                continue

            # La position a disparu pendant la deconnexion
            entry  = pos["entry"]
            size   = pos["size"]
            ptype  = pos["type"]

            fill    = fill_map.get(coin)
            exit_px = float(fill["px"]) if fill else pos["sl"]
            reason  = "SL/TP HYPERLIQUID" if fill else "SL ESTIME"

            if ptype == "long":
                pnl_pct = (exit_px - entry) / entry * 100
            else:
                pnl_pct = (entry - exit_px) / entry * 100
            pnl_usd = size * pnl_pct / 100
            win     = pnl_usd > 0

            ghost_trades.append({
                "trade_uid": pos.get("trade_uid"),  # v4.264 — fermeture en base par identifiant exact
                "strategy":  pos.get("strategy", "forex"),
                "trade_mode": "live",
                "symbol":  coin,
                "slot_key": slot_key,  # v4.126 — pour un routage fiable cote appelant
                "is_accum": is_accum,  # v4.126 — sait si ca va dans accum_states
                "type":    ptype,
                "entry":   entry,
                "exit":    exit_px,
                "pnl":     round(pnl_usd, 4),
                "pnl_pct": round(pnl_pct, 2),
                "reason":  reason,
                "win":     win,
                "ts":      time.time(),
            })
            print(f"[RECONCILE] {coin} ferme pendant deconnexion | {reason} @ ${exit_px:.2f} | PnL: ${pnl_usd:.2f}")

    except Exception as e:
        print(f"[RECONCILE] Erreur : {e}")
    return ghost_trades, real_entry_prices

def emergency_close_all(exchange, info, wallet_address, cfg):
    """Fermeture d'urgence de toutes les positions ouvertes sur Hyperliquid.
    Utilisé si la reprise est impossible.
    """
    try:
        # v4.264 — tous les DEX (forex xyz compris)
        exch_positions = fetch_exchange_positions(info, wallet_address, cfg.get("SYMBOLS", [])) or {}
        for coin, ep in exch_positions.items():
            szi = ep["szi"]
            if szi == 0:
                continue
            try:
                if is_spot(coin, cfg):
                    api_ticker = cfg.get("SPOT_TICKER_MAP", {}).get(coin, coin)
                    sz = abs(round(szi, 6))
                    exchange.market_open(api_ticker, szi < 0, sz)  # vendre si long, racheter si short
                else:
                    exchange.market_close(coin)
                print(f"[URGENCE] {coin} ferme avec succes")
            except Exception as e:
                print(f"[URGENCE] Erreur fermeture {coin} : {e}")
    except Exception as e:
        print(f"[URGENCE] Erreur recuperation positions : {e}")

def ticker_from_slot_key(slot_key):
    """Extrait le vrai ticker API depuis une cle slot.
    "BTC_0"  → "BTC"
    "SOL_2"  → "SOL"
    "BTC_1"  → "BTC"  (deuxieme slot BTC)
    Si pas de suffixe _N, retourne tel quel (compatibilite).
    """
    parts = slot_key.rsplit("_", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0]
    return slot_key


def get_prices(info, slot_keys, cfg):
    """Recupere les prix mark price pour une liste de slot_keys."""
    try:
        meta, asset_ctxs = info.meta_and_asset_ctxs()
        universe = meta.get("universe", [])

        mark_prices = {}
        for i, ctx in enumerate(asset_ctxs):
            if i < len(universe) and ctx and ctx.get("markPx"):
                name = universe[i].get("name", "")
                try:
                    mark_prices[name] = float(ctx["markPx"])
                except (ValueError, TypeError):
                    pass

        result = {}
        for k in slot_keys:
            t = ticker_from_slot_key(k)
            if t in mark_prices and mark_prices[t] > 0:
                result[k] = mark_prices[t]

        missing = [k for k in slot_keys if k not in result]
        if missing:
            mids = info.all_mids()
            for k in missing:
                t = ticker_from_slot_key(k)
                if t in mids:
                    try:
                        v = float(mids[t])
                        if v > 0:
                            result[k] = v
                    except (ValueError, TypeError):
                        pass

        # v4.168 — SUR DEMANDE EXPLICITE : les tickers forex (HIP-3, DEX
        # "xyz") ne sont JAMAIS presents dans meta_and_asset_ctxs()/all_mids()
        # SANS le parametre "dex" — meme cause que pour la souscription
        # WebSocket, corrigee ici pour le chemin REST (cycle classique).
        # Repli specifique, ne s active que si des tickers forex manquent
        # encore apres les tentatives ci-dessus.
        forex_syms = set(cfg.get("FOREX_MODE_SYMBOLS", []))
        still_missing = [k for k in slot_keys if k not in result and ticker_from_slot_key(k) in forex_syms]
        if still_missing:
            try:
                forex_mids = info.all_mids(dex="xyz")
                for k in still_missing:
                    t = ticker_from_slot_key(k)
                    if t in forex_mids:
                        try:
                            v = float(forex_mids[t])
                            if v > 0:
                                result[k] = v
                        except (ValueError, TypeError):
                            pass
            except Exception as e:
                print(f"[FOREX-PRICES] Echec recuperation prix forex (dex xyz) : {e}")

        return result

    except Exception as e:
        try:
            mids = info.all_mids()
            result = {}
            for k in slot_keys:
                t = ticker_from_slot_key(k)
                if t in mids:
                    try:
                        v = float(mids[t])
                        if v > 0:
                            result[k] = v
                    except (ValueError, TypeError):
                        pass
            return result
        except Exception:
            return {}
def is_spot(symbol, cfg):
    # symbol peut etre une slot_key "BTC_0" — extraire le vrai ticker
    return ticker_from_slot_key(symbol) in cfg.get("SPOT_SYMBOLS", [])

def place_order(exchange, symbol, is_buy, size_usd, price, cfg, sl_price=None, tp_price=None, leverage=1, trade_uid=None):
    """Passe un ordre market d entree avec SL et TP sur Hyperliquid.
    symbol peut etre une slot_key "BTC_0" — le vrai ticker est extrait automatiquement.

    v4.5 — FIX CRITIQUE : size_usd est la MARGE (E), pas le notionnel. Sur
    Hyperliquid (et tout exchange a marge), le levier reduit la marge
    REQUISE pour un notionnel donne — il n augmente PAS automatiquement la
    quantite d un ordre dimensionne sur la marge seule. Avant ce fix,
    l ordre reel envoye ne representait toujours qu un notionnel de 1x
    (size_usd / price), quel que soit le levier applique via
    update_leverage() juste avant : un trade "x3" n ouvrait en realite
    qu une position de la meme taille qu un trade "x1", desynchronisant
    completement le PnL reel du compte Hyperliquid par rapport a la
    logique interne du bot (SL/TTP, calcules eux sur E x levier). Le
    notionnel reel doit etre size_usd x leverage.
    """
    ticker = ticker_from_slot_key(symbol)
    # v4.98 — SUR DEMANDE EXPLICITE : recupere la VRAIE precision (szDecimals)
    # de cet actif precis depuis l API Hyperliquid (mise en cache 10 min),
    # au lieu d un arrondi fixe (4 ou 6 decimales) applique aveuglement a
    # tous les actifs — cause probable d une partie des "Ordre non execute"
    # observes (Hyperliquid rejette immediatement un ordre dont la taille
    # ou le prix ne respecte pas la precision exacte exigee pour CET actif).
    asset_is_spot = is_spot(symbol, cfg)
    try:
        perp_map, spot_map = _get_sz_decimals_map(exchange.info)
        sz_decimals = spot_map.get(ticker, 4) if asset_is_spot else perp_map.get(ticker, 4)
    except Exception as e:
        print(f"[SZDECIMALS] Repli sur 4 decimales par defaut pour {ticker} : {e}")
        sz_decimals = 4
    try:
        if asset_is_spot:
            # Spot n a pas de notion de levier — inchange.
            sz = format_size_hl(max(size_usd / price, 0), sz_decimals)
            api_ticker = cfg.get("SPOT_TICKER_MAP", {}).get(ticker, ticker)
            result = exchange.market_open(api_ticker, is_buy, sz)
            entry_ok = result and result.get("status") == "ok"
            spot_err_msg = None
            real_fill_price_spot = None
            if not entry_ok:
                spot_err_msg = str(result)[:300] if result else "Aucune reponse de l'exchange (spot)"
            else:
                # v4.226 — meme extraction du vrai prix de remplissage que
                # le chemin perp, voir commentaire detaille plus bas.
                try:
                    spot_statuses = result.get("response", {}).get("data", {}).get("statuses", [])
                    if spot_statuses and isinstance(spot_statuses[0], dict) and "filled" in spot_statuses[0]:
                        real_fill_price_spot = float(spot_statuses[0]["filled"]["avgPx"])
                except (KeyError, TypeError, ValueError, AttributeError):
                    pass

            if entry_ok:
                position_mock = {"type": "long" if is_buy else "short", "entry": price, "size": size_usd}
                protective_orders = []

                if sl_price is not None:
                    sl_order = _build_sl_order(ticker, position_mock, sl_price, cfg, sz_decimals, True)
                    if sl_order:
                        protective_orders.append(sl_order)
                    else:
                        print(f"[ORDER] SL spot {ticker} : asset ID inconnu — protection interne uniquement")

                if tp_price is not None:
                    tp_order = _build_tp_order(ticker, position_mock, tp_price, cfg, sz_decimals, True)
                    if tp_order:
                        protective_orders.append(tp_order)
                    else:
                        print(f"[ORDER] TP spot {ticker} : asset ID inconnu — gere en interne uniquement")

                if protective_orders:
                    prot_result = exchange.bulk_orders(protective_orders, grouping="na")
                    prot_ok = prot_result and prot_result.get("status") == "ok"
                    if not prot_ok:
                        print(f"[ORDER] SL/TP spot {ticker} non poses — protection interne uniquement")

            return entry_ok, spot_err_msg, real_fill_price_spot

        # ── PERP : entree + SL + TP en groupe atomique normalTpsl ──
        notional_usd = size_usd * max(leverage, 1)
        # v4.100 — SUR DEMANDE EXPLICITE : Hyperliquid exige un notionnel
        # minimum de $10 par ordre — verifie AVANT de tenter l appel API,
        # pour eviter un echec systematique et donner un message clair
        # plutot qu attendre le rejet de l exchange (observe : GMX avec
        # $5.49 de notionnel, bien en dessous du minimum).
        if notional_usd < 10.0:
            err_msg = f"Notionnel ${notional_usd:.2f} sous le minimum Hyperliquid de $10 — augmentez la taille par trade ou le levier."
            print(f"[ORDER] {ticker} : {err_msg}")
            return False, err_msg, None
        sz = format_size_hl(max(notional_usd / price, 0), sz_decimals)
        print(f"[SIZE-DIAG] {ticker} : size_usd(E)={size_usd} | leverage={leverage} | notional_usd={notional_usd} | price={price} | sz_decimals={sz_decimals} | sz calcule={sz}")
        # v4.5 — pos_mock["size"] doit etre le NOTIONNEL reel (deja leverage)
        # pour que _build_sl_order/_build_tp_order calculent la meme
        # quantite sz que l ordre d entree ci-dessus — sinon les ordres
        # protecteurs ne couvriraient qu une fraction (1/levier) de la
        # position reellement ouverte.
        pos_mock = {"type": "long" if is_buy else "short", "entry": price, "size": notional_usd}

        # v4.166 — SUR DEMANDE EXPLICITE : les marches HIP-3 (forex) exigent
        # le nom COMPLET "dex:coin" (ex: "xyz:EUR") pour le PASSAGE D ORDRE
        # specifiquement (le SDK Hyperliquid officiel route via
        # coin.split(":")[0] en interne) — contrairement a allMids, qui
        # utilise un ticker BRUT avec un parametre "dex" separe. Les deux
        # conventions coexistent, gerees ici uniquement pour l ordre.
        # v4.169 — FIX : ticker contient DEJA le nom complet "xyz:EUR" nativement
        # (SYMBOLS/FOREX_SYMBOLS le stockent ainsi) — aucune
        # transformation supplementaire necessaire, contrairement a la
        # version precedente qui aurait double le prefixe.
        order_ticker = ticker

        entry_order = {
            "coin":        order_ticker,
            "is_buy":      is_buy,
            "sz":          sz,
            "limit_px":    format_price_hl(price * 1.01 if is_buy else price * 0.99, sz_decimals, False),
            "order_type":  {"limit": {"tif": "Ioc"}},
            "reduce_only": False,
        }
        # v4.264 — identifiant de trade HyperBot transmis comme cloid : relie
        # sans ambiguite la position reelle a son mode source et a son heure
        # d ouverture (voir make_trade_uid / reprise au redemarrage).
        if trade_uid:
            try:
                from hyperliquid.utils.types import Cloid
                entry_order["cloid"] = Cloid.from_str(trade_uid)
            except Exception as e_cloid:
                print(f"[ORDER] cloid {trade_uid} non applique ({e_cloid}) — ordre envoye sans identifiant client.")
        orders = [entry_order]

        if sl_price is not None:
            sl_order = _build_sl_order(order_ticker, pos_mock, sl_price, cfg, sz_decimals, False)
            if sl_order:
                orders.append(sl_order)

        if tp_price is not None:
            tp_order = _build_tp_order(order_ticker, pos_mock, tp_price, cfg, sz_decimals, False)
            if tp_order:
                orders.append(tp_order)

        grouping = "normalTpsl" if len(orders) > 1 else "na"
        result = exchange.bulk_orders(orders, grouping=grouping)

        if result and result.get("status") == "ok":
            statuses = result.get("response", {}).get("data", {}).get("statuses", [])
            if statuses and "error" in statuses[0]:
                err_msg = statuses[0]["error"]
                print(f"[ORDER] Erreur place_order {ticker} : {err_msg}")
                return False, err_msg, None
            # v4.226 — SUR DEMANDE EXPLICITE, FIX BUG CRITIQUE : extrait le
            # VRAI prix moyen de remplissage (avgPx) confirme par
            # Hyperliquid — jusqu ici totalement ignore, le bot utilisait
            # son propre prix VISE (celui vu au moment de decider d ouvrir),
            # different du prix REELLEMENT execute (slippage). Confirme par
            # un cas reel : $0.03 d ecart de PnL entre le bot et
            # Hyperliquid sur un trade de seulement 7 minutes, sans lien
            # avec le funding. Utiliser ce prix reel comme point de
            # reference elimine cet ecart a la source, essentiel pour un
            # timing SL/TTP precis (une seule source de verite au lieu de
            # deux qui divergent).
            real_fill_price = None
            if statuses and isinstance(statuses[0], dict) and "filled" in statuses[0]:
                try:
                    real_fill_price = float(statuses[0]["filled"]["avgPx"])
                except (KeyError, TypeError, ValueError):
                    pass
            print(f"[FILL-PRICE-DIAG] {ticker} : statuses brut = {statuses} | real_fill_price extrait = {real_fill_price}")
            return bool(statuses), None, real_fill_price
        err_msg = str(result)[:300] if result else "Aucune reponse de l'exchange"
        print(f"[ORDER] Echec place_order {ticker} : {err_msg}")
        return False, err_msg, None

    except Exception as e:
        print(f"[ORDER] Erreur place_order {ticker} : {e}")
        return False, str(e), None

# v4.265 — prix REEL d execution de la derniere fermeture confirmee, par
# ticker (moyenne ponderee des remplissages) — lu par _safe_close_position
# pour calculer le PnL sur le vrai prix Hyperliquid, pas sur le prix vu par
# le bot au moment de decider.
_LAST_CLOSE_FILL = {}


def _record_fill(fills, result):
    status = _order_first_status(result) if result else None
    if status and "filled" in status:
        try:
            fills.append((float(status["filled"]["totalSz"]), float(status["filled"]["avgPx"])))
        except (KeyError, TypeError, ValueError):
            pass


def _weighted_fill_price(fills):
    total = sum(sz for sz, _ in fills)
    return sum(sz * px for sz, px in fills) / total if total > 0 else None


def pop_last_close_fill(ticker):
    """Prix reel de la derniere fermeture confirmee de ce ticker (ou None)."""
    return _LAST_CLOSE_FILL.pop(ticker, None)


def close_order(exchange, symbol, position, cfg):
    """Ferme une position REELLE — perp ou spot selon le symbole — et ne
    retourne True que si la fermeture est CONFIRMEE.

    v4.264 — FIX BUG CRITIQUE : l ancienne version renvoyait True des que
    Hyperliquid ACCEPTAIT la requete (status "ok"), meme si l ordre IOC n
    avait rien rempli (statut "error" dans la reponse) — le bot se croyait
    ferme alors que la position restait ouverte. A l inverse, le SDK renvoie
    None quand la position n existe DEJA plus (SL natif declenche entre
    temps), ce qui etait compte comme un echec. Desormais :
      - perp : lit la position reelle (bon DEX, forex compris) AVANT et
        APRES l ordre ; True si elle n existe plus, False sinon ;
      - reliquat partiel : une seconde tentative, puis verification ;
      - position deja absente : True (rien a fermer, deja fait).
    """
    ticker = ticker_from_slot_key(symbol)
    try:
        if is_spot(symbol, cfg):
            if not position:
                print(f"[CLOSE] {ticker} spot : position inconnue — fermeture impossible")
                return False
            try:
                _, spot_map = _get_sz_decimals_map(exchange.info)
                sz_decimals = spot_map.get(ticker, 6)
            except Exception:
                sz_decimals = 6
            sz = format_size_hl(max(position["size"] / position["entry"], 0), sz_decimals)
            api_ticker = cfg.get("SPOT_TICKER_MAP", {}).get(ticker, ticker)
            is_buy = position["type"] == "short"
            _LAST_CLOSE_FILL.pop(ticker, None)
            result = exchange.market_open(api_ticker, is_buy, sz)
            status = _order_first_status(result) if result else None
            ok = bool(result and result.get("status") == "ok" and status and "filled" in status)
            if not ok:
                print(f"[CLOSE] {ticker} spot : fermeture non confirmee — reponse {str(result)[:300]}")
            else:
                fills = []
                _record_fill(fills, result)
                _LAST_CLOSE_FILL[ticker] = _weighted_fill_price(fills)
            return ok

        wallet = exchange_wallet_address(exchange)
        szi_before = get_exchange_position_szi(exchange.info, wallet, ticker)
        if szi_before == 0.0:
            print(f"[CLOSE] {ticker} : aucune position reelle sur Hyperliquid — deja fermee (SL natif ?), fermeture confirmee.")
            return True

        _LAST_CLOSE_FILL.pop(ticker, None)
        fills = []
        for attempt in (1, 2):
            result = exchange.market_close(ticker)
            if result is not None:
                status = _order_first_status(result)
                if result.get("status") != "ok" or (status and "error" in status):
                    print(f"[CLOSE] {ticker} : tentative {attempt} rejetee — {str(result)[:300]}")
                _record_fill(fills, result)
            time.sleep(0.5)  # laisse l etat de compte se mettre a jour
            szi_after = get_exchange_position_szi(exchange.info, wallet, ticker)
            if szi_after == 0.0:
                _LAST_CLOSE_FILL[ticker] = _weighted_fill_price(fills)
                return True
            if szi_after is None:
                print(f"[CLOSE] {ticker} : etat reel illisible apres fermeture — considere NON confirme.")
                return False
            print(f"[CLOSE] {ticker} : position encore ouverte apres tentative {attempt} (szi={szi_after}).")
        return False
    except Exception as e:
        print(f"[CLOSE] Erreur fermeture {ticker} : {type(e).__name__}: {e}")
        return False

def _spot_sl_asset(symbol, cfg):
    """Retourne l asset ID numerique pour les ordres trigger spot (10000 + index).
    Utilise SPOT_SL_ASSET_MAP si disponible, sinon tente de parser le ticker @NNN.
    Retourne None si non resolvable (SL natif impossible).
    """
    asset_map = cfg.get("SPOT_SL_ASSET_MAP", {})
    if symbol in asset_map:
        return asset_map[symbol]
    # Fallback : parser "@182" → 10182
    ticker = cfg.get("SPOT_TICKER_MAP", {}).get(symbol, "")
    if ticker.startswith("@"):
        try:
            return 10000 + int(ticker[1:])
        except ValueError:
            pass
    return None


def _build_sl_order(symbol, position, sl_price, cfg, sz_decimals=4, is_spot_asset=False):
    """Construit un ordre SL trigger avec buffer de securite mark price.
    Long  : trigger decale legerement sous sl_price (buffer vers le bas)
    Short : trigger decale legerement au dessus de sl_price (buffer vers le haut)
    Garantit que le SL ne se declenche pas sur un micro-ecart mark/mid.

    v4.98 — SUR DEMANDE EXPLICITE : sz_decimals REEL de l actif (recupere
    depuis l API, plus un arrondi fixe code en dur qui causait des rejets
    d ordre sur les actifs a precision differente). Idem pour les prix
    (format_price_hl, au lieu d un round(...,2) universel — totalement
    invalide sur un actif a $0.09 comme DOGE, par exemple).
    """
    is_long    = position["type"] == "long"
    close_side = not is_long
    sz         = format_size_hl(max(position["size"] / position["entry"], 0), sz_decimals)

    buffer     = cfg.get("MARK_PRICE_BUFFER_PCT", 0.05) / 100
    trigger_px = format_price_hl(sl_price * (1 - buffer) if is_long else sl_price * (1 + buffer), sz_decimals, is_spot_asset)
    limit_px   = format_price_hl(trigger_px * 0.99 if is_long else trigger_px * 1.01, sz_decimals, is_spot_asset)

    if is_spot(symbol, cfg):
        asset_id = _spot_sl_asset(symbol, cfg)
        if asset_id is None:
            return None
        coin_field = str(asset_id)
    else:
        coin_field = symbol

    return {
        "coin":        coin_field,
        "is_buy":      close_side,
        "sz":          sz,
        "limit_px":    limit_px,
        "order_type":  {"trigger": {"triggerPx": trigger_px, "isMarket": True, "tpsl": "sl"}},
        "reduce_only": True,
    }


def _build_tp_order(symbol, position, tp_price, cfg, sz_decimals=4, is_spot_asset=False):
    """Construit un ordre TP trigger avec buffer de securite mark price.
    Long  : trigger decale legerement au dessus de tp_price (buffer vers le haut)
    Short : trigger decale legerement sous tp_price (buffer vers le bas)
    Garantit que le TP ne se declenche pas trop tot sur un micro-ecart mark/mid.

    v4.98 — meme fix de precision reelle par actif que _build_sl_order.
    """
    is_long    = position["type"] == "long"
    close_side = not is_long
    sz         = format_size_hl(max(position["size"] / position["entry"], 0), sz_decimals)

    buffer     = cfg.get("MARK_PRICE_BUFFER_PCT", 0.05) / 100
    trigger_px = format_price_hl(tp_price * (1 + buffer) if is_long else tp_price * (1 - buffer), sz_decimals, is_spot_asset)
    limit_px   = format_price_hl(trigger_px * 0.99 if is_long else trigger_px * 1.01, sz_decimals, is_spot_asset)

    if is_spot(symbol, cfg):
        asset_id = _spot_sl_asset(symbol, cfg)
        if asset_id is None:
            return None
        coin_field = str(asset_id)
    else:
        coin_field = symbol

    return {
        "coin":        coin_field,
        "is_buy":      close_side,
        "sz":          sz,
        "limit_px":    limit_px,
        "order_type":  {"trigger": {"triggerPx": trigger_px, "isMarket": True, "tpsl": "tp"}},
        "reduce_only": True,
    }


def _get_open_orders_by_type(info, wallet_address, symbol):
    """Retourne les ordres SL et TP actifs sur Hyperliquid pour un symbole.
    Classe les ordres en deux listes : sl_oids et tp_oids.
    """
    sl_oids, tp_oids = [], []
    try:
        dex = _dex_of(ticker_from_slot_key(symbol))
        open_orders = info.open_orders(wallet_address, dex) if dex else info.open_orders(wallet_address)
        for o in open_orders:
            if o.get("coin") != symbol or not o.get("reduceOnly", False):
                continue
            otype = o.get("orderType", "").lower()
            oid   = o["oid"]
            tpsl  = o.get("tpsl", "").lower()
            if tpsl == "sl" or "stop" in otype:
                sl_oids.append(oid)
            elif tpsl == "tp" or "take profit" in otype or "tp" in otype:
                tp_oids.append(oid)
    except Exception as e:
        print(f"[ORDERS] Erreur lecture ordres {symbol} : {e}")
    return sl_oids, tp_oids


def update_sl_on_hyperliquid(exchange, info, wallet_address, symbol, position, new_sl, cfg):
    """Met a jour le Stop Loss sur Hyperliquid pour le trailing SL.
    Supporte les PERP et le SPOT (si l asset ID est connu dans SPOT_SL_ASSET_MAP).
    Strategie ATOMIQUE : pose le nouveau SL en premier, annule l ancien ensuite.
    """
    # Pour le spot, verifier que l asset ID est resolvable
    if is_spot(symbol, cfg) and _spot_sl_asset(symbol, cfg) is None:
        print(f"[TSL] {symbol} spot : asset ID inconnu — SL gere en interne uniquement")
        return

    try:
        # ── ETAPE 1 : construire et poser le NOUVEAU SL ──────────────────────
        new_sl_order = _build_sl_order(symbol, position, new_sl, cfg)
        if new_sl_order is None:
            print(f"[TSL] {symbol} : impossible de construire l ordre SL")
            return

        result = exchange.bulk_orders([new_sl_order], grouping="na")
        new_sl_ok = result and result.get("status") == "ok"

        if not new_sl_ok:
            print(f"[TSL] ECHEC pose nouveau SL {symbol} @ {new_sl:.2f} — ancien SL conserve")
            return

        # ── ETAPE 2 : annuler les ANCIENS SL seulement si le nouveau est confirme ─
        sl_oids, _ = _get_open_orders_by_type(info, wallet_address, symbol)
        if sl_oids:
            cancels = [{"coin": symbol, "oid": oid} for oid in sl_oids]
            exchange.bulk_cancel(cancels)

    except Exception as e:
        print(f"[TSL] Erreur mise a jour SL Hyperliquid {symbol} : {e}")


def cancel_tp_on_hyperliquid(exchange, info, wallet_address, symbol, cfg):
    """Annule le TP fixe sur Hyperliquid quand le Trailing TP s active.
    Supporte les PERP et le SPOT.
    """
    try:
        _, tp_oids = _get_open_orders_by_type(info, wallet_address, symbol)
        if tp_oids:
            cancels = [{"coin": symbol, "oid": oid} for oid in tp_oids]
            exchange.bulk_cancel(cancels)
            print(f"[TTP] TP fixe annule sur Hyperliquid pour {symbol} ({len(tp_oids)} ordre(s))")
        else:
            print(f"[TTP] Aucun TP fixe actif sur Hyperliquid pour {symbol}")
    except Exception as e:
        print(f"[TTP] Erreur annulation TP {symbol} : {e}")


def ensure_sl_on_hyperliquid(exchange, info, wallet_address, symbol, position, cfg):
    """Verifie qu un SL actif existe sur Hyperliquid pour une position donnee.
    Si aucun SL n est detecte, en repose un immediatement.
    Supporte les PERP et le SPOT (si l asset ID est resolvable).
    """
    if is_spot(symbol, cfg) and _spot_sl_asset(symbol, cfg) is None:
        print(f"[GUARD] {symbol} spot : asset ID inconnu — SL natif impossible, protection interne uniquement")
        return

    try:
        sl_oids, _ = _get_open_orders_by_type(info, wallet_address, symbol)

        if sl_oids:
            print(f"[GUARD] {symbol} : SL actif detecte ({len(sl_oids)} ordre(s)) — OK")
            return

        # Aucun SL detecte — reposer un SL de secours immediatement
        # v4.5 — FIX : position["size"] est la MARGE (E), pas le notionnel.
        # _build_sl_order calcule sz = size/entry — sans le levier, le SL de
        # secours ne couvrirait qu une fraction (1/levier) de la position
        # reellement ouverte sur Hyperliquid, laissant le reste sans
        # protection. On reconstruit un pos_mock avec le notionnel reel.
        notional_position = dict(position)
        notional_position["size"] = position.get("size", 0) * max(position.get("leverage", 1), 1)
        rescue_sl = _build_sl_order(symbol, notional_position, position["sl"], cfg)
        if rescue_sl is None:
            print(f"[GUARD] {symbol} : impossible de construire le SL de secours")
            return

        result = exchange.bulk_orders([rescue_sl], grouping="na")
        ok = result and result.get("status") == "ok"
        status = "POSE" if ok else "ECHEC"
        print(f"[GUARD] {symbol} : SL manquant — SL de secours {status} @ ${position['sl']:.2f}")

    except Exception as e:
        print(f"[GUARD] Erreur verification SL {symbol} : {e}")

# ─────────────────────────────────────────────
#  PLAGE HORAIRE
# ─────────────────────────────────────────────
def is_trading_hours(cfg):
    start = cfg["TRADE_HOUR_START"]
    end   = cfg["TRADE_HOUR_END"]
    if start == 0 and end == 24:
        return True
    from datetime import datetime as _dt, timezone, timedelta
    now_utc = _dt.now(timezone.utc)
    month = now_utc.month
    paris_offset = 2 if 3 < month < 10 else 1
    if month == 3 and now_utc.day >= 25:
        paris_offset = 2
    elif month == 10 and now_utc.day >= 25:
        paris_offset = 1
    hour = (now_utc + timedelta(hours=paris_offset)).hour
    return start <= hour < end

def is_forex_open():
    """Vérifie si le marché Forex/Or est ouvert.
    Ouvert : Lundi 00h01 — Vendredi 22h00 (heure Paris)
    Fermé  : Vendredi 22h00 — Lundi 00h01 + chaque nuit 22h-00h01
    Calcul basé sur UTC pour éviter les problèmes de changement d'heure.
    """
    from datetime import datetime as _dt, timezone, timedelta
    now_utc = _dt.now(timezone.utc)
    month = now_utc.month
    paris_offset = 2 if 3 < month < 10 else 1
    if month == 3 and now_utc.day >= 25:
        paris_offset = 2
    elif month == 10 and now_utc.day >= 25:
        paris_offset = 1
    now = now_utc + timedelta(hours=paris_offset)

    weekday = now.weekday()  # 0=Lundi, 4=Vendredi, 5=Samedi, 6=Dimanche
    hour = now.hour
    minute = now.minute

    if weekday == 5:   # Samedi
        return False
    if weekday == 6:   # Dimanche
        return False
    if weekday == 4 and hour >= 22:
        return False
    if hour == 22 or hour == 23:
        return False
    if hour == 0 and minute == 0:
        return False
    return True

def is_crypto_offpeak(cfg):
    """Heures creuses du marche crypto — periode de liquidite generalement
    la plus faible (fin de session US, Europe deja fermee depuis plusieurs
    heures, avant reprise de la session asiatique), calculee en UTC (le
    marche crypto est mondial et 24/7, contrairement au Forex/PAXG deja
    gere separement via FOREX_SYMBOLS).
    Fenetre par defaut : 21h00-23h00 UTC — Europe ferme ~16h UTC, US ferme
    ~20h-22h UTC, Asie ne rouvre que vers minuit UTC : c est le seul moment
    ou les trois grandes sessions sont simultanement creuses.
    Ajustable via CRYPTO_OFFPEAK_HOUR_START_UTC / _END_UTC dans CONFIG.
    Ne bloque que les NOUVELLES entrees ; les positions ouvertes continuent
    d etre gerees normalement.
    """
    from datetime import datetime as _dt, timezone
    start = cfg.get("CRYPTO_OFFPEAK_HOUR_START_UTC", 21)
    end   = cfg.get("CRYPTO_OFFPEAK_HOUR_END_UTC", 23)
    hour  = _dt.now(timezone.utc).hour
    if start <= end:
        return start <= hour < end
    return hour >= start or hour < end  # plage qui traverse minuit UTC

def fetch_cpi_events_from_finnhub(api_key):
    """Recupere les prochaines annonces CPI US depuis l API Economic Calendar
    de Finnhub (https://finnhub.io/docs/api/economic-calendar).
    Retourne une liste de datetime UTC (triee), ou [] en cas d echec/cle absente.
    Necessite une cle Finnhub valide (voir HYPERBOT_FINNHUB_API_KEY).
    Note : l acces a cet endpoint peut necessiter un abonnement Finnhub payant
    selon les conditions actuelles de l API — en cas d echec, le bot continue
    de trader normalement (pas de blackout CPI applique) et logue l erreur.
    """
    if not api_key:
        return []
    import urllib.request, json
    from datetime import datetime as _dt, timezone, timedelta
    try:
        today = _dt.now(timezone.utc).date()
        frm = (today - timedelta(days=1)).isoformat()
        to  = (today + timedelta(days=35)).isoformat()
        url = f"https://finnhub.io/api/v1/calendar/economic?from={frm}&to={to}&token={api_key}"
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        events = data.get("economicCalendar") or data.get("data") or []
        cpi_dates = []
        for ev in events:
            name    = (ev.get("event") or "").upper()
            country = (ev.get("country") or "").upper()
            if "CPI" not in name and "CONSUMER PRICE" not in name:
                continue
            if country not in ("US", "USA", ""):
                continue
            raw = ev.get("time") or ev.get("date") or ""
            dt_val = None
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                try:
                    dt_val = _dt.strptime(raw, fmt).replace(tzinfo=timezone.utc)
                    break
                except ValueError:
                    continue
            if dt_val:
                cpi_dates.append(dt_val)
        return sorted(cpi_dates)
    except Exception as e:
        print(f"[CPI] Erreur recuperation calendrier Finnhub : {e}")
        return []

# ─────────────────────────────────────────────
#  STATE PAR SYMBOLE
# ─────────────────────────────────────────────
# v4.186 — SUR DEMANDE EXPLICITE : taux de frais taker Hyperliquid, utilise
# pour ESTIMER les frais reels payes sur les trades LIVE (ouverture +
# fermeture, le bot utilise des ordres IOC qui sont quasi-toujours taker).
# Valeur de base par defaut (avant reduction VIP/staking eventuelle) —
# ajustable si besoin, mais reste une estimation, pas un chiffre exact lu
# depuis Hyperliquid (l API ne renvoie pas directement le frais par ordre).
FEE_RATE_TAKER_ESTIMATE = 0.00045  # 0.045%

class SymbolState:
    def __init__(self):
        self.position      = None
        self.price_history = deque(maxlen=500)
        self.vol_history   = deque(maxlen=50)
        self.trades        = 0
        self.wins          = 0
        self.pnl           = 0.0
        # v4.89 — SUR DEMANDE EXPLICITE : ne JAMAIS melanger capital virtuel
        # (paper) et capital reel (live) — avant ce fix, self.pnl melangeait
        # les deux, faussant le dimensionnement des trades des DEUX cotes
        # des qu un mode passait en live pendant qu un autre restait en
        # paper. self.pnl reste calcule (= paper_pnl + live_pnl) pour la
        # compatibilite avec l affichage existant, mais le dimensionnement
        # des trades utilise desormais UNIQUEMENT le pot qui correspond au
        # mode effectif de CE trade precis.
        self.paper_pnl     = 0.0
        self.live_pnl      = 0.0
        self.closed_trades = []
        self.current_price = 0.0
        self.current_rsi   = None
        self.current_macd  = None
        self.current_atr_pct = None   # ATR% du dernier cycle — pour affichage dashboard
        self.current_adx = None       # ADX du dernier cycle — force de tendance (mode trend/reversal)
        # v4.32 — memorise le dernier mode trend/reversal retenu, pour
        # hysteresis (voir _process) : evite que le mode bascule a chaque
        # cycle si l ADX oscille juste autour du seuil.
        self.last_rsi_mode = None
        self._last_status_log_ts = None  # limite la frequence du log "latent" (voir _manage_position_impl)
        self.current_sig   = None
        self.prev_macd     = None
        self.prev_sig      = None
        self.collecting    = True
        self.peak_price    = None
        self.trailing_tp_active = False
        self.peak_pnl_usd  = None   # Pic de PnL latent en $ (etage 2 du Trailing TP)
        self.tp_stage      = 0      # 0=inactif, 1=Quick Profit arme, 2=Trailing illimite
        # v4.11 — Protection anticipee ("tier 0") : trailing intermediaire
        # entre 0 et l armement principal (arm1), pour eviter qu un trade qui
        # monte pres du seuil (ex: +0.99%) sans jamais l atteindre ne rende
        # tout son gain en cas de retournement (aucune protection actuelle
        # tant que tp_stage reste a 0).
        self.tier0_armed        = False
        self.tier0_peak_pnl_usd = None
        # v4.18 — SUR DEMANDE EXPLICITE : suit le pic de PnL latent DES
        # L OUVERTURE, independamment des seuils de trailing (tier0_arm=0.5%,
        # arm1=1.0%). Avant ce fix, un trade qui montait a +0.3% sans jamais
        # atteindre 0.5% (seuil du tier0) puis repartait en perte n affichait
        # AUCUN pic dans l historique — alors qu il y en avait bien un. Ce
        # champ est purement informatif/diagnostic : il n influence AUCUNE
        # decision de sortie, contrairement a peak_pnl_usd/tier0_peak_pnl_usd.
        self.absolute_peak_pnl_usd = None
        # v4.15 — SUR DEMANDE EXPLICITE : apres la fermeture d un trade, exige
        # que le croisement EMA (ema_bull/ema_bear) soit RETOMBE puis se soit
        # RECROISE avant d autoriser une reouverture dans le MEME sens — pas
        # juste "le cooldown de 15 min a expire alors que le signal n a
        # jamais bouge". Garantit que chaque reouverture est justifiee par un
        # evenement technique frais et distinct, pas la continuation muette
        # du meme signal qui a deja donne le trade precedent.
        self.long_signal_stale  = False  # True juste apres une fermeture LONG, tant que ema_bull n est pas retombe au moins une fois
        self.short_signal_stale = False  # True juste apres une fermeture SHORT, tant que ema_bear n est pas retombe au moins une fois
        # v4.25/v4.31 — SUR DEMANDE EXPLICITE : apres une FERMETURE (gain OU
        # perte, v4.31 — initialement seulement apres un gain, etendu suite
        # a une serie de 7+ pertes consecutives observee sur ARB LONG) dans
        # un sens donne, la reouverture dans ce MEME sens exige que toutes
        # les conditions d entree soient reunies sur PLUSIEURS cycles
        # CONSECUTIFS (pas un seul) — un signal qui hesite (vrai un cycle,
        # faux le suivant) fait repartir le compteur a zero. Empeche un
        # actif choppy de re-declencher "techniquement frais" (EMA repasse
        # vite) mais pas reellement nouveau, toutes les 40-70 minutes.
        # Le nom "post_win" est reste pour limiter les changements, mais le
        # declenchement couvre desormais TOUTE fermeture (voir close_position
        # et les blocs STOP LOSS / SL SECURITE / TTP dans _manage_position_impl).
        self.post_win_confirm_long  = False
        self.post_win_confirm_short = False
        self.confirm_count_long  = 0
        self.confirm_count_short = 0
        # v4.26 — SUR DEMANDE EXPLICITE : evite qu un signal jamais soutenu
        # 18 cycles d affilee ne bloque l actif INDEFINIMENT dans ce sens.
        # Compte le temps d ATTENTE total (pas remis a zero par un flicker,
        # contrairement a confirm_count) — au-dela de POST_WIN_MAX_WAIT_CYCLES,
        # bascule sur un second indicateur independant (Bollinger) pour
        # decider, puis leve la contrainte dans tous les cas (succes ou non)
        # pour ne jamais rester bloque plus longtemps que ce delai.
        self.post_win_wait_long  = 0
        self.post_win_wait_short = 0
        self.mtf_prices    = deque(maxlen=350)  # v4.130 - 350 pour couvrir 10h+ (300 requis) avec marge
        # v4.36 — SUR DEMANDE EXPLICITE : suivi du plus HAUT/BAS reel entre
        # deux echantillonnages (alimente en temps reel par le WebSocket, pas
        # seulement au moment du cycle) — permet de batir de vraies bougies
        # OHLC (open/high/low/close) au lieu d un simple point de prix tous
        # les ~2 min, et un vrai calcul d ATR (True Range de Wilder,
        # haut-bas-cloture) au lieu de cloture-a-cloture (qui sous-estimait
        # fortement la volatilite reelle — voir _process/calc_atr).
        self.window_high = None   # plus haut vu depuis le dernier point de bougie
        self.window_low  = None   # plus bas vu depuis le dernier point de bougie
        self.candle_history = deque(maxlen=750)  # v4.132 - 750 pour couvrir 24h+ (720 requis) avec marge, (high, low, close) par bougie ~2min
        # v4.202 — SUR DEMANDE EXPLICITE : bougies 1h agregees depuis
        # candle_history (~2min/bougie -> ~30 bougies = 1h), pour le MACD 1h
        # et la tendance dynamique (Accumulation/Spot-Accum uniquement).
        self.candle_history_1h = deque(maxlen=200)  # (high, low, close) par bougie 1h, ~8 jours d historique
        self.current_1h_high = None
        self.current_1h_low = None
        self.current_1h_candles_count = 0  # compteur de bougies ~2min accumulees vers la prochaine bougie 1h
        # v4.202 (suite) — tendance dynamique : direction courante, point de
        # depart (S/R qui s etend aux nouveaux extremes), et compteur de
        # bougies 1h CONSECUTIVES en sens oppose (retournement confirme a 3).
        self.dynamic_trend_direction = None  # "up", "down", ou None (pas encore etabli)
        self.dynamic_trend_support = None    # plus bas atteint depuis le debut de la tendance en cours
        self.rising_support = None           # v4.278 — dernier creux ascendant (achat sur repli)
        self.dynamic_trend_resistance = None  # plus haut atteint depuis le debut de la tendance en cours
        self.dynamic_trend_reversal_streak = 0  # bougies 1h consecutives en sens oppose
        # v4.39 — FIX BUG CRITIQUE : l echantillonnage MTF (bougies + EMA200)
        # se basait sur len(price_history) % MTF_STEP == 0 — hors
        # price_history est une deque PLAFONNEE (maxlen=500), dont la
        # longueur se FIGE definitivement a 500 une fois pleine (surtout
        # apres une RESTAURATION depuis la sauvegarde, ou elle peut deja
        # etre pleine des le premier cycle). Si ce reste fige n est jamais 0,
        # l echantillonnage gele SILENCIEUSEMENT pour toujours (bougies et
        # EMA200 cessent tous les deux de progresser). Remplace par un
        # compteur de cycles dedie, qui ne se fige jamais.
        self.cycle_count = 0
        # v4.40 — SUR DEMANDE EXPLICITE : instantane de l etat de TOUTES les
        # portes d entree (RSI, MACD, amplitude, niveaux, fraicheur,
        # confirmation post-trade, separation EMA200...) capture a CHAQUE
        # cycle — expose a la demande via /api/entry-diagnostics/{ticker},
        # pour comprendre en direct pourquoi un actif ne trade pas, sans
        # devoir chasser les bons logs dans une fenetre horaire expiree.
        self.last_gate_snapshot = {}
        # v4.43 — Suivi du trailing Spot-Accumulation (arme une fois a
        # SPOT_ACCUM_TTP_ARM_PCT, puis trailing depuis le pic). Reinitialise
        # a chaque ouverture/fermeture d une position de ce mode.
        self.spot_accum_armed = False
        self.spot_accum_peak_pnl_pct = None
        # v4.47 — compteur de cycles consecutifs ou le prix est reste sous
        # l EMA200 (retournement de tendance) — remis a zero des que le prix
        # repasse au-dessus.
        self.spot_accum_reversal_count = 0
        # v4.64 — SUR DEMANDE EXPLICITE : meme mecanisme de retournement
        # confirme, applique a Accumulation (LONG et SHORT).
        self.accumulation_reversal_count = 0
        # v4.75 — compteurs de stabilite de la tendance (cycles consecutifs).
        self.trend_up_streak = 0
        self.trend_down_streak = 0
        # v4.119 — SUR DEMANDE EXPLICITE : compteur de cycles CONSECUTIFS ou
        # le PnL est reste sous 50% du pic, pour les PETITS pics (< 1%) —
        # distingue un repli SOUTENU d un simple aller-retour ponctuel, avant
        # de fermer. Remis a 0 des que le PnL repasse au-dessus du seuil.
        self.small_peak_giveback_streak = 0
        # v4.55 — instantane diagnostic de chaque clause d entree
        # Spot-Accumulation, expose via /api/entry-diagnostics.
        self.spot_accum_gate_snapshot = {}
        # v4.80 — SUR DEMANDE EXPLICITE : meme diagnostic detaille pour
        # Accumulation, qui n en avait aucun jusqu ici.
        self.accumulation_gate_snapshot = {}
        # v4.21 — Historique des valeurs d indicateurs calculees (RSI, MACD,
        # EMA200, ATR, support/resistance) a chaque cycle, pour affichage en
        # graphe cote interface (diagnostic/surveillance). Purement
        # informatif — n influence aucune decision de trading.
        self.indicator_history = deque(maxlen=300)
        self.forex_was_open  = None
        self.forex_reopen_time = None
        self.last_price_time = None   # Horodatage du dernier prix enregistre
        self.prev_ema_s      = None   # EMA courte du cycle precedent (detection pivot)
        self.prev_ema_l      = None   # EMA longue du cycle precedent (detection pivot)
        self.consec_bull     = 0      # Nombre de cycles consecutifs haussiers (EMA bull)
        self.consec_bear     = 0      # Nombre de cycles consecutifs baissiers (EMA bear)
        # v4.9 — Cooldown de reentree dans le MEME sens apres une fermeture
        # (voir close_position / _check_reentry_cooldown)
        self.last_closed_direction = None
        self.last_closed_at        = None

    def reset_indicators(self):
        """Reinitialise les donnees de prix et indicateurs techniques.
        Appele a la reactivation d une paire si les donnees sont trop anciennes.
        Preserve : trades, PnL, position ouverte, historique des trades fermes.
        """
        self.price_history  = deque(maxlen=500)
        self.vol_history    = deque(maxlen=50)
        self.mtf_prices     = deque(maxlen=350)  # v4.130 - 350 pour couvrir 10h+ (300 requis) avec marge
        self.window_high = None
        self.window_low  = None
        self.candle_history = deque(maxlen=750)  # v4.132 - coherent avec le nouveau maxlen
        self.candle_history_1h = deque(maxlen=200)
        self.current_1h_high = None
        self.current_1h_low = None
        self.current_1h_candles_count = 0
        self.dynamic_trend_direction = None
        self.dynamic_trend_support = None
        self.dynamic_trend_resistance = None
        self.dynamic_trend_reversal_streak = 0
        self.cycle_count = 0
        self.last_gate_snapshot = {}
        self.indicator_history = deque(maxlen=300)
        self.current_rsi    = None
        self.current_macd   = None
        self.current_sig    = None
        self.prev_macd      = None
        self.prev_sig       = None
        self.collecting     = True
        self.last_price_time = None
        self.forex_was_open  = None
        self.prev_ema_s      = None
        self.prev_ema_l      = None
        self.consec_bull     = 0
        self.consec_bear     = 0

    def open_position(self, ptype, entry, sl, tp, size, confidence=None, leverage=1, strategy="forex"):
        self.position = {
            "type": ptype, "entry": entry, "sl": sl, "tp": tp,
            "size": size, "opened_at": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "confidence": confidence, "leverage": leverage,
            "strategy": strategy,  # v4.8 — "forex" ou "accumulation", pour differencier partout en aval
        }
        self.peak_price = entry
        self.trailing_tp_active = False
        self.peak_pnl_usd = None
        self.tp_stage = 0
        self.tier0_armed = False
        self.tier0_peak_pnl_usd = None
        self.absolute_peak_pnl_usd = None
        self.sl_breach_streak = 0  # v4.149 — SUR DEMANDE EXPLICITE : compteur de "patience" SL
        self.ttp_breach_streak = 0  # v4.154 — SUR DEMANDE EXPLICITE : compteur de "patience" TTP
        # v4.267 — FIX BUG : ces variables de trailing (partagees par
        # Spot-Accum ET Funding Contrarian) n etaient remises a zero qu a l
        # ouverture d un trade Spot-Accum. Un trade Funding heritait donc du
        # PIC du trade precedent sur le meme emplacement (ex: +2,32 %) : le
        # trailing etait "arme" des l ouverture et fermait la position dans
        # la minute (WIF, UNI, DOGE, INJ ouverts/fermes a ~0 %, frais perdus).
        self.spot_accum_armed = False
        self.spot_accum_peak_pnl_pct = None
        self.spot_accum_velocity_checkpoint = None
        self.sl_flow_patience_since = None  # v4.270
        # v4.285 — FIX BUG : un signal "etoile filante" detecte AVANT l entree
        # (et deja confirme depuis 30 min) restait memorise sur l actif : la
        # position LONG etait fermee ~6 secondes apres son ouverture, a ~0 %,
        # en payant les frais (31 trades Spot-Accum sur 92 du 23 au 25/09).
        # Seul un signal apparu APRES l ouverture peut desormais la fermer.
        self.shooting_star_pending_close = None
        self.shooting_star_pending_since = None

    def trades_last_24h(self):
        cutoff = datetime.now().timestamp() - 86400
        recent = [t for t in self.closed_trades if t.get("ts", 0) >= cutoff]
        count = len(recent)
        wins = sum(1 for t in recent if t["win"])
        pnl = sum(t["pnl"] for t in recent)
        wr = wins / count * 100 if count > 0 else 0.0
        return {"trades": count, "wins": wins, "pnl": pnl, "win_rate": wr}

    def update_trailing_stop(self, price, delta_pct):
        if not self.position:
            return
        pos = self.position
        delta = delta_pct / 100
        if pos["type"] == "long":
            if price > self.peak_price:
                self.peak_price = price
                new_sl = self.peak_price * (1 - delta)
                if new_sl > pos["sl"]:
                    pos["sl"] = new_sl
        elif pos["type"] == "short":
            if price < self.peak_price:
                self.peak_price = price
                new_sl = self.peak_price * (1 + delta)
                if new_sl < pos["sl"]:
                    pos["sl"] = new_sl

    def close_position(self, exit_price, reason):
        p = self.position
        if p["type"] == "long":
            pnl_pct = (exit_price - p["entry"]) / p["entry"] * 100
        else:
            pnl_pct = (p["entry"] - exit_price) / p["entry"] * 100
        # v4.2 — FIX CRITIQUE : le levier n etait JAMAIS applique ici, alors
        # qu il l est bien dans le calcul de declenchement (_manage_position_impl).
        # Consequence : un trade a levier x3 se fermait au bon moment (le
        # declenchement SL/TTP, lui, appliquait deja correctement le levier),
        # mais le PnL $ enregistre/comptabilise ne reflétait que le levier x1
        # — sous-evaluant le vrai gain/perte jusqu a 3x sur les trades a
        # levier x2/x3, faussant le capital suivi par le bot ET (en mode live)
        # le desynchronisant du solde reel Hyperliquid.
        pnl_usd = p["size"] * p.get("leverage", 1) * pnl_pct / 100
        self.pnl    += pnl_usd
        # v4.89 — SUR DEMANDE EXPLICITE : alimente EXCLUSIVEMENT le pot qui
        # correspond au mode REEL de CE trade precis (memorise sur la
        # position a son ouverture, voir _finalize_open) — jamais melange
        # avec l autre pot. Repli sur "paper" si absent (positions ouvertes
        # avant ce fix).
        trade_mode = p.get("effective_mode", "paper")
        if trade_mode == "live":
            self.live_pnl += pnl_usd
        else:
            self.paper_pnl += pnl_usd
        self.trades += 1
        win = pnl_usd > 0
        if win:
            self.wins += 1
        # v4.15 — Pic de PnL latent atteint pendant la vie du trade, visible
        # dans le log/l historique de fermeture — que ce soit le pic du
        # trailing principal (peak_pnl_usd) ou celui de la protection
        # anticipee (tier0_peak_pnl_usd) si le trade n a jamais depasse
        # arm1. None si le trade n a jamais ete en profit (ex: SL direct).
        # v4.18 — priorite : pic tier1 (le plus fin) > pic tier0 > pic absolu
        # (des le 1er cycle en profit, couvre les trades qui n ont jamais
        # atteint 0.5% mais ont quand meme ete brievement en profit avant
        # de partir en perte — auparavant invisibles dans l historique).
        peak_pnl_usd_at_close = (
            self.peak_pnl_usd if self.peak_pnl_usd is not None
            else self.tier0_peak_pnl_usd if self.tier0_peak_pnl_usd is not None
            else self.absolute_peak_pnl_usd
        )
        # v4.17 — pic aussi en % de mouvement de prix (prefere par l utilisateur
        # a l affichage en $) : reconverti ici, ou E et le levier de CE trade
        # precis sont connus avec certitude (size/leverage stockes sur la
        # position au moment de son ouverture).
        E_at_close = p.get("size", 0)
        leverage_at_close = p.get("leverage", 1)
        peak_pnl_pct_at_close = (
            round(peak_pnl_usd_at_close / (E_at_close * leverage_at_close) * 100, 3)
            if peak_pnl_usd_at_close is not None and E_at_close and leverage_at_close
            else None
        )
        # v4.186 — SUR DEMANDE EXPLICITE : frais estimes payes a Hyperliquid
        # (ouverture + fermeture), UNIQUEMENT pour les trades LIVE — le
        # paper n a aucun frais reel. Notionnel = E x levier, applique aux
        # deux jambes (ouverture et fermeture) du trade.
        fees_paid = None
        if trade_mode == "live":
            notional = E_at_close * leverage_at_close
            fees_paid = round(notional * FEE_RATE_TAKER_ESTIMATE * 2, 4)
        trade = {
            "time": datetime.now().strftime("%H:%M:%S"), "symbol": "",
            "type": p["type"], "entry": p["entry"], "exit": exit_price,
            "pnl": pnl_usd, "reason": reason, "win": win,
            "ts": datetime.now().timestamp(),
            "strategy": p.get("strategy", "forex"),  # v4.8
            "trade_uid": p.get("trade_uid"),  # v4.264 — identifiant exact du trade
            "trade_mode": trade_mode,  # v4.89 — paper ou live REEL de ce trade precis
            "peak_pnl_usd": peak_pnl_usd_at_close,  # v4.15
            "peak_pnl_pct": peak_pnl_pct_at_close,  # v4.17
            "fees_paid": fees_paid,  # v4.186
            "entry_mechanism": p.get("entry_mechanism", "Non enregistré"),  # v4.243
            # v4.342 — suivi du chemin du PnL (Funding) : voir _funding_pnl_track
            "pnl_min_pct": p.get("pp_min"),
            "pnl_min_after_touch_pct": p.get("pp_min_after"),
            "peak_at_min_after_pct": p.get("pp_peak_at_min"),
            "touch_delay_sec": (round(p["pp_touch_ts"] - p["pp_t0"]) if p.get("pp_touch_ts") and p.get("pp_t0") else None),
            "path_complete": ((1 if p.get("pp_complete") else 0) if "pp_complete" in p else None),
        }
        self.closed_trades.append(trade)
        # v4.3 — FIX FUITE MEMOIRE : seul un historique glissant de 24h est
        # necessaire ici (voir trades_last_24h, seule utilisation reelle de
        # cette liste). Sans purge, self.closed_trades grossit indefiniment
        # pendant toute la duree de vie du process — sur un bot tournant
        # 24h/24 pendant des jours/semaines, ca finit par declencher un
        # "Out of memory" sur Railway. L historique COMPLET reste de toute
        # facon persiste en base (voir db.py / get_all_closed_trades), donc
        # rien n est perdu en purgeant cette copie en memoire.
        cutoff_24h = datetime.now().timestamp() - 86400
        self.closed_trades = [t for t in self.closed_trades if t.get("ts", 0) >= cutoff_24h]
        # v4.9 — Cooldown de reentree : on retient la direction et l heure de
        # CETTE fermeture, pour empecher une reouverture dans le MEME sens
        # avant un delai minimum (voir _check_reentry_cooldown). Un signal
        # dans le sens OPPOSE (retournement) n est pas concerne.
        self.last_closed_direction = p["type"]
        self.last_closed_at = time.time()
        self.last_closed_was_loss = pnl_usd < 0  # v4.269 — delai de re-entree apres perte
        # v4.15 — Exige un croisement EMA frais et distinct avant d autoriser
        # une reouverture dans le MEME sens (voir _process, ou ce flag est
        # leve des que la condition retombe au moins une fois).
        if p["type"] == "long":
            self.long_signal_stale = True
        else:
            self.short_signal_stale = True
        self.position = None
        self.peak_price = None
        self.trailing_tp_active = False
        self.peak_pnl_usd = None
        self.tp_stage = 0
        self.tier0_armed = False
        self.tier0_peak_pnl_usd = None
        self.absolute_peak_pnl_usd = None
        return pnl_usd, win, trade

    def win_rate(self):
        return 0.0 if self.trades == 0 else self.wins / self.trades * 100

# ─────────────────────────────────────────────
#  CAPITAL PERSISTANT — INTERETS COMPOSES
# ─────────────────────────────────────────────
# Fichiers de persistance specifiques au profil (swing/scalp)
# Evite les conflits d ecriture quand 2 instances tournent dans le meme dossier
_PROFILE_SUFFIX = CONFIG.get("PROFILE", "swing")
CAPITAL_FILE = f"hyperbot_capital_{_PROFILE_SUFFIX}.json"
STATE_FILE   = f"hyperbot_session_state_{_PROFILE_SUFFIX}.json"
LOG_FILE     = f"hyperbot_log_{_PROFILE_SUFFIX}.txt"

def write_log(msg, level="info"):
    """Ecrit un message dans le fichier de log avec horodatage.
    Conservation : 24 dernières heures. Epuration automatique si > 500KB.
    Format : 2026-06-14 23:11:26 [LEVEL] message
    """
    from datetime import datetime as _dt, timedelta as _td
    import os
    try:
        timestamp = _dt.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{timestamp} [{level.upper():8s}] {msg}\n"

        # Ajouter la ligne immediatement (leger et rapide)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)

        # Epuration 24h — declenchee si > 500KB (~6h de logs)
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > 500 * 1024:
            cutoff = _dt.now() - _td(hours=24)
            kept = []
            with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
                for l in f:
                    try:
                        line_date = _dt.strptime(l[:19], "%Y-%m-%d %H:%M:%S")
                        if line_date >= cutoff:
                            kept.append(l)
                    except Exception:
                        kept.append(l)
            with open(LOG_FILE, "w", encoding="utf-8") as f:
                f.writelines(kept)

    except Exception:
        pass  # Ne jamais bloquer le bot pour un log

def save_session_state(mode, profile, symbols, active_slots, running):
    """Sauvegarde l etat courant de la session (mode, profil, slots, statut).
    Permet un redemarrage automatique dans le meme etat apres coupure.
    Ecriture atomique (tmp + replace) pour eviter les conflits Windows.
    """
    import json, os
    data = {
        "mode":         mode,
        "profile":      profile,
        "symbols":      symbols,
        "active_slots": sorted(list(active_slots)),
        "running":      running,
    }
    tmp_file = STATE_FILE + ".tmp"
    import time
    for attempt in range(3):
        try:
            with open(tmp_file, "w") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp_file, STATE_FILE)
            return
        except PermissionError as e:
            # Fichier verrouille temporairement (OneDrive/Defender scan en cours)
            # Reessai avec micro-pause — la sauvegarde suivante (60s) reessaiera aussi
            if attempt < 2:
                time.sleep(0.15)
                continue
            print(f"[STATE] Permission refusee apres 3 essais — ignore ({e})")
            try:
                if os.path.exists(tmp_file):
                    os.remove(tmp_file)
            except Exception:
                pass
        except Exception as e:
            print(f"[STATE] Erreur sauvegarde: {e}")
            return

def load_session_state():
    """Charge l etat de la derniere session, ou None si absent/invalide."""
    import json, os
    if not os.path.exists(STATE_FILE):
        return None
    try:
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"[STATE] Erreur lecture: {e} — etat ignore")
        return None


def load_capital(default_capital):
    """Charge le capital depuis le fichier de sauvegarde.
    Si le fichier n'existe pas, utilise le capital par defaut de la config.
    """
    import json, os
    if os.path.exists(CAPITAL_FILE):
        try:
            with open(CAPITAL_FILE, "r") as f:
                data = json.load(f)
            capital = data.get("capital", default_capital)
            sessions = data.get("sessions", 0)
            total_pnl = data.get("total_pnl", 0.0)
            print(f"[CAPITAL] Capital charge: ${capital:.2f} | Sessions: {sessions} | PnL total: ${total_pnl:.2f}")
            return capital, sessions, total_pnl
        except Exception as e:
            print(f"[CAPITAL] Erreur lecture fichier: {e} — capital par defaut utilise")
    return default_capital, 0, 0.0

def save_capital(capital, sessions, total_pnl):
    """Sauvegarde le capital apres chaque session.
    Utilise un fichier temporaire + remplacement atomique pour eviter
    les erreurs de permission Windows (fichier verrouille par un autre processus
    ou marque lecture seule).
    """
    import json, os
    from datetime import datetime as _dt
    data = {
        "capital":    round(capital, 4),
        "sessions":   sessions,
        "total_pnl":  round(total_pnl, 4),
        "last_saved": _dt.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    tmp_file = CAPITAL_FILE + ".tmp"
    import time
    for attempt in range(3):
        try:
            with open(tmp_file, "w") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp_file, CAPITAL_FILE)
            return
        except PermissionError as e:
            if attempt < 2:
                time.sleep(0.15)
                continue
            print(f"[CAPITAL] Permission refusee apres 3 essais — ignore ({e})")
            try:
                if os.path.exists(tmp_file):
                    os.remove(tmp_file)
            except Exception:
                pass
        except Exception as e:
            print(f"[CAPITAL] Erreur sauvegarde: {e}")
            return


BATCH_FILE = f"hyperbot_batch_{_PROFILE_SUFFIX}.json"

def load_batch_entry_size():
    """Charge la taille d entree E figee pour le lot en cours (survit a un
    redemarrage/redeploiement tant que le lot de trades n est pas termine).
    Retourne None si aucun lot n est en cours (E sera recalcule au prochain
    trade, des que le capital le permet)."""
    import json, os
    if os.path.exists(BATCH_FILE):
        try:
            with open(BATCH_FILE, "r") as f:
                data = json.load(f)
            # v4.297 — un lot PAR mode (paper / live) ; ancien format = paper
            if isinstance(data.get("by_mode"), dict):
                return {k: v for k, v in data["by_mode"].items() if v}
            v = data.get("batch_entry_size")
            return {"paper": v} if v else {}
        except Exception as e:
            print(f"[BATCH] Erreur lecture fichier: {e} — E sera recalcule")
    return None

def save_batch_entry_size(value):
    """Sauvegarde la taille d entree E figee pour le lot en cours."""
    import json, os, time
    if isinstance(value, dict):  # v4.297 — un lot par mode
        data = {"by_mode": {k: round(v, 6) for k, v in value.items() if v}}
    else:
        data = {"batch_entry_size": round(value, 6) if value is not None else None}
    tmp_file = BATCH_FILE + ".tmp"
    for attempt in range(3):
        try:
            with open(tmp_file, "w") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp_file, BATCH_FILE)
            return
        except PermissionError as e:
            if attempt < 2:
                time.sleep(0.15)
                continue
            print(f"[BATCH] Permission refusee apres 3 essais — ignore ({e})")
        except Exception as e:
            print(f"[BATCH] Erreur sauvegarde: {e}")
            return


# ─────────────────────────────────────────────
class BotEngine:
    def __init__(self, cfg, event_queue):
        self.cfg = cfg
        self.q = event_queue
        self._recent_entries = {}  # v4.269 — horodatages des entrees recentes par mode
        # v4.14 — SUR DEMANDE EXPLICITE : separation entre le MOTEUR (collecte
        # de prix, indicateurs, WebSocket, gestion des positions ouvertes —
        # doit tourner en continu, sauf vraie panne) et le TRADING (ouverture
        # de NOUVELLES positions — seul ce qui est controle par le bouton
        # Demarrer/Arreter). self.running = moteur actif ; self.trading_enabled
        # = nouvelles entrees autorisees. Les positions deja ouvertes
        # continuent TOUJOURS d etre gerees (SL/TP), meme trading_enabled=False.
        self.running = False
        self.trading_enabled = False
        # v4.33 — cache du funding rate par ticker (rafraichi periodiquement,
        # pas a chaque cycle — voir _refresh_funding_rates_if_due).
        self.funding_rates = {}          # ticker -> taux horaire (float)
        self._funding_last_refresh = 0.0  # timestamp du dernier rafraichissement
        # v4.314 — SUR DEMANDE EXPLICITE : cache de la performance historique
        # REELLE par actif (taux de reussite sur les trades top-down deja
        # fermes), pour ponderer la priorisation des candidats top-down —
        # rafraichi periodiquement (voir _refresh_asset_win_rates_if_due),
        # jamais recalcule a chaque cycle (requete DB couteuse sinon).
        self._asset_win_rates = {}       # ticker -> {"win_rate": 0-100, "n": int} ou absent si echantillon insuffisant
        self._asset_win_rates_last_refresh = 0.0
        self._asset_win_rates_all = {}  # v4.331 : ticker -> {"win_rate", "n"} sur TOUS les modes (hors manuel)
        # Sauvegarder les noms originaux (vrais tickers sans suffixe)
        # Normaliser : si cfg["SYMBOLS"] contient deja des slot_keys, les extraire
        raw_symbols = [ticker_from_slot_key(s) for s in cfg["SYMBOLS"]]
        self._original_symbols = raw_symbols[:]  # ["BTC", "PAXG", "SOL"]
        # v4.210 — SUR DEMANDE EXPLICITE : diagnostic direct des reglages SL
        # structurel au demarrage — verifie s ils correspondent aux
        # defauts attendus ou si un override en base les a modifies.
        print(f"[SL-CONFIG-DIAG] STRUCTURAL_SL_HARD_CAP_PCT={cfg.get('STRUCTURAL_SL_HARD_CAP_PCT', 0.5)} | SPOT_ACCUM_HARD_SL_PCT={cfg.get('SPOT_ACCUM_HARD_SL_PCT', 5.0)} | EXCHANGE_SAFETY_SL_MULT={cfg.get('EXCHANGE_SAFETY_SL_MULT', 2.0)} | SL_PATIENCE_CYCLES={cfg.get('SL_PATIENCE_CYCLES', 10)} | SPOT_ACCUM_SL_ENABLED={cfg.get('SPOT_ACCUM_SL_ENABLED', False)} | SPOT_ACCUM_SL_PCT_OF_PNL={cfg.get('SPOT_ACCUM_SL_PCT_OF_PNL', 1.5)}")

        # Construire les slot_keys proprement : "BTC_0", "PAXG_1", "SOL_2"
        slot_keys = [f"{s}_{i}" for i, s in enumerate(raw_symbols)]
        self.states = {k: SymbolState() for k in slot_keys}
        # v4.121 — SUR DEMANDE EXPLICITE : Accumulation dispose de son PROPRE
        # jeu d emplacements, independant de self.states (partage par
        # Normal/Funding/Spot-Accum) — CETTE LIGNE avait ete perdue lors
        # d une manipulation de fichiers precedente, causant une erreur 500
        # sur tous les endpoints references self.accum_states (deja utilise
        # partout ailleurs dans le fichier, mais jamais initialise).
        self.accum_states = {k: SymbolState() for k in slot_keys}
        cfg["SYMBOLS"] = slot_keys
        self._all_symbols = slot_keys[:]
        # Chargement capital persistant — interets composes
        self.capital, self.sessions, self.total_pnl_all = load_capital(cfg["CAPITAL_USD"])
        # v4.89 — SUR DEMANDE EXPLICITE : pot de capital LIVE, SEPARE du
        # capital paper ci-dessus — synchronise depuis Hyperliquid des
        # qu un mode passe en live (voir sync_capital_from_hyperliquid).
        # Repli sur CAPITAL_USD tant qu aucune synchronisation n a eu lieu.
        self.live_capital_base = cfg["CAPITAL_USD"]
        self.cfg["CAPITAL_USD"] = self.capital
        # v4.1 — E fige par lot de trades (voir _enter_position) : None tant
        # qu aucun lot n est en cours, recalcule au prochain trade ouvert.
        self.batch_entry_sizes = load_batch_entry_size() or {}  # v4.297 — {"paper": E, "live": E}
        self.cycle = 0
        self.info = None
        self.exchange = None
        # Fix v3.1 : evite qu un stop() sans start() prealable (ex: fermeture
        # de l app sans jamais avoir clique DEMARRER) n incremente le compteur
        # de sessions et ne resauvegarde le capital pour rien.
        self._started = False
        # Confiance minimale dynamique par actif (ticker -> seuil %)
        # Absent de ce dict = utilise CONFIDENCE_MIN_PCT (seuil de base)
        self.confidence_thresholds = {}
        # v3.2 — Horodatage de la derniere elevation de seuil par actif (ticker
        # -> datetime UTC) — permet une decroissance automatique apres un
        # delai (CONFIDENCE_RESET_HOURS) si l actif n a pas eu l occasion de
        # gagner entre-temps, pour eviter qu il ne reste bloque indefiniment
        # (plus le seuil est haut, moins il a de chances de se qualifier pour
        # une nouvelle tentative qui lui permettrait de redescendre).
        self.confidence_threshold_set_at = {}
        # v3.2 — File d attente des candidats valides du cycle en cours,
        # classes par confiance decroissante puis executes en priorite dans
        # cet ordre jusqu a MAX_OPEN_TRADES (voir _finalize_pending_candidates).
        self._pending_candidates = []
        # v4.8 — File d attente separee pour les candidats du mode
        # Accumulation (independante des candidats normaux, plafond propre
        # ACCUMULATION_MAX_TRADES — voir _finalize_pending_accumulation_candidates).
        self._pending_accumulation_candidates = []
        # v4.33 — File d attente separee pour les candidats du mode Funding
        # Contrarian (independante, plafond propre FUNDING_MODE_MAX_TRADES).
        self._pending_funding_candidates = []
        # v4.43 — File d attente separee pour Spot-Accumulation.
        self._pending_spot_accum_candidates = []
        # Cache du calendrier CPI Finnhub (evite d appeler l API a chaque cycle)
        self._cpi_events = []
        self._cpi_last_fetch = None
        # ── v3.1 : surveillance temps reel des positions via WebSocket ───────
        # self.lock protege les mutations d etat (position, pnl, ...) car
        # _manage_position peut desormais etre appele soit depuis le thread
        # du cycle (_run, toutes les CYCLE_INTERVAL sec) soit depuis le thread
        # WebSocket du SDK Hyperliquid (_on_ws_allmids, a chaque tick de prix).
        self.lock = threading.Lock()
        self._ws_subscribed = False
        self._last_ws_tick = None   # timestamp (time.time()) du dernier tick allMids recu
        self._ws_was_healthy = None  # None = pas encore evalue ; sert a detecter les transitions
        self._last_ws_reconnect_attempt = None  # limite la frequence des tentatives de reconnexion
        # v4.7 — une coupure WebSocket PROLONGEE (>5 min en continu) doit etre
        # traitee comme un "arret" pour la fiabilite des indicateurs, au meme
        # titre qu un redemarrage complet du process : au-dela de ce delai on
        # ne peut plus faire confiance a la fraicheur des donnees collectees
        # (voir _check_ws_health_alert).
        self._ws_unhealthy_since = None      # timestamp (time.time()) du DEBUT de la coupure en cours
        self._ws_fresh_collection_forced = False  # evite de redeclencher en boucle pour la meme coupure
        self.all_mids = {}  # v3.2 : cache brut de tous les prix Hyperliquid (affichage marche complet)

    # ── Sauvegarde des positions ouvertes pour reconciliation au redemarrage ──
    POSITIONS_FILE = "hyperbot_positions.json"
    CONFIDENCE_FILE = "hyperbot_confidence.json"
    INDICATOR_STATE_FILE = "hyperbot_indicators.json"
    # v4.22 — SUR DEMANDE EXPLICITE : fichier SEPARE pour l historique de
    # diagnostic (graphes RSI/MACD/EMA200/ATR/S-R, voir indicator_history).
    # Contrairement a INDICATOR_STATE_FILE (regle des 5 min, pour la
    # FIABILITE des indicateurs de TRADING), celui-ci est toujours restaure
    # a l identique quelle que soit la duree de la coupure — c est un
    # historique de consultation, pas une donnee de decision, une coupure
    # longue n invalide pas l interet de regarder ce qui s est passe avant.
    INDICATOR_HISTORY_FILE = "hyperbot_indicator_history.json"
    INDICATOR_RESUME_MAX_GAP_SEC = 300  # 5 min — au-dela, on repart en collecte fraiche

    def _save_open_positions(self):
        """Sauvegarde les positions ouvertes (live ET paper depuis v3.2, pour
        survivre a un redemarrage/redeploiement) — indexe par slot_key (ex:
        "BTC_0"), pas par ticker brut, pour eviter toute ambiguite si un
        meme ticker occupait plusieurs emplacements.
        v3.2 — FIX CRITIQUE : sauvegarde aussi l etat du Trailing TP (pic de
        profit atteint, etage Quick Profit/Trailing) — sans ca, un
        redemarrage faisait "oublier" au bot qu une position avait deja
        depasse son pic, lui faisant reprendre une reference basse et rater
        la fermeture qui aurait du se produire (perte de l avantage acquis)."""
        import json, os
        # v4.373 — FIX BUG CRITIQUE : la sauvegarde automatique (toutes les 5 s) demarrait AVANT la reprise des positions. Quand le demarrage
        # prenait plus de 5 s (redeploiement), elle ECRASAIT le fichier avec 0 position, puis la reprise ne trouvait plus rien (Swing Support,
        # Spot-Accum perdus). Aucune ecriture tant que la reprise n est pas terminee.
        if not getattr(self, "_positions_restored", False):
            return
        positions = {}
        for sym, st in self.states.items():
            if st.position:
                snapshot = dict(st.position)
                snapshot["_peak_pnl_usd"] = st.peak_pnl_usd
                snapshot["_tp_stage"] = st.tp_stage
                snapshot["_trailing_tp_active"] = st.trailing_tp_active
                snapshot["_tier0_armed"] = st.tier0_armed  # v4.11
                snapshot["_tier0_peak_pnl_usd"] = st.tier0_peak_pnl_usd  # v4.11
                snapshot["_absolute_peak_pnl_usd"] = st.absolute_peak_pnl_usd  # v4.18
                # v4.63 — FIX BUG CRITIQUE : spot_accum_armed/spot_accum_peak_pnl_pct
                # n etaient JAMAIS sauvegardes — a chaque redemarrage du bot,
                # TOUS les trades Spot-Accum ouverts perdaient leur etat
                # d armement silencieusement, sans se rearmer automatiquement
                # si le prix avait entre-temps refluee sous le seuil (observe :
                # pic +2.64% affiche, jamais arme apres plusieurs redemarrages).
                snapshot["_spot_accum_armed"] = st.spot_accum_armed
                snapshot["_spot_accum_peak_pnl_pct"] = st.spot_accum_peak_pnl_pct
                positions[sym] = snapshot
        # v4.121 — SUR DEMANDE EXPLICITE : sauvegarde aussi les positions
        # Accumulation (emplacement desormais separe) — prefixe "ACCUM__"
        # pour les distinguer au chargement, meme fichier partage.
        for sym, st in self.accum_states.items():
            if st.position:
                snapshot = dict(st.position)
                snapshot["_peak_pnl_usd"] = st.peak_pnl_usd
                snapshot["_tp_stage"] = st.tp_stage
                snapshot["_trailing_tp_active"] = st.trailing_tp_active
                snapshot["_tier0_armed"] = st.tier0_armed
                snapshot["_tier0_peak_pnl_usd"] = st.tier0_peak_pnl_usd
                snapshot["_absolute_peak_pnl_usd"] = st.absolute_peak_pnl_usd
                snapshot["_spot_accum_armed"] = st.spot_accum_armed  # v4.264 — manquaient pour Accumulation
                snapshot["_spot_accum_peak_pnl_pct"] = st.spot_accum_peak_pnl_pct
                positions[f"ACCUM__{sym}"] = snapshot
        try:
            # v4.264 — ecriture ATOMIQUE (fichier temporaire + remplacement) :
            # un arret Railway (SIGTERM) pendant l ecriture ne peut plus
            # laisser un fichier tronque, donc illisible, donc TOUTES les
            # positions perdues au redemarrage.
            # v4.373 : verrou + fichier temporaire unique par fil (deux fils ecrivaient le meme .tmp : "No such file ... .tmp")
            lock = getattr(self, "_pos_save_lock", None)
            if lock is None:
                lock = self._pos_save_lock = threading.Lock()
            with lock:
                tmp_path = f"{self.POSITIONS_FILE}.{threading.get_ident()}.tmp"
                with open(tmp_path, "w") as f:
                    json.dump(positions, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, self.POSITIONS_FILE)
            print(f"[POSITIONS] Sauvegarde OK : {len(positions)} position(s) -> {os.path.abspath(self.POSITIONS_FILE)} (coins: {[ticker_from_slot_key(k) for k in positions]})")
        except Exception as e:
            print(f"[POSITIONS] ERREUR sauvegarde : {e}")

    def _load_saved_positions(self):
        """Charge les positions sauvegardees lors de la derniere session live."""
        import json, os
        abspath = os.path.abspath(self.POSITIONS_FILE)
        if not os.path.exists(self.POSITIONS_FILE):
            print(f"[POSITIONS] Aucun fichier trouve a {abspath} — rien a restaurer.")
            return {}
        try:
            with open(self.POSITIONS_FILE, "r") as f:
                data = json.load(f)
            print(f"[POSITIONS] Chargement OK depuis {abspath} : {len(data)} position(s) trouvee(s) (coins: {[ticker_from_slot_key(k) for k in data]})")
            # v4.264 — le fichier n est PLUS vide apres lecture : si le
            # process s arretait entre cette lecture et la sauvegarde
            # suivante, toutes les positions etaient perdues. La reprise
            # etant desormais idempotente, le fichier est simplement reecrit
            # avec l etat reel a la fin de _restore_positions_at_startup.
            return data if isinstance(data, dict) else {}
        except Exception as e:
            print(f"[POSITIONS] ERREUR lecture : {e}")
            return {}

    # ─────────────────────────────────────────────────────────────────────
    #  v4.264 — REPRISE DES POSITIONS AU DEMARRAGE
    # ─────────────────────────────────────────────────────────────────────
    _TRACKING_EXTRA_KEYS = ("_peak_pnl_usd", "_tp_stage", "_trailing_tp_active", "_tier0_armed",
                            "_tier0_peak_pnl_usd", "_absolute_peak_pnl_usd", "_spot_accum_armed",
                            "_spot_accum_peak_pnl_pct")

    @staticmethod
    def _reset_tracking(target):
        target.peak_pnl_usd = None
        target.tp_stage = 0
        target.trailing_tp_active = False
        target.tier0_armed = False
        target.tier0_peak_pnl_usd = None
        target.absolute_peak_pnl_usd = None
        target.spot_accum_armed = False
        target.spot_accum_peak_pnl_pct = None

    @staticmethod
    def _opened_at_from_iso(iso_str):
        """Horodatage ISO UTC (base) -> format d affichage local du bot."""
        try:
            dt = datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
            return dt.astimezone().strftime("%d/%m/%Y %H:%M:%S")
        except (ValueError, TypeError):
            return None

    def _resolve_slot(self, pool, preferred_slot, ticker):
        """Emplacement a utiliser pour une position restauree : celui d
        origine s il existe encore et est libre, sinon un emplacement libre
        du meme actif."""
        if preferred_slot in pool and not pool[preferred_slot].position:
            return preferred_slot
        return next((k for k, st in pool.items() if ticker_from_slot_key(k) == ticker and not st.position), None)

    def _restore_positions_at_startup(self, any_strategy_live):
        """Reprise UNIQUE et idempotente des positions apres un redemarrage :

        1. Positions SAUVEGARDEES (source principale : elles contiennent le
           mode source, l heure d ouverture, les niveaux, l etat du
           trailing) -> restaurees a l identique dans leur emplacement.
           Une position LIVE est confrontee a Hyperliquid : si elle n y est
           plus, elle a ete fermee pendant la coupure et est cloturee
           proprement (prix de sortie reel si disponible).
        2. Positions REELLES Hyperliquid non couvertes par la sauvegarde
           (fichier perdu, crash) -> identifiees par leur trade_uid : base
           locale, puis cloid de l ordre d entree dans l historique
           Hyperliquid. Le repli "forex" n est utilise qu en dernier recours,
           avec une alerte explicite.
        3. Traces "pending" dont l ordre n a finalement ouvert aucune
           position -> supprimees.
        Si Hyperliquid est illisible, rien n est deduit ni supprime : les
        positions sauvegardees sont restaurees telles quelles."""
        import os
        cfg = self.cfg
        wallet = cfg.get("WALLET_ADDRESS")
        try:                                                          # v4.373 : copie de securite du fichier avant toute reprise
            import shutil
            if os.path.exists(self.POSITIONS_FILE):
                shutil.copy2(self.POSITIONS_FILE, self.POSITIONS_FILE + ".bak")
        except Exception as e:
            print(f"[POSITIONS] copie de securite impossible : {e}")
        saved = self._load_saved_positions() or {}
        real_tickers = sorted({ticker_from_slot_key(k) for k in cfg["SYMBOLS"]})
        saved_tickers = [ticker_from_slot_key(k.replace("ACCUM__", "", 1)) for k in saved]
        has_saved_live = any(isinstance(p, dict) and p.get("effective_mode") == "live" for p in saved.values())

        exch = None
        has_manual_live = bool(getattr(self, "manual", None) is not None and self.manual.live_perp_coins())  # v4.273
        if self.info is not None and wallet and (any_strategy_live or has_saved_live or has_manual_live):
            exch = fetch_exchange_positions(self.info, wallet, real_tickers + saved_tickers)
            if exch is None:
                self.emit("log", {"msg": "⚠️ Positions Hyperliquid illisibles au demarrage — positions sauvegardees restaurees telles quelles, aucune fermeture deduite, aucune trace supprimee.", "level": "warn"})

        claimed = {}
        restored, closed_offline, adopted = 0, 0, 0

        # ── Phase 1 : positions sauvegardees ──
        for key, spos in saved.items():
            if not isinstance(spos, dict) or "type" not in spos or "entry" not in spos:
                continue
            is_accum = key.startswith("ACCUM__")
            saved_slot = key[len("ACCUM__"):] if is_accum else key
            ticker = ticker_from_slot_key(saved_slot)
            pool = self.accum_states if is_accum else self.states
            slot = self._resolve_slot(pool, saved_slot, ticker)
            pos = dict(spos)
            extras = {k: pos.pop(k) for k in list(pos) if k.startswith("_")}
            if slot is None:
                self.emit("log", {"msg": f"[{ticker}] ⚠️ Position sauvegardee ({pos.get('strategy', '?')}) mais aucun emplacement disponible pour cet actif dans la configuration actuelle — non restauree, verifiez-la manuellement.", "level": "error"})
                continue
            target = pool[slot]
            mode_pos = pos.get("effective_mode") or self._position_mode(pos)
            pos["effective_mode"] = mode_pos
            pos["slot_key"] = slot
            action = "LONG" if pos["type"] == "long" else "SHORT"
            if not pos.get("trade_uid"):
                rows = [r for r in db.find_open_trades_for_recovery(ticker, action)
                        if (r.get("strategy") or "forex") == pos.get("strategy", "forex") and r.get("trade_uid")]
                if rows:
                    pos["trade_uid"] = rows[0]["trade_uid"]

            if mode_pos == "live" and exch is not None:
                ep = exch.get(ticker)
                still_open = ep is not None and ((ep["szi"] > 0) == (pos["type"] == "long"))
                if not still_open:
                    target.position = pos
                    self._reset_tracking(target)
                    self._close_offline_position(target, slot, ticker, pos)
                    closed_offline += 1
                    continue
                claimed.setdefault(ticker, set()).add(pos["type"])
                if ep["entry"] > 0 and pos.get("entry") and abs(ep["entry"] - pos["entry"]) / pos["entry"] > 0.0005:
                    self.emit("log", {"msg": f"[{ticker}] Prix d entree corrige avec la valeur reelle Hyperliquid : ${pos['entry']:.6g} -> ${ep['entry']:.6g}", "level": "dim"})
                    pos["entry"] = ep["entry"]

            target.position = pos
            self._reset_tracking(target)
            for k in self._TRACKING_EXTRA_KEYS:
                if k in extras and extras[k] is not None:
                    setattr(target, k[1:], extras[k])
            restored += 1
            self.emit("log", {"msg": f"[{ticker}] Position {pos.get('strategy', 'forex')} {pos['type'].upper()} @ ${pos['entry']:.6g} restauree ({mode_pos}, ouverte le {pos.get('opened_at', '?')})", "level": "warn"})
            if mode_pos == "live" and self.exchange and exch is not None:
                try:
                    ensure_sl_on_hyperliquid(self.exchange, self.info, wallet, ticker, pos, cfg)
                except Exception as e:
                    print(f"[RECOVER] Verification SL {ticker} impossible : {e}")

        # ── Phase 2 : positions reelles non couvertes par la sauvegarde ──
        manual = getattr(self, "manual", None)
        manual_coins = manual.live_perp_coins() if manual is not None else set()
        if exch is not None and manual is not None:
            try:
                manual.reconcile(exch)  # v4.273 — positions manuelles fermees pendant la coupure
            except Exception as e:
                print(f"[RECOVER] Reconciliation manuelle impossible : {e}")
        if exch is not None:
            for coin, ep in exch.items():
                direction = "long" if ep["szi"] > 0 else "short"
                if direction in claimed.get(coin, set()) or coin in manual_coins:
                    continue
                if coin not in real_tickers:
                    self.emit("log", {"msg": f"[{coin}] Position reelle hors configuration du bot (ouverte manuellement ?) — ignoree.", "level": "dim"})
                    continue
                if self._adopt_exchange_position(coin, ep, direction):
                    adopted += 1

            # ── Phase 3 : traces prealables sans position reelle ──
            tracked = {st.position.get("trade_uid") for pool in (self.states, self.accum_states)
                       for st in pool.values() if st.position}
            try:
                for row in db.list_pending_trades():
                    if row["trade_uid"] in tracked:
                        continue
                    ep = exch.get(row["coin"])
                    held = ep is not None and ((ep["szi"] > 0) == (row["action"] == "LONG"))
                    if not held:
                        db.discard_pending_trade(row["trade_uid"])
                        print(f"[RECOVER] Trace pending {row['trade_uid']} ({row['coin']}) sans position reelle — supprimee.")
            except Exception as e:
                print(f"[RECOVER] Nettoyage des traces pending impossible : {e}")

        rebuilt = 0
        try:
            rebuilt = self._rebuild_paper_from_db()
        except Exception as e:
            print(f"[RECOVER] reconstruction depuis la base impossible : {type(e).__name__}: {e}")
        summary = f"Reprise : {restored} position(s) restauree(s), {adopted} retrouvee(s) sur Hyperliquid, {closed_offline} fermee(s) pendant la coupure" \
                  + (f", {rebuilt} reconstruite(s) depuis la base." if rebuilt else ".")
        self.emit("log", {"msg": summary, "level": "ok" if not (adopted or closed_offline or rebuilt) else "warn"})
        print(f"[RECOVER] {summary}")
        self._positions_restored = True            # v4.373 : a partir d ici seulement, la sauvegarde peut ecrire
        self._save_open_positions()

    def _rebuild_paper_from_db(self, max_age_h=96):
        """v4.373 — Filet de securite : une position PAPER Swing Support / Spot-Accum encore 'ouverte' en base (moins de 4 jours) mais absente
        du fichier de positions (ecrase) est reconstruite avec ses niveaux d'origine. Les etats de suivi (pic, armement) repartent de zero.
        Ne touche jamais un emplacement deja occupe."""
        from datetime import timezone, timedelta
        since = (datetime.now(timezone.utc) - timedelta(hours=max_age_h)).strftime("%Y-%m-%dT%H:%M:%S")
        rows = db.recent_open_paper_trades(since, ("swing_support", "spot_accumulation"))
        tracked = {st.position.get("trade_uid") for pool in (self.states, self.accum_states) for st in pool.values() if st.position}
        n = 0
        for row in rows:
            uid = row["trade_uid"]
            if uid in tracked or not row.get("entry_price") or not row.get("stop_loss"):
                continue
            coin, action = row["coin"], row["action"]
            is_accum = bool(row.get("is_accum_slot"))
            pool = self.accum_states if is_accum else self.states
            slot = self._resolve_slot(pool, row.get("slot_key"), coin)
            if slot is None:
                continue
            entry = float(row["entry_price"])
            long_side = action == "LONG"
            lev = row.get("leverage") or 1
            opened_at = self._opened_at_from_iso(row.get("created_at")) or datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            try:
                opened_ts = datetime.fromisoformat(str(row["created_at"]).replace("Z", "+00:00")).replace(tzinfo=timezone.utc).timestamp()
            except Exception:
                opened_ts = time.time()
            pos = {"type": "long" if long_side else "short", "entry": entry, "sl": float(row["stop_loss"]),
                   "tp": float(row["take_profit1"] or entry * (1.02 if long_side else 0.98)), "size": float(row.get("size_usd") or 0),
                   "peak": entry, "opened_at": opened_at, "confidence": row.get("confidence"), "leverage": lev,
                   "strategy": row["strategy"], "effective_mode": "paper", "slot_key": slot, "trade_uid": uid, "recovered": True,
                   "recovered_from_db": True}
            if row.get("sl_pct_used") is not None:
                pos["sl_pct_of_e"] = row["sl_pct_used"]
            if row.get("ttp_arm1_pct_used") is not None:
                pos["ttp_arm1_pct"] = row["ttp_arm1_pct_used"]
            if row["strategy"] == "swing_support":
                ev = db.supsw_event_by_uid(uid) or {}
                pos["engine"] = "supsw"
                pos["supsw"] = {"opened_ts": opened_ts, "zone_low": ev.get("zone_low") or pos["sl"], "zone_high": ev.get("zone_high"),
                                "sl0": pos["sl"], "tp": pos["tp"], "inval": ev.get("inval") or ev.get("zone_low"),
                                "setup": ev.get("setup") or "?", "kind": ev.get("setup_kind") or "?", "ctx": {}, "flow_entry": ev.get("flow"),
                                "signal_t": ev.get("signal_t")}
            if pos["size"] <= 0:
                continue
            target = pool[slot]
            target.position = pos
            self._reset_tracking(target)
            n += 1
            self.emit("log", {"msg": f"[{coin}] Position {row['strategy']} {action} @ ${entry:.6g} RECONSTRUITE depuis la base (fichier de positions ecrase) — niveaux d origine, suivi du pic reparti de zero.", "level": "warn"})
        return n

    def _close_offline_position(self, target, slot, ticker, pos):
        """Position LIVE sauvegardee absente d Hyperliquid : fermee pendant
        la coupure (SL natif, liquidation, fermeture manuelle). Cloture
        comptable avec le prix de sortie reel si un fill est retrouve."""
        exit_px, reason = None, "FERMEE PENDANT COUPURE (prix estime)"
        try:
            fills = self.info.user_fills(self.cfg.get("WALLET_ADDRESS")) or []
            for f in fills:  # plus recents d abord
                if f.get("coin") == ticker and str(f.get("dir", "")).startswith("Close"):
                    exit_px = float(f["px"])
                    reason = "SL/TP HYPERLIQUID (pendant coupure)"
                    break
        except Exception as e:
            print(f"[RECOVER] Fills indisponibles pour {ticker} : {e}")
        if exit_px is None:
            exit_px = target.current_price or pos.get("sl") or pos["entry"]
        pnl, _, trade = target.close_position(exit_px, reason)
        trade["symbol"] = slot
        trade["exit_price_source"] = "hyperliquid" if reason.startswith("SL/TP HYPERLIQUID") else "estime"
        self.emit("trade", trade)
        self.emit("log", {"msg": f"[{ticker}] {reason} @ ${exit_px:.6g} | PnL: ${pnl:.2f} (mode {pos.get('strategy', 'forex')})", "level": "win" if pnl > 0 else "loss"})

    def _adopt_exchange_position(self, coin, ep, direction):
        """Rattache une position REELLE non suivie a son trade d origine
        (mode source, heure, emplacement) via son trade_uid."""
        cfg = self.cfg
        wallet = cfg.get("WALLET_ADDRESS")
        action = "LONG" if direction == "long" else "SHORT"
        row, uid, source = None, None, None
        rows = db.find_open_trades_for_recovery(coin, action)
        if rows:
            row, uid, source = rows[0], rows[0].get("trade_uid"), "base locale"
        if not uid:
            uid_hist = find_trade_uid_in_history(self.info, wallet, coin, direction == "long")
            if uid_hist:
                uid = uid_hist
                row = db.get_trade_by_uid(uid) or row
                source = "identifiant d ordre Hyperliquid (cloid)"
        decoded = decode_trade_uid(uid) if uid else None
        strategy = (row or {}).get("strategy") or (decoded or {}).get("strategy")
        if strategy == "manual" and getattr(self, "manual", None) is not None:
            # v4.273 — position ouverte manuellement : rendue au trading manuel
            self.manual.adopt_orphan(coin, ep, uid, (decoded or {}).get("opened_ts"))
            return True
        identified = strategy is not None
        if not identified:
            strategy = "forex"

        opened_at = self._opened_at_from_iso((row or {}).get("created_at")) if row else None
        if opened_at is None and decoded:
            opened_at = datetime.fromtimestamp(decoded["opened_ts"]).strftime("%d/%m/%Y %H:%M:%S")
        if opened_at is None:
            opened_at = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        is_accum = bool((row or {}).get("is_accum_slot")) or strategy == "accumulation"
        pool = self.accum_states if is_accum else self.states
        slot = self._resolve_slot(pool, (row or {}).get("slot_key"), coin)
        if slot is None:
            self.emit("log", {"msg": f"[{coin}] ⚠️ Position reelle {action} trouvee mais aucun emplacement libre pour la suivre — verifiez-la manuellement sur Hyperliquid.", "level": "error"})
            return False

        leverage = ep.get("leverage") or (row or {}).get("leverage") or 1
        notional = abs(ep["szi"]) * ep["entry"]
        sl_pct = cfg.get("RECOVERY_RESCUE_SL_PCT", 15.0)
        tp_pct = cfg.get("SYMBOL_TP_PCT", {}).get(coin, cfg["TAKE_PROFIT_PCT"])
        entry = ep["entry"]
        pos = {
            "type": direction, "entry": entry,
            "sl": entry * (1 - sl_pct / 100) if direction == "long" else entry * (1 + sl_pct / 100),
            "tp": entry * (1 + tp_pct / 100) if direction == "long" else entry * (1 - tp_pct / 100),
            "size": notional / max(leverage, 1), "peak": entry, "opened_at": opened_at,
            "confidence": (row or {}).get("confidence"), "leverage": leverage,
            "strategy": strategy, "effective_mode": "live", "slot_key": slot,
            "trade_uid": uid or make_trade_uid(strategy), "recovered": True,
        }
        if (row or {}).get("sl_pct_used") is not None:
            pos["sl_pct_of_e"] = row["sl_pct_used"]
        if (row or {}).get("ttp_arm1_pct_used") is not None:
            pos["ttp_arm1_pct"] = row["ttp_arm1_pct_used"]
        target = pool[slot]
        target.position = pos
        self._reset_tracking(target)
        try:
            db.upsert_open_trade({"trade_uid": pos["trade_uid"], "coin": coin, "action": action,
                                  "entry": entry, "strategy": strategy, "trade_mode": "live",
                                  "slot_key": slot, "is_accum_slot": is_accum,
                                  "confidence": pos["confidence"], "leverage": leverage,
                                  "size_usd": pos["size"]})
        except Exception as e:
            print(f"[RECOVER] Ecriture base {coin} impossible : {e}")
        if identified:
            self.emit("log", {"msg": f"[{coin}] Position {strategy} {action} @ ${entry:.6g} retrouvee via {source} — mode et heure d ouverture d origine rétablis ({opened_at}).", "level": "warn"})
        else:
            self.emit("log", {"msg": f"[{coin}] ⚠️ Position reelle {action} @ ${entry:.6g} NON IDENTIFIEE (aucune trace locale ni identifiant HyperBot — ouverte hors du bot ?) — suivie par defaut en mode forex. Verifiez-la.", "level": "error"})
        if self.exchange:
            try:
                ensure_sl_on_hyperliquid(self.exchange, self.info, wallet, coin, pos, cfg)
            except Exception as e:
                print(f"[RECOVER] Verification SL {coin} impossible : {e}")
        return True

    def _save_confidence_thresholds(self):
        """Sauvegarde les seuils de confiance dynamiques par actif (survit aux
        redemarrages/redeploiements) — sans ca, un Max Loss qui avait rendu
        un actif plus exigeant serait oublie au prochain demarrage, comme si
        de rien n etait. v3.2 — sauvegarde aussi l horodatage de chaque
        elevation, pour que la decroissance automatique par delai (voir
        _decay_confidence_thresholds) survive elle aussi aux redemarrages."""
        import json
        try:
            payload = {
                "thresholds": self.confidence_thresholds,
                "set_at": {k: v.isoformat() for k, v in self.confidence_threshold_set_at.items()},
            }
            with open(self.CONFIDENCE_FILE, "w") as f:
                json.dump(payload, f, indent=2)
        except Exception as e:
            print(f"[CONFIANCE] Erreur sauvegarde : {e}")

    def _load_confidence_thresholds(self):
        """Restaure les seuils de confiance dynamiques par actif au demarrage,
        ainsi que leurs horodatages (pour la decroissance automatique).
        Compatible avec l ancien format (juste un dict plat de seuils, sans
        horodatages) pour ne pas perdre une sauvegarde faite avant ce fix."""
        import json, os
        if not os.path.exists(self.CONFIDENCE_FILE):
            return
        try:
            with open(self.CONFIDENCE_FILE, "r") as f:
                data = json.load(f)
            if "thresholds" in data:
                self.confidence_thresholds = {k: float(v) for k, v in data["thresholds"].items()}
                self.confidence_threshold_set_at = {
                    k: datetime.fromisoformat(v) for k, v in data.get("set_at", {}).items()
                }
            else:
                # ancien format : dict plat de seuils, sans horodatages
                self.confidence_thresholds = {k: float(v) for k, v in data.items()}
            if self.confidence_thresholds:
                print(f"[CONFIANCE] Seuils restaures : {self.confidence_thresholds}")
        except Exception as e:
            print(f"[CONFIANCE] Erreur lecture : {e}")

    def _save_indicator_state(self):
        """Sauvegarde l etat des indicateurs (prix collectes, compteurs de
        cycles consecutifs, etc.) avec un horodatage precis. Ne sera restaure
        au demarrage QUE si l arret a dure moins de
        INDICATOR_RESUME_MAX_GAP_SEC secondes (voir _load_indicator_state) —
        au-dela, une collecte fraiche est toujours preferable (un trou de
        donnees trop long fausserait les indicateurs)."""
        import json
        from datetime import timezone
        try:
            snapshot = {}
            for slot_key, st in self.states.items():
                snapshot[slot_key] = {
                    "price_history": list(st.price_history),
                    "vol_history": list(st.vol_history),
                    "mtf_prices": list(st.mtf_prices),
                    "candle_history": list(st.candle_history),  # v4.79 — FIX : jamais sauvegarde avant
                    "collecting": st.collecting,
                    "consec_bull": st.consec_bull,
                    "consec_bear": st.consec_bear,
                    "prev_ema_s": st.prev_ema_s,
                    "prev_ema_l": st.prev_ema_l,
                    "prev_macd": st.prev_macd,
                    "prev_sig": st.prev_sig,
                }
            payload = {"saved_at": datetime.now(timezone.utc).isoformat(), "states": snapshot}
            with open(self.INDICATOR_STATE_FILE, "w") as f:
                json.dump(payload, f)
        except Exception as e:
            print(f"[INDICATEURS] Erreur sauvegarde : {e}")

    def _load_indicator_state_if_recent(self):
        """Restaure l etat des indicateurs UNIQUEMENT si le fichier a ete
        sauvegarde il y a moins de INDICATOR_RESUME_MAX_GAP_SEC secondes —
        evite de reprendre une collecte avec un trou de donnees trop
        important (fausserait RSI/EMA/MACD/ATR)."""
        import json, os
        from datetime import timezone
        if not os.path.exists(self.INDICATOR_STATE_FILE):
            print("[INDICATEURS] Aucun etat sauvegarde trouve — collecte fraiche.")
            return
        try:
            with open(self.INDICATOR_STATE_FILE, "r") as f:
                payload = json.load(f)
            saved_at = datetime.fromisoformat(payload["saved_at"])
            gap = (datetime.now(timezone.utc) - saved_at).total_seconds()
            if gap > self.INDICATOR_RESUME_MAX_GAP_SEC:
                print(f"[INDICATEURS] Etat sauvegarde il y a {gap:.0f}s (> {self.INDICATOR_RESUME_MAX_GAP_SEC}s) — collecte fraiche.")
                return
            restored = 0
            for slot_key, data in payload.get("states", {}).items():
                st = self.states.get(slot_key)
                if not st:
                    continue
                st.price_history = deque(data.get("price_history", []), maxlen=500)
                st.vol_history = deque(data.get("vol_history", []), maxlen=50)
                st.mtf_prices = deque(data.get("mtf_prices", []), maxlen=350)  # v4.130 - coherent avec le nouveau maxlen
                # v4.79 — FIX : candle_history n etait jamais restaure —
                # chaque redemarrage (meme rapide, dans la fenetre de reprise)
                # effacait silencieusement l historique de bougies utilise
                # pour le support/resistance et l ATR reel.
                st.candle_history = deque([tuple(c) for c in data.get("candle_history", [])], maxlen=750)  # v4.132 - coherent avec le nouveau maxlen
                st.collecting = data.get("collecting", True)
                st.consec_bull = data.get("consec_bull", 0)
                st.consec_bear = data.get("consec_bear", 0)
                st.prev_ema_s = data.get("prev_ema_s")
                st.prev_ema_l = data.get("prev_ema_l")
                st.prev_macd = data.get("prev_macd")
                st.prev_sig = data.get("prev_sig")
                restored += 1
            print(f"[INDICATEURS] Etat restaure ({gap:.0f}s d arret) : {restored} actif(s), collecte NON relancee.")
            self.emit("log", {"msg": f"⏩ Reprise rapide : etat des indicateurs restaure ({gap:.0f}s d arret, < {self.INDICATOR_RESUME_MAX_GAP_SEC}s) — pas de nouvelle collecte necessaire.", "level": "ok"})
        except Exception as e:
            print(f"[INDICATEURS] Erreur lecture, collecte fraiche par securite : {e}")

    def _save_indicator_history(self):
        """v4.22 — Sauvegarde l historique de diagnostic (indicator_history :
        RSI/MACD/EMA200/ATR/support-resistance dates, pour les graphes cote
        interface). Fichier SEPARE de _save_indicator_state : celui-ci est
        toujours restaure integralement au demarrage, quelle que soit la
        duree de la coupure — c est un historique de consultation pour
        comprendre a posteriori ce que le bot a fait, pas une donnee dont la
        fraicheur conditionne une decision de trading."""
        import json
        try:
            snapshot = {sym: list(st.indicator_history) for sym, st in self.states.items()}
            with open(self.INDICATOR_HISTORY_FILE, "w") as f:
                json.dump(snapshot, f)
        except Exception as e:
            print(f"[INDICATEURS] Erreur sauvegarde historique diagnostic : {e}")

    def _load_indicator_history(self):
        """v4.22 — Restaure l historique de diagnostic SANS condition de
        delai (contrairement a _load_indicator_state_if_recent) — a appeler
        une seule fois au demarrage."""
        import json, os
        if not os.path.exists(self.INDICATOR_HISTORY_FILE):
            return
        try:
            with open(self.INDICATOR_HISTORY_FILE, "r") as f:
                snapshot = json.load(f)
            restored = 0
            for sym, points in snapshot.items():
                st = self.states.get(sym)
                if not st:
                    continue
                st.indicator_history = deque(points, maxlen=300)
                restored += 1
            print(f"[INDICATEURS] Historique diagnostic restaure : {restored} actif(s).")
        except Exception as e:
            print(f"[INDICATEURS] Erreur lecture historique diagnostic : {e}")

    def force_fresh_collection(self):
        """Force une collecte entierement fraiche pour tous les actifs,
        meme si un etat recent aurait pu etre restaure — utile si on
        soupconne un probleme sur les indicateurs sans attendre un
        redeploiement. Ne touche pas aux positions ouvertes ni au PnL."""
        import os
        for st in self.states.values():
            st.reset_indicators()
        try:
            if os.path.exists(self.INDICATOR_STATE_FILE):
                os.remove(self.INDICATOR_STATE_FILE)
        except Exception as e:
            print(f"[INDICATEURS] Erreur suppression fichier lors du forcage : {e}")

    def reset_confidence_penalties(self):
        """v3.2 — Reinitialisation CIBLEE : remet tous les seuils de confiance
        dynamiques a la base, sans toucher aux positions ouvertes, au
        capital ni a l historique des trades. Utile quand de nombreux actifs
        sont "au frigo" suite a une serie de pertes (ex: pendant des tests
        intensifs), sans avoir a attendre la decroissance automatique (2h
        par defaut) ni a faire une reinitialisation complete destructrice."""
        count = len(self.confidence_thresholds)
        self.confidence_thresholds = {}
        self.confidence_threshold_set_at = {}
        self._save_confidence_thresholds()
        self.emit("log", {"msg": f"🔄 {count} penalite(s) de confiance reinitialisee(s) manuellement — tous les actifs repartent au seuil de base.", "level": "ok"})

    def clear_all_persisted_files(self):
        """v3.2 — FIX CRITIQUE : supprime les fichiers de sauvegarde sur
        disque (positions, seuils de confiance, etat des indicateurs).
        Sans cet appel, une reinitialisation depuis l interface (base de
        donnees + memoire vidées) ne servait a rien : au prochain
        demarrage, le bot relisait ces fichiers restes intacts et
        restaurait les anciennes positions comme si de rien n etait.
        v4.1 — supprime aussi le fichier de capital et le fichier de E fige
        par lot : sans ca, un capital/E perime restait sur disque et etait
        recharge tel quel au prochain demarrage, meme apres une remise a
        zero explicite depuis l interface."""
        import os
        for f in (self.POSITIONS_FILE, self.CONFIDENCE_FILE, self.INDICATOR_STATE_FILE,
                  CAPITAL_FILE, BATCH_FILE, self.INDICATOR_HISTORY_FILE):
            try:
                if os.path.exists(f):
                    os.remove(f)
                    print(f"[RESET] Fichier supprime : {f}")
            except Exception as e:
                print(f"[RESET] Erreur suppression {f} : {e}")
        self.confidence_thresholds = {}
        self.confidence_threshold_set_at = {}
        self.emit("log", {"msg": "🔄 Fichiers de sauvegarde (positions, confiance, indicateurs, capital, lot) purges suite a une reinitialisation.", "level": "warn"})

    def emit(self, etype, data=None):
        self.q.put({"type": etype, "data": data or {}})
        # Sauvegarde simultanee dans le fichier de log
        if etype == "log" and data and "msg" in data:
            write_log(data["msg"], data.get("level", "info"))

    def start_engine(self):
        """v4.14 — Demarre le MOTEUR (connexion Hyperliquid, WebSocket,
        boucle de collecte/gestion) — independamment du trading. Idempotent :
        ne fait rien si deja demarre. A appeler UNE SEULE FOIS, au demarrage
        du process (voir api.py), pas a chaque clic sur Demarrer. Une fois
        lance, tourne en continu (avec reconnexion automatique, voir _run)
        jusqu a l arret du process lui-meme — le bouton Demarrer/Arreter
        n a plus aucune prise dessus, seul self.trading_enabled en depend."""
        if self.running:
            return
        self.running = True
        threading.Thread(target=self._run, daemon=True).start()
        # v3.2 : sauvegarde dediee toutes les 5s, decouplee du cycle de trading
        # (15s) — reduit la fenetre de "fraicheur perdue" en cas de coupure
        # brutale (redeploiement, crash) sans avoir a acce le rythme du cycle
        # de trading lui-meme. Cout negligeable (petit fichier JSON local).
        threading.Thread(target=self._periodic_save_loop, daemon=True).start()

    def start(self):
        """v4.14 — Bouton "Demarrer" : autorise desormais UNIQUEMENT
        l ouverture de nouveaux trades (self.trading_enabled). Le moteur
        (WebSocket, indicateurs, gestion des positions deja ouvertes) tourne
        deja en continu depuis le demarrage du process — ce bouton ne le
        redemarre pas, il n a plus besoin de l etre."""
        if not self.running:
            self.start_engine()  # filet de securite si jamais pas encore lance
        self.trading_enabled = True
        self._started = True
        # Marquer le debut de session dans le fichier log
        write_log(f"{'='*60}", "info")
        write_log(f"TRADING ACTIVE — v{BOT_VERSION} (build {BOT_BUILD}) | mode={self.cfg.get('MODE')} | profil={self.cfg.get('PROFILE')}", "info")
        write_log(f"Actifs : {[s for s in self.cfg.get('SYMBOLS', [])]}", "info")
        write_log(f"{'='*60}", "info")

    def _periodic_save_loop(self):
        while self.running:
            time.sleep(5)
            if not self.running:
                break
            try:
                self._save_open_positions()
                self._save_confidence_thresholds()
                self._save_indicator_history()  # v4.22 — historique diagnostic, sans condition de delai
            except Exception as e:
                print(f"[SAVE-5S] Erreur : {e}")

    def _persist_capital_snapshot(self):
        """v4.3 — FIX RESILIENCE CRITIQUE : sauvegarde le capital courant
        (baseline de la session + PnL realise jusqu ici) APRES CHAQUE TRADE,
        pas seulement lors d un arret propre (stop()). Avant ce fix, un
        redemarrage non-propre (crash, kill Railway, "Out of memory"...)
        perdait TOUT le PnL realise de la session en cours de la
        comptabilite persistee du capital — meme si chaque trade individuel
        restait, lui, correctement enregistre en base (SQLite, via db.py,
        independamment de ce mecanisme). Symptome observe concretement :
        le capital affiche (~96$, -3.98%) ne correspondait plus a la somme
        reelle de tous les trades enregistres (net +0.35$ depuis le debut),
        a cause d un redemarrage force pendant la fuite memoire corrigee en
        v4.3 (voir close_position/self.closed_trades)."""
        total_pnl = sum(s.pnl for s in self.states.values()) + sum(s.pnl for s in self.accum_states.values())
        current_capital = self.cfg["CAPITAL_USD"] + total_pnl
        save_capital(current_capital, self.sessions, self.total_pnl_all)

    def stop(self):
        """v4.14 — Bouton "Arreter" : bloque UNIQUEMENT l ouverture de
        nouveaux trades (self.trading_enabled = False). Les positions deja
        ouvertes continuent d etre gerees normalement (SL/TP actifs) — le
        moteur (WebSocket, indicateurs) continue de tourner sans
        interruption, ne se deconnecte plus a l arret."""
        self.trading_enabled = False
        if not self._started:
            # Jamais demarre (ex: le trading n a jamais ete active) — rien a
            # sauvegarder, evite de gonfler le compteur de sessions.
            return
        self._started = False
        # Sauvegarde du capital pour interets composes
        total_pnl = sum(s.pnl for s in self.states.values()) + sum(s.pnl for s in self.accum_states.values())
        new_capital = self.cfg["CAPITAL_USD"] + total_pnl
        self.sessions += 1
        self.total_pnl_all += total_pnl
        save_capital(new_capital, self.sessions, self.total_pnl_all)
        self.emit("log", {"msg": f"Capital sauvegarde: ${new_capital:.2f} (session #{self.sessions})", "level": "ok"})

    def _sim_prices(self):
        import random
        # Prix de base pour la simulation paper — etendu a tous les actifs courants
        base = {
            "BTC": 63500, "ETH": 1700, "SOL": 68, "BNB": 605,
            "PAXG": 4290, "XRP": 1.17, "DOGE": 0.09, "ADA": 0.17,
            "AVAX": 6.81, "LINK": 8.03, "DOT": 0.99, "UNI": 2.58,
            "NEAR": 2.24, "AAVE": 64.41, "LTC": 43.21, "BCH": 211.0,
            "HYPE": 64.57, "TAO": 217.0, "HBAR": 0.08, "POL": 0.08,
        }
        return {
            k: base.get(ticker_from_slot_key(k), 10.0) * (1 + random.uniform(-0.005, 0.005))
            for k in self.cfg["SYMBOLS"]
        }

    def _send_snapshot(self):
        total_pnl = sum(s.pnl for s in self.states.values()) + sum(s.pnl for s in self.accum_states.values())
        # Stats 24h glissantes
        # v4.121 — SUR DEMANDE EXPLICITE : inclut aussi accum_states, sinon
        # les trades Accumulation disparaissent des stats 24h du dashboard.
        h24_trades = sum(s.trades_last_24h()["trades"] for s in self.states.values()) + sum(s.trades_last_24h()["trades"] for s in self.accum_states.values())
        h24_pnl    = sum(s.trades_last_24h()["pnl"]    for s in self.states.values()) + sum(s.trades_last_24h()["pnl"]    for s in self.accum_states.values())
        h24_wins   = sum(s.trades_last_24h()["wins"]   for s in self.states.values()) + sum(s.trades_last_24h()["wins"]   for s in self.accum_states.values())
        h24_wr     = h24_wins / h24_trades * 100 if h24_trades > 0 else 0.0
        self.emit("snapshot", {
            "cycle": self.cycle,
            "capital": self.capital + total_pnl,
            "total_pnl": total_pnl,
            "in_hours": is_trading_hours(self.cfg),
            "ws_connected": self.info is not None,
            "ws_healthy": self._is_ws_healthy(),
            "h24": {"trades": h24_trades, "pnl": h24_pnl, "win_rate": h24_wr},
            "states": {
                sym: {
                    "price": st.current_price, "rsi": st.current_rsi,
                    "macd": st.current_macd, "pnl": st.pnl,
                    "trades": st.trades, "win_rate": st.win_rate(),
                    "position": st.position, "collecting": st.collecting,
                    "atr_pct": st.current_atr_pct,
                } for sym, st in self.states.items()
            }
        })

    # ── v3.1 : Score de confiance et seuil dynamique par actif ──────────────
    def _score_confidence(self, direction, macd_bull, macd_bear, bb_low_ok, bb_up_ok,
                           vol_ok, ema200, ema_mid, price, momentum_pct,
                           momentum_threshold, consec, min_consec, cfg,
                           support=None, resistance=None):
        """Calcule un score de confiance 0-100% a partir des confirmations
        optionnelles disponibles pour ce signal (en plus des filtres deja
        obligatoires comme RSI/EMA/tendance qui ont deja valide avant d arriver
        ici). Seuls les indicateurs reellement calculables pour ce cycle/symbole
        entrent dans le score : celui-ci est ramene sur 100% du poids
        REELLEMENT disponible, pas du poids total theorique. Ainsi un actif
        sans EMA_MID configure n est pas penalise pour un indicateur absent.

        v4.2 — Retourne aussi le detail brut (quel indicateur etait
        confirme/disponible), pour permettre une calibration ulterieure des
        poids a partir des resultats REELS des trades (voir api.py, module
        de calibration de la confiance). Sans cette trace, impossible de
        savoir apres coup si "MACD confirme" a effectivement correle avec
        des trades gagnants sur CE bot, sur CES marches.
        """
        w = cfg.get("CONFIDENCE_WEIGHTS", {})
        earned, available = 0.0, 0.0
        breakdown = {}

        def add(key, confirmed):
            nonlocal earned, available
            pts = w.get(key, 0)
            available += pts
            if confirmed:
                earned += pts
            breakdown[key] = bool(confirmed)

        add("macd", macd_bull if direction == "long" else macd_bear)
        add("bollinger", bb_low_ok if direction == "long" else bb_up_ok)
        add("volume", vol_ok)

        if ema200 is not None:
            add("ema200", price > ema200 if direction == "long" else price < ema200)
        if ema_mid is not None:
            add("ema_mid", price > ema_mid if direction == "long" else price < ema_mid)
        if momentum_pct is not None:
            # Confirmation franche : momentum au-dela de la moitie du seuil de blocage,
            # dans le sens du signal (pas juste "non oppose")
            half = momentum_threshold / 2
            add("momentum", momentum_pct >= half if direction == "long" else momentum_pct <= -half)

        add("consec", consec > min_consec)

        # v3.2 — Comportement de trader : casser une resistance (LONG) ou un
        # support (SHORT) recent est une vraie confirmation de force, pas
        # juste l absence d obstacle — un trader experimente y voit un
        # signal a part entiere, pas seulement un "pas de blocage".
        if direction == "long" and resistance is not None:
            add("breakout", price > resistance)
        elif direction == "short" and support is not None:
            add("breakout", price < support)

        score = 100.0 if available <= 0 else earned / available * 100
        return score, breakdown

    def _get_confidence_threshold(self, ticker):
        base = self.cfg.get("CONFIDENCE_MIN_PCT", 65.0)
        return self.confidence_thresholds.get(ticker, base)

    def _close_all_trades_for_strategy(self, strategy, paper_only=False):
        """v4.88 — SUR DEMANDE EXPLICITE : ferme TOUTES les positions
        actuellement ouvertes pour une strategie precise (utilise juste
        avant de basculer ce mode en live, sur confirmation explicite de
        l utilisateur) — au prix courant, motif "MANUEL (bascule live)".
        Ne touche a aucune autre strategie. Retourne le nombre de trades
        fermes."""
        closed_count = 0
        for slot_key, state in list(self.states.items()):
            pos = state.position
            if pos and pos.get("strategy", "forex") == strategy:
                if paper_only and self._position_mode(pos) == "live":
                    continue  # v4.298 — la bascule en live ne ferme plus les positions REELLES
                price = state.current_price or pos.get("entry")
                if price is None:
                    continue
                _result = self._safe_close_position(state, price, "MANUEL (bascule live)", ticker_from_slot_key(slot_key), pos, slot_key, None)
                if _result is None:
                    continue  # fermeture reelle non confirmee : position conservee
                pnl, _, trade = _result
                self.emit("trade", trade)
                closed_count += 1
        # v4.121 — SUR DEMANDE EXPLICITE : Accumulation a desormais son
        # PROPRE emplacement (self.accum_states), jamais parcouru ci-dessus.
        if strategy == "accumulation":
            for slot_key, accum_state in list(self.accum_states.items()):
                pos = accum_state.position
                if pos:
                    if paper_only and self._position_mode(pos) == "live":
                        continue  # v4.298
                    price = accum_state.current_price or pos.get("entry")
                    if price is None:
                        continue
                    _result = self._safe_close_position(accum_state, price, "MANUEL (bascule live)", ticker_from_slot_key(slot_key), pos, slot_key, None)
                    if _result is None:
                        continue
                    pnl, _, trade = _result
                    self.emit("trade", trade)
                    closed_count += 1
        if closed_count:
            self._save_open_positions()
        return closed_count

    def _set_gate_blocked(self, state, price, reason):
        """v4.264 — diagnostic d entree : les sorties anticipees de _process
        (collecte, forex ferme, chauffe, horaires, ATR, CPI, prix absent)
        laissaient le diagnostic VIDE ("pas encore de donnees") sans jamais
        dire pourquoi — cas permanent des actifs forex. Enregistre desormais
        la raison exacte du blocage, horodatee."""
        state.last_gate_snapshot = {"ts": time.time(), "price": price, "blocked_reason": reason}

    # ─────────────────────────────────────────────────────────────────────
    #  v4.265 — SUIVI APRES SORTIE (Spot-Accum / Accumulation)
    # ─────────────────────────────────────────────────────────────────────
    def _maybe_start_trade_followups(self):
        """Lance, au plus une fois par minute et dans un fil separe (aucun
        impact sur la reactivite du trading), le suivi des trades fermes
        depuis plus d une heure."""
        if self.info is None:
            return
        now = time.time()
        if now - getattr(self, "_last_followup_run", 0) < 60:
            return
        running = getattr(self, "_followup_thread", None)
        if running is not None and running.is_alive():
            return
        self._last_followup_run = now
        self._followup_thread = threading.Thread(target=self._run_trade_followups, daemon=True)
        self._followup_thread.start()

    def _candles(self, coin, interval, start_ms, end_ms):
        try:
            data = self.info.post("/info", {"type": "candleSnapshot", "req": {
                "coin": coin, "interval": interval, "startTime": int(start_ms), "endTime": int(end_ms)}})
            return data if isinstance(data, list) else []
        except Exception as e:
            print(f"[FOLLOWUP] Bougies {coin} indisponibles : {e}")
            return None

    def _reconcile_live_fills(self):
        """v4.294 — dans la minute qui suit la fermeture d un trade LIVE : frais
        REELS et PnL REEL ("closedPnl") lus dans les remplissages
        Hyperliquid, pour afficher dans l historique le resultat exact du
        compte a cote du calcul du bot."""
        wallet = self.cfg.get("WALLET_ADDRESS")
        if not wallet or self.info is None:
            return
        try:
            rows = db.list_live_trades_needing_fills(limit=10)
        except Exception as e:
            print(f"[FILLS] Lecture base impossible : {e}")
            return
        for row in rows:
            try:
                opened = datetime.fromisoformat(row["created_at"]).timestamp()
                closed = datetime.fromisoformat(row["closed_at"]).timestamp()
                fills = self.info.user_fills_by_time(wallet, int(opened * 1000) - 5_000, int(closed * 1000) + 30_000) or []
            except Exception as e:
                print(f"[FILLS] Remplissages {row.get('coin')} indisponibles : {e}")
                continue
            mine = [f for f in fills if f.get("coin") == row["coin"]]
            if not mine:
                db.save_trade_followup(row["id"], {"fills_status": "aucun remplissage trouve"})
                continue
            fees = sum(float(f.get("fee") or 0) for f in mine)
            closed_pnl = sum(float(f.get("closedPnl") or 0) for f in mine)
            db.save_trade_followup(row["id"], {"fees_real": round(fees, 6), "pnl_real_hl": round(closed_pnl, 6),
                                               "fills_status": f"ok ({len(mine)} remplissages)"})

    def _run_trade_followups(self):
        """Pour chaque trade Spot-Accum / Accumulation ferme depuis plus d
        une heure : prix a +30 et +60 min apres la sortie, plus haut/plus bas
        sur cette heure (bougies Hyperliquid), et pour le live, frais et PnL
        reels issus des remplissages Hyperliquid."""
        self._reconcile_live_fills()  # v4.294 — rapide (une minute apres la fermeture)
        try:
            pending = db.list_trades_needing_followup(limit=5)
        except Exception as e:
            print(f"[FOLLOWUP] Lecture base impossible : {e}")
            return
        wallet = self.cfg.get("WALLET_ADDRESS")
        for row in pending:
            try:
                closed = datetime.fromisoformat(row["closed_at"]).timestamp()
                opened = datetime.fromisoformat(row["created_at"]).timestamp()
            except (TypeError, ValueError):
                db.save_trade_followup(row["id"], {"followup_status": "dates invalides"})
                continue
            age_days = (time.time() - closed) / 86400
            interval, step_ms = ("1m", 60_000) if age_days < 3 else ("5m", 300_000)
            start_ms = int(closed * 1000)
            end_ms = start_ms + 61 * 60_000
            candles = self._candles(row["coin"], interval, start_ms - step_ms, end_ms)
            if candles is None:
                continue  # erreur reseau : nouvel essai a la prochaine passe
            fields = {}
            if candles:
                def close_at(target_ms):
                    best = None
                    for c in candles:
                        if int(c["t"]) <= target_ms:
                            best = c
                    return float(best["c"]) if best else None
                in_hour = [c for c in candles if start_ms - step_ms < int(c["t"]) <= start_ms + 60 * 60_000]
                fields["price_after_30m"] = close_at(start_ms + 30 * 60_000)
                fields["price_after_60m"] = close_at(start_ms + 60 * 60_000)
                if in_hour:
                    fields["high_60m"] = max(float(c["h"]) for c in in_hour)
                    fields["low_60m"] = min(float(c["l"]) for c in in_hour)
            if row.get("trade_mode") == "live" and wallet and not row.get("fills_status"):  # v4.294 : deja releve sinon
                try:
                    fills = self.info.user_fills_by_time(wallet, int(opened * 1000) - 60_000, int(closed * 1000) + 60_000) or []
                    mine = [f for f in fills if f.get("coin") == row["coin"]]
                    if mine:
                        fees = [float(f["fee"]) for f in mine if f.get("fee") is not None]
                        fields["fees_real"] = round(sum(fees), 6) if fees else None
                        fields["pnl_real_hl"] = round(sum(float(f.get("closedPnl", 0) or 0) for f in mine), 6)
                except Exception as e:
                    print(f"[FOLLOWUP] Remplissages {row['coin']} indisponibles : {e}")
            fields["followup_status"] = "ok" if candles else "bougies indisponibles"
            db.save_trade_followup(row["id"], fields)

        # v4.269 — SIMULATION "ET SI LE SL AVAIT ETE PLUS LARGE ?" pour les
        # trades sortis par stop loss : minute par minute apres la sortie,
        # le prix est-il revenu au niveau d ENTREE avant de toucher un SL a
        # 0,75 / 1 / 1,5 % ? (bougie ambigue -> compte comme SL, par prudence)
        try:
            to_sim = db.list_trades_needing_sim(limit=5)
        except Exception as e:
            print(f"[FOLLOWUP-SIM] Lecture base impossible : {e}")
            return
        for row in to_sim:
            try:
                closed = datetime.fromisoformat(row["closed_at"]).timestamp()
            except (TypeError, ValueError):
                db.save_trade_followup(row["id"], {"sim_status": "dates invalides"})
                continue
            entry = row.get("entry_price")
            if not entry:
                db.save_trade_followup(row["id"], {"sim_status": "prix d entree inconnu"})
                continue
            age_days = (time.time() - closed) / 86400
            interval, step_ms = ("1m", 60_000) if age_days < 3 else ("5m", 300_000)
            # Debut a la bougie SUIVANT la minute de sortie : la bougie de sortie
            # contient des prix anterieurs au SL (parfois meme l entree).
            # v4.290 — fenetre portee a 2 h (etude de la PATIENCE : combien de
            # temps et quel recul avant que le prix revienne a l entree).
            window_min = self.cfg.get("SL_FOLLOWUP_WINDOW_MIN", 120)
            start_ms = int(closed * 1000) // step_ms * step_ms + step_ms
            candles = self._candles(row["coin"], interval, start_ms, start_ms + window_min * 60_000)
            if candles is None:
                continue
            if not candles:
                db.save_trade_followup(row["id"], {"sim_status": "bougies indisponibles", "sim2_status": "bougies indisponibles"})
                continue
            candles = sorted(candles, key=lambda c: int(c["t"]))
            is_long = row.get("action") == "LONG"
            suffix = "" if interval == "1m" else " (5m)"
            fields = {"sim_status": "ok" + suffix, "sim2_status": "ok" + suffix}
            for col, sl_pct in (("sim_sl_075", 0.75), ("sim_sl_100", 1.0), ("sim_sl_150", 1.5), ("sim_sl_200", 2.0)):
                sl_px = entry * (1 - sl_pct / 100) if is_long else entry * (1 + sl_pct / 100)
                outcome = "aucun"
                for c in candles:
                    hi, lo = float(c["h"]), float(c["l"])
                    hit_sl = lo <= sl_px if is_long else hi >= sl_px
                    back = hi >= entry if is_long else lo <= entry
                    if hit_sl:
                        outcome = "sl"
                        break
                    if back:
                        outcome = "entree"
                        break
                fields[col] = outcome
            # Delai avant retour a l entree, et pire recul (depuis l ENTREE)
            # subi avant ce retour : largeur de SL qui aurait permis de tenir.
            worst = 0.0
            back_min = None
            for c in candles:
                hi, lo = float(c["h"]), float(c["l"])
                adverse = (entry - lo) / entry * 100 if is_long else (hi - entry) / entry * 100
                worst = max(worst, adverse)
                if (hi >= entry) if is_long else (lo <= entry):
                    back_min = round((int(c["t"]) - int(closed * 1000)) / 60_000, 1)
                    break
            fields["sl_back_min"] = back_min
            fields["sl_mae_pct"] = round(worst, 3)
            last_close = float(candles[-1]["c"])
            fields["sl_mark_120_pct"] = round(((last_close - entry) / entry * 100) * (1 if is_long else -1), 3)
            db.save_trade_followup(row["id"], fields)

    # ─────────────────────────────────────────────────────────────────────
    #  v4.276 — REGIME DE MARCHE ET PLAGES HORAIRES PAR MODE
    # ─────────────────────────────────────────────────────────────────────
    def market_regime(self):
        """Regime global : {"regime": "haussier"|"baissier"|"neutre"|"inconnu",
        "detail": texte, ...}. Recalcule au plus toutes les
        MARKET_REGIME_REFRESH_SEC secondes (une requete de bougies)."""
        cfg = self.cfg
        now = time.time()
        cache = getattr(self, "_regime_cache", None)
        if cache and now - cache["ts"] < cfg.get("MARKET_REGIME_REFRESH_SEC", 300):
            return cache
        ref = cfg.get("MARKET_REGIME_REF_TICKER", "BTC")
        tf = cfg.get("MARKET_REGIME_TIMEFRAME", "1h")
        tf_sec = {"15m": 900, "30m": 1800, "1h": 3600, "4h": 14400}.get(tf, 3600)
        result = {"ts": now, "regime": "inconnu", "detail": "donnees insuffisantes", "ref": ref, "timeframe": tf}
        candles = None
        if self.info is not None:
            end_ms = int(now * 1000)
            candles = self._candles(ref, tf, end_ms - 260 * tf_sec * 1000, end_ms)
        closes = [float(c["c"]) for c in sorted(candles or [], key=lambda c: int(c["t"]))]
        # Largeur de marche : part des actifs crypto suivis au-dessus de leur EMA200
        forex = set(cfg.get("FOREX_MODE_SYMBOLS", []))
        # v4.280 — FIX INCOHERENCE : la largeur de marche utilisait l EMA200
        # COURT TERME de chaque actif (200 points de 2 min ~ 6 h 40), alors
        # que BTC est juge sur l EMA200 1h (~8 jours) ; et elle n etait pas
        # disponible pendant ~1 h 40 apres un redemarrage — le regime etait
        # alors "confirme" sur BTC seul. Elle utilise desormais l EMA200 1h de
        # chaque actif (meme unite de temps que BTC).
        up = down = total = 0
        for slot, st in self.states.items():
            t = ticker_from_slot_key(slot)
            if t in forex or ":" in t:
                continue
            ema = getattr(st, "ema200_1h", None)
            try:  # v4.291 — prix FRAIS (flux temps reel) plutot que state.current_price
                px = float((self.all_mids or {}).get(t) or 0) or st.current_price or getattr(st, "last_close_1h", None)
            except (TypeError, ValueError):
                px = st.current_price or getattr(st, "last_close_1h", None)
            if ema and px:
                total += 1
                up += px > ema * 1.001     # marge de 0,1 % : un actif colle a son EMA ne compte ni pour l un ni pour l autre
                down += px < ema * 0.999
        breadth_up = (up / total * 100) if total else None
        breadth_down = (down / total * 100) if total else None
        result.update({"breadth_up_pct": round(breadth_up, 1) if breadth_up is not None else None,
                       "breadth_down_pct": round(breadth_down, 1) if breadth_down is not None else None, "breadth_count": total})
        if len(closes) >= 200:
            ema_fast, ema_slow, last = calc_ema(closes, 50), calc_ema(closes, 200), closes[-1]
            margin = cfg.get("MARKET_REGIME_MIN_DIST_PCT", 0.2) / 100  # ecart minimal a l EMA200 pour parler de tendance
            ref_bull = last > ema_slow * (1 + margin) and ema_fast > ema_slow
            ref_bear = last < ema_slow * (1 - margin) and ema_fast < ema_slow
            min_b = cfg.get("MARKET_REGIME_BREADTH_PCT", 60)
            result.update({"ref_price": last, "ref_ema50": ema_fast, "ref_ema200": ema_slow})
            breadth_txt = (f", actifs ({total}) : {breadth_up:.0f} % au-dessus / {breadth_down:.0f} % sous leur EMA200 1h"
                           if breadth_up is not None else ", largeur de marche pas encore mesurable")
            min_count = cfg.get("MARKET_REGIME_MIN_ASSETS", 8)
            breadth_ok = breadth_up is not None and total >= min_count
            # v4.280 — "confirme" exige VRAIMENT les deux conditions : sans
            # largeur de marche mesurable, le regime reste neutre.
            if ref_bull and breadth_ok and breadth_up >= min_b:
                result["regime"] = "haussier"
            elif ref_bear and breadth_ok and breadth_down >= min_b:
                result["regime"] = "baissier"
            else:
                result["regime"] = "neutre"
            pos_txt = "au-dessus de" if last > ema_slow else "sous"
            result["detail"] = f"{ref} {tf} {pos_txt} son EMA200 ({(last / ema_slow - 1) * 100:+.2f} %) (EMA50 {'>' if ema_fast > ema_slow else '<'} EMA200){breadth_txt}"
        prev = getattr(self, "_regime_cache", None)
        if prev and prev.get("regime") != result["regime"] and result["regime"] != "inconnu":
            self.emit("log", {"msg": f"🧭 Regime de marche : {prev.get('regime')} -> {result['regime'].upper()} ({result['detail']})", "level": "warn"})
        self._regime_cache = result
        return result

    # ─────────────────────────────────────────────────────────────────────
    #  v4.286 — SITUATION DE MARCHE (fond 1h x court terme 5 min)
    # ─────────────────────────────────────────────────────────────────────
    def situation(self, state, price=None):
        """Croise la tendance de FOND de l actif (prix / EMA200 1h, ~8 jours)
        et sa tendance COURT TERME (prix / EMA80 5 min, ~6 h 40) :
          hausse saine | repli dans une hausse | baisse saine |
          rebond dans une baisse | fond neutre.
        Repli sur le regime global si l EMA200 1h de l actif est inconnue."""
        price = price or state.current_price
        ema_s = self._trend_ema(state)
        court = None
        if price and ema_s:
            court = "haussier" if price > ema_s else "baissier"
        ema_l = getattr(state, "ema200_1h", None)
        m = self.cfg.get("MARKET_REGIME_MIN_DIST_PCT", 0.2) / 100
        if price and ema_l:
            fond = "haussier" if price > ema_l * (1 + m) else "baissier" if price < ema_l * (1 - m) else "neutre"
            src = "EMA200 1h de l actif"
        else:
            r = self.market_regime().get("regime")
            fond = r if r in ("haussier", "baissier") else "neutre"
            src = "regime global (EMA200 1h de l actif pas encore connue)"
        if fond == "neutre" or court is None:
            name = "fond neutre"
        elif fond == court:
            name = "hausse saine" if fond == "haussier" else "baisse saine"
        else:
            name = "repli dans une hausse" if fond == "haussier" else "rebond dans une baisse"
        return {"fond": fond, "court": court, "name": name, "fond_source": src,
                "ema_court": ema_s, "ema_fond": ema_l}

    def _continuation_check(self, ticker, state, price, direction):
        """v4.287 — VOIE "CONTINUATION" : entree dans une tendance saine SANS
        proximite d un niveau, si l ensemble des signaux la justifie.
        Retourne (True, detail) ou (False, raison du refus)."""
        cfg = self.cfg
        long_side = direction == "long"
        # 1) flux franc ET confirme (lecture courante + lecture precedente)
        need = cfg.get("CONTINUATION_MIN_FLOW", 0.3)
        fp = self._compute_trade_flow_pressure(ticker, price_now=price)
        hist = list(getattr(state, "trade_flow_history", None) or [])
        prev = hist[-1] if hist else None
        ok_flow = (fp is not None and prev is not None and
                   (fp >= need and prev >= need if long_side else fp <= -need and prev <= -need))
        if not ok_flow:
            return False, f"flux pas assez franc et confirme (actuel {fp if fp is None else round(fp, 2)}, precedent {prev if prev is None else round(prev, 2)}, requis {'+' if long_side else '-'}{need})"
        # 2) marche vivant
        q = self.market_quality(ticker, state)
        min_act = cfg.get("CONTINUATION_MIN_ACTIVITY", 1.0)
        if q.get("activity_ratio") is None or q["activity_ratio"] < min_act:
            return False, f"activite insuffisante ({q.get('activity_ratio')}x < {min_act}x)"
        # 3) pas d exces : ecart a l EMA de tendance <= N x ATR 5 min
        ema = self._trend_ema(state)
        c5 = list(getattr(state, "candle_history_5m", None) or [])[-14:]
        if not ema or len(c5) < 5:
            return False, "EMA / bougies 5 min indisponibles"
        atr = sum(c[0] - c[1] for c in c5) / len(c5)
        ext = (price - ema) / atr if long_side else (ema - price) / atr
        max_ext = cfg.get("CONTINUATION_MAX_EXTENSION_ATR", 1.5)
        if atr <= 0 or ext > max_ext:
            return False, f"prix trop eloigne de son EMA ({ext:.1f} ATR > {max_ext})"
        # 4) place pour gagner : prochain niveau oppose a >= N x le SL
        cap_key = "SPOT_ACCUM_SL_CAP_PCT" if long_side else "ACCUMULATION_SL_CAP_PCT"
        sl_pct = cfg.get(cap_key) or cfg.get("STRUCTURAL_SL_HARD_CAP_PCT", 0.5)
        if long_side:
            levels = [lv for lv in (self._short_level(state, "resistance"), getattr(state, "dynamic_trend_resistance", None)) if lv and lv > price]
            room = (min(levels) - price) / price * 100 if levels else None
        else:
            levels = [lv for lv in (self._short_level(state, "support"), getattr(state, "dynamic_trend_support", None)) if lv and lv < price]
            room = (price - max(levels)) / price * 100 if levels else None
        need_room = sl_pct * cfg.get("CONTINUATION_MIN_ROOM_X_SL", 2.0)
        if room is not None and room < need_room:
            return False, f"niveau oppose trop proche ({room:.2f}% < {need_room:.2f}%)"
        # 5) bougie dans le sens du trade
        if cfg.get("REQUIRE_ENTRY_CANDLE_COLOR", True):
            if not (self._is_candle_bullish_now(state) if long_side else self._is_candle_bearish_now(state)):
                return False, "bougie actuelle pas dans le sens du trade"
        room_txt = f"{room:.2f}%" if room is not None else "sans plafond proche"
        return True, f"flux {fp:+.2f}/{prev:+.2f}, activite {q['activity_ratio']}x, ecart EMA {ext:.1f} ATR, marge {room_txt}"

    def _structural_support(self, state):
        """Support de STRUCTURE (1h) : support ascendant s il existe, sinon
        plus bas depuis le debut de la tendance 1h."""
        return getattr(state, "rising_support", None) or getattr(state, "dynamic_trend_support", None)

    def _structural_resistance(self, state):
        return getattr(state, "falling_resistance", None) or getattr(state, "dynamic_trend_resistance", None)

    def _short_level(self, state, kind):
        return (getattr(state, "sr_display", None) or {}).get(kind)

    def _first_near_level(self, state, price, candidates, side):
        """Premier niveau "atteint" parmi les candidats (ordre de priorite) :
        proche au sens de l ATR et du bon cote du prix."""
        mult = self.cfg.get("ENTRY_ATR_PROXIMITY_MULTIPLIER", 1.0)
        for label, level in candidates:
            if not level:
                continue
            good_side = price > level * 0.995 if side == "support" else price < level * 1.005
            if good_side and self._is_near_level_atr(state, price, level, mult):
                return label, level
        return None, None

    def _regime_blocks(self, strategy):
        """Texte de blocage si le regime interdit ce mode, sinon None."""
        if not self.cfg.get("MARKET_REGIME_FILTER_ENABLED", 1):
            return None
        r = self.market_regime()
        if strategy == "spot_accumulation" and r["regime"] == "baissier":
            return f"marche baissier confirme ({r['detail']})"
        if strategy == "accumulation" and r["regime"] == "haussier":
            return f"marche haussier confirme ({r['detail']})"
        return None

    def _hours_block(self, strategy):
        """Texte de blocage si l heure UTC est hors de la plage du mode."""
        prefix = {"spot_accumulation": "SPOT_ACCUM", "accumulation": "ACCUMULATION", "funding_contrarian": "FUNDING"}.get(strategy)
        if not prefix:
            return None
        start = int(self.cfg.get(f"{prefix}_TRADE_HOUR_START_UTC", 0))
        end = int(self.cfg.get(f"{prefix}_TRADE_HOUR_END_UTC", 24))
        if (start, end) in ((0, 24), (0, 0)) or start == end:
            return None
        h = datetime.utcnow().hour
        inside = start <= h < end if start < end else (h >= start or h < end)
        return None if inside else f"hors plage horaire du mode ({start}h-{end}h UTC)"

    # ─────────────────────────────────────────────────────────────────────
    #  v4.292 — CONTROLE DE SYNCHRONISATION BOT <-> HYPERLIQUID (live)
    # ─────────────────────────────────────────────────────────────────────
    def _maybe_start_live_sync(self):
        if self.info is None or not self.cfg.get("WALLET_ADDRESS"):
            return
        if time.time() - getattr(self, "_last_live_sync", 0) < self.cfg.get("LIVE_SYNC_INTERVAL_SEC", 120):
            return
        th = getattr(self, "_live_sync_thread", None)
        if th is not None and th.is_alive():
            return
        self._last_live_sync = time.time()
        self._live_sync_thread = threading.Thread(target=self.run_live_sync, daemon=True)
        self._live_sync_thread.start()

    def _bot_live_positions(self):
        """Positions que le bot croit LIVE, par actif : [(libelle, state, pos, slot)]."""
        out = {}
        for pool_name, pool in (("", self.states), ("ACCUM__", self.accum_states)):
            for slot, st in pool.items():
                pos = st.position
                if pos and self._position_mode(pos) == "live":
                    out.setdefault(ticker_from_slot_key(slot), []).append(
                        {"mode": pos.get("strategy", "forex"), "state": st, "pos": pos, "slot": slot})
        manual = getattr(self, "manual", None)
        if manual is not None:
            for it in list(manual.items.values()):
                if it.get("status") == "open" and it.get("mode") == "live" and it.get("market") == "perp":
                    out.setdefault(it["ticker"], []).append({"mode": "manual", "state": None, "pos": None, "item": it})
        return out

    def run_live_sync(self):
        """Compare les positions LIVE du bot avec celles REELLEMENT tenues sur
        Hyperliquid (tous DEX), actif par actif, et corrige ce qui peut l etre
        sans risque :
          - position fantome (bot : ouverte / Hyperliquid : aucune) -> cloturee
            cote bot au prix reel de sortie (SL natif, liquidation...) ;
          - position orpheline (Hyperliquid : ouverte / bot : aucune) ->
            reprise par le bot (identification par cloid, SL de secours) ;
          - prix d entree / taille differents -> alignes sur Hyperliquid ;
          - SL natif absent -> repose.
        Une anomalie de position n est corrigee qu apres DEUX controles
        consecutifs (evite les faux ecarts pendant une ouverture ou une
        fermeture en cours) ; un sens oppose n est jamais corrige
        automatiquement (alerte seulement)."""
        cfg = self.cfg
        wallet = cfg.get("WALLET_ADDRESS")
        bot_pos = self._bot_live_positions()
        tickers = sorted({ticker_from_slot_key(s) for s in cfg["SYMBOLS"]} | set(bot_pos))
        exch = fetch_exchange_positions(self.info, wallet, tickers)
        report = {"ts": time.time(), "rows": [], "ok": exch is not None}
        if exch is None:
            report["error"] = "positions Hyperliquid illisibles pour le moment"
            self.live_sync_report = report
            return report
        auto = cfg.get("LIVE_SYNC_AUTO_FIX", 1)
        prev_flags = getattr(self, "_live_sync_flags", {})
        flags = {}
        now = time.time()
        for coin in sorted(set(bot_pos) | set(exch)):
            entries = bot_pos.get(coin, [])
            ep = exch.get(coin)
            row = {"coin": coin, "bot": [], "hl": None, "status": "ok", "action": None}
            bot_szi = 0.0
            for e in entries:
                if e["mode"] == "manual":
                    it = e["item"]
                    q = it["qty"] * (1 if it["direction"] == "long" else -1)
                    row["bot"].append({"mode": "manuel", "sens": it["direction"], "entree": it["entry_price"], "qte": round(abs(q), 8)})
                else:
                    pos = e["pos"]
                    q = pos["size"] * pos.get("leverage", 1) / pos["entry"] * (1 if pos["type"] == "long" else -1)
                    row["bot"].append({"mode": pos.get("strategy", "forex"), "sens": pos["type"], "entree": pos["entry"],
                                       "qte": round(abs(q), 8), "ouverte": pos.get("opened_at")})
                bot_szi += q
            # v4.293 — position unique du bot : on memorise la quantite REELLE
            # executee, le PnL latent Hyperliquid et on aligne silencieusement
            # les petits ecarts (arrondi de la quantite a l execution) pour que
            # le PnL affiche par le bot parte de la meme base que Hyperliquid.
            if ep and len(entries) == 1 and entries[0]["mode"] != "manual" and (bot_szi > 0) == (ep["szi"] > 0):
                pos1 = entries[0]["pos"]
                pos1["hl_qty"] = abs(ep["szi"])
                pos1["hl_unrealized_pnl"] = ep.get("unrealized_pnl")
                pos1["hl_sync_ts"] = now
                if abs(abs(ep["szi"]) - abs(bot_szi)) / abs(ep["szi"]) <= cfg.get("LIVE_SYNC_SIZE_TOLERANCE", 0.05):
                    lev1 = pos1.get("leverage", 1) or 1
                    pos1["entry"] = ep["entry"]
                    pos1["size"] = abs(ep["szi"]) * ep["entry"] / lev1
            if ep:
                row["hl"] = {"sens": "long" if ep["szi"] > 0 else "short", "qte": abs(ep["szi"]), "entree": ep["entry"],
                             "levier": ep.get("leverage"), "liquidation": ep.get("liquidation_px"),
                             "pnl_latent": ep.get("unrealized_pnl"), "valeur": ep.get("position_value")}
            recent = any(e.get("pos") and (now - self._pos_opened_ts(e["pos"])) < 90 for e in entries)
            if entries and not ep:
                row["status"] = "fantome"
            elif ep and not entries:
                row["status"] = "orpheline"
            elif entries and ep:
                if (bot_szi > 0) != (ep["szi"] > 0):
                    row["status"] = "sens oppose"
                elif abs(abs(ep["szi"]) - abs(bot_szi)) / abs(ep["szi"]) > cfg.get("LIVE_SYNC_SIZE_TOLERANCE", 0.05):
                    row["status"] = "taille differente"
                elif len(entries) == 1 and entries[0]["mode"] != "manual" \
                        and abs(ep["entry"] - entries[0]["pos"]["entry"]) / ep["entry"] > 0.005:
                    row["status"] = "prix d entree different"  # v4.293 : ecarts < 0,5 % alignes silencieusement
            if row["status"] != "ok":
                flags[coin] = row["status"]
            confirmed = prev_flags.get(coin) == row["status"] and not recent
            try:
                if row["status"] == "fantome" and auto and confirmed:
                    for e in entries:
                        if e["mode"] == "manual":
                            self.manual.reconcile(exch)  # cloture les positions manuelles absentes d Hyperliquid
                        else:
                            self._close_offline_position(e["state"], e["slot"], coin, dict(e["pos"]))
                    row["action"] = "cloturee cote bot (plus ouverte sur Hyperliquid)"
                elif row["status"] == "orpheline" and auto and confirmed and coin in {ticker_from_slot_key(s) for s in cfg["SYMBOLS"]}:
                    if self._adopt_exchange_position(coin, ep, "long" if ep["szi"] > 0 else "short"):
                        row["action"] = "reprise par le bot"
                elif row["status"] in ("taille differente", "prix d entree different") and auto and confirmed and len(entries) == 1 and entries[0]["mode"] != "manual":
                    pos = entries[0]["pos"]
                    lev = pos.get("leverage", 1) or 1
                    pos["entry"] = ep["entry"]
                    pos["size"] = abs(ep["szi"]) * ep["entry"] / lev
                    row["action"] = "alignee sur Hyperliquid (entree et taille)"
                    self._save_open_positions()
                elif row["status"] == "sens oppose":
                    row["action"] = "ALERTE : verification manuelle requise (non corrige automatiquement)"
                elif row["status"] != "ok" and not confirmed:
                    row["action"] = "a confirmer au prochain controle"
            except Exception as e_fix:
                row["action"] = f"correction impossible : {e_fix}"
            # SL natif present ?
            if ep and entries and self.exchange is not None and cfg.get("LIVE_SYNC_CHECK_NATIVE_SL", 1):
                try:
                    sl_oids, _tp = _get_open_orders_by_type(self.info, wallet, coin)
                    row["sl_natif"] = bool(sl_oids)
                    if not sl_oids and auto and len(entries) == 1 and entries[0]["mode"] != "manual":
                        ensure_sl_on_hyperliquid(self.exchange, self.info, wallet, coin, entries[0]["pos"], cfg)
                        row["action"] = (row["action"] + " ; " if row["action"] else "") + "SL natif repose"
                except Exception as e_sl:
                    row["sl_natif"] = None
                    print(f"[LIVE-SYNC] Controle SL natif {coin} impossible : {e_sl}")
            if row["status"] != "ok" or row.get("action"):
                self.emit("log", {"msg": f"🔄 Synchro Hyperliquid [{coin}] : {row['status']}" + (f" — {row['action']}" if row.get("action") else ""),
                                  "level": "error" if row["status"] == "sens oppose" else "warn"})
            report["rows"].append(row)
        self._live_sync_flags = flags
        try:
            acct = sync_capital_from_hyperliquid(self.info, wallet)
            report["compte"] = {"valeur_compte": acct}
            if acct:
                self.live_equity_real, self.live_equity_ts = acct, time.time()  # v4.297 — dimensionnement live
        except Exception:
            pass
        report["anomalies"] = sum(1 for r in report["rows"] if r["status"] != "ok")
        self.live_sync_report = report
        return report

    def _pos_opened_ts(self, pos):
        try:
            return datetime.strptime(pos.get("opened_at", ""), "%d/%m/%Y %H:%M:%S").timestamp()
        except (TypeError, ValueError):
            return 0

    def _publish_opportunities(self, strategy, candidates):
        """v4.273 — transmet les candidats d entree du cycle au trading manuel."""
        manual = getattr(self, "manual", None)
        if manual is not None:
            try:
                manual.record_candidates(strategy, candidates)
            except Exception as e:
                print(f"[MANUEL] Publication des opportunites impossible : {e}")

    def _manual_tick(self):
        manual = getattr(self, "manual", None)
        if manual is not None:
            manual.on_tick()

    def _discard_pending_trade(self, trade_uid, ticker):
        """v4.264 — retire la trace prealable d un trade dont l ordre n a
        finalement pas abouti."""
        try:
            db.discard_pending_trade(trade_uid)
        except Exception as e:
            print(f"[TRADE-INDEX] Suppression trace {trade_uid} ({ticker}) impossible : {e}")

    def _position_mode(self, pos):
        """v4.264 — mode REEL (paper/live) d une position : celui memorise a
        son ouverture. Repli sur le reglage courant de sa strategie pour les
        positions anciennes sans cette information."""
        if (pos or {}).get("strategy") in ("forex_scalp", "forex_swing", "swing_support", "trend_follow"):      # v4.353 / v4.361 / v4.362 / v4.365 : PAPER par construction
            return "paper"
        recorded = (pos or {}).get("effective_mode")
        if recorded in ("paper", "live"):
            return recorded
        strategy = (pos or {}).get("strategy", "forex")
        if strategy == "funding_contrarian" and not self.cfg.get("FUNDING_MODE_LIVE_ALLOWED", False):
            return "paper"
        return self._effective_mode(strategy)

    def _effective_mode(self, strategy):
        """v4.87 — SUR DEMANDE EXPLICITE : chaque mode (normal, accumulation,
        funding_contrarian, spot_accumulation) peut desormais basculer
        INDEPENDAMMENT entre paper et live, via STRATEGY_MODE_OVERRIDE.
        None (par defaut pour normal/accumulation/spot_accumulation) = suit
        le mode global du bot — aucun changement de comportement tant que
        rien n est personnalise. Le garde-fou Funding existant
        (FUNDING_MODE_LIVE_ALLOWED) s applique TOUJOURS en plus, meme si
        cette fonction renvoie "live" pour funding_contrarian — double
        protection, pas de retrait de securite existante."""
        if strategy in ("forex_scalp", "forex_swing", "swing_support", "trend_follow"):         # v4.353 / v4.361 / v4.362 / v4.365 : PAPER par construction
            return "paper"
        override = self.cfg.get("STRATEGY_MODE_OVERRIDE", {}).get(strategy)
        if override in ("paper", "live"):
            return override
        return self.cfg["MODE"]

    def _compute_spot_accum_dynamic_leverage(self, ticker):
        """v4.178 — SUR DEMANDE EXPLICITE : levier dynamique 2-5x pour
        Spot-Accum, uniquement quand l entree qualifie via le flirt S/R +
        couleur de bougie (pas via une cassure fraiche). Reutilise le
        suivi de confiance par actif DEJA existant (confidence_thresholds)
        comme proxy de performance historique — seuil bas (proche de
        CONFIDENCE_MIN_PCT) = actif performant -> levier haut (5x). Seuil
        haut (proche de CONFIDENCE_MAX_PCT, releve apres des pertes) =
        actif penalise -> levier bas (2x)."""
        cfg = self.cfg
        base = cfg.get("CONFIDENCE_MIN_PCT", 65.0)
        ceiling = cfg.get("CONFIDENCE_MAX_PCT", 87.0)
        threshold = self.confidence_thresholds.get(ticker, base)
        if ceiling <= base:
            return 2
        ratio = (ceiling - threshold) / (ceiling - base)
        ratio = max(0.0, min(1.0, ratio))
        leverage = 2 + ratio * (cfg.get("SPOT_ACCUM_MAX_DYNAMIC_LEVERAGE", 5) - 2)
        return round(leverage)

    def _compute_accumulation_dynamic_leverage(self, ticker):
        """v4.191 — SUR DEMANDE EXPLICITE : miroir exact de
        _compute_spot_accum_dynamic_leverage, pour Accumulation (desormais
        l oppose de Spot-Accum — short uniquement)."""
        cfg = self.cfg
        base = cfg.get("CONFIDENCE_MIN_PCT", 65.0)
        ceiling = cfg.get("CONFIDENCE_MAX_PCT", 87.0)
        threshold = self.confidence_thresholds.get(ticker, base)
        if ceiling <= base:
            return 2
        ratio = (ceiling - threshold) / (ceiling - base)
        ratio = max(0.0, min(1.0, ratio))
        leverage = 2 + ratio * (cfg.get("ACCUMULATION_MAX_DYNAMIC_LEVERAGE", 5) - 2)
        return round(leverage)

    def _compute_prudent_leverage(self, ticker, confidence, rsi_mode):
        """v3.2 — Levier prudent, calcule INDIVIDUELLEMENT pour chaque trade
        (plus une valeur fixe globale).
        v4.194 — SUR DEMANDE EXPLICITE : remplace l ancien palier fixe
        (x1/x2/x3 selon la confiance) par le MEME degrade progressif que
        Accumulation/Spot-Accum (2-5x, base sur la performance historique
        REELLE de l actif via confidence_thresholds) — coherence totale
        entre les 4 modes : penalise les mauvais actifs (seuil releve ->
        levier bas), favorise les bons (seuil bas -> levier haut). JAMAIS
        de levier en mode "reversal" (deja plus risque par nature).
        """
        if rsi_mode not in ("trend", "accumulation"):
            return 1
        return self._compute_performance_leverage(ticker)

    def _compute_performance_leverage(self, ticker):
        """v4.194 — SUR DEMANDE EXPLICITE : fonction UNIQUE de levier
        base-performance, partagee par les 4 modes (remplace les 3
        fonctions quasi-identiques _compute_prudent_leverage/
        _compute_accumulation_dynamic_leverage/
        _compute_spot_accum_dynamic_leverage, qui restent pour
        compatibilite mais delegue desormais ici)."""
        cfg = self.cfg
        base = cfg.get("CONFIDENCE_MIN_PCT", 65.0)
        ceiling = cfg.get("CONFIDENCE_MAX_PCT", 87.0)
        threshold = self.confidence_thresholds.get(ticker, base)
        if ceiling <= base:
            return 2
        ratio = (ceiling - threshold) / (ceiling - base)
        ratio = max(0.0, min(1.0, ratio))
        leverage = 2 + ratio * (cfg.get("MAX_DYNAMIC_LEVERAGE", 5) - 2)
        return round(leverage)

    def _compute_performance_size_multiplier(self, ticker):
        """v4.194 — SUR DEMANDE EXPLICITE : multiplicateur de TAILLE de
        position (independant du levier) — 0.5x pour un actif au plafond
        de penalite (CONFIDENCE_MAX_PCT), 1.5x pour un actif au sommet de
        sa performance (CONFIDENCE_MIN_PCT, jamais penalise). Applique a
        la taille de base (deja calculee par equity/max_trades) sur les 4
        modes — favorise les gagnants, penalise les mauvais actifs sur les
        DEUX dimensions (levier ET taille), pas seulement le levier."""
        cfg = self.cfg
        base = cfg.get("CONFIDENCE_MIN_PCT", 65.0)
        ceiling = cfg.get("CONFIDENCE_MAX_PCT", 87.0)
        threshold = self.confidence_thresholds.get(ticker, base)
        if ceiling <= base:
            return 1.0
        ratio = (ceiling - threshold) / (ceiling - base)
        ratio = max(0.0, min(1.0, ratio))
        min_mult = cfg.get("PERFORMANCE_SIZE_MULT_MIN", 0.5)
        max_mult = cfg.get("PERFORMANCE_SIZE_MULT_MAX", 1.5)
        return round(min_mult + ratio * (max_mult - min_mult), 3)

    def _volume_confirms_accumulation(self, state, recent_candles=6, baseline_candles=48, min_ratio=1.15):
        """v4.218 — SUR DEMANDE EXPLICITE : confirme un vrai etat
        d accumulation via le VRAI volume (pas la volatilite du proxy
        "vol_history") — une accumulation genuine s accompagne souvent
        d un volume anormalement ELEVE, malgre un prix qui bouge peu.
        Compare le volume MOYEN des dernieres heures (recent_candles) a une
        base plus longue (baseline_candles) — retourne True si le volume
        recent depasse ce ratio minimal, None si l historique 1h est
        insuffisant (ne bloque jamais sur donnees manquantes, c est a l
        appelant de decider quoi faire de None)."""
        candles = list(state.candle_history_1h)
        if len(candles) < baseline_candles:
            return None
        recent_vols = [c[3] for c in candles[-recent_candles:]]
        baseline_vols = [c[3] for c in candles[-baseline_candles:]]
        if not baseline_vols or sum(baseline_vols) <= 0:
            return None
        recent_avg = sum(recent_vols) / len(recent_vols)
        baseline_avg = sum(baseline_vols) / len(baseline_vols)
        if baseline_avg <= 0:
            return None
        return (recent_avg / baseline_avg) >= min_ratio

    def _compute_macd_1h(self, state):
        """v4.203 — SUR DEMANDE EXPLICITE : MACD calcule sur les VRAIES
        bougies 1h Hyperliquid (closes de state.candle_history_1h), pas sur
        l echantillonnage ~2min habituel. Retourne (macd_line, signal_line)
        ou (None, None) si pas assez d historique (35 bougies 1h minimum,
        ~1.5 jours)."""
        closes = [c[2] for c in state.candle_history_1h]
        return calc_macd(closes)

    def _fetch_recent_trades(self, ticker, count=100):
        """v4.240 — SUR DEMANDE EXPLICITE : recupere le VRAI flux de
        transactions recentes d Hyperliquid (endpoint recentTrades) — pour
        chaque transaction executee : prix, taille, sens (B=achat agressif,
        A=vente agressive), horodatage. Source totalement independante des
        bougies/volume par bougie deja utilisees — permet de mesurer la
        PRESSION DIRECTIONNELLE reelle (qui frappe le marche, pas
        seulement combien). Retourne une liste de dicts bruts, ou [] en
        cas d echec."""
        try:
            raw = self.info.post("/info", {"type": "recentTrades", "coin": ticker})
            if not raw or not isinstance(raw, list):
                return []
            return [{"sz": r.get("sz"), "side": r.get("side"), "t": r.get("time")} for r in raw[:count]]
        except Exception as e:
            # v4.262 — SUR DEMANDE EXPLICITE : rendu VISIBLE dans l
            # interface (pas seulement les logs bruts Railway, que l
            # utilisateur ne consulte pas systematiquement) — throttle a
            # une alerte toutes les 10 min par ticker pour eviter le bruit
            # si l echec est repete/persistant.
            now_ts = time.time()
            last_warned = self._trade_flow_fetch_warn_ts.get(ticker, 0) if hasattr(self, "_trade_flow_fetch_warn_ts") else 0
            if now_ts - last_warned > 600:
                if not hasattr(self, "_trade_flow_fetch_warn_ts"):
                    self._trade_flow_fetch_warn_ts = {}
                self._trade_flow_fetch_warn_ts[ticker] = now_ts
                self.emit("log", {"msg": f"[{ticker}] ⚠️ Echec recuperation flux de transactions : {e}", "level": "warn"})
            print(f"[TRADES-FLOW] Echec recuperation flux transactions pour {ticker} : {e}")
            return []

    def _compute_trade_flow_pressure(self, ticker, price_now=None):
        """v4.240 — SUR DEMANDE EXPLICITE : calcule la PRESSION
        DIRECTIONNELLE reelle sur les dernieres transactions — ratio du
        volume achete de facon agressive (side='B') contre vendu de facon
        agressive (side='A'). Retourne un float entre -1.0 (100% vente
        agressive) et +1.0 (100% achat agressif), ou None si pas assez de
        donnees. Comble un trou identifie : une tendance DEJA engagee,
        loin de tout support/resistance, n etait auparavant jamais
        capturee par aucun mecanisme d entree (tous bases sur des niveaux
        de prix ou des bougies, pas sur la pression reelle du marche).
        v4.242 — SUR DEMANDE EXPLICITE : garde-fou VOLUME MINIMAL — une
        poignee de petites transactions sur un marche creux pouvait
        auparavant generer un ratio extreme sans representer un vrai
        mouvement de marche. Exige desormais un volume NOTIONNEL total
        minimal (en $) sur l echantillon, sinon retourne None (donnee
        jugee non fiable) plutot qu un signal trompeur.
        v4.263 — SUR DEMANDE EXPLICITE : lit desormais depuis le buffer
        WebSocket temps reel (_ws_trades_buffer, alimente en continu par
        _on_ws_trades) au lieu du sondage REST periodique — plus de
        latence, plus de risque d echec de requete repete. Repli sur le
        sondage REST UNIQUEMENT si le buffer WebSocket n a jamais recu de
        donnees pour ce ticker (abonnement pas encore etabli, ou echoue) —
        garantit une continuite de fonctionnement, pas un point de
        defaillance unique."""
        trades, _newest = self._trade_flow_window(ticker)
        if len(trades) < 20:
            return None
        buy_volume = 0.0
        sell_volume = 0.0
        for t in trades:
            try:
                sz = float(t.get("sz", 0))
                side = t.get("side", "")
                if side == "B":
                    buy_volume += sz
                elif side == "A":
                    sell_volume += sz
            except (TypeError, ValueError):
                continue
        total = buy_volume + sell_volume
        if total <= 0:
            return None
        # Garde-fou volume minimal — approxime le notionnel avec le prix
        # actuel (les prix individuels des transactions sont proches sur
        # une fenetre aussi courte).
        if price_now and price_now > 0:
            notional_total = total * price_now
            min_notional = self.cfg.get("TRADE_FLOW_MIN_NOTIONAL_USD", 5000.0)
            if notional_total < min_notional:
                return None
        return (buy_volume - sell_volume) / total

    def _trade_flow_ws_alive(self):
        """Le flux WebSocket 'trades' recoit-il encore des donnees ?"""
        subscribed_at = getattr(self, "_ws_trades_subscribed_at", None)
        if subscribed_at is None:
            return False
        dead_after = self.cfg.get("TRADE_FLOW_WS_DEAD_SEC", 120)
        last_any = getattr(self, "_ws_trades_last_any", None)
        if last_any is None:
            # abonnement recent : on laisse le temps aux premieres donnees
            return time.time() - subscribed_at < dead_after
        return time.time() - last_any < dead_after

    def _trade_flow_window(self, ticker):
        """v4.266 — transactions des TRADE_FLOW_WINDOW_SEC dernieres secondes
        (WebSocket si vivant, sinon REST avec un petit cache de 20s), et
        horodatage de la plus recente. Une fenetre vide signifie "pas d
        activite recente" : aucune pression n est alors calculee."""
        window_ms = self.cfg.get("TRADE_FLOW_WINDOW_SEC", 180) * 1000
        now_ms = int(time.time() * 1000)
        if self._trade_flow_ws_alive():
            source = list(getattr(self, "_ws_trades_buffer", {}).get(ticker) or [])
        else:
            cache = getattr(self, "_rest_trades_cache", None)
            if cache is None:
                cache = self._rest_trades_cache = {}
            hit = cache.get(ticker)
            if hit and time.time() - hit[0] < 20:
                source = hit[1]
            else:
                source = self._fetch_recent_trades(ticker, count=2000)
                cache[ticker] = (time.time(), source)
        recent = []
        for t in source:
            try:
                t_ms = int(t.get("t") or 0)
            except (TypeError, ValueError):
                continue
            if t_ms >= now_ms - window_ms:
                recent.append(t)
        newest = max((int(t["t"]) for t in recent), default=None)
        return recent, newest

    def _maybe_refresh_trade_flow(self, ticker, state):
        """v4.240 — SUR DEMANDE EXPLICITE : rafraichit la pression
        directionnelle au maximum une fois toutes les 60s par actif, et
        maintient un historique COURT pour exiger une pression SOUTENUE
        dans le temps (pas un simple pic ponctuel) avant de la considerer
        comme un signal fiable.
        v4.242 — SUR DEMANDE EXPLICITE : maintient aussi un historique du
        PRIX a chaque rafraichissement, pour permettre la verification de
        corr elation prix/pression (voir _trend_persistence_confirmed)."""
        now = time.time()
        last_refresh = getattr(state, "trade_flow_last_refresh", 0)
        if now - last_refresh < 60:
            return
        state.trade_flow_last_refresh = now
        # v4.266 — une lecture n est enregistree que si la fenetre contient
        # des transactions NOUVELLES depuis la lecture precedente : avant,
        # sur un actif peu actif, les memes transactions pouvaient etre
        # recomptees a chaque lecture et simuler une "pression soutenue".
        _, newest = self._trade_flow_window(ticker)
        if newest is None or newest <= getattr(state, "trade_flow_last_newest_ms", 0):
            return
        pressure = self._compute_trade_flow_pressure(ticker, price_now=state.current_price)
        if pressure is None or state.current_price is None:
            return
        state.trade_flow_last_newest_ms = newest
        for attr in ("trade_flow_history", "trade_flow_price_history", "trade_flow_ts_history"):
            if getattr(state, attr, None) is None:
                setattr(state, attr, deque(maxlen=10))
        state.trade_flow_history.append(pressure)
        state.trade_flow_price_history.append(state.current_price)
        state.trade_flow_ts_history.append(now)

    def _trend_persistence_confirmed(self, state, direction):
        """v4.240 — SUR DEMANDE EXPLICITE : confirme une PRESSION
        DIRECTIONNELLE soutenue (pas ponctuelle) — exige que les
        dernieres lectures de pression (~10 x 60s = ~10 min d historique)
        soient TOUTES au-dela d un seuil minimal, dans le sens demande.
        direction="long" : pression acheteuse soutenue. direction="short" :
        pression vendeuse soutenue. Retourne False si historique
        insuffisant (jamais bloquant — c est un BYPASS supplementaire).
        v4.242 — SUR DEMANDE EXPLICITE : garde-fou CORRELATION PRIX —
        exige desormais que le prix ait REELLEMENT bouge dans le meme
        sens que la pression rapportee, sur la meme fenetre. Sans ce
        garde-fou, une "pression acheteuse" pouvait exister sans que le
        prix ne reagisse (absorbee par des vendeurs, mur de liquidite) —
        un signal trompeur, non confirme par le marche lui-meme."""
        history = getattr(state, "trade_flow_history", None)
        min_samples = self.cfg.get("TREND_PERSISTENCE_MIN_SAMPLES", 6)
        if not history or len(history) < min_samples:
            return False
        # v4.266 — les lectures doivent etre CONSECUTIVES et recentes : une
        # absence d activite (aucune lecture pendant plusieurs minutes)
        # interrompt la "pression soutenue".
        ts_hist = getattr(state, "trade_flow_ts_history", None)
        if not ts_hist or len(ts_hist) < min_samples:
            return False
        recent_ts = list(ts_hist)[-min_samples:]
        if time.time() - recent_ts[-1] > 120 or recent_ts[-1] - recent_ts[0] > (min_samples - 1) * 60 * 1.6:
            return False
        threshold = self.cfg.get("TREND_PERSISTENCE_PRESSURE_THRESHOLD", 0.15)
        recent = list(history)[-min_samples:]
        if direction == "long":
            pressure_ok = all(p >= threshold for p in recent)
        else:
            pressure_ok = all(p <= -threshold for p in recent)
        if not pressure_ok:
            return False

        price_history = getattr(state, "trade_flow_price_history", None)
        if not price_history or len(price_history) < min_samples:
            return False
        price_recent = list(price_history)[-min_samples:]
        price_move_pct = (price_recent[-1] - price_recent[0]) / price_recent[0] * 100 if price_recent[0] else 0
        min_price_move = self.cfg.get("TREND_PERSISTENCE_MIN_PRICE_MOVE_PCT", 0.1)
        if direction == "long":
            return price_move_pct >= min_price_move
        else:
            return price_move_pct <= -min_price_move

    def _fetch_candles(self, ticker, interval="1h", count=60):
        """v4.203/236 — SUR DEMANDE EXPLICITE : recupere les VRAIES bougies
        d Hyperliquid (alignees sur l horloge, via l endpoint candleSnapshot
        officiel), pour l intervalle demande — remplace l agregation
        synthetique de bougies ~2min du bot (mtf_prices), qui n est PAS
        alignee sur les vraies bougies Hyperliquid (confirme par un cas
        reel : un desaccord entre le range detecte par le bot et le
        mouvement reel observe sur le graphique Hyperliquid, pour la meme
        periode approximative). Retourne une liste de (high, low, close,
        volume), la plus ancienne en premier, ou [] en cas d echec.
        v4.204 — exclut la bougie EN COURS de formation (valeur instable
        tant que non cloturee).
        v4.218 — inclut le VRAI volume de transactions (champ "v")."""
        try:
            interval_sec = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}.get(interval, 3600)
            end_ms = int(time.time() * 1000)
            start_ms = end_ms - count * interval_sec * 1000
            req = {"coin": ticker, "interval": interval, "startTime": start_ms, "endTime": end_ms}
            raw = self.info.post("/info", {"type": "candleSnapshot", "req": req})
            if not raw or not isinstance(raw, list):
                return []
            now_ms = int(time.time() * 1000)
            closed_only = [c for c in raw if c.get("T", 0) <= now_ms]
            return [(float(c["h"]), float(c["l"]), float(c["c"]), float(c.get("v", 0))) for c in closed_only]
        except Exception as e:
            print(f"[CANDLES] Echec recuperation bougies {interval} pour {ticker} : {e}")
            return []

    def _fetch_1h_candles(self, ticker, count=60):
        """Repli de compatibilite — voir _fetch_candles (intervalle generalise)."""
        return self._fetch_candles(ticker, "1h", count)

    def _update_dynamic_trend(self, state):
        """v4.203 — SUR DEMANDE EXPLICITE : tendance dynamique — suit une
        direction depuis son POINT DE DEPART (S/R qui s etend aux nouveaux
        extremes a chaque bougie 1h) jusqu a un retournement CONFIRME sur 3
        bougies 1h CONSECUTIVES en sens oppose. Retraite l INTEGRALITE de
        state.candle_history_1h depuis le debut a chaque appel — plus
        simple et plus sur qu un suivi incremental, puisque cet historique
        est desormais rafraichi PERIODIQUEMENT (voir
        _maybe_refresh_dynamic_trend) via les VRAIES bougies 1h Hyperliquid,
        pas ajoute bougie par bougie. Utilisee par Accumulation et
        Spot-Accum uniquement."""
        candles_1h = list(state.candle_history_1h)
        if len(candles_1h) < 2:
            return
        direction = None
        support = None
        resistance = None
        reversal_streak = 0
        for i in range(1, len(candles_1h)):
            high_now, low_now, close_now = candles_1h[i][:3]
            close_prev = candles_1h[i - 1][2]
            candle_up = close_now >= close_prev

            if direction is None:
                direction = "up" if candle_up else "down"
                support = low_now
                resistance = high_now
                reversal_streak = 0
                continue

            trend_matches = (candle_up and direction == "up") or (not candle_up and direction == "down")
            if trend_matches:
                reversal_streak = 0
                if low_now < support:
                    support = low_now
                if high_now > resistance:
                    resistance = high_now
            else:
                reversal_streak += 1
                if reversal_streak >= 3:
                    last_3 = candles_1h[i-2:i+1]
                    direction = "down" if direction == "up" else "up"
                    support = min(c[1] for c in last_3)
                    resistance = max(c[0] for c in last_3)
                    reversal_streak = 0

        state.dynamic_trend_direction = direction
        state.dynamic_trend_support = support
        state.dynamic_trend_resistance = resistance
        state.dynamic_trend_reversal_streak = reversal_streak
        state.rising_support = self._compute_rising_support(candles_1h) if direction == "up" else None  # v4.278
        state.falling_resistance = self._compute_falling_resistance(candles_1h) if direction == "down" else None  # v4.286

    def _compute_falling_resistance(self, candles_1h):
        """v4.286 — RESISTANCE DESCENDANTE (miroir du support ascendant) :
        dernier sommet de rebond "plus haut plus bas" d une tendance
        baissiere en 1h, non casse depuis (aucune cloture au-dessus)."""
        k = int(self.cfg.get("SPOT_ACCUM_PIVOT_CANDLES", 2))
        window = candles_1h[-int(self.cfg.get("SPOT_ACCUM_PIVOT_LOOKBACK_CANDLES", 72)):]
        if len(window) < 2 * k + 3:
            return None
        pivots = []
        for i in range(k, len(window) - k):
            high_i = window[i][0]
            if all(high_i > window[j][0] for j in range(i - k, i + k + 1) if j != i):
                pivots.append((i, high_i))
        if len(pivots) < 2:
            return None
        (_, high_prev), (i_last, high_last) = pivots[-2], pivots[-1]
        if high_last >= high_prev:
            return None
        if any(c[2] > high_last for c in window[i_last + 1:]):
            return None
        return high_last

    def _compute_rising_support(self, candles_1h):
        """v4.278 — SUPPORT ASCENDANT : dernier creux de repli ("plus bas
        plus haut") d une tendance haussiere, sur les bougies 1h. Un creux
        est une bougie dont le plus bas est inferieur a celui des K bougies
        voisines de chaque cote (K = SPOT_ACCUM_PIVOT_CANDLES). Le support
        n est retenu que si ce creux est PLUS HAUT que le precedent (structure
        haussiere intacte) et n a pas ete casse depuis (cloture en dessous).
        Contrairement au support dynamique (plus bas depuis le DEBUT de la
        tendance, fige pendant toute une hausse reguliere), il remonte avec
        la tendance : Spot-Accum peut ainsi acheter les REPLIS."""
        k = int(self.cfg.get("SPOT_ACCUM_PIVOT_CANDLES", 2))
        window = candles_1h[-int(self.cfg.get("SPOT_ACCUM_PIVOT_LOOKBACK_CANDLES", 72)):]
        if len(window) < 2 * k + 3:
            return None
        pivots = []
        for i in range(k, len(window) - k):
            low_i = window[i][1]
            if all(low_i < window[j][1] for j in range(i - k, i + k + 1) if j != i):
                pivots.append((i, low_i))
        if len(pivots) < 2:
            return None
        (_, low_prev), (i_last, low_last) = pivots[-2], pivots[-1]
        if low_last <= low_prev:
            return None  # plus de "plus bas plus haut" : structure haussiere rompue
        if any(c[2] < low_last for c in window[i_last + 1:]):
            return None  # creux casse depuis (cloture en dessous)
        return low_last

    # ─────────────────────────────────────────────────────────────────────
    #  v4.330 — ENTONNOIR D ENTREE (instrumentation en LECTURE SEULE)
    #  Sur demande : "voir le nombre de fois dans une semaine que toutes les
    #  conditions s alignent". Chaque compte = UN actif sur UNE fenetre de 15
    #  min (bucket M15) ou l etape a ete atteinte au moins une fois. Ne change
    #  AUCUNE decision de trading ; toute erreur ici est ignoree.
    # ─────────────────────────────────────────────────────────────────────
    def _funnel_note(self, mode, ticker, stage, shadow=False):
        try:
            if not self.cfg.get("ENTRY_FUNNEL_TRACKING", 1):
                return
            cur = getattr(self, "_funnel_cur", None)
            if cur is None:
                cur = self._funnel_cur = {}
            bucket = int(time.time() // 900)
            key = (mode, ticker)
            prev = cur.get(key)
            if prev and prev[0] == bucket:
                prev[1].add(stage)
                return
            if prev:
                self._funnel_flush(mode, ticker, prev[0], prev[1])
            cur[key] = (bucket, {stage})
        except Exception:
            pass

    def _funnel_flush(self, mode, ticker, bucket, stages):
        try:
            day = datetime.utcfromtimestamp(bucket * 900).strftime("%Y-%m-%d")
            counts = getattr(self, "_funnel_counts", None)
            if counts is None:
                counts = self._funnel_counts = {}
            ck = (day, mode)
            if ck not in counts:
                try:
                    counts[ck] = json.loads(db.get_meta(f"funnel:{day}:{mode}") or "{}")
                except Exception:
                    counts[ck] = {}
            for st in stages:
                counts[ck][st] = counts[ck].get(st, 0) + 1
            db.set_meta(f"funnel:{day}:{mode}", json.dumps(counts[ck]))
            # v4.332 — QUELS actifs (pas seulement combien de fenetres) : un meme
            # actif peut compter dans plusieurs fenetres de 15 min consecutives.
            keep = {"signal_flux", "h1", "objectif", "candidat", "nt_signal_flux"} & set(stages)
            if keep:
                assets = getattr(self, "_funnel_assets", None)
                if assets is None:
                    assets = self._funnel_assets = {}
                if ck not in assets:
                    try:
                        assets[ck] = json.loads(db.get_meta(f"funnel_assets:{day}:{mode}") or "{}")
                    except Exception:
                        assets[ck] = {}
                changed = False
                for st in keep:
                    lst = assets[ck].setdefault(st, [])
                    if ticker not in lst:
                        lst.append(ticker)
                        changed = True
                if changed:
                    db.set_meta(f"funnel_assets:{day}:{mode}", json.dumps(assets[ck]))
        except Exception:
            pass

    def _funnel_rr_note(self, mode, ticker, kind, plan):
        """v4.337 — journalise (lecture seule) POURQUOI un signal complet est refuse
        apres la structure H1 : kind = "sl_far" (SL > MTF_MAX_RISK_PCT), "sans_objectif"
        (aucune zone/objectif reel), "rr" (R:R < minimum). Garde risque %, gain % et
        R:R reels pour calibrer les seuils avec des chiffres. Une entree par actif,
        type et fenetre de 15 min ; 120 dernieres conservees par jour et par mode."""
        try:
            if not self.cfg.get("ENTRY_FUNNEL_TRACKING", 1):
                return
            bucket = int(time.time() // 900)
            seen = getattr(self, "_funnel_rr_seen", None)
            if seen is None:
                seen = self._funnel_rr_seen = set()
            ident = (mode, ticker, kind, bucket)
            if ident in seen:
                return
            if len(seen) > 5000:
                seen.clear()
            seen.add(ident)
            day = datetime.utcfromtimestamp(bucket * 900).strftime("%Y-%m-%d")
            store = getattr(self, "_funnel_rr", None)
            if store is None:
                store = self._funnel_rr = {}
            key = (day, mode)
            if key not in store:
                try:
                    store[key] = json.loads(db.get_meta(f"funnel_rr:{day}:{mode}") or "[]")
                except Exception:
                    store[key] = []
            store[key].append({
                "t": ticker, "k": kind,
                "rr": round(plan["rr"], 2) if plan.get("rr") is not None else None,
                "risk": round(plan["risk_pct"], 2),
                "gain": round(plan["reward_pct"], 2) if plan.get("reward_pct") is not None else None,
            })
            store[key] = store[key][-120:]
            db.set_meta(f"funnel_rr:{day}:{mode}", json.dumps(store[key]))
        except Exception:
            pass

    # ─────────────────────────────────────────────────────────────────────
    #  v4.338 — SUIVI DES SIGNAUX BLOQUES (lecture seule)
    #  Pour chaque signal complet (zone + signal M15) arrete par un filtre, on
    #  enregistre le prix, le SL et l objectif HYPOTHETIQUES (memes regles que
    #  le vrai trade), puis on regarde 2 h plus tard ce que le prix a fait :
    #  +1R atteint avant le SL ? SL touche d abord ? objectif ? Les trades
    #  REELLEMENT pris ("candidat") servent de reference. Aucun effet sur les
    #  entrees ; toute erreur est ignoree.
    # ─────────────────────────────────────────────────────────────────────
    def _blocked_pending_get(self):
        pend = getattr(self, "_blocked_pending", None)
        if pend is None:
            try:
                pend = json.loads(db.get_meta("blocked_pending") or "[]")
            except Exception:
                pend = []
            self._blocked_pending = pend
        return pend

    def _blocked_note(self, mode, ticker, gate, long_side, price, zone, target, extreme, atr, plan=None):
        try:
            cfg = self.cfg
            if not cfg.get("ENTRY_FUNNEL_TRACKING", 1) or not cfg.get("BLOCKED_FOLLOWUP_ENABLED", 1):
                return
            bucket = int(time.time() // 900)
            seen = getattr(self, "_blocked_seen", None)
            if seen is None:
                seen = self._blocked_seen = set()
            ident = (mode, ticker, gate, bucket)
            if ident in seen:
                return
            if len(seen) > 5000:
                seen.clear()
            seen.add(ident)
            if plan is None:
                plan = mtf.plan_trade("long" if long_side else "short", price, zone, target, extreme, atr)
            if not plan:
                return
            pend = self._blocked_pending_get()
            pend.append({
                "ts": int(time.time()), "mode": mode, "t": ticker, "g": gate,
                "d": "long" if long_side else "short", "p": price, "sl": plan["sl"], "tp": plan.get("tp"),
                "risk": round(plan["risk_pct"], 3),
                "rr": round(plan["rr"], 2) if plan.get("rr") is not None else None,
            })
            self._blocked_pending = pend[-600:]
            db.set_meta("blocked_pending", json.dumps(self._blocked_pending))
        except Exception:
            pass

    @staticmethod
    def _blocked_eval(ev, candles, horizon_sec):
        """Premier niveau touche apres l evenement : "sl", "r1" (+1R), "tp" ou "none",
        plus MFE/MAE et variation finale (% du prix). Bougies M15 dont l ouverture
        est APRES l evenement. Si SL et +1R/objectif sont touches dans la meme
        bougie, l ordre est inconnu : compte comme SL (prudent) et marque "amb"."""
        t0 = ev["ts"] * 1000
        start = (t0 // 900000 + 1) * 900000
        end = t0 + horizon_sec * 1000
        cs = [c for c in candles if start <= c["t"] < end]
        if len(cs) < max(4, horizon_sec // 900 - 2):
            return None
        p, sl, tp = ev["p"], ev["sl"], ev.get("tp")
        long_side = ev["d"] == "long"
        risk = (p - sl) if long_side else (sl - p)
        if risk <= 0:
            return None
        r1 = p + risk if long_side else p - risk
        outcome, amb = None, False
        for c in cs:
            if long_side:
                hs, h1, ht = c["l"] <= sl, c["h"] >= r1, bool(tp) and c["h"] >= tp
            else:
                hs, h1, ht = c["h"] >= sl, c["l"] <= r1, bool(tp) and c["l"] <= tp
            if outcome is None:
                if hs and (h1 or ht):
                    outcome, amb = "sl", True
                elif hs:
                    outcome = "sl"
                elif ht:
                    outcome = "tp"
                elif h1:
                    outcome = "r1"
        if long_side:
            mfe = (max(c["h"] for c in cs) - p) / p * 100
            mae = (p - min(c["l"] for c in cs)) / p * 100
            fin = (cs[-1]["c"] - p) / p * 100
        else:
            mfe = (p - min(c["l"] for c in cs)) / p * 100
            mae = (max(c["h"] for c in cs) - p) / p * 100
            fin = (p - cs[-1]["c"]) / p * 100
        return {"o": outcome or "none", "amb": amb, "mfe": round(mfe, 3), "mae": round(mae, 3), "fin": round(fin, 3)}

    def _blocked_store(self, ev, res):
        day = datetime.utcfromtimestamp(ev["ts"]).strftime("%Y-%m-%d")
        key = f"blocked_agg:{day}:{ev['mode']}"
        try:
            agg = json.loads(db.get_meta(key) or "{}")
        except Exception:
            agg = {}
        g = agg.setdefault(ev["g"], {"n": 0, "sl": 0, "r1": 0, "tp": 0, "none": 0, "amb": 0,
                                     "mfe": [], "mae": [], "fin": [], "ev": []})
        g["n"] += 1
        g[res["o"]] += 1
        if res["amb"]:
            g["amb"] += 1
        for k in ("mfe", "mae", "fin"):
            g[k] = (g[k] + [res[k]])[-200:]
        g["ev"] = (g["ev"] + [{"t": ev["t"], "o": res["o"], "mfe": res["mfe"], "mae": res["mae"], "risk": ev["risk"]}])[-60:]
        db.set_meta(key, json.dumps(agg))

    def _blocked_followups_if_due(self):
        """Evalue les signaux bloques dont le delai d observation est ecoule (toutes
        les 5 min au plus). Les evenements de plus de 12 h non evaluables sont abandonnes."""
        try:
            if not self.cfg.get("BLOCKED_FOLLOWUP_ENABLED", 1):
                return
            now = time.time()
            if now - getattr(self, "_blocked_last_run", 0) < 300:
                return
            self._blocked_last_run = now
            pend = self._blocked_pending_get()
            if not pend:
                return
            horizon = int(self.cfg.get("BLOCKED_FOLLOWUP_HORIZON_MIN", 120)) * 60
            keep, by_ticker = [], {}
            for ev in pend:
                age = now - ev["ts"]
                if age < horizon + 900:
                    keep.append(ev)
                elif age <= 12 * 3600:
                    by_ticker.setdefault(ev["t"], []).append(ev)
            for ticker, evs in by_ticker.items():
                candles = self._mtf_candles(ticker, "15m", 60)
                for ev in evs:
                    res = self._blocked_eval(ev, candles, horizon) if candles else None
                    if res is None:
                        keep.append(ev)      # donnees insuffisantes : reessaye jusqu a expiration
                    else:
                        self._blocked_store(ev, res)
            self._blocked_pending = keep[-600:]
            db.set_meta("blocked_pending", json.dumps(self._blocked_pending))
        except Exception as e:
            print(f"[BLOCKED-FOLLOWUP] erreur ignoree : {e}")

    def _funnel_flow_ok(self, ticker, price, long_side):
        """(flux, ok) — ok = favorable OU non mesure (meme regle que l entree)."""
        flow = self._compute_trade_flow_pressure(ticker, price)
        if flow is None:
            return None, True
        req = self.cfg.get("MTF_MIN_FLOW_PRESSURE", 0.0)
        return flow, ((flow >= req) if long_side else (flow <= -req))

    def _funnel_shadow(self, mode, ticker, price, v, long_side):
        """Quand la TENDANCE bloque : compte quand meme les alignements
        zone / signal M15 / flux (etapes "nt_*") pour mesurer ce que ferait
        une regle SANS tendance."""
        try:
            zone = v["support"] if long_side else v["resistance"]
            inside = v["in_support"] if long_side else v["in_resistance"]
            if not zone or not inside:
                return
            self._funnel_note(mode, ticker, "nt_zone")
            lower = self._mtf_candles(ticker, v["lower_tf"], 60, cache_only=True)
            nm, ex = mtf.candle_signal(lower, "long" if long_side else "short")
            if not nm:
                return
            if (long_side and ex > zone["high"] + v["tolerance"]) or (not long_side and ex < zone["low"] - v["tolerance"]):
                return
            self._funnel_note(mode, ticker, "nt_signal")
            if self._funnel_flow_ok(ticker, price, long_side)[1]:
                self._funnel_note(mode, ticker, "nt_signal_flux")
        except Exception:
            pass

    def _mm_reason(self, state, direction):
        """v4.329 — DIAGNOSTIC : pourquoi _measured_move_level ne renvoie rien
        (avant, un seul message "pas de creux/sommet 1h exploitable" pour 5
        causes differentes). Reprend les memes conditions, dans le meme ordre."""
        k = int(self.cfg.get("SPOT_ACCUM_PIVOT_CANDLES", 2))
        candles = list(state.candle_history_1h)
        window = candles[-int(self.cfg.get("SPOT_ACCUM_PIVOT_LOOKBACK_CANDLES", 72)):]
        if len(window) < 2 * k + 3:
            return f"historique 1h insuffisant ({len(window)} bougies)"
        long = direction == "long"
        col = 1 if long else 0
        name = "creux" if long else "sommet"
        pivots = [(i, window[i][col]) for i in range(k, len(window) - k)
                  if all((window[i][col] < window[j][col]) if long else (window[i][col] > window[j][col])
                         for j in range(i - k, i + k + 1) if j != i)]
        if len(pivots) < 2:
            return f"{len(pivots)} {name} 1h sur {len(window)} bougies (2 requis)"
        (_, prev), (i_last, last) = pivots[-2], pivots[-1]
        if long and last <= prev:
            return "dernier creux plus bas que le precedent (structure haussiere rompue)"
        if (not long) and last >= prev:
            return "dernier sommet plus haut que le precedent (structure baissiere rompue)"
        if long and any(c[2] < last for c in window[i_last + 1:]):
            return "dernier creux casse depuis (cloture en dessous)"
        if (not long) and any(c[2] > last for c in window[i_last + 1:]):
            return "dernier sommet casse depuis (cloture au-dessus)"
        return "amplitude du swing precedent inexploitable"

    def _measured_move_level(self, state, direction):
        """v4.321 — SUR DEMANDE EXPLICITE : chemin d entree DISTINCT du
        top-down zone-H4 (branche "OU" a part entiere, pas une condition de
        plus empilee dessus) — pour une tendance qui s eloigne de toute zone
        H4 testee (elle peut mettre des annees a y revenir pendant que le
        prix progresse). S appuie sur le support ascendant / la resistance
        descendante deja existants (_compute_rising_support /
        _compute_falling_resistance, ancien moteur, jamais branches sur le
        top-down) comme point d entree, ET calcule ici un objectif propre et
        justifie — l amplitude du dernier mouvement (swing precedent),
        projetee depuis ce nouveau point d appui ("mesure de mouvement" /
        "measured move", technique standard : la jambe precedente sert de
        gabarit pour la suivante). Retourne (niveau_entree, objectif) ou
        None si la structure ne qualifie pas (pas assez de pivots, jambe
        precedente trop faible)."""
        k = int(self.cfg.get("SPOT_ACCUM_PIVOT_CANDLES", 2))
        candles_1h = list(state.candle_history_1h)
        window = candles_1h[-int(self.cfg.get("SPOT_ACCUM_PIVOT_LOOKBACK_CANDLES", 72)):]
        if len(window) < 2 * k + 3:
            return None
        if direction == "long":
            entry_level = self._compute_rising_support(candles_1h)
            if entry_level is None:
                return None
            pivots = [(i, window[i][1]) for i in range(k, len(window) - k)
                     if all(window[i][1] < window[j][1] for j in range(i - k, i + k + 1) if j != i)]
            if len(pivots) < 2:
                return None
            i_prev, i_last = pivots[-2][0], pivots[-1][0]
            swing_high = max((c[0] for c in window[i_prev:i_last + 1]), default=None)
            if swing_high is None or swing_high <= pivots[-2][1]:
                return None
            amplitude = swing_high - pivots[-2][1]
            return entry_level, entry_level + amplitude
        else:
            entry_level = self._compute_falling_resistance(candles_1h)
            if entry_level is None:
                return None
            pivots = [(i, window[i][0]) for i in range(k, len(window) - k)
                     if all(window[i][0] > window[j][0] for j in range(i - k, i + k + 1) if j != i)]
            if len(pivots) < 2:
                return None
            i_prev, i_last = pivots[-2][0], pivots[-1][0]
            swing_low = min((c[1] for c in window[i_prev:i_last + 1]), default=None)
            if swing_low is None or swing_low >= pivots[-2][1]:
                return None
            amplitude = pivots[-2][1] - swing_low
            return entry_level, entry_level - amplitude

    def _maybe_refresh_dynamic_trend(self, ticker, state):
        """v4.203 / v4.284 — bougies 1h. Depuis la 4.284, elles arrivent en
        TEMPS REEL par WebSocket (voir _on_ws_candle) : l appel REST ne sert
        plus qu au chargement initial de l historique, a la resynchronisation
        de securite (toutes les 6 h) et au repli si le flux est muet."""
        now = time.time()
        store = self._candle_store(ticker, "1h")
        if store.get("ready") and not store.get("stale_resync") and self._ws_candle_fresh(store, 3600) and state.candle_history_1h \
                and now - store.get("synced_at", 0) < self.cfg.get("CANDLE_REST_RESYNC_SEC", 21600):
            return  # flux WebSocket actif : rien a recharger
        last_refresh = getattr(state, "dynamic_trend_last_refresh", 0)
        if not store.get("stale_resync") and now - last_refresh < 3600 and state.candle_history_1h \
                and now - store.get("synced_at", 0) < self.cfg.get("CANDLE_REST_RESYNC_SEC", 21600):
            return
        candles = self._fetch_candles_t(ticker, "1h", 210)
        if candles:
            self._reset_candle_store(store, candles, 250)
            self._apply_1h_candles(ticker)
            for st in self._states_for_ticker(ticker):
                st.dynamic_trend_last_refresh = now

    def _apply_1h_candles(self, ticker):
        """Recalcule tout ce qui depend des bougies 1h CLOTUREES de l actif
        (EMA200 1h, habitudes, tendance dynamique, support ascendant)."""
        store = self._candle_store(ticker, "1h")
        candles = [c[1:] for c in store["closed"]]
        if not candles:
            return
        for state in self._states_for_ticker(ticker):
            closes_1h = [c[2] for c in candles]
            state.ema200_1h = calc_ema(closes_1h, 200) if len(closes_1h) >= 200 else None
            state.last_close_1h = closes_1h[-1]
            # v4.281 — HABITUDES DE L ACTIF (7 derniers jours de bougies 1h) :
            # amplitude horaire mediane et volume horaire median (en $). Base
            # de la "qualite du marche" et de l anti-range RELATIF.
            recent_1h = candles[-168:]
            ranges = sorted((c[0] - c[1]) / c[2] * 100 for c in recent_1h if c[2] > 0)
            notionals = sorted(c[3] * c[2] for c in recent_1h if len(c) > 3)
            if len(ranges) >= 24:
                state.baseline_range_1h_pct = ranges[len(ranges) // 2]
            if len(notionals) >= 24:
                state.baseline_notional_1h = notionals[len(notionals) // 2]
            # La tendance dynamique n utilise que les 60 dernieres (comportement inchange)
            state.candle_history_1h = deque(candles[-60:], maxlen=200)
            self._update_dynamic_trend(state)

    def _maybe_refresh_5m_candles(self, ticker, state):
        """v4.236 / v4.284 — bougies 5 min. Depuis la 4.284, elles arrivent en
        TEMPS REEL par WebSocket : chaque bougie est prise en compte A L
        INSTANT ou elle se cloture (la derniere bougie cloturee, soit
        "l avant-derniere" du graphique). L appel REST ne sert plus qu au
        chargement initial, a la resynchronisation de securite (6 h) et au
        repli, cale sur les clotures, si le flux est muet."""
        now = time.time()
        store = self._candle_store(ticker, "5m")
        if store.get("ready") and not store.get("stale_resync") and self._ws_candle_fresh(store, 300) and getattr(state, "candle_history_5m", None) \
                and now - store.get("synced_at", 0) < self.cfg.get("CANDLE_REST_RESYNC_SEC", 21600):
            return  # flux WebSocket actif : rien a recharger
        bucket = int((now - 10) // 300)
        if not store.get("stale_resync") and bucket == getattr(state, "candles_5m_bucket", None) \
                and getattr(state, "candle_history_5m", None) \
                and now - store.get("synced_at", 0) < self.cfg.get("CANDLE_REST_RESYNC_SEC", 21600):
            return
        # 500 bougies (~41 h) : historique suffisant pour que l EMA de tendance
        # converge vers la valeur du graphique, et pour le support / la
        # resistance d Accumulation (~24 h = 288 bougies).
        candles = self._fetch_candles_t(ticker, "5m", 500)
        if candles:
            self._reset_candle_store(store, candles, 600)
            self._apply_5m_candles(ticker)
            for st in self._states_for_ticker(ticker):
                st.candles_5m_bucket = bucket

    def _apply_5m_candles(self, ticker):
        store = self._candle_store(ticker, "5m")
        candles = deque((c[1:] for c in store["closed"]), maxlen=600)
        for state in self._states_for_ticker(ticker):
            state.candle_history_5m = candles
            state.candles_5m_last_refresh = time.time()

    # ─────────────────────────────────────────────────────────────────────
    #  v4.284 — FLUX DE BOUGIES EN TEMPS REEL (WebSocket Hyperliquid)
    # ─────────────────────────────────────────────────────────────────────
    def _candle_store(self, ticker, interval):
        stores = getattr(self, "_candles_ws", None)
        if stores is None:
            stores = self._candles_ws = {}
        return stores.setdefault((ticker, interval), {"closed": deque(), "forming": None, "last_msg": 0, "ready": False})

    def _reset_candle_store(self, store, candles_t, maxlen):
        store["closed"] = deque(candles_t, maxlen=maxlen)
        store["ready"] = True
        store["synced_at"] = time.time()
        store["stale_resync"] = False

    def _states_for_ticker(self, ticker):
        return [st for slot, st in self.states.items() if ticker_from_slot_key(slot) == ticker]

    def _ws_candle_fresh(self, store, interval_sec):
        """Le flux a-t-il donne signe de vie recemment ? (un actif sans aucune
        transaction ne recoit pas de mise a jour : repli REST au-dela)."""
        return time.time() - store.get("last_msg", 0) < interval_sec * 2 + 60

    def _fetch_candles_t(self, ticker, interval, count):
        """Comme _fetch_candles, avec l horodatage d ouverture en tete :
        (t, high, low, close, volume), bougies CLOTUREES uniquement."""
        try:
            interval_sec = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}.get(interval, 3600)
            end_ms = int(time.time() * 1000)
            req = {"coin": ticker, "interval": interval, "startTime": end_ms - count * interval_sec * 1000, "endTime": end_ms}
            raw = self.info.post("/info", {"type": "candleSnapshot", "req": req})
            if not raw or not isinstance(raw, list):
                return []
            return [(int(c["t"]), float(c["h"]), float(c["l"]), float(c["c"]), float(c.get("v", 0)))
                    for c in raw if c.get("T", 0) <= end_ms]
        except Exception as e:
            print(f"[CANDLES] Echec recuperation bougies {interval} pour {ticker} : {e}")
            return []

    def _subscribe_candles(self):
        """Abonnement aux bougies 5 min et 1h de chaque actif (demarrage ET
        chaque reconnexion). Apres une reconnexion, l historique est marque a
        resynchroniser : les bougies manquees pendant la coupure sont
        rechargees par REST au prochain cycle."""
        if not self.cfg.get("CANDLE_WS_ENABLED", 1):
            return
        tickers = sorted({ticker_from_slot_key(s) for s in self.cfg["SYMBOLS"]})
        ok = 0
        for t in tickers:
            for iv in ("5m", "1h"):
                self._candle_store(t, iv)["stale_resync"] = True
                try:
                    self.info.subscribe({"type": "candle", "coin": t, "interval": iv}, self._on_ws_candle)
                    ok += 1
                except Exception as e:
                    print(f"[WS-CANDLES] Echec abonnement {t} {iv} : {e}")
        self.emit("log", {"msg": f"Bougies temps reel actives ({ok}/{len(tickers) * 2} flux 5 min + 1h) — indicateurs mis a jour a chaque cloture.", "level": "ok"})

    def _on_ws_candle(self, msg):
        """Mise a jour de la bougie EN COURS ; quand une nouvelle bougie
        commence, la precedente est cloturee et tous les indicateurs qui en
        dependent sont recalcules immediatement."""
        try:
            d = msg.get("data") or {}
            ticker, iv, t = d.get("s"), d.get("i"), int(d.get("t", 0))
            if not ticker or iv not in ("5m", "1h") or not t:
                return
            store = self._candle_store(ticker, iv)
            store["last_msg"] = time.time()
            forming = store.get("forming")
            if forming and t > forming["t"] and store.get("ready"):
                closed = store["closed"]
                if not closed or closed[-1][0] < forming["t"]:
                    closed.append((forming["t"], forming["h"], forming["l"], forming["c"], forming["v"]))
                    if iv == "5m":
                        self._apply_5m_candles(ticker)
                    else:
                        self._apply_1h_candles(ticker)
            store["forming"] = {"t": t, "o": float(d["o"]), "h": float(d["h"]), "l": float(d["l"]),
                                "c": float(d["c"]), "v": float(d.get("v", 0))}
        except Exception as e:
            print(f"[WS-CANDLES] Erreur traitement bougie : {e}")

    def _is_candle_bullish_now(self, state):
        """v4.175 — SUR DEMANDE EXPLICITE : la bougie EN COURS est-elle verte
        (haussiere) ? Utilise pour confirmer une entree LONG pres du
        support — differe de _candle_color_confirms_reversal (qui detecte
        un CHANGEMENT de couleur pour une SORTIE), ici on veut simplement
        la couleur ACTUELLE. Retourne None si pas assez de bougies
        (n empeche pas l entree par defaut, gere par l appelant)."""
        candles = list(state.candle_history)
        if len(candles) < 2:
            return None
        return candles[-1][2] >= candles[-2][2]

    def _is_candle_bearish_now(self, state):
        """v4.175 — meme principe que _is_candle_bullish_now, pour une
        entree SHORT pres de la resistance (bougie rouge)."""
        bullish = self._is_candle_bullish_now(state)
        return None if bullish is None else not bullish

    def _is_near_level_simple(self, price, level, max_pct):
        """v4.175 — SUR DEMANDE EXPLICITE : remplace la fenetre de
        proximite precise (5-15% de l amplitude) par un simple "assez
        proche" — % de distance directe au niveau (pas relatif a l
        amplitude), la couleur de bougie faisant desormais le plus gros du
        travail de confirmation plutot qu une precision de zone.
        v4.209 — CONSERVEE pour compatibilite, mais remplacee partout par
        _is_near_level_atr (proximite relative a la volatilite reelle de
        l actif, pas un % fixe identique pour tous)."""
        if level is None or level <= 0 or price is None:
            return False
        return abs(price - level) / level * 100 <= max_pct

    def _is_near_level_atr(self, state, price, level, atr_multiplier=1.0):
        """v4.209 — SUR DEMANDE EXPLICITE : remplace la proximite % FIXE
        (identique pour tous les actifs, ex: 1.0%) par une proximite
        RELATIVE A LA VOLATILITE REELLE de l actif (ATR) — un actif tres
        volatil (grand ATR) a une fenetre de "proximite" plus large, un
        actif calme (petit ATR) une fenetre plus etroite. Corrige le
        handicap signale : un % fixe est structurellement trop strict pour
        les actifs volatils (ratant de bons signaux) et trop laxiste pour
        les actifs calmes. Repli sur ENTRY_LEVEL_PROXIMITY_PCT (ancien
        comportement) si l ATR n est pas encore disponible (historique
        insuffisant)."""
        if level is None or level <= 0 or price is None:
            return False
        atr_abs, _ = calc_true_range_atr(list(state.candle_history), self.cfg.get("ATR_PERIOD", 14))
        if atr_abs is None or atr_abs <= 0:
            fallback_pct = self.cfg.get("ENTRY_LEVEL_PROXIMITY_PCT", 1.0)
            return abs(price - level) / level * 100 <= fallback_pct
        return abs(price - level) <= atr_abs * atr_multiplier

    def _structural_sl_broken(self, state, pos, price):
        """v4.176 — SUR DEMANDE EXPLICITE : generalise a TOUS les modes (sauf
        le mode range d Accumulation, qui garde sa propre logique) le SL
        structurel construit pour Spot-Accum — LONG : rupture CONFIRMEE du
        support memorise a l entree. SHORT : rupture CONFIRMEE de la
        resistance memorisee a l entree. Confirmation = patience
        (SL_PATIENCE_CYCLES) + changement de couleur de bougie, exactement
        comme le mecanisme Spot-Accum d origine.
        v4.190 — FIX BUG CRITIQUE : l ancien compteur STRICTEMENT
        consecutif se remettait a 0 des qu un SEUL cycle repassait au-dessus
        du niveau — dans un marche qui descend de facon IRREGULIERE
        (remonte brievement plusieurs fois avant de continuer sa chute), ce
        compteur n atteignait JAMAIS le seuil, laissant le prix deriver tres
        loin sous le support SANS AUCUNE protection (perte de -6% observee
        sur un cas reel, POL). Remplace par une FENETRE GLISSANTE : compte
        les cycles "casses" sur les N derniers cycles (N = 1.5x la
        patience), tolerant les breves remontees sans tout reinitialiser."""
        cfg = self.cfg
        direction = pos.get("type", "long")
        level = pos.get("support_at_entry") if direction == "long" else pos.get("resistance_at_entry")
        if level is None:
            return False
        broken = (price < level) if direction == "long" else (price > level)
        patience = cfg.get("SL_PATIENCE_CYCLES", 10)
        window_size = int(patience * 1.5)
        if not hasattr(state, "sl_breach_window") or state.sl_breach_window is None:
            state.sl_breach_window = deque(maxlen=window_size)
        elif state.sl_breach_window.maxlen != window_size:
            state.sl_breach_window = deque(state.sl_breach_window, maxlen=window_size)
        state.sl_breach_window.append(broken)
        breach_count = sum(state.sl_breach_window)
        # Diagnostic — SUR DEMANDE EXPLICITE, pour comprendre les FUTURS cas
        # de derive prolongee sous le support sans declenchement du SL.
        if broken and breach_count >= patience // 2 and breach_count < patience:
            print(f"[SL-STRUCT-DIAG] {pos.get('strategy')} {direction} — prix sous niveau depuis {breach_count}/{window_size} cycles recents (seuil {patience}), niveau={level:.6f}, prix={price:.6f}")
        return breach_count >= patience and self._candle_color_confirms_reversal(state, direction)

    def _candle_color_confirms_reversal(self, state, direction):
        """v4.162 — SUR DEMANDE EXPLICITE : extrait de _ttp_confirmed_to_close
        pour reutilisation SANS le compteur de patience partage (evite une
        collision avec ttp_breach_streak si utilise par un mecanisme
        different, comme RETOURNEMENT CONFIRME, qui a deja son PROPRE
        compteur de patience dedie)."""
        candles = list(state.candle_history)
        if len(candles) < 2:
            return True  # pas assez de bougies : ne bloque pas la fermeture
        prev_close, cur_close = candles[-2][2], candles[-1][2]
        prev_prev_close = candles[-3][2] if len(candles) >= 3 else prev_close
        prev_color_up = prev_close >= prev_prev_close
        cur_color_up = cur_close >= prev_close
        if direction == "long":
            return not (prev_color_up and cur_color_up)
        else:
            return not (not prev_color_up and not cur_color_up)

    def _ttp_confirmed_to_close(self, state, direction):
        """v4.154 — SUR DEMANDE EXPLICITE : meme principe de "patience" que
        le Stop Loss (anti-meche), applique au Trailing Take Profit, COUPLE
        a une confirmation par changement de couleur de bougie — les deux
        conditions sont synchronisees sur les MEMES donnees temps reel
        (candle_history, alimente en direct par le WebSocket).
        1) Patience : le declencheur de repli doit rester actif pendant
           TTP_PATIENCE_CYCLES cycles CONSECUTIFS (compteur remis a zero
           des que la condition de repli n est plus vraie).
        2) Couleur de bougie : la bougie EN COURS doit avoir change de
           couleur par rapport a la precedente, dans le sens du
           retournement attendu (rouge apres verte pour un LONG qui
           referme, verte apres rouge pour un SHORT) — confirme que le
           marche a REELLEMENT commence a s inverser, pas seulement une
           meche isolee sur la bougie precedente.
        Retourne True seulement si les DEUX conditions sont reunies."""
        cfg = self.cfg
        patience_cycles = cfg.get("TTP_PATIENCE_CYCLES", 5)
        state.ttp_breach_streak = getattr(state, "ttp_breach_streak", 0) + 1
        patience_ok = state.ttp_breach_streak >= patience_cycles
        color_ok = self._candle_color_confirms_reversal(state, direction)
        return patience_ok and color_ok

    def _detect_shooting_star_pattern(self, state):
        """v4.227 — SUR DEMANDE EXPLICITE : detecte le motif de bougie
        japonaise "etoile filante" (shooting star) — petit corps, longue
        meche HAUTE (au moins 2x le corps), meche basse quasi inexistante.
        Signal baissier classique, typiquement apres une hausse — les
        acheteurs ont pousse le prix plus haut, mais les vendeurs ont
        rejete cette hausse avant la cloture.
        Approxime le prix d ouverture par la cloture de la bougie
        PRECEDENTE (candle_history ne stocke pas l ouverture explicitement,
        approximation standard largement utilisee ailleurs dans ce bot).
        Retourne (est_etoile_filante, est_rouge) ou (False, None) si
        historique insuffisant."""
        candles = list(state.candle_history)
        if len(candles) < 2:
            return False, None
        high, low, close = candles[-1]
        open_approx = candles[-2][2]
        body = abs(close - open_approx)
        upper_wick = high - max(open_approx, close)
        lower_wick = min(open_approx, close) - low
        if body <= 0 and upper_wick <= 0:
            return False, None
        min_upper_wick_ratio = self.cfg.get("SHOOTING_STAR_MIN_UPPER_WICK_RATIO", 2.0)
        max_lower_wick_ratio = self.cfg.get("SHOOTING_STAR_MAX_LOWER_WICK_RATIO", 0.3)
        is_shooting_star = (
            body > 0
            and upper_wick >= body * min_upper_wick_ratio
            and lower_wick <= upper_wick * max_lower_wick_ratio
        )
        is_red = close < open_approx
        return is_shooting_star, is_red

    def _shooting_star_confirmed(self, state, ticker=None):
        """v4.227 — SUR DEMANDE EXPLICITE : suit la confirmation d une
        etoile filante ROUGE sur une duree soutenue (30 min par defaut)
        avant de la considerer confirmee — evite d agir sur un simple
        motif ponctuel sans suite reelle. Appelee a CHAQUE cycle : detecte
        le motif, demarre/maintient un chronometre de confirmation tant
        que le prix reste sous la cloture de la bougie ayant declenche le
        signal, et renvoie True une fois la duree requise atteinte.
        v4.247 — SUR DEMANDE EXPLICITE : si un ticker est fourni (usage
        cote ENTREE — l usage cote SORTIE reste inchange, ticker=None),
        exige EN PLUS une confirmation par le VRAI flux de transactions au
        moment ou le motif se forme — une vraie meche de rejet devrait
        s accompagner d une poussee de vente agressive au sommet. Ne
        bloque QUE si le flux est disponible et contredit clairement
        (jamais bloquant si donnees insuffisantes)."""
        is_star, is_red = self._detect_shooting_star_pattern(state)
        confirm_minutes = self.cfg.get("SHOOTING_STAR_CONFIRM_MINUTES", 30)
        now = time.time()

        if is_star and is_red:
            flow_ok = True
            if ticker is not None and self.cfg.get("SHOOTING_STAR_FLOW_CONFIRM_ENABLED", True):
                flow_pressure_ss = self._compute_trade_flow_pressure(ticker, price_now=state.current_price)
                if flow_pressure_ss is not None:
                    flow_ok = flow_pressure_ss <= -self.cfg.get("SHOOTING_STAR_FLOW_THRESHOLD", 0.1)
            if flow_ok and getattr(state, "shooting_star_pending_close", None) is None:
                state.shooting_star_pending_close = state.candle_history[-1][2]
                state.shooting_star_pending_since = now
        pending_close = getattr(state, "shooting_star_pending_close", None)
        if pending_close is None:
            return False

        # Le retournement doit se maintenir — prix reste sous la cloture
        # ayant declenche le signal. Une remontee au-dessus invalide/reset.
        if state.current_price is not None and state.current_price > pending_close:
            state.shooting_star_pending_close = None
            state.shooting_star_pending_since = None
            return False

        pending_since = getattr(state, "shooting_star_pending_since", None)
        if pending_since is None:
            return False
        if now - pending_since >= confirm_minutes * 60:
            return True
        return False

    def _detect_failed_breakout(self, state, direction, level, lookback_candles=20, ticker=None):
        """v4.221 — SUR DEMANDE EXPLICITE : detecte une CASSURE RATEE
        (fausse cassure) — un signal technique fort et INDEPENDANT du
        RSI/MACD. Principe : le prix a recemment DEPASSE un niveau cle
        (resistance pour un futur SHORT, support pour un futur LONG), mais
        n a PAS tenu — il est retombe de l autre cote. Ce pattern piege les
        traders qui avaient suivi la cassure initiale, et precede souvent
        un retournement franc, quel que soit l etat du RSI/MACD a cet
        instant (qui peuvent rester dans le sens de la tendance de fond
        malgre ce signal local fort).
        direction="short" : cherche une cassure RATEE au-DESSUS d une
        resistance (level) — signal de vente.
        direction="long" : cherche une cassure RATEE en-DESSOUS d un
        support (level) — signal d achat.
        Retourne True si detecte, False sinon (jamais bloquant — c est un
        BYPASS supplementaire, pas une exigence).

        v4.222/223/224 — SUR DEMANDE EXPLICITE : conception finale, apres
        reflexion sur la LOGIQUE de chaque garde-fou (pas juste leur
        nombre) :
        - MAGNITUDE, ANTI-REPETITION, RECENCE : 3 garde-fous OBLIGATOIRES,
          non-redondants entre eux (chacun protege contre un risque
          DIFFERENT : bruit, spam, coincidence retardee).
        - COULEUR DE BOUGIE : retiree — largement REDONDANTE avec la
          magnitude (une cloture qui depasse deja nettement le seuil de
          retour est mecaniquement de la bonne couleur dans l ecrasante
          majorite des cas).
        - VOLUME : contrairement a la couleur, le volume est une
          information VRAIMENT INDEPENDANTE du prix (participation reelle
          du marche) — le retirer purement et simplement etait une erreur.
          Mais l ajouter comme 4e condition OBLIGATOIRE recreait le risque
          initial (garde-fous qui ne s alignent jamais). Solution retenue :
          le signal se valide si le volume confirme, OU si le mouvement de
          prix est EXCEPTIONNELLEMENT fort (largement au-dessus du minimum
          requis) — un mouvement assez dramatique se suffit a lui-meme,
          meme sans confirmation de volume, evitant qu un volume
          insuffisant/indisponible ne bloque un signal par ailleurs tres
          clair."""
        if level is None or level <= 0:
            return False
        candles = list(state.candle_history)
        if len(candles) < lookback_candles:
            return False
        recent = candles[-lookback_candles:]
        current_close = recent[-1][2]

        # Garde-fou RECENCE : la cassure initiale doit avoir eu lieu dans
        # une fenetre COURTE et RECENTE (pas n importe ou dans les 20
        # bougies) — exclut la toute derniere bougie (le retour lui-meme).
        recency_window = self.cfg.get("FAILED_BREAKOUT_RECENCY_CANDLES", 8)
        breakout_search_window = recent[-(recency_window + 1):-1]
        if not breakout_search_window:
            return False

        min_magnitude_pct = self.cfg.get("FAILED_BREAKOUT_MIN_MAGNITUDE_PCT", 0.3)
        # Seuil "exceptionnel" pour le chemin alternatif sans volume —
        # multiple du minimum requis, pas une valeur independante a régler.
        strong_multiplier = self.cfg.get("FAILED_BREAKOUT_STRONG_MAGNITUDE_MULTIPLIER", 2.5)

        if direction == "short":
            # Garde-fou MAGNITUDE : la cassure au-dessus doit depasser le
            # niveau d au moins ce %, pas un simple depassement de bruit.
            breakout_threshold = level * (1 + min_magnitude_pct / 100)
            matching_breakout_highs = [c[0] for c in breakout_search_window if c[0] > breakout_threshold]
            if not matching_breakout_highs:
                return False
            breakout_extent_pct = (max(matching_breakout_highs) - level) / level * 100
            # Le retour en dessous doit AUSSI depasser ce % minimal.
            return_threshold = level * (1 - min_magnitude_pct / 100)
            if current_close >= return_threshold:
                return False
            return_extent_pct = (level - current_close) / level * 100
        else:
            breakout_threshold = level * (1 - min_magnitude_pct / 100)
            matching_breakout_lows = [c[1] for c in breakout_search_window if c[1] < breakout_threshold]
            if not matching_breakout_lows:
                return False
            breakout_extent_pct = (level - min(matching_breakout_lows)) / level * 100
            return_threshold = level * (1 + min_magnitude_pct / 100)
            if current_close <= return_threshold:
                return False
            return_extent_pct = (current_close - level) / level * 100

        # Chemin VOLUME-ou-FLUX-ou-MAGNITUDE-EXCEPTIONNELLE — voir docstring.
        # v4.247 — SUR DEMANDE EXPLICITE : ajoute le VRAI flux de
        # transactions (plus precis que le volume par bougie, transaction
        # par transaction) comme voie de confirmation supplementaire — une
        # cassure ratee devrait s accompagner d une poussee agressive dans
        # le sens du retournement au moment du rejet.
        movement_is_exceptional = (
            breakout_extent_pct >= min_magnitude_pct * strong_multiplier
            or return_extent_pct >= min_magnitude_pct * strong_multiplier
        )
        if not movement_is_exceptional and self.cfg.get("FAILED_BREAKOUT_REQUIRE_VOLUME", True):
            vol_confirms_fb = self._volume_confirms_accumulation(
                state, recent_candles=2,
                min_ratio=self.cfg.get("FAILED_BREAKOUT_VOLUME_MIN_RATIO", 1.3),
            )
            flow_confirms_fb = None
            if ticker is not None:
                flow_pressure_fb = self._compute_trade_flow_pressure(ticker, price_now=state.current_price)
                if flow_pressure_fb is not None:
                    flow_threshold = self.cfg.get("FAILED_BREAKOUT_FLOW_THRESHOLD", 0.15)
                    flow_confirms_fb = (flow_pressure_fb <= -flow_threshold) if direction == "short" else (flow_pressure_fb >= flow_threshold)
            # N importe laquelle des deux sources DISPONIBLES qui confirme
            # suffit. Rejette UNIQUEMENT si AU MOINS UNE source est
            # disponible et qu AUCUNE ne confirme (les deux absentes ->
            # laisse passer, coherent avec "donnees insuffisantes, jamais
            # bloquant").
            readings = [r for r in (vol_confirms_fb, flow_confirms_fb) if r is not None]
            if readings and not any(readings):
                return False

        # Garde-fou ANTI-REPETITION : cooldown minimal entre deux
        # declenchements sur le MEME actif+direction.
        cooldown_sec = self.cfg.get("FAILED_BREAKOUT_COOLDOWN_SEC", 1800)
        cooldown_attr = f"_last_failed_breakout_{direction}"
        last_trigger = getattr(state, cooldown_attr, 0)
        now = time.time()
        if now - last_trigger < cooldown_sec:
            return False
        setattr(state, cooldown_attr, now)
        # v4.248 — SUR DEMANDE EXPLICITE : calcule une INTENSITE du signal
        # (0.0-1.0) — plutot que de filtrer sur la tendance externe (en
        # retard sur ce type d evenement par nature), utilise la force du
        # signal LUI-MEME : magnitude de la cassure + du retour,
        # normalisee par rapport au seuil "exceptionnel". Le score de
        # confiance du trade resultant sera module par cette intensite —
        # une cassure ratee franche merite plus de confiance qu une
        # cassure ratee a peine au-dessus du minimum requis, meme si
        # toutes deux "valident" techniquement le signal.
        strong_threshold_calc = min_magnitude_pct * strong_multiplier
        intensity = min(1.0, (breakout_extent_pct + return_extent_pct) / (strong_threshold_calc * 2)) if strong_threshold_calc > 0 else 0.5
        state.failed_breakout_intensity = round(intensity, 3)
        return True

    def _detect_fresh_breakout(self, state, direction, lookback_candles):
        """v4.148 — SUR DEMANDE EXPLICITE : detecte le DEBUT d un mouvement
        de facon IMMEDIATE — contrairement a l EMA200 (lent par nature,
        exige un franchissement d une moyenne mobile) ou a la confirmation
        longue duree (exige des heures de mouvement deja accumule), verifie
        simplement si le prix ACTUEL vient d etablir un nouveau plus haut
        (LONG) ou plus bas (SHORT) sur les 'lookback_candles' dernieres
        bougies — signal structurel classique, quasi instantane, pour
        capturer un retournement ou une cassure au moment ou elle se
        produit, pas des heures plus tard.
        v4.156 — FIX BUG CRITIQUE : la version precedente se declenchait
        sur PRESQUE CHAQUE bougie d une tendance simplement reguliere (5/5
        declenchements observes sur une simulation de hausse lineaire,
        SANS consolidation) — "nouveau plus haut sur N bougies" est
        quasi-toujours vrai en tendance fluide, ne capturant donc PAS une
        vraie cassure mais n importe quelle poursuite de mouvement deja en
        cours. Exige desormais que les bougies PRECEDENTES (avant celle qui
        casse) aient ete relativement PLATES (vraie consolidation prealable,
        via _is_market_ranging) — une vraie cassure suit un resserrement,
        pas une simple continuation."""
        candles = list(state.candle_history)
        if len(candles) < lookback_candles:
            return False
        recent = candles[-lookback_candles:]
        consolidation_candles = recent[:-1]
        if len(consolidation_candles) < 5:
            return False
        cons_closes = [c[2] for c in consolidation_candles]
        cons_min, cons_max = min(cons_closes), max(cons_closes)
        cons_range_pct = (cons_max - cons_min) / cons_min * 100 if cons_min > 0 else None
        max_consolidation_pct = self.cfg.get("BREAKOUT_MAX_PRIOR_CONSOLIDATION_PCT", 3.0)
        if cons_range_pct is None or cons_range_pct > max_consolidation_pct:
            return False  # les bougies precedentes bougeaient deja trop — pas une vraie consolidation
        # v4.156 (suite) — une consolidation genuine OSCILLE (le prix finit
        # proche d ou il a commence), contrairement a une derive
        # directionnelle lente qui peut accidentellement avoir une faible
        # amplitude TOTALE tout en progressant CONSTAMMENT dans un sens.
        # Exige que le deplacement NET (debut->fin) reste une PETITE part
        # de l amplitude totale observee (sinon : deja une tendance, pas
        # une consolidation plate).
        net_change = abs(cons_closes[-1] - cons_closes[0])
        total_range = cons_max - cons_min
        max_directionality_ratio = self.cfg.get("BREAKOUT_MAX_CONSOLIDATION_DIRECTIONALITY", 0.5)
        if total_range <= 0 or (net_change / total_range) > max_directionality_ratio:
            return False  # derive directionnelle deguisee en "faible amplitude", pas une vraie consolidation
        if direction == "long":
            prior_high = max(c[0] for c in consolidation_candles)
            return recent[-1][2] > prior_high
        else:
            prior_low = min(c[1] for c in consolidation_candles)
            return recent[-1][2] < prior_low

    def _long_term_momentum_confirmed(self, state, direction, lookback_candles, min_change_pct):
        """v4.135 — SUR DEMANDE EXPLICITE : capture les mouvements LENTS en
        "escalier" (petits paliers sur plusieurs heures, direction nette
        mais sans franchissement net de l EMA200 a court terme) — compare
        le prix ACTUEL au prix d il y a 'lookback_candles' bougies (~2min
        chacune), via candle_history. Si l ecart net depasse min_change_pct
        dans le sens attendu, confirme la tendance MEME SI l EMA200 (fenetre
        courte, ~6h40) ne la confirme pas lui-meme — sert d alternative,
        pas de remplacement, a la verification EMA200 existante."""
        candles = list(state.candle_history)
        if len(candles) < lookback_candles:
            return False
        price_now = candles[-1][2]  # cloture la plus recente
        price_then = candles[-lookback_candles][2]  # cloture il y a N bougies
        if price_then <= 0:
            return False
        change_pct = (price_now - price_then) / price_then * 100
        if direction == "long":
            return change_pct >= min_change_pct
        else:
            return change_pct <= -min_change_pct

    def _unified_trend_confirmed(self, prices, trend_ok, state=None, streak_attr=None, min_stability_cycles=None, adx_threshold_override=None, momentum_min_change_override=None):
        """v4.58 — SUR DEMANDE EXPLICITE : verification de tendance PARTAGEE
        par les 3 modes (normal, Accumulation, Spot-Accumulation) — EMA200
        (trend_ok, deja calcule par l appelant) ET ADX >= seuil (tendance
        REELLEMENT forte, pas juste un franchissement de justesse).
        v4.75 — SUR DEMANDE EXPLICITE : parametres optionnels state/
        streak_attr/min_stability_cycles — si fournis, exige EN PLUS que la
        tendance soit STABLE depuis au moins ce nombre de cycles (pas juste
        vraie a l instant). Laisse None (par defaut) = comportement
        INCHANGE — seul Accumulation les fournit explicitement, le mode
        normal continue de fonctionner exactement comme avant.
        v4.115 — SUR DEMANDE EXPLICITE : adx_threshold_override permet a
        Accumulation d utiliser SON PROPRE seuil ADX (20 par defaut),
        DIFFERENT de celui du mode normal (25, inchange) — observe que
        AUCUN actif ne depassait jamais 25 sur un lot de 30, suggerant un
        seuil trop strict pour un usage courant."""
        if not trend_ok:
            # v4.135 — SUR DEMANDE EXPLICITE : avant d abandonner sur un
            # EMA200 court terme non confirme, verifie si un mouvement lent
            # en escalier sur plusieurs heures confirme quand meme la
            # meme direction — capture les cas ou l EMA200 (fenetre
            # courte) ne franchit jamais nettement, meme avec une
            # direction nette sur plusieurs heures.
            if state is not None and streak_attr is not None:
                direction = "long" if streak_attr == "trend_up_streak" else "short"
                lookback = self.cfg.get("LONG_TERM_MOMENTUM_LOOKBACK_CANDLES", 180)
                min_change = momentum_min_change_override if momentum_min_change_override is not None else self.cfg.get("LONG_TERM_MOMENTUM_MIN_CHANGE_PCT", 2.0)
                if self._long_term_momentum_confirmed(state, direction, lookback, min_change):
                    return True
            return False
        cfg = self.cfg
        if min_stability_cycles is not None and state is not None and streak_attr is not None:
            if getattr(state, streak_attr, 0) < min_stability_cycles:
                return False
        if not cfg.get("UNIFIED_REQUIRE_ADX_CONFIRM", True):
            return True
        adx = calc_adx(list(state.mtf_prices) if len(state.mtf_prices) >= (cfg.get("ADX_PERIOD", 14)*2+1) else prices, cfg.get("ADX_PERIOD", 14))
        adx_threshold = adx_threshold_override if adx_threshold_override is not None else cfg.get("ADX_TREND_THRESHOLD", 25.0)
        return adx is not None and adx >= adx_threshold

    # ─────────────────────────────────────────────────────────────────────
    #  v4.283 — TENDANCE ET SUPPORT/RESISTANCE SUR LES VRAIES BOUGIES 5 MIN
    # ─────────────────────────────────────────────────────────────────────
    def _trend_ema(self, state):
        """EMA de tendance de l actif, calculee sur les VRAIES bougies 5 min
        d Hyperliquid (EMA TREND_EMA_PERIOD_5M, 80 par defaut = ~6 h 40, meme
        horizon que l ancienne EMA200 sur points de 2 min) : verifiable sur le
        graphique Hyperliquid (EMA 80, unite 5 min). Repli sur l ancien calcul
        interne tant que l historique 5 min n est pas disponible."""
        period = int(self.cfg.get("TREND_EMA_PERIOD_5M", 80))
        c5 = getattr(state, "candle_history_5m", None)
        if c5 and len(c5) >= period:
            value, source = calc_ema([c[2] for c in c5], period), f"EMA{period} bougies 5 min"
        else:
            value = calc_ema(list(state.mtf_prices), 200) if len(state.mtf_prices) >= 5 else None
            source = "EMA200 interne (repli : bougies 5 min pas encore chargees)"
        state.trend_ema_value, state.trend_ema_source = value, source
        return value

    def _sr_levels(self, state, period_2min_candles):
        """Support/resistance = plus bas / plus haut des N dernieres VRAIES
        bougies 5 min (N = periode historique en bougies ~2 min x 2/5, pour
        garder le meme horizon). Repli sur les bougies internes."""
        n5 = max(5, round(period_2min_candles * 2 / 5))
        c5 = getattr(state, "candle_history_5m", None)
        if c5 and len(c5) >= n5:
            sup, res = calc_support_resistance_from_candles(c5, n5)
            return sup, res, f"{n5} bougies 5 min"
        sup, res = calc_support_resistance_from_candles(state.candle_history, period_2min_candles)
        return sup, res, "bougies internes (repli)"

    def _anti_range_threshold(self, state, absolute_pct, lookback_5m):
        """v4.281 — (seuil effectif en %, "relatif"|"absolu")."""
        base = getattr(state, "baseline_range_1h_pct", None)
        if self.cfg.get("ANTI_RANGE_RELATIVE_ENABLED", 1) and base:
            hours = max(lookback_5m * 5 / 60, 1 / 12)
            return base * self.cfg.get("ANTI_RANGE_REL_MULT", 0.6) * (hours ** 0.5), "relatif"
        return absolute_pct, "absolu"

    def _anti_range_text(self, state, absolute_pct, lookback_5m):
        pct, kind = self._anti_range_threshold(state, absolute_pct, lookback_5m)
        dur = lookback_5m * 5
        dur_txt = f"{dur // 60} h {dur % 60:02d}" if dur >= 60 else f"{dur} min"
        return f"marche en range (mouvement < {pct:.2f}% sur {dur_txt}, seuil {kind}{' a l actif' if kind == 'relatif' else ''})"

    def spread_pct(self, ticker):
        """v4.282 — SPREAD : ecart entre la meilleure offre d achat et de vente
        du carnet d ordres, en % du prix milieu. Lu a la demande (REST
        l2Book) avec un cache de MARKET_QUALITY_SPREAD_CACHE_SEC secondes par
        actif. None si le carnet est indisponible."""
        cache = getattr(self, "_spread_cache", None)
        if cache is None:
            cache = self._spread_cache = {}
        now = time.time()
        hit = cache.get(ticker)
        if hit and now - hit[0] < self.cfg.get("MARKET_QUALITY_SPREAD_CACHE_SEC", 30):
            return hit[1]
        value = None
        if self.info is not None:
            try:
                book = self.info.post("/info", {"type": "l2Book", "coin": ticker}) or {}
                levels = book.get("levels") or []
                if len(levels) == 2 and levels[0] and levels[1]:
                    bid, ask = float(levels[0][0]["px"]), float(levels[1][0]["px"])
                    mid = (bid + ask) / 2
                    if mid > 0 and ask >= bid:
                        value = round((ask - bid) / mid * 100, 4)
            except Exception as e:
                print(f"[SPREAD] Carnet d ordres {ticker} indisponible : {e}")
        cache[ticker] = (now, value)
        return value

    def market_quality(self, ticker, state):
        """v4.281 — QUALITE DU MARCHE d un actif, relative a ses habitudes :
          vol_ratio      : amplitude de la derniere heure / amplitude horaire mediane (7 j)
          activity_ratio : volume $ de la derniere heure / volume horaire median (7 j)
          flow           : pression du flux (fenetre courante)
        None pour une mesure indisponible."""
        vol_ratio = activity_ratio = None
        c5 = list(getattr(state, "candle_history_5m", None) or [])[-12:]
        if len(c5) >= 6:
            last_close = c5[-1][2]
            if last_close > 0 and getattr(state, "baseline_range_1h_pct", None):
                rng = (max(c[0] for c in c5) - min(c[1] for c in c5)) / last_close * 100
                vol_ratio = rng / state.baseline_range_1h_pct
            if getattr(state, "baseline_notional_1h", None) and len(c5[0]) > 3:
                notional = sum(c[3] * c[2] for c in c5) * (12 / len(c5))
                activity_ratio = notional / state.baseline_notional_1h
        flow = self._compute_trade_flow_pressure(ticker, price_now=state.current_price)
        return {"vol_ratio": round(vol_ratio, 2) if vol_ratio is not None else None,
                "activity_ratio": round(activity_ratio, 2) if activity_ratio is not None else None,
                "flow": round(flow, 2) if flow is not None else None,
                "spread_pct": self.spread_pct(ticker)}

    def _market_quality_block(self, ticker, state, strategy):
        """v4.281 — filtres de qualite du marche (DESACTIVES par defaut, a
        calibrer avec l export CSV) : marche endormi, deserte, flux sans
        conviction (ce dernier reglable par mode).
        v4.317 — SUR DEMANDE EXPLICITE : calibre pour Funding Contrarian a
        partir d une vraie analyse de serie (voir /api/stats/funding-streaks) —
        la plus longue serie perdante (11 trades, -0.49 USDC) etait
        concentree sur une fenetre horaire precise avec spread +71% plus
        large (0.024% vs 0.014%) ET activite anormalement ELEVEE (1.19x vs
        0.99x) que la serie gagnante — signature d un mouvement de marche
        directionnel (news/volatilite generale), pas un vrai desequilibre
        de funding a corriger. Ajoute un plafond MAXIMUM d activite (le
        filtre existant ne bloquait qu un marche TROP CALME, jamais TROP
        ACTIF) et des seuils PAR STRATEGIE (repli sur le seuil global si
        non defini) pour ne calibrer QUE Funding sans affecter Spot-Accum/
        Accumulation tant qu on n a pas leur propre analyse."""
        cfg = self.cfg
        prefix = {"spot_accumulation": "SPOT_ACCUM", "accumulation": "ACCUMULATION",
                  "funding_contrarian": "FUNDING", "forex": "FOREX"}.get(strategy, "FOREX")
        min_vol = cfg.get(f"{prefix}_MIN_VOL_RATIO", cfg.get("MARKET_QUALITY_MIN_VOL_RATIO", 0)) or 0
        min_act = cfg.get(f"{prefix}_MIN_ACTIVITY_RATIO", cfg.get("MARKET_QUALITY_MIN_ACTIVITY_RATIO", 0)) or 0
        max_act = cfg.get(f"{prefix}_MAX_ACTIVITY_RATIO", cfg.get("MARKET_QUALITY_MAX_ACTIVITY_RATIO", 0)) or 0
        min_conv = cfg.get(f"{prefix}_MIN_FLOW_CONVICTION", 0) or 0
        max_spread = cfg.get(f"{prefix}_MAX_SPREAD_PCT", cfg.get("MARKET_QUALITY_MAX_SPREAD_PCT", 0)) or 0
        if not (min_vol or min_act or max_act or min_conv or max_spread):
            return None
        q = self.market_quality(ticker, state)
        if max_spread and q["spread_pct"] is not None and q["spread_pct"] > max_spread:
            return f"spread trop large ({q['spread_pct']:.3f}% > {max_spread}%)"
        if min_vol and q["vol_ratio"] is not None and q["vol_ratio"] < min_vol:
            return f"marche endormi (volatilite {q['vol_ratio']:.2f}x son habitude < {min_vol}x)"
        if min_act and q["activity_ratio"] is not None and q["activity_ratio"] < min_act:
            return f"marche deserte (activite {q['activity_ratio']:.2f}x son habitude < {min_act}x)"
        if max_act and q["activity_ratio"] is not None and q["activity_ratio"] > max_act:
            return f"marche trop actif ({q['activity_ratio']:.2f}x son habitude > {max_act}x — mouvement directionnel probable, pas un repli a corriger)"
        if min_conv:
            if q["flow"] is None:
                return "flux sans donnees suffisantes (conviction exigee)"
            if abs(q["flow"]) < min_conv:
                return f"flux sans conviction (|{q['flow']:+.2f}| < {min_conv})"
        return None

    def _is_market_ranging(self, state, min_range_pct, lookback):
        """v4.127 — SUR DEMANDE EXPLICITE : detecteur de range DIRECT, base
        sur le mouvement REEL du prix. Mesure simplement : le prix a-t-il
        bouge de plus de min_range_pct sur les 'lookback' dernieres
        bougies ? Si non, marche considere en range, bloque l entree.
        v4.236 — SUR DEMANDE EXPLICITE : utilise desormais de VRAIES
        bougies 5 min d Hyperliquid (alignees sur l horloge), au lieu de
        mtf_prices (echantillonnage interne du bot, PAS synchronise avec
        les bougies reelles) — confirme comme source de confusion : un
        desaccord entre le range detecte et le mouvement visible sur le
        graphique Hyperliquid pour une periode pourtant similaire.
        'lookback' s exprime desormais en bougies 5 min. Repli sur
        mtf_prices si les bougies 5 min ne sont pas encore disponibles."""
        # v4.281 — SEUIL RELATIF A L ACTIF (sur demande explicite) : le seuil
        # absolu (2 % pour toutes les cryptos) jugeait "calme" un marche
        # normal pour BTC et "actif" un marche mou pour WIF. Le mouvement
        # minimal exige est desormais proportionnel a l amplitude HABITUELLE
        # de l actif (mediane horaire sur 7 jours), mise a l echelle de la
        # fenetre (racine du temps). Seuil absolu = repli si l habitude n est
        # pas encore connue.
        min_range_pct = self._anti_range_threshold(state, min_range_pct, lookback)[0]
        candles_5m = getattr(state, "candle_history_5m", None)
        if candles_5m and len(candles_5m) >= 5:
            recent = list(candles_5m)[-lookback:]
            highs = [c[0] for c in recent]
            lows = [c[1] for c in recent]
            price_range_pct = (max(highs) - min(lows)) / min(lows) * 100
            return price_range_pct < min_range_pct
        mtf = list(state.mtf_prices)[-lookback:]
        if len(mtf) < 5:
            return False
        price_range_pct = (max(mtf) - min(mtf)) / min(mtf) * 100
        return price_range_pct < min_range_pct

    def _unified_sr_amplitude_ok(self, support, resistance):
        """v4.58 — Amplitude S/R PARTAGEE par les 3 modes : la fourchette
        support-resistance doit faire au moins UNIFIED_MIN_SR_AMPLITUDE_PCT
        (3% par defaut) — evite les fourchettes trop plates.
        v4.113 — SUR DEMANDE EXPLICITE : mis en pause temporairement — jugee
        possiblement peu fiable (sensible au bruit, ne mesure pas si le
        niveau a ete reellement teste plusieurs fois) — desactivable via
        UNIFIED_REQUIRE_SR_AMPLITUDE (False = neutralise cette obligation
        en attendant un mecanisme plus robuste base sur les touches
        multiples du support/resistance)."""
        cfg = self.cfg
        if not cfg.get("UNIFIED_REQUIRE_SR_AMPLITUDE", False):
            return True
        if support is None or resistance is None or support <= 0:
            return False
        min_pct = cfg.get("UNIFIED_MIN_SR_AMPLITUDE_PCT", 3.0)
        return (resistance - support) / support * 100 >= min_pct

    def _unified_proximity_ok(self, price, support, resistance, direction, allow_breakout=True, min_pct_override=None, max_pct_override=None, amplitude_override=None):
        """v4.108 — FIX BUG CRITIQUE : le calcul precedent (v4.58) exprimait
        1-5% en % du PRIX du support — incoherent avec le seuil structurel
        du trailing (70% de l AMPLITUDE support-resistance) et pouvant
        techniquement autoriser une entree au-dela de la resistance si
        l amplitude etait proche du minimum (2%). Desormais exprime en % de
        l AMPLITUDE elle-meme (coherent avec le reste du systeme) —
        recalibre a 5-10% (au lieu de 1-5%, qui deviendrait ridiculement
        etroit en mouvement de prix reel une fois exprime en % d amplitude).
        Pour un LONG, le prix doit etre entre UNIFIED_MIN/MAX_ABOVE_SUPPORT_PCT
        pourcent DE L AMPLITUDE au-dessus du support (et symetriquement pour
        un SHORT, sous la resistance). allow_breakout=True permet aussi une
        cassure nette comme alternative (utilise par Normal/Accumulation,
        pas par Spot-Accum).
        v4.117 — SUR DEMANDE EXPLICITE : min_pct_override/max_pct_override
        permettent a Accumulation d utiliser SA PROPRE fenetre (5-20% par
        defaut), plus large que celle du mode normal (5-10%, inchangee) —
        observe que la fenetre 5-10% etait le principal facteur bloquant
        Accumulation une fois l ADX et l amplitude minimale traites.
        v4.133 — SUR DEMANDE EXPLICITE : amplitude_override permet de
        calculer le % de proximite par rapport a une amplitude DIFFERENTE
        de (resistance-support) — utilise par Accumulation, dont le S/R est
        desormais calcule sur 24h (niveaux structurels), mais dont l
        amplitude de reference pour juger la proximite reste basee sur les
        4 dernieres heures (plus reactif aux conditions recentes)."""
        cfg = self.cfg
        min_pct = min_pct_override if min_pct_override is not None else cfg.get("UNIFIED_MIN_ABOVE_SUPPORT_PCT", 5.0)
        max_pct = max_pct_override if max_pct_override is not None else cfg.get("UNIFIED_MAX_ABOVE_SUPPORT_PCT", 10.0)
        if direction == "long":
            if support is None or support <= 0 or resistance is None or resistance <= support:
                return False
            amplitude = amplitude_override if amplitude_override is not None else (resistance - support)
            if amplitude is None or amplitude <= 0:
                return False
            dist_pct = (price - support) / amplitude * 100
            in_window = min_pct <= dist_pct <= max_pct
            breakout = allow_breakout and price > resistance
            return in_window or breakout
        else:  # short
            if resistance is None or resistance <= 0 or support is None or support >= resistance:
                return False
            amplitude = amplitude_override if amplitude_override is not None else (resistance - support)
            if amplitude is None or amplitude <= 0:
                return False
            dist_pct = (resistance - price) / amplitude * 100
            in_window = min_pct <= dist_pct <= max_pct
            breakout = allow_breakout and price < support
            return in_window or breakout

    def _gate_active_or_auto_activate(self, ticker, confidence, direction):
        """v4.4 — Auto-activation SUPPRIMEE sur demande explicite : un actif
        non selectionne reste desormais TOUJOURS bloque, quelle que soit la
        confiance du signal (meme 99%).
        v4.44 — SUR DEMANDE EXPLICITE : chaque mode (Accumulation, Funding
        Contrarian, Spot-Accumulation) peut desormais avoir sa PROPRE liste
        d actifs actifs, independante de la liste globale (Marches). Tant
        qu aucune liste dediee n est definie pour un mode (valeur None),
        il continue de retomber sur la liste globale ACTIVE_COINS — aucun
        changement de comportement tant que vous ne personnalisez rien.
        Retourne True si le trade peut s executer, False sinon.
        """
        mode_key_map = {
            "accumulation": "ACCUMULATION_ACTIVE_COINS",
            "funding_contrarian": "FUNDING_ACTIVE_COINS",
            "spot_accumulation": "SPOT_ACCUM_ACTIVE_COINS",
        }
        override_key = mode_key_map.get(direction)
        if override_key:
            override_list = self.cfg.get(override_key)
            if override_list is not None:
                return ticker in override_list
        active_coins = self.cfg.get("ACTIVE_COINS")
        if active_coins is None or ticker in active_coins:
            return True  # pas de restriction, ou deja actif
        return False  # inactif -> bloque, quelle que soit la confiance

    def _register_max_loss(self, ticker, entry_confidence=None):
        """Apres un Max Loss / SL securite sur un actif, on releve son seuil
        de confiance minimum requis a : confiance qu avait CE trade a son
        entree + 5% (pas juste +5% sur le dernier seuil utilise) — plus un
        trade perdant avait ete pris avec confiance elevee, plus la barre
        remonte haut pour le prochain, jusqu au plafond CONFIDENCE_MAX_PCT.
        Si la confiance d entree n est pas disponible (positions recuperees
        apres un crash, anciennes positions), on se rabat sur l ancien
        comportement (+5% sur le seuil actuel).
        """
        from datetime import timezone
        base = self.cfg.get("CONFIDENCE_MIN_PCT", 65.0)
        step = self.cfg.get("CONFIDENCE_STEP_PCT", 5.0)
        cap  = self.cfg.get("CONFIDENCE_MAX_PCT", 90.0)
        current = self.confidence_thresholds.get(ticker, base)
        if entry_confidence is not None:
            new_threshold = min(entry_confidence + step, cap)
        else:
            new_threshold = min(current + step, cap)
        if new_threshold != current:
            self.confidence_thresholds[ticker] = new_threshold
            self.confidence_threshold_set_at[ticker] = datetime.now(timezone.utc)
            self.emit("log", {"msg": f"[{ticker}] Confiance minimale requise relevee a {new_threshold:.0f}% (confiance d entree {entry_confidence:.0f}% + {step:.0f}%)" if entry_confidence is not None else f"[{ticker}] Confiance minimale requise relevee a {new_threshold:.0f}% (apres perte)", "level": "warn"})
            self._save_confidence_thresholds()

    def _register_win(self, ticker):
        """v4.3 — Apres une sortie positive (TTP), la confiance minimum
        requise redescend D UN PAS (CONFIDENCE_STEP_PCT) vers la base, PAS
        d un coup jusqu a la base. Avant ce fix, un unique petit gain (meme
        de quelques centimes) effacait INTEGRALEMENT toute la mefiance
        accumulee suite a plusieurs pertes — sur un marche agite/en range,
        ca cree un cycle perte-perte-perte-petit gain(reset total)-perte...
        qui empeche la protection de vraiment s accumuler et laisse le bot
        retrader activement un actif difficile toute une journee (observe
        concretement le 15/07 : 78 trades, 30.8% de reussite, pire journee).
        Desormais il faut autant de gains que de pertes essuyees pour
        redescendre completement a la base — une seule bonne surprise ne
        suffit plus a effacer un historique de pertes recentes."""
        base = self.cfg.get("CONFIDENCE_MIN_PCT", 65.0)
        step = self.cfg.get("CONFIDENCE_STEP_PCT", 5.0)
        current = self.confidence_thresholds.get(ticker, base)
        if current > base:
            from datetime import timezone
            new_threshold = max(current - step, base)
            if new_threshold <= base:
                self.confidence_thresholds[ticker] = base
                self.confidence_threshold_set_at.pop(ticker, None)
            else:
                self.confidence_thresholds[ticker] = new_threshold
                # on rafraichit l horodatage : le decay (v3.2) continue de
                # s appliquer normalement depuis ce nouveau palier, pas
                # depuis l ancien plus eleve.
                self.confidence_threshold_set_at[ticker] = datetime.now(timezone.utc)
            self.emit("log", {"msg": f"[{ticker}] Confiance minimale requise abaissee a {self.confidence_thresholds.get(ticker, base):.0f}% (apres gain Quick Profit/Trailing TP — descente progressive, pas totale)", "level": "ok"})
            self._save_confidence_thresholds()

    def _decay_confidence_thresholds(self):
        """v3.2 — Sur demande : evite qu un actif reste bloque INDEFINIMENT a
        un seuil de confiance eleve. Auparavant, la SEULE facon de redescendre
        etait de GAGNER un trade sur cet actif — mais plus le seuil est haut,
        moins il a de chances de se qualifier pour une nouvelle tentative qui
        lui permettrait justement de redescendre (risque de blocage
        permanent). Apres CONFIDENCE_RESET_HOURS (defaut 2h) sans avoir pu
        rejouer sa chance sur cet actif, le seuil redescend automatiquement
        au niveau de base — lui donnant une nouvelle occasion equitable,
        plutot que de rester "au frigo" indefiniment.
        Appelee une fois par cycle (verification peu couteuse)."""
        from datetime import timezone
        reset_hours = self.cfg.get("CONFIDENCE_RESET_HOURS", 2.0)
        if reset_hours <= 0:
            return  # decroissance desactivee si regle a 0 ou moins
        base = self.cfg.get("CONFIDENCE_MIN_PCT", 65.0)
        now = datetime.now(timezone.utc)
        for ticker in list(self.confidence_thresholds.keys()):
            set_at = self.confidence_threshold_set_at.get(ticker)
            if set_at is None:
                continue  # pas d horodatage (ex: restaure d une ancienne sauvegarde) -> ne pas forcer un reset immediat
            elapsed_h = (now - set_at).total_seconds() / 3600
            if elapsed_h >= reset_hours:
                self.confidence_thresholds[ticker] = base
                self.confidence_threshold_set_at.pop(ticker, None)
                self.emit("log", {
                    "msg": f"[{ticker}] Confiance minimale requise redescendue a {base:.0f}% ({elapsed_h:.1f}h sans nouvelle tentative — nouvelle chance equitable accordee)",
                    "level": "ok"
                })
                self._save_confidence_thresholds()

    # ── v3.1 : Blackout CPI (Finnhub) ────────────────────────────────────────
    def _refresh_cpi_events_if_needed(self):
        """Rafraichit le calendrier CPI depuis Finnhub au maximum toutes les
        CPI_CACHE_REFRESH_HOURS heures (evite de spammer l API a chaque cycle).
        En cas d echec, timeout ou cle absente, le cache reste vide et aucun
        blackout n est applique (le bot continue de trader normalement).
        v3.1 : l appel reseau tourne dans un thread separe avec timeout — un
        gel de ce bloc (observe en usage reel, coincidant exactement avec la
        toute premiere execution de ce code apres la collecte) ne doit plus
        jamais bloquer le cycle principal du bot.
        """
        from datetime import datetime as _dt, timezone
        now = _dt.now(timezone.utc)
        refresh_h = self.cfg.get("CPI_CACHE_REFRESH_HOURS", 12)
        if self._cpi_last_fetch is not None and (now - self._cpi_last_fetch).total_seconds() < refresh_h * 3600:
            return
        self._cpi_last_fetch = now
        api_key = self.cfg.get("FINNHUB_API_KEY", "")
        if not api_key:
            return
        result = {}
        def _worker():
            result["events"] = fetch_cpi_events_from_finnhub(api_key)
        t = threading.Thread(target=_worker, daemon=True)
        t.start()
        t.join(timeout=self.cfg.get("PRICE_FETCH_TIMEOUT_SEC", 10))
        if t.is_alive():
            self.emit("log", {"msg": "⚠ Calendrier CPI (Finnhub) : reponse trop lente — abandon, nouvel essai au prochain rafraichissement", "level": "warn"})
            return
        events = result.get("events", [])
        self._cpi_events = events
        if events:
            next_ev = events[0].strftime("%d/%m %H:%M UTC")
            self.emit("log", {"msg": f"Calendrier CPI (Finnhub) mis a jour — prochaine annonce : {next_ev}", "level": "info"})
        else:
            self.emit("log", {"msg": "Calendrier CPI (Finnhub) : aucune donnee recuperee — blackout CPI inactif", "level": "dim"})

    def _is_cpi_blackout(self):
        """Retourne (True, datetime_event) si l instant present tombe dans la
        fenetre de blackout autour d une annonce CPI, sinon (False, None)."""
        if not self._cpi_events:
            return False, None
        from datetime import datetime as _dt, timezone, timedelta
        now    = _dt.now(timezone.utc)
        before = timedelta(minutes=self.cfg.get("CPI_BLACKOUT_BEFORE_MIN", 15))
        after  = timedelta(minutes=self.cfg.get("CPI_BLACKOUT_AFTER_MIN", 30))
        for ev in self._cpi_events:
            if ev - before <= now <= ev + after:
                return True, ev
        return False, None

    def _subscribe_trade_flow(self):
        """v4.266 — abonnement au flux 'trades' de chaque actif (demarrage ET
        chaque reconnexion WebSocket)."""
        trade_flow_tickers = sorted({ticker_from_slot_key(s) for s in self.cfg["SYMBOLS"]})
        ws_trades_subscribed = 0
        for tf_ticker in trade_flow_tickers:
            try:
                self.info.subscribe({"type": "trades", "coin": tf_ticker}, self._on_ws_trades)
                ws_trades_subscribed += 1
            except Exception as e:
                print(f"[WS-TRADES] Echec abonnement flux transactions pour {tf_ticker} : {e}")
        self._ws_trades_subscribed_at = time.time()
        if ws_trades_subscribed > 0:
            self.emit("log", {"msg": f"Flux de transactions temps reel actif ({ws_trades_subscribed}/{len(trade_flow_tickers)} actifs, fenetre {self.cfg.get('TRADE_FLOW_WINDOW_SEC', 180)}s).", "level": "ok"})
        else:
            self.emit("log", {"msg": "⚠️ Flux de transactions WebSocket indisponible — repli sur requetes REST.", "level": "warn"})

    def _on_ws_trades(self, msg):
        """v4.263 — SUR DEMANDE EXPLICITE : callback WebSocket Hyperliquid
        — flux 'trades' temps reel (un abonnement par actif). Alimente un
        buffer en memoire (deque plafonnee) par ticker, lu ensuite par
        _compute_trade_flow_pressure — remplace le sondage REST periodique
        (toutes les 60s) par une alimentation continue, sans latence ni
        risque d echec de requete repete. Format attendu : {"channel":
        "trades", "data": [{"coin": ..., "side": "B"|"A", "sz": ..., ...}]}."""
        try:
            data = msg.get("data") if isinstance(msg, dict) else None
            if not data or not isinstance(data, list):
                return
            if not hasattr(self, "_ws_trades_buffer"):
                self._ws_trades_buffer = {}
            # v4.266 — horodatage de chaque transaction (fenetre de temps
            # fixe) + heure de derniere reception (detection d un flux mort).
            now_ms = int(time.time() * 1000)
            keep_ms = max(self.cfg.get("TRADE_FLOW_WINDOW_SEC", 180) * 2, 600) * 1000
            if not hasattr(self, "_ws_trades_last_rx"):
                self._ws_trades_last_rx = {}
            # v4.349 — enregistreur de flux : meme transactions, agregees par tranche de 5 minutes. Protege : une
            # erreur ici ne doit JAMAIS perturber le calcul de pression utilise par les entrees.
            try:
                if self.cfg.get("FLOW_RECORDER_ENABLED", 1):
                    rec = getattr(self, "flow_recorder", None)
                    if rec is None:
                        rec = self.flow_recorder = FlowRecorder(big_usd=self.cfg.get("FLOW_RECORDER_BIG_TRADE_USD", 5000.0))
                    rec.on_trades(data, now_ms)
            except Exception as e:
                print(f"[FLUX-ENREGISTREUR] erreur ignoree : {e}")
            for t in data:
                coin = t.get("coin")
                if not coin:
                    continue
                buf = self._ws_trades_buffer.get(coin)
                if buf is None:
                    buf = deque(maxlen=20000)
                    self._ws_trades_buffer[coin] = buf
                try:
                    t_ms = int(t.get("time") or now_ms)
                except (TypeError, ValueError):
                    t_ms = now_ms
                buf.append({"sz": t.get("sz"), "side": t.get("side"), "t": t_ms})
                while buf and buf[0]["t"] < now_ms - keep_ms:
                    buf.popleft()
                self._ws_trades_last_rx[coin] = time.time()
            self._ws_trades_last_any = time.time()
        except Exception as e:
            print(f"[WS-TRADES] Erreur traitement flux trades : {e}")

    def _on_ws_allmids(self, msg):
        """Callback WebSocket Hyperliquid — flux 'allMids' (prix mid de tous
        les actifs, mis a jour en temps reel par l exchange).
        Tourne dans le thread interne du SDK Hyperliquid (PAS le thread
        principal du bot _run) : des qu un prix arrive pour un actif ayant
        une position ouverte, on verifie IMMEDIATEMENT Max Loss / SL securite /
        Trailing TP via _manage_position (thread-safe grace a self.lock),
        sans attendre le prochain cycle de CYCLE_INTERVAL secondes.
        Format du message attendu : {"channel": "allMids", "data": {"mids": {...}}}
        (a verifier selon la version du SDK hyperliquid-python-sdk installee —
        cette integration n a pas pu etre testee en conditions reelles, faute
        d acces reseau dans l environnement de developpement).
        """
        try:
            data = msg.get("data", {}) if isinstance(msg, dict) else {}
            mids = data.get("mids", {})
            if not mids:
                return
            self._last_ws_tick = time.time()
            # v3.2 : cache BRUT de tous les prix recus (pas seulement nos
            # symboles tradés) — permet a l API web d afficher un marche
            # complet (jusqu a 30 cryptos) sans avoir besoin d ouvrir une
            # position sur chacun.
            # v4.199 — FIX BUG CRITIQUE : remplace l ECRASEMENT complet
            # (self.all_mids = mids) par une FUSION — l ancien code effacait
            # systematiquement les cles forex ("xyz:EUR" etc., ajoutees par
            # _on_ws_allmids_forex) a chaque tick crypto, qui survient bien
            # plus frequemment — confirme par des logs reels montrant le
            # forex reçu via WS mais absent de self.all_mids au moment ou le
            # cycle principal le consultait 10s plus tard.
            if not isinstance(self.all_mids, dict):
                self.all_mids = {}
            self.all_mids.update(mids)
            # v4.199 (suite) — les cles NATIVES absentes du dernier message
            # (actif temporairement sans mise a jour) doivent neanmoins
            # rester disponibles — mids contient TOUJOURS l ensemble complet
            # des actifs natifs a chaque tick (contrairement au forex, qui
            # arrive par un flux separe), donc aucun risque de cle crypto
            # perimee ici.
            # v4.36 — SUR DEMANDE EXPLICITE : suit le plus HAUT/BAS reel de
            # TOUS les actifs (pas seulement ceux en position) a chaque tick
            # WebSocket — alimente de vraies bougies OHLC pour l EMA200 et un
            # vrai calcul d ATR (Wilder), au lieu d un simple point de prix
            # espace de 2 minutes qui ne voyait jamais les mouvements entre
            # deux echantillonnages.
            for slot_key, state in self.states.items():
                ticker = ticker_from_slot_key(slot_key)
                raw = mids.get(ticker)
                if raw is None:
                    continue
                try:
                    tick_price = float(raw)
                except (TypeError, ValueError):
                    continue
                if tick_price <= 0:
                    continue
                # v4.291 — FIX : le prix courant n etait mis a jour par le
                # WebSocket QUE pour les actifs EN POSITION (et le cycle REST
                # ne l ecrit plus quand le WebSocket est sain) : pour tous les
                # autres, il restait fige (dernier prix connu, ou 0). Le
                # diagnostic affichait donc "prix en dessous" partout et une
                # situation "fond neutre", et la largeur du regime de marche
                # pouvait etre calculee sur des prix perimes.
                state.current_price = tick_price
                if state.window_high is None or tick_price > state.window_high:
                    state.window_high = tick_price
                if state.window_low is None or tick_price < state.window_low:
                    state.window_low = tick_price

            for slot_key, state in list(self.states.items()):
                if not state.position:
                    continue
                ticker = ticker_from_slot_key(slot_key)
                raw = mids.get(ticker)
                if raw is None:
                    continue
                try:
                    price = float(raw)
                except (TypeError, ValueError):
                    continue
                if price <= 0:
                    continue
                state.current_price = price
                self._manage_position(slot_key, price, state)

            # v4.121/v4.145 — SUR DEMANDE EXPLICITE : gere aussi, EN
            # PARALLELE, les positions Accumulation (emplacement separe de
            # self.states) — meme logique de surveillance temps reel. Ce
            # bloc avait disparu lors d une manipulation de fichiers
            # anterieure, expliquant un PnL/prix actuel fige (jamais mis a
            # jour) sur les positions Accumulation.
            for slot_key, accum_state in list(self.accum_states.items()):
                if not accum_state.position:
                    continue
                ticker = ticker_from_slot_key(slot_key)
                raw = mids.get(ticker)
                if raw is None:
                    continue
                try:
                    price = float(raw)
                except (TypeError, ValueError):
                    continue
                if price <= 0:
                    continue
                accum_state.current_price = price
                self._manage_position(slot_key, price, accum_state)
            self._manual_tick()  # v4.273 — positions et ordres programmes manuels
        except Exception as e:
            print(f"[WS] Erreur traitement flux allMids : {e}")

    def _on_ws_allmids_forex(self, msg):
        """v4.166 — SUR DEMANDE EXPLICITE : callback dedie au flux 'allMids'
        du DEX HIP-3 "xyz" (forex, mode Normal) — l API Hyperliquid isole
        ce DEX du DEX natif, une souscription SEPAREE (avec le champ "dex")
        est necessaire pour recevoir EUR/JPY/KRW/DXY. Meme logique que
        _on_ws_allmids (fenetre haut/bas, prix courant, gestion de
        position), mais scoped uniquement aux tickers forex — FUSIONNE
        dans self.all_mids (cles brutes "EUR" etc., aucune collision
        possible avec les tickers crypto existants) plutot que de
        l ecraser."""
        try:
            data = msg.get("data", {}) if isinstance(msg, dict) else {}
            mids = data.get("mids", {})
            if not mids:
                return
            self._last_ws_tick = time.time()
            # Fusion (pas ecrasement) — self.all_mids garde aussi les
            # cryptos alimentees par _on_ws_allmids.
            if not isinstance(self.all_mids, dict):
                self.all_mids = {}
            self.all_mids.update(mids)

            forex_tickers = set(self.cfg.get("FOREX_MODE_SYMBOLS", []))
            for slot_key, state in self.states.items():
                ticker = ticker_from_slot_key(slot_key)
                if ticker not in forex_tickers:
                    continue
                raw = mids.get(ticker)
                if raw is None:
                    continue
                try:
                    tick_price = float(raw)
                except (TypeError, ValueError):
                    continue
                if tick_price <= 0:
                    continue
                # v4.291 — FIX : le prix courant n etait mis a jour par le
                # WebSocket QUE pour les actifs EN POSITION (et le cycle REST
                # ne l ecrit plus quand le WebSocket est sain) : pour tous les
                # autres, il restait fige (dernier prix connu, ou 0). Le
                # diagnostic affichait donc "prix en dessous" partout et une
                # situation "fond neutre", et la largeur du regime de marche
                # pouvait etre calculee sur des prix perimes.
                state.current_price = tick_price
                if state.window_high is None or tick_price > state.window_high:
                    state.window_high = tick_price
                if state.window_low is None or tick_price < state.window_low:
                    state.window_low = tick_price

            for slot_key, state in list(self.states.items()):
                ticker = ticker_from_slot_key(slot_key)
                if ticker not in forex_tickers or not state.position:
                    continue
                raw = mids.get(ticker)
                if raw is None:
                    continue
                try:
                    price = float(raw)
                except (TypeError, ValueError):
                    continue
                if price <= 0:
                    continue
                state.current_price = price
                self._manage_position(slot_key, price, state)
            self._manual_tick()  # v4.273
        except Exception as e:
            print(f"[WS-FOREX] Erreur traitement flux allMids (dex xyz) : {e}")

    def _is_ws_healthy(self):
        """True si le WebSocket est abonne ET a recu un tick recemment
        (moins de WS_STALE_AFTER_SEC secondes)."""
        stale_after = self.cfg.get("WS_STALE_AFTER_SEC", 20)
        return (
            self._ws_subscribed
            and self._last_ws_tick is not None
            and (time.time() - self._last_ws_tick) < stale_after
        )

    def _refresh_funding_rates_if_due(self):
        """v4.33 — Rafraichit le cache de funding rate (self.funding_rates),
        au plus une fois toutes les FUNDING_REFRESH_SEC secondes (5 min par
        defaut) — le funding ne bouge pas assez vite pour justifier un appel
        a chaque cycle de 10s, et ca evite de solliciter l API inutilement.
        Echoue silencieusement (log + conserve l ancien cache) en cas
        d erreur reseau, pour ne jamais interrompre le reste du cycle."""
        if not self.cfg.get("FUNDING_MODE_ENABLED", False):
            return
        if self.info is None:
            return
        now_ts = time.time()
        refresh_sec = self.cfg.get("FUNDING_REFRESH_SEC", 300)
        if (now_ts - self._funding_last_refresh) < refresh_sec:
            return
        try:
            meta, ctxs = self.info.meta_and_asset_ctxs()
            universe = meta.get("universe", [])
            new_rates = {}
            for asset, ctx in zip(universe, ctxs):
                name = asset.get("name")
                funding_raw = ctx.get("funding")
                if name and funding_raw is not None:
                    try:
                        new_rates[name] = float(funding_raw)
                    except (TypeError, ValueError):
                        continue
            if new_rates:
                self.funding_rates = new_rates
                self._funding_last_refresh = now_ts
        except Exception as e:
            print(f"[FUNDING] Erreur rafraichissement funding rate : {e} — cache precedent conserve.")

    def _forex_dynamic_sl(self, state, pos, price, pnl_pct, ticker, symbol, mode):
        """v4.336 — STOP LOSS DYNAMIQUE (Forex). Retourne True si la position a ete
        fermee ce cycle (ou si une fermeture est en cours de nouvelle tentative).
        Le SL ne fait que MONTER pour un long (descendre pour un short) :
          - avant armement (pic < FOREX_TTP_ARM_PCT) : rien, le SL initial reste ;
          - apres armement : plancher = entree + frais (FOREX_DSL_FEE_PAD_PCT), puis
            suiveur = pic - max(marge ATR, FOREX_DSL_TRAIL_FRACTION x pic).
        Le SL est memorise sur la position (dsl_sl, persiste au redemarrage), reflete
        dans pos["sl"] (affichage) et, en live, pousse vers le SL natif Hyperliquid."""
        cfg = self.cfg
        long_side = pos["type"] == "long"
        entry = pos["entry"]
        peak = max(pos.get("dsl_peak") or 0.0, pnl_pct)
        pos["dsl_peak"] = peak
        arm = cfg.get("FOREX_TTP_ARM_PCT", 0.15)
        if peak >= arm and price > 0:
            atr_abs, _ = calc_true_range_atr(list(state.candle_history), cfg.get("ATR_PERIOD", 14))
            if atr_abs is not None and atr_abs > 0:
                tol = atr_abs / price * 100 * cfg.get("TTP_ATR_TOLERANCE_MULTIPLIER", 1.0)
            else:
                tol = cfg.get("FOREX_DSL_FALLBACK_TOL_PCT", 0.08)
            dist = max(tol, peak * cfg.get("FOREX_DSL_TRAIL_FRACTION", 0.40))
            floor_pct = max(cfg.get("FOREX_DSL_FEE_PAD_PCT", 0.10), peak - dist)
            new_sl = entry * (1 + floor_pct / 100) if long_side else entry * (1 - floor_pct / 100)
            cur = pos.get("dsl_sl")
            if cur is None or (long_side and new_sl > cur) or ((not long_side) and new_sl < cur):
                pos["dsl_sl"] = new_sl
                if (long_side and new_sl > pos["sl"]) or ((not long_side) and new_sl < pos["sl"]):
                    pos["sl"] = new_sl
                pushed = pos.get("dsl_sl_pushed")
                step = cfg.get("FOREX_DSL_EXCHANGE_MIN_STEP_PCT", 0.03)
                if mode == "live" and self.exchange is not None and (
                        pushed is None or abs(new_sl - pushed) / entry * 100 >= step):
                    try:
                        update_sl_on_hyperliquid(self.exchange, self.info, self.cfg.get("WALLET_ADDRESS"), ticker,
                                                 {"type": pos["type"], "entry": entry,
                                                  "size": pos["size"] * pos.get("leverage", 1)},
                                                 new_sl, cfg)
                        pos["dsl_sl_pushed"] = new_sl
                    except Exception as e:
                        print(f"[DSL] Mise a jour du SL natif {ticker} impossible : {e}")
                self._save_open_positions()
        sl = pos.get("dsl_sl")
        if sl is not None and ((long_side and price <= sl) or ((not long_side) and price >= sl)):
            _result = self._safe_close_position(state, price, "STOP DYNAMIQUE (Forex)", ticker, pos, symbol, mode)
            if _result is None:
                return True   # fermeture reelle non confirmee : on reessaie au cycle suivant
            pnl, _, trade = _result
            self.emit("trade", trade)
            if pnl > 0:
                self._register_win(ticker)
            else:
                self._register_max_loss(ticker, pos.get("confidence"))
            if long_side:
                state.post_win_confirm_long = True
                state.confirm_count_long = 0
            else:
                state.post_win_confirm_short = True
                state.confirm_count_short = 0
            self.emit("log", {"msg": f"[{ticker}] ⚡ Forex STOP DYNAMIQUE @ ${price:.5g} (SL ${sl:.5g}, pic +{peak:.2f}%) | PnL: ${pnl:.2f}",
                              "level": "win" if pnl > 0 else "loss"})
            self._save_open_positions()
            self._persist_capital_snapshot()
            return True
        return False

    def _funding_pnl_track(self, state, pos, pnl_pct, touch_pct=None):
        """v4.342 — SUIVI DU CHEMIN DU PNL (LECTURE SEULE, aucun effet sur une decision).
        Sur demande : savoir, pour chaque trade Funding, par quel chemin il est passe
        (ex. : pic +0.9 %, recul a -0.1 %, puis repart au-dessus de +1 %). Note sur la
        position : pp_min = PnL minimal de tout le trade ; pp_touch_ts = 1re fois que
        le PnL atteint FUNDING_PNL_TRACK_TOUCH_PCT ; pp_min_after = PnL minimal APRES ce
        premier contact ; pp_peak_at_min = pic atteint au moment de ce minimum.
        pp_complete = le suivi couvre tout le trade (ouverture < 3 min avant le 1er tick
        suivi) ; faux pour un trade deja ouvert au deploiement (minimum partiel)."""
        try:
            now = time.time()
            if pos.get("pp_min") is None or pnl_pct < pos["pp_min"]:
                pos["pp_min"] = round(pnl_pct, 4)
            if "pp_complete" not in pos:
                try:
                    age = (datetime.now() - datetime.strptime(pos["opened_at"], "%d/%m/%Y %H:%M:%S")).total_seconds()
                except Exception:
                    age = None
                pos["pp_complete"] = bool(age is not None and age < 180)
                pos["pp_t0"] = now - (age if age is not None and age > 0 else 0)
            touch = touch_pct if touch_pct is not None else self.cfg.get("FUNDING_PNL_TRACK_TOUCH_PCT", 0.5)
            if pos.get("pp_touch_ts") is None and pnl_pct >= touch:
                pos["pp_touch_ts"] = now
            if pos.get("pp_touch_ts") is not None:
                if pos.get("pp_min_after") is None or pnl_pct < pos["pp_min_after"]:
                    pos["pp_min_after"] = round(pnl_pct, 4)
                    cur_peak = max(getattr(state, "spot_accum_peak_pnl_pct", None) or 0.0, pnl_pct)
                    pos["pp_peak_at_min"] = round(cur_peak, 4)
        except Exception:
            pass

    def _forex_crypto_eligible(self, ticker):
        """v4.331 — vrai si le mode Forex (Normal) peut trader cette CRYPTO :
        actif de la liste manuelle FOREX_CRYPTO_SYMBOLS, OU score suffisant
        (taux de reussite historique >= FOREX_CRYPTO_MIN_WIN_RATE sur au moins
        FOREX_CRYPTO_MIN_TRADES trades, tous modes hors manuel)."""
        cfg = self.cfg
        if not cfg.get("FOREX_CRYPTO_ENABLED", 1):
            return False
        if ticker in cfg.get("FOREX_MODE_SYMBOLS", []) or ticker.startswith("xyz:"):
            return False  # deja un actif forex, ou hors univers crypto
        if ticker in cfg.get("FOREX_CRYPTO_SYMBOLS", []):
            return True
        sc = getattr(self, "_asset_win_rates_all", {}).get(ticker)
        return bool(sc and sc["win_rate"] >= cfg.get("FOREX_CRYPTO_MIN_WIN_RATE", 55.0))

    def _refresh_asset_win_rates_if_due(self):
        """v4.314 — SUR DEMANDE EXPLICITE : rafraichit self._asset_win_rates
        (taux de reussite REEL par actif, calcule depuis l historique des
        trades top-down deja fermes — spot_accumulation et accumulation),
        au plus une fois toutes les ASSET_WIN_RATE_REFRESH_SEC secondes (15
        min par defaut) — c est une donnee lente (ne change significativement
        qu avec de nouveaux trades fermes), inutile de la recalculer a
        chaque cycle. Objectif : donner au score de priorisation des
        candidats un vrai "comportement plus large" par actif, pas
        seulement les donnees de l instant T — demande explicite."""
        now_ts = time.time()
        refresh_sec = self.cfg.get("ASSET_WIN_RATE_REFRESH_SEC", 900)
        if (now_ts - self._asset_win_rates_last_refresh) < refresh_sec:
            return
        try:
            trades = db.get_all_closed_trades()
            by_coin = {}
            by_coin_all = {}
            for t in trades:
                pnl = t.get("pnl")
                coin = t.get("coin")
                if pnl is None or not coin:
                    continue
                strat = t.get("strategy") or ""
                if strat != "manual":
                    by_coin_all.setdefault(coin, []).append(pnl)   # v4.331 : score du mode Forex-crypto
                if strat not in ("spot_accumulation", "accumulation"):
                    continue
                by_coin.setdefault(coin, []).append(pnl)
            min_trades = self.cfg.get("ASSET_WIN_RATE_MIN_TRADES", 5)
            new_rates = {}
            for coin, pnls in by_coin.items():
                if len(pnls) < min_trades:
                    continue  # echantillon insuffisant — reste neutre (absent du cache)
                wins = sum(1 for p in pnls if p > 0)
                new_rates[coin] = {"win_rate": round(wins / len(pnls) * 100, 1), "n": len(pnls)}
            self._asset_win_rates = new_rates
            # v4.331 — meme calcul sur tous les modes, seuil propre au mode Forex-crypto
            fx_min = self.cfg.get("FOREX_CRYPTO_MIN_TRADES", 10)
            self._asset_win_rates_all = {
                c: {"win_rate": round(sum(1 for p_ in v if p_ > 0) / len(v) * 100, 1), "n": len(v)}
                for c, v in by_coin_all.items() if len(v) >= fx_min}
            self._asset_win_rates_last_refresh = now_ts
        except Exception as e:
            print(f"[ASSET-WIN-RATE] Erreur rafraichissement : {e} — cache precedent conserve.")

    def _check_ws_health_alert(self):
        """ALARME WebSocket — appelee une fois par cycle depuis _run.
        Detecte les TRANSITIONS de sante (sain -> defaillant, defaillant ->
        retabli) et emet un log bien visible a chaque changement d etat
        (pas de spam a chaque cycle). C est ce log qui sert d alarme visible
        dans le panneau LOG EN DIRECT du dashboard (niveau 'error' = rouge).

        v3.2 — FIX : tente aussi activement une RECONNEXION toutes les 60s
        tant que le WebSocket reste defaillant (auparavant, le code se
        contentait de detecter/loguer la panne et attendait passivement une
        reconnexion automatique du SDK qui peut ne jamais survenir). Cette
        partie s execute a CHAQUE appel, contrairement a l alarme qui ne
        reagit qu aux transitions — sinon une seule tentative aurait lieu au
        moment de la panne, jamais repetee ensuite.
        """
        if self.info is None:
            return  # pas de connexion Hyperliquid du tout (paper simule sans cle)
        healthy = self._is_ws_healthy()

        # v4.7 — Une coupure WebSocket CONTINUE de plus de 5 min est traitee
        # comme un "arret" pour la fiabilite des indicateurs : au-dela de ce
        # delai, on ne peut plus faire confiance aux donnees accumulees
        # pendant la panne (le flux temps reel etant la source principale de
        # verite des prix), donc on force une collecte entierement fraiche
        # pour tous les actifs — exactement comme au redemarrage du process
        # apres un arret de plus de INDICATOR_RESUME_MAX_GAP_SEC.
        now_ts = time.time()
        if not healthy:
            if self._ws_unhealthy_since is None:
                self._ws_unhealthy_since = now_ts
                self._ws_fresh_collection_forced = False
            elif not self._ws_fresh_collection_forced:
                downtime = now_ts - self._ws_unhealthy_since
                if downtime >= self.INDICATOR_RESUME_MAX_GAP_SEC:
                    self.force_fresh_collection()
                    self._ws_fresh_collection_forced = True
                    msg = f"🔄 Coupure WebSocket de {downtime:.0f}s (> {self.INDICATOR_RESUME_MAX_GAP_SEC}s) — collecte des indicateurs relancee entierement a zero par securite."
                    self.emit("log", {"msg": msg, "level": "warn"})
                    self.emit("ws_event", {"kind": "fresh_collection_forced", "message": msg})
        else:
            self._ws_unhealthy_since = None
            self._ws_fresh_collection_forced = False

        if not healthy:
            if self._last_ws_reconnect_attempt is None or (now_ts - self._last_ws_reconnect_attempt) >= 60:
                self._last_ws_reconnect_attempt = now_ts
                try:
                    # v3.2 — FIX : un simple re-abonnement (self.info.subscribe)
                    # sur le MEME objet Info ne recree pas de connexion
                    # physique si le websocket sous-jacent est reellement mort
                    # — il peut silencieusement echouer a vie. On recree donc
                    # entierement la connexion (nouvel objet Info + Exchange,
                    # nouveau websocket), exactement comme au demarrage initial.
                    cfg = self.cfg
                    new_info, new_exchange, conn_error = connect_hyperliquid(cfg["PRIVATE_KEY"], cfg["WALLET_ADDRESS"])
                    if conn_error:
                        raise RuntimeError(conn_error)
                    self.info = new_info
                    self.exchange = new_exchange
                    self.info.subscribe({"type": "allMids"}, self._on_ws_allmids)
                    try:
                        self.info.subscribe({"type": "allMids", "dex": "xyz"}, self._on_ws_allmids_forex)
                    except Exception:
                        pass  # v4.166 — repli silencieux, deja logge au demarrage initial
                    # v4.266 — FIX BUG CRITIQUE : le flux de transactions n
                    # etait PAS reabonne apres une reconnexion — le tampon
                    # restait fige sur ses dernieres transactions et la
                    # pression calculee ne changeait plus jusqu au prochain
                    # redemarrage complet.
                    self._subscribe_trade_flow()
                    self._subscribe_candles()  # v4.284
                    self._ws_subscribed = True
                    self._last_ws_tick = time.time()  # evite un "faux mort" immediat le temps du 1er tick
                    msg = "🔄 Reconnexion WebSocket effectuee (nouvelle connexion etablie)."
                    self.emit("log", {"msg": msg, "level": "warn"})
                    self.emit("ws_event", {"kind": "reconnect_success", "message": msg})
                except Exception as e:
                    msg = f"🔄 Echec de la reconnexion WebSocket : {e} — nouvelle tentative dans 60s."
                    self.emit("log", {"msg": msg, "level": "warn"})
                    self.emit("ws_event", {"kind": "reconnect_failed", "message": msg})

        if self._ws_was_healthy is None:
            self._ws_was_healthy = healthy
            return
        if healthy == self._ws_was_healthy:
            return
        if healthy:
            msg = "✅ WebSocket retabli — surveillance temps reel des positions active"
            self.emit("log", {"msg": msg, "level": "ok"})
            self.emit("ws_event", {"kind": "restored", "message": msg})
        else:
            stale_after = self.cfg.get("WS_STALE_AFTER_SEC", 20)
            msg = f"🔴 ALARME — WebSocket hors service (aucun tick depuis {stale_after}s) — bascule sur surveillance par cycle ({self.cfg.get('CYCLE_INTERVAL')}s)"
            self.emit("log", {"msg": msg, "level": "error"})
            self.emit("ws_event", {"kind": "disconnected", "message": msg})
        self._ws_was_healthy = healthy

    def _maybe_manage_position_via_cycle(self, symbol, price, state):
        """Le WebSocket (_on_ws_allmids) est desormais la source PRINCIPALE de
        surveillance des positions ouvertes (Max Loss / SL securite / Trailing
        TP / Quick Profit) : il verifie ces seuils a CHAQUE tick de prix recu,
        bien plus reactif que le cycle. Tant que le WebSocket est actif et
        recoit des ticks recemment, le cycle NE FAIT RIEN sur les positions
        ouvertes — il se contente d afficher le prix.
        Le cycle ne reprend la main que si le WebSocket n est pas abonne, ou
        n a plus donne signe de vie depuis WS_STALE_AFTER_SEC secondes
        (deconnexion silencieuse) : filet de secours pour ne jamais laisser
        une position totalement sans surveillance active du bot.
        """
        if self._is_ws_healthy():
            return  # le websocket gere deja cette position en temps reel
        self._manage_position(symbol, price, state)

    def _get_prices_with_timeout(self, timeout_sec):
        """Recupere les prix via get_prices() avec un delai maximum.
        get_prices() fait un appel reseau bloquant vers Hyperliquid sans
        timeout expose par le SDK — en cas d accroc reseau, cet appel peut
        rester bloque tres longtemps et geler tout le cycle du bot (plus
        aucune collecte, plus aucun log — symptome observe en usage reel).
        Ici, l appel tourne dans un thread separe : si le delai est depasse,
        on ABANDONNE ce thread (il restera bloque en arriere-plan jusqu a ce
        que l appel reseau finisse par echouer/reussir de son cote — sans
        consequence puisqu on ignore son resultat) et on rend la main
        immediatement au cycle, qui reessaiera au tour suivant avec un
        thread neuf. Le bot ne se fige donc plus jamais indefiniment.
        """
        if self.info is None:
            return self._sim_prices()
        cfg = self.cfg
        result = {}
        def _worker():
            try:
                result["value"] = get_prices(self.info, cfg["SYMBOLS"], cfg)
            except Exception as e:
                result["error"] = e
        t = threading.Thread(target=_worker, daemon=True)
        t.start()
        t.join(timeout=timeout_sec)
        if t.is_alive():
            self.emit("log", {
                "msg": f"⚠ Recuperation des prix bloquee depuis plus de {timeout_sec}s (coupure reseau probable) — cycle ignore, nouvelle tentative au prochain cycle",
                "level": "error"
            })
            return {}
        if "error" in result:
            self.emit("log", {"msg": f"Erreur recuperation des prix : {result['error']}", "level": "warn"})
            return {}
        return result.get("value", {})

    def _process_with_timeout(self, sym, price):
        """Wrapper de securite autour de _process : execute le traitement
        complet d un symbole (indicateurs, filtres, CPI, entree) dans un
        thread separe avec un delai maximum (PROCESS_TIMEOUT_SEC).
        - Si ce traitement ne termine pas a temps (gel du a une cause
          quelconque — reseau, disque...), ce symbole est ignore pour ce
          cycle, avec une alarme explicite.
        - Si ce traitement leve une EXCEPTION (bug), celle-ci est desormais
          capturee et loguee en detail (message + traceback complet) au lieu
          d etre avalee silencieusement — fix v3.1 : la version precedente ne
          capturait pas les exceptions (try/finally sans except), ce qui
          masquait totalement l erreur reelle (le symbole disparaissait du
          log sans aucune trace).
        """
        import traceback
        timeout_sec = self.cfg.get("PROCESS_TIMEOUT_SEC", 12)
        done = threading.Event()
        error_holder = {}
        def _worker():
            try:
                self._process(sym, price)
            except Exception as e:
                error_holder["error"] = e
                error_holder["trace"] = traceback.format_exc()
            finally:
                done.set()
        t = threading.Thread(target=_worker, daemon=True)
        t.start()
        ticker = ticker_from_slot_key(sym)
        if not done.wait(timeout=timeout_sec):
            self.emit("log", {
                "msg": f"⚠ [{ticker}] Traitement du cycle bloque depuis {timeout_sec}s — ignore pour ce cycle, nouvelle tentative au prochain",
                "level": "error"
            })
            if sym in self.states:
                self._set_gate_blocked(self.states[sym], price, f"traitement bloque (> {timeout_sec}s)")
            return
        if "error" in error_holder:
            print(error_holder["trace"])  # traceback complet dans la console/stdout
            self.emit("log", {
                "msg": f"🔴 ERREUR [{ticker}] {type(error_holder['error']).__name__}: {error_holder['error']}",
                "level": "error"
            })
            if sym in self.states:
                self._set_gate_blocked(self.states[sym], price, f"erreur de traitement : {type(error_holder['error']).__name__}: {error_holder['error']}")

    def _run(self):
        cfg = self.cfg
        self.emit("log", {"msg": "Connexion a Hyperliquid...", "level": "info"})
        # v3.2 : la cle API + le wallet Hyperliquid sont desormais OBLIGATOIRES,
        # en mode paper COMME en mode live — plus de repli silencieux vers des
        # prix simules (_sim_prices). Le paper trading doit s appuyer sur les
        # vraies donnees de marche (prix + WebSocket), seule la passation
        # d ordres reels reste desactivee en paper (voir place_order/close_order,
        # tous deux gates par cfg["MODE"]=="live").
        if not cfg["PRIVATE_KEY"] or not cfg["WALLET_ADDRESS"]:
            self.emit("log", {
                "msg": "ERREUR : cle API et/ou wallet Hyperliquid manquants — obligatoires desormais (paper ET live). Configurez HYPERBOT_PRIVATE_KEY / HYPERBOT_WALLET_ADDRESS (ou via l API /api/config/hyperliquid) puis redemarrez.",
                "level": "error"
            })
            self.running = False
            self.emit("stopped", {})
            return

        # v4.14 — SUR DEMANDE EXPLICITE : le moteur de collecte (WS + prix +
        # indicateurs + gestion des positions ouvertes) doit tourner en
        # continu, independamment du bouton Demarrer/Arreter du trading —
        # seule une VRAIE panne doit l interrompre, avec reconnexion
        # automatique. Avant ce fix, un echec de connexion initial faisait
        # abandonner _run() DEFINITIVEMENT (self.running=False, return) sans
        # aucune tentative de reconnexion — desormais on reessaie
        # indefiniment, toutes les 30s, jusqu a ce que la connexion aboutisse.
        retry_delay = 30
        while True:
            self.info, self.exchange, conn_error = connect_hyperliquid(cfg["PRIVATE_KEY"], cfg["WALLET_ADDRESS"])
            if self.info is not None:
                break
            msg = f"Connexion Hyperliquid echouee — {conn_error} — nouvelle tentative dans {retry_delay}s."
            self.emit("log", {"msg": msg, "level": "error"})
            self.emit("ws_event", {"kind": "connect_failed", "message": msg})
            time.sleep(retry_delay)
        self.emit("log", {"msg": "Connexion etablie.", "level": "ok"})
        self.emit("ws_event", {"kind": "connected", "message": "Connexion Hyperliquid etablie avec succes."})

        # ── v3.1 : abonnement WebSocket temps reel (flux allMids) ─────────
        # Objectif : verifier Max Loss / SL securite / Trailing TP a CHAQUE
        # tick de prix recu, sans attendre le prochain cycle (CYCLE_INTERVAL).
        # v3.2 : actif desormais systematiquement en paper ET en live, puisque
        # la connexion Hyperliquid est obligatoire dans les deux modes — seul
        # le PASSAGE D ORDRE reel reste reserve au mode live (voir plus bas et
        # _on_ws_allmids -> _manage_position -> close_order).
        # Repli automatique sur la surveillance par cycle (15s) si l abonnement
        # echoue (SDK incompatible, pas de reseau, etc.) — aucune perte de
        # fonctionnalite, juste moins reactif.
        try:
            self.info.subscribe({"type": "allMids"}, self._on_ws_allmids)
            self._ws_subscribed = True
            self.emit("log", {"msg": "WebSocket temps reel actif — surveillance Max Loss/TP en direct (independante du cycle)", "level": "ok"})
        except Exception as e:
            self._ws_subscribed = False
        # v4.263 — SUR DEMANDE EXPLICITE : migration du flux de transactions
        # (pression directionnelle) du sondage REST periodique (toutes les
        # 60s, sujet a des echecs/latence) vers un VRAI abonnement
        # WebSocket temps reel — canal "trades" natif et gratuit d
        # Hyperliquid (meme connexion, deja utilisee pour allMids), un
        # abonnement distinct par actif (contrairement a allMids, ce canal
        # ne fournit pas tous les actifs en une seule souscription).
        self._subscribe_trade_flow()
        self._subscribe_candles()  # v4.284 — bougies 5 min / 1h en temps reel
        # v4.166 — SUR DEMANDE EXPLICITE : souscription SEPAREE pour le DEX
        # HIP-3 "xyz" (forex, mode Normal) — l API Hyperliquid isole les
        # DEX builder-deployes du DEX natif par defaut, un simple
        # {"type": "allMids"} SANS le champ "dex" ne retourne QUE les
        # actifs natifs (BTC, ETH, etc.), jamais EUR/JPY/KRW/DXY. Fusionne
        # dans le MEME self.all_mids (cle brute "EUR", pas de collision
        # possible avec les tickers crypto existants). Repli silencieux si
        # le SDK installe ne supporte pas encore ce parametre — le forex
        # resterait alors sans donnees, mais le reste du bot continue de
        # fonctionner normalement.
        try:
            self.info.subscribe({"type": "allMids", "dex": "xyz"}, self._on_ws_allmids_forex)
            self.emit("log", {"msg": "WebSocket forex (DEX xyz) actif — EUR/JPY/KRW/DXY en direct.", "level": "ok"})
        except Exception as e:
            self.emit("log", {"msg": f"Echec abonnement WebSocket forex (DEX xyz) : {e} — le SDK installe ne supporte peut etre pas encore ce parametre. Le mode Normal restera sans donnees tant que ce n est pas resolu.", "level": "warn"})
            self.emit("log", {"msg": f"Echec abonnement WebSocket ({e}) — repli sur surveillance par cycle ({cfg['CYCLE_INTERVAL']}s)", "level": "warn"})

        # ── Application reelle du levier configure (fix v3.1 : LEVERAGE ──
        # existait dans CONFIG mais n etait jamais envoye a Hyperliquid) ──
        if cfg["MODE"] == "live":
            leverage = cfg.get("LEVERAGE", 1)
            real_tickers_lev = list({ticker_from_slot_key(s) for s in cfg["SYMBOLS"]})
            lev_errors = []
            for t in real_tickers_lev:
                # v4.163 — SUR DEMANDE EXPLICITE : marge isolee pour le forex.
                # v4.171 — FIX : meme distinction que pour le levier par
                # trade — seul le prefixe "xyz:" (vrais marches HIP-3)
                # exige la marge isolee, pas PAXG.
                is_cross_margin_startup = not t.startswith("xyz:")
                # v4.166 — meme nom qualifie "dex:coin" que pour le passage d ordre.
                lev_ticker_startup = t
                try:
                    self.exchange.update_leverage(leverage, lev_ticker_startup, is_cross=is_cross_margin_startup)
                except Exception as e:
                    lev_errors.append(t)
                    print(f"[LEVERAGE] Echec x{leverage} sur {t} : {e}")
            if lev_errors:
                self.emit("log", {"msg": f"Levier x{leverage} : echec sur {', '.join(lev_errors)} (verifiez manuellement sur Hyperliquid).", "level": "warn"})
            else:
                self.emit("log", {"msg": f"Levier x{leverage} applique (cross margin) sur : {', '.join(real_tickers_lev)}", "level": "ok"})

        # ── Synchronisation capital réel Hyperliquid ──
        # v4.89 — SUR DEMANDE EXPLICITE : alimente desormais un pot SEPARE
        # (self.live_capital_base), plus jamais self.capital/CAPITAL_USD qui
        # restent reserves au paper — evite d ecraser le capital virtuel de
        # session avec un solde reel des qu UN SEUL mode tourne en live.
        # v4.232 — SUR DEMANDE EXPLICITE, FIX BUG CRITIQUE : cfg["MODE"]
        # est le mode GLOBAL unique — mais chaque strategie peut basculer
        # INDEPENDAMMENT en live via STRATEGY_MODE_OVERRIDE (_effective_mode).
        # Ce bloc (sync capital, reconciliation, recuperation de positions)
        # etait conditionne UNIQUEMENT sur ce mode global : si celui-ci
        # restait "paper" (ex: Forex, mode par defaut) alors qu une AUTRE
        # strategie (Spot-Accum) tournait reellement en live, tout ce bloc
        # etait saute — les positions live de CETTE strategie n etaient
        # JAMAIS recuperees au redemarrage, confirme par un cas reel (3
        # positions Spot-Accum orphelines sur Hyperliquid, jamais
        # retrouvees malgre plusieurs redeploiements). Se declenche
        # desormais si AU MOINS UNE strategie est effectivement live.
        any_strategy_live = cfg["MODE"] == "live" or any(
            self._effective_mode(s) == "live"
            for s in ("forex", "accumulation", "spot_accumulation", "funding_contrarian")
        )
        print(f"[RECOVER-DIAG] any_strategy_live={any_strategy_live} | cfg[MODE]={cfg['MODE']} | forex={self._effective_mode('forex')} | accumulation={self._effective_mode('accumulation')} | spot_accumulation={self._effective_mode('spot_accumulation')} | funding_contrarian={self._effective_mode('funding_contrarian')}")
        if any_strategy_live:
            real_balance = sync_capital_from_hyperliquid(self.info, cfg["WALLET_ADDRESS"])
            if real_balance is not None and real_balance > 0:
                self.live_capital_base = real_balance
                self.emit("log", {"msg": f"Capital LIVE synchronise depuis Hyperliquid : ${real_balance:.2f} (capital paper inchange)", "level": "ok"})
            else:
                self.emit("log", {"msg": "Sync capital LIVE echouee — capital local utilise pour le pot live.", "level": "warn"})

        # v4.264 — REPRISE UNIFIEE DES POSITIONS (live ET paper, tous modes,
        # tous DEX) : voir _restore_positions_at_startup.
        self._restore_positions_at_startup(any_strategy_live)

        self._load_confidence_thresholds()
        # v3.2 — REACTIVE : la collecte des indicateurs (notamment l EMA
        # intermediaire, desormais calibree sur une vraie fenetre de 25-50
        # min) est trop longue pour repartir de zero a chaque redemarrage
        # anodin (redeploiement, coupure breve). Reprise rapide reactivee
        # avec un seuil elargi a 10 minutes cumulees (voir
        # INDICATOR_RESUME_MAX_GAP_SEC) : au-dela, collecte fraiche par
        # securite (un trou de donnees trop long fausserait les indicateurs).
        self._load_indicator_state_if_recent()
        self._load_indicator_history()  # v4.22 — sans condition de delai
        # (session de trading 24h retiree — voir jour calendaire UTC fixe,
        # gere cote API pour les statistiques uniquement, sans blocage)

        symbols_display = ", ".join(self._original_symbols)
        self.emit("log", {"msg": f"Demarrage | {symbols_display} | ${cfg['CAPITAL_USD']}", "level": "ok"})
        # v4.169 — SUR DEMANDE EXPLICITE : liste TOUS les DEX HIP-3
        # disponibles au demarrage (diagnostic ponctuel) — le DEX "xyz" s
        # est avere ne contenir QUE des actions/matieres premieres, jamais
        # de forex malgre la documentation initiale consultee. Cherche
        # directement le bon nom de DEX plutot que de continuer a deviner.
        if self.info is not None:
            dexs = None
            for attempt_name, attempt_fn in [
                ("info.perp_dexs()", lambda: self.info.perp_dexs()),
                ("info.post /info type=perpDexs", lambda: self.info.post("/info", {"type": "perpDexs"})),
                ("requete brute via session HTTP interne", lambda: self.info.session.post(f"{self.info.base_url}/info", json={"type": "perpDexs"}).json()),
            ]:
                try:
                    dexs = attempt_fn()
                    print(f"[PERPDEXS-DIAG] Succes via {attempt_name} : {dexs}")
                    break
                except Exception as e:
                    print(f"[PERPDEXS-DIAG] Echec via {attempt_name} : {e}")
            if dexs is None:
                print("[PERPDEXS-DIAG] Aucune methode n a fonctionne pour lister les DEX HIP-3.")
        self.emit("log", {"msg": f"Plage horaire : {cfg['TRADE_HOUR_START']}h-{cfg['TRADE_HOUR_END']}h Paris", "level": "info"})

        # v3.2 — signale la fin complete de l initialisation (reconciliation
        # Hyperliquid en live deja faite, restauration paper deja faite) :
        # permet a l API web de lancer un nettoyage automatique des
        # signaux/trades orphelins juste apres, en connaissant avec
        # certitude l etat REEL des positions a cet instant precis.
        self.emit("startup_ready", {})

        while self.running:
            try:
                self.cycle += 1
                self._check_ws_health_alert()
                self._refresh_funding_rates_if_due()  # v4.33
                self._refresh_asset_win_rates_if_due()  # v4.314
                self._blocked_followups_if_due()  # v4.338
                self._refresh_held_mtf_data()  # v4.341
                self._flow_recorder_flush_if_due()  # v4.349
                try:                                  # v4.353 — scalp Forex : jamais bloquant pour le reste du cycle
                    self._scalp_cycle()
                    self._scalp_journal_eval_if_due()
                    self._fxswing_cycle()               # v4.361
                    self._swing_journal_eval_if_due()   # v4.359
                    self._supsw_cycle()                 # v4.362
                    self._supsw_eval_if_due()           # v4.362
                    self._trendf_cycle()                # v4.365
                    self._trendf_eval_if_due()          # v4.365
                except Exception as _e_sc:
                    print(f"[SCALP] cycle ignore : {type(_e_sc).__name__}: {_e_sc}")
                self._decay_confidence_thresholds()
                prices = self._get_prices_with_timeout(cfg.get("PRICE_FETCH_TIMEOUT_SEC", 10))
                in_hours = is_trading_hours(cfg)

                # v3.2 — DIAGNOSTIC : verifie explicitement combien de prix (sur
                # les 30 configures) sont reellement recuperes a chaque cycle, et
                # lesquels manquent — permet de confirmer/infirmer un probleme de
                # resolution de prix pour une partie des actifs (sans quoi ceux-la
                # ne collectent jamais, silencieusement, indefiniment).
                if self.cycle % 4 == 1:  # une fois toutes les ~4 cycles (~1 min), pas a chaque cycle
                    missing_syms = [ticker_from_slot_key(s) for s in cfg["SYMBOLS"] if s not in prices]
                    self.emit("log", {
                        "msg": f"🔍 DIAGNOSTIC cycle {self.cycle} : {len(prices)}/{len(cfg['SYMBOLS'])} prix recuperes | in_hours={in_hours} | manquants: {missing_syms if missing_syms else 'aucun'}",
                        "level": "warn"
                    })

                if not in_hours:
                    self.emit("log", {"msg": f"Hors plage {cfg['TRADE_HOUR_START']}h-{cfg['TRADE_HOUR_END']}h — nouvelles entrees suspendues (positions actives conservees)", "level": "dim"})
                    for sym in cfg["SYMBOLS"]:
                        if sym in prices:
                            self.states[sym].current_price = prices[sym]
                    # Continuer a gerer les positions ouvertes (SL / Trailing TP)
                    forex_syms_fallback_oh = set(cfg.get("FOREX_MODE_SYMBOLS", []))
                    for sym in cfg["SYMBOLS"]:
                        if sym in prices and self.states[sym].position:
                            self._process_with_timeout(sym, prices[sym])
                        elif (sym not in prices and ticker_from_slot_key(sym) in forex_syms_fallback_oh
                              and self.states[sym].position and isinstance(self.all_mids, dict)):
                            fb_price = self.all_mids.get(ticker_from_slot_key(sym))
                            if fb_price is not None:
                                try:
                                    fb_price = float(fb_price)
                                    if fb_price > 0:
                                        self._process_with_timeout(sym, fb_price)
                                except (TypeError, ValueError):
                                    pass
                    self._save_open_positions()
                    self._save_confidence_thresholds()
                    self._save_indicator_state()
                    self._send_snapshot()
                    time.sleep(cfg["CYCLE_INTERVAL"])
                    continue

                if not prices:
                    self.emit("log", {"msg": "Prix indisponibles", "level": "warn"})
                    time.sleep(cfg["CYCLE_INTERVAL"])
                    continue

                self._pending_candidates = []
                self._pending_accumulation_candidates = []
                self._pending_funding_candidates = []
                self._pending_spot_accum_candidates = []
                # v4.198 — SUR DEMANDE EXPLICITE : filet de secours pour le
                # forex — si le ticker est absent du dict REST (prices,
                # get_prices()) mais present dans self.all_mids (alimente
                # separement par le WebSocket dedie au DEX "xyz", qui peut
                # fonctionner meme quand le REST echoue) — utilise cette
                # valeur plutot que de sauter completement le traitement de
                # ce cycle. Confirme par un cas reel : WS recevait bien les
                # prix forex, mais prices (REST) ne les contenait jamais,
                # empechant _process d etre appele du tout pour ces tickers.
                forex_syms_fallback = set(cfg.get("FOREX_MODE_SYMBOLS", []))
                for sym in cfg["SYMBOLS"]:
                    if sym in prices:
                        self._process_with_timeout(sym, prices[sym])
                        continue
                    processed = False
                    if ticker_from_slot_key(sym) in forex_syms_fallback and isinstance(self.all_mids, dict):
                        fallback_price = self.all_mids.get(ticker_from_slot_key(sym))
                        if fallback_price is not None:
                            try:
                                fallback_price = float(fallback_price)
                                if fallback_price > 0:
                                    self._process_with_timeout(sym, fallback_price)
                                    processed = True
                            except (TypeError, ValueError):
                                pass
                    if not processed and sym in self.states:
                        # v4.264 — rend visible dans le diagnostic un actif
                        # jamais traite faute de prix (cas typique : DEX xyz).
                        dex_note = f" (DEX {_dex_of(ticker_from_slot_key(sym))})" if _dex_of(ticker_from_slot_key(sym)) else ""
                        self._set_gate_blocked(self.states[sym], None, f"prix indisponible{dex_note} — actif non analyse ce cycle")
                self._finalize_pending_candidates()
                self._finalize_pending_accumulation_candidates()
                self._finalize_pending_funding_candidates()
                self._finalize_pending_spot_accum_candidates()
                self._maybe_start_trade_followups()  # v4.265 — hors du fil de trading
                self._maybe_start_live_sync()  # v4.292 — controle bot <-> Hyperliquid (fil separe)
                if getattr(self, "manual", None) is not None:
                    try:
                        self.manual.on_cycle()  # v4.273 — expirations + secours si WebSocket muet
                    except Exception as e_man:
                        print(f"[MANUEL] Erreur cycle : {e_man}")

                self._save_open_positions()
                self._save_confidence_thresholds()
                self._save_indicator_state()
                self._send_snapshot()
                time.sleep(cfg["CYCLE_INTERVAL"])
            except Exception as e:
                # v3.2 — FILET DE SECURITE CRITIQUE : sans ce try/except, une
                # exception inattendue (ex: le bug "timezone" du 05/07/2026)
                # tuait le thread _run() COMPLETEMENT et SILENCIEUSEMENT — le
                # bot semblait actif (WebSocket + positions geres normalement)
                # mais plus aucun cycle, plus aucune collecte, plus aucune
                # nouvelle entree, sans la moindre trace visible dans l app.
                # Desormais : erreur loguee bien visible + le cycle suivant
                # continue normalement au lieu de mourir.
                import traceback
                err_msg = f"🔴 ERREUR CRITIQUE dans le cycle {self.cycle} : {type(e).__name__}: {e}"
                print(f"[_run] {err_msg}")
                print(traceback.format_exc())
                self.emit("log", {"msg": err_msg + " — le cycle suivant va reprendre normalement.", "level": "error"})
                time.sleep(cfg.get("CYCLE_INTERVAL", 10))

        self.emit("stopped", {})

    def _manage_position(self, symbol, price, state):
        """Wrapper thread-safe : _manage_position_impl peut etre appelee soit
        depuis le thread du cycle (_run), soit depuis le thread WebSocket
        (_on_ws_allmids). Le lock evite qu une meme position soit traitee/
        fermee deux fois en parallele (ex: MAX LOSS declenche par les deux
        threads presque simultanement)."""
        with self.lock:
            self._manage_position_impl(symbol, price, state)

    def _safe_close_position(self, state, price, reason, ticker, pos, symbol, mode):
        """v4.259 — SUR DEMANDE EXPLICITE, FIX BUG CRITIQUE SYSTEMIQUE :
        remplace le pattern repete 23 fois dans ce fichier ou l ordre de
        fermeture REEL (close_order) etait envoye APRES que le bot ait
        deja marque la position comme fermee en interne (state.
        close_position), et SURTOUT sans jamais verifier son succes —
        confirme par un cas reel GRAVE : des positions LIVE fermees cote
        bot mais RESTEES OUVERTES sur Hyperliquid, totalement hors de
        toute surveillance. Cette fonction INVERSE l ordre : en mode live,
        tente D ABORD l ordre reel, et n enregistre la fermeture cote bot
        QUE s il a reussi (ou si on est en paper, ou aucun ordre reel n
        est necessaire). En cas d echec reel, ne touche PAS a l etat
        interne — la position reste \"ouverte\" cote bot, geree normalement
        au prochain cycle (nouvelle tentative naturelle), au lieu d un
        desaccord silencieux avec la realite. Retourne (pnl, _, trade) en
        cas de succes, ou None en cas d echec (l appelant doit alors
        return immediatement, sans toucher au reste de son etat)."""
        # v4.264 — le mode REEL est celui dans lequel la position a ete
        # OUVERTE (memorise sur la position), pas le reglage courant de la
        # strategie : une position ouverte en live puis la strategie
        # rebasculee en paper etait fermee en interne SANS ordre reel.
        mode = self._position_mode(pos) if pos else mode
        exit_source = "simulation" if mode != "live" else "bot"
        if mode == "live" and self.exchange:
            close_ok = close_order(self.exchange, ticker, pos, self.cfg)
            if not close_ok:
                self.emit("log", {"msg": f"[{ticker}] ⚠️⚠️ ALERTE CRITIQUE : ordre de fermeture REJETE par Hyperliquid — position laissee OUVERTE cote bot egalement (nouvelle tentative au prochain cycle), pour rester coherent avec la realite. Verification manuelle recommandee si ceci persiste.", "level": "error"})
                print(f"[CLOSE-ORDER-FAIL] {ticker} : ordre de fermeture reel a echoue — fermeture cote bot ANNULEE, reste synchronise avec Hyperliquid.")
                return None
            # v4.265 — PnL calcule sur le VRAI prix d execution Hyperliquid
            # quand il est connu (meme logique que l entree depuis v4.226).
            real_exit = pop_last_close_fill(ticker)
            if real_exit:
                if abs(real_exit - price) / price > 0.0001:
                    print(f"[EXIT-FILL] {ticker} : prix vise {price:.6g} -> execute {real_exit:.6g} (glissement {((real_exit - price) / price * 100):+.3f}%)")
                price = real_exit
                exit_source = "hyperliquid"
        if pos and pos.get("sl_patience_used") and not reason.startswith("STOP LOSS ("):
            reason = f"{reason} · apres patience SL"  # v4.270 — trade sauve (ou non) par la patience
        pnl, _, trade = state.close_position(price, reason)
        trade["symbol"] = symbol
        trade["exit_price_source"] = exit_source
        # v4.310 — SUR DEMANDE EXPLICITE : pour un trade top-down, enregistre
        # si la tendance H4 D ORIGINE (celle qui justifiait l entree) est
        # ENCORE intacte au moment precis de la fermeture — independant des
        # etats internes du bot (mtf_be_done, tp_stage, etc.), recalcule a
        # neuf depuis les vraies bougies H4 en cache. Objectif : distinguer
        # objectivement un simple repli dans une tendance encore valide
        # (tendance toujours dans le sens du trade a la sortie) d un vrai
        # changement de regime (tendance deja repassee neutre/opposee) —
        # question posee explicitement sur une serie de pertes SL/structure
        # cassee. cache_only=True : ne bloque jamais la fermeture sur un
        # appel reseau, utilise les bougies H4 deja en cache (rafraichies a
        # chaque cloture de bougie par le cycle normal).
        if pos and pos.get("engine") == "mtf":
            try:
                mv = self.mtf_view(ticker, price, cache_only=True)
                # v4.341 : donnees H4 perimees => None (inconnu) plutot qu une valeur fausse
                if mv and mv.get("ok") and mv.get("data_fresh", True):
                    want = "haussiere" if pos["type"] == "long" else "baissiere"
                    trade["mtf_trend_at_close"] = mv["trend"]
                    trade["mtf_trend_intact_at_close"] = (mv["trend"] == want)
                else:
                    trade["mtf_trend_at_close"] = None
                    trade["mtf_trend_intact_at_close"] = None
            except Exception as e:
                print(f"[MTF-TREND-CLOSE] {ticker} : verification impossible : {e}")
                trade["mtf_trend_at_close"] = None
                trade["mtf_trend_intact_at_close"] = None
        return pnl, _, trade

    def _manage_position_impl(self, symbol, price, state):
        """v4.10 — Moteur de risque ASYMETRIQUE, sur demande explicite :
        - Le SL reste en % de E (perte $ PLAFONNEE, independante du levier) :
          E=20$, SL=1% -> perte de 0.20$ a x1 COMME a x3. Ce qui change avec
          le levier, c est UNIQUEMENT la distance de prix necessaire pour
          l atteindre (1% a x1, 0.33% a x3 — de plus en plus proche a mesure
          que le levier monte), jamais le montant $ perdu.
        - Le TP reste en % de MOUVEMENT DE PRIX REEL (inchange) : le levier
          y amplifie librement le gain $, sans plafond.
        1) Stop Loss (-SL_PCT_OF_E % de E, defaut -1.0%) : sortie immediate
           geree par le bot, perte $ plafonnee quel que soit le levier.
        2) SL Hyperliquid : filet de securite uniquement (cas ou le bot
           serait en retard/deconnecte) — ne devrait quasiment jamais se
           declencher avant le Stop Loss ci-dessus en usage normal.
        3) Trailing Take Profit (TTP), en % de MOUVEMENT DE PRIX REEL :
           - Arme des que le PRIX bouge de TTP_ARM1_PRICE_PCT (defaut 1.2%)
             dans le sens du trade. Seuil de sortie fixe a
             TTP_LOCK1_PRICE_PCT (defaut 1.0%) tant que le PIC de mouvement
             n a pas rejoint TTP_ARM2_PRICE_PCT.
           - Des que le pic de mouvement atteint TTP_ARM2_PRICE_PCT (defaut
             1.5%), le seuil de sortie devient pic - TTP_TRAIL_GAP_PRICE_PCT
             (defaut 0.3%) et continue de suivre le pic a l infini (trailing
             pur, sans plafond) — capture le maximum atteint des que le
             profit cesse de progresser.
        """
        cfg    = self.cfg
        ticker = ticker_from_slot_key(symbol)
        pos    = state.position
        if not pos:
            return
        # v4.87 — SUR DEMANDE EXPLICITE : chaque mode peut desormais
        # basculer independamment entre paper et live (voir
        # _effective_mode). Remplace la lecture directe de cfg["MODE"] par
        # la resolution par-strategie — aucun changement de comportement
        # pour un mode qui n a pas ete personnalise (retombe sur le mode
        # global, exactement comme avant).
        mode = self._position_mode(pos)  # v4.264 — mode fige a l ouverture
        if pos.get("engine") == "mtf":   # v4.299 — position top-down : gestion dediee
            return self._manage_mtf(symbol, price, state, pos, ticker_from_slot_key(symbol), mode)
        if pos.get("engine") == "scalp":   # v4.353 — position de scalp Forex : gestion dediee (paper)
            return self._manage_scalp(symbol, price, state, pos, ticker)
        if pos.get("engine") == "fxswing":   # v4.361 — position de Swing Forex : gestion dediee (paper)
            return self._manage_fxswing(symbol, price, state, pos, ticker)
        if pos.get("engine") == "supsw":     # v4.362 — position de Swing Support : gestion dediee (paper)
            return self._manage_supsw(symbol, price, state, pos, ticker)
        if pos.get("engine") == "trendf":    # v4.365 — position du module Tendance : gestion dediee (paper)
            return self._manage_trendf(symbol, price, state, pos, ticker)
        # v4.33 — SECURITE EXPLICITE : un trade "funding_contrarian" reste
        # simule (paper) meme si le bot tourne globalement en mode live, tant
        # que FUNDING_MODE_LIVE_ALLOWED n est pas active manuellement — ce
        # mode est experimental, protege un capital deja fragilise pendant
        # sa phase de validation. Ne change RIEN pour les autres strategies.
        # Ce garde-fou reste actif MEME si STRATEGY_MODE_OVERRIDE force
        # "live" pour funding_contrarian — double protection, jamais retiree.
        if pos.get("strategy") == "funding_contrarian" and not cfg.get("FUNDING_MODE_LIVE_ALLOWED", False):
            mode = "paper"

        # E = taille de l entree (avant levier) — base du SL (v4.10, perte $
        # plafonnee) ; le TP, lui, raisonne en % de mouvement de prix pur.
        E = pos["size"]
        strat_tag = "🎯 " if pos.get("strategy") == "accumulation" else ""  # v4.8 — visible dans les logs de sortie

        # ── PnL latent en $ ───────────────────────────────────────────────
        # v4.231 — SUR DEMANDE EXPLICITE, ROLLBACK URGENT DE v4.229 : le
        # passage a un pnl_pct amplifie par le levier a eu un effet
        # RETROACTIF DANGEREUX sur les positions DEJA ouvertes au moment du
        # redeploiement — une position avec une petite perte BRUTE (ex:
        # -0.15%, jugee sure) et un levier x5 devenait instantanement
        # -0.75% AMPLIFIE, depassant le plafond de securite (-0.5%) et
        # declenchant une fermeture IMMEDIATE sur le cote BOT (mais pas
        # toujours proprement repercutee cote Hyperliquid), confirme par un
        # cas reel (toutes les positions fermees cote bot au redeploiement,
        # pas cote Hyperliquid). RETOUR au mouvement de prix BRUT — la
        # securite des positions existantes prime sur la coherence
        # d affichage, qui devra etre traitee UNIQUEMENT au niveau de l
        # affichage (api.py), jamais dans les comparaisons de seuils
        # internes qui peuvent affecter des positions deja en cours.
        leverage_now = pos.get("leverage", 1) or 1
        if pos["type"] == "long":
            pnl_pct = (price - pos["entry"]) / pos["entry"] * 100
        else:
            pnl_pct = (pos["entry"] - price) / pos["entry"] * 100
        pnl_usd = E * leverage_now * pnl_pct / 100

        # v4.179 — SUR DEMANDE EXPLICITE : plafond de duree maximale (12h)
        # pour tous les modes SAUF Spot-Accum (philosophie explicitement
        # patiente, "tenir tant que l actif existe") — ferme uniquement si
        # le PnL est POSITIF OU NEUTRE a ce moment (jamais force une perte,
        # une position encore perdante apres 12h continue normalement,
        # geree par les mecanismes de sortie habituels).
        if pos.get("strategy") != "spot_accumulation" and cfg.get("MAX_HOLD_DURATION_ENABLED", True):
            try:
                opened_dt = datetime.strptime(pos["opened_at"], "%d/%m/%Y %H:%M:%S")
                hours_open = (datetime.now() - opened_dt).total_seconds() / 3600
            except (ValueError, KeyError, TypeError):
                hours_open = 0
            max_hold_hours = cfg.get("MAX_HOLD_DURATION_HOURS", 12)
            if hours_open >= max_hold_hours and pnl_usd >= 0:
                _result = self._safe_close_position(state, price, "DUREE MAX ATTEINTE", ticker, pos, symbol, mode)

                if _result is None:

                    return

                pnl, _, trade = _result
                self.emit("trade", trade)
                self.emit("log", {"msg": f"[{ticker}] {strat_tag}⏱️ DUREE MAX ATTEINTE ({hours_open:.1f}h >= {max_hold_hours}h, PnL neutre/positif) @ ${price:.2f} | PnL: ${pnl:.2f}", "level": "win" if pnl > 0 else "dim"})
                self._save_open_positions()
                self._persist_capital_snapshot()
                return

        # v4.18 — Suivi du pic ABSOLU, des le premier cycle en profit, quel
        # que soit le seuil de trailing atteint (ou pas) — purement
        # informatif, n influence aucune decision de sortie ci-dessous.
        if pnl_usd > 0 and (state.absolute_peak_pnl_usd is None or pnl_usd > state.absolute_peak_pnl_usd):
            state.absolute_peak_pnl_usd = pnl_usd

        # v4.64 — SUR DEMANDE EXPLICITE : sortie sur RETOURNEMENT DE TENDANCE
        # CONFIRME pour Accumulation — independante du SL, peut se declencher
        # BIEN AVANT que le SL ne soit touche. Fonctionne dans LES DEUX SENS
        # (LONG : sort si le prix repasse durablement SOUS l EMA200 ; SHORT :
        # sort si le prix repasse durablement AU-DESSUS). Ne fait RIEN
        # d autre : si aucun retournement n est confirme, la fonction
        # continue normalement vers la gestion SL/TTP habituelle plus bas —
        # ce bloc n intervient que pour fermer plus tot, jamais pour bloquer
        # les autres mecanismes de sortie.
        if pos.get("strategy") == "accumulation" and cfg.get("ACCUMULATION_REVERSAL_EXIT_ENABLED", True):
            min_maturity = cfg.get("ACCUMULATION_REVERSAL_MIN_EMA_MATURITY", 100)
            data_mature = len(state.mtf_prices) >= min_maturity
            data_healthy = self._is_ws_healthy() if self.info is not None else True
            ema200_now = self._trend_ema(state)

            if not data_mature or not data_healthy or ema200_now is None:
                reason_skip = "EMA200 pas assez mature" if not data_mature else ("collecte instable" if not data_healthy else "EMA200 indisponible")
                if state.accumulation_reversal_count > 0:
                    self.emit("log", {"msg": f"[{ticker}] 🎯 Retournement en cours d'evaluation suspendu ({reason_skip}) — compteur conserve a {state.accumulation_reversal_count}", "level": "dim"})
            else:
                is_long = pos.get("type") == "long"
                reversed_now = (price < ema200_now) if is_long else (price > ema200_now)
                if reversed_now:
                    state.accumulation_reversal_count += 1
                else:
                    state.accumulation_reversal_count = 0

            confirm_needed = cfg.get("ACCUMULATION_REVERSAL_CONFIRM_CYCLES", 180)
            if state.accumulation_reversal_count >= confirm_needed:
                # v4.162 — SUR DEMANDE EXPLICITE : ajoute la meme protection
                # patience+couleur de bougie que SL/TTP — meme avec le
                # compteur de cycles deja requis ci-dessus, ce mecanisme
                # s est avere le plus mauvais du bot (12% de reussite sur
                # 26 trades reels observes). Exige desormais AUSSI cette
                # confirmation supplementaire avant de fermer reellement.
                if not self._candle_color_confirms_reversal(state, pos.get("type", "long")):
                    self._save_open_positions()
                    return
                # v4.264 — ordre reel D ABORD, fermeture cote bot seulement si confirmee
                _result = self._safe_close_position(state, price, "RETOURNEMENT CONFIRME", ticker, pos, symbol, mode)
                if _result is None:
                    return
                pnl, _, trade = _result
                self.emit("trade", trade)
                if pnl > 0:
                    self._register_win(ticker)
                else:
                    self._register_max_loss(ticker, pos.get("confidence"))
                self.emit("log", {"msg": f"[{ticker}] 🎯 Accumulation RETOURNEMENT CONFIRME (tendance opposee depuis {confirm_needed} cycles, donnees matures et saines) @ ${price:.2f} | PnL: ${pnl:.2f}", "level": "warn"})
                state.accumulation_reversal_count = 0
                self._save_open_positions()
                return

        # v4.212 — SUR DEMANDE EXPLICITE : Accumulation utilise desormais
        # EXACTEMENT le meme mecanisme de sortie dedie que Spot-Accum
        # (SL structurel, TTP arme/tolerance uniques, retournement confirme)
        # — plus AUCUNE trace de son ancien systeme (tier0/tier1 partage
        # avec Forex). Generalise pour fonctionner en LONG (Spot-Accum) et
        # SHORT (Accumulation) via pos["type"], au lieu du "long" fige en
        # dur d origine.
        # v4.234 — SUR DEMANDE EXPLICITE : generalise le mecanisme de
        # sortie dynamique (SL structurel, TTP arme/tolerance ATR-relative,
        # filet inconditionnel, vitesse de repli, retournement confirme) a
        # TOUS les modes bases sur le support/resistance — Forex,
        # Accumulation, Spot-Accum, et Funding (tente, la nature mean-
        # reversion de ce dernier pourrait s averer moins adaptee a l usage,
        # a observer). Resout AUSSI la crise de classification recente : la
        # strategie exacte d une position (mal etiquetee "forex" au lieu de
        # "spot_accumulation" apres une recuperation orpheline) n a plus
        # d impact sur la LOGIQUE de sortie, puisque tous les modes
        # partagent desormais ce meme mecanisme.
        # v4.249 — SUR DEMANDE EXPLICITE : Funding retire de ce mecanisme
        # unifie (etait ajoute en v4.234) — reconsideration honnete : ce
        # mecanisme est pense pour du SUIVI DE TENDANCE (laisser courir
        # tant que la bougie confirme), philosophiquement incompatible
        # avec Funding, qui parie sur un retour RAPIDE et BREF a la
        # moyenne apres un exces de taux de financement — voir le nouveau
        # mecanisme dedie plus bas (_manage_funding_position).
        if pos.get("strategy") in ("spot_accumulation", "accumulation", "forex"):
            # v4.212 — label dynamique pour les messages, correct pour les
            # deux modes partageant desormais ce meme bloc de sortie.
            _label_map_sa = {
                "spot_accumulation": "🌱 Spot-Accum",
                "accumulation": "🎯 Accumulation",
                "forex": "⚡ Forex",
                "funding_contrarian": "💰 Funding",
            }
            mode_label_sa = _label_map_sa.get(pos.get("strategy"), "🎯 Accumulation")

            # v4.336 — SUR DEMANDE EXPLICITE : SL dynamique Forex (voir _forex_dynamic_sl)
            _forex_dsl_on = pos.get("strategy") == "forex" and bool(cfg.get("FOREX_DYNAMIC_SL", 1))
            if pos.get("strategy") == "forex":      # v4.388 : suivi du chemin du PnL (lecture seule) ; seuil = armement du trailing Forex
                self._funding_pnl_track(state, pos, pnl_pct, touch_pct=cfg.get("FOREX_TTP_ARM_PCT", 0.15))
            # v4.399 : SANS PROGRES (Forex). Export 15/10 : 48 trades restes > 2 h sans jamais atteindre +0.15 % de pic
            # finissent a -0.21 % en moyenne (-3.5 $ bruts). On les coupe apres N minutes si le pic reste sous le seuil.
            if pos.get("strategy") == "forex" and cfg.get("FOREX_STALE_ENABLED", 1):
                _pk = max(pos.get("dsl_peak") or 0.0, pnl_pct)
                pos["dsl_peak"] = _pk
                if _pk < cfg.get("FOREX_STALE_PEAK_PCT", 0.15):
                    try:
                        _age = (datetime.now() - datetime.strptime(pos.get("opened_at", ""), "%d/%m/%Y %H:%M:%S")).total_seconds() / 60.0
                    except (ValueError, TypeError):
                        _age = 0.0
                    if _age >= cfg.get("FOREX_STALE_MIN", 90):
                        _result = self._safe_close_position(state, price, "FOREX SANS PROGRES", ticker, pos, symbol, mode)
                        if _result is None:
                            return
                        pnl, _, trade = _result
                        self.emit("trade", trade)
                        if pnl > 0:
                            self._register_win(ticker)
                        self.emit("log", {"msg": f"[{ticker}] ⚡ Forex : sans progres apres {_age:.0f} min (pic +{_pk:.2f}% sous +{cfg.get('FOREX_STALE_PEAK_PCT', 0.15)}%) @ ${price:.4f} | PnL: ${pnl:.2f}", "level": "win" if pnl > 0 else "loss"})
                        self._save_open_positions()
                        self._persist_capital_snapshot()
                        return
            if _forex_dsl_on and self._forex_dynamic_sl(state, pos, price, pnl_pct, ticker, symbol, mode):
                return

            # v4.227 — SUR DEMANDE EXPLICITE : etoile filante ROUGE
            # confirmee sur 30 min — signal de sortie pour une position
            # LONG existante (Spot-Accum). Verifiee tot dans le bloc,
            # independamment du SL/TTP normal — un signal technique fort
            # justifie une sortie proactive, pas seulement reactive.
            if pos["type"] == "long" and cfg.get("SHOOTING_STAR_DETECTION_ENABLED", True) and self._shooting_star_confirmed(state):
                _result = self._safe_close_position(state, price, "ETOILE FILANTE CONFIRMEE", ticker, pos, symbol, mode)

                if _result is None:

                    return

                pnl, _, trade = _result
                self.emit("trade", trade)
                if pnl > 0:
                    self._register_win(ticker)
                state.post_win_confirm_long = True
                state.confirm_count_long = 0
                self.emit("log", {"msg": f"[{ticker}] {mode_label_sa} ETOILE FILANTE confirmee (30 min) @ ${price:.2f} | PnL: ${pnl:.2f}", "level": "win" if pnl > 0 else "loss"})
                self._save_open_positions()
                self._persist_capital_snapshot()
                return

            # v4.238 — SUR DEMANDE EXPLICITE, FIX BUG CRITIQUE : si les DEUX
            # seuils (0.5% structurel ET 5% plafond dur) sont deja depasses
            # au MEME cycle (ex: prix ayant fortement chute pendant que le
            # bot etait momentanement moins reactif), le plafond DUR (5%,
            # verifie plus bas dans le code jusqu ici) l emportait
            # TOUJOURS sur le seuil plus protecteur (0.5%), qui n avait
            # alors JAMAIS sa chance — confirme par un cas reel (ARB fermee
            # a -5.05% via ce plafond, alors que le seuil de 0.5% aurait du
            # proteger bien avant). Verifie desormais le seuil le PLUS
            # PROTECTEUR en priorite absolue, quel que soit son
            # emplacement dans le reste du code.
            # v4.275 — FIX : ce bloc gere Spot-Accum, Accumulation ET Forex. Le
            # plafond "Spot-Accum" s appliquait donc aux trois, et celui
            # d Accumulation (ACCUMULATION_SL_CAP_PCT) n etait jamais lu.
            _strat_cap = pos.get("strategy")
            _cap_key = {"spot_accumulation": "SPOT_ACCUM_SL_CAP_PCT", "accumulation": "ACCUMULATION_SL_CAP_PCT"}.get(_strat_cap)
            immediate_cap_pct = (cfg.get(_cap_key) if _cap_key else None) or cfg.get("STRUCTURAL_SL_HARD_CAP_PCT", 0.5)
            # v4.270 — SUR DEMANDE EXPLICITE : PATIENCE DU SL PILOTEE PAR LE FLUX.
            # Quand le plafond est atteint, si le flux de transactions reste
            # FAVORABLE a la position (acheteurs dominants pour un long), le
            # bot attend au lieu de couper — dans la limite d une perte
            # maximale et d une duree maximale. Donnees 18-23/09 : apres 71 %
            # des SL Spot-Accum, le prix est revenu a l entree dans l heure.
            sl_reason = "STOP LOSS (plafond immediat)"
            if pnl_pct > -immediate_cap_pct:
                state.sl_flow_patience_since = None  # repasse au-dessus du plafond : patience terminee
            elif (pos.get("strategy") == "spot_accumulation"  # v4.275 — patience reservee a Spot-Accum (demandee pour ce mode)
                  and cfg.get("SPOT_ACCUM_SL_FLOW_PATIENCE_ENABLED", True) and (cfg.get("SPOT_ACCUM_SL_FLOW_MAX_WAIT_SEC", 900) or 0) > 0):
                max_loss_pct = cfg.get("SPOT_ACCUM_SL_FLOW_MAX_PCT", 1.0)
                max_wait = cfg.get("SPOT_ACCUM_SL_FLOW_MAX_WAIT_SEC", 900)
                since = getattr(state, "sl_flow_patience_since", None)
                flow_sl = self._compute_trade_flow_pressure(ticker, price_now=price)
                thr_sl = cfg.get("SPOT_ACCUM_SL_FLOW_THRESHOLD", 0.2)
                favorable_sl = flow_sl is not None and (flow_sl >= thr_sl if pos["type"] == "long" else flow_sl <= -thr_sl)
                # v4.271 — SUR DEMANDE EXPLICITE : la patience exige AUSSI que
                # la tendance de fond tienne (meme regle que le maintien de
                # tendance du trailing : prix du bon cote de l EMA200). Un
                # flux acheteur ponctuel pendant une vraie cassure de
                # tendance ne justifie pas d attendre. EMA200 indisponible
                # -> benefice du doute, comme pour le trailing (v4.214).
                trend_ok_sl = True
                ema200_sl = None
                if cfg.get("SPOT_ACCUM_SL_PATIENCE_REQUIRE_TREND", 1):
                    ema200_sl = self._trend_ema(state)
                    if ema200_sl is not None:
                        trend_ok_sl = (price > ema200_sl) if pos["type"] == "long" else (price < ema200_sl)
                if pnl_pct <= -max_loss_pct:
                    sl_reason = "STOP LOSS (plafond patience)" if since else sl_reason
                elif since and time.time() - since > max_wait:
                    sl_reason = "STOP LOSS (patience expiree)"
                elif since and not trend_ok_sl:
                    sl_reason = "STOP LOSS (tendance cassee)"
                elif favorable_sl and trend_ok_sl:
                    if since is None:
                        state.sl_flow_patience_since = time.time()
                        pos["sl_patience_used"] = True
                        trend_txt = f", tendance intacte (EMA200 ${ema200_sl:.4g})" if ema200_sl is not None else ""
                        self.emit("log", {"msg": f"[{ticker}] {mode_label_sa} SL atteint ({pnl_pct:.2f}%) mais flux favorable ({flow_sl:+.2f}){trend_txt} — patience (max -{max_loss_pct:.2f}% / {max_wait // 60} min)", "level": "warn"})
                    return
                elif since:
                    sl_reason = "STOP LOSS (flux defavorable)"
            if pnl_pct <= -immediate_cap_pct and sl_reason:
                _result = self._safe_close_position(state, price, sl_reason, ticker, pos, symbol, mode)

                if _result is None:

                    return

                pnl, _, trade = _result
                self.emit("trade", trade)
                self.emit("log", {"msg": f"[{ticker}] {mode_label_sa} {sl_reason} : perte {pnl_pct:.2f}% (plafond {immediate_cap_pct:.2f}%) @ ${price:.4f} | PnL: ${pnl:.2f}", "level": "loss"})
                if pos["type"] == "long":
                    state.post_win_confirm_long = True
                    state.confirm_count_long = 0
                else:
                    state.post_win_confirm_short = True
                    state.confirm_count_short = 0
                self._register_max_loss(ticker, pos.get("confidence"))
                self._save_open_positions()
                self._persist_capital_snapshot()
                return

            # 0) v4.70 — SUR DEMANDE EXPLICITE : PLAFOND DUR en dernier
            #    recours — ferme QUOI QU IL ARRIVE au-dela de ce seuil,
            #    INDEPENDANT du retournement (contrairement au SL
            #    conditionnel ci-dessous). Complete les 2 mecanismes
            #    existants (SL conditionnel + retournement confirme) qui,
            #    ENSEMBLE, ne garantissaient aucune perte maximale absolue
            #    (une perte pouvait continuer de se creuser tant qu aucun
            #    des deux n etait satisfait). ACTIF par defaut (-5%),
            #    contrairement au SL conditionnel qui reste desactive par
            #    defaut.
            if cfg.get("SPOT_ACCUM_HARD_SL_ENABLED", True):
                hard_sl_pct = cfg.get("SPOT_ACCUM_HARD_SL_PCT", 5.0)
                if pnl_pct <= -hard_sl_pct:
                    # v4.264 — ordre reel D ABORD, fermeture cote bot seulement si confirmee
                    _result = self._safe_close_position(state, price, "STOP LOSS", ticker, pos, symbol, mode)
                    if _result is None:
                        return
                    pnl, _, trade = _result
                    self.emit("trade", trade)
                    self.emit("log", {"msg": f"[{ticker}] {mode_label_sa} PLAFOND DUR atteint ({hard_sl_pct:.1f}% du PnL, sans condition de retournement) @ ${price:.2f} | PnL: ${pnl:.2f}", "level": "loss"})
                    self._register_max_loss(ticker, pos.get("confidence"))
                    self._save_open_positions()
                    return

            # v4.211 — SUR DEMANDE EXPLICITE : ancien SL simple (%PnL) retire
            # — faisait doublon avec le SL structurel (rupture de support
            # confirmee + plafond immediat de securite), et s executait
            # PLUS TOT dans cette fonction, court-circuitant systematiquement
            # la logique structurelle des qu il etait active. Confirme comme
            # cause reelle d un clustering de pertes a exactement -1.5% (sa
            # valeur par defaut), incompatible avec la variabilite attendue
            # d un SL base sur la structure de marche.

            # v4.47/v4.54 — SUR DEMANDE EXPLICITE : fermeture si un
            # RETOURNEMENT DE TENDANCE est CONFIRME. v4.54 corrige une vraie
            # faille : l EMA200 ne se met a jour qu une fois toutes les
            # ~2 min (echantillonnage par bougie), alors que l ancienne
            # confirmation ne durait que 18 cycles (~3 min) — l EMA200
            # elle-meme n avait quasiment pas bouge sur cette fenetre, ce
            # n etait donc pas une vraie confirmation de retournement, juste
            # du bruit de prix a court terme sous une ligne quasi figee.
            # Corrige sur DEUX axes distincts, pas seulement la duree :
            #   1) DUREE allongee a un niveau coherent avec la vitesse reelle
            #      de l EMA200 (30 min par defaut, au lieu de 3 min).
            #   2) QUALITE DES DONNEES : n accepte le signal que si l EMA200
            #      est suffisamment MATURE (assez de bougies accumulees, pas
            #      juste le minimum de 10) ET que la collecte est SAINE en ce
            #      moment (pas de coupure WebSocket recente, pas de gap de
            #      donnees) — un retournement calcule sur des donnees
            #      fraichement redemarrees ou une collecte instable n est pas
            #      fiable, meme si le compteur de cycles est au complet.
            if cfg.get("SPOT_ACCUM_REVERSAL_EXIT_ENABLED", True):
                min_maturity = cfg.get("SPOT_ACCUM_REVERSAL_MIN_EMA_MATURITY", 100)
                data_mature = len(state.mtf_prices) >= min_maturity
                data_healthy = self._is_ws_healthy() if self.info is not None else True
                ema200_now = self._trend_ema(state)

                if not data_mature or not data_healthy or ema200_now is None:
                    # Donnees pas assez fiables pour juger d un retournement
                    # — on n accumule ni ne remet a zero le compteur, on
                    # attend simplement d avoir une lecture de confiance.
                    reason_skip = "EMA200 pas assez mature" if not data_mature else ("collecte instable" if not data_healthy else "EMA200 indisponible")
                    if state.spot_accum_reversal_count > 0:
                        self.emit("log", {"msg": f"[{ticker}] 🌱 Retournement en cours d'evaluation suspendu ({reason_skip}) — compteur conserve a {state.spot_accum_reversal_count}", "level": "dim"})
                elif ((price < ema200_now) if pos.get("type") == "long" else (price > ema200_now)):
                    # v4.322 — FIX BUG CONFIRME : ce compteur ne testait QUE
                    # "prix sous l EMA" (logique LONG), pour tous les modes
                    # partageant ce bloc (Spot-Accum, Accumulation, Forex).
                    # Pour un SHORT, "prix sous l EMA" = la tendance CONTINUE
                    # dans le sens du trade : le compteur montait donc
                    # pendant que le short gagnait, et apres ~30 min
                    # (SPOT_ACCUM_REVERSAL_CONFIRM_CYCLES) la position etait
                    # fermee en plein mouvement favorable ("RETOURNEMENT
                    # CONFIRME"), puis re-ouverte quelques minutes plus tard
                    # puisque les conditions d entree restaient valides.
                    # Observe sur PAXG SHORT (fermes apres 26 a 65 min) : 14
                    # des 16 sorties "RETOURNEMENT CONFIRME" du lot Forex
                    # etaient des SHORT. Le bloc dedie Accumulation (plus
                    # haut) gerait deja correctement les deux sens.
                    state.spot_accum_reversal_count += 1
                else:
                    state.spot_accum_reversal_count = 0

                confirm_needed = cfg.get("SPOT_ACCUM_REVERSAL_CONFIRM_CYCLES", 180)  # ~30 min par defaut
                if state.spot_accum_reversal_count >= confirm_needed:
                    # v4.212 — generalise via pos["type"] (long pour
                    # Spot-Accum, short pour Accumulation).
                    if not self._candle_color_confirms_reversal(state, pos["type"]):
                        self._save_open_positions()
                        return
                    # v4.264 — ordre reel D ABORD, fermeture cote bot seulement si confirmee
                    _result = self._safe_close_position(state, price, "RETOURNEMENT CONFIRME", ticker, pos, symbol, mode)
                    if _result is None:
                        return
                    pnl, _, trade = _result
                    self.emit("trade", trade)
                    if pnl > 0:
                        self._register_win(ticker)
                    else:
                        self._register_max_loss(ticker, pos.get("confidence"))
                    self.emit("log", {"msg": f"[{ticker}] {mode_label_sa} RETOURNEMENT CONFIRME (prix {'sous' if pos.get('type') == 'long' else 'au-dessus de'} l'EMA depuis {confirm_needed} cycles, donnees matures et saines) @ ${price:.2f} | PnL: ${pnl:.2f}", "level": "warn"})
                    state.spot_accum_reversal_count = 0
                    self._save_open_positions()
                    return

            # v4.73 — SUR DEMANDE EXPLICITE : TP conditionnel sur retournement
            # RETIRE — largement redondant avec le TTP (qui s arme des 1%,
            # avant le seuil de 1.5% de ce mecanisme, et surveille deja tout
            # repli depuis le pic avec une marge plus serree de 0.5%). Le cas
            # ou ce mecanisme apportait une vraie valeur ajoutee (le prix
            # atteint le seuil et franchit l EMA200 dans le MEME instant,
            # sans jamais monter plus haut avant) etait juge trop etroit pour
            # justifier la complexite et le risque de redondance/confusion.

            # 2) Objectif : SPOT_ACCUM_TARGET_SR_PCT (80% par defaut) de la
            #    distance support-resistance MESUREE A L ENTREE — fermeture
            #    immediate des que le prix l atteint, meme si le trailing
            #    n a pas encore suivi jusque-la.
            target_price = pos.get("target_price")
            if cfg.get("SPOT_ACCUM_TARGET_EXIT_ENABLED", False) and target_price is not None and price >= target_price:
                # v4.107 — SUR DEMANDE EXPLICITE : motif distinct de "TRAILING
                # TAKE PROFIT" — permet de voir dans l historique lequel des
                # 2 mecanismes a reellement ferme le trade (objectif atteint
                # vs repli depuis le pic).
                # v4.264 — ordre reel D ABORD, fermeture cote bot seulement si confirmee
                _result = self._safe_close_position(state, price, "OBJECTIF ATTEINT", ticker, pos, symbol, mode)
                if _result is None:
                    return
                pnl, _, trade = _result
                self.emit("trade", trade)
                self._register_win(ticker)
                self.emit("log", {"msg": f"[{ticker}] {mode_label_sa} OBJECTIF ATTEINT (80% distance S/R) @ ${price:.2f} | PnL: +${pnl:.2f}", "level": "win"})
                self._save_open_positions()
                return

            # 3) Trailing : arme une fois le PnL >= SPOT_ACCUM_TTP_ARM_PCT
            #    (3% par defaut) OU le prix atteint trailing_arm_price
            #    (v4.49 — support + 70% de la distance support-resistance,
            #    seuil structurel qui S ADDITIONNE au seuil de PnL — arme
            #    des que L UN DES DEUX est atteint, celui qui arrive en
            #    premier), puis suit le pic avec une marge de
            #    SPOT_ACCUM_TTP_TOLERANCE_PCT (0.5% par defaut).
            arm_pct = cfg.get("SPOT_ACCUM_TTP_ARM_PCT", 3.0)
            if pos.get("strategy") == "forex":   # v4.333 : seuil propre au Forex
                arm_pct = cfg.get("FOREX_TTP_ARM_PCT", arm_pct)
            # v4.230 — SUR DEMANDE EXPLICITE : tolerance desormais relative
            # a la volatilite REELLE de l actif (ATR), pas un % fixe
            # identique pour tous — un actif volatil a une tolerance plus
            # large, un actif calme plus stricte. Convertit l ATR (prix
            # absolu) en % de PnL EQUIVALENT (integre le levier, puisque
            # pnl_pct est desormais lui-meme amplifie par le levier depuis
            # v4.229) — repli sur l ancien % fixe si l ATR n est pas encore
            # disponible.
            atr_abs_ttp, _ = calc_true_range_atr(list(state.candle_history), cfg.get("ATR_PERIOD", 14))
            if atr_abs_ttp is not None and atr_abs_ttp > 0 and price > 0:
                atr_pct_of_price = atr_abs_ttp / price * 100
                tolerance_pct = atr_pct_of_price * cfg.get("TTP_ATR_TOLERANCE_MULTIPLIER", 1.0)
            else:
                tolerance_pct = cfg.get("SPOT_ACCUM_TTP_TOLERANCE_PCT", 0.5)
            if pos.get("countertrend"):
                # v4.286 — trade CONTRE la tendance de fond : mouvement attendu
                # court, trailing arme plus tot et plus serre
                _ct = cfg.get("COUNTERTREND_TTP_MULT", 0.6)
                arm_pct *= _ct
                tolerance_pct *= _ct
            # v4.254 — SUR DEMANDE EXPLICITE : prolonge la tolerance si le
            # flux de transactions confirme TOUJOURS la direction du trade
            # — une vraie conviction de marche qui perdure justifie de
            # laisser un peu plus de marge avant de couper, plutot qu une
            # tolerance fixe ignorant si le marche soutient encore le
            # mouvement.
            if ticker is not None and cfg.get("TTP_FLOW_EXTEND_TOLERANCE_ENABLED", True):
                flow_pressure_extend = self._compute_trade_flow_pressure(ticker, price_now=price)
                if flow_pressure_extend is not None:
                    favorable_threshold = cfg.get("TTP_FLOW_EXTEND_THRESHOLD", 0.2)
                    flow_still_favorable = (flow_pressure_extend >= favorable_threshold) if pos["type"] == "long" else (flow_pressure_extend <= -favorable_threshold)
                    if flow_still_favorable:
                        tolerance_pct *= cfg.get("TTP_FLOW_EXTEND_MULTIPLIER", 1.5)
            # v4.173 — SUR DEMANDE EXPLICITE : retire l armement via le prix
            # structurel (support + 70% de la distance S/R) — pouvait armer
            # le trailing sur un PnL minuscule (0.29% observe) des que ce
            # prix etait proche de l entree, contraire a la philosophie
            # patiente de Spot-Accum. Seul le seuil de PnL declenche
            # desormais l armement.
            if not state.spot_accum_armed:
                if pnl_pct >= arm_pct:
                    state.spot_accum_armed = True
                    state.spot_accum_peak_pnl_pct = pnl_pct
            else:
                if state.spot_accum_peak_pnl_pct is None or pnl_pct > state.spot_accum_peak_pnl_pct:
                    state.spot_accum_peak_pnl_pct = pnl_pct

                # v4.254 — SUR DEMANDE EXPLICITE : sortie ACCELEREE sur
                # retournement BRUTAL du flux de transactions reel —
                # repond a un cas reel observe : une bougie verte pleine se
                # transformant en etoile filante EN COURS DE FORMATION,
                # rendant tout le profit AVANT meme que la bougie ne
                # cloture (les verifications de couleur/motif ne se
                # declenchent qu a la CLOTURE, ratant ce mouvement en
                # cours). Le flux de transactions, lui, se met a jour en
                # CONTINU — une inversion brutale et forte de la pression
                # (ex: passe d achat dominant a vente fortement dominante)
                # peut etre detectee PENDANT la formation de la bougie,
                # permettant de sortir avant que le profit ne soit
                # integralement rendu. INDEPENDANT de l etat de la bougie
                # en cours, contrairement au reste du mecanisme TTP.
                if cfg.get("TTP_FLOW_REVERSAL_EXIT_ENABLED", True) and ticker is not None:
                    flow_pressure_ttp = self._compute_trade_flow_pressure(ticker, price_now=price)
                    if flow_pressure_ttp is not None:
                        flow_reversal_threshold = cfg.get("TTP_FLOW_REVERSAL_THRESHOLD", -0.4) if pos["type"] == "long" else cfg.get("TTP_FLOW_REVERSAL_THRESHOLD", -0.4) * -1
                        flow_reversed = (flow_pressure_ttp <= flow_reversal_threshold) if pos["type"] == "long" else (flow_pressure_ttp >= -flow_reversal_threshold)
                        # N agit que si un pic significatif existe deja (pas
                        # sur un trade a peine ouvert, sans profit a proteger).
                        _fr_min_peak = (cfg.get("FOREX_TTP_FLOW_REVERSAL_MIN_PEAK_PCT", 0.12) if pos.get("strategy") == "forex"
                                        else cfg.get("TTP_FLOW_REVERSAL_MIN_PEAK_PCT", 0.3))   # v4.333
                        if flow_reversed and state.spot_accum_peak_pnl_pct >= _fr_min_peak:
                            _result = self._safe_close_position(state, price, "TRAILING TAKE PROFIT (retournement flux)", ticker, pos, symbol, mode)

                            if _result is None:

                                return

                            pnl, _, trade = _result
                            self.emit("trade", trade)
                            if pnl > 0:
                                self._register_win(ticker)
                            if pos["type"] == "long":
                                state.post_win_confirm_long = True
                                state.confirm_count_long = 0
                            else:
                                state.post_win_confirm_short = True
                                state.confirm_count_short = 0
                            self.emit("log", {"msg": f"[{ticker}] {mode_label_sa} retournement BRUTAL du flux detecte (pression {flow_pressure_ttp:+.2f}, pic +{state.spot_accum_peak_pnl_pct:.2f}%) — sortie avant cloture de bougie @ ${price:.2f} | PnL: ${pnl:.2f}", "level": "win" if pnl > 0 else "loss"})
                            self._save_open_positions()
                            self._persist_capital_snapshot()
                            return

                # v4.213 — SUR DEMANDE EXPLICITE : filet de securite
                # INCONDITIONNEL — INDEPENDANT de la couleur de bougie. La
                # logique inversee (v4.197) n applique AUCUNE limite de
                # repli tant que la bougie ne confirme pas un retournement
                # — si les bougies restent ambigues (jamais 2 consecutives
                # de meme couleur), un pic pourrait se degrader
                # integralement sans jamais declencher de protection. Ce
                # filet ferme QUOI QU IL ARRIVE au-dela de ce repli maximal
                # depuis le pic, peu importe la couleur de bougie.
                # v4.216 — SUR DEMANDE EXPLICITE : priorite au trailing
                # normal (0.4%, avec confirmation de couleur) pour les
                # PETITS pics — ce filet inconditionnel ne s active
                # desormais QUE si le pic a atteint au moins ce seuil
                # minimal, laissant le mecanisme normal seul gerer les
                # pics plus modestes (accepte le risque de redonnage total
                # sur un tres petit pic en echange de ne jamais fermer
                # prematurement sur une simple ambiguite de bougie).
                # v4.230 — SUR DEMANDE EXPLICITE : declencheur de VITESSE de
                # repli — independant de l ampleur absolue (contrairement
                # au filet ci-dessous). Un repli RAPIDE depuis le pic est un
                # signal fort en soi, meme si son ampleur totale reste sous
                # le seuil du filet inconditionnel (2% de pic minimum).
                # Suit un "checkpoint" du PnL toutes les
                # TTP_VELOCITY_WINDOW_SEC secondes, et compare la vitesse de
                # repli depuis ce point de reference.
                velocity_window_sec = cfg.get("TTP_VELOCITY_WINDOW_SEC", 60)
                velocity_giveback_pct = cfg.get("TTP_VELOCITY_GIVEBACK_PCT", 0.6)
                now_ts = time.time()
                checkpoint = getattr(state, "spot_accum_velocity_checkpoint", None)
                if checkpoint is None or now_ts - checkpoint[0] >= velocity_window_sec:
                    state.spot_accum_velocity_checkpoint = (now_ts, pnl_pct)
                elif checkpoint[1] - pnl_pct >= velocity_giveback_pct:
                    _result = self._safe_close_position(state, price, "TRAILING TAKE PROFIT (repli rapide)", ticker, pos, symbol, mode)

                    if _result is None:

                        return

                    pnl, _, trade = _result
                    self.emit("trade", trade)
                    if pnl > 0:
                        self._register_win(ticker)
                    if pos["type"] == "long":
                        state.post_win_confirm_long = True
                        state.confirm_count_long = 0
                    else:
                        state.post_win_confirm_short = True
                        state.confirm_count_short = 0
                    self.emit("log", {"msg": f"[{ticker}] {mode_label_sa} repli RAPIDE detecte ({checkpoint[1]:.2f}% -> {pnl_pct:.2f}% en moins de {velocity_window_sec}s) @ ${price:.2f} | PnL: ${pnl:.2f}", "level": "win" if pnl > 0 else "loss"})
                    self._save_open_positions()
                    self._persist_capital_snapshot()
                    return

                unconditional_giveback_pct = cfg.get("TTP_UNCONDITIONAL_GIVEBACK_PCT", 1.0)
                min_peak_for_unconditional = cfg.get("TTP_UNCONDITIONAL_GIVEBACK_MIN_PEAK_PCT", 2.0)
                if state.spot_accum_peak_pnl_pct >= min_peak_for_unconditional and state.spot_accum_peak_pnl_pct - pnl_pct >= unconditional_giveback_pct:
                    _result = self._safe_close_position(state, price, "TRAILING TAKE PROFIT (repli maximal)", ticker, pos, symbol, mode)

                    if _result is None:

                        return

                    pnl, _, trade = _result
                    self.emit("trade", trade)
                    if pnl > 0:
                        self._register_win(ticker)
                    if pos["type"] == "long":
                        state.post_win_confirm_long = True
                        state.confirm_count_long = 0
                    else:
                        state.post_win_confirm_short = True
                        state.confirm_count_short = 0
                    self.emit("log", {"msg": f"[{ticker}] {mode_label_sa} repli maximal atteint (pic +{state.spot_accum_peak_pnl_pct:.2f}%, repli {unconditional_giveback_pct}%, sans confirmation de couleur) @ ${price:.2f} | PnL: ${pnl:.2f}", "level": "win" if pnl > 0 else "loss"})
                    self._save_open_positions()
                    self._persist_capital_snapshot()
                    return
                # v4.197 — SUR DEMANDE EXPLICITE : INVERSE la logique — retire
                # la sortie IMMEDIATE sur simple changement de couleur,
                # remplacee par une PORTE vers la verification de tolerance
                # ci-dessous (meme principe que tier0/tier1).
                color_reversed_sa = cfg.get("EARLY_REVERSAL_EXIT_ENABLED", True) and self._candle_color_confirms_reversal(state, pos["type"])
                # v4.254 — SUR DEMANDE EXPLICITE : le flux de transactions
                # peut AUSSI ouvrir cette porte, independamment de la
                # couleur de bougie — un retournement de pression reel est
                # au moins aussi fiable qu une simple couleur de bougie, et
                # reagit plus vite (pas besoin d attendre la cloture).
                if not color_reversed_sa and cfg.get("TTP_FLOW_REVERSAL_EXIT_ENABLED", True) and ticker is not None:
                    flow_pressure_gate = self._compute_trade_flow_pressure(ticker, price_now=price)
                    if flow_pressure_gate is not None:
                        gate_threshold = cfg.get("TTP_FLOW_GATE_THRESHOLD", 0.15)
                        color_reversed_sa = (flow_pressure_gate <= -gate_threshold) if pos["type"] == "long" else (flow_pressure_gate >= gate_threshold)
                # v4.335 — SUR DEMANDE EXPLICITE : PLANCHER PUR pour Forex, comme pour
                # Funding (v4.315) — la fermeture ne depend plus d une bougie ou d un
                # flux qui se retourne : des que le PnL repasse sous (pic - marge de
                # repli ATR), on ferme. Desactivable via FOREX_PURE_FLOOR = 0.
                if pos.get("strategy") == "forex" and cfg.get("FOREX_PURE_FLOOR", 1):
                    color_reversed_sa = True
                if color_reversed_sa and pnl_pct <= state.spot_accum_peak_pnl_pct - tolerance_pct and not _forex_dsl_on:
                    # v4.83 — SUR DEMANDE EXPLICITE : meme filtre "la
                    # tendance tient toujours" que tier0/tier1 (v4.77/v4.82),
                    # etendu a Spot-Accum — le seuil structurel (support +
                    # 70% de la fourchette) peut armer le trailing a un PnL
                    # minuscule sur une fourchette etroite, rendant le pic
                    # memorise (et donc la marge de sortie) lui aussi
                    # minuscule. Sans ce filtre, un simple repli de 0.5%
                    # depuis ce pic deja tres bas suffisait a fermer, meme
                    # en pleine tendance haussiere intacte.
                    # v4.214 — SUR DEMANDE EXPLICITE : par defaut True (pas
                    # False) si l EMA200 n est pas encore disponible (donnees
                    # insuffisantes, position tres recente) — donne le
                    # benefice du doute plutot que de fermer aveuglement une
                    # position dont on ne peut pas encore confirmer que la
                    # tendance de fond a genuinement change.
                    trend_still_intact_sa = True
                    # v4.333 — SUR DEMANDE EXPLICITE : filtre desactive pour Forex (comme
                    # pour Funding) — un repli depuis le pic ne doit plus etre ignore
                    # sous pretexte que le prix reste du bon cote de l EMA200. La
                    # confirmation (bougie/flux + patience) reste inchangee.
                    # v4.334 — CORRECTION D UN BUG DE MON CORRECTIF v4.333 : la valeur par
                    # defaut de trend_still_intact_sa est True (benefice du doute quand l
                    # EMA200 est indisponible). Avec le filtre desactive, elle restait donc
                    # True et la branche "tendance intacte -> position maintenue" s
                    # executait TOUJOURS (le plafond de redonnage ne s active qu a partir
                    # d un pic de 2.5 %, jamais atteint en Forex) : l inverse du but voulu.
                    # Desormais, filtre desactive => "tendance intacte" = False.
                    _hold_on = cfg.get("TTP_TREND_HOLD_FILTER_ENABLED", True)
                    if pos.get("strategy") == "forex":
                        _hold_on = bool(cfg.get("FOREX_TREND_HOLD_FILTER_ENABLED", 0))
                        if not _hold_on:
                            trend_still_intact_sa = False
                    if _hold_on:
                        ema200_hold_sa = self._trend_ema(state)
                        if ema200_hold_sa is not None:
                            trend_still_intact_sa = (price > ema200_hold_sa) if pos["type"] == "long" else (price < ema200_hold_sa)
                    if trend_still_intact_sa:
                        # v4.103 — SUR DEMANDE EXPLICITE : le plafond pour
                        # les PETITS pics (< min_peak_for_cap) est retire —
                        # juge nuisible, laisse le TTP de base et le
                        # retournement confirme gerer seuls cette zone. Le
                        # plafond pour les GROS pics (>= min_peak_for_cap,
                        # 20% par defaut) reste actif, inchange.
                        min_peak_for_cap_sa = cfg.get("TTP_MAX_GIVEBACK_MIN_PEAK_PCT", 2.5)
                        giveback_cap_triggered_sa = False
                        if state.spot_accum_peak_pnl_pct >= min_peak_for_cap_sa:
                            max_giveback_pct_sa = cfg.get("TTP_MAX_GIVEBACK_PCT_OF_PEAK", 20.0)
                            giveback_floor_sa = state.spot_accum_peak_pnl_pct * (1 - max_giveback_pct_sa / 100)
                            if pnl_pct <= giveback_floor_sa:
                                giveback_cap_triggered_sa = True
                        if not giveback_cap_triggered_sa:
                            state.ttp_breach_streak = 0  # v4.154 — reinitialise la patience TTP
                            self.emit("log", {"msg": f"[{ticker}] 🌱 Repli Spot-Accum a {pnl_pct:.2f}% (pic {state.spot_accum_peak_pnl_pct:.2f}%) mais tendance de fond toujours intacte — position maintenue", "level": "dim"})
                            self._save_open_positions()
                            return
                        # sinon : plafond de redonnage atteint — v4.154, exige aussi la confirmation patience+bougie
                        if not self._ttp_confirmed_to_close(state, pos["type"]):
                            self._save_open_positions()
                            return
                    # v4.264 — ordre reel D ABORD, fermeture cote bot seulement si confirmee
                    _result = self._safe_close_position(state, price, "TRAILING TAKE PROFIT", ticker, pos, symbol, mode)
                    if _result is None:
                        return
                    pnl, _, trade = _result
                    self.emit("trade", trade)
                    self._register_win(ticker)
                    self.emit("log", {"msg": f"[{ticker}] {mode_label_sa} TTP @ ${price:.2f} | pic +{state.spot_accum_peak_pnl_pct:.2f}% | PnL: +${pnl:.2f}", "level": "win"})
                    self._save_open_positions()
                    return
            self._save_open_positions()
            return

        # ── 1. STOP LOSS — % de E, perte $ PLAFONNEE quel que soit le levier ──
        # v4.10 — compare pnl_usd (deja amplifie par le levier via la formule
        # ci-dessus) au plafond E*sl_pct/100 (LUI independant du levier) :
        # le mouvement de prix necessaire pour toucher ce plafond $ diminue
        # donc mecaniquement quand le levier monte, mais le $ perdu reste fixe.
        # v4.24 — priorite au seuil MEMORISE sur la position (adaptatif ou
        # fixe, fige au moment de l ouverture) — repli sur la config globale
        # actuelle pour les positions ouvertes avant ce fix (champ absent).
        sl_pct_of_e = pos.get("sl_pct_of_e", cfg.get("SL_PCT_OF_E", 1.0))
        sl_usd = -E * sl_pct_of_e / 100
        # v4.176 — SUR DEMANDE EXPLICITE : le SL structurel (rupture
        # CONFIRMEE du support/resistance memorise a l entree) s applique
        # desormais a TOUS les modes SAUF : Funding (logique fondee sur le
        # taux de financement, pas la structure de prix — laisse inchangee
        # a la demande explicite) et les entrees Accumulation qualifiees
        # specifiquement via son mode "trader le range" (garde son propre
        # SL % de prix classique, coherent avec sa logique dediee).
        # v4.249 — SUR DEMANDE EXPLICITE : mecanisme de sortie DEDIE pour
        # Funding — sa these d entree (taux de financement EXTREME) est
        # elle-meme le meilleur signal de sortie : une fois le taux revenu
        # DANS la fourchette normale, la these d origine est resolue,
        # independamment du PnL du moment (peut sortir en gain OU en
        # perte modeste — l objectif est de ne pas s attarder une fois la
        # raison d etre du trade disparue, coherent avec la nature de
        # retour a la moyenne RAPIDE de ce mode).
        if pos.get("strategy") == "funding_contrarian" and cfg.get("FUNDING_EXIT_ON_RATE_NORMALIZED", True):
            hourly_rate_now = self.funding_rates.get(ticker)
            if hourly_rate_now is not None:
                annual_pct_now = hourly_rate_now * 24 * 365 * 100
                normalize_threshold = cfg.get("FUNDING_ANNUAL_THRESHOLD_PCT", 25.0) * cfg.get("FUNDING_EXIT_NORMALIZE_RATIO", 0.4)
                rate_normalized = abs(annual_pct_now) < normalize_threshold
                if rate_normalized:
                    _result = self._safe_close_position(state, price, "TAUX DE FINANCEMENT NORMALISE", ticker, pos, symbol, mode)

                    if _result is None:

                        return

                    pnl, _, trade = _result
                    self.emit("trade", trade)
                    if pnl > 0:
                        self._register_win(ticker)
                    self.emit("log", {"msg": f"[{ticker}] 💰 Funding : taux normalise ({annual_pct_now:+.1f}% annualise, sous le seuil de sortie) — these d entree resolue @ ${price:.4f} | PnL: ${pnl:.2f}", "level": "win" if pnl > 0 else "loss"})
                    self._save_open_positions()
                    self._persist_capital_snapshot()
                    return

        use_structural_sl = (
            pos.get("strategy") != "funding_contrarian"
            and not pos.get("entered_via_range", False)
        )

        # v4.227 — SUR DEMANDE EXPLICITE : etoile filante ROUGE confirmee —
        # signal de sortie pour une position LONG (Forex), meme principe
        # que pour Spot-Accum. Funding exclu, coherent avec le reste des
        # mecanismes bases sur la structure de prix.
        if (pos["type"] == "long" and pos.get("strategy") != "funding_contrarian"
                and cfg.get("SHOOTING_STAR_DETECTION_ENABLED", True) and self._shooting_star_confirmed(state)):
            _result = self._safe_close_position(state, price, "ETOILE FILANTE CONFIRMEE", ticker, pos, symbol, mode)

            if _result is None:

                return

            pnl, _, trade = _result
            self.emit("trade", trade)
            if pnl > 0:
                self._register_win(ticker)
            state.post_win_confirm_long = True
            state.confirm_count_long = 0
            self.emit("log", {"msg": f"[{ticker}] {strat_tag}ETOILE FILANTE confirmee (30 min) @ ${price:.2f} | PnL: ${pnl:.2f}", "level": "win" if pnl > 0 else "loss"})
            self._save_open_positions()
            self._persist_capital_snapshot()
            return

        if use_structural_sl:
            structural_level = pos.get("support_at_entry") if pos["type"] == "long" else pos.get("resistance_at_entry")
            # v4.193 — SUR DEMANDE EXPLICITE : filet de securite IMMEDIAT,
            # SANS attendre la confirmation (patience + couleur de bougie)
            # du SL structurel — la confirmation complete peut prendre trop
            # de temps (fenetre glissante de 15 cycles, ~150s), laissant le
            # temps a une perte significative de se creuser avant de
            # declencher. Ferme immediatement des que la perte atteint ce
            # plafond, meme si la rupture structurelle n est pas encore
            # confirmee.
            # v4.229 — pnl_pct integre desormais deja le levier a la
            # source (voir plus haut) — hard_cap_pct reste tel quel, sans
            # division supplementaire (qui aurait double-compte le levier).
            hard_cap_pct = cfg.get("STRUCTURAL_SL_HARD_CAP_PCT", 0.5)
            if pos.get("strategy") == "accumulation" and cfg.get("ACCUMULATION_SL_CAP_PCT"):
                hard_cap_pct = cfg["ACCUMULATION_SL_CAP_PCT"]  # v4.269
            if pnl_pct <= -hard_cap_pct:
                _result = self._safe_close_position(state, price, "STOP LOSS (plafond immediat)", ticker, pos, symbol, mode)

                if _result is None:

                    return

                pnl, _, trade = _result
                self.emit("trade", trade)
                self.emit("log", {"msg": f"[{ticker}] {strat_tag}STOP LOSS plafond immediat : perte {pnl_pct:.2f}% (x{leverage_now}) >= {hard_cap_pct:.2f}% @ ${price:.4f} | PnL: ${pnl:.2f}", "level": "loss"})
                if pos["type"] == "long":
                    state.post_win_confirm_long = True
                    state.confirm_count_long = 0
                else:
                    state.post_win_confirm_short = True
                    state.confirm_count_short = 0
                self._register_max_loss(ticker, pos.get("confidence"))
                self._save_open_positions()
                self._persist_capital_snapshot()
                return
            if self._structural_sl_broken(state, pos, price):
                level_label = "support" if pos["type"] == "long" else "resistance"
                # v4.264 — ordre reel D ABORD, fermeture cote bot seulement si confirmee
                _result = self._safe_close_position(state, price, f"STOP LOSS ({level_label} rompu)", ticker, pos, symbol, mode)
                if _result is None:
                    return
                pnl, _, trade = _result
                self.emit("trade", trade)
                self.emit("log", {"msg": f"[{ticker}] {strat_tag}STOP LOSS : {level_label} ${structural_level:.4f} rompu et confirme @ ${price:.4f} | PnL: ${pnl:.2f}", "level": "loss"})
                if pos["type"] == "long":
                    state.post_win_confirm_long = True
                    state.confirm_count_long = 0
                else:
                    state.post_win_confirm_short = True
                    state.confirm_count_short = 0
                self._register_max_loss(ticker, pos.get("confidence"))
                self._save_open_positions()
                self._persist_capital_snapshot()
                return
        else:
            # v4.149 — SUR DEMANDE EXPLICITE : "patience" avant de fermer sur
            # Stop Loss — exige que le prix reste au-dela du seuil pendant
            # SL_PATIENCE_CYCLES cycles CONSECUTIFS (defaut 10, ~100s) avant de
            # fermer reellement. Une mèche breve (pic de bruit qui touche le
            # seuil puis repart aussitot dans le bon sens) n a plus le temps de
            # declencher une fermeture — seul un vrai effondrement SOUTENU
            # continue de le faire, puisque le compteur ne progresse que si le
            # depassement persiste d un cycle au suivant (repli a 0 des que le
            # prix revient dans les clous).
            sl_patience_cycles = cfg.get("SL_PATIENCE_CYCLES", 10)
            if pnl_usd <= sl_usd:
                state.sl_breach_streak = getattr(state, "sl_breach_streak", 0) + 1
            else:
                state.sl_breach_streak = 0
            if pnl_usd <= sl_usd and state.sl_breach_streak >= sl_patience_cycles:
                _result = self._safe_close_position(state, price, "STOP LOSS", ticker, pos, symbol, mode)

                if _result is None:

                    return

                pnl, _, trade = _result
                self.emit("trade", trade)
                peak_str = f" | pic atteint avant la chute : +${trade['peak_pnl_usd']:.2f}" if trade.get("peak_pnl_usd") else ""
                self.emit("log", {"msg": f"[{ticker}] {strat_tag}STOP LOSS @ ${price:.2f} | PnL: ${pnl:.2f} (plafond -${-sl_usd:.2f} = {sl_pct_of_e:.2f}% de E, mouvement de prix requis a x{pos.get('leverage',1)} : {sl_pct_of_e/max(pos.get('leverage',1),1):.2f}%){peak_str}", "level": "loss"})
                # v4.31 — SUR DEMANDE EXPLICITE, suite a une serie de 7+ pertes
                # consecutives dans le MEME sens observee (ARB LONG) : la
                # confirmation renforcee (voir plus bas) se declenche desormais
                # APRES TOUTE fermeture dans un sens donne — gain OU perte — pas
                # seulement apres un gain. Une perte prouve que la direction
                # etait fausse, raison de plus d exiger une reconfirmation
                # soutenue avant de retenter le meme pari.
                if pos["type"] == "long":
                    state.post_win_confirm_long = True
                    state.confirm_count_long = 0
                else:
                    state.post_win_confirm_short = True
                    state.confirm_count_short = 0
                self._register_max_loss(ticker, pos.get("confidence"))
                self._save_open_positions()  # sauvegarde en live ET en paper
                self._persist_capital_snapshot()  # v4.3 - resilience crash/OOM
                return

        # ── 2. SL Hyperliquid — filet de securite (ne devrait presque jamais
        #      se declencher en premier, le Stop Loss bot est plus serre) ────
        sl_hit = (pos["type"] == "long" and price <= pos["sl"]) or \
                 (pos["type"] == "short" and price >= pos["sl"])
        if sl_hit:
            _result = self._safe_close_position(state, price, "SL SECURITE HYPERLIQUID", ticker, pos, symbol, mode)

            if _result is None:

                return

            pnl, _, trade = _result
            self.emit("trade", trade)
            peak_str = f" | pic atteint avant la chute : +${trade['peak_pnl_usd']:.2f}" if trade.get("peak_pnl_usd") else ""
            self.emit("log", {"msg": f"[{ticker}] {strat_tag}SL SECURITE @ ${price:.2f} | PnL: ${pnl:.2f}{peak_str}", "level": "loss"})
            # v4.31 — meme raisonnement que pour le STOP LOSS ci-dessus.
            if pos["type"] == "long":
                state.post_win_confirm_long = True
                state.confirm_count_long = 0
            else:
                state.post_win_confirm_short = True
                state.confirm_count_short = 0
            self._register_max_loss(ticker, pos.get("confidence"))
            self._save_open_positions()  # sauvegarde en live ET en paper
            self._persist_capital_snapshot()  # v4.3 - resilience crash/OOM
            return

        # ── 3. Trailing Take Profit — % de MOUVEMENT DE PRIX REEL (v4.7) ────
        # A la difference du SL (qui reste en % de E, plafonnant le $ perdu
        # quel que soit le levier), le TP ne doit PAS etre reduit par le
        # levier : ces seuils sont de vrais % de mouvement de PRIX, et c est
        # le levier qui amplifie librement le $ gagne pour ce meme mouvement.
        leverage = pos.get("leverage", 1)
        # v4.24 — priorite aux seuils MEMORISES sur la position (adaptatifs
        # ou fixes, figes a l ouverture) — repli sur la config globale pour
        # les positions ouvertes avant ce fix.
        arm1_price_pct  = pos.get("ttp_arm1_pct",  cfg.get("TTP_ARM1_PRICE_PCT", 1.0))
        lock1_price_pct = pos.get("ttp_lock1_pct", cfg.get("TTP_LOCK1_PRICE_PCT", 0.8))
        arm2_price_pct  = pos.get("ttp_arm2_pct",  cfg.get("TTP_ARM2_PRICE_PCT", 1.3))
        gap_price_pct   = pos.get("ttp_gap_pct",   cfg.get("TTP_TRAIL_GAP_PRICE_PCT", 0.3))
        # v4.56 — FIX BUG CRITIQUE : priorite au seuil MEMORISE sur la
        # position (mis a l echelle si SL/TTP adaptatif etait actif a
        # l ouverture) — repli sur la config globale FIXE pour les
        # positions ouvertes avant ce fix (champ absent). Avant ce fix, ces
        # deux valeurs restaient TOUJOURS fixes meme quand arm1_price_pct
        # etait mis a l echelle par l adaptatif, inversant leur relation
        # logique sur les actifs a faible ATR — armement/desarmement du
        # tier0/tier1 en boucle, plusieurs fois par minute (observe sur TIA).
        tier0_arm_pct   = pos.get("tier0_arm_pct", cfg.get("TTP_TIER0_ARM_PRICE_PCT", 0.5))
        tier0_gap_pct   = pos.get("tier0_gap_pct", cfg.get("TTP_TIER0_GAP_PRICE_PCT", 0.42))

        # v4.256 — SUR DEMANDE EXPLICITE : retire le TTP classique (par
        # mouvement de PRIX) pour Funding — sa these de sortie n est pas
        # "le prix a suffisamment bouge", mais "le taux de financement
        # s est normalise" (voir le mecanisme dedie plus haut,
        # FUNDING_EXIT_ON_RATE_NORMALIZED). Prendre un profit sur un
        # simple mouvement de prix, AVANT que le taux n ait eu le temps de
        # se normaliser, sortirait prematurement d une position dont la
        # these n est pas encore resolue. Le SL structurel/classique
        # (calcule plus haut dans cette fonction) reste actif normalement
        # — protection necessaire independamment de cette decision.
        # v4.258 — SUR DEMANDE EXPLICITE, FIX BUG CRITIQUE : le retrait pur
        # et simple du TTP (v4.256) laissait un trou — confirme par un cas
        # reel : des positions montant jusqu a +3% de pic, puis rendant
        # TOUT le gain jusqu a 0%, sans qu AUCUN mecanisme ne protege le
        # pic si le taux ne se normalise pas assez vite. Remplace desormais
        # par un TTP DEDIE, simple et fixe (pas le systeme tier0/tier1
        # complexe de Forex) : arme a +1.5% de pic, puis tolere un repli
        # de 0.5% avant de fermer — un filet minimal, laissant toujours la
        # normalisation du taux etre le signal PRIORITAIRE (verifie plus
        # haut dans cette fonction, avant ce point), mais sans jamais
        # risquer de rendre un gain significatif dans l attente.
        if pos.get("strategy") == "funding_contrarian" and cfg.get("FUNDING_SKIP_CLASSIC_TTP", True):
            self._funding_pnl_track(state, pos, pnl_pct)   # v4.342 : suivi du chemin du PnL (lecture seule)
            arm_pct_funding = cfg.get("FUNDING_TTP_ARM_PCT", 0.5)
            tolerance_pct_funding = cfg.get("FUNDING_TTP_TOLERANCE_PCT", 0.5)
            if state.spot_accum_peak_pnl_pct is None or pnl_pct > state.spot_accum_peak_pnl_pct:
                state.spot_accum_peak_pnl_pct = pnl_pct
            peak_f = state.spot_accum_peak_pnl_pct
            # v4.374 — SANS PROGRES : le trade n'a jamais pris un petit gain apres N minutes (21 des 22 trades dans ce cas finissaient au stop)
            if cfg.get("FUNDING_STALE_ENABLED", 1) and not state.spot_accum_armed and peak_f < cfg.get("FUNDING_STALE_PEAK_PCT", 0.15):
                try:
                    age_min = (datetime.now() - datetime.strptime(pos.get("opened_at", ""), "%d/%m/%Y %H:%M:%S")).total_seconds() / 60.0
                except (ValueError, TypeError):
                    age_min = 0.0
                if age_min >= cfg.get("FUNDING_STALE_MIN", 20):
                    _result = self._safe_close_position(state, price, "FUNDING SANS PROGRES", ticker, pos, symbol, mode)
                    if _result is None:
                        return
                    pnl, _, trade = _result
                    self.emit("trade", trade)
                    if pnl > 0:
                        self._register_win(ticker)
                    self.emit("log", {"msg": f"[{ticker}] 💰 Funding : sans progres apres {age_min:.0f} min (pic +{peak_f:.2f}% sous +{cfg.get('FUNDING_STALE_PEAK_PCT', 0.15)}%) @ ${price:.4f} | PnL: ${pnl:.2f}", "level": "win" if pnl > 0 else "loss"})
                    self._save_open_positions()
                    self._persist_capital_snapshot()
                    return
            if peak_f >= arm_pct_funding:
                if not state.spot_accum_armed:
                    state.spot_accum_armed = True
                    self.emit("log", {"msg": f"[{ticker}] 💰 Funding TTP arme a +{peak_f:.2f}% (seuil {arm_pct_funding}%)", "level": "signal"})
                giveback_f = peak_f - pnl_pct
                effective_tol = tolerance_pct_funding
                flow_state = "neutre"
                flow_f = None
                # v4.268 — le flux n est consulte que lorsque le repli devient
                # significatif (moitie de la tolerance) : inutile de le lire a
                # chaque tick quand le trade progresse.
                if cfg.get("FUNDING_TTP_FLOW_ENABLED", True) and ticker is not None and giveback_f >= tolerance_pct_funding * 0.5:
                    flow_f = self._compute_trade_flow_pressure(ticker, price_now=price)
                    if flow_f is not None:
                        thr_f = cfg.get("FUNDING_TTP_FLOW_THRESHOLD", 0.2)
                        favorable = flow_f <= -thr_f if pos["type"] == "short" else flow_f >= thr_f
                        against = flow_f >= thr_f if pos["type"] == "short" else flow_f <= -thr_f
                        if favorable:
                            effective_tol = max(tolerance_pct_funding, cfg.get("FUNDING_TTP_FLOW_MAX_TOLERANCE_PCT", 0.9))
                            flow_state = "favorable"
                        elif against:
                            effective_tol = min(tolerance_pct_funding, cfg.get("FUNDING_TTP_FLOW_FAST_TOLERANCE_PCT", 0.25))
                            flow_state = "contraire"
                min_lock_f = cfg.get("FUNDING_TTP_MIN_LOCK_PCT", 0.3)
                floor_hit = pnl_pct <= min_lock_f
                if giveback_f >= effective_tol or floor_hit:
                    if floor_hit and giveback_f < effective_tol:
                        reason_f = "TRAILING TAKE PROFIT (Funding, plancher)"
                    elif flow_state == "contraire":
                        reason_f = "TRAILING TAKE PROFIT (Funding, flux contraire)"
                    elif flow_state == "favorable":
                        reason_f = "TRAILING TAKE PROFIT (Funding, apres patience)"
                    else:
                        reason_f = "TRAILING TAKE PROFIT (Funding)"
                    _result = self._safe_close_position(state, price, reason_f, ticker, pos, symbol, mode)
                    if _result is None:
                        return
                    pnl, _, trade = _result
                    self.emit("trade", trade)
                    if pnl > 0:
                        self._register_win(ticker)
                    flow_txt = f" | flux {flow_f:+.2f} ({flow_state})" if flow_f is not None else ""
                    self.emit("log", {"msg": f"[{ticker}] 💰 Funding TTP @ ${price:.4f} | pic +{peak_f:.2f}% | repli {giveback_f:.2f}% / tolerance {effective_tol:.2f}%{flow_txt} | PnL: ${pnl:.2f}", "level": "win" if pnl > 0 else "loss"})
                    self._save_open_positions()
                    self._persist_capital_snapshot()
                elif flow_state == "favorable" and giveback_f >= tolerance_pct_funding:
                    now_ts_f = time.time()
                    if now_ts_f - getattr(state, "_funding_patience_log_ts", 0) > 60:
                        state._funding_patience_log_ts = now_ts_f
                        self.emit("log", {"msg": f"[{ticker}] 💰 Funding TTP : repli {giveback_f:.2f}% depuis +{peak_f:.2f}% mais flux toujours favorable ({flow_f:+.2f}) — patience (tolerance {effective_tol:.2f}%, plancher +{min_lock_f:.2f}%)", "level": "dim"})
            return

        if state.tp_stage == 0:
            # ── Promotion directe vers le tier 1 (arm1 atteint) — desactive
            # le tier 0 au passage, le trailing principal prend le relai.
            if pnl_pct >= arm1_price_pct:
                state.tp_stage = 1
                state.peak_pnl_usd = pnl_usd
                state.tier0_armed = False
                self.emit("log", {"msg": f"[{ticker}] Prix +{pnl_pct:.2f}% (+${pnl_usd:.2f} a x{leverage}) — TTP arme (sortie si repli a +{lock1_price_pct:.2f}% de mouvement de prix)", "level": "signal"})
                # confirme sur ce cycle ; la verification de fermeture ne se
                # fera qu a partir du PROCHAIN cycle (evite une fermeture
                # instantanee si le seuil de sortie initial etait deja atteint
                # ce meme cycle).
                return

            # ── v4.11 — Tier 0 : protection anticipee pour les trades qui
            # n atteignent jamais arm1. S arme des que le prix atteint
            # tier0_arm_pct, puis trail avec une marge plus large (tier0_gap_pct).
            if not state.tier0_armed and pnl_pct >= tier0_arm_pct:
                state.tier0_armed = True
                state.tier0_peak_pnl_usd = pnl_usd
                self.emit("log", {"msg": f"[{ticker}] Prix +{pnl_pct:.2f}% (+${pnl_usd:.2f} a x{leverage}) — protection anticipee armee (sortie si repli a plus de {tier0_gap_pct:.2f}% sous le pic)", "level": "signal"})
                return

            if state.tier0_armed:
                if state.tier0_peak_pnl_usd is None or pnl_usd > state.tier0_peak_pnl_usd:
                    state.tier0_peak_pnl_usd = pnl_usd
                tier0_peak_pct = (state.tier0_peak_pnl_usd / (E * leverage) * 100) if E and leverage else 0
                tier0_lock_pct = tier0_peak_pct - tier0_gap_pct

                # v4.197 — SUR DEMANDE EXPLICITE : INVERSE la logique de
                # sortie anticipee — tant que la bougie EN COURS confirme
                # TOUJOURS la tendance (pas de retournement), on laisse
                # COURIR le trailing SANS AUCUNE limite de repli (le pic
                # peut grandir librement). Des que la couleur de bougie
                # confirme un retournement, la verification de tolerance
                # habituelle (giveback depuis le pic) prend le relais
                # normalement — remplace l ancienne sortie IMMEDIATE sur
                # simple changement de couleur (jugee trop agressive,
                # coupait des mouvements encore valides).
                # v4.315 — SUR DEMANDE EXPLICITE : meme traitement que le
                # filtre de tendance (v4.305) — pour Funding Contrarian,
                # n exige plus la confirmation de couleur de bougie avant de
                # fermer au plancher tier0. Cette confirmation peut arriver
                # TROP TARD lors d un retournement rapide : le prix franchit
                # deja le plancher de protection, mais la fermeture est
                # reportee jusqu a ce que la bougie confirme aussi — le
                # temps que les deux s alignent, le prix a parfois deja
                # atteint le SL complet. Confirme par 3 cas reels (WIF
                # +0.53%->-0.94%, SUI +0.86%->-0.82%, SUI +0.75%->-0.72%).
                color_reversed_t0 = pos.get("strategy") == "funding_contrarian" or (
                    cfg.get("EARLY_REVERSAL_EXIT_ENABLED", True) and self._candle_color_confirms_reversal(state, pos["type"])
                )
                if color_reversed_t0 and pnl_pct <= tier0_lock_pct:
                    # v4.82 — SUR DEMANDE EXPLICITE : meme filtre "la
                    # tendance tient toujours" que le tier1 (v4.77), etendu
                    # au tier0 — c est justement le tier0 (armement plus bas,
                    # 0.15-0.5% typiquement) qui produisait les sorties a
                    # pic microscopique (0.06-0.09%) signalees. Sans ce
                    # filtre, un simple repli de la marge tier0 (souvent
                    # tres fine) suffisait a fermer, meme en pleine tendance
                    # intacte.
                    # v4.305 — SUR DEMANDE EXPLICITE : DESACTIVE pour Funding
                    # Contrarian — ce mode vise un scalp rapide sur un
                    # desequilibre de funding, pas un suivi de tendance ; le
                    # laisser "courir sans limite tant que l EMA200 tient"
                    # a produit 7 allers-retours complets (pic +0.5 a +0.93%
                    # -> perte finale quasi symetrique) sur le lot du 26/09,
                    # confirmes un par un. Pour ce mode, un plancher de
                    # profit ferme (pic - marge, sans exception tendance)
                    # est desormais toujours applique.
                    trend_still_intact_t0 = False
                    if pos.get("strategy") != "funding_contrarian" and cfg.get("TTP_TREND_HOLD_FILTER_ENABLED", True):
                        ema200_hold_t0 = self._trend_ema(state)
                        if ema200_hold_t0 is not None:
                            trend_still_intact_t0 = (price > ema200_hold_t0) if pos["type"] == "long" else (price < ema200_hold_t0)
                    if trend_still_intact_t0:
                        # v4.103 — SUR DEMANDE EXPLICITE : plafond des
                        # PETITS pics retire (juge nuisible) — laisse le TTP
                        # de base et le retournement confirme gerer seuls.
                        # Le plafond des GROS pics reste actif, inchange.
                        min_peak_for_cap_t0 = cfg.get("TTP_MAX_GIVEBACK_MIN_PEAK_PCT", 2.5)
                        giveback_cap_triggered_t0 = False
                        if tier0_peak_pct >= min_peak_for_cap_t0:
                            max_giveback_pct_t0 = cfg.get("TTP_MAX_GIVEBACK_PCT_OF_PEAK", 20.0)
                            giveback_floor_t0 = tier0_peak_pct * (1 - max_giveback_pct_t0 / 100)
                            if pnl_pct <= giveback_floor_t0:
                                giveback_cap_triggered_t0 = True
                        if not giveback_cap_triggered_t0:
                            state.ttp_breach_streak = 0  # v4.154 — reinitialise la patience TTP
                            self.emit("log", {"msg": f"[{ticker}] ${price:.2f} Repli tier0 a {pnl_pct:.2f}% (verrou {tier0_lock_pct:.2f}%) mais tendance de fond toujours intacte — position maintenue", "level": "dim"})
                            self._save_open_positions()
                            return
                        # sinon : plafond de redonnage atteint — v4.154, exige aussi la confirmation patience+bougie
                        if not self._ttp_confirmed_to_close(state, pos["type"]):
                            self._save_open_positions()
                            return
                    _result = self._safe_close_position(state, price, "TRAILING TAKE PROFIT", ticker, pos, symbol, mode)

                    if _result is None:

                        return

                    pnl, _, trade = _result
                    self.emit("trade", trade)
                    self._register_win(ticker)
                    # v4.25 — apres un gain, exige une confirmation renforcee
                    # (plusieurs cycles consecutifs) avant de rouvrir dans le MEME sens.
                    if pos["type"] == "long":
                        state.post_win_confirm_long = True
                        state.confirm_count_long = 0
                    else:
                        state.post_win_confirm_short = True
                        state.confirm_count_short = 0
                    self.emit("log", {"msg": f"[{ticker}] {strat_tag}TTP SORTIE (protection anticipee) @ ${price:.2f} | pic +{tier0_peak_pct:.2f}% de mouvement (+${state.tier0_peak_pnl_usd:.2f} a x{leverage}) | PnL: +${pnl:.2f}", "level": "win"})
                    self._save_open_positions()  # sauvegarde en live ET en paper
                    self._persist_capital_snapshot()  # v4.3 - resilience crash/OOM
                    return
                else:
                    self.emit("log", {"msg": f"[{ticker}] ${price:.2f} Protection anticipee active | mouvement +{pnl_pct:.2f}% (+${pnl_usd:.2f} a x{leverage}) | pic +{tier0_peak_pct:.2f}% (sortie si repli a +{tier0_lock_pct:.2f}%)", "level": "dim"})
                    return

            # ni tier0 ni arm1 atteints : rien a faire, la fonction continue
            # (log "rien de declenche" gere plus bas dans la fonction).

        if state.tp_stage == 1:
            # v4.11 — Reactivation defensive du tier 0 si le prix retombe
            # jusqu a son seuil d armement pendant que tier 1 est actif — ne
            # devrait normalement jamais arriver tant que lock1_price_pct
            # reste superieur a tier0_arm_pct (tier 1 fermerait deja le trade
            # avant), mais protege si les seuils sont reconfigures autrement.
            # v4.120 — SUR DEMANDE EXPLICITE : marge d hysteresis ajoutee —
            # sans elle, un prix qui oscille tout pres de tier0_arm_pct
            # pouvait faire basculer tier1<->tier0 a chaque cycle (observe
            # sur WIF). Le seuil de REACTIVATION est desormais legerement
            # EN DESSOUS du seuil d ARMEMENT initial (marge de 15% par
            # defaut), creant une zone tampon qui evite le battement.
            hysteresis_margin = cfg.get("TIER0_REARM_HYSTERESIS_PCT", 15.0)
            tier0_reactivation_threshold = tier0_arm_pct * (1 - hysteresis_margin / 100)
            if pnl_pct <= tier0_reactivation_threshold:
                state.tp_stage = 0
                state.tier0_armed = True
                state.tier0_peak_pnl_usd = state.peak_pnl_usd
                self.emit("log", {"msg": f"[{ticker}] Repli net sous {tier0_reactivation_threshold:.2f}% (seuil d'armement {tier0_arm_pct:.2f}%) — retour a la protection anticipee (tier1 -> tier0)", "level": "warn"})
                return

            if state.peak_pnl_usd is None or pnl_usd > state.peak_pnl_usd:
                state.peak_pnl_usd = pnl_usd
            # Pic reconverti en % de mouvement de prix (le levier est fixe
            # pour la duree du trade, cette reconversion est donc exacte).
            peak_price_pct = (state.peak_pnl_usd / (E * leverage) * 100) if E and leverage else 0

            # v4.197 — SUR DEMANDE EXPLICITE : INVERSE la logique — retire la
            # sortie IMMEDIATE sur simple changement de couleur (jugee trop
            # agressive), remplacee par une PORTE vers la verification de
            # tolerance habituelle ci-dessous : tant que la bougie confirme
            # TOUJOURS la tendance, aucune limite de repli ne s applique (le
            # pic grandit librement) — des que la couleur confirme un
            # retournement, le TTP normal (pic - marge) reprend la main.
            color_reversed_t1 = cfg.get("EARLY_REVERSAL_EXIT_ENABLED", True) and self._candle_color_confirms_reversal(state, pos["type"])

            # v4.60 — SUR DEMANDE EXPLICITE : des l armement (tier 1), le
            # trailing devient IMMEDIATEMENT dynamique — sortie = pic - marge
            # fixe, mis a jour a chaque nouveau pic, sans attendre un second
            # palier (arm2). Exemple confirme : arme a 1%, pic 1.45% -> sortie
            # a 0.95% ; pic 2% -> sortie a 1.5%. Remplace l ancien
            # comportement (verrou fixe a lock1 tant que arm2 pas atteint).
            # Applique a Normal/Accumulation/Funding — PAS Spot-Accumulation,
            # qui a deja son propre trailing dynamique independant.
            if cfg.get("TTP_DYNAMIC_FROM_ARM1", True):
                # v4.67 — SUR DEMANDE EXPLICITE : le gap du trailing dynamique
                # peut desormais aussi etre surcharge PAR MODE (Accumulation,
                # Funding) — avant ce fix, un seul reglage global s appliquait
                # partout, empechant d ajuster le "combien on redonne depuis
                # le pic" independamment par mode, alors que le SL l etait deja.
                if pos.get("strategy") == "accumulation" and cfg.get("ACCUMULATION_TTP_DYNAMIC_TRAIL_GAP_PCT") is not None:
                    dynamic_gap_pct = cfg.get("ACCUMULATION_TTP_DYNAMIC_TRAIL_GAP_PCT")
                elif pos.get("strategy") == "funding_contrarian" and cfg.get("FUNDING_TTP_DYNAMIC_TRAIL_GAP_PCT") is not None:
                    dynamic_gap_pct = cfg.get("FUNDING_TTP_DYNAMIC_TRAIL_GAP_PCT")
                else:
                    dynamic_gap_pct = cfg.get("TTP_DYNAMIC_TRAIL_GAP_PCT", 0.5)
                current_lock_pct = peak_price_pct - dynamic_gap_pct
            # Tant que le pic n a pas atteint le 2e seuil (arm2), le seuil de
            # sortie reste fixe a lock1. Des que le pic atteint/depasse arm2,
            # le TTP se resserre en continu : sortie = pic - gap, a l infini.
            elif peak_price_pct >= arm2_price_pct:
                current_lock_pct = peak_price_pct - gap_price_pct
            else:
                current_lock_pct = lock1_price_pct

            if color_reversed_t1 and pnl_pct <= current_lock_pct:
                # v4.77 — SUR DEMANDE EXPLICITE : avant de fermer via le
                # trailing, verifie si la TENDANCE DE FOND tient toujours
                # (EMA200 dans le bon sens, verification simple et rapide —
                # pas l ADX, juste la direction a l instant present). Si la
                # tendance est toujours intacte, un simple repli de 0.5%
                # depuis le pic ne suffit pas a justifier une sortie —
                # laisse le trade courir, le pic continue d etre suivi.
                # Applique a Normal/Accumulation/Funding (pas Spot-Accum,
                # qui a son propre trailing independant).
                trend_still_intact = False  # par defaut si le filtre est desactive : comportement d origine (ferme normalement)
                # v4.305 — SUR DEMANDE EXPLICITE : meme exclusion que tier0
                # ci-dessus pour Funding Contrarian (voir commentaire tier0) —
                # plancher de profit toujours ferme, jamais de "on laisse
                # courir tant que l EMA200 tient" pour ce mode.
                if pos.get("strategy") != "funding_contrarian" and cfg.get("TTP_TREND_HOLD_FILTER_ENABLED", True):
                    ema200_hold = self._trend_ema(state)
                    if ema200_hold is not None:
                        if pos["type"] == "long":
                            trend_still_intact = price > ema200_hold
                        else:
                            trend_still_intact = price < ema200_hold
                    else:
                        trend_still_intact = False  # donnee indisponible -> ne bloque pas la sortie normale
                if trend_still_intact:
                    # v4.84/v4.85 — SUR DEMANDE EXPLICITE : plafond de
                    # redonnage maximum, MEME tendance intacte — sans ca, un
                    # trade pouvait redonner 74% de son pic (observe sur
                    # LINK) sans jamais fermer tant que le prix restait
                    # au-dessus de l EMA200. Ferme quand meme si le PnL
                    # retombe sous 20% du pic (peak * 0.80), peu importe la
                    # tendance. v4.85 : ne s applique QU AU-DELA d un pic
                    # minimum (2.5% par defaut) — sur un petit pic, ce
                    # plafond serait trop serre et genererait des sorties
                    # prematurees, allant a l encontre de la protection de
                    # tendance elle-meme. v4.103 — SUR DEMANDE EXPLICITE :
                    # le plafond des PETITS pics (v4.86) est retire — juge
                    # nuisible, laisse le TTP de base et le retournement
                    # confirme gerer seuls cette zone. Le plafond des GROS
                    # pics reste actif, inchange.
                    giveback_cap_triggered = False
                    min_peak_for_cap = cfg.get("TTP_MAX_GIVEBACK_MIN_PEAK_PCT", 2.5)
                    if peak_price_pct >= min_peak_for_cap:
                        max_giveback_pct = cfg.get("TTP_MAX_GIVEBACK_PCT_OF_PEAK", 20.0)
                        giveback_floor = peak_price_pct * (1 - max_giveback_pct / 100)
                        if pnl_pct <= giveback_floor:
                            giveback_cap_triggered = True
                    else:
                        # v4.119 — SUR DEMANDE EXPLICITE : pour les PETITS
                        # pics (< min_peak_for_cap), plafond de 50% du pic —
                        # mais UNIQUEMENT si le repli est SOUTENU sur
                        # plusieurs cycles consecutifs (pas juste un
                        # aller-retour ponctuel/bruit). Le compteur
                        # s incremente a CHAQUE appel de cette fonction, y
                        # compris sur mise a jour WebSocket en temps reel,
                        # pas seulement au cycle ~10s.
                        small_peak_giveback_pct = cfg.get("TTP_SMALL_PEAK_GIVEBACK_PCT", 50.0)
                        small_peak_giveback_min_cycles = cfg.get("TTP_SMALL_PEAK_GIVEBACK_MIN_CYCLES", 5)
                        small_giveback_floor = peak_price_pct * (1 - small_peak_giveback_pct / 100)
                        if pnl_pct <= small_giveback_floor:
                            state.small_peak_giveback_streak += 1
                        else:
                            state.small_peak_giveback_streak = 0
                        if state.small_peak_giveback_streak >= small_peak_giveback_min_cycles:
                            giveback_cap_triggered = True
                    if not giveback_cap_triggered:
                        state.ttp_breach_streak = 0  # v4.154 — reinitialise la patience TTP
                        self.emit("log", {"msg": f"[{ticker}] ${price:.2f} Repli a {pnl_pct:.2f}% (verrou {current_lock_pct:.2f}%) mais tendance de fond toujours intacte — position maintenue", "level": "dim"})
                        self._save_open_positions()
                        return
                    state.small_peak_giveback_streak = 0
                    # Plafond de redonnage atteint malgre la tendance intacte — v4.154, exige aussi la confirmation patience+bougie
                    if not self._ttp_confirmed_to_close(state, pos["type"]):
                        self._save_open_positions()
                        return
                    _result = self._safe_close_position(state, price, "TRAILING TAKE PROFIT", ticker, pos, symbol, mode)

                    if _result is None:

                        return

                    pnl, _, trade = _result
                    self.emit("trade", trade)
                    self._register_win(ticker)
                    if pos["type"] == "long":
                        state.post_win_confirm_long = True
                        state.confirm_count_long = 0
                    else:
                        state.post_win_confirm_short = True
                        state.confirm_count_short = 0
                    self.emit("log", {"msg": f"[{ticker}] {strat_tag}TTP SORTIE (plafond de redonnage {cfg.get('TTP_MAX_GIVEBACK_PCT_OF_PEAK', 20.0):.0f}% du pic atteint, tendance ignoree) @ ${price:.2f} | pic +{peak_price_pct:.2f}% | PnL: +${pnl:.2f}", "level": "win"})
                    self._save_open_positions()
                    self._persist_capital_snapshot()
                    return
                else:
                    # v4.315 — SUR DEMANDE EXPLICITE : meme bypass que pour
                    # le tier0 ci-dessus — pour Funding Contrarian, n exige
                    # plus la confirmation de couleur de bougie (patience
                    # incluse) avant de fermer au plancher tier1. C est
                    # precisement cette branche (tendance jugee "non
                    # intacte", toujours vraie pour Funding depuis v4.305)
                    # qui gerait les trades Funding — la confirmation
                    # pouvait arriver trop tard lors d un retournement rapide.
                    if pos.get("strategy") != "funding_contrarian" and not self._ttp_confirmed_to_close(state, pos["type"]):
                        self._save_open_positions()
                        return
                    _result = self._safe_close_position(state, price, "TRAILING TAKE PROFIT", ticker, pos, symbol, mode)

                    if _result is None:

                        return

                    pnl, _, trade = _result
                    self.emit("trade", trade)
                    self._register_win(ticker)
                    # v4.25 — apres un gain, exige une confirmation renforcee
                    # (plusieurs cycles consecutifs) avant de rouvrir dans le MEME sens.
                    if pos["type"] == "long":
                        state.post_win_confirm_long = True
                        state.confirm_count_long = 0
                    else:
                        state.post_win_confirm_short = True
                        state.confirm_count_short = 0
                    self.emit("log", {"msg": f"[{ticker}] {strat_tag}TTP SORTIE @ ${price:.2f} | pic +{peak_price_pct:.2f}% de mouvement (+${state.peak_pnl_usd:.2f} a x{leverage}) | PnL: +${pnl:.2f}", "level": "win"})
                    self._save_open_positions()  # sauvegarde en live ET en paper
                    self._persist_capital_snapshot()  # v4.3 - resilience crash/OOM
                    return
            else:
                state.ttp_breach_streak = 0  # v4.154 — reinitialise la patience TTP (pas de repli en cours)
                self.emit("log", {"msg": f"[{ticker}] ${price:.2f} TTP actif | mouvement +{pnl_pct:.2f}% (+${pnl_usd:.2f} a x{leverage}) | pic +{peak_price_pct:.2f}% (sortie si repli a +{current_lock_pct:.2f}%)", "level": "dim"})
                return

        # ── 4. Rien de declenche — affichage du latent ──────────────────────
        # v3.2 — FIX : ce log se declenchait a CHAQUE tick WebSocket (plusieurs
        # fois par seconde), inondant le buffer de logs et poussant hors de
        # vue les messages plus rares (collecte des autres actifs, alarmes...).
        # Limite desormais a une fois toutes les 30 secondes par actif.
        now_ts = time.time()
        if state._last_status_log_ts is None or (now_ts - state._last_status_log_ts) >= 30:
            state._last_status_log_ts = now_ts
            self.emit("log", {"msg": f"[{ticker}] ${price:.2f} {pos['type'].upper()} | latent: ${pnl_usd:+.2f} ({pnl_pct:+.2f}% de prix) | Stop Loss: -${-sl_usd:.2f} ({sl_pct_of_e:.2f}% de E) | SL secu: ${pos['sl']:.2f}", "level": "dim"})

    def _process(self, symbol, price):
        cfg   = self.cfg
        ticker = ticker_from_slot_key(symbol)   # vrai ticker API (ex: "BTC" depuis "BTC_0")
        state = self.states[symbol]
        # v4.163 — SUR DEMANDE EXPLICITE : isolation complete entre forex
        # (Normal uniquement) et crypto (tous les autres modes) — evite
        # qu Accumulation/Funding/Spot-Accum n evaluent par erreur un
        # ticker forex (marge croisee incompatible avec l exigence de
        # marge isolee des marches HIP-3), et qu Normal continue d
        # evaluer des cryptos alors qu il est desormais dedie au forex.
        is_forex_ticker = ticker in cfg.get("FOREX_MODE_SYMBOLS", [])
        forex_momentum_override = cfg.get("FOREX_LONG_TERM_MOMENTUM_MIN_CHANGE_PCT", 0.4) if is_forex_ticker else None  # v4.272
        # v4.262 — SUR DEMANDE EXPLICITE, FIX BUG CRITIQUE : deplace ici
        # depuis un bloc qui ne s executait qu une fois toutes les ~2
        # minutes (echantillonnage MTF, usage totalement different) —
        # cause reelle du probleme "jamais de donnees" signale. Ce point,
        # en tout debut de _process, s execute a CHAQUE appel — le
        # throttle de 60s deja present DANS _maybe_refresh_trade_flow
        # reste la seule limite de frequence desormais, independante de
        # tout autre mecanisme.
        self._maybe_refresh_trade_flow(ticker, state)
        if ticker == "BTC":
            print(f"[MTF-DIAG] _process ENTREE pour BTC, prix={price}, collecting={state.collecting}")
        # v3.2 — FIX : ne pas ecraser le prix avec la valeur REST (cycle,
        # potentiellement vieille de 15s) si le WebSocket est sain — il
        # fournit deja une valeur plus fraiche en continu pour les actifs en
        # position. Sans ce garde-fou, les deux sources ecrivaient
        # concurremment sur state.current_price depuis des threads
        # differents, causant un "retour en arriere" brutal et confus du
        # PnL affiche des qu un cycle REST arrivait apres plusieurs ticks
        # WebSocket plus recents.
        if not self._is_ws_healthy():
            state.current_price = price
        state.last_price_time = datetime.now()
        state.price_history.append(price)
        # v4.36 — Secours : met aussi a jour le plus haut/bas via le prix du
        # CYCLE (REST), pas seulement le WebSocket — garantit que le suivi
        # haut/bas continue meme si le WS est temporairement indisponible.
        if state.window_high is None or price > state.window_high:
            state.window_high = price
        if state.window_low is None or price < state.window_low:
            state.window_low = price

        if len(state.price_history) >= 2:
            vol = abs(list(state.price_history)[-1] - list(state.price_history)[-2])
            state.vol_history.append(vol)

        prices = list(state.price_history)
        rsi = calc_rsi(prices, cfg["RSI_PERIOD"])

        # EMA specifiques au symbole ou globales
        ema_short = cfg.get("SYMBOL_EMA_SHORT", {}).get(ticker, cfg["EMA_SHORT"])
        ema_long  = cfg.get("SYMBOL_EMA_LONG",  {}).get(ticker, cfg["EMA_LONG"])
        ema_s = calc_ema(prices, ema_short)
        ema_l = calc_ema(prices, ema_long)

        # EMA intermediaire — tendance 25-50 minutes
        # BTC/ETH : EMA50 swing (25 min) ou EMA60 scalp (30 min)
        # Prix > EMA_MID → tendance haussiere de fond → bloquer SHORT
        # Prix < EMA_MID → tendance baissiere de fond → bloquer LONG
        ema_mid_period = cfg.get("SYMBOL_EMA_MID", {}).get(ticker, cfg.get("EMA_MID_PERIOD", 50))
        ema_mid = calc_ema(prices, ema_mid_period) if ema_mid_period else None
        macd, sig = calc_macd(prices, cfg["MACD_FAST"], cfg["MACD_SLOW"], cfg["MACD_SIGNAL"])
        bb_up, bb_mid, bb_low = calc_bollinger(prices, cfg["BB_PERIOD"], cfg["BB_STD"])

        # EMA 200 multi-timeframe — 1 point tous les MTF_CANDLE_SEC (2 min par
        # defaut), pour couvrir 200 x 2min = 6h40 de tendance longue, QUEL
        # QUE SOIT le CYCLE_INTERVAL utilise pour le reste du bot (verifs
        # rapides toutes les 10s, mais la fenetre de tendance de fond doit
        # rester longue terme, independamment de cette frequence).
        # v4.13 — FIX : l ancien MTF_STEP=4 etait code en dur en supposant un
        # CYCLE_INTERVAL de 30s (4x30s=2min/point, 200x2min=6h40) — avec le
        # CYCLE_INTERVAL reel de 10s, ca ne donnait qu un point toutes les
        # 40s, soit une fenetre reelle de ~2h13 seulement (3x plus courte que
        # prevu). MTF_STEP est desormais calcule pour toujours viser
        # MTF_CANDLE_SEC secondes par point, peu importe le CYCLE_INTERVAL.
        MTF_CANDLE_SEC = 120  # 2 minutes par point -> 200 x 2min = 6h40 au total
        MTF_STEP = max(1, round(MTF_CANDLE_SEC / cfg["CYCLE_INTERVAL"]))
        # v4.39 — FIX BUG CRITIQUE : utilise desormais state.cycle_count (un
        # compteur dedie, incremente ici) plutot que len(prices) — price_history
        # est une deque PLAFONNEE (maxlen=500) dont la longueur se FIGE
        # definitivement une fois pleine (surtout si restauree deja pleine
        # depuis la sauvegarde), ce qui gelait SILENCIEUSEMENT l echantillonnage
        # (bougies ET EMA200) des que ce reste fige n etait jamais 0.
        state.cycle_count += 1
        if state.cycle_count % MTF_STEP == 0:
            state.mtf_prices.append(price)
            # v4.123 — SUR DEMANDE EXPLICITE : print() visible directement
            # dans les logs Railway (contrairement a self.emit, qui ne va
            # que vers l interface web) — diagnostic direct de l accumulation
            # MTF sans dependre d un autre panneau de l interface.
            if ticker == "BTC":
                print(f"[MTF-DIAG] BTC echantillon pris — mtf_prices={len(state.mtf_prices)}/5 requis, cycle_count={state.cycle_count}, MTF_STEP={MTF_STEP}")
            # v4.36 — Cloture de la bougie ~2min en cours : capture le plus
            # haut/bas REELLEMENT vu depuis le dernier point (alimente en
            # direct par le WebSocket entre deux echantillonnages, secours
            # REST sinon — voir _on_ws_allmids / plus haut dans _process),
            # pas juste ce point de prix isole. Sert a un vrai calcul d ATR
            # (Wilder, haut-bas-cloture) — voir plus bas.
            candle_high = state.window_high if state.window_high is not None else price
            candle_low  = state.window_low  if state.window_low  is not None else price
            state.candle_history.append((candle_high, candle_low, price))
            # v4.203 — SUR DEMANDE EXPLICITE : la bougie 1h dynamique/MACD
            # utilise desormais les VRAIES bougies 1h Hyperliquid (rafraichi
            # periodiquement, voir _maybe_refresh_dynamic_trend), plus une
            # agregation synthetique depuis nos echantillons ~2min.
            # Uniquement pour Accumulation/Spot-Accum (seuls modes
            # utilisant ce mecanisme) — pas de cout inutile pour les autres.
            if cfg.get("ACCUMULATION_ENABLED", False) or cfg.get("SPOT_ACCUM_ENABLED", True):
                self._maybe_refresh_dynamic_trend(ticker, state)
            self._maybe_refresh_5m_candles(ticker, state)  # v4.283 — tous les modes (EMA de tendance, S/R)
            # Nouvelle fenetre : redemarre le suivi haut/bas a partir de ce
            # point de cloture (qui devient l ouverture approximative de la
            # bougie suivante).
            state.window_high = price
            state.window_low  = price
        ema200 = self._trend_ema(state)
        # v4.12 — FIX FAILLE : quand l EMA200 n est pas encore calculable
        # (donnees insuffisantes, ex: juste apres un redemarrage), l ancien
        # code mettait trend_up ET trend_down a True SIMULTANEMENT — la
        # protection contre les trades a contre-tendance etait alors
        # entierement contournee (les deux sens autorises sans aucun filtre).
        # Desormais, si l EMA200 est indisponible, AUCUN des deux sens n est
        # autorise tant que la donnee n est pas fiable — plus prudent qu un
        # bypass complet, au prix d attendre la collecte plutot que de
        # trader a l aveugle sur la tendance de fond.
        trend_up   = ema200 is not None and price > ema200   # au dessus EMA200 = tendance haussiere
        trend_down = ema200 is not None and price < ema200   # en dessous EMA200 = tendance baissiere

        # v4.75 — SUR DEMANDE EXPLICITE : compteur de STABILITE de la
        # tendance — combien de cycles CONSECUTIFS trend_up/trend_down est
        # reste vrai. Utilise a l ENTREE (Spot-Accum, Accumulation) pour
        # exiger une tendance qui tient depuis un moment, pas juste vraie a
        # l instant du signal (evite d entrer juste avant un retournement
        # deja amorce — observe : pics de 0.14-0.48% suivis d un SL).
        if trend_up:
            state.trend_up_streak = getattr(state, "trend_up_streak", 0) + 1
            state.trend_down_streak = 0
        elif trend_down:
            state.trend_down_streak = getattr(state, "trend_down_streak", 0) + 1
            state.trend_up_streak = 0
        else:
            state.trend_up_streak = 0
            state.trend_down_streak = 0

        # Sauvegarde du MACD du cycle precedent pour detection crossover (Trailing TP)
        state.prev_macd = state.current_macd
        state.prev_sig  = state.current_sig

        state.current_rsi  = rsi
        state.current_macd = macd
        state.current_sig  = sig

        needed = max(cfg["RSI_PERIOD"]+1, ema_long, cfg["MACD_SLOW"]+cfg["MACD_SIGNAL"], cfg["BB_PERIOD"], cfg.get("SR_PERIOD", 0)+1, ema_mid_period or 0)
        if any(v is None for v in [rsi, ema_s, ema_l, macd, sig, bb_up]):
            state.collecting = True
            self.emit("log", {"msg": f"[{ticker}] Collecte... ({len(prices)}/{needed})", "level": "dim"})
            self._set_gate_blocked(state, price, f"collecte des indicateurs en cours ({len(prices)}/{needed} points)")
            return
        state.collecting = False

        # Stocker l ATR% courant pour affichage dashboard — calcule a chaque cycle
        # independamment de l etat (position ouverte ou non, filtre bloque ou non)
        # v4.36 — vrai calcul (haut/bas/cloture), repli sur l ancien (cloture-
        # a-cloture) tant que candle_history n a pas assez de bougies (~28 min).
        _, _atr_pct_now = calc_true_range_atr(list(state.candle_history), cfg.get("ATR_PERIOD", 14))
        if _atr_pct_now is None:
            _, _atr_pct_now = calc_atr(prices, cfg.get("ATR_PERIOD", 14))
        state.current_atr_pct = _atr_pct_now

        # v4.122 — FIX BUG CRITIQUE : ce "return" bloquait TOUTE la suite de
        # _process (y compris les verifications Accumulation/Funding/
        # Spot-Accum plus bas) des que le mode NORMAL avait deja une
        # position ouverte sur cet actif — meme schema que le bug
        # MAX_OPEN_TRADES corrige precedemment. Desormais, gere la position
        # normale si presente, mais NE bloque QUE la decision finale du
        # mode normal lui-meme (voir plus bas), laisse la fonction
        # continuer vers les autres modes, chacun avec son propre
        # emplacement independant.
        normal_already_has_position = bool(state.position)
        if normal_already_has_position:
            self._maybe_manage_position_via_cycle(symbol, price, state)

        # v4.145 — SUR DEMANDE EXPLICITE : meme secours par cycle pour
        # Accumulation (emplacement separe, self.accum_states) — sans ca,
        # si le WebSocket est indisponible, les positions Accumulation
        # n auraient RIEN pour verifier SL/TTP entre deux cycles, en plus
        # de ne jamais voir leur prix actuel mis a jour.
        accum_state_here = self.accum_states.get(symbol)
        if accum_state_here is not None and accum_state_here.position:
            self._maybe_manage_position_via_cycle(symbol, price, accum_state_here)

        # v3.2 — Le blocage "session 23h45" est retire : les nouvelles entrees
        # restent possibles jusqu a 23h59:59 UTC. Le decoupage en jours
        # calendaires UTC (00h00-23h59:59) ne sert plus qu aux statistiques
        # (voir api.py), avec attribution au jour d OUVERTURE du trade — les
        # positions ouvertes a cheval sur minuit continuent normalement
        # jusqu a leur fermeture naturelle, sans aucune interruption.

        # ── v3.2 web : max_open_trades (pilote depuis l interface) — le filtre
        #    active_coins est applique plus loin (apres le calcul de confiance),
        #    pour permettre l auto-activation d un actif inactif si une
        #    opportunite tres forte est detectee.
        # v4.111 — FIX BUG CRITIQUE : ce plafond utilisait un "return"
        # global qui interrompait TOUTE la fonction _process AVANT d
        # atteindre les verifications Accumulation/Funding/Spot-Accum plus
        # bas (chacune ayant deja son PROPRE plafond dedie, correctement
        # filtre par strategie, verifie dans _finalize_pending_*_candidates
        # — mais jamais atteint puisque la fonction s arretait ici avant
        # meme de collecter leurs candidats). Le plafond du mode normal
        # lui-meme est deja correctement applique plus tard, lors de
        # _finalize_pending_candidates (compte uniquement les positions
        # strategy=="forex"), donc AUCUNE verification supplementaire n
        # est necessaire ici — il suffisait de retirer ce blocage global.
        # ── Plage horaire — bloque les NOUVELLES entrées en paper ET en live ──
        if not is_trading_hours(cfg):
            self.emit("log", {"msg": f"[{ticker}] Hors plage horaire — aucune nouvelle entree", "level": "dim"})
            self._set_gate_blocked(state, price, "hors plage horaire de trading")
            return

        # Filtre volume
        vol_ok = True
        if len(state.vol_history) >= 10 and cfg["VOLUME_MIN_RATIO"] > 1.0:
            avg_vol = sum(list(state.vol_history)[:-1]) / (len(state.vol_history) - 1)
            cur_vol = list(state.vol_history)[-1]
            vol_ok = cur_vol >= avg_vol * cfg["VOLUME_MIN_RATIO"]

        # Filtre ATR — bloque les entrees en marche range (volatilite insuffisante)
        atr_excluded = ticker in cfg.get("ATR_EXCLUDE_SYMBOLS", [])
        if cfg.get("ATR_FILTER", False) and not atr_excluded:
            atr_period  = cfg.get("ATR_PERIOD", 14)
            # Seuil specifique au symbole ou seuil global
            atr_min_pct = cfg.get("ATR_MIN_PCT_BY_SYMBOL", {}).get(ticker,
                          cfg.get("ATR_MIN_PCT", 0.06))
            # v4.36 — FIX CALIBRAGE : l ancien calc_atr(prices,...) mesurait
            # la variation cloture-a-cloture sur des points espaces de
            # CYCLE_INTERVAL (10s) — structurellement quasi nulle, ce qui
            # bloquait la quasi-totalite des actifs en permanence (confirme
            # par les logs : "ATR X% < seuil" sur pratiquement tous les
            # actifs au meme cycle). Utilise desormais de vraies bougies
            # haut/bas/cloture (~2 min, alimentees en direct par le
            # WebSocket) — repli sur l ancien calcul tant que candle_history
            # n a pas assez de bougies (~28 min apres un demarrage/redemarrage).
            atr_pct = None
            if len(state.candle_history) >= atr_period + 1:
                _, atr_pct = calc_true_range_atr(list(state.candle_history), atr_period)
            if atr_pct is None:
                _, atr_pct = calc_atr(prices, atr_period)
            if atr_pct is not None and atr_pct < atr_min_pct:
                self.emit("log", {
                    "msg": f"[{ticker}] ATR {atr_pct:.3f}% < {atr_min_pct}% — marche en range, entree bloquee | RSI:{rsi:.1f}",
                    "level": "dim"
                })
                self._set_gate_blocked(state, price, f"ATR {atr_pct:.3f}% < seuil {atr_min_pct}% (marche en range)")
                return

        # Pour les symboles or (PAXG) — respecter les horaires Forex + periode de chauffe
        if ticker in cfg.get("FOREX_MODE_SYMBOLS", []):
            forex_now = is_forex_open()

            # Detection de la transition ferme → ouvert
            if state.forex_was_open is not None and not state.forex_was_open and forex_now:
                state.forex_reopen_time = datetime.now()
                warmup = cfg.get("FOREX_WARMUP_MINUTES", 15)
                self.emit("log", {
                    "msg": f"[{ticker}] Forex reouvert — chauffe {warmup} min avant entrees",
                    "level": "warn"
                })

            state.forex_was_open = forex_now

            if not forex_now:
                self.emit("log", {"msg": f"[{ticker}] Marche Forex ferme — {symbol} ignore", "level": "dim"})
                self._set_gate_blocked(state, price, "marche forex ferme (22h-00h01 Paris chaque nuit, et week-end)")
                return

            # Verifier si la periode de chauffe est ecoulee
            if state.forex_reopen_time is not None:
                warmup_min = cfg.get("FOREX_WARMUP_MINUTES", 15)
                elapsed = (datetime.now() - state.forex_reopen_time).total_seconds() / 60
                remaining = warmup_min - elapsed
                if remaining > 0:
                    self.emit("log", {
                        "msg": f"[{ticker}] Chauffe Forex : encore {remaining:.0f} min avant entrees — observation en cours",
                        "level": "dim"
                    })
                    self._set_gate_blocked(state, price, f"chauffe apres reouverture du forex (encore {remaining:.0f} min)")
                    return
                else:
                    # Chauffe terminee — on ne reinitialise pas forex_reopen_time
                    # pour ne pas redeclencher la chauffe au prochain cycle
                    pass
        else:
            # ── Cryptos (tout ce qui n est pas dans FOREX_SYMBOLS) ───────────
            # Heures creuses + blackout CPI : bloquent uniquement les NOUVELLES
            # entrees. Les positions deja ouvertes continuent d etre gerees
            # normalement par _manage_position. PAXG/or n est pas concerne ici
            # (deja gere ci-dessus par la logique Forex).
            # CRYPTO_OFFPEAK_ENABLED / CPI_BLACKOUT_ENABLED : bascules pilotees
            # depuis l interface web (filter_hours / filter_macro) — v3.2 web.
            if cfg.get("CRYPTO_OFFPEAK_ENABLED", True) and is_crypto_offpeak(cfg):
                self.emit("log", {
                    "msg": f"[{ticker}] Heures creuses crypto ({cfg.get('CRYPTO_OFFPEAK_HOUR_START_UTC',2)}h-{cfg.get('CRYPTO_OFFPEAK_HOUR_END_UTC',6)}h UTC) — nouvelles entrees suspendues",
                    "level": "dim"
                })
                self._set_gate_blocked(state, price, "heures creuses crypto")
                return

            if cfg.get("CPI_BLACKOUT_ENABLED", True):
                self._refresh_cpi_events_if_needed()
                cpi_blackout, cpi_event = self._is_cpi_blackout()
                if cpi_blackout:
                    self.emit("log", {
                        "msg": f"[{ticker}] Blackout CPI ({cpi_event.strftime('%d/%m %H:%M UTC')}) — nouvelles entrees suspendues",
                        "level": "warn"
                    })
                    self._set_gate_blocked(state, price, f"blackout CPI ({cpi_event.strftime('%d/%m %H:%M UTC')})")
                    return

        ema_bull = ema_s > ema_l
        ema_bear = ema_s < ema_l

        # v4.15 — Des que la condition retombe (ema_bull/ema_bear devient
        # faux) apres une fermeture dans ce sens, on considere qu un futur
        # retour a True sera un VRAI nouveau croisement, pas la continuation
        # du signal deja traite — le flag "perime" est leve.
        if state.long_signal_stale and not ema_bull:
            state.long_signal_stale = False
        if state.short_signal_stale and not ema_bear:
            state.short_signal_stale = False

        # Filtre pivot — detection du croisement EMA frais (cycle N-1 → cycle N)
        # Pour un LONG : EMA courte vient de passer AU-DESSUS de EMA longue
        # Pour un SHORT : EMA courte vient de passer EN-DESSOUS de EMA longue
        # Evite d entrer sur un croisement ancien — on veut le pivot tout frais
        require_pivot = ticker in cfg.get("PIVOT_CONFIRM_SYMBOLS", [])
        pivot_bull = True  # par defaut : pas de filtre pivot
        pivot_bear = True

        if require_pivot and state.prev_ema_s is not None and state.prev_ema_l is not None:
            prev_bull = state.prev_ema_s > state.prev_ema_l
            prev_bear = state.prev_ema_s < state.prev_ema_l
            # Croisement haussier frais : etait baissier avant, haussier maintenant
            pivot_bull = (not prev_bull) and ema_bull
            # Croisement baissier frais : etait haussier avant, baissier maintenant
            pivot_bear = (not prev_bear) and ema_bear

        # Compteur de cycles consecutifs dans le sens de la tendance EMA
        if ema_bull:
            state.consec_bull += 1
            state.consec_bear  = 0
        elif ema_bear:
            state.consec_bear += 1
            state.consec_bull  = 0
        else:
            state.consec_bull  = 0
            state.consec_bear  = 0

        # Nombre minimum de cycles consecutifs requis par symbole
        # PAXG : 2 cycles consecutifs dans le sens de la tendance avant entree
        # Evite les faux croisements EMA de courte duree sur l or
        min_consec = cfg.get("CONSEC_CONFIRM_SYMBOLS", {}).get(ticker, 1)

        # Support / Resistance — calcule desormais pour TOUS les profils
        # (avant : uniquement pour confirmer un breakout en scalp). Sert
        # aussi au nouveau filtre "marge suffisante pour Quick Profit"
        # (voir plus bas) : LONG bloque si la resistance est trop proche
        # pour laisser la place a un Quick Profit, SHORT bloque si le
        # support est trop proche.
        is_scalp = cfg.get("PROFILE") == "scalp"
        sr_period = cfg.get("SR_PERIOD", 50)
        # v4.78 — SUR DEMANDE EXPLICITE : privilegie desormais les VRAIES
        # bougies (haut/bas, jusqu a ~6h40 d historique) au lieu des prix
        # bruts echantillonnes au cycle (~8 min seulement avec les reglages
        # par defaut) — confirme visuellement (graphique Hyperliquid) que
        # l ancien calcul etait bien trop court pour representer des
        # niveaux structurels reels. Repli automatique sur l ancien calcul
        # tant que candle_history n a pas encore assez de bougies
        # accumulees (redemarrage recent).
        sr_period_candles = cfg.get("SR_PERIOD_CANDLES", 100)
        support, resistance, sr_source = self._sr_levels(state, sr_period_candles)  # v4.283 — vraies bougies 5 min
        if support is None or resistance is None:
            support, resistance = calc_support_resistance(prices, sr_period)
            sr_source = "prix bruts (repli)"
        state.sr_display = {"support": support, "resistance": resistance, "source": sr_source}

        # v4.132 — SUR DEMANDE EXPLICITE : Accumulation utilise desormais un
        # S/R calcule sur 24h (720 bougies ~2min), distinct de la fenetre
        # plus courte du mode normal (~3h20 par defaut) — repli sur le S/R
        # partage tant que candle_history n a pas encore 720 bougies
        # accumulees (redemarrage recent).
        accum_sr_period_candles = cfg.get("ACCUMULATION_SR_PERIOD_CANDLES", 720)
        support_accum, resistance_accum, _src_ac = self._sr_levels(state, accum_sr_period_candles)  # v4.283
        state.sr_display.update({"support_accum": support_accum, "resistance_accum": resistance_accum, "source_accum": _src_ac})
        if support_accum is None or resistance_accum is None:
            support_accum, resistance_accum = support, resistance

        # v4.21 — SUR DEMANDE EXPLICITE : trace des indicateurs pour affichage
        # en graphe (RSI, MACD, EMA200, ATR, support/resistance). Purement
        # informatif, n influence aucune decision — voir _process pour le
        # detail des memes calculs utilises pour trader.
        # v4.36 — vrai calcul (haut/bas/cloture), repli sur l ancien si pas
        # encore assez de bougies (~28 min apres un demarrage).
        _, atr_pct_snapshot = calc_true_range_atr(list(state.candle_history), cfg.get("ATR_PERIOD", 14))
        if atr_pct_snapshot is None:
            _, atr_pct_snapshot = calc_atr(prices, cfg.get("ATR_PERIOD", 14))
        state.indicator_history.append({
            "ts": time.time(),
            "price": price,
            "rsi": round(rsi, 2) if rsi is not None else None,
            "macd": round(macd, 6) if macd is not None else None,
            "macd_signal": round(sig, 6) if sig is not None else None,
            "ema200": round(ema200, 6) if ema200 is not None else None,
            "atr_pct": round(atr_pct_snapshot, 4) if atr_pct_snapshot is not None else None,
            "support": round(support, 6) if support is not None else None,
            "resistance": round(resistance, 6) if resistance is not None else None,
        })

        # Sauvegarder les EMA pour le prochain cycle
        state.prev_ema_s = ema_s
        state.prev_ema_l = ema_l

        # ── Filtre Momentum Instantane — "ce qui se passe MAINTENANT" ──────
        # Les EMA moyennent le passe (12-26 cycles = 6-13 min) et peuvent
        # generer un signal qui contredit le mouvement TRES recent.
        # On calcule le % de variation sur les MOMENTUM_PERIOD derniers cycles
        # (defaut 4 cycles = 2 min). Si ce mouvement recent est fortement
        # oppose au signal (au-dela de MOMENTUM_THRESHOLD_PCT), on bloque
        # l entree — "maintenant" prevaut sur la moyenne.
        # Applique a TOUS les actifs, SWING et SCALP.
        momentum_period    = cfg.get("MOMENTUM_PERIOD", 4)
        momentum_threshold = cfg.get("MOMENTUM_THRESHOLD_PCT", 0.15)
        momentum_pct = None
        if len(prices) > momentum_period:
            ref_price = prices[-(momentum_period+1)]
            if ref_price > 0:
                momentum_pct = (price - ref_price) / ref_price * 100

        # v4.16 — SUR DEMANDE EXPLICITE : le mode Accumulation tourne EN
        # PARALLELE de la logique normale ci-dessous, evalue independamment
        # a CHAQUE cycle — plus seulement en repli quand la logique normale
        # ne trouve rien. Les deux systemes peuvent donc chacun proposer un
        # candidat sur le meme actif au meme cycle (garde-fou anti-double-
        # ouverture dans _finalize_open : un seul slot par actif, le premier
        # candidat finalise gagne).
        # v4.163 — SUR DEMANDE EXPLICITE : Accumulation/Funding/Spot-Accum
        # ignorent completement les tickers forex (dedies exclusivement au
        # mode Normal, voir isolation en tete de _process).
        if not is_forex_ticker:
            self._check_accumulation_signal(
                symbol, ticker, price, support_accum, resistance_accum, rsi, momentum_pct,
                ema200, trend_up, trend_down, prices, state, self.accum_states[symbol]
            )

            # v4.33 — Mode Funding Contrarian, lui aussi EN PARALLELE, evalue
            # independamment chaque cycle — meme garde-fou anti-double-ouverture
            # dans _finalize_open (un seul slot par actif).
            self._check_funding_contrarian_signal(symbol, ticker, price, rsi, prices, state)

            # v4.43 — Mode Spot-Accumulation, lui aussi EN PARALLELE.
            # v4.196 — SUR DEMANDE EXPLICITE : utilise desormais le MEME S/R
            # 24h qu Accumulation (support_accum/resistance_accum), au lieu
            # du S/R generique ~3h20 — evite que support et resistance
            # representent deux structures de marche DIFFERENTES et
            # deconnectees (confirme par un cas reel : Accumulation signale
            # une resistance 24h pendant que Spot-Accum signale encore un
            # support ~3h20, sans rapport reel entre les deux niveaux).
            self._check_spot_accumulation_signal(symbol, ticker, price, support_accum, resistance_accum, rsi, trend_up, prices, state)
        else:
            # v4.170 — SUR DEMANDE EXPLICITE : message explicite plutot que
            # de laisser un instantane perime ("pas encore de donnees") —
            # ces modes IGNORENT volontairement le forex, ce n est pas un
            # manque de donnees. Meme message que PAXG (actif exclu).
            forex_snap = {"blocker": "actif non selectionne pour ce mode"}
            self.accum_states[symbol].accumulation_gate_snapshot = forex_snap
            state.spot_accum_gate_snapshot = forex_snap

        # v4.19 — Respect des niveaux, FUSIONNE dans la logique principale
        # (pas juste Accumulation) : un LONG a besoin d un rebond pres du
        # support OU d une cassure nette de la resistance ; un SHORT
        # l inverse. Si REQUIRE_LEVEL_RESPECT est desactive, les deux
        # variables restent True (aucune restriction ajoutee — comportement
        # d avant ce changement).
        if cfg.get("REQUIRE_LEVEL_RESPECT", True):
            # v4.209 — SUR DEMANDE EXPLICITE : remplace le % fixe par une
            # proximite relative a l ATR (volatilite reelle de l actif).
            atr_mult = cfg.get("ENTRY_ATR_PROXIMITY_MULTIPLIER", 1.0)
            long_level_ok = (
                (support is not None and price >= support and self._is_near_level_atr(state, price, support, atr_mult))
                or (resistance is not None and price > resistance)
            )
            short_level_ok = (
                (resistance is not None and price <= resistance and self._is_near_level_atr(state, price, resistance, atr_mult))
                or (support is not None and price < support)
            )
            # v4.175 — SUR DEMANDE EXPLICITE : exige AUSSI une bougie de la
            # bonne couleur (verte pour LONG pres du support, rouge pour
            # SHORT pres de la resistance) — remplace la precision de la
            # fenetre par une confirmation directe du momentum de la
            # bougie en cours.
            if cfg.get("REQUIRE_ENTRY_CANDLE_COLOR", True):
                bullish_now = self._is_candle_bullish_now(state)
                bearish_now = self._is_candle_bearish_now(state)
                if bullish_now is False:
                    long_level_ok = False
                if bearish_now is False:
                    short_level_ok = False
        else:
            long_level_ok = True
            short_level_ok = True

        # Seuils RSI specifiques au symbole
        rsi_oversold   = cfg.get("SYMBOL_RSI_OVERSOLD",  {}).get(ticker, cfg["RSI_OVERSOLD"])
        rsi_overbought = cfg.get("SYMBOL_RSI_OVERBOUGHT", {}).get(ticker, cfg["RSI_OVERBOUGHT"])

        # v3.2 — Mode RSI DETECTE AUTOMATIQUEMENT via l ADX (force de la
        # tendance), au lieu d un mode fixe par symbole :
        # "reversal" : RSI < oversold (survente) pour LONG — parie sur un
        #              retournement/oscillation. Adapte a un marche en RANGE
        #              (ADX faible — pas de direction nette soutenue).
        # "trend"    : RSI > 50 pour LONG, RSI < 50 pour SHORT — suit le
        #              momentum en cours. Adapte a un marche DIRECTIONNEL
        #              (ADX eleve — vraie tendance en cours).
        # Un override manuel via SYMBOL_RSI_MODE reste prioritaire si
        # configure explicitement pour un actif (force le mode quel que
        # soit l ADX) — sinon, detection automatique a chaque cycle.
        manual_mode = cfg.get("SYMBOL_RSI_MODE", {}).get(ticker)
        adx = calc_adx(list(state.mtf_prices) if len(state.mtf_prices) >= (cfg.get("ADX_PERIOD", 14)*2+1) else prices, cfg.get("ADX_PERIOD", 14))
        state.current_adx = adx
        if manual_mode:
            rsi_mode = manual_mode
        elif adx is not None:
            # v4.32 — SUR DEMANDE EXPLICITE, suite a une repetition de trades
            # LONG observee sur ARB : ajoute une HYSTERESIS autour du seuil
            # ADX pour eviter que le mode trend/reversal ne bascule a chaque
            # cycle des que l ADX oscille juste autour de 25 (plausible sur
            # un actif choppy) — chaque bascule change les regles d entree
            # (RSI>50 en trend vs survente/surachat en reversal), ce qui
            # peut lui-meme contribuer a l instabilite observee. Zone
            # ambigue (a +/- ADX_HYSTERESIS_MARGIN du seuil) : garde le
            # dernier mode retenu au lieu de trancher sur un seul cycle.
            adx_trend_threshold = cfg.get("ADX_TREND_THRESHOLD", 25.0)
            hysteresis_margin = cfg.get("ADX_HYSTERESIS_MARGIN", 3.0)
            if adx >= adx_trend_threshold + hysteresis_margin:
                rsi_mode = "trend"
            elif adx < adx_trend_threshold - hysteresis_margin:
                rsi_mode = "reversal"
            else:
                # Zone ambigue : conserve le dernier mode retenu (par defaut
                # le plus restrictif, "reversal", si jamais encore determine).
                rsi_mode = state.last_rsi_mode or "reversal"
            state.last_rsi_mode = rsi_mode
        else:
            # v4.32 — FIX FAILLE (meme type que EMA200/support-resistance
            # deja corriges) : quand l ADX n est pas encore calculable, le
            # repli etait "trend" — le mode le PLUS PERMISSIF des deux (RSI>50
            # suffit, contre RSI en survente/surachat pour "reversal"). Corrige
            # pour repartir sur le mode le plus restrictif par prudence, en
            # attendant une donnee fiable.
            rsi_mode = "reversal"  # ADX pas encore calculable — repli PRUDENT (etait "trend" par erreur)
        if rsi_mode == "trend":
            rsi_buy  = rsi > 50   # momentum haussier confirme
            rsi_sell = rsi < 50   # momentum baissier confirme
        else:
            rsi_buy  = rsi < rsi_oversold
            rsi_sell = rsi > rsi_overbought
        macd_bull = macd > sig
        macd_bear = macd < sig
        bb_low_ok = price <= bb_low
        bb_up_ok  = price >= bb_up

        # v4.20 — SUR DEMANDE EXPLICITE, suite a un lot de trades fouettes
        # par le bruit (pics de 0.01% a 0.66% avant SL) : deux gardes-fous
        # supplementaires, cumulatifs avec le respect des niveaux (v4.19).
        #
        # 1) CONFIRMATION DE DIRECTION : le MACD doit desormais confirmer la
        #    direction pour TOUS les actifs (pas seulement ceux listes dans
        #    SYMBOL_REQUIRE_MACD_BB) — un second indicateur independant qui
        #    doit etre d accord avec RSI+EMA, pas juste eux seuls.
        direction_confirmed_long  = True
        direction_confirmed_short = True
        if cfg.get("REQUIRE_DIRECTION_CONFIRM", True):
            direction_confirmed_long  = macd_bull
            direction_confirmed_short = macd_bear

        # 2) COHERENCE D AMPLITUDE : le mouvement de prix recent (ATR) doit
        #    etre d un ordre de grandeur coherent avec le SL/TTP configures.
        #    Trop CALME (< MIN_RATIO x SL) -> le marche n a probablement pas
        #    assez d amplitude pour atteindre le TTP, le trade stagne. Trop
        #    AGITE (> MAX_RATIO x SL) -> le bruit normal suffit a lui seul a
        #    toucher le SL avant qu un vrai mouvement ne se developpe —
        #    exactement le symptome observe (pics minuscules puis SL rapide).
        amplitude_coherent = True
        if cfg.get("REQUIRE_AMPLITUDE_COHERENCE", True):
            # v4.36 — vrai calcul (haut/bas/cloture), repli sur l ancien si
            # pas encore assez de bougies.
            _, atr_pct_now = calc_true_range_atr(list(state.candle_history), cfg.get("ATR_PERIOD", 14))
            if atr_pct_now is None:
                _, atr_pct_now = calc_atr(prices, cfg.get("ATR_PERIOD", 14))
            sl_pct_ref = cfg.get("SL_PCT_OF_E", 1.0)
            min_ratio  = cfg.get("MIN_AMPLITUDE_TO_SL_RATIO", 0.5)
            max_ratio  = cfg.get("MAX_AMPLITUDE_TO_SL_RATIO", 2.5)
            if atr_pct_now is not None:
                amplitude_coherent = (sl_pct_ref * min_ratio) <= atr_pct_now <= (sl_pct_ref * max_ratio)
            # Si l ATR n est pas encore calculable, on reste prudent et on
            # bloque (coherent avec la posture adoptee pour EMA200/support-
            # resistance : mieux vaut attendre une donnee fiable).
            else:
                amplitude_coherent = False

        # v4.37 — SUR DEMANDE EXPLICITE, desactive par defaut : bloque un
        # LONG si le support est proche ET en dessous de l EMA200 (marche
        # sans separation nette de sa moyenne longue), un SHORT si la
        # resistance est proche ET au dessus. Vise les marches en range pur,
        # ou support ET resistance s agglutinent autour de la moyenne.
        sr_ema_long_ok = True
        sr_ema_short_ok = True
        if cfg.get("REQUIRE_SR_EMA200_SEPARATION", False) and ema200 is not None and ema200 > 0:
            sr_proximity = cfg.get("SR_EMA200_PROXIMITY_PCT", 0.5)
            if support is not None and support < ema200:
                dist_support_ema200 = (ema200 - support) / ema200 * 100
                if dist_support_ema200 <= sr_proximity:
                    sr_ema_long_ok = False
            if resistance is not None and resistance > ema200:
                dist_resistance_ema200 = (resistance - ema200) / ema200 * 100
                if dist_resistance_ema200 <= sr_proximity:
                    sr_ema_short_ok = False

        long_level_ok  = long_level_ok and direction_confirmed_long and amplitude_coherent and sr_ema_long_ok
        short_level_ok = short_level_ok and direction_confirmed_short and amplitude_coherent and sr_ema_short_ok

        # v4.221 — SUR DEMANDE EXPLICITE : une CASSURE RATEE (fausse
        # cassure) est un signal fort et INDEPENDANT du RSI/MACD — bypass
        # les exigences ci-dessus (direction_confirmed, amplitude_coherent,
        # sr_ema) si detectee, evitant qu un marche en tendance forte
        # (MACD structurellement dans un seul sens) bloque INDEFINIMENT ce
        # signal local, meme quand il se produit clairement.
        failed_breakout_short = False
        failed_breakout_long = False
        if cfg.get("FAILED_BREAKOUT_DETECTION_ENABLED", True):
            if resistance is not None:
                failed_breakout_short = self._detect_failed_breakout(state, "short", resistance, cfg.get("FAILED_BREAKOUT_LOOKBACK_CANDLES", 20), ticker=ticker)
            if support is not None:
                failed_breakout_long = self._detect_failed_breakout(state, "long", support, cfg.get("FAILED_BREAKOUT_LOOKBACK_CANDLES", 20), ticker=ticker)
            if failed_breakout_short:
                short_level_ok = True
            if failed_breakout_long:
                long_level_ok = True

        # v4.227 — SUR DEMANDE EXPLICITE : etoile filante ROUGE confirmee
        # sur 30 min — signal baissier, s applique au SHORT uniquement
        # (une etoile filante n a pas d equivalent haussier logique ici).
        if cfg.get("SHOOTING_STAR_DETECTION_ENABLED", True) and self._shooting_star_confirmed(state, ticker=ticker):
            short_level_ok = True

        # v4.240 — SUR DEMANDE EXPLICITE : pression directionnelle soutenue
        # (flux de transactions reel, independant de tout niveau de prix)
        # — comble un trou identifie : une tendance DEJA engagee, loin de
        # tout support/resistance, n etait auparavant jamais capturee (ex:
        # cas reel BTC, +3.2% soutenu sans aucune reaction du bot). Bypass
        # les exigences de proximite quand une pression soutenue (~10 min)
        # confirme la direction.
        if cfg.get("TREND_PERSISTENCE_ENABLED", True):
            if self._trend_persistence_confirmed(state, "long"):
                long_level_ok = True
            if self._trend_persistence_confirmed(state, "short"):
                short_level_ok = True

        # v4.25/v4.26 — SUR DEMANDE EXPLICITE, suite a une repetition observee
        # de trades LONG sur un actif choppy (ARB : re-declenchement "frais"
        # techniquement toutes les 40-70 min, mais pas un vrai signal nouveau
        # en substance) : apres un GAIN dans un sens donne, la reouverture
        # dans ce MEME sens exige que TOUTES les conditions d entree soient
        # reunies sur PLUSIEURS CYCLES CONSECUTIFS (pas juste 1) — un signal
        # qui hesite (vrai un cycle, faux le suivant) fait repartir le
        # compteur a zero, forcant une vraie confirmation soutenue.
        # v4.26 — FIX : un signal jamais soutenu 18 cycles d affilee ne doit
        # PAS bloquer l actif indefiniment (risque reel : l actif ne trade
        # plus jamais dans ce sens). Passe un delai maximum d attente
        # (POST_WIN_MAX_WAIT_CYCLES), on consulte un second indicateur
        # independant (Bollinger — prix a un extreme de bande, confirmation
        # statistique alternative) pour trancher, puis la contrainte est
        # levee dans tous les cas — jamais bloque plus longtemps que ce delai.
        confirm_cycles_needed = cfg.get("POST_WIN_CONFIRM_CYCLES", 18)
        max_wait_cycles       = cfg.get("POST_WIN_MAX_WAIT_CYCLES", 90)

        if state.post_win_confirm_long:
            state.post_win_wait_long += 1
            if long_level_ok:
                state.confirm_count_long += 1
            else:
                state.confirm_count_long = 0

            if state.confirm_count_long >= confirm_cycles_needed:
                # Confirmation soutenue atteinte normalement — voie principale
                state.post_win_confirm_long = False
                state.confirm_count_long = 0
                state.post_win_wait_long = 0
            elif state.post_win_wait_long >= max_wait_cycles:
                # v4.27 — FIX : Bollinger devient reellement DECISIF, pas un
                # simple filtre de plus — s il confirme, il TRANCHE et
                # autorise directement (sans exiger en plus les autres
                # conditions ce cycle precis), c est justement le but de
                # "consulter un autre indicateur pour decider".
                fallback_ok = bb_low_ok  # prix a/sous la bande basse = extreme statistique favorable a un LONG
                state.post_win_confirm_long = False
                state.confirm_count_long = 0
                state.post_win_wait_long = 0
                if fallback_ok:
                    self.emit("log", {"msg": f"[{ticker}] LONG — confirmation post-gain jamais soutenue apres {max_wait_cycles} cycles, Bollinger favorable — TRANCHE, trade autorise", "level": "warn"})
                    long_level_ok = True
                else:
                    self.emit("log", {"msg": f"[{ticker}] LONG — confirmation post-gain jamais soutenue apres {max_wait_cycles} cycles, Bollinger pas plus favorable — pas de trade ce cycle, contrainte levee pour la suite", "level": "warn"})
                    long_level_ok = False
            else:
                if long_level_ok:
                    self.emit("log", {"msg": f"[{ticker}] LONG qualifie mais en attente de confirmation post-gain ({state.confirm_count_long}/{confirm_cycles_needed} cycles consecutifs, {state.post_win_wait_long}/{max_wait_cycles} max)", "level": "dim"})
                long_level_ok = False

        if state.post_win_confirm_short:
            state.post_win_wait_short += 1
            if short_level_ok:
                state.confirm_count_short += 1
            else:
                state.confirm_count_short = 0

            if state.confirm_count_short >= confirm_cycles_needed:
                state.post_win_confirm_short = False
                state.confirm_count_short = 0
                state.post_win_wait_short = 0
            elif state.post_win_wait_short >= max_wait_cycles:
                fallback_ok = bb_up_ok  # prix a/sur la bande haute = extreme statistique favorable a un SHORT
                state.post_win_confirm_short = False
                state.confirm_count_short = 0
                state.post_win_wait_short = 0
                if fallback_ok:
                    self.emit("log", {"msg": f"[{ticker}] SHORT — confirmation post-gain jamais soutenue apres {max_wait_cycles} cycles, Bollinger favorable — TRANCHE, trade autorise", "level": "warn"})
                    short_level_ok = True
                else:
                    self.emit("log", {"msg": f"[{ticker}] SHORT — confirmation post-gain jamais soutenue apres {max_wait_cycles} cycles, Bollinger pas plus favorable — pas de trade ce cycle, contrainte levee pour la suite", "level": "warn"})
                    short_level_ok = False
            else:
                if short_level_ok:
                    self.emit("log", {"msg": f"[{ticker}] SHORT qualifie mais en attente de confirmation post-gain ({state.confirm_count_short}/{confirm_cycles_needed} cycles consecutifs, {state.post_win_wait_short}/{max_wait_cycles} max)", "level": "dim"})
                short_level_ok = False

        # v4.58 — SUR DEMANDE EXPLICITE : mode SIMPLIFIE — remplace TOUT ce
        # qui precede (fraicheur, respect des niveaux, MACD/EMA200 par
        # symbole, coherence d amplitude, separation EMA200, confirmation
        # post-trade) par les 3 conditions communes aux 3 modes (tendance +
        # ADX, proximite 1-5% avec cassure en alternative, amplitude S/R
        # suffisante). Le RSI reste demande separement (rsi_buy/rsi_sell,
        # deja calcules plus haut, inchange) comme filtre leger en plus.
        # ACTIVE PAR DEFAUT — repasser a False pour retrouver l ancien
        # comportement complet (aucun code retire, juste court-circuite).
        if cfg.get("UNIFIED_SIMPLIFIED_MODE", True):
            # v4.76 — SUR DEMANDE EXPLICITE : meme stabilite de tendance que
            # Spot-Accum/Accumulation, etendue au mode normal (cluster de
            # 19 entrees groupees observe le 04/09, sans cette protection).
            normal_stability_cycles = cfg.get("FOREX_TREND_STABILITY_CYCLES", 24)
            long_level_ok = (
                self._unified_trend_confirmed(prices, trend_up, state, "trend_up_streak", normal_stability_cycles, momentum_min_change_override=forex_momentum_override)
                and self._unified_proximity_ok(price, support, resistance, "long", min_pct_override=cfg.get("FOREX_ENTRY_MIN_PCT", 5.0), max_pct_override=cfg.get("FOREX_ENTRY_MAX_PCT", 30.0))
                and self._unified_sr_amplitude_ok(support, resistance)
            )
            short_level_ok = (
                self._unified_trend_confirmed(prices, trend_down, state, "trend_down_streak", normal_stability_cycles, momentum_min_change_override=forex_momentum_override)
                and self._unified_proximity_ok(price, support, resistance, "short", min_pct_override=cfg.get("FOREX_ENTRY_MIN_PCT", 5.0), max_pct_override=cfg.get("FOREX_ENTRY_MAX_PCT", 30.0))
                and self._unified_sr_amplitude_ok(support, resistance)
            )

        # Pour certains symboles (SOL), MACD + BB sont OBLIGATOIRES pour entrer
        require_macd_bb  = ticker in cfg.get("SYMBOL_REQUIRE_MACD_BB", [])

        # Pour certains symboles (BTC), l EMA200 est OBLIGATOIRE pour confirmer la direction
        # Long uniquement si prix > EMA200 | Short uniquement si prix < EMA200
        require_ema200 = ticker in cfg.get("SYMBOL_REQUIRE_EMA200", [])

        signal = None
        reasons = []

        # v4.40 — SUR DEMANDE EXPLICITE : instantane complet de l etat de
        # TOUTES les portes d entree, capture a CHAQUE cycle (ecrase le
        # precedent) — expose a la demande via /api/entry-diagnostics/{ticker},
        # pour comprendre en direct pourquoi un actif ne trade pas, au lieu
        # de devoir chasser les bons logs dans une fenetre horaire expiree.
        # v4.58 — SUR DEMANDE EXPLICITE : detail des 3 sous-conditions
        # unifiees, pour un diagnostic fiable (les anciens messages
        # "rebond/cassure" ne refletent plus la vraie raison de blocage
        # quand UNIFIED_SIMPLIFIED_MODE est actif).
        unified_active = cfg.get("UNIFIED_SIMPLIFIED_MODE", True)
        if unified_active:
            # v4.96 — FIX BUG CRITIQUE : ce diagnostic appelait
            # _unified_trend_confirmed SANS les parametres de stabilite
            # (state, streak_attr, min_stability_cycles), alors que la VRAIE
            # decision (long_level_ok/short_level_ok, calculee plus haut) les
            # utilise depuis v4.76. Divergence : le diagnostic voyait la
            # tendance "confirmee" (verification faible, sans stabilite)
            # alors que la vraie decision la jugeait "pas assez stable" —
            # aucune des 3 raisons ne matchait, d ou "raison inconnue"
            # (observe sur SEI, AAVE). Reutilise desormais EXACTEMENT les
            # memes parametres que la decision reelle.
            trend_confirmed_long = self._unified_trend_confirmed(prices, trend_up, state, "trend_up_streak", normal_stability_cycles, momentum_min_change_override=forex_momentum_override)
            trend_confirmed_short = self._unified_trend_confirmed(prices, trend_down, state, "trend_down_streak", normal_stability_cycles, momentum_min_change_override=forex_momentum_override)
            proximity_long_ok = self._unified_proximity_ok(price, support, resistance, "long")
            proximity_short_ok = self._unified_proximity_ok(price, support, resistance, "short")
            amplitude_ok = self._unified_sr_amplitude_ok(support, resistance)
        else:
            trend_confirmed_long = trend_confirmed_short = proximity_long_ok = proximity_short_ok = amplitude_ok = None

        state.last_gate_snapshot = {
            "ts": time.time(),
            "price": price,
            "rsi": round(rsi, 2) if rsi is not None else None,
            "rsi_mode": rsi_mode,
            "rsi_buy": rsi_buy,
            "rsi_sell": rsi_sell,
            "ema_bull": ema_bull,
            "ema_bear": ema_bear,
            "trend_up": trend_up,
            "trend_down": trend_down,
            "long_signal_stale": state.long_signal_stale,
            "short_signal_stale": state.short_signal_stale,
            "unified_mode_active": unified_active,
            "unified_trend_confirmed_long": trend_confirmed_long,
            "unified_trend_confirmed_short": trend_confirmed_short,
            "unified_proximity_long_ok": proximity_long_ok,
            "unified_proximity_short_ok": proximity_short_ok,
            "unified_amplitude_ok": amplitude_ok,
            "direction_confirmed_long": direction_confirmed_long,
            "direction_confirmed_short": direction_confirmed_short,
            "amplitude_coherent": amplitude_coherent,
            "sr_ema_long_ok": sr_ema_long_ok,
            "sr_ema_short_ok": sr_ema_short_ok,
            "post_win_confirm_long": state.post_win_confirm_long,
            "post_win_confirm_short": state.post_win_confirm_short,
            "confirm_count_long": state.confirm_count_long,
            "confirm_count_short": state.confirm_count_short,
            "post_win_wait_long": state.post_win_wait_long,
            "post_win_wait_short": state.post_win_wait_short,
            "long_level_ok_final": long_level_ok,
            "short_level_ok_final": short_level_ok,
            "would_enter_long": bool(long_level_ok) if cfg.get("UNIFIED_FULL_SIMPLIFIED_MODE", True) else bool(rsi_buy and ema_bull and trend_up and not state.long_signal_stale and long_level_ok),
            "would_enter_short": bool(short_level_ok) if cfg.get("UNIFIED_FULL_SIMPLIFIED_MODE", True) else bool(rsi_sell and ema_bear and trend_down and not state.short_signal_stale and short_level_ok),
        }

        # v4.15 — diagnostics (hors chaine if/elif principale, pour ne pas la
        # perturber) : signale quand un signal qualifierait sur tous les
        # autres criteres, seule la fraicheur (croisement pas encore
        # renouvele depuis la derniere fermeture dans ce sens) le bloque.
        if rsi_buy and ema_bull and trend_up and state.long_signal_stale:
            self.emit("log", {"msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} LONG qualifie mais signal pas encore renouvele depuis la derniere fermeture — attente d une retombee puis d un nouveau croisement EMA", "level": "dim"})
        if rsi_sell and ema_bear and trend_down and state.short_signal_stale:
            self.emit("log", {"msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} SHORT qualifie mais signal pas encore renouvele depuis la derniere fermeture — attente d une retombee puis d un nouveau croisement EMA", "level": "dim"})
        if unified_active:
            # v4.58 — messages de diagnostic COHERENTS avec la vraie logique
            # active (evite le message trompeur "ni rebond ni cassure" qui
            # decrivait l ancienne logique, plus utilisee pour la decision).
            if rsi_buy and ema_bull and trend_up and not state.long_signal_stale and not long_level_ok:
                raisons = []
                if not trend_confirmed_long:
                    up_streak_ok_n = state.trend_up_streak >= normal_stability_cycles
                    if not up_streak_ok_n:
                        raisons.append(f"duree insuffisante ({state.trend_up_streak}/{normal_stability_cycles} cycles requis)")
                    else:
                        adx_diag_n = calc_adx(list(state.mtf_prices) if len(state.mtf_prices) >= (cfg.get("ADX_PERIOD", 14)*2+1) else prices, cfg.get("ADX_PERIOD", 14))
                        adx_diag_n_str = f"{adx_diag_n:.1f}" if adx_diag_n is not None else "indisponible"
                        raisons.append(f"duree OK ({state.trend_up_streak} cycles) mais ADX {adx_diag_n_str} < {cfg.get('ADX_TREND_THRESHOLD', 25.0)} (tendance pas assez forte)")
                if not proximity_long_ok:
                    raisons.append(f"hors fenetre {cfg.get('UNIFIED_MIN_ABOVE_SUPPORT_PCT', 5.0)}-{cfg.get('UNIFIED_MAX_ABOVE_SUPPORT_PCT', 10.0)}% de l'amplitude, du support (et pas de cassure)")
                if not amplitude_ok: raisons.append("fourchette S/R trop etroite")
                self.emit("log", {"msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} LONG qualifie mais base commune non reunie : {', '.join(raisons) if raisons else 'raison inconnue'}", "level": "dim"})
            if rsi_sell and ema_bear and trend_down and not state.short_signal_stale and not short_level_ok:
                raisons = []
                if not trend_confirmed_short:
                    down_streak_ok_n = state.trend_down_streak >= normal_stability_cycles
                    if not down_streak_ok_n:
                        raisons.append(f"duree insuffisante ({state.trend_down_streak}/{normal_stability_cycles} cycles requis)")
                    else:
                        adx_diag_n2 = calc_adx(list(state.mtf_prices) if len(state.mtf_prices) >= (cfg.get("ADX_PERIOD", 14)*2+1) else prices, cfg.get("ADX_PERIOD", 14))
                        adx_diag_n2_str = f"{adx_diag_n2:.1f}" if adx_diag_n2 is not None else "indisponible"
                        raisons.append(f"duree OK ({state.trend_down_streak} cycles) mais ADX {adx_diag_n2_str} < {cfg.get('ADX_TREND_THRESHOLD', 25.0)} (tendance pas assez forte)")
                if not proximity_short_ok:
                    raisons.append(f"hors fenetre {cfg.get('UNIFIED_MIN_ABOVE_SUPPORT_PCT', 5.0)}-{cfg.get('UNIFIED_MAX_ABOVE_SUPPORT_PCT', 10.0)}% de l'amplitude, de la resistance (et pas de cassure)")
                if not amplitude_ok: raisons.append("fourchette S/R trop etroite")
                self.emit("log", {"msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} SHORT qualifie mais base commune non reunie : {', '.join(raisons) if raisons else 'raison inconnue'}", "level": "dim"})
        else:
            if rsi_buy and ema_bull and trend_up and not state.long_signal_stale and not long_level_ok:
                self.emit("log", {"msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} LONG qualifie mais ni rebond sur support ni cassure de resistance — pas de raison structurelle, signal ignore", "level": "dim"})
            if rsi_sell and ema_bear and trend_down and not state.short_signal_stale and not short_level_ok:
                self.emit("log", {"msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} SHORT qualifie mais ni rebond sur resistance ni cassure de support — pas de raison structurelle, signal ignore", "level": "dim"})

        # v4.76 — SUR DEMANDE EXPLICITE : simplification COMPLETE — retire
        # RSI, croisement EMA courte/longue et fraicheur du signal de la
        # decision finale, qui ne repose plus QUE sur long_level_ok/
        # short_level_ok (deja les 3 conditions communes + stabilite
        # ci-dessus) — alignement total avec Accumulation/Spot-Accum.
        # ACTIF par defaut ; aucun code retire, juste court-circuite.
        if cfg.get("UNIFIED_FULL_SIMPLIFIED_MODE", True):
            long_entry_ok = long_level_ok
            short_entry_ok = short_level_ok
        else:
            long_entry_ok = rsi_buy and ema_bull and trend_up and not state.long_signal_stale and long_level_ok
            short_entry_ok = rsi_sell and ema_bear and trend_down and not state.short_signal_stale and short_level_ok

        # v4.163 — SUR DEMANDE EXPLICITE : le mode Normal est desormais
        # dedie exclusivement au forex — ignore completement les cryptos
        # (voir isolation inverse pour Accumulation/Funding/Spot-Accum plus
        # haut dans _process).
        # v4.331 — SUR DEMANDE EXPLICITE : exception pour les cryptos a bon
        # score (voir _forex_crypto_eligible) — le mode Forex peut les trader.
        if not (is_forex_ticker or self._forex_crypto_eligible(ticker)):
            long_entry_ok = False
            short_entry_ok = False

        # v4.155 — SUR DEMANDE EXPLICITE : le mode normal n a AUCUN outil
        # dedie pour bien trader un marche en range (contrairement a
        # Accumulation, qui dispose desormais d un mode "trader le range"
        # a part entiere) — bloque donc l entree si le marche est
        # reellement en range, meme si le mode simplifie signale un signal
        # LONG/SHORT valide. Meme principe de precaution que Spot-Accum.
        if cfg.get("FOREX_REQUIRE_ANTI_RANGE", True):
            is_ranging_normal = self._is_market_ranging(state, cfg.get("FOREX_ANTI_RANGE_MIN_PCT", 0.25), cfg.get("FOREX_ANTI_RANGE_LOOKBACK", 200))
            # v4.272 — rend ce blocage VISIBLE dans le diagnostic
            if isinstance(state.last_gate_snapshot, dict) and is_forex_ticker:
                state.last_gate_snapshot["forex_ranging"] = bool(is_ranging_normal)
                state.last_gate_snapshot["forex_range_text"] = self._anti_range_text(state, cfg.get("FOREX_ANTI_RANGE_MIN_PCT", 0.25), cfg.get("FOREX_ANTI_RANGE_LOOKBACK", 200))
            if is_ranging_normal:
                long_entry_ok = False
                short_entry_ok = False

        # v4.122 — SUR DEMANDE EXPLICITE : bloque la decision finale du mode
        # normal si une position normale est deja ouverte sur cet actif —
        # remplace l ancien "return" precoce qui bloquait aussi les autres
        # modes (voir plus haut, ou normal_already_has_position est defini).
        if normal_already_has_position:
            long_entry_ok = False
            short_entry_ok = False

        if long_entry_ok:
            # v3.2 — FIX : ce filtre ne s applique qu en mode "reversal". En
            # mode "trend" (suivi de tendance), un RSI eleve (85-97) est
            # justement la MEILLEURE confirmation du signal — pas un danger a
            # eviter. Applique sans distinction, ce filtre bloquait exactement
            # les signaux les plus forts du mode trend, contredisant sa propre
            # logique et asphyxiant le nombre de trades pris.
            rsi_extreme_high = cfg.get("RSI_EXTREME_HIGH", 85)
            if rsi_mode == "reversal" and rsi > rsi_extreme_high:
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} LONG bloque — zone de surachat extreme (RSI > {rsi_extreme_high}), risque de retournement",
                    "level": "dim"
                })
                return
            # Filtre EMA intermediaire — tendance 25-50 min
            # Bloquer LONG si prix sous EMA_MID (tendance baissiere de fond)
            if ema_mid is not None and price < ema_mid:
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} LONG bloque — prix sous EMA{ema_mid_period} ${ema_mid:.2f} (tendance baissiere 25-50 min)",
                    "level": "dim"
                })
                return
            # Filtre Momentum Instantane — bloque LONG si le marche vient
            # de chuter fortement MAINTENANT (contredit le signal haussier)
            if momentum_pct is not None and momentum_pct <= -momentum_threshold:
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} LONG bloque — momentum instantane {momentum_pct:+.2f}% (baisse en cours MAINTENANT)",
                    "level": "dim"
                })
                return
            # Filtre cycles consecutifs — PAXG : 2 cycles haussiers requis
            if state.consec_bull < min_consec:
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} signal potentiel — tendance haussiere {state.consec_bull}/{min_consec} cycles confirmes",
                    "level": "dim"
                })
                return
            # Filtre pivot — croisement EMA frais obligatoire si configure
            if require_pivot and not pivot_bull:
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} signal potentiel — pivot EMA non confirme, attente croisement frais",
                    "level": "dim"
                })
                return
            # Filtre EMA200 obligatoire : bloquer les longs si prix < EMA200
            if require_ema200 and ema200 is not None and price <= ema200:
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} LONG bloque — prix sous EMA200 (${ema200:.2f}), tendance baissiere",
                    "level": "dim"
                })
                return
            # Filtre MACD + BB obligatoire si configure pour ce symbole
            if require_macd_bb and not (macd_bull and bb_low_ok):
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} signal potentiel — MACD/BB insuffisants (obligatoires pour {symbol})",
                    "level": "dim"
                })
                return
            # Filtre Support/Resistance — SCALP uniquement
            # LONG valide seulement si le prix CASSE au-dessus de la resistance recente
            if is_scalp and resistance is not None and price <= resistance:
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} signal potentiel — prix sous resistance ${resistance:.2f}, pas de breakout",
                    "level": "dim"
                })
                return
            # Filtre Confiance — dernier filtre, score 0-100% des confirmations
            # optionnelles disponibles. Seuil dynamique par actif (v3.1).
            confidence, conf_breakdown = self._score_confidence(
                "long", macd_bull, macd_bear, bb_low_ok, bb_up_ok, vol_ok,
                ema200, ema_mid, price, momentum_pct, momentum_threshold,
                state.consec_bull, min_consec, cfg,
                support=support, resistance=resistance
            )
            if failed_breakout_long:
                confidence += getattr(state, "failed_breakout_intensity", 0.5) * cfg.get("FAILED_BREAKOUT_MAX_CONFIDENCE_BONUS", 10.0)
            conf_threshold = self._get_confidence_threshold(ticker)
            if confidence < conf_threshold:
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} LONG bloque — confiance {confidence:.0f}% < seuil requis {conf_threshold:.0f}%",
                    "level": "dim"
                })
                return
            # v4.0 — Filtre marge Resistance : bloque un LONG si la resistance
            # recente (plus haut des 50 derniers cycles) est trop proche pour
            # laisser assez de place au 1er seuil du TTP avant de s y heurter.
            # Estimation conservatrice (sans levier) : si la marge suffit pour
            # x1, elle suffit d autant plus pour un levier plus eleve. Le seuil
            # TTP etant deja exprime en % de E (mouvement de prix a x1), on
            # l utilise directement, sans conversion via CAPITAL/POSITION_PCT.
            if resistance is not None and price <= resistance and cfg.get("SR_MIN_ROOM_FILTER", True):
                min_room_pct = cfg.get("TTP_ARM1_PRICE_PCT", 1.0)
                room_pct = (resistance - price) / price * 100
                if room_pct < min_room_pct:
                    self.emit("log", {
                        "msg": f"[{ticker}] ${price:.2f} LONG bloque — resistance ${resistance:.2f} trop proche ({room_pct:.2f}% < {min_room_pct:.2f}% necessaire pour le TTP)",
                        "level": "dim"
                    })
                    return
            if not self._gate_active_or_auto_activate(ticker, confidence, "long"):
                return
            signal = "long"
            adx_str = f"ADX {adx:.0f}" if adx is not None else "ADX ?"
            reasons = [f"RSI {rsi:.1f}", f"EMA{ema_short}/{ema_long} hausse", f"{adx_str} ({rsi_mode})", f"confiance {confidence:.0f}%"]
            if resistance is not None and price > resistance:
                reasons.append(f"breakout resistance ${resistance:.2f}")
            if macd_bull:
                reasons.append("MACD hausse")
            if bb_low_ok:
                reasons.append("BB bas OK")
            if ema200:
                reasons.append(f"EMA200 ↑ ${ema200:.0f}")
            if ema_mid:
                reasons.append(f"EMA{ema_mid_period} ↑ ${ema_mid:.0f}")
            if is_scalp and resistance is not None:
                reasons.append(f"breakout R ${resistance:.2f}")
            if momentum_pct is not None:
                reasons.append(f"momentum {momentum_pct:+.2f}%")
        elif short_entry_ok:
            # v3.2 — FIX : meme correction que pour LONG — ce filtre ne
            # s applique qu en mode "reversal". En mode "trend", un RSI tres
            # bas (5-20) est la meilleure confirmation de la continuation
            # baissiere, pas un danger.
            rsi_extreme_low = cfg.get("RSI_EXTREME_LOW", 15)
            if rsi_mode == "reversal" and rsi < rsi_extreme_low:
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} SHORT bloque — zone de survente extreme (RSI < {rsi_extreme_low}), risque de rebond",
                    "level": "dim"
                })
                return
            # Filtre EMA intermediaire — tendance 25-50 min
            # Bloquer SHORT si prix au-dessus EMA_MID (tendance haussiere de fond)
            if ema_mid is not None and price > ema_mid:
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} SHORT bloque — prix au-dessus EMA{ema_mid_period} ${ema_mid:.2f} (tendance haussiere 25-50 min)",
                    "level": "dim"
                })
                return
            # Filtre Momentum Instantane — bloque SHORT si le marche vient
            # de monter fortement MAINTENANT (contredit le signal baissier)
            # C est le cas BTC observe : signal SHORT alors que le prix
            # est en hausse nette sur les derniers cycles
            if momentum_pct is not None and momentum_pct >= momentum_threshold:
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} SHORT bloque — momentum instantane {momentum_pct:+.2f}% (hausse en cours MAINTENANT)",
                    "level": "dim"
                })
                return
            # Filtre cycles consecutifs — PAXG : 2 cycles baissiers requis
            if state.consec_bear < min_consec:
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} signal potentiel — tendance baissiere {state.consec_bear}/{min_consec} cycles confirmes",
                    "level": "dim"
                })
                return
            # Filtre pivot — croisement EMA frais obligatoire si configure
            if require_pivot and not pivot_bear:
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} signal potentiel — pivot EMA non confirme, attente croisement frais",
                    "level": "dim"
                })
                return
            # Filtre EMA200 obligatoire : bloquer les shorts si prix > EMA200
            if require_ema200 and ema200 is not None and price >= ema200:
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} SHORT bloque — prix sur EMA200 (${ema200:.2f}), tendance haussiere",
                    "level": "dim"
                })
                return
            if require_macd_bb and not (macd_bear and bb_up_ok):
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} signal potentiel — MACD/BB insuffisants (obligatoires pour {symbol})",
                    "level": "dim"
                })
                return
            # Filtre Support/Resistance — SCALP uniquement
            # SHORT valide seulement si le prix CASSE en-dessous du support recent
            if is_scalp and support is not None and price >= support:
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} signal potentiel — prix au-dessus support ${support:.2f}, pas de breakout",
                    "level": "dim"
                })
                return
            # Filtre Confiance — dernier filtre, score 0-100% des confirmations
            # optionnelles disponibles. Seuil dynamique par actif (v3.1).
            confidence, conf_breakdown = self._score_confidence(
                "short", macd_bull, macd_bear, bb_low_ok, bb_up_ok, vol_ok,
                ema200, ema_mid, price, momentum_pct, momentum_threshold,
                state.consec_bear, min_consec, cfg,
                support=support, resistance=resistance
            )
            # v4.248 — SUR DEMANDE EXPLICITE : bonus de confiance
            # proportionnel a l intensite de la cassure ratee — voir
            # Accumulation pour le raisonnement complet.
            if failed_breakout_short:
                confidence += getattr(state, "failed_breakout_intensity", 0.5) * cfg.get("FAILED_BREAKOUT_MAX_CONFIDENCE_BONUS", 10.0)
            conf_threshold = self._get_confidence_threshold(ticker)
            if confidence < conf_threshold:
                self.emit("log", {
                    "msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} SHORT bloque — confiance {confidence:.0f}% < seuil requis {conf_threshold:.0f}%",
                    "level": "dim"
                })
                return
            # v4.0 — Filtre marge Support : bloque un SHORT si le support
            # recent (plus bas des 50 derniers cycles) est trop proche pour
            # laisser assez de place au 1er seuil du TTP avant de s y heurter.
            if support is not None and price >= support and cfg.get("SR_MIN_ROOM_FILTER", True):
                min_room_pct = cfg.get("TTP_ARM1_PRICE_PCT", 1.0)
                room_pct = (price - support) / price * 100
                if room_pct < min_room_pct:
                    self.emit("log", {
                        "msg": f"[{ticker}] ${price:.2f} SHORT bloque — support ${support:.2f} trop proche ({room_pct:.2f}% < {min_room_pct:.2f}% necessaire pour le TTP)",
                        "level": "dim"
                    })
                    return
            if not self._gate_active_or_auto_activate(ticker, confidence, "short"):
                return
            signal = "short"
            adx_str = f"ADX {adx:.0f}" if adx is not None else "ADX ?"
            reasons = [f"RSI {rsi:.1f}", f"EMA{ema_short}/{ema_long} baisse", f"{adx_str} ({rsi_mode})", f"confiance {confidence:.0f}%"]
            if support is not None and price < support:
                reasons.append(f"breakdown support ${support:.2f}")
            if macd_bear:
                reasons.append("MACD baisse")
            if bb_up_ok:
                reasons.append("BB haut OK")
            if ema200:
                reasons.append(f"EMA200 ↓ ${ema200:.0f}")
            if ema_mid:
                reasons.append(f"EMA{ema_mid_period} ↓ ${ema_mid:.0f}")
            if is_scalp and support is not None:
                reasons.append(f"breakout S ${support:.2f}")
            if momentum_pct is not None:
                reasons.append(f"momentum {momentum_pct:+.2f}%")
        else:
            self.emit("log", {"msg": f"[{ticker}] ${price:.2f} RSI:{rsi:.1f} ATTENDRE", "level": "dim"})
            return

        if not vol_ok and cfg["VOLUME_MIN_RATIO"] > 1.0:
            self.emit("log", {"msg": f"[{ticker}] Signal ignore - volume faible", "level": "dim"})
            return
        _q_block = self._market_quality_block(ticker, state, "forex")  # v4.281
        if _q_block:
            self.emit("log", {"msg": f"[{ticker}] Signal Forex ignore — {_q_block}", "level": "dim"})
            if isinstance(state.last_gate_snapshot, dict):
                state.last_gate_snapshot["quality_block"] = _q_block
            return

        # v3.2 — Nouvelle approche "meilleur score de confiance" : au lieu
        # d executer immediatement (ce qui favorise arbitrairement le premier
        # actif rencontre dans l ordre de balayage des 30 symboles), on met
        # ce candidat valide en file d attente. Une fois TOUS les symboles du
        # cycle evalues, _run() classe tous les candidats par confiance
        # decroissante et remplit les slots disponibles (MAX_OPEN_TRADES)
        # en priorite avec les meilleurs — les autres sont laisses de cote
        # pour ce cycle (ils resteront candidats aux cycles suivants si le
        # signal persiste).
        self._pending_candidates.append({
            "symbol": symbol, "ticker": ticker, "state": state, "signal": signal,
            "price": price, "confidence": confidence, "rsi": rsi, "rsi_mode": rsi_mode,
            "reasons": reasons, "prices": prices, "conf_breakdown": conf_breakdown,
            "support": support, "resistance": resistance,
        })

    # ─────────────────────────────────────────────────────────────────────
    #  v4.299 — MOTEUR "TOP-DOWN" MULTI-UNITES DE TEMPS
    # ─────────────────────────────────────────────────────────────────────
    _TF_SEC = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}   # v4.353 : + 1m / 5m (scalp)

    @staticmethod
    def _mtf_is_complete(sec, candles, now_ms):
        """v4.341 — la derniere bougie CLOTUREE connue est-elle bien la plus recente
        possible ? Les bougies sont contigues : apres la bougie d ouverture t, la
        suivante se cloture a t + 2 x duree - 1 ms. Si cet instant est deja passe
        (le bot recharge ~10 s apres chaque limite de periode) et que la bougie
        manque, les donnees ont du retard sur la realite. Independant de
        l alignement exact des periodes."""
        if not candles:
            return False
        return now_ms < candles[-1]["t"] + 2 * sec * 1000 - 1

    @staticmethod
    def _mtf_is_fresh(sec, candles, now_ms, grace_ms):
        """v4.341 — meme test avec une tolerance (MTF_DATA_GRACE_SEC, 90 s par
        defaut) : laisse au cycle le temps de recharger juste apres une limite de
        periode, mais signale comme PERIMEE toute serie qui n a pas ete mise a jour
        depuis. Sert a ne jamais decider sur des bougies ecartees du reel."""
        if not candles:
            return False
        return now_ms < candles[-1]["t"] + 2 * sec * 1000 - 1 + grace_ms

    def _mtf_candles(self, ticker, tf, count, cache_only=False):
        """Bougies CLOTUREES Hyperliquid (avec prix d ouverture), rechargees
        une seule fois par bougie (juste apres sa cloture).
        v4.341 — SUR DEMANDE EXPLICITE (donnees synchronisees avec la realite) :
        si la bougie qui vient de se cloturer n est pas encore dans la reponse, la
        periode n est PAS marquee comme chargee : nouvelle tentative 20 s plus tard
        (3 essais au plus par periode, puis on accepte). Auparavant, une reponse en
        retard figeait la serie jusqu a la periode suivante (jusqu a 4 h en H4).
        Un echec reseau ou une reponse vide declenchent la meme temporisation au
        lieu d un appel a chaque cycle."""
        sec = self._TF_SEC[tf]
        cache = getattr(self, "_mtf_cache", None)
        if cache is None:
            cache = self._mtf_cache = {}
        meta_all = getattr(self, "_mtf_meta", None)
        if meta_all is None:
            meta_all = self._mtf_meta = {}
        key = (ticker, tf)
        now = time.time()
        bucket = int((now - 10) // sec)
        hit = cache.get(key)
        if hit and (hit[0] == bucket or cache_only):
            return hit[1]
        out = hit[1] if hit else []
        if cache_only:
            return out
        meta = meta_all.setdefault(key, {})
        if meta.get("wait_bucket") == bucket and now < meta.get("retry_at", 0):
            return out          # temporisation en cours (donnees en retard ou echec recent)
        if meta.get("wait_bucket") != bucket:
            meta["wait_bucket"] = bucket
            meta["tries"] = 0
        meta["tries"] = meta.get("tries", 0) + 1
        retry_sec = self.cfg.get("MTF_FETCH_RETRY_SEC", 20)
        try:
            end_ms = int(now * 1000)
            raw = self.info.post("/info", {"type": "candleSnapshot", "req": {
                "coin": ticker, "interval": tf, "startTime": end_ms - (count + 1) * sec * 1000, "endTime": end_ms}})
            if isinstance(raw, list) and raw:
                out = [{"t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]), "l": float(c["l"]),
                        "c": float(c["c"]), "v": float(c.get("v", 0))} for c in raw if int(c.get("T", 0)) <= end_ms]
                complete = self._mtf_is_complete(sec, out, end_ms)
                if complete or meta["tries"] >= self.cfg.get("MTF_FETCH_MAX_TRIES", 3):
                    cache[key] = (bucket, out)
                    meta["accepted_incomplete"] = not complete
                    if not complete:
                        print(f"[MTF] {ticker} {tf} : derniere bougie cloturee toujours absente apres {meta['tries']} essais — donnees acceptees en l etat")
                else:
                    # serie recue mais en retard : on la garde, sans marquer la periode comme chargee
                    cache[key] = (hit[0] if hit else bucket - 1, out)
                    meta["retry_at"] = now + retry_sec
            else:
                meta["retry_at"] = now + retry_sec
        except Exception as e:
            meta["retry_at"] = now + retry_sec
            print(f"[MTF] Bougies {tf} {ticker} indisponibles : {e}")
        return out

    # ───────────────────────── v4.361 — SWING FOREX (sous-mode, PAPER) ─────────────────────────
    # Meme logique que le Swing crypto (tendance et zones Daily, signal de retournement H4 a la cloture, objectif sur la
    # zone opposee, gain/risque minimal), appliquee aux actifs Forex (EUR, JPY, KRW, DXY, PAXG). Sous-mode SEPARE : les
    # marches HIP-3 restent isoles des modes Spot-Accum / Accumulation (isolation v4.163). Un slot par actif, partage avec
    # le Forex normal et le Scalp. PAPER par construction. Frais et spread comptes dans le plan.
    def _fxswing_state_cfg(self):
        c = self.cfg
        return {
            "syms": c.get("FXSWING_SYMBOLS", ["xyz:EUR", "xyz:JPY", "xyz:KRW", "xyz:DXY", "PAXG"]),
            "risk_pct": float(c.get("FXSWING_RISK_PCT", 0.5)), "max_notional": float(c.get("FXSWING_MAX_NOTIONAL_USD", 60.0)),
            "lev": max(int(c.get("FXSWING_LEVERAGE", 3)), 1), "max_trades": int(c.get("FXSWING_MAX_TRADES", 2)),
            "min_risk": float(c.get("FXSWING_MIN_RISK_PCT", 0.25)), "max_risk": float(c.get("FXSWING_MAX_RISK_PCT", 2.0)),
            "min_rr": float(c.get("FXSWING_MIN_RR", 2.0)), "min_net_rr": float(c.get("FXSWING_MIN_NET_RR", 1.2)),
            "max_age": int(c.get("FXSWING_SIGNAL_MAX_AGE_SEC", 1800)), "max_spread": float(c.get("FXSWING_MAX_SPREAD_PCT", 0.05)),
            "hold_days": float(c.get("FXSWING_MAX_HOLD_DAYS", 10)), "cooldown": int(c.get("FXSWING_COOLDOWN_SEC", 14400)),
            "slip": float(c.get("FOREX_SCALP_SLIPPAGE_PCT", 0.005)), "fee": float(c.get("FEE_ROUND_TRIP_PCT", 0.09)),
        }

    def _fxswing_open_count(self):
        return sum(1 for st in list(self.states.values()) if st.position and st.position.get("strategy") == "forex_swing")

    def _fxswing_cycle(self):
        cfg = self.cfg
        if not cfg.get("FXSWING_ENABLED", 0) or self.info is None:
            return
        now = time.time()
        if now - getattr(self, "_fxswing_last_cycle", 0) < 30:
            return
        self._fxswing_last_cycle = now
        enabled_open = self.trading_enabled and cfg.get("STRATEGY_TRADING_ENABLED", {}).get("forex_swing", True)
        for ticker in self._fxswing_state_cfg()["syms"]:
            try:
                self._fxswing_eval_ticker(ticker, now, enabled_open)
            except Exception as e:
                print(f"[SWING-FX] {ticker} : evaluation ignoree : {type(e).__name__}: {e}")

    def _fxswing_side(self, ticker, v, long_side, price, spread_pct, lower, now, K):
        """Evalue UN sens. Retourne (candidat | None, motif, signal_detecte)."""
        cfg = self.cfg
        side = "long" if long_side else "short"
        ef, es = v.get("ema_fast"), v.get("ema_slow")
        want = "haussiere" if long_side else "baissiere"
        if long_side:
            pullback = ef is not None and es is not None and ef > es and price > es
        else:
            pullback = ef is not None and es is not None and ef < es and price < es
        if not (v["trend"] == want or pullback):
            return None, f"tendance Daily {v['trend']} (requis : {want})", False
        zone = v["support"] if long_side else v["resistance"]
        target = v["resistance"] if long_side else v["support"]
        if target is zone:
            target = None
        inside = v["in_support"] if long_side else v["in_resistance"]
        kind = "support" if long_side else "resistance"
        if not inside or zone is None:
            return None, f"prix hors d une zone de {kind} Daily (plus proche : {self._zone_txt(zone)})", False
        name, extreme = mtf.candle_signal(lower, side)
        if not name and cfg.get("FXSWING_DOUBLE_PATTERN", 1):
            name, extreme = mtf.double_pattern(lower, side)
        if not name:
            return None, f"prix dans la zone de {kind} Daily {self._zone_txt(zone)} — attente d un signal de bougie ou de figure H4", False
        age = now - (lower[-1]["t"] / 1000 + self._TF_SEC.get(v["lower_tf"], 14400))
        info = (zone, target, extreme, name, age)
        if age > K["max_age"]:
            if (long_side and extreme <= zone["high"] + v["tolerance"]) or (not long_side and extreme >= zone["low"] - v["tolerance"]):
                self._swing_log("forex_swing", ticker, long_side, price, zone, target, extreme, v, name, lower,
                                "filtre : signal hors fenetre d entree (trop ancien)", age=age, force=True)
            return None, f"signal H4 ({name}) trop ancien : cloture il y a {age / 60:.0f} min (maximum {K['max_age'] / 60:.0f})", True
        if (long_side and extreme > zone["high"] + v["tolerance"]) or (not long_side and extreme < zone["low"] - v["tolerance"]):
            return None, f"signal H4 ({name}) hors de la zone de {kind}", False
        flow = None
        try:
            flow = self._compute_trade_flow_pressure(ticker, price)
        except Exception:
            flow = None
        if flow is not None and cfg.get("FXSWING_FLOW_VETO", 1):
            thr = float(cfg.get("FXSWING_MIN_FLOW_PRESSURE", -0.4))
            req = thr if long_side else -thr
            if (long_side and flow < req) or ((not long_side) and flow > req):
                self._swing_log("forex_swing", ticker, long_side, price, zone, target, extreme, v, name, lower,
                                "filtre : flux de transactions hostile", flow=flow, age=age, force=True)
                return None, f"flux de transactions hostile ({flow:+.2f})", True
        plan = mtf.plan_trade(side, price, zone, target, extreme, v["atr"])
        if not plan:
            self._swing_log("forex_swing", ticker, long_side, price, zone, target, extreme, v, name, lower,
                            "filtre : plan de trade invalide", flow=flow, age=age, force=True)
            return None, "plan de trade invalide", True
        rp = plan["risk_pct"]
        if rp < K["min_risk"] or rp > K["max_risk"]:
            self._swing_log("forex_swing", ticker, long_side, price, zone, target, extreme, v, name, lower,
                            "filtre : stop trop serre" if rp < K["min_risk"] else "filtre : stop trop eloigne", plan=plan, flow=flow, age=age, force=True)
            return None, (f"stop {rp:.3f} % hors limites ({K['min_risk']}-{K['max_risk']} %)"), True
        if plan.get("tp") is None or plan.get("rr") is None:
            self._swing_log("forex_swing", ticker, long_side, price, zone, target, extreme, v, name, lower,
                            "filtre : aucun objectif reel (zone opposee)", plan=plan, flow=flow, age=age, force=True)
            return None, f"aucune zone {'de resistance' if long_side else 'de support'} Daily distincte au-dela (pas d objectif reel)", True
        cost = K["fee"] + spread_pct
        net_rr = (plan["reward_pct"] - cost) / (rp + cost)
        if plan["rr"] < K["min_rr"] or net_rr < K["min_net_rr"]:
            self._swing_log("forex_swing", ticker, long_side, price, zone, target, extreme, v, name, lower,
                            "filtre : gain/risque insuffisant", plan=plan, flow=flow, age=age, force=True)
            return None, (f"gain/risque {plan['rr']:.2f} (net de frais {net_rr:.2f}) sous le minimum "
                          f"({K['min_rr']} brut, {K['min_net_rr']} net)"), True
        key = self._swing_log("forex_swing", ticker, long_side, price, zone, target, extreme, v, name, lower,
                              "candidat (non ouvert)", prio=2, plan=plan, flow=flow, age=age, force=True)
        return {"side": side, "zone": zone, "target": target, "extreme": extreme, "name": name, "plan": plan, "flow": flow,
                "net_rr": net_rr, "key": key, "trend": v["trend"], "age": age}, None, True

    def _fxswing_eval_ticker(self, ticker, now, enabled_open):
        cfg = self.cfg
        K = self._fxswing_state_cfg()
        symbol, state = self._scalp_state_for(ticker)
        if state is None:
            return
        snap = {"ts": now, "blocker": None}
        state.fxswing_snapshot = snap
        if state.position:
            sp = state.position.get("strategy") or "?"
            snap["blocker"] = "swing FX deja ouvert sur cet actif" if sp == "forex_swing" else f"slot occupe par {sp}"
            return
        if not is_forex_open():
            snap["blocker"] = "marche Forex ferme"
            return
        price = float((self.all_mids or {}).get(ticker) or 0)
        if price <= 0:
            snap["blocker"] = "prix indisponible"
            return
        v = self.mtf_view(ticker, price, force_tf=("1d", "4h"))
        if not v["ok"]:
            snap["blocker"] = v["why"]
            return
        if not v.get("data_fresh", True):
            snap["blocker"] = "donnees Daily perimees — entree suspendue"
            return
        lower = self._mtf_candles(ticker, "4h", 60)
        if len(lower) < 10:
            snap["blocker"] = f"historique 4h insuffisant ({len(lower)} bougies)"
            return
        try:
            q = self.market_quality(ticker, state)
            spread_pct = q.get("spread_pct") if q else None
        except Exception:
            spread_pct = None
        spread_pct = spread_pct if spread_pct is not None else 0.02
        snap.update({"trend": v["trend"], "spread_pct": spread_pct,
                     "support": self._zone_txt(v["support"]) if v.get("support") else None,
                     "resistance": self._zone_txt(v["resistance"]) if v.get("resistance") else None})
        if spread_pct > K["max_spread"]:
            snap["blocker"] = f"spread trop large ({spread_pct:.3f} % > {K['max_spread']} %)"
            return
        reasons, best = [], None
        for long_side in (True, False):
            cand, why, detected = self._fxswing_side(ticker, v, long_side, price, spread_pct, lower, now, K)
            if cand is not None and (best is None or cand["plan"]["rr"] > best["plan"]["rr"]):
                best = cand
            elif why:
                reasons.append(("LONG" if long_side else "SHORT", why, detected))
        if best is None:
            det = [r for r in reasons if r[2]]
            shown = det if det else reasons          # v4.364 : montre les DEUX sens (avant : seul le premier, donc toujours le LONG)
            snap["blocker"] = " | ".join((r[0] + " : " if r[0] else "") + r[1] for r in shown) if shown else "aucun setup"
            return
        # un candidat : controles de portefeuille avant ouverture
        blocker = None
        if not enabled_open:
            blocker = "swing FX arrete (bouton Marche/Arret ou trading global)"
        elif self._fxswing_open_count() >= K["max_trades"]:
            blocker = "nombre maximal de swings FX simultanes atteint"
        elif now - getattr(state, "fx_last_loss_ts", 0) < K["cooldown"]:
            blocker = "pause apres une perte sur cet actif"
        if blocker:
            snap["blocker"] = blocker
            return
        self._fxswing_open(symbol, ticker, state, best, price, spread_pct, K)

    def _fxswing_open(self, symbol, ticker, state, cand, price, spread_pct, K):
        cfg = self.cfg
        side, plan, zone = cand["side"], cand["plan"], cand["zone"]
        fill = scalp.fill_price(side, price, spread_pct, K["slip"], "entry")
        capital = cfg.get("CAPITAL_USD", 100.0)
        rp = abs(fill - plan["sl"]) / fill * 100
        notional = max(min(capital * K["risk_pct"] / rp, K["max_notional"]), 10.0)
        size = round(notional / K["lev"], 4)
        trade_uid = make_trade_uid("forex_swing")
        state.open_position(side, fill, plan["sl"], plan["tp"], size, confidence=round(cand["net_rr"] * 10, 1), leverage=K["lev"], strategy="forex_swing")
        pos = state.position
        pos["effective_mode"] = "paper"
        pos["trade_uid"] = trade_uid
        pos["slot_key"] = symbol
        pos["engine"] = "fxswing"
        pos["mtf_swing"] = True
        pos["mtf_r"] = abs(fill - plan["sl"])
        pos["swing_info"] = {"pattern": cand["name"], "trend": cand["trend"], "zone": [zone.get("low"), zone.get("high")],
                             "touches": zone.get("touches"), "risk_pct": round(rp, 4), "rr": plan.get("rr"), "reward_pct": plan.get("reward_pct")}
        pos["fx"] = {"opened_ts": time.time(), "zone_low": zone["low"], "zone_high": zone["high"], "sl0": plan["sl"],
                     "tp": plan["tp"], "best": fill, "cost_pct": K["fee"] + spread_pct, "setup": cand["name"]}
        label = "LONG" if side == "long" else "SHORT"
        reasons = [f"🌊 Swing Forex {side} : {cand['name']} en zone {'support' if side == 'long' else 'resistance'} Daily "
                   f"{self._zone_txt(zone)} ({zone.get('touches')} contacts)",
                   f"tendance Daily {cand['trend']}", "signal H4 a la cloture",
                   f"stop {rp:.3f} % · objectif {plan['reward_pct']:.3f} % (gain/risque {plan['rr']:.2f}, net de frais {cand['net_rr']:.2f})",
                   f"spread {spread_pct:.3f} % · frais supposes {K['fee']} %"]
        try:
            quality = self.market_quality(ticker, state)
        except Exception:
            quality = {"vol_ratio": None, "activity_ratio": None}
        opened_event = {
            "vol_ratio": quality.get("vol_ratio"), "activity_ratio": quality.get("activity_ratio"), "flow_at_entry": cand.get("flow"),
            "spread_at_entry": spread_pct, "trade_uid": trade_uid, "slot_key": symbol, "is_accum_slot": False, "coin": ticker,
            "action": label, "confidence": pos.get("confidence"), "leverage": K["lev"], "position_size_pct": None,
            "size_usd": round(size, 4), "sl_pct_used": round(rp, 4), "ttp_arm1_pct_used": None, "adaptive_sl_ttp": None,
            "strategy": "forex_swing", "trade_mode": "paper", "risk_reward": round(plan["rr"], 2), "timeframe": "SWING_D1H4",
            "entry": fill, "stop_loss": plan["sl"], "take_profit1": plan["tp"], "take_profit2": plan["tp"], "rsi": None,
            "entry_reasons": " | ".join(reasons), "confidence_breakdown": json.dumps({"net_rr": round(cand["net_rr"], 3)})}
        try:
            db.upsert_open_trade(opened_event)
            opened_event["persisted"] = True
        except Exception as e:
            print(f"[SWING-FX] ecriture synchrone {trade_uid} impossible ({e}) — repli sur la file d evenements.")
        self.emit("trade_opened", opened_event)
        self.emit("log", {"msg": f"[{ticker}] 🌊 SWING FX {label} {cand['name']} @ {fill:.6g} | SL {plan['sl']:.6g} ({rp:.3f} %) | "
                                 f"TP {plan['tp']:.6g} ({plan['reward_pct']:.3f} %, R:R {plan['rr']:.2f})", "level": "signal"})
        if cand.get("key"):
            try:
                db.swing_event_mark_taken(cand["key"][0], cand["key"][1], cand["key"][2], trade_uid)
            except Exception as _e:
                print(f"[SWING-FX] marquage pris ignore : {_e}")
        self._save_open_positions()

    def _manage_fxswing(self, symbol, price, state, pos, ticker):
        K = self._fxswing_state_cfg()
        fx = pos.get("fx")
        if not fx:
            return
        long_side = pos["type"] == "long"
        sgn = 1 if long_side else -1
        entry, r = pos["entry"], pos.get("mtf_r") or abs(pos["entry"] - fx["sl0"])
        gain = sgn * (price - entry)
        fx["best"] = max(fx.get("best", entry), price) if long_side else min(fx.get("best", entry), price)
        best_gain = sgn * (fx["best"] - entry)
        pnl_usd_now = pos["size"] * pos.get("leverage", 1) * gain / entry
        if pnl_usd_now > 0 and (state.peak_pnl_usd is None or pnl_usd_now > state.peak_pnl_usd):
            state.peak_pnl_usd = pnl_usd_now
            state.absolute_peak_pnl_usd = pnl_usd_now
        reason = None
        if (long_side and price <= pos["sl"]) or ((not long_side) and price >= pos["sl"]):
            reason = "SWING FX STOP (verrou)" if fx.get("lock_done") or fx.get("be_done") else "SWING FX STOP"
        elif (long_side and price >= fx["tp"]) or ((not long_side) and price <= fx["tp"]):
            reason = "SWING FX OBJECTIF"
        elif time.time() - fx["opened_ts"] >= K["hold_days"] * 86400:
            reason = "SWING FX DUREE MAX"
        else:
            try:                                  # zone cassee a la CLOTURE Daily (bougie cloturee apres l entree)
                d1 = self._mtf_candles(ticker, "1d", 5, cache_only=True)
                if d1:
                    c = d1[-1]
                    buf = 0.3 * (mtf.atr(d1) or 0)
                    if c["t"] / 1000 + 86400 > fx["opened_ts"]:
                        if (long_side and c["c"] < fx["zone_low"] - buf) or ((not long_side) and c["c"] > fx["zone_high"] + buf):
                            reason = "SWING FX ZONE CASSEE"
            except Exception:
                pass
        if reason is None and r > 0:
            if best_gain >= 1.5 * r and not fx.get("lock_done"):
                new_sl = entry + sgn * 0.6 * r
                if (long_side and new_sl > pos["sl"]) or ((not long_side) and new_sl < pos["sl"]):
                    pos["sl"] = new_sl
                fx["lock_done"] = True
            elif best_gain >= 1.0 * r and not fx.get("be_done"):
                pad = min(entry * fx.get("cost_pct", 0.1) / 100, 0.5 * best_gain)
                new_sl = entry + sgn * pad
                if (long_side and new_sl > pos["sl"]) or ((not long_side) and new_sl < pos["sl"]):
                    pos["sl"] = new_sl
                fx["be_done"] = True
        if reason is None:
            return
        try:
            q = self.market_quality(ticker, state)
            spread_pct = (q.get("spread_pct") if q else None) or 0.02
        except Exception:
            spread_pct = 0.02
        exit_px = scalp.fill_price(pos["type"], price, spread_pct, K["slip"], "exit")
        _result = self._safe_close_position(state, exit_px, reason, ticker, pos, symbol, "paper")
        if _result is None:
            return
        pnl, _, trade = _result
        trade["entry_mechanism"] = f"SWING FX {fx.get('setup', '')}"
        if pnl < 0:
            state.fx_last_loss_ts = time.time()
        self.emit("trade", trade)
        self.emit("log", {"msg": f"[{ticker}] 🌊 {reason} @ ${exit_px:.6g} | PnL: ${pnl:.2f}", "level": "win" if pnl > 0 else "loss"})
        self._save_open_positions()

    # ───────────────────────── v4.362 — SWING SUPPORT (crypto, PAPER) ─────────────────────────
    # Tendance Daily haussiere + prix dans un support Daily + (retest tenu sur les dernieres bougies 1 h | balayage de liquidite).
    # Stop large (filet), objectif proche, SORTIE ANTICIPEE si cloture 1 h sous la zone ET flux hostile. Le contexte (BTC, Weekly,
    # funding, flux) est ENREGISTRE sans filtrer ; le journal mesure ensuite si les sorties anticipees etaient trop precoces.
    def _supsw_state_cfg(self):
        c = self.cfg
        K = {
            "syms": c.get("SUPSW_SYMBOLS", ["BTC", "ETH", "HYPE", "TAO", "SUI"]),
            "sl_pct": float(c.get("SUPSW_SL_PCT", 5.0)), "tp_pct": float(c.get("SUPSW_TP_PCT", 2.0)),
            "risk_pct": float(c.get("SUPSW_RISK_PCT", 1.0)), "max_notional": float(c.get("SUPSW_MAX_NOTIONAL_USD", 60.0)),
            "lev": max(int(c.get("SUPSW_LEVERAGE", 3)), 1), "max_trades": int(c.get("SUPSW_MAX_TRADES", 3)),
            "window": max(int(c.get("SUPSW_WINDOW", 5)), 3), "max_age": int(c.get("SUPSW_SIGNAL_MAX_AGE_SEC", 900)),
            "max_spread": float(c.get("SUPSW_MAX_SPREAD_PCT", 0.05)), "hold_days": float(c.get("SUPSW_MAX_HOLD_DAYS", 5)),
            "cooldown": int(c.get("SUPSW_COOLDOWN_SEC", 14400)), "exit_buf": float(c.get("SUPSW_EXIT_BUFFER_ATR", 0.3)),
            "exit_flow": float(c.get("SUPSW_EXIT_FLOW", -0.4)),
            "trail_on": bool(int(c.get("SUPSW_TRAIL_ON", 1))), "trail_lock": float(c.get("SUPSW_TRAIL_LOCK_PCT", 0.5)),
            "trail_pct": max(float(c.get("SUPSW_TRAIL_PCT", 0.5)), 0.1),
            "btc_on": bool(int(c.get("SUPSW_BTC_GATE_ON", 1))), "btc24_min": float(c.get("SUPSW_BTC24_MIN", 0.0)),
            "btc_h4_mode": int(c.get("SUPSW_BTC_H4_MODE", 2)),
            "rec_on": bool(int(c.get("SUPSW_RECENT_ON", 1))), "rec_h": max(int(c.get("SUPSW_RECENT_HOURS", 4)), 1),
            "rec_asset": float(c.get("SUPSW_RECENT_ASSET_MIN_PCT", 1.5)), "rec_btc": float(c.get("SUPSW_RECENT_BTC_MIN_PCT", 0.5)),
            "res_on": bool(int(c.get("SUPSW_RES_FILTER_ON", 1))), "res_min": float(c.get("SUPSW_RES_MIN_PCT", 3.0)),
            "ride_on": bool(int(c.get("SUPSW_RIDE_ON", 1))), "res_buf": float(c.get("SUPSW_RES_BUFFER_PCT", 0.3)),
            "ride_max": float(c.get("SUPSW_RIDE_MAX_PCT", 6.0)), "ride_trail": max(float(c.get("SUPSW_RIDE_TRAIL_PCT", 1.0)), 0.1),
            "re_on": bool(int(c.get("SUPSW_REENTRY_ON", 1))), "re_h": float(c.get("SUPSW_REENTRY_HOURS", 12)),
            "re_tol": float(c.get("SUPSW_REENTRY_TOL_PCT", 0.2)),
            "exit_level": int(c.get("SUPSW_EXIT_LEVEL", 1)), "exit_min": float(c.get("SUPSW_EXIT_MIN_PCT", 0.5)),
            "exit_max": float(c.get("SUPSW_EXIT_MAX_PCT", 3.0)), "need_h4_end": bool(c.get("SUPSW_EXIT_REQUIRE_H4_END", 0)),
            "slip": float(c.get("FOREX_SCALP_SLIPPAGE_PCT", 0.005)), "fee": float(c.get("FEE_ROUND_TRIP_PCT", 0.09)),
        }
        # v4.366 : stop / objectif selon le levier (desactive par defaut). La sortie anticipee reste TOUJOURS au-dessus du stop.
        if int(c.get("SUPSW_LEVER_SCALE", 0)) == 1:
            K["sl_pct"], K["tp_pct"] = sups.scaled_levels(
                K["lev"], K["sl_pct"], K["tp_pct"], float(c.get("SUPSW_MARGIN_RISK_PCT", 7.5)), float(c.get("SUPSW_MIN_SL_PCT", 1.5)),
                float(c.get("SUPSW_TP_LEV1", 2.0)), float(c.get("SUPSW_TP_LEV2", 1.5)), float(c.get("SUPSW_TP_LEV3", 1.0)))
        K["trail_lock"] = max(min(K["trail_lock"], K["tp_pct"] * 0.9), 0.0)      # le verrou reste sous l objectif
        K["exit_max"] = min(K["exit_max"], round(K["sl_pct"] * 0.8, 3))
        K["exit_min"] = min(K["exit_min"], K["exit_max"])
        return K

    def _supsw_open_count(self):
        n = 0
        for pool in (self.states, self.accum_states):
            for st in list(pool.values()):
                if st.position and st.position.get("strategy") == "swing_support":
                    n += 1
        return n

    def _supsw_prefetch_open(self):
        """Charge (hors flux WebSocket) les bougies dont la gestion des positions ouvertes a besoin : 1 h, et tendances H4 / Daily.
        La gestion lit ensuite le cache seul, pour ne jamais faire d appel reseau depuis le thread WebSocket."""
        for pool in (self.states, self.accum_states):
            for sk, st in list(pool.items()):
                if st.position and st.position.get("strategy") == "swing_support":
                    tk = ticker_from_slot_key(sk)
                    try:
                        px = float((self.all_mids or {}).get(tk) or 0) or st.position["entry"]
                        self._mtf_candles(tk, "1h", 30)
                        self.mtf_view(tk, px, force_tf=("4h", "1h"))
                        self.mtf_view(tk, px, force_tf=("1d", "1h"))
                    except Exception as e:
                        print(f"[SWING-SUP] prechargement {tk} ignore : {type(e).__name__}: {e}")

    def _supsw_cycle(self):
        cfg = self.cfg
        if self.info is None:
            return
        now = time.time()
        if now - getattr(self, "_supsw_last_cycle", 0) < 30:
            return
        self._supsw_last_cycle = now
        self._supsw_prefetch_open()          # meme si le mode vient d etre eteint : les positions ouvertes restent gerees
        try:
            self._supsw_watch_cycle(now)     # v4.378 : alertes sur les autres actifs (si demandees)
        except Exception as e:
            print(f"[SWING-SUP] surveillance ignoree : {type(e).__name__}: {e}")
        if not cfg.get("SUPSW_ENABLED", 0):
            return
        enabled_open = self.trading_enabled and cfg.get("STRATEGY_TRADING_ENABLED", {}).get("swing_support", True)
        for ticker in self._supsw_state_cfg()["syms"]:
            try:
                self._supsw_eval_ticker(ticker, now, enabled_open)
            except Exception as e:
                print(f"[SWING-SUP] {ticker} : evaluation ignoree : {type(e).__name__}: {e}")

    def _supsw_reserved(self, ticker):
        """v4.375 : True si cet actif est reserve a Swing Support (liste SUPSW_SYMBOLS) et doit donc etre ignore par
        Tendance, Accumulation et Spot-Accum. Sans effet si Swing Support est eteint ou si la reservation est coupee."""
        try:
            c = self.cfg
            if not c.get("SUPSW_ENABLED", 0) or not int(c.get("SUPSW_RESERVE_ASSETS", 1)):
                return False
            return str(ticker) in set(c.get("SUPSW_SYMBOLS", ["BTC", "ETH", "HYPE", "TAO", "SUI"]))
        except Exception:
            return False

    def _supsw_context(self, ticker, v, price):
        """Contexte ENREGISTRE (jamais filtrant) : tendance de BTC, tendance Weekly, funding annualise."""
        ctx = {"btc_trend": None, "weekly_trend": None, "funding_ann": None, "btc_chg24": None, "btc_h4": None, "h4_trend": None,
               "res_low": None, "res_dist_pct": None, "chg_recent": None, "btc_chg_recent": None}
        try:      # v4.392 : variation sur les dernieres heures (fenetre SUPSW_RECENT_HOURS) de l actif et de BTC, sur bougies 1 h cloturees
            n = max(int(self.cfg.get("SUPSW_RECENT_HOURS", 4)), 1)
            for key, tk in (("chg_recent", ticker), ("btc_chg_recent", "BTC")):
                cc = self._mtf_candles(tk, "1h", 30, cache_only=True)
                if cc and len(cc) > n and float(cc[-1 - n]["c"]) > 0:
                    ctx[key] = round((float(cc[-1]["c"]) / float(cc[-1 - n]["c"]) - 1) * 100, 3)
        except Exception:
            pass
        try:      # v4.391 : prochaine resistance Daily AU-DESSUS de la zone de support et du prix (distance en % du prix)
            sup = v.get("support") or {}
            zs = mtf.find_zones(self._mtf_candles(ticker, "1d", 230, cache_only=True), lookback=self.cfg.get("MTF_ZONE_LOOKBACK", 120),
                                max_width_atr=(self.cfg.get("MTF_ZONE_MAX_WIDTH_ATR", 0) or None))
            lim = max(float(sup.get("high") or 0), float(price or 0))
            above = [z for z in zs if z["low"] > lim]
            if above and price:
                r = min(above, key=lambda z: z["low"])
                ctx["res_low"] = round(float(r["low"]), 8)
                ctx["res_dist_pct"] = round((float(r["low"]) / float(price) - 1) * 100, 3)
        except Exception:
            pass
        try:      # v4.387 : contexte de marche enregistre (jamais filtrant) : BTC sur 24 h, tendance H4 de BTC et de l actif
            cb = self._mtf_candles("BTC", "1h", 30, cache_only=True)
            if cb and len(cb) >= 25 and float(cb[-25]["c"]) > 0:
                ctx["btc_chg24"] = round((float(cb[-1]["c"]) / float(cb[-25]["c"]) - 1) * 100, 3)
        except Exception:
            pass
        try:
            bpx4 = float((self.all_mids or {}).get("BTC") or 0)
            if bpx4 > 0:
                b4 = self.mtf_view("BTC", bpx4, cache_only=True, force_tf=("4h", "1h"))
                ctx["btc_h4"] = b4.get("trend") if b4.get("ok") else None
            if price:
                a4 = self.mtf_view(ticker, float(price), cache_only=True, force_tf=("4h", "1h"))
                ctx["h4_trend"] = a4.get("trend") if a4.get("ok") else None
        except Exception:
            pass
        try:
            if ticker == "BTC":
                ctx["btc_trend"] = v.get("trend")
            else:
                bpx = float((self.all_mids or {}).get("BTC") or 0)
                if bpx > 0:
                    bv = self.mtf_view("BTC", bpx, cache_only=True, force_tf=("1d", "1h"))
                    ctx["btc_trend"] = bv.get("trend") if bv.get("ok") else None
        except Exception:
            pass
        try:
            ctx["weekly_trend"] = sups.weekly_trend(self._mtf_candles(ticker, "1d", 230, cache_only=True))
        except Exception:
            pass
        try:
            hr = (getattr(self, "funding_rates", None) or {}).get(ticker)
            if hr is not None:
                ctx["funding_ann"] = round(float(hr) * 24 * 365 * 100, 2)
        except Exception:
            pass
        return ctx

    def _supsw_log(self, ticker, c1h, v, setup, ctx, price, flow, decision, prio, K):
        """Journalise UN signal 1 h et la decision. Une ligne par (actif, bougie 1 h). Retourne (event_id, signal_t) ou None."""
        try:
            signal_t = int(c1h[-1]["t"] + 3_600_000)
            seen = getattr(self, "_supsw_seen", None)
            if seen is None:
                seen = self._supsw_seen = {}
            key = (ticker, signal_t)
            if seen.get(key, (0, None))[0] >= prio:
                return seen[key][1], signal_t
            now = time.time()
            for k in [k for k in list(seen) if k[1] < (now - 8 * 3600) * 1000]:
                seen.pop(k, None)
            ref = float(c1h[-1]["c"])
            zone = v["support"] or {}
            eid = db.supsw_upsert({
                "ts": int(now * 1000), "signal_t": signal_t, "asset": ticker, "side": "long", "setup": setup["name"] if setup else None,
                "setup_kind": setup["kind"] if setup else None, "trend": v.get("trend"), "weekly_trend": ctx.get("weekly_trend"),
                "btc_trend": ctx.get("btc_trend"), "funding_ann": ctx.get("funding_ann"), "flow": flow,
                "btc_chg24": ctx.get("btc_chg24"), "btc_h4": ctx.get("btc_h4"), "h4_trend": ctx.get("h4_trend"),
                "res_low": ctx.get("res_low"), "res_dist_pct": ctx.get("res_dist_pct"),
                "chg_recent": ctx.get("chg_recent"), "btc_chg_recent": ctx.get("btc_chg_recent"),
                "atr_pct": (v["atr"] / price * 100) if price else None, "hour_utc": datetime.utcfromtimestamp(now).hour,
                "zone_low": zone.get("low"), "zone_high": zone.get("high"), "zone_touches": zone.get("touches"),
                "held": setup.get("held") if setup else None, "pierce_pct": setup.get("pierce_pct") if setup else None,
                "price": price, "entry_ref": ref, "sl": ref * (1 - K["sl_pct"] / 100), "tp": ref * (1 + K["tp_pct"] / 100),
                "risk_pct": K["sl_pct"], "reward_pct": K["tp_pct"], "decision": decision, "prio": prio, "trade_uid": None})
            seen[key] = (prio, eid)
            return eid, signal_t
        except Exception as e:
            print(f"[SWING-SUP] journal ignore : {type(e).__name__}: {e}")
            return None

    def _supsw_eval_ticker(self, ticker, now, enabled_open, watch=False):
        cfg = self.cfg
        K = self._supsw_state_cfg()
        symbol, state = self._scalp_state_for(ticker)
        if state is None:
            return
        snap = {"ts": now, "blocker": None}
        if not watch:                      # v4.378 : surveillance d alertes (actifs hors liste) : aucun effet sur la carte ni le journal
            state.supsw_snapshot = snap
        occ = None
        for pool in (self.states, self.accum_states):
            st = pool.get(symbol)
            if st is not None and st.position:
                occ = st.position.get("strategy") or "?"
                break
        if occ is not None:
            snap["blocker"] = "swing support deja ouvert sur cet actif" if occ == "swing_support" else f"actif deja ouvert dans un autre mode ({occ})"
            return
        price = float((self.all_mids or {}).get(ticker) or 0)
        if price <= 0:
            snap["blocker"] = "prix indisponible"
            return
        v = self.mtf_view(ticker, price, force_tf=("1d", "1h"))
        if not v["ok"]:
            snap["blocker"] = v["why"]
            return
        if not v.get("data_fresh", True):
            snap["blocker"] = "donnees Daily perimees — entree suspendue"
            return
        ef, es = v.get("ema_fast"), v.get("ema_slow")
        pullback = ef is not None and es is not None and ef > es and price > es
        snap["trend"] = v["trend"]
        snap["support"] = self._zone_txt(v["support"]) if v.get("support") else None
        if not (v["trend"] == "haussiere" or pullback):
            snap["blocker"] = f"tendance Daily {v['trend']} (requis : haussiere)"
            return
        zone = v["support"]
        if zone is None or not v["in_support"]:
            snap["blocker"] = f"prix hors d un support Daily (plus proche : {self._zone_txt(zone)})"
            return
        c1h = self._mtf_candles(ticker, "1h", 60)
        if len(c1h) < K["window"] + 8:
            snap["blocker"] = f"historique 1h insuffisant ({len(c1h)} bougies)"
            return
        setup, why = sups.detect_setup(c1h, zone, v["tolerance"], K["window"])
        if setup is None:
            snap["blocker"] = f"dans le support {self._zone_txt(zone)} — {why}"
            return
        snap["setup"] = setup["name"]
        ctx = self._supsw_context(ticker, v, price)
        flow = None
        try:
            flow = self._compute_trade_flow_pressure(ticker, price)
        except Exception:
            flow = None
        age = now - (c1h[-1]["t"] / 1000 + 3600)
        try:
            q = self.market_quality(ticker, state)
            spread_pct = q.get("spread_pct") if q else None
        except Exception:
            spread_pct = None
        spread_pct = spread_pct if spread_pct is not None else 0.02
        blocker = None
        if age > K["max_age"]:
            blocker = f"signal 1 h trop ancien ({age / 60:.0f} min, maximum {K['max_age'] / 60:.0f})"
        elif spread_pct > K["max_spread"]:
            blocker = f"spread trop large ({spread_pct:.3f} % > {K['max_spread']} %)"
        elif not enabled_open:
            blocker = "swing support arrete (bouton Marche/Arret ou trading global)"
        elif self._supsw_open_count() >= K["max_trades"]:
            blocker = "nombre maximal de swings support simultanes atteint"
        elif now - getattr(state, "supsw_last_loss_ts", 0) < K["cooldown"]:
            blocker = "pause apres une perte sur cet actif"
        elif K["btc_on"] and ctx.get("btc_chg24") is not None and ctx["btc_chg24"] < K["btc24_min"]:
            blocker = f"BTC en baisse sur 24 h ({ctx['btc_chg24']:+.2f} %, minimum {K['btc24_min']:+g} %)"
        elif K["btc_on"] and K["btc_h4_mode"] >= 1 and ctx.get("btc_h4") is not None and (
                ctx["btc_h4"] == "baissiere" or (K["btc_h4_mode"] >= 2 and ctx["btc_h4"] != "haussiere")):
            blocker = f"tendance H4 de BTC {ctx['btc_h4']}" + (" (haussiere exigee)" if K["btc_h4_mode"] >= 2 else "")
        elif K["rec_on"] and ctx.get("chg_recent") is not None and ctx["chg_recent"] < -K["rec_asset"]:
            blocker = f"chute en cours : actif {ctx['chg_recent']:+.2f} % sur {K['rec_h']} h"
        elif K["rec_on"] and ctx.get("btc_chg_recent") is not None and ctx["btc_chg_recent"] < -K["rec_btc"]:
            blocker = f"chute en cours : BTC {ctx['btc_chg_recent']:+.2f} % sur {K['rec_h']} h"
        elif K["res_on"] and ctx.get("res_dist_pct") is not None and ctx["res_dist_pct"] < K["res_min"]:
            blocker = (f"resistance Daily trop proche ({ctx['res_dist_pct']:.1f} % au-dessus, minimum {K['res_min']:g} %) "
                       f"— retournement probable")
        else:
            blocker = self._supsw_reentry_blocker(ticker, c1h, price, now, K)
        if watch:                          # v4.378 : actif hors des 5 du mode -> alerte seulement, jamais d ouverture ni de journal
            if age <= K["max_age"] and spread_pct <= K["max_spread"]:
                self._supsw_alert(ticker, v, ctx, zone, setup, price, flow, None, True)
            return
        # v4.376 : alerte telephone « Swing Support » (si coche dans Trading Manuel) — setup valide, hors signal perime ou spread trop large
        if not (blocker and (blocker.startswith("signal 1 h trop ancien") or blocker.startswith("spread"))):
            self._supsw_alert(ticker, v, ctx, zone, setup, price, flow, blocker, False)
        if blocker:
            snap["blocker"] = f"{setup['kind']} detecte — {blocker}"
            self._supsw_log(ticker, c1h, v, setup, ctx, price, flow, f"refus : {blocker.split(' (')[0]}", 1, K)
            return
        res = self._supsw_log(ticker, c1h, v, setup, ctx, price, flow, "candidat (non ouvert)", 2, K)
        self._supsw_open(symbol, ticker, state, setup, ctx, v, zone, price, spread_pct, flow, res, K)

    def _supsw_alert(self, ticker, v, ctx, zone, setup, price, flow, blocker, watch_only):
        """v4.378 : alerte telephone d un setup Swing Support valide, avec sa note de qualite (0-100) filtree par reglage."""
        push = getattr(self, "push", None)
        try:
            if push is None or not push.setup_ready("swing_support", ticker, "long"):
                return
            mt = getattr(self, "manual", None)
            if mt is not None and mt.ticker_busy(ticker):      # v4.382 : actif deja occupe (y compris par un trade manuel)
                return
            h4_up = False
            try:
                bv = self.mtf_view(ticker, price, force_tf=("4h", "1h"))
                h4_up = bool(bv.get("ok") and bv.get("trend") == "haussiere")
            except Exception:
                pass
            score, why = sups.quality_score(v["trend"], ctx.get("weekly_trend"), ctx.get("btc_trend"), h4_up,
                                            zone.get("touches", 1), flow, ctx.get("funding_ann"))
            if watch_only:
                status = "actif hors des 5 du mode : alerte seulement"
            else:
                status = "ouvert par le bot (Paper)" if not blocker else f"le bot n'a pas ouvert ce trade ({blocker.split(' (')[0]})"
            push.on_setup("swing_support", ticker, "long", f"🧱 Swing Support · 📈 LONG (achat) {ticker} · note {score}/100 : {setup['kind']} détecté",
                          f"{setup['name']} · support {self._zone_txt(zone)} · prix {price:.6g} · {why} · {status}", score=score, price=price)
        except Exception as e:
            print(f"[SWING-SUP] alerte ignoree : {type(e).__name__}: {e}")

    def _supsw_watch_cycle(self, now):
        """v4.378 : cherche des setups Swing Support sur TOUS les autres actifs crypto (alerte seulement), 6 actifs par passage."""
        push = getattr(self, "push", None)
        if push is None or not push.watch_wanted("swing_support"):
            return
        cfg = self.cfg
        own = set(cfg.get("SUPSW_SYMBOLS", ["BTC", "ETH", "HYPE", "TAO", "SUI"]))
        fx = set(cfg.get("FOREX_SYMBOLS", []))
        uni = [t for t in cfg.get("SYMBOLS", []) if t not in own and t not in fx and not str(t).startswith("xyz:")]
        if not uni:
            return
        i = getattr(self, "_supsw_watch_i", 0) % len(uni)
        n = min(6, len(uni))
        self._supsw_watch_i = (i + n) % len(uni)
        for t in [uni[(i + k) % len(uni)] for k in range(n)]:
            try:
                self._supsw_eval_ticker(t, now, True, watch=True)
            except Exception as e:
                print(f"[SWING-SUP] surveillance {t} ignoree : {type(e).__name__}: {e}")

    def _supsw_reentry_blocker(self, ticker, c1h, price, now, K):
        """v4.389 : apres un trade GAGNANT sur cet actif (dans les N dernieres heures) : on reprend si le prix continue de monter
        (>= prix de sortie - tolerance) ; s il a baisse, on attend la fin de la baisse (point bas fait depuis >= 2 bougies 1 h et
        derniere bougie 1 h verte). Retourne le texte du refus, ou None."""
        if not K["re_on"]:
            return None
        try:
            last = db.supsw_last_exit(ticker)
        except Exception:
            return None
        if not last or (last.get("pnl_pct") or 0) <= 0 or not last.get("exit_price"):
            return None
        t_exit = (last.get("exit_ts") or 0) / 1000
        if now - t_exit > K["re_h"] * 3600:
            return None
        xp = float(last["exit_price"])
        if price >= xp * (1 - K["re_tol"] / 100):
            return None                                        # ca continue de monter
        cs = [c for c in c1h if c["t"] / 1000 >= t_exit - 3600]
        if len(cs) < 3:
            return f"baisse apres un gain (sortie {xp:.6g}) — on attend la fin de la baisse"
        lows = [float(c["l"]) for c in cs]
        i_low = lows.index(min(lows))
        green = float(cs[-1]["c"]) > float(cs[-1]["o"])
        if i_low <= len(cs) - 3 and green:
            return None                                        # point bas fait, la baisse semble finie
        return f"baisse apres un gain (sortie {xp:.6g}, prix {price:.6g}) — on attend la fin de la baisse pres du support"

    def _supsw_open(self, symbol, ticker, state, setup, ctx, v, zone, price, spread_pct, flow, res, K):
        fill = scalp.fill_price("long", price, spread_pct, K["slip"], "entry")
        sl = fill * (1 - K["sl_pct"] / 100)
        tp = fill * (1 + K["tp_pct"] / 100)
        arm_px = tp
        rd = ctx.get("res_dist_pct")
        if K["ride_on"] and rd is not None:       # v4.391 : on vise la resistance (si elle est plus loin que l objectif fixe)
            tp_eff = min(rd - K["res_buf"], K["ride_max"])
            if tp_eff > K["tp_pct"]:
                tp = fill * (1 + tp_eff / 100)
        capital = self.cfg.get("CAPITAL_USD", 100.0)
        notional = max(min(capital * K["risk_pct"] / K["sl_pct"], K["max_notional"]), 10.0)
        size = round(notional / K["lev"], 4)
        trade_uid = make_trade_uid("swing_support")
        state.open_position("long", fill, sl, tp, size, confidence=60.0, leverage=K["lev"], strategy="swing_support")
        pos = state.position
        pos["effective_mode"] = "paper"
        pos["trade_uid"] = trade_uid
        pos["slot_key"] = symbol
        pos["engine"] = "supsw"
        inval = sups.invalidation_level(fill, setup, zone["low"], K["exit_level"], K["exit_min"], K["exit_max"])
        pos["supsw"] = {"opened_ts": time.time(), "zone_low": zone["low"], "zone_high": zone["high"], "sl0": sl, "tp": tp, "arm": arm_px, "inval": inval,
                        "setup": setup["name"], "kind": setup["kind"], "ctx": ctx, "flow_entry": flow, "signal_t": res[1] if res else None}
        rr_txt = f"{K['tp_pct']:.2f}/{K['sl_pct']:.2f}"
        reasons = [f"🧱 Swing Support : {setup['name']} — support Daily {self._zone_txt(zone)}",
                   f"tendance Daily {v['trend']} · Weekly {ctx.get('weekly_trend') or '?'} · BTC {ctx.get('btc_trend') or '?'}"
                   f" · funding {ctx.get('funding_ann') if ctx.get('funding_ann') is not None else '?'} %/an",
                   f"stop {K['sl_pct']:.2f} % (filet) · objectif {(tp / fill - 1) * 100:.2f} % ({'resistance Daily ' + format(ctx.get('res_low') or 0, '.6g') if tp > arm_px else 'fixe'}; verrou des {K['tp_pct']:.2f} %) · sortie anticipee : cloture 1 h sous {inval:.6g} ({(1 - inval / fill) * 100:.2f} % sous l entree, {'plus bas du retest' if K['exit_level'] else 'bas de zone'}) + flux < {K['exit_flow']}",
                   f"spread {spread_pct:.3f} % · frais supposes {K['fee']} %"]
        try:
            quality = self.market_quality(ticker, state)
        except Exception:
            quality = {"vol_ratio": None, "activity_ratio": None}
        opened_event = {
            "vol_ratio": quality.get("vol_ratio"), "activity_ratio": quality.get("activity_ratio"), "flow_at_entry": flow,
            "spread_at_entry": spread_pct, "trade_uid": trade_uid, "slot_key": symbol, "is_accum_slot": False, "coin": ticker,
            "action": "LONG", "confidence": pos.get("confidence"), "leverage": K["lev"], "position_size_pct": None,
            "size_usd": round(size, 4), "sl_pct_used": round(K["sl_pct"], 4), "ttp_arm1_pct_used": None, "adaptive_sl_ttp": None,
            "strategy": "swing_support", "trade_mode": "paper", "risk_reward": round(K["tp_pct"] / K["sl_pct"], 2), "timeframe": "SWING_SUP",
            "entry": fill, "stop_loss": sl, "take_profit1": tp, "take_profit2": tp, "rsi": None,
            "entry_reasons": " | ".join(reasons), "confidence_breakdown": json.dumps({"setup": setup["kind"], **{k: v_ for k, v_ in ctx.items()}})}
        try:
            db.upsert_open_trade(opened_event)
            opened_event["persisted"] = True
        except Exception as e:
            print(f"[SWING-SUP] ecriture synchrone {trade_uid} impossible ({e}) — repli sur la file d evenements.")
        self.emit("trade_opened", opened_event)
        self.emit("log", {"msg": f"[{ticker}] 🧱 SWING SUPPORT LONG {setup['kind']} @ {fill:.6g} | SL {sl:.6g} ({K['sl_pct']:.2f} %) | "
                                 f"TP {tp:.6g} ({K['tp_pct']:.2f} %)", "level": "signal"})
        if res and res[0]:
            try:
                db.supsw_mark_taken(ticker, res[1], trade_uid)
                db.supsw_update(res[0], {"entry_ref": fill, "price": fill, "sl": sl, "tp": tp, "inval": inval})    # niveaux REELS du trade pris
                seen = getattr(self, "_supsw_seen", {})
                seen[(ticker, res[1])] = (3, res[0])
            except Exception as _e:
                print(f"[SWING-SUP] marquage pris ignore : {_e}")
        self._save_open_positions()

    def _supsw_trend_snapshot(self, ticker, price):
        """Tendance H4 et Daily a l instant de la sortie (enregistrees ; H4 'oui' = encore haussiere)."""
        h4 = d1 = None
        try:
            bv = self.mtf_view(ticker, price, cache_only=True, force_tf=("4h", "1h"))
            if bv.get("ok"):
                h4 = "oui" if bv["trend"] == "haussiere" else "non"
        except Exception:
            pass
        try:
            dv = self.mtf_view(ticker, price, cache_only=True, force_tf=("1d", "1h"))
            if dv.get("ok"):
                d1 = dv["trend"]
        except Exception:
            pass
        return h4, d1

    _SUPSW_ALERT_NAMES = ("flux", "support_casse", "h4_baisse", "btc_baisse", "sous_entree_2h", "pas_de_gain_3h", "moitie_du_stop")

    def _supsw_alert_probe(self, ticker, price, pos, sp, K):
        """v4.370 — MESURE SEULEMENT (aucune sortie n en depend) : compte les signes de degradation d un trade Swing Support ouvert.
        Au plus une mesure par minute. Resultat dans sp : al_max (maximum atteint), al_n (dernier compte), al_flags, al_hist."""
        now = time.time()
        if now - sp.get("al_ts", 0) < 60:
            return
        sp["al_ts"] = now
        entry = pos["entry"]
        age_h = (now - sp["opened_ts"]) / 3600.0
        sp["pk"] = max(sp.get("pk") or entry, price)
        if price < entry:
            sp.setdefault("under_since", now)
        else:
            sp.pop("under_since", None)
        f = {}
        try:
            fl = self._compute_trade_flow_pressure(ticker, price)
            f["flux"] = bool(fl is not None and fl < K["exit_flow"] * 0.5)
        except Exception:
            f["flux"] = False
        try:
            c1h = self._mtf_candles(ticker, "1h", 30, cache_only=True)
            f["support_casse"] = bool(c1h and sups.zone_broken_1h(c1h, sp.get("inval") or sp["zone_low"], sp["opened_ts"], K["exit_buf"] * (mtf.atr(c1h) or 0)))
        except Exception:
            f["support_casse"] = False
        try:
            h4_ok, _ = self._supsw_trend_snapshot(ticker, price)
            f["h4_baisse"] = (h4_ok == "non")
        except Exception:
            f["h4_baisse"] = False
        try:
            bt = None
            if ticker != "BTC":
                bpx = float((self.all_mids or {}).get("BTC") or 0)
                if bpx > 0:
                    bv = self.mtf_view("BTC", bpx, cache_only=True, force_tf=("1d", "1h"))
                    bt = bv.get("trend") if bv.get("ok") else None
            f["btc_baisse"] = bool(bt is not None and bt != "haussiere")
        except Exception:
            f["btc_baisse"] = False
        f["sous_entree_2h"] = bool(sp.get("under_since") and now - sp["under_since"] >= 7200)
        f["pas_de_gain_3h"] = bool(age_h >= 3 and (sp["pk"] / entry - 1) * 100 < 0.3)
        try:
            f["moitie_du_stop"] = bool(entry > pos["sl"] and (entry - price) >= 0.5 * (entry - pos["sl"]))
        except Exception:
            f["moitie_du_stop"] = False
        n = sum(1 for v in f.values() if v)
        if n != sp.get("al_n") or not sp.get("al_hist"):
            h = sp.setdefault("al_hist", [])
            h.append([round(age_h, 2), n, round((price / entry - 1) * 100, 3)])
            del h[:-80]
        sp["al_n"], sp["al_flags"] = n, f
        sp["al_max"] = max(sp.get("al_max", 0), n)

    def _manage_supsw(self, symbol, price, state, pos, ticker):
        K = self._supsw_state_cfg()
        sp = pos.get("supsw")
        if not sp:
            return
        try:
            self._supsw_alert_probe(ticker, price, pos, sp, K)
        except Exception as e:
            print(f"[SWING-SUP] mesure des alertes ignoree {ticker} : {type(e).__name__}: {e}")
        entry = pos["entry"]
        gain = price - entry
        pnl_usd_now = pos["size"] * pos.get("leverage", 1) * gain / entry
        if pnl_usd_now > 0 and (state.peak_pnl_usd is None or pnl_usd_now > state.peak_pnl_usd):
            state.peak_pnl_usd = pnl_usd_now
            state.absolute_peak_pnl_usd = pnl_usd_now
        reason, early, flow_exit = None, False, None
        # v4.389/v4.391 : a l objectif fixe (verrou) on ne ferme plus ; si une resistance est visee (sp["tp"] > sp["arm"]) le stop suit
        # le plus haut a distance LARGE jusqu a la resistance, puis serre (distance normale) une fois la resistance atteinte.
        arm_px = sp.get("arm") or sp["tp"]
        ride = sp["tp"] > arm_px * 1.0005
        if K["trail_on"] and not sp.get("trail") and price >= arm_px and price > pos["sl"]:
            sp["trail"], sp["trail_ts"], sp["peak"] = True, time.time(), price
            dist = K["ride_trail"] if ride else K["trail_pct"]
            pos["sl"] = max(pos["sl"], entry * (1 + K["trail_lock"] / 100), price * (1 - dist / 100))
            self.emit("log", {"msg": f"[{ticker}] 🧱 Verrou {arm_px:.6g} atteint : on laisse courir"
                                     + (f" vers la résistance {sp['tp']:.6g}" if ride else "")
                                     + f" — stop remonté à {pos['sl']:.6g} (+{(pos['sl'] / entry - 1) * 100:.2f} % verrouillé)", "level": "signal"})
            self._save_open_positions()
        if sp.get("trail"):
            if ride and not sp.get("ride_end") and price >= sp["tp"]:
                sp["ride_end"] = True
                self.emit("log", {"msg": f"[{ticker}] 🧱 Résistance {sp['tp']:.6g} atteinte : le stop serre à {K['trail_pct']:g} % sous le plus haut", "level": "signal"})
            dist = K["ride_trail"] if (ride and not sp.get("ride_end")) else K["trail_pct"]
            tighten = bool(sp.get("ride_end")) and not sp.get("ride_tight")      # une seule fois : on resserre le stop au passage de la resistance
            if price > sp.get("peak", 0) or tighten:
                sp["peak"] = max(price, sp.get("peak", 0))
                if tighten:
                    sp["ride_tight"] = True
                new_sl = max(pos["sl"], sp["peak"] * (1 - dist / 100))
                if new_sl > pos["sl"]:
                    pos["sl"] = new_sl
                    if time.time() - sp.get("sl_saved_ts", 0) > 60:
                        sp["sl_saved_ts"] = time.time()
                        self._save_open_positions()
        if price <= pos["sl"]:
            reason = "SWING SUPPORT TRAILING OBJECTIF" if sp.get("trail") else "SWING SUPPORT STOP"
        elif price >= sp["tp"] and not sp.get("trail") and not K["trail_on"]:
            reason = "SWING SUPPORT OBJECTIF"
        elif time.time() - sp["opened_ts"] >= K["hold_days"] * 86400:
            reason = "SWING SUPPORT DUREE MAX"
        elif sp.get("trail"):
            pass                                  # gain verrouille : seul le stop suiveur (ou la duree max) ferme
        else:
            try:
                c1h = self._mtf_candles(ticker, "1h", 30, cache_only=True)
                if c1h:
                    buf = K["exit_buf"] * (mtf.atr(c1h) or 0)
                    if sups.zone_broken_1h(c1h, sp.get("inval") or sp["zone_low"], sp["opened_ts"], buf):
                        flow_exit = self._compute_trade_flow_pressure(ticker, price)
                        sp["flow_exit"] = flow_exit
                        if flow_exit is not None and flow_exit < K["exit_flow"]:
                            h4_ok, _ = self._supsw_trend_snapshot(ticker, price)
                            if (not K["need_h4_end"]) or h4_ok == "non":
                                reason, early = "SWING SUPPORT CASSE (flux)", True
                        elif not sp.get("broken_noflow_ts"):
                            sp["broken_noflow_ts"] = time.time()      # support casse mais flux non hostile : on garde, on note
            except Exception as e:
                print(f"[SWING-SUP] sortie anticipee {ticker} ignoree : {type(e).__name__}: {e}")
        if reason is None:
            return
        try:
            q = self.market_quality(ticker, state)
            spread_pct = (q.get("spread_pct") if q else None) or 0.02
        except Exception:
            spread_pct = 0.02
        h4_ok, d1_tr = self._supsw_trend_snapshot(ticker, price) if early or reason else (None, None)
        exit_px = scalp.fill_price(pos["type"], price, spread_pct, K["slip"], "exit")
        trade_uid = pos.get("trade_uid")
        _result = self._safe_close_position(state, exit_px, reason, ticker, pos, symbol, "paper")
        if _result is None:
            return
        pnl, _, trade = _result
        trade["entry_mechanism"] = f"SWING SUPPORT {sp.get('kind', '')}"
        if pnl < 0:
            state.supsw_last_loss_ts = time.time()
        try:
            db.supsw_record_exit(trade_uid, {
                "exit_ts": int(time.time() * 1000), "exit_price": exit_px, "exit_reason": reason, "early_exit": 1 if early else 0,
                "pnl_pct": round((exit_px / entry - 1) * 100, 4), "hold_h": round((time.time() - sp["opened_ts"]) / 3600, 2),
                "flow_exit": sp.get("flow_exit"), "h4_intact_exit": h4_ok, "d1_trend_exit": d1_tr,
                "peak_pct": round((max(sp.get("pk") or entry, sp.get("peak") or entry) / entry - 1) * 100, 4),
                "trail_armed": 1 if sp.get("trail") else 0,
                "alerts_max": sp.get("al_max"), "alerts_exit": sp.get("al_n"),
                "alerts_json": json.dumps({"flags": sp.get("al_flags"), "hist": sp.get("al_hist")}) if sp.get("al_hist") else None})
        except Exception as e:
            print(f"[SWING-SUP] enregistrement de la sortie ignore : {e}")
        self.emit("trade", trade)
        self.emit("log", {"msg": f"[{ticker}] 🧱 {reason} @ ${exit_px:.6g} | PnL: ${pnl:.2f}", "level": "win" if pnl > 0 else "loss"})
        self._save_open_positions()

    def _supsw_eval_if_due(self):
        """Mesure sur bougies 1 h : (a) l issue de CHAQUE signal journalise a 24 h puis 72 h ; (b) pour les trades sortis, ce qui s est passe
        1 h / 4 h / 24 h apres la sortie et si l objectif ou le stop aurait ete touche (sortie anticipee trop tot ?)."""
        if self.info is None or not self.cfg.get("SUPSW_ENABLED", 0):
            return
        now = time.time()
        if now - getattr(self, "_supsw_eval_last", 0) < 600:
            return
        self._supsw_eval_last = now
        try:
            now_ms = int(now * 1000)
            sig = db.supsw_pending_signals(now_ms, limit=60)
            ext = db.supsw_pending_exits(now_ms, limit=40)
            if not sig and not ext:
                return
            by = {}
            for e in sig:
                by.setdefault(e["asset"], {"sig": [], "ext": []})["sig"].append(e)
            for e in ext:
                by.setdefault(e["asset"], {"sig": [], "ext": []})["ext"].append(e)
            for asset, grp in list(by.items())[:3]:
                t0 = min([e["signal_t"] for e in grp["sig"]] + [e["exit_ts"] for e in grp["ext"]])
                raw = self.info.post("/info", {"type": "candleSnapshot", "req": {"coin": asset, "interval": "1h",
                                                                                 "startTime": t0 - 3_600_000, "endTime": now_ms}})
                time.sleep(0.3)
                if not isinstance(raw, list) or not raw:
                    continue
                cs = [{"t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]), "l": float(c["l"]), "c": float(c["c"])} for c in raw]
                for e in grp["sig"]:
                    horizon = 24 if e["evaluated"] == 0 else 72
                    out = swj.evaluate_outcome("long", e["entry_ref"], e["sl"], e["tp"], cs, e["signal_t"], horizon)
                    if out is None:
                        continue
                    db.supsw_update(e["id"], {**out, "evaluated": 1 if horizon == 24 else 2})
                for e in grp["ext"]:
                    horizon = 24 if e["post_eval"] == 0 else 72
                    out = swj.evaluate_outcome("long", e["entry_ref"], e["sl"], e["tp"], cs, e["exit_ts"], horizon)
                    if out is None:
                        continue
                    pm = sups.post_exit_metrics("long", e["exit_price"], cs, e["exit_ts"])
                    upd = {"post_eval": 1 if horizon == 24 else 2, "after_outcome": out["outcome"], "after_hit_h": out["hit_h"],
                           "after_mfe": out["mfe72"] if horizon == 72 else out["mfe24"],
                           "after_mae": out["mae72"] if horizon == 72 else out["mae24"],
                           "too_early": 1 if (e.get("early_exit") and out["outcome"] == "tp") else 0, **pm}
                    db.supsw_update(e["id"], upd)
            if now - getattr(self, "_supsw_prune_last", 0) > 86400:
                self._supsw_prune_last = now
                db.supsw_events_prune(int((now - 120 * 86400) * 1000))
        except Exception as e:
            print(f"[SWING-SUP] evaluation ignoree : {type(e).__name__}: {e}")

    # ───────────────────────── v4.365 — TENDANCE CONFIRMEE (module separe, PAPER) ─────────────────────────
    # Entree quand la tendance est CONFIRMEE (Daily ET H4 dans le meme sens + momentum 1 h + flux), longs et shorts ; on reste tant
    # qu elle tient : stop suiveur (multiple de l ATR H4), sortie quand la tendance cesse. Slot unique par actif (comme Hyperliquid).
    def _trendf_cfg(self):
        c = self.cfg
        return {
            "syms": c.get("TRENDF_SYMBOLS", ["BTC", "ETH", "HYPE", "TAO", "SUI", "PAXG", "xyz:EUR", "xyz:JPY"]),
            "allow_short": bool(c.get("TRENDF_ALLOW_SHORT", 1)), "risk_pct": float(c.get("TRENDF_RISK_PCT", 1.0)),
            "max_notional": float(c.get("TRENDF_MAX_NOTIONAL_USD", 60.0)), "lev": max(int(c.get("TRENDF_LEVERAGE", 3)), 1),
            "max_trades": int(c.get("TRENDF_MAX_TRADES", 3)), "stop_atr": float(c.get("TRENDF_STOP_ATR", 3.0)),
            "min_stop": float(c.get("TRENDF_MIN_STOP_PCT", 0.8)), "max_stop": float(c.get("TRENDF_MAX_STOP_PCT", 6.0)),
            "entry_mode": int(c.get("TRENDF_ENTRY_MODE", 0)), "break_bars": max(int(c.get("TRENDF_BREAK_BARS", 12)), 3),
            "min_flow": float(c.get("TRENDF_MIN_FLOW", 0.2)), "max_ext": float(c.get("TRENDF_MAX_EXT_ATR", 0.0)),
            "max_spread": float(c.get("TRENDF_MAX_SPREAD_PCT", 0.05)), "hold_days": float(c.get("TRENDF_MAX_HOLD_DAYS", 30)),
            "cooldown": int(c.get("TRENDF_COOLDOWN_SEC", 14400)), "h4_exit": bool(c.get("TRENDF_EXIT_H4_REVERSE", 1)),
            "slip": float(c.get("FOREX_SCALP_SLIPPAGE_PCT", 0.005)), "fee": float(c.get("FEE_ROUND_TRIP_PCT", 0.09)),
        }

    @staticmethod
    def _trendf_is_fx(ticker):
        return ticker.startswith("xyz:") or ticker == "PAXG"

    def _trendf_open_count(self):
        n = 0
        for pool in (self.states, self.accum_states):
            for st in list(pool.values()):
                if st.position and st.position.get("strategy") == "trend_follow":
                    n += 1
        return n

    def _trendf_prefetch_open(self):
        """Charge (hors flux WebSocket) les bougies dont la gestion a besoin ; la gestion lit ensuite le cache seul."""
        for pool in (self.states, self.accum_states):
            for sk, st in list(pool.items()):
                if st.position and st.position.get("strategy") == "trend_follow":
                    tk = ticker_from_slot_key(sk)
                    try:
                        self._mtf_candles(tk, "1d", 230)
                        self._mtf_candles(tk, "4h", 260)
                    except Exception as e:
                        print(f"[TENDANCE] prechargement {tk} ignore : {type(e).__name__}: {e}")

    def _trendf_cycle(self):
        cfg = self.cfg
        if self.info is None:
            return
        now = time.time()
        if now - getattr(self, "_trendf_last_cycle", 0) < 30:
            return
        self._trendf_last_cycle = now
        self._trendf_prefetch_open()          # meme si le mode vient d etre eteint : les positions ouvertes restent gerees
        if not cfg.get("TRENDF_ENABLED", 0):
            return
        enabled_open = self.trading_enabled and cfg.get("STRATEGY_TRADING_ENABLED", {}).get("trend_follow", True)
        for ticker in self._trendf_cfg()["syms"]:
            try:
                self._trendf_eval_ticker(ticker, now, enabled_open)
            except Exception as e:
                print(f"[TENDANCE] {ticker} : evaluation ignoree : {type(e).__name__}: {e}")

    def _trendf_log(self, ticker, side, c1h, v, ctx, price, flow, ext, decision, prio, K, c4):
        """Journalise UNE entree candidate (ou refusee) avec son contexte. Retourne (event_id, signal_t) ou None."""
        try:
            signal_t = int(c1h[-1]["t"] + 3_600_000)
            seen = getattr(self, "_trendf_seen", None)
            if seen is None:
                seen = self._trendf_seen = {}
            bucket = signal_t // (4 * 3_600_000)             # au plus une ligne de refus par actif, sens et periode de 4 h
            key = (ticker, side, bucket)
            if seen.get(key, (0, None))[0] >= prio:
                return seen[key][1], seen[key][2] if len(seen[key]) > 2 else signal_t
            now = time.time()
            for k in [k for k in list(seen) if k[2] < (now // (4 * 3600)) - 6]:
                seen.pop(k, None)
            ref = float(c1h[-1]["c"])
            atr4 = mtf.atr(c4) or 0
            sdist = trf.stop_distance(ref, atr4, K["stop_atr"], K["min_stop"], K["max_stop"])
            sgn = 1 if side == "long" else -1
            eid = db.trendf_upsert({
                "ts": int(now * 1000), "signal_t": signal_t, "asset": ticker, "side": side, "d1_trend": v.get("trend"),
                "h4_trend": mtf.trend(c4)[0], "weekly_trend": ctx.get("weekly_trend"), "btc_trend": ctx.get("btc_trend"),
                "funding_ann": ctx.get("funding_ann"), "flow": flow, "atr_pct": (v["atr"] / price * 100) if price else None,
                "ext_atr": ext, "hour_utc": datetime.utcfromtimestamp(now).hour, "price": price, "entry_ref": ref,
                "sl": ref - sgn * sdist, "risk_pct": round(sdist / ref * 100, 3), "entry_mode": K["entry_mode"],
                "decision": decision, "prio": prio, "trade_uid": None})
            seen[key] = (prio, eid, signal_t)
            return eid, signal_t
        except Exception as e:
            print(f"[TENDANCE] journal ignore : {type(e).__name__}: {e}")
            return None

    def _trendf_eval_ticker(self, ticker, now, enabled_open):
        cfg = self.cfg
        K = self._trendf_cfg()
        symbol, state = self._scalp_state_for(ticker)
        if state is None:
            return
        snap = {"ts": now, "blocker": None}
        state.trendf_snapshot = snap
        if self._supsw_reserved(ticker):
            snap["blocker"] = "actif reserve a Swing Support"
            return
        occ = None
        for pool in (self.states, self.accum_states):
            st = pool.get(symbol)
            if st is not None and st.position:
                occ = st.position.get("strategy") or "?"
                break
        if occ is not None:
            snap["blocker"] = "position tendance deja ouverte sur cet actif" if occ == "trend_follow" else f"actif deja ouvert dans un autre mode ({occ})"
            return
        if self._trendf_is_fx(ticker) and not is_forex_open():
            snap["blocker"] = "marche ferme"
            return
        price = float((self.all_mids or {}).get(ticker) or 0)
        if price <= 0:
            snap["blocker"] = "prix indisponible"
            return
        v = self.mtf_view(ticker, price, force_tf=("1d", "1h"))
        if not v["ok"]:
            snap["blocker"] = v["why"]
            return
        if not v.get("data_fresh", True):
            snap["blocker"] = "donnees Daily perimees — entree suspendue"
            return
        c4 = self._mtf_candles(ticker, "4h", 260)
        if len(c4) < 200:
            snap["blocker"] = f"historique 4h insuffisant ({len(c4)} bougies)"
            return
        tr4 = mtf.trend(c4)[0]
        snap.update({"d1": v["trend"], "h4": tr4})
        sides = trf.confirmed_sides(v["trend"], tr4, K["allow_short"])
        if not sides:
            snap["blocker"] = f"tendance non confirmee : Daily {v['trend']} · H4 {tr4}"
            return
        c1h = self._mtf_candles(ticker, "1h", 60)
        if len(c1h) < 30:
            snap["blocker"] = f"historique 1h insuffisant ({len(c1h)} bougies)"
            return
        side = sides[0]
        ok, why = trf.momentum_ok(c1h, side)
        if ok and K["entry_mode"] == 1:
            ok, why = trf.breakout_ok(c1h, side, K["break_bars"])
        if not ok:
            snap["blocker"] = f"tendance {side.upper()} confirmee (Daily {v['trend']}, H4 {tr4}) — attente : {why}"
            return
        flow = None
        try:
            flow = self._compute_trade_flow_pressure(ticker, price)
        except Exception:
            flow = None
        req = K["min_flow"] if side == "long" else -K["min_flow"]
        ext = trf.extension_atr(side, price, v.get("ema_fast"), v.get("atr"))
        try:
            q = self.market_quality(ticker, state)
            spread_pct = q.get("spread_pct") if q else None
        except Exception:
            spread_pct = None
        spread_pct = spread_pct if spread_pct is not None else 0.02
        snap.update({"side": side, "flow": flow, "ext": ext})
        ctx = self._supsw_context(ticker, v, price)
        blocker = None
        if flow is not None and ((side == "long" and flow < req) or (side == "short" and flow > req)):
            blocker = f"flux {flow:+.2f} pas assez dans le sens du trade (minimum {req:+.2f})"
        elif K["max_ext"] > 0 and ext is not None and ext > K["max_ext"]:
            blocker = f"entree trop tardive : prix a {ext:.1f} ATR Daily de sa moyenne (maximum {K['max_ext']})"
        elif spread_pct > K["max_spread"]:
            blocker = f"spread trop large ({spread_pct:.3f} % > {K['max_spread']} %)"
        elif not enabled_open:
            blocker = "module arrete (bouton Marche/Arret ou trading global)"
        elif self._trendf_open_count() >= K["max_trades"]:
            blocker = "nombre maximal de positions tendance atteint"
        elif now - getattr(state, "trendf_last_exit_ts", 0) < K["cooldown"]:
            blocker = "pause apres la derniere sortie sur cet actif"
        if blocker:
            snap["blocker"] = f"tendance {side.upper()} confirmee — {blocker}"
            self._trendf_log(ticker, side, c1h, v, ctx, price, flow, ext, f"refus : {blocker.split(' (')[0].split(' :')[0]}", 1, K, c4)
            return
        res = self._trendf_log(ticker, side, c1h, v, ctx, price, flow, ext, "candidat (non ouvert)", 2, K, c4)
        self._trendf_open(symbol, ticker, state, side, ctx, v, tr4, c4, price, spread_pct, flow, ext, res, K)

    def _trendf_open(self, symbol, ticker, state, side, ctx, v, tr4, c4, price, spread_pct, flow, ext, res, K):
        sgn = 1 if side == "long" else -1
        fill = scalp.fill_price(side, price, spread_pct, K["slip"], "entry")
        dist = trf.stop_distance(fill, mtf.atr(c4), K["stop_atr"], K["min_stop"], K["max_stop"])
        sl = fill - sgn * dist
        stop_pct = dist / fill * 100
        far_tp = fill * 3 if side == "long" else fill * 0.1            # aucun objectif : sentinelle jamais atteinte
        capital = self.cfg.get("CAPITAL_USD", 100.0)
        notional = max(min(capital * K["risk_pct"] / stop_pct, K["max_notional"]), 10.0)
        size = round(notional / K["lev"], 4)
        trade_uid = make_trade_uid("trend_follow")
        state.open_position(side, fill, sl, far_tp, size, confidence=60.0, leverage=K["lev"], strategy="trend_follow")
        pos = state.position
        pos["effective_mode"] = "paper"
        pos["trade_uid"] = trade_uid
        pos["slot_key"] = symbol
        pos["engine"] = "trendf"
        pos["trendf"] = {"opened_ts": time.time(), "side": side, "dist": dist, "best": fill, "sl0": sl, "moved": False, "peak_pct": 0.0,
                         "ctx": ctx, "flow_entry": flow, "ext_atr": ext, "d1": v["trend"], "h4": tr4, "signal_t": res[1] if res else None}
        label = "LONG" if side == "long" else "SHORT"
        reasons = [f"📈 Tendance confirmee {side} : Daily {v['trend']} + H4 {tr4}, momentum 1 h et flux dans le sens"
                   f"{' + cassure ' + str(K['break_bars']) + ' h' if K['entry_mode'] == 1 else ''}",
                   f"Weekly {ctx.get('weekly_trend') or '?'} · BTC {ctx.get('btc_trend') or '?'} · funding "
                   f"{ctx.get('funding_ann') if ctx.get('funding_ann') is not None else '?'} %/an · flux {flow if flow is not None else '?'} "
                   f"· retard {ext if ext is not None else '?'} ATR Daily",
                   f"stop suiveur {stop_pct:.2f} % ({K['stop_atr']} x ATR H4) · aucun objectif : on reste tant que la tendance tient",
                   f"spread {spread_pct:.3f} % · frais supposes {K['fee']} %"]
        try:
            quality = self.market_quality(ticker, state)
        except Exception:
            quality = {"vol_ratio": None, "activity_ratio": None}
        opened_event = {
            "vol_ratio": quality.get("vol_ratio"), "activity_ratio": quality.get("activity_ratio"), "flow_at_entry": flow,
            "spread_at_entry": spread_pct, "trade_uid": trade_uid, "slot_key": symbol, "is_accum_slot": False, "coin": ticker,
            "action": label, "confidence": pos.get("confidence"), "leverage": K["lev"], "position_size_pct": None,
            "size_usd": round(size, 4), "sl_pct_used": round(stop_pct, 4), "ttp_arm1_pct_used": None, "adaptive_sl_ttp": None,
            "strategy": "trend_follow", "trade_mode": "paper", "risk_reward": None, "timeframe": "TREND_D1H4",
            "entry": fill, "stop_loss": sl, "take_profit1": None, "take_profit2": None, "rsi": None,
            "entry_reasons": " | ".join(reasons),
            "confidence_breakdown": json.dumps({"mode": K["entry_mode"], "ext_atr": ext, **{k: v_ for k, v_ in ctx.items()}})}
        try:
            db.upsert_open_trade(opened_event)
            opened_event["persisted"] = True
        except Exception as e:
            print(f"[TENDANCE] ecriture synchrone {trade_uid} impossible ({e}) — repli sur la file d evenements.")
        self.emit("trade_opened", opened_event)
        self.emit("log", {"msg": f"[{ticker}] 📈 TENDANCE {label} @ {fill:.6g} | stop suiveur {sl:.6g} ({stop_pct:.2f} %) | Daily {v['trend']} · H4 {tr4}",
                          "level": "signal"})
        if res and res[0]:
            try:
                db.trendf_mark_taken(ticker, side, res[1], trade_uid)
                db.trendf_update(res[0], {"entry_ref": fill, "price": fill, "sl": sl, "risk_pct": round(stop_pct, 3)})
                seen = getattr(self, "_trendf_seen", {})
                seen[(ticker, side, res[1] // (4 * 3_600_000))] = (3, res[0], res[1])
            except Exception as _e:
                print(f"[TENDANCE] marquage pris ignore : {_e}")
        self._save_open_positions()

    def _manage_trendf(self, symbol, price, state, pos, ticker):
        K = self._trendf_cfg()
        tf = pos.get("trendf")
        if not tf:
            return
        side = pos["type"]
        long_side = side == "long"
        sgn = 1 if long_side else -1
        entry = pos["entry"]
        gain_pct = sgn * (price - entry) / entry * 100
        tf["peak_pct"] = max(tf.get("peak_pct", 0.0), gain_pct)
        tf["best"] = max(tf.get("best", entry), price) if long_side else min(tf.get("best", entry), price)
        new_sl = trf.trail_stop(side, tf["best"], tf["dist"], pos["sl"])
        if (long_side and new_sl > pos["sl"]) or ((not long_side) and new_sl < pos["sl"]):
            pos["sl"] = new_sl
            tf["moved"] = True
        pnl_usd_now = pos["size"] * pos.get("leverage", 1) * sgn * (price - entry) / entry
        if pnl_usd_now > 0 and (state.peak_pnl_usd is None or pnl_usd_now > state.peak_pnl_usd):
            state.peak_pnl_usd = pnl_usd_now
            state.absolute_peak_pnl_usd = pnl_usd_now
        reason = None
        d1_label = h4_label = None
        if (long_side and price <= pos["sl"]) or ((not long_side) and price >= pos["sl"]):
            reason = "TENDANCE STOP SUIVEUR" if tf.get("moved") else "TENDANCE STOP INITIAL"
        elif time.time() - tf["opened_ts"] >= K["hold_days"] * 86400:
            reason = "TENDANCE DUREE MAX"
        else:
            try:                                  # tendance jugee sur bougies CLOTUREES apres l entree (jamais sur le prix du moment)
                c1d = self._mtf_candles(ticker, "1d", 230, cache_only=True)
                c4 = self._mtf_candles(ticker, "4h", 260, cache_only=True)
                if c1d and len(c1d) >= 200 and c1d[-1]["t"] / 1000 + 86400 > tf["opened_ts"]:
                    d1_label = mtf.trend(c1d)[0]
                if c4 and len(c4) >= 200 and c4[-1]["t"] / 1000 + 14400 > tf["opened_ts"]:
                    h4_label = mtf.trend(c4)[0]
                ended, why = trf.trend_end(side, d1_label, h4_label, K["h4_exit"])
                if ended:
                    reason = why
            except Exception as e:
                print(f"[TENDANCE] verification de tendance {ticker} ignoree : {type(e).__name__}: {e}")
        if reason is None:
            return
        try:
            q = self.market_quality(ticker, state)
            spread_pct = (q.get("spread_pct") if q else None) or 0.02
        except Exception:
            spread_pct = 0.02
        exit_px = scalp.fill_price(pos["type"], price, spread_pct, K["slip"], "exit")
        trade_uid = pos.get("trade_uid")
        _result = self._safe_close_position(state, exit_px, reason, ticker, pos, symbol, "paper")
        if _result is None:
            return
        pnl, _, trade = _result
        trade["entry_mechanism"] = f"TENDANCE {side} (Daily {tf.get('d1')} · H4 {tf.get('h4')})"
        state.trendf_last_exit_ts = time.time()
        try:
            db.trendf_record_exit(trade_uid, {
                "exit_ts": int(time.time() * 1000), "exit_price": exit_px, "exit_reason": reason,
                "pnl_pct": round(sgn * (exit_px / entry - 1) * 100, 4), "hold_h": round((time.time() - tf["opened_ts"]) / 3600, 2),
                "peak_pct": round(tf.get("peak_pct", 0.0), 4), "d1_trend_exit": d1_label, "h4_trend_exit": h4_label})
        except Exception as e:
            print(f"[TENDANCE] enregistrement de la sortie ignore : {e}")
        self.emit("trade", trade)
        self.emit("log", {"msg": f"[{ticker}] 📈 {reason} @ ${exit_px:.6g} | PnL: ${pnl:.2f}", "level": "win" if pnl > 0 else "loss"})
        self._save_open_positions()

    def _trendf_eval_if_due(self):
        """Mesure sur bougies 1 h : (a) chaque signal a 24 h puis 72 h (stop initial fixe, sans objectif : rendement a 24/72 h) ;
        (b) pour les trades sortis, ce que le prix a fait 1 h / 4 h / 24 h apres la sortie."""
        if self.info is None or not self.cfg.get("TRENDF_ENABLED", 0):
            return
        now = time.time()
        if now - getattr(self, "_trendf_eval_last", 0) < 600:
            return
        self._trendf_eval_last = now
        try:
            now_ms = int(now * 1000)
            sig = db.trendf_pending_signals(now_ms, limit=60)
            ext = db.trendf_pending_exits(now_ms, limit=40)
            if not sig and not ext:
                return
            by = {}
            for e in sig:
                by.setdefault(e["asset"], {"sig": [], "ext": []})["sig"].append(e)
            for e in ext:
                by.setdefault(e["asset"], {"sig": [], "ext": []})["ext"].append(e)
            for asset, grp in list(by.items())[:3]:
                t0 = min([e["signal_t"] for e in grp["sig"]] + [e["exit_ts"] for e in grp["ext"]])
                raw = self.info.post("/info", {"type": "candleSnapshot", "req": {"coin": asset, "interval": "1h",
                                                                                 "startTime": t0 - 3_600_000, "endTime": now_ms}})
                time.sleep(0.3)
                if not isinstance(raw, list) or not raw:
                    continue
                cs = [{"t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]), "l": float(c["l"]), "c": float(c["c"])} for c in raw]
                for e in grp["sig"]:
                    horizon = 24 if e["evaluated"] == 0 else 72
                    far = e["entry_ref"] * 10 if e["side"] == "long" else e["entry_ref"] * 0.0001     # aucun objectif : seul le stop compte
                    out = swj.evaluate_outcome(e["side"], e["entry_ref"], e["sl"], far, cs, e["signal_t"], horizon)
                    if out is None:
                        continue
                    db.trendf_update(e["id"], {**out, "evaluated": 1 if horizon == 24 else 2})
                for e in grp["ext"]:
                    horizon = 24 if e["post_eval"] == 0 else 72
                    far = e["entry_ref"] * 10 if e["side"] == "long" else e["entry_ref"] * 0.0001
                    out = swj.evaluate_outcome(e["side"], e["entry_ref"], e["sl"], far, cs, e["exit_ts"], horizon)
                    if out is None:
                        continue
                    pm = sups.post_exit_metrics(e["side"], e["exit_price"], cs, e["exit_ts"])
                    db.trendf_update(e["id"], {"post_eval": 1 if horizon == 24 else 2,
                                               "after_mfe": out["mfe72"] if horizon == 72 else out["mfe24"],
                                               "after_mae": out["mae72"] if horizon == 72 else out["mae24"], **pm})
            if now - getattr(self, "_trendf_prune_last", 0) > 86400:
                self._trendf_prune_last = now
                db.trendf_events_prune(int((now - 120 * 86400) * 1000))
        except Exception as e:
            print(f"[TENDANCE] evaluation ignoree : {type(e).__name__}: {e}")

    # ───────────────────────── v4.359 — JOURNAL DU SWING (lecture seule) ─────────────────────────
    def _swing_active(self):
        return bool(self.cfg.get("MTF_USE_DAILY", 0) and self.cfg.get("MTF_USE_H4_LOWER", 0))

    def _swing_log(self, mode, ticker, long_side, price, zone, target, extreme, v, name, lower, decision, prio=1,
                   plan=None, flow=None, age=None, force=False):
        """Journalise UN signal H4 en zone Daily et la decision prise. Une ligne par (actif, sens, bougie du signal) ;
        une decision de rang superieur (candidat, pris) remplace un refus. Retourne la cle de l evenement ou None."""
        try:
            if (not force and not self._swing_active()) or not lower:
                return None
            side = "long" if long_side else "short"
            tf_sec = self._TF_SEC.get(v["lower_tf"], 14400)
            signal_t = int(lower[-1]["t"] + tf_sec * 1000)                 # cloture de la bougie du signal
            key = (ticker, side, signal_t)
            seen = getattr(self, "_swing_seen", None)
            if seen is None:
                seen = self._swing_seen = {}
            if seen.get(key, 0) >= prio:
                return key
            seen[key] = prio
            now = time.time()
            for k in [k for k, _ in list(seen.items()) if k[2] < (now - 8 * 3600) * 1000]:
                seen.pop(k, None)
            entry_ref = float(lower[-1]["c"])                              # prix de reference : cloture du signal
            hyp = plan
            if hyp is None:
                try:
                    hyp = mtf.plan_trade(side, entry_ref, zone, target, extreme, v["atr"])
                except Exception:
                    hyp = None
            hyp = hyp or {}
            tp = hyp.get("tp")
            db.swing_event_upsert({
                "ts": int(now * 1000), "signal_t": signal_t, "asset": ticker, "side": side, "mode": mode, "pattern": name,
                "trend": v.get("trend"), "zone_low": zone.get("low"), "zone_high": zone.get("high"), "zone_touches": zone.get("touches"),
                "price": price, "entry_ref": entry_ref, "atr_pct": (v["atr"] / price * 100) if price else None, "flow": flow,
                "hour_utc": datetime.utcfromtimestamp(now).hour,
                "age_min": round(age / 60, 1) if age is not None else round((now - signal_t / 1000) / 60, 1),
                "decision": decision, "prio": prio, "sl": hyp.get("sl"), "tp": tp, "risk_pct": hyp.get("risk_pct"),
                "reward_pct": hyp.get("reward_pct"), "rr": hyp.get("rr"), "hypothetical": 0 if plan is not None else 1})
            return key
        except Exception as e:
            print(f"[SWING-JOURNAL] ecriture ignoree : {type(e).__name__}: {e}")
            return None

    def _swing_journal_eval_if_due(self):
        """Mesure l issue des setups journalises sur bougies 1H : a 24 h puis a 72 h. 3 actifs au plus par passage."""
        if self.info is None or not (self._swing_active() or self.cfg.get("FXSWING_ENABLED", 0)):
            return
        now = time.time()
        if now - getattr(self, "_swing_eval_last", 0) < 600:
            return
        self._swing_eval_last = now
        try:
            pend = db.swing_events_pending(int(now * 1000), limit=60)
            if not pend:
                return
            by = {}
            for ev in pend:
                by.setdefault(ev["asset"], []).append(ev)
            fee = float(self.cfg.get("FEE_ROUND_TRIP_PCT", 0.09))
            for asset, evs in list(by.items())[:3]:
                t0 = min(e["signal_t"] for e in evs)
                raw = self.info.post("/info", {"type": "candleSnapshot", "req": {"coin": asset, "interval": "1h",
                                                                                 "startTime": t0 - 3_600_000, "endTime": int(now * 1000)}})
                time.sleep(0.3)
                if not isinstance(raw, list) or not raw:
                    continue
                cs = [{"t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]), "l": float(c["l"]), "c": float(c["c"])} for c in raw]
                for e in evs:
                    horizon = 24 if e["evaluated"] == 0 else 72
                    out = swj.evaluate_outcome(e["side"], e["entry_ref"], e["sl"], e["tp"], cs, e["signal_t"], horizon)
                    if out is None:
                        continue
                    fin = out["fin72"] if (horizon == 72 and out["fin72"] is not None) else out["fin24"]
                    r = swj.r_multiple(out["outcome"], e.get("risk_pct"), e.get("reward_pct"), fin)
                    db.swing_event_update(e["id"], {**out, "evaluated": 1 if horizon == 24 else 2,
                                                    "r_est": None if r is None else round(r, 3)})
            if now - getattr(self, "_swing_prune_last", 0) > 86400:
                self._swing_prune_last = now
                db.swing_events_prune(int((now - 120 * 86400) * 1000))
        except Exception as e:
            print(f"[SWING-JOURNAL] evaluation ignoree : {type(e).__name__}: {e}")

    # ───────────────────────── v4.353 — SCALP FOREX (paper) ─────────────────────────
    def _scalp_state_for(self, ticker):
        for sk, st in self.states.items():
            if ticker_from_slot_key(sk) == ticker:
                return sk, st
        return None, None

    def _scalp_open_count(self):
        return sum(1 for st in list(self.states.values()) if st.position and st.position.get("strategy") == "forex_scalp")

    def _scalp_today_pnl(self):
        """Perte/gain paper du scalp aujourd hui (UTC), relu en base toutes les 5 minutes."""
        now = time.time()
        c = getattr(self, "_scalp_pnl_cache", None)
        if c and now - c[0] < 300:
            return c[1]
        try:
            day0 = datetime.utcfromtimestamp(now).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            tot = sum((t.get("pnl") or 0.0) for t in db.get_all_closed_trades(since=day0) if t.get("strategy") == "forex_scalp")
        except Exception:
            tot = 0.0
        self._scalp_pnl_cache = (now, tot)
        return tot

    def _scalp_log_event(self, ticker, info, decision, price, spread_pct, flow, plan=None, trade_uid=None):
        """Journalise un setup DETECTE (pris ou filtre), au plus une fois par (actif, sens, setup, niveau) toutes les 10 min."""
        try:
            lv = info.get("level") or {}
            key = (ticker, info.get("side"), info.get("setup"), round(lv.get("p", 0), 5), decision.split(":")[0])
            cache = getattr(self, "_scalp_log_seen", None)
            if cache is None:
                cache = self._scalp_log_seen = {}
            now = time.time()
            if now - cache.get(key, 0) < 600:
                return
            cache[key] = now
            for k in [k for k, v in cache.items() if now - v > 3600]:
                cache.pop(k, None)
            real = plan is not None
            pl = plan or info.get("hyp") or {}
            atr5 = info.get("atr5") or 0.0
            db.scalp_event_insert({
                "ts": int(now * 1000), "asset": ticker, "side": info.get("side"), "setup": info.get("setup"),
                "level_kinds": "+".join(lv.get("kinds", [])), "level_price": lv.get("p"), "level_w": lv.get("w"),
                "price": price, "spread_pct": spread_pct, "atr5_pct": (atr5 / price * 100) if price else None,
                "bias": info.get("bias"), "flow": flow, "hour_utc": datetime.utcfromtimestamp(now).hour,
                "decision": decision, "sl": pl.get("sl"), "tp": pl.get("tp"), "risk_pct": pl.get("risk_pct"),
                "tp_pct": pl.get("tp_pct"), "rr": pl.get("rr"), "net_rr": pl.get("net_rr"),
                "cost_pct": self.cfg.get("FOREX_SCALP_FEE_PCT", 0.089) + (spread_pct or 0.0),
                "hypothetical": 0 if real else 1, "trade_uid": trade_uid})
        except Exception as e:
            print(f"[SCALP-JOURNAL] ecriture ignoree : {e}")

    def _scalp_cycle(self):
        """Evalue chaque actif Forex configure et ouvre un scalp paper si un setup passe tous les filtres.
        N interfere PAS avec le mode Forex : un seul slot par actif, donc le scalp ne s ouvre que sur un actif libre."""
        cfg = self.cfg
        if not cfg.get("FOREX_SCALP_ENABLED", 0) or self.info is None:
            return
        now = time.time()
        if now - getattr(self, "_scalp_last_cycle", 0) < 8:
            return
        self._scalp_last_cycle = now
        if not is_forex_open():
            return
        enabled_open = self.trading_enabled and cfg.get("STRATEGY_TRADING_ENABLED", {}).get("forex_scalp", True)
        for ticker in cfg.get("FOREX_SCALP_SYMBOLS", ["xyz:EUR", "xyz:JPY", "xyz:KRW", "PAXG"]):
            try:
                self._scalp_eval_ticker(ticker, now, enabled_open)
            except Exception as e:
                print(f"[SCALP] {ticker} : evaluation ignoree : {type(e).__name__}: {e}")

    def _scalp_eval_ticker(self, ticker, now, enabled_open):
        cfg = self.cfg
        symbol, state = self._scalp_state_for(ticker)
        if state is None:
            return
        snap = {"ts": now, "blocker": None}
        state.scalp_snapshot = snap
        if state.position and state.position.get("strategy") == "forex_scalp":
            snap["blocker"] = "scalp deja ouvert sur cet actif"      # gere par _manage_scalp ; pas de journal en double
            return
        price = float((self.all_mids or {}).get(ticker) or 0)
        if price <= 0:
            snap["blocker"] = "prix indisponible"
            return
        h1 = self._mtf_candles(ticker, "1h", 200)
        m15 = self._mtf_candles(ticker, "15m", 60)
        m5 = self._mtf_candles(ticker, "5m", 60)
        m1 = self._mtf_candles(ticker, "1m", 40)
        if len(h1) < 60 or len(m5) < 25:
            snap["blocker"] = f"historique insuffisant (1H {len(h1)}, 5 min {len(m5)})"
            return
        try:
            q = self.market_quality(ticker, state)
            spread_pct = q.get("spread_pct") if q else None
        except Exception:
            spread_pct = None
        spread_pct = spread_pct if spread_pct is not None else 0.02
        flow = self._compute_trade_flow_pressure(ticker, price)
        levels = scalp.build_levels(h1, int(now * 1000), price, scalp.round_step_for(ticker, price))
        bias, binfo = scalp.bias_15m(m15, price)
        snap.update({"bias": bias, "levels": [(round(l["p"], 6), "+".join(l["kinds"]), l["w"]) for l in levels
                     if abs(l["p"] - price) / price < 0.0030][:4], "spread_pct": spread_pct})
        ctx = {"price": price, "spread_pct": spread_pct, "m5": m5, "m1": m1, "levels": levels, "bias": bias, "now": now, "cfg": cfg}
        cand, why, detected, info = scalp.find_setup(ctx)
        if not detected:
            snap["blocker"] = why
            return
        # un setup existe : on le journalise quoi qu il arrive, avec sa decision
        blocker = None
        if cand is None:
            blocker = why
        elif not enabled_open:
            blocker = "filtre : scalp arrete (bouton Marche/Arret ou trading global)"
        elif state.position:
            blocker = f"filtre : slot occupe par {state.position.get('strategy', '?')}"
        elif self._scalp_open_count() >= cfg.get("FOREX_SCALP_MAX_TRADES", 2):
            blocker = "filtre : nombre maximal de scalps simultanes atteint"
        elif time.time() - getattr(state, "scalp_last_loss_ts", 0) < cfg.get("FOREX_SCALP_COOLDOWN_SEC", 300):
            blocker = "filtre : pause apres une perte sur cet actif"
        elif self._scalp_today_pnl() <= -abs(cfg.get("FOREX_SCALP_DAILY_LOSS_USD", 1.0)):
            blocker = "filtre : limite de perte journaliere du scalp atteinte"
        else:
            for _p in (self.states, self.accum_states):          # un actif ouvert ailleurs (autre pool) est indisponible
                for sk2, st2 in _p.items():
                    if ticker_from_slot_key(sk2) == ticker and st2.position:
                        blocker = f"filtre : actif deja ouvert ({st2.position.get('strategy', '?')})"
        if blocker:
            snap["blocker"] = blocker
            self._scalp_log_event(ticker, info, blocker, price, spread_pct, flow)
            return
        self._scalp_open(symbol, ticker, state, cand, price, spread_pct, flow)

    def _scalp_open(self, symbol, ticker, state, cand, price, spread_pct, flow):
        cfg = self.cfg
        side = cand["side"]
        fill = scalp.fill_price(side, price, spread_pct, cfg.get("FOREX_SCALP_SLIPPAGE_PCT", 0.005), "entry")
        lev = max(int(cfg.get("FOREX_SCALP_LEVERAGE", 3)), 1)
        size = round(float(cfg.get("FOREX_SCALP_NOTIONAL_USD", 30.0)) / lev, 4)
        trade_uid = make_trade_uid("forex_scalp")
        state.open_position(side, fill, cand["sl"], cand["tp"], size, confidence=round(cand["net_rr"] * 10, 1), leverage=lev, strategy="forex_scalp")
        pos = state.position
        pos["effective_mode"] = "paper"
        pos["trade_uid"] = trade_uid
        pos["slot_key"] = symbol
        pos["engine"] = "scalp"
        pos["scalp"] = {"r": abs(fill - cand["sl"]), "level": cand["level"]["p"], "opened_ts": time.time(), "cost_pct": cand["cost_pct"],
                        "best": fill, "setup": cand["setup"]}
        lv = cand["level"]
        reasons = [f"SCALP : {cand['setup']} sur {'+'.join(lv['kinds'])} (confluence {lv['w']}) a {lv['p']:.6g}",
                   f"biais 15 min {cand['bias']}", f"rejet 5 min + confirmation 1 min", f"stop {cand['risk_pct']:.3f} %",
                   f"objectif {cand['tp_kind']} {cand['tp_pct']:.3f} % (gain/risque {cand['rr']:.2f}, net de frais {cand['net_rr']:.2f})",
                   f"spread {spread_pct:.3f} % · frais supposes {cfg.get('FOREX_SCALP_FEE_PCT', 0.089)} %"]
        label = "LONG" if side == "long" else "SHORT"
        try:
            quality = self.market_quality(ticker, state)
        except Exception:
            quality = {"vol_ratio": None, "activity_ratio": None, "flow": None, "spread_pct": None}
        opened_event = {
            "vol_ratio": quality.get("vol_ratio"), "activity_ratio": quality.get("activity_ratio"), "flow_at_entry": flow,
            "spread_at_entry": spread_pct, "trade_uid": trade_uid, "slot_key": symbol, "is_accum_slot": False, "coin": ticker,
            "action": label, "confidence": pos.get("confidence"), "leverage": lev, "position_size_pct": None,
            "size_usd": round(size, 4), "sl_pct_used": None, "ttp_arm1_pct_used": None, "adaptive_sl_ttp": None,
            "strategy": "forex_scalp", "trade_mode": "paper", "risk_reward": round(cand["rr"], 2), "timeframe": "scalp",
            "entry": fill, "stop_loss": cand["sl"], "take_profit1": cand["tp"], "take_profit2": cand["tp"], "rsi": None,
            "entry_reasons": " | ".join(reasons), "confidence_breakdown": json.dumps({"net_rr": round(cand["net_rr"], 3), "w": lv["w"]}),
        }
        try:
            db.upsert_open_trade(opened_event)
            opened_event["persisted"] = True
        except Exception as e:
            print(f"[SCALP] ecriture synchrone {trade_uid} impossible ({e}) — repli sur la file d evenements.")
        self.emit("trade_opened", opened_event)
        self.emit("log", {"msg": f"[{ticker}] ⚡ SCALP {label} {cand['setup']} @ {fill:.6g} | SL {cand['sl']:.6g} ({cand['risk_pct']:.3f} %) | "
                                 f"TP {cand['tp']:.6g} ({cand['tp_pct']:.3f} %, net RR {cand['net_rr']:.2f})", "level": "signal"})
        self._recent_entries.setdefault("forex_scalp", []).append(time.time())
        self._scalp_log_event(ticker, {"side": side, "setup": cand["setup"], "level": lv, "bias": cand["bias"], "atr5": cand["atr5"]},
                              "pris", price, spread_pct, flow, plan=cand, trade_uid=trade_uid)
        self._save_open_positions()

    def _manage_scalp(self, symbol, price, state, pos, ticker):
        cfg = self.cfg
        sc = pos.get("scalp")
        if not sc:
            return
        long_side = pos["type"] == "long"
        gain = (price - pos["entry"]) if long_side else (pos["entry"] - price)
        pnl_usd_now = pos["size"] * pos.get("leverage", 1) * gain / pos["entry"]
        if pnl_usd_now > 0 and (state.peak_pnl_usd is None or pnl_usd_now > state.peak_pnl_usd):
            state.peak_pnl_usd = pnl_usd_now
            state.absolute_peak_pnl_usd = pnl_usd_now
        m5 = self._mtf_candles(ticker, "5m", 30, cache_only=True)
        flow = self._compute_trade_flow_pressure(ticker, price)
        dec = scalp.manage(pos, price, time.time(), m5, flow, cfg)
        if dec["new_sl"] is not None:
            pos["sl"] = dec["new_sl"]
        if not dec["exit"]:
            return
        try:
            q = self.market_quality(ticker, state)
            spread_pct = (q.get("spread_pct") if q else None) or 0.02
        except Exception:
            spread_pct = 0.02
        exit_px = scalp.fill_price(pos["type"], price, spread_pct, cfg.get("FOREX_SCALP_SLIPPAGE_PCT", 0.005), "exit")
        _result = self._safe_close_position(state, exit_px, dec["exit"], ticker, pos, symbol, "paper")
        if _result is None:
            return
        pnl, _, trade = _result
        trade["entry_mechanism"] = f"SCALP {sc.get('setup', '')}"
        if pnl < 0:
            state.scalp_last_loss_ts = time.time()
        self._scalp_pnl_cache = None
        self.emit("trade", trade)
        self.emit("log", {"msg": f"[{ticker}] ⚡ {dec['exit']} @ ${exit_px:.6g} | PnL: ${pnl:.2f}", "level": "win" if pnl > 0 else "loss"})
        self._save_open_positions()

    def _scalp_journal_eval_if_due(self):
        """Mesure ce que chaque setup journalise serait devenu (60 min plus tard) : 3 actifs au plus par passage, 1 requete chacun."""
        cfg = self.cfg
        if not cfg.get("FOREX_SCALP_ENABLED", 0) or self.info is None:
            return
        now = time.time()
        if now - getattr(self, "_scalp_eval_last", 0) < 120:
            return
        self._scalp_eval_last = now
        try:
            pend = db.scalp_events_pending(int((now - 65 * 60) * 1000), limit=80)
            if not pend:
                return
            by = {}
            for ev in pend:
                by.setdefault(ev["asset"], []).append(ev)
            for asset, evs in list(by.items())[:3]:
                t0 = min(e["ts"] for e in evs)
                t1 = max(e["ts"] for e in evs) + 61 * 60_000
                raw = self.info.post("/info", {"type": "candleSnapshot", "req": {"coin": asset, "interval": "1m", "startTime": t0 - 60_000, "endTime": t1}})
                time.sleep(0.3)
                if not isinstance(raw, list) or not raw:
                    continue
                cs = [{"t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]), "l": float(c["l"]), "c": float(c["c"])} for c in raw]
                for e in evs:
                    entry = scalp.fill_price(e["side"], e["price"], e.get("spread_pct") or 0.02, cfg.get("FOREX_SCALP_SLIPPAGE_PCT", 0.005), "entry")
                    out = scalp.evaluate_outcome(e["side"], entry, e["sl"], e["tp"], cs, e["ts"], 60)
                    if out is None:
                        continue
                    db.scalp_event_update(e["id"], {**out, "evaluated": 1})
            if now - getattr(self, "_scalp_prune_last", 0) > 86400:
                self._scalp_prune_last = now
                db.scalp_events_prune(int((now - 60 * 86400) * 1000))
        except Exception as e:
            print(f"[SCALP-JOURNAL] evaluation ignoree : {type(e).__name__}: {e}")

    def _flow_recorder_flush_if_due(self):
        """v4.349 — ecrit en base les tranches de flux terminees (toutes les 60 s au plus) et purge les
        tranches de plus de FLOW_RECORDER_KEEP_DAYS jours (une fois par jour). Hors du fil WebSocket."""
        try:
            rec = getattr(self, "flow_recorder", None)
            if rec is None or not self.cfg.get("FLOW_RECORDER_ENABLED", 1):
                return
            now = time.time()
            if now - getattr(self, "_flow_last_flush", 0) < 60:
                return
            self._flow_last_flush = now
            alive = (now - getattr(self, "_ws_trades_last_any", 0)) < 90
            rows, alive_t = rec.drain(int(now * 1000), stream_alive=alive)
            if rows:
                db.flow_insert_many(rows)
            if alive_t:
                db.flow_alive_insert(alive_t)
            if now - getattr(self, "_flow_last_prune", 0) > 86400:
                self._flow_last_prune = now
                keep = int(self.cfg.get("FLOW_RECORDER_KEEP_DAYS", 120))
                db.flow_prune(int((now - keep * 86400) * 1000))
        except Exception as e:
            print(f"[FLUX-ENREGISTREUR] ecriture ignoree : {e}")

    def _refresh_held_mtf_data(self):
        """v4.341 — SUR DEMANDE EXPLICITE : rafraichit les bougies de l unite majeure
        (H4) des actifs qui ont une position top-down OUVERTE. Sans cela, l evaluation
        d entree (seul endroit qui rechargeait ces bougies) s arretait avant de les
        lire pour un actif occupe : la sortie "tendance retournee" et le diagnostic
        "tendance intacte a la sortie" travaillaient sur un historique fige, ou vide
        apres un redemarrage. Appele a chaque cycle depuis la boucle principale (pas
        depuis le flux de prix) : _mtf_candles ne fait un appel reseau qu une fois
        par periode et par actif."""
        try:
            cfg = self.cfg
            major_tf = "1d" if cfg.get("MTF_USE_DAILY", 0) else cfg.get("MTF_MAJOR_TF", "4h")
            done = set()
            for pool in (self.states, self.accum_states):
                for slot, st in pool.items():
                    pos = st.position
                    if not pos or pos.get("engine") != "mtf":
                        continue
                    ticker = ticker_from_slot_key(slot)
                    if ticker in done:
                        continue
                    done.add(ticker)
                    self._mtf_candles(ticker, major_tf, 260 if major_tf == "4h" else 230)
            # v4.343 — positions MANUELLES avec une regle de sortie sur la tendance H4 : memes bougies a jour
            manual = getattr(self, "manual", None)
            if manual is not None:
                for it in list(manual.items.values()):
                    if it.get("status") == "open" and it.get("trend_exit") and it.get("ticker") not in done:
                        done.add(it["ticker"])
                        self._mtf_candles(it["ticker"], major_tf, 260 if major_tf == "4h" else 230)
        except Exception as e:
            print(f"[MTF] Rafraichissement des actifs occupes : erreur ignoree : {e}")

    def mtf_view(self, ticker, price, cache_only=False, force_tf=None):
        """Lecture complete pour le diagnostic et l entree. force_tf = (unite majeure, unite inferieure) : Swing Forex."""
        cfg = self.cfg
        major_tf = force_tf[0] if force_tf else ("1d" if cfg.get("MTF_USE_DAILY", 0) else cfg.get("MTF_MAJOR_TF", "4h"))
        # v4.352 — profil Swing : signal sur bougies H4 (MTF_USE_H4_LOWER=1), tendance et zones en Daily
        lower_tf = force_tf[1] if force_tf else ("4h" if cfg.get("MTF_USE_H4_LOWER", 0) else
                    ("1h" if cfg.get("MTF_USE_H1", 0) else cfg.get("MTF_LOWER_TF", "15m")))
        major = self._mtf_candles(ticker, major_tf, 260 if major_tf == "4h" else 230, cache_only=cache_only)
        if len(major) < 60:
            return {"ok": False, "why": f"historique {major_tf} insuffisant ({len(major)} bougies)"}
        # v4.327 — SUR DEMANDE EXPLICITE : la tendance de fond suit le prix LIVE
        # (flux WebSocket) au lieu de la seule derniere cloture H4 (jusqu a 4 h
        # de retard sur un retournement). Le prix courant est ajoute comme
        # derniere valeur PROVISOIRE de la serie (cloture en cours) ; desactivable
        # via MTF_TREND_LIVE_PRICE=0 (retour au calcul sur bougies cloturees).
        trend_candles = major
        if price and cfg.get("MTF_TREND_LIVE_PRICE", 1):
            trend_candles = major + [{**major[-1], "c": float(price)}]
        tr, ef, es = mtf.trend(trend_candles)
        a = mtf.atr(major) or 0
        zones = mtf.find_zones(major, lookback=cfg.get("MTF_ZONE_LOOKBACK", 120),
                               max_width_atr=(cfg.get("MTF_ZONE_MAX_WIDTH_ATR", 0) or None))   # v4.357
        sup, res = mtf.nearest_zones(zones, price)
        tol = a * cfg.get("MTF_ZONE_TOLERANCE_ATR", 0.25)
        # v4.341 — synchronisation avec la realite : ces bougies sont-elles a jour ?
        _fresh = self._mtf_is_fresh(self._TF_SEC[major_tf], major, int(time.time() * 1000),
                                    int(cfg.get("MTF_DATA_GRACE_SEC", 90)) * 1000)
        return {"ok": True, "major_tf": major_tf, "lower_tf": lower_tf, "trend": tr, "ema_fast": ef, "ema_slow": es,
                "atr": a, "support": sup, "resistance": res, "tolerance": tol,
                "in_support": mtf.in_zone(price, sup, tol), "in_resistance": mtf.in_zone(price, res, tol),
                "data_fresh": _fresh, "last_closed_t": major[-1]["t"]}

    @staticmethod
    def _zone_txt(z):
        return f"{z['low']:.6g}-{z['high']:.6g} ({z['touches']} contact{'s' if z['touches'] > 1 else ''})" if z else "aucune"

    def _mtf_entry(self, mode, symbol, ticker, price, rsi, prices, state, pos_state):
        """1) tendance de fond + zones cles sur l unite MAJEURE ;
        2) prix dans une zone dans le sens de la tendance ;
        3) signal de bougie sur l unite INFERIEURE ;
        4) v4.304 — confirmation de structure H1 (pente EMA) ET du flux reel
           de transactions — au-dela de la seule bougie ;
        5) plan : SL au-dela de la zone, objectif = zone opposee, R:R minimal."""
        cfg = self.cfg
        long_side = mode == "spot_accumulation"
        prefix = "SPOT_ACCUM" if long_side else "ACCUMULATION"
        snap = {"ts": time.time(), "enabled": cfg.get(f"{prefix}_ENABLED", False), "engine": "mtf"}
        if long_side:
            state.spot_accum_gate_snapshot = snap
        else:
            pos_state.accumulation_gate_snapshot = snap
        if not snap["enabled"]:
            snap["blocker"] = "mode desactive"
            return
        if self._supsw_reserved(ticker):      # v4.375 : actifs dedies a Swing Support
            snap["blocker"] = "actif reserve a Swing Support"
            return
        # v4.327 — SUR DEMANDE EXPLICITE : un actif deja ouvert dans N IMPORTE
        # QUEL autre mode (Spot-Accum, Accumulation, Funding, Forex — pools
        # self.states ET self.accum_states) est indisponible ; remplace le
        # blocage global par regime de marche. Verifie les DEUX pools (avant :
        # seulement celui du mode courant, donc un short Accumulation pouvait
        # s ouvrir sur un actif deja long en Spot-Accum ou en Funding).
        _occ = None
        for _pool in (self.states, self.accum_states):
            _st = _pool.get(symbol)
            if _st is not None and _st.position:
                _occ = _st.position.get("strategy") or "?"
                break
        if _occ is not None:
            snap["blocker"] = ("position deja ouverte" if _occ == mode
                               else f"actif deja ouvert dans un autre mode ({_occ})")
            return
        if pos_state.position:
            occ_strat = pos_state.position.get("strategy") or "?"
            # v4.320 — SUR DEMANDE EXPLICITE : precise QUELLE strategie
            # occupe reellement le slot partage (self.states, commun a
            # Normal/Spot-Accum/Funding/Forex pour un meme actif) — sans
            # ca, un slot occupe par Funding affichait "position deja
            # ouverte" sous l etiquette Spot-Accum, laissant croire a tort
            # que Spot-Accum lui-meme avait un trade ouvert. Confirme par
            # un cas reel (NEAR : Spot-Accum disait "deja ouverte", le
            # trade reel apparaissait sous Funding).
            snap["blocker"] = (f"position deja ouverte (occupee par {occ_strat})" if occ_strat != mode
                                else "position deja ouverte")
            return
        if not self._gate_active_or_auto_activate(ticker, 100, mode):
            snap["blocker"] = "actif non selectionne pour ce mode"
            return
        gate = self._hours_block(mode) or self._market_quality_block(ticker, state, mode)
        if gate:
            snap["blocker"] = gate
            return
        if self.info is None:
            snap["blocker"] = "donnees Hyperliquid indisponibles"
            return
        v = self.mtf_view(ticker, price)
        if not v["ok"]:
            snap["blocker"] = v["why"]
            return
        self._funnel_note(mode, ticker, "eval")
        # v4.341 — SUR DEMANDE EXPLICITE : pas d entree sur des bougies ecartees du reel.
        if not v.get("data_fresh", True) and cfg.get("MTF_BLOCK_ON_STALE_DATA", 1):
            snap["blocker"] = (f"donnees {v['major_tf']} perimees (derniere bougie cloturee : "
                               f"{datetime.utcfromtimestamp(v['last_closed_t'] / 1000).strftime('%d/%m %H:%M')} UTC) — entree suspendue")
            return
        _lbl = {"4h": "H4", "1d": "Daily", "15m": "M15", "1h": "H1"}
        M, L = _lbl.get(v["major_tf"], v["major_tf"]), _lbl.get(v["lower_tf"], v["lower_tf"])
        want = "haussiere" if long_side else "baissiere"
        snap["situation"] = f"tendance {M} {v['trend']}"
        # v4.329 — SUR DEMANDE EXPLICITE (calibrage) : la tendance stricte
        # (prix > EMA50 > EMA200) devient "neutre" pile quand le prix recule
        # sous l EMA50 — c est-a-dire au moment exact ou Spot-Accum veut
        # acheter (repli sur support). Constate : 9 des 10 actifs "neutres"
        # etaient DANS leur zone de support, donc bloques par ca. "OU" justifie
        # par l objectif (acheter les replis / vendre les rebonds) : tendance
        # stricte OU structure intacte avec repli (EMA50 > EMA200 ET prix >
        # EMA200 pour un long ; miroir pour un short). Le prix repassant sous
        # l EMA200 (vraie rupture) reste bloque. Les SORTIES gardent la tendance
        # stricte (v["trend"]) : aucun changement de ce cote.
        _ef, _es = v.get("ema_fast"), v.get("ema_slow")
        if long_side:
            _pullback = _ef is not None and _es is not None and _ef > _es and price > _es
        else:
            _pullback = _ef is not None and _es is not None and _ef < _es and price < _es
        _strict = v["trend"] == want
        # v4.348 — SUR DEMANDE EXPLICITE : exigence de tendance REGLABLE par mode (profil d activite).
        # 0 = comportement d origine (stricte ou repli) ; 1 = on refuse seulement une structure H4 OPPOSEE
        # (long : EMA50 < EMA200 ET prix < EMA200 ; short : l inverse) ; 2 = aucune exigence. Les modes 1 et 2
        # permettent d acheter / vendre aussi dans une tendance neutre ou meme contraire, avec la meme
        # taille calculee sur le risque et les memes sorties.
        _tmode = int(cfg.get(f"{prefix}_TREND_MODE", 0) or 0)
        _opp_struct = (_ef is not None and _es is not None and
                       ((_ef < _es and price < _es) if long_side else (_ef > _es and price > _es)))
        if _tmode >= 2:
            _trend_ok = True
        elif _tmode == 1:
            _trend_ok = not _opp_struct
        else:
            _trend_ok = _strict or (cfg.get("MTF_ENTRY_ALLOW_PULLBACK_TREND", 1) and _pullback)
        if not _trend_ok:
            _ema_txt = (f" (prix {price:.6g} · EMA50 {_ef:.6g} · EMA200 {_es:.6g})"
                        if _ef is not None and _es is not None else "")
            snap["blocker"] = f"tendance {M} {v['trend']} (requis : {want}){_ema_txt}"
            self._funnel_shadow(mode, ticker, price, v, long_side)
            return
        self._funnel_note(mode, ticker, "tendance")
        _opposite_label = "baissiere" if long_side else "haussiere"
        # une entree prise alors que la tendance stricte est DEJA opposee ne doit pas etre fermee aussitot
        # par la sortie "tendance retournee" : elle est desactivee pour CETTE position
        mtf_trend_exit_off = bool(_tmode >= 1 and (v["trend"] == _opposite_label or _opp_struct))
        if _strict:
            trend_txt = want
        elif _trend_ok and _tmode >= 1 and not (_strict or _pullback):
            trend_txt = f"{v['trend']} (exigence assouplie : {'aucune' if _tmode >= 2 else 'pas opposee'})"
        else:
            trend_txt = f"{want} (repli sous l EMA50)" if long_side else f"{want} (rebond sur l EMA50)"
        zone = v["support"] if long_side else v["resistance"]
        target = v["resistance"] if long_side else v["support"]
        # v4.307 — FIX BUG CONFIRME : nearest_zones() (mtf_analysis.py) peut
        # renvoyer LA MEME zone comme support ET resistance quand le prix
        # est a l interieur d une zone unique (elle satisfait les deux
        # conditions low<=price ET high>=price a la fois). Sans ce
        # correctif, "target" recevait alors cette meme zone, et
        # plan_trade() calculait un objectif silencieusement invalide
        # (target_zone["low"] > price presque toujours faux dans ce cas),
        # retombant sur le repli R:R minimal SANS AUCUNE trace dans le
        # diagnostic — confirme sur 8 actifs reels (AAVE, BTC, CRV, ETH,
        # INJ, PENDLE, SUSHI, WIF) qui affichaient support == resistance.
        # Traite desormais explicitement ce cas comme "pas de zone opposee
        # distincte" (target=None), pour que le repli R:R minimal se
        # declenche de facon EXPLICITE et intentionnelle plutot que par
        # effet de bord.
        if target is zone:
            target = None
        inside = v["in_support"] if long_side else v["in_resistance"]
        kind = "support" if long_side else "resistance"
        # v4.321 — SUR DEMANDE EXPLICITE : chemin d entree DISTINCT ("OU"
        # justifie, pas une condition de plus empilee sur le premier) pour
        # une tendance qui s eloigne de toute zone H4 testee — un vrai
        # support/resistance peut mettre des ANNEES a revenir pendant que
        # le prix progresse (cas reel observe : SEI +15% en quelques
        # heures, zone H4 la plus proche a 3.85% de distance, jamais
        # atteinte). S appuie sur le support ascendant / la resistance
        # descendante (1h, deja existants — _compute_rising_support /
        # _compute_falling_resistance) comme point d entree, avec un
        # objectif propre et justifie : l amplitude du swing PRECEDENT,
        # projetee depuis ce nouveau point d appui (mesure de mouvement).
        # Comme cet objectif est un vrai niveau (pas un repli arbitraire),
        # il satisfait de lui-meme MTF_REQUIRE_REAL_TARGET_ZONE (v4.308) —
        # aucun bypass necessaire, chaque chemin est cohérent sur ses
        # propres termes.
        mtf_path = "zone"
        if not inside:
            mm = self._measured_move_level(state, "long" if long_side else "short")
            mm_ok = False
            if mm is not None and cfg.get("MTF_TREND_PATH_ENABLED", 1):
                entry_level, mm_target = mm
                tol = v["atr"] * cfg.get("MTF_TREND_ZONE_TOLERANCE_ATR", 0.25)
                if (abs(price - entry_level) <= tol and
                        ((long_side and mm_target > price) or (not long_side and mm_target < price))):
                    zone = {"low": entry_level - tol, "high": entry_level + tol, "touches": 1}
                    target = {"low": mm_target, "high": mm_target, "touches": 1}
                    kind = "support ascendant" if long_side else "resistance descendante"
                    mtf_path = "tendance"
                    mm_ok = True
            if not mm_ok:
                dist = (abs(price - (zone["high"] if long_side else zone["low"])) / price * 100) if zone else None
                # v4.328 — diagnostic : POURQUOI le chemin "tendance" ne s active pas
                if not cfg.get("MTF_TREND_PATH_ENABLED", 1):
                    tp_txt = "chemin tendance desactive"
                elif mm is None:
                    tp_txt = f"chemin tendance : {self._mm_reason(state, 'long' if long_side else 'short')}"
                else:
                    d_atr = abs(price - mm[0]) / v["atr"] if v["atr"] else None
                    tp_txt = (f"chemin tendance : niveau 1h a {d_atr:.2f} ATR H4 (tolerance {cfg.get('MTF_TREND_ZONE_TOLERANCE_ATR', 0.25)})"
                              if d_atr is not None else "chemin tendance : ATR indisponible")
                snap["blocker"] = (f"tendance {M} {want} — attente du prix dans une zone de {kind} {M} "
                                   f"(plus proche : {self._zone_txt(zone)}" + (f", a {dist:.2f} %)" if dist is not None else ")")
                                   + f" · {tp_txt}")
                return
        elif target is None and cfg.get("MTF_TREND_PATH_ENABLED", 1):
            # v4.328 — zone UNIQUE (aucune zone opposee distincte) : au lieu de
            # bloquer (MTF_REQUIRE_REAL_TARGET_ZONE), essaie l objectif par
            # MESURE DE MOUVEMENT (swing precedent projete) — un vrai niveau
            # issu de la structure, pas un R:R arbitraire. Constate : 5 des 9
            # actifs en zone de support sur le dernier releve etaient en zone
            # unique, donc bloques meme si le signal de bougie apparaissait.
            mm2 = self._measured_move_level(state, "long" if long_side else "short")
            if mm2 is not None:
                _lvl2, _tgt2 = mm2
                if (long_side and _tgt2 > price) or (not long_side and _tgt2 < price):
                    target = {"low": _tgt2, "high": _tgt2, "touches": 1}
                    mtf_path = "zone+mesure"
        self._funnel_note(mode, ticker, "zone")
        lower = self._mtf_candles(ticker, v["lower_tf"], 60)
        name, extreme = mtf.candle_signal(lower, "long" if long_side else "short")
        if not name and cfg.get("MTF_DOUBLE_PATTERN_ENABLED", 0):      # v4.352 — figures a deux points (double creux / sommet)
            name, extreme = mtf.double_pattern(lower, "long" if long_side else "short")
        if not name:
            snap["blocker"] = (f"prix dans la zone de {kind} {M} {self._zone_txt(zone)} — attente d un signal de bougie "
                               f"{'ou de figure ' if cfg.get('MTF_DOUBLE_PATTERN_ENABLED', 0) else ''}{L}")
            return
        # v4.352 — SUR DEMANDE EXPLICITE (Swing, prise de position chirurgicale) : un signal sur bougie H4 reste
        # "le dernier" pendant 4 heures ; sans limite on pourrait entrer 3 h 59 apres la figure, a un prix qui n a
        # plus rien a voir. MTF_SIGNAL_MAX_AGE_SEC > 0 : entree seulement dans les N secondes qui suivent la cloture.
        _max_age = int(cfg.get("MTF_SIGNAL_MAX_AGE_SEC", 0) or 0)
        if _max_age > 0 and lower:
            _age = time.time() - (lower[-1]["t"] / 1000 + self._TF_SEC.get(v["lower_tf"], 900))
            if _age > _max_age:
                snap["blocker"] = (f"signal {L} ({name}) trop ancien : bougie cloturee il y a {_age / 60:.0f} min "
                                   f"(maximum {_max_age / 60:.0f} min : entree a la cloture)")
                if (long_side and extreme <= zone["high"] + v["tolerance"]) or (not long_side and extreme >= zone["low"] - v["tolerance"]):
                    self._swing_log(mode, ticker, long_side, price, zone, target, extreme, v, name, lower,
                                    "filtre : signal hors fenetre d entree (trop ancien)", age=_age)   # v4.359
                return
        # le motif doit avoir touche la zone
        if (long_side and extreme > zone["high"] + v["tolerance"]) or (not long_side and extreme < zone["low"] - v["tolerance"]):
            snap["blocker"] = f"signal {L} ({name}) hors de la zone de {kind}"
            return
        self._funnel_note(mode, ticker, "signal")
        if self._funnel_flow_ok(ticker, price, long_side)[1]:
            self._funnel_note(mode, ticker, "signal_flux")
        # v4.304 — SUR DEMANDE EXPLICITE : la bougie {L} seule ne suffit plus
        # — exige aussi que la structure H1 (pente de l EMA courte) confirme
        # le retournement, au-dela d une seule bougie qui peut etre un
        # rebond sans lendemain (observe sur le lot du 26/09 : 7 LONG
        # altcoins pris dans une baisse generale, tous stoppes).
        if cfg.get("MTF_REQUIRE_H1_STRUCTURE", 1):
            h1_candles = self._mtf_candles(ticker, "1h", 60)
            ema_period = cfg.get("MTF_H1_STRUCTURE_EMA_PERIOD", 20)
            h1_closes = [c["c"] for c in h1_candles]
            ema_now = mtf.ema(h1_closes, ema_period)
            ema_prev = mtf.ema(h1_closes[:-1], ema_period) if len(h1_closes) > ema_period else None
            # v4.329 — SUR DEMANDE EXPLICITE (calibrage) : la pente de l EMA20 H1
            # ne monte presque jamais AU MOMENT d un repli sur support (c est
            # justement la que l entree a lieu) : ce filtre unique a bloque 3
            # signaux sur 3 vus dans les releves (BTC, AAVE, OP). "OU" justifie
            # par l objectif — le rebond est confirme si (a) l EMA20 H1 repart
            # dans le sens du trade, OU (b) le prix a deja repris la moyenne H1
            # (au-dessus de l EMA20 H1 pour un long, en dessous pour un short).
            _ema_dir = (ema_now is not None and ema_prev is not None and
                        (ema_now > ema_prev if long_side else ema_now < ema_prev))
            _reclaimed = ema_now is not None and ((price > ema_now) if long_side else (price < ema_now))
            h1_ok = _ema_dir or _reclaimed
            if not h1_ok:
                snap["blocker"] = (f"signal {L} ({name}) mais structure H1 pas encore confirmee "
                                   f"(EMA{ema_period} H1 ne {'monte' if long_side else 'baisse'} pas ET prix "
                                   f"{'sous' if long_side else 'au-dessus de'} l EMA{ema_period} H1)")
                self._blocked_note(mode, ticker, "h1", long_side, price, zone, target, extreme, v["atr"])
                return
            self._funnel_note(mode, ticker, "h1")
        # v4.304 — SUR DEMANDE EXPLICITE : exige aussi que le flux reel de
        # transactions (achat/vente agressif, deja calcule ailleurs pour la
        # tolerance du trailing) ne soit pas hostile au sens du trade —
        # filtre un rebond sur bougie sans vraie pression acheteuse/vendeuse
        # reelle derriere. Prudence : donnee indisponible = trade bloque
        # (pas ignore silencieusement) quand la confirmation est exigee.
        flow = self._compute_trade_flow_pressure(ticker, price)
        if cfg.get("MTF_REQUIRE_FLOW_CONFIRM", 1):
            min_flow = cfg.get("MTF_MIN_FLOW_PRESSURE", 0.0)
            req = min_flow if long_side else -min_flow
            # v4.328 — CORRECTION DE CALIBRAGE : "flux indisponible" (moins de 20
            # transactions ou moins de 5 000 $ echanges sur 180 s) n est PAS un
            # flux defavorable — c est un manque de donnees, frequent sur les
            # actifs peu liquides (constate : SUSHI et INJ, signal M15 haussier
            # en zone de support, bloques uniquement pour cette raison ; ~11
            # actifs sur 29 sans donnee au meme instant). Bloquer sur ce cas
            # (choix de v4.304) excluait justement les actifs calmes ; le score
            # reste neutre (50) pour le flux. Repasser MTF_FLOW_MISSING_BLOCKS
            # a 1 pour l ancien comportement.
            if flow is None:
                flow_ok = not cfg.get("MTF_FLOW_MISSING_BLOCKS", 0)
            else:
                flow_ok = (flow >= req) if long_side else (flow <= req)
            if not flow_ok:
                snap["blocker"] = (f"signal {L} ({name}) mais flux de transactions "
                                   f"{'indisponible' if flow is None else f'{flow:+.2f}'} "
                                   f"(requis {'>=' if long_side else '<='} {req:+.2f})")
                self._blocked_note(mode, ticker, "flux", long_side, price, zone, target, extreme, v["atr"])
                self._swing_log(mode, ticker, long_side, price, zone, target, extreme, v, name, lower,
                                "filtre : flux de transactions hostile", flow=flow)   # v4.359
                return
        plan = mtf.plan_trade("long" if long_side else "short", price, zone, target, extreme, v["atr"])
        if not plan:
            snap["blocker"] = "plan de trade invalide"
            self._swing_log(mode, ticker, long_side, price, zone, target, extreme, v, name, lower, "filtre : plan de trade invalide", flow=flow)
            return
        if plan["risk_pct"] > cfg.get("MTF_MAX_RISK_PCT", 4.0):
            snap["blocker"] = f"SL trop eloigne ({plan['risk_pct']:.2f} % > {cfg.get('MTF_MAX_RISK_PCT', 4.0)} %)"
            self._funnel_rr_note(mode, ticker, "sl_far", plan)
            self._blocked_note(mode, ticker, "sl_far", long_side, price, zone, target, extreme, v["atr"], plan=plan)
            self._swing_log(mode, ticker, long_side, price, zone, target, extreme, v, name, lower,
                            "filtre : stop trop eloigne", plan=plan, flow=flow)   # v4.359
            return
        min_rr = cfg.get("MTF_MIN_RR", 1.5)
        self._funnel_note(mode, ticker, "sl_ok")          # v4.332 : plan valide, SL pas trop eloigne
        if plan["rr"] is not None:
            self._funnel_note(mode, ticker, "objectif")   # v4.332 : un objectif reel existe
        if plan["rr"] is None:
            # v4.308 — SUR DEMANDE EXPLICITE : auparavant, l absence de zone
            # opposee distincte (voir v4.307, "zone unique") declenchait un
            # repli automatique vers un objectif synthetique (min_rr x le
            # risque), non rattache a aucune structure de marche reelle —
            # jugee peu fiable (rien ne garantit que le prix reagira
            # precisement a ce niveau arbitraire). Bloque desormais ce cas
            # au lieu de trader avec un objectif synthetique.
            if cfg.get("MTF_REQUIRE_REAL_TARGET_ZONE", 1):
                snap["blocker"] = (f"aucune zone {'de resistance' if long_side else 'de support'} distincte "
                                    f"pour un objectif reel (zone unique support/resistance) — trade ignore")
                self._funnel_rr_note(mode, ticker, "sans_objectif", plan)
                self._blocked_note(mode, ticker, "sans_objectif", long_side, price, zone, target, extreme, v["atr"], plan=plan)
                self._swing_log(mode, ticker, long_side, price, zone, target, extreme, v, name, lower,
                                "filtre : aucun objectif reel (zone opposee)", plan=plan, flow=flow)   # v4.359
                return
            # repli conserve si MTF_REQUIRE_REAL_TARGET_ZONE est desactive
            plan["tp"] = price * (1 + plan["risk_pct"] * min_rr / 100) if long_side else price * (1 - plan["risk_pct"] * min_rr / 100)
            plan["reward_pct"], plan["rr"] = plan["risk_pct"] * min_rr, min_rr
        elif plan["rr"] < min_rr:
            snap["blocker"] = f"rapport gain/risque {plan['rr']:.2f} < {min_rr} (objectif {kind} oppose trop proche)"
            self._funnel_rr_note(mode, ticker, "rr", plan)
            self._blocked_note(mode, ticker, "rr", long_side, price, zone, target, extreme, v["atr"], plan=plan)
            self._swing_log(mode, ticker, long_side, price, zone, target, extreme, v, name, lower,
                            "filtre : gain/risque insuffisant", plan=plan, flow=flow)   # v4.359
            return
        # taille calculee sur le RISQUE : perte au SL = MTF_RISK_PCT du capital
        capital = self.cfg.get("CAPITAL_USD", 100.0)
        if self._effective_mode(mode) == "live" and getattr(self, "live_equity_real", None):
            capital = self.live_equity_real
        notional = min(capital * cfg.get("MTF_RISK_PCT", 0.5) / plan["risk_pct"], cfg.get("MTF_MAX_NOTIONAL_USD", 30.0))
        snap["blocker"] = None
        self._funnel_note(mode, ticker, "candidat")
        self._blocked_note(mode, ticker, "candidat", long_side, price, zone, target, extreme, v["atr"], plan=plan)
        _swing_key = self._swing_log(mode, ticker, long_side, price, zone, target, extreme, v, name, lower,
                                     "candidat (non ouvert)", prio=2, plan=plan, flow=flow)    # v4.359
        label = ("🌊 Swing long" if long_side else "🌊 Swing short") if _swing_key else \
                ("🌱 Spot-Accumulation" if long_side else "🎯 Accumulation (short)")
        flow_txt = f"flux {flow:+.2f}" if flow is not None else "flux indisponible"
        # v4.314 — SUR DEMANDE EXPLICITE : score composite de priorisation —
        # "comportement plus large" de l actif, pas seulement les donnees de
        # l instant T. 5 facteurs, chacun ramene a une echelle 0-100 puis
        # pondere (poids configurables, voir CONFIG). Un facteur dont la
        # donnee est indisponible (flux non mesure, historique insuffisant)
        # reste NEUTRE (50) plutot que de penaliser ou avantager par defaut.
        rr_score = min(max((plan["rr"] - min_rr) / max(4.0 - min_rr, 0.01), 0.0), 1.0) * 100
        flow_score = min(abs(flow), 1.0) * 100 if flow is not None else 50.0
        zone_score = min(zone.get("touches", 1) / 10.0, 1.0) * 100
        hist = self._asset_win_rates.get(ticker)
        hist_score = hist["win_rate"] if hist else 50.0
        mq = self.market_quality(ticker, state)
        quality_penalty = 0.0
        if mq.get("spread_pct") is not None:
            quality_penalty += min(mq["spread_pct"] * 1000, 50)
        if mq.get("activity_ratio") is not None and mq["activity_ratio"] < 0.3:
            quality_penalty += 20
        quality_score = max(0.0, 100.0 - quality_penalty)
        confidence = (
            cfg.get("MTF_SCORE_WEIGHT_RR", 0.40) * rr_score +
            cfg.get("MTF_SCORE_WEIGHT_FLOW", 0.20) * flow_score +
            cfg.get("MTF_SCORE_WEIGHT_ZONE", 0.15) * zone_score +
            cfg.get("MTF_SCORE_WEIGHT_HISTORY", 0.15) * hist_score +
            cfg.get("MTF_SCORE_WEIGHT_QUALITY", 0.10) * quality_score
        )
        score_txt = (f"score {confidence:.0f} (R:R {rr_score:.0f} · flux {flow_score:.0f} · zone {zone_score:.0f} · "
                     f"historique {hist_score:.0f}{'' if hist else ' (neutre, echantillon insuffisant)'} · marche {quality_score:.0f})")
        path_lbl = {"tendance": "top-down (tendance, mesure de mouvement)", "zone+mesure": "top-down (zone unique, objectif par mesure de mouvement)"}.get(mtf_path, "top-down")
        path = (f"{path_lbl} : tendance {M} {trend_txt} · zone de {kind} {M} {self._zone_txt(zone)} · signal {L} : {name} · {flow_txt} · "
                f"SL {plan['sl']:.6g} (-{plan['risk_pct']:.2f} %) · objectif {plan['tp']:.6g} (+{plan['reward_pct']:.2f} %, R:R {plan['rr']:.1f}) · {score_txt}")
        reasons = [f"{label} — voie : {path}", f"RSI {rsi:.1f}" if rsi is not None else "RSI ?"]
        cand = {"symbol": symbol, "ticker": ticker, "state": state if long_side else pos_state,
                "signal": "long" if long_side else "short", "price": price, "confidence": confidence,
                "rsi": rsi, "rsi_mode": mode, "reasons": reasons, "prices": prices, "conf_breakdown": {},
                "strategy": mode, "countertrend": False, "force_paper": False, "entered_via_flirt": False,
                "engine": "mtf", "mtf_sl": plan["sl"], "mtf_tp": plan["tp"], "mtf_notional": notional,
                "mtf_path": mtf_path,  # v4.321 — "zone" (H4 classique) ou "tendance" (mesure de mouvement)
                "mtf_trend_exit_off": mtf_trend_exit_off,  # v4.348
                "mtf_swing": bool(_swing_key), "swing_key": _swing_key,   # v4.359 : trade du profil Swing
                "swing_info": ({"pattern": name, "trend": v.get("trend"), "zone": [zone.get("low"), zone.get("high")],
                                "touches": zone.get("touches"), "risk_pct": plan["risk_pct"], "rr": plan.get("rr"),
                                "reward_pct": plan.get("reward_pct")} if _swing_key else None)}
        if long_side:
            cand.update({"support_at_entry": zone["low"], "resistance_at_entry": target["high"] if target else None,
                         "mtf_zone_bound": zone["low"]})  # v4.309 — borne de la zone, pour la cassure structurelle
            self._pending_spot_accum_candidates.append(cand)
        else:
            cand.update({"entered_via_range": False, "support": target["low"] if target else None, "resistance": zone["high"],
                         "mtf_zone_bound": zone["high"]})  # v4.309 — borne de la zone, pour la cassure structurelle
            self._pending_accumulation_candidates.append(cand)

    def _manage_mtf(self, symbol, price, state, pos, ticker, mode):
        """Gestion d une position "top-down" : SL sur la structure, objectif
        sur la zone opposee, SL remonte au prix d entree a +1R, puis suiveur a
        1R du meilleur prix au-dela de +2R."""
        cfg = self.cfg
        long_side = pos["type"] == "long"
        r = pos.get("mtf_r") or abs(pos["entry"] - pos["sl"])
        gain = (price - pos["entry"]) if long_side else (pos["entry"] - price)
        best = pos.get("mtf_best", pos["entry"])
        best = max(best, price) if long_side else min(best, price)
        pos["mtf_best"] = best
        # v4.301 — FIX BUG CRITIQUE : le pic de PnL latent (peak_pnl_usd /
        # absolute_peak_pnl_usd) n etait JAMAIS mis a jour pour les positions
        # "top-down" (engine == "mtf") — _manage_position_impl retourne ICI
        # (voir plus haut, ligne ~7351) AVANT d atteindre le bloc generique
        # de suivi du pic (v4.18), reserve aux autres moteurs. Consequence :
        # close_position() ne trouvait jamais de pic memorise pour un trade
        # top-down, et enregistrait systematiquement peak_pnl_usd/
        # peak_pnl_pct a NULL en base, meme sur un trade brievement en
        # profit avant de repartir en perte. Reproduit ici, en miroir exact
        # de la logique generique (v4.18), a partir du gain deja calcule
        # ci-dessus (levier toujours x1 sur ce moteur, voir _finalize_open).
        pnl_usd_now = pos["size"] * pos.get("leverage", 1) * gain / pos["entry"]
        if pnl_usd_now > 0 and (state.peak_pnl_usd is None or pnl_usd_now > state.peak_pnl_usd):
            state.peak_pnl_usd = pnl_usd_now
            state.absolute_peak_pnl_usd = pnl_usd_now
        reason = None
        # v4.311 — SUR DEMANDE EXPLICITE : ferme IMMEDIATEMENT si la tendance
        # H4 D ORIGINE (celle qui justifiait le trade) s est completement
        # RETOURNEE (pas juste neutre — l inverse exact) pendant que la
        # position est ouverte, au lieu d attendre le SL/la cassure de zone.
        # Verifie en priorite (avant meme la cassure de zone) : un
        # retournement complet de la tendance de fond est l invalidation la
        # plus fondamentale, independante d ou se trouve le prix par rapport
        # a la zone ou au SL. cache_only=True : jamais d appel reseau
        # bloquant, relit juste les bougies H4 deja en cache (rafraichies a
        # chaque cloture naturelle de bougie).
        if cfg.get("MTF_CLOSE_ON_TREND_REVERSAL", 1) and not pos.get("mtf_trend_exit_off"):   # v4.348
            want = "haussiere" if long_side else "baissiere"
            opposite = "baissiere" if long_side else "haussiere"
            mv = self.mtf_view(ticker, price, cache_only=True)
            # v4.341 : uniquement si l historique H4 est a jour (voir _refresh_held_mtf_data)
            if mv and mv.get("ok") and mv.get("data_fresh", True) and mv["trend"] == opposite:
                reason = "TENDANCE RETOURNEE (H4)"
        # v4.309 — SUR DEMANDE EXPLICITE : cassure structurelle de la zone
        # elle-meme (support pour un long, resistance pour un short) —
        # generalement ATTEINTE AVANT le SL classique, puisque celui-ci
        # inclut en plus l extreme de la bougie de signal et une marge ATR
        # (donc plus loin de l entree que la zone seule). Confirme par un
        # cas reel (AAVE : zone de support 155.3-156.91, SL a 154.0127 —
        # le prix avait deja casse la zone bien avant d atteindre le SL,
        # sans qu aucune sortie ne se declenche). Sortie IMMEDIATE des que
        # le prix passe la borne, y compris intra-bougie (pas d attente
        # d une bougie cloturee) — voulu explicitement, quitte a sortir sur
        # une simple meche.
        zone_bound = pos.get("mtf_zone_bound")
        if reason:
            pass
        elif cfg.get("MTF_CLOSE_ON_ZONE_BREAK", 1) and zone_bound is not None and (
            (long_side and price < zone_bound) or (not long_side and price > zone_bound)
        ):
            reason = "STRUCTURE CASSEE (zone top-down)"
        elif (long_side and price <= pos["sl"]) or (not long_side and price >= pos["sl"]):
            reason = "STOP LOSS (zone top-down)" if not pos.get("mtf_be_done") else "STOP SUIVEUR (top-down)"
        elif pos.get("tp") and ((long_side and price >= pos["tp"]) or (not long_side and price <= pos["tp"])):
            reason = "TAKE PROFIT (zone opposee)"
        if reason:
            _result = self._safe_close_position(state, price, reason, ticker, pos, symbol, mode)
            if _result is None:
                return
            pnl, _, trade = _result
            self.emit("trade", trade)
            self.emit("log", {"msg": f"[{ticker}] {reason} @ ${price:.6g} | PnL: ${pnl:.2f}", "level": "win" if pnl > 0 else "loss"})
            self._save_open_positions()
            return
        new_sl = None
        if r > 0 and gain >= cfg.get("MTF_BREAKEVEN_AT_R", 1.0) * r and not pos.get("mtf_be_done"):
            fee_pad = pos["entry"] * 0.001
            new_sl = pos["entry"] + fee_pad if long_side else pos["entry"] - fee_pad
            pos["mtf_be_done"] = True
            self.emit("log", {"msg": f"[{ticker}] Top-down : +1R atteint — SL remonte au prix d entree ({new_sl:.6g})", "level": "signal"})
        if r > 0 and pos.get("mtf_be_done") and abs(best - pos["entry"]) >= cfg.get("MTF_TRAIL_FROM_R", 2.0) * r:
            trail = best - r if long_side else best + r
            if (long_side and trail > pos["sl"]) or (not long_side and trail < pos["sl"]):
                new_sl = trail
        # v4.340 — SUR DEMANDE EXPLICITE (cas TIA : pic +2.93 % rendu en entier). Entre
        # +1R et +2R, la seule protection etait l entree : avec un stop initial large
        # (R de 1.5 a 2.9 %), le suiveur (a partir de +2R) ne s armait jamais et le
        # gain revenait a zero. Verrou PROGRESSIF : une fois le breakeven atteint (donc
        # jamais avant +1R), le SL monte pour garantir MTF_LOCK_FRACTION du gain maximal
        # (0.5 = la moitie du pic). Il ne fait que MONTER (long) / DESCENDRE (short) et
        # se combine avec le suiveur ci-dessus (le plus favorable l emporte). 0 = desactive.
        _lock_frac = cfg.get("MTF_LOCK_FRACTION", 0.5)
        if r > 0 and pos.get("mtf_be_done") and _lock_frac > 0:
            _mfe = (best - pos["entry"]) if long_side else (pos["entry"] - best)
            if _mfe >= cfg.get("MTF_BREAKEVEN_AT_R", 1.0) * r:
                _lock = pos["entry"] + _lock_frac * _mfe if long_side else pos["entry"] - _lock_frac * _mfe
                _cur = new_sl if new_sl is not None else pos["sl"]
                if (long_side and _lock > _cur) or ((not long_side) and _lock < _cur):
                    new_sl = _lock
        # v4.393 — SUR DEMANDE EXPLICITE (cas UNI : pic +4,15 % rendu pour moitie ; pics +1,4 % et +1,9 % sortis en perte). Le verrou
        # ci-dessus s arme en R : avec un stop large (R de 3 a 4 %), il n armait qu a +3 % de gain. Ce verrou-ci s arme en % du prix :
        # des que le meilleur gain atteint MTF_PCT_LOCK_ARM_PCT, le SL garantit une part (MTF_PCT_LOCK_FRACTION, plus haute au-dela de
        # MTF_PCT_LOCK_BIG_PCT) du pic, jamais moins que MTF_PCT_LOCK_FLOOR_PCT. Il ne fait que MONTER (long) / DESCENDRE (short).
        if cfg.get("MTF_PCT_LOCK_ON", 1) and pos["entry"] > 0:
            _mfe_pct = ((best - pos["entry"]) if long_side else (pos["entry"] - best)) / pos["entry"] * 100
            if _mfe_pct >= cfg.get("MTF_PCT_LOCK_ARM_PCT", 1.0):
                _f = cfg.get("MTF_PCT_LOCK_BIG_FRACTION", 0.75) if _mfe_pct >= cfg.get("MTF_PCT_LOCK_BIG_PCT", 3.0) else cfg.get("MTF_PCT_LOCK_FRACTION", 0.6)
                _lp = max(cfg.get("MTF_PCT_LOCK_FLOOR_PCT", 0.15), _f * _mfe_pct)
                _lvl = pos["entry"] * (1 + _lp / 100) if long_side else pos["entry"] * (1 - _lp / 100)
                _cur = new_sl if new_sl is not None else pos["sl"]
                if (long_side and _lvl > _cur) or ((not long_side) and _lvl < _cur):
                    new_sl = _lvl
                    if not pos.get("mtf_be_done"):
                        pos["mtf_be_done"] = True            # libelle de sortie « STOP SUIVEUR » et suivi R actif
                        self.emit("log", {"msg": f"[{ticker}] Top-down : pic +{_mfe_pct:.2f} % — SL verrouillé à +{_lp:.2f} % ({_lvl:.6g})", "level": "signal"})
        if new_sl is not None and new_sl != pos["sl"]:
            pos["sl"] = new_sl
            # v4.340 : en live, ne pousse le SL natif que s il a bouge d au moins
            # MTF_SL_PUSH_MIN_STEP_PCT depuis le dernier envoi (evite un appel API a
            # chaque tick). En paper, pos["sl"] est mis a jour a chaque tick, sans delai.
            _pushed = pos.get("mtf_sl_pushed")
            _step = cfg.get("MTF_SL_PUSH_MIN_STEP_PCT", 0.05)
            if mode == "live" and self.exchange is not None and (
                    _pushed is None or abs(new_sl - _pushed) / pos["entry"] * 100 >= _step):
                try:
                    update_sl_on_hyperliquid(self.exchange, self.info, self.cfg.get("WALLET_ADDRESS"), ticker,
                                             {"type": pos["type"], "entry": pos["entry"], "size": pos["size"] * pos.get("leverage", 1)},
                                             new_sl, cfg)
                    pos["mtf_sl_pushed"] = new_sl
                except Exception as e:
                    print(f"[MTF] Mise a jour du SL natif {ticker} impossible : {e}")
            self._save_open_positions()

    def _simple_entry(self, mode, symbol, ticker, price, rsi, prices, state, pos_state):
        """v4.289 — MOTEUR D ENTREE SIMPLE (Spot-Accum et Accumulation).

        Remplace la longue chaine de conditions (une quinzaine, toutes
        eliminatoires, dont six mesuraient la tendance) qui n etaient
        presque jamais reunies en meme temps — ou trop tard. Trois etapes :

          1. GARDE-FOUS (protegent le capital) : mode actif, actif
             selectionne, plage horaire, qualite du marche (si activee),
             situation compatible (jamais a contre-tendance de fond), etoile
             filante en cours (achats).
          2. UN SIGNAL parmi trois : proche d un niveau (structure 1h ou
             5 min), cassure fraiche, continuation (tendance franche).
             En "fin de repli / fin de rebond" : niveau 1h uniquement.
          3. UNE CONFIRMATION : le flux ne contredit pas l entree (et, en fin
             de repli / rebond, il la confirme : retournement).

        Supprimes : stabilite 12 cycles, ADX, anti-range, couleur de bougie,
        fourchette S/R minimale, seuil de confiance appris, voies fausse
        cassure / volume / tendance persistante / etoile filante (doublons).
        Restent dans l ouverture : plafond de positions, delai apres perte,
        limite de rafales, conflit entre modes."""
        cfg = self.cfg
        long_side = mode == "spot_accumulation"
        prefix = "SPOT_ACCUM" if long_side else "ACCUMULATION"
        snap = {"ts": time.time(), "enabled": cfg.get(f"{prefix}_ENABLED", False), "engine": "simple"}
        if long_side:
            state.spot_accum_gate_snapshot = snap
        else:
            pos_state.accumulation_gate_snapshot = snap
        if not snap["enabled"]:
            snap["blocker"] = "mode desactive"
            return
        if pos_state.position:
            occ_strat = pos_state.position.get("strategy") or "?"
            # v4.320 — voir la meme correction dans _mtf_entry ci-dessus.
            snap["blocker"] = (f"position deja ouverte (occupee par {occ_strat})" if occ_strat != mode
                                else "position deja ouverte")
            return
        if not self._gate_active_or_auto_activate(ticker, 100, mode):
            snap["blocker"] = "actif non selectionne pour ce mode"
            return
        gate = self._hours_block(mode) or self._market_quality_block(ticker, state, mode)
        if gate:
            snap["blocker"] = gate
            return
        if long_side and cfg.get("SHOOTING_STAR_DETECTION_ENABLED", True) and getattr(state, "shooting_star_pending_close", None) is not None:
            snap["blocker"] = "etoile filante (signal baissier) en cours de confirmation"
            return

        # ── Situation ──────────────────────────────────────────────────
        sit = self.situation(state, price)
        situation = sit["name"]
        snap["situation"] = situation
        against = "baisse saine" if long_side else "hausse saine"
        reversal = "repli dans une hausse" if long_side else "rebond dans une baisse"   # fin de repli / fin de rebond
        countertrend = "rebond dans une baisse" if long_side else "repli dans une hausse"
        if situation == against:
            snap["blocker"] = f"{against} : fond (1h) ET court terme (5 min) contraires"
            return
        if situation == countertrend and not cfg.get("SITUATION_ALLOW_COUNTERTREND", 0):
            snap["blocker"] = f"{countertrend} : trade a contre-tendance desactive"
            return
        if situation == "fond neutre" and sit.get("court") != ("haussier" if long_side else "baissier"):
            snap["blocker"] = f"fond neutre et court terme {sit.get('court') or '?'}"
            return

        # ── Signal ─────────────────────────────────────────────────────
        side = "support" if long_side else "resistance"
        struct = self._structural_support(state) if long_side else self._structural_resistance(state)
        short_lvl = self._short_level(state, side)
        dyn = getattr(state, "dynamic_trend_support" if long_side else "dynamic_trend_resistance", None)
        if situation == reversal:
            cands = [(f"{side} de structure 1h", struct)]
        else:
            cands = [(f"{side} de tendance 1h", dyn), (f"{side} de structure 1h", struct), (f"{side} 5 min", short_lvl)]
        lvl_label, lvl = self._first_near_level(state, price, cands, side)
        fp = self._compute_trade_flow_pressure(ticker, price_now=price)
        snap["entry_flow_pressure"] = fp
        path = force_paper = None
        if lvl_label:
            art = "du" if long_side else "de la"
            path = (f"fin de {'repli' if long_side else 'rebond'} sur {lvl_label} ${lvl:.6g}" if situation == reversal
                    else f"proche {art} {lvl_label} ${lvl:.6g}")
        elif situation != reversal:
            if self._detect_fresh_breakout(state, "long" if long_side else "short", cfg.get(f"{prefix}_BREAKOUT_LOOKBACK_CANDLES", 30)):
                path = "cassure fraiche"
            elif cfg.get("CONTINUATION_ENABLED", 1):
                ok, detail = self._continuation_check(ticker, state, price, "long" if long_side else "short")
                snap["continuation_detail"] = detail
                if ok:
                    path = f"continuation ({detail})"
                    force_paper = bool(cfg.get("CONTINUATION_PAPER_ONLY", 1))
        if not path:
            seen, parts = set(), []
            for lab, v in cands:
                if v and round(v, 10) not in seen:
                    seen.add(round(v, 10))
                    parts.append(f"{lab} ${v:.6g}")
            listed = " / ".join(parts) or "aucun niveau"
            extra = f" ; continuation : {snap['continuation_detail']}" if snap.get("continuation_detail") else ""
            snap["blocker"] = f"aucun signal : pas proche d {'un support' if long_side else 'une resistance'} ({listed}), pas de cassure fraiche{extra}"
            return

        # ── Confirmation par le flux ───────────────────────────────────
        veto = cfg.get("ENTRY_FLOW_CONTRADICTION_THRESHOLD", 0.3)
        if fp is not None and (fp <= -veto if long_side else fp >= veto):
            snap["blocker"] = f"flux contraire ({fp:+.2f})"
            return
        if situation == reversal:
            need = cfg.get("SITUATION_REVERSAL_MIN_FLOW", 0.2)
            if fp is None or (fp < need if long_side else fp > -need):
                snap["blocker"] = f"{reversal} : le flux ne confirme pas encore le retournement ({'?' if fp is None else f'{fp:+.2f}'})"
                return
        confirm_min = cfg.get(f"{prefix}_ENTRY_FLOW_CONFIRM_MIN")
        if confirm_min:
            if fp is None or (fp < confirm_min if long_side else fp > -confirm_min):
                snap["blocker"] = f"flux insuffisant ({'?' if fp is None else f'{fp:+.2f}'}, requis {'+' if long_side else '-'}{confirm_min})"
                return

        # ── Candidat (la confiance ne sert qu au classement) ───────────
        flow_bonus = max(0.0, (fp if long_side else -fp)) * 15 if fp is not None else 0.0
        confidence = min(70.0 + flow_bonus + (5.0 if lvl_label else 0.0), 90.0)
        snap["confidence"] = round(confidence, 1)
        snap["blocker"] = None
        label = "🌱 Spot-Accumulation" if long_side else "🎯 Accumulation (short)"
        reasons = [f"{label} — voie : {path} [situation : {situation}]",
                   f"flux {fp:+.2f}" if fp is not None else "flux ?",
                   f"RSI {rsi:.1f}" if rsi is not None else "RSI ?"]
        cand = {"symbol": symbol, "ticker": ticker, "state": pos_state if not long_side else state,
                "signal": "long" if long_side else "short", "price": price, "confidence": confidence,
                "rsi": rsi, "rsi_mode": mode, "reasons": reasons, "prices": prices, "conf_breakdown": {},
                "strategy": mode, "countertrend": situation == countertrend, "force_paper": bool(force_paper),
                # v4.295 — FIX RISQUE : "entered_via_flirt" declenche le levier
                # DYNAMIQUE 2-5x de Spot-Accum/Accumulation. Le moteur simple le
                # mettait a vrai pour toute entree pres d un niveau : le
                # notionnel des trades est passe de ~15-20 $ a 35-180 $ (SL a
                # -0,87 $ sur NEAR). Levier dynamique desormais OPTIONNEL
                # (desactive par defaut : levier 1, comme avant).
                "entered_via_flirt": bool(lvl_label) and bool(cfg.get("SIMPLE_ENGINE_DYNAMIC_LEVERAGE", 0))}
        if long_side:
            cand.update({"support_at_entry": lvl if lvl_label else short_lvl, "resistance_at_entry": self._short_level(state, "resistance")})
            self._pending_spot_accum_candidates.append(cand)
        else:
            cand.update({"entered_via_range": False, "support": self._short_level(state, "support"),
                         "resistance": lvl if lvl_label else short_lvl})
            self._pending_accumulation_candidates.append(cand)

    def _check_accumulation_signal(self, symbol, ticker, price, support, resistance,
                                    rsi, momentum_pct, ema200, trend_up, trend_down,
                                    prices, state, accum_state):
        """v4.191 — SUR DEMANDE EXPLICITE : Accumulation devient l OPPOSE de
        Spot-Accum — SHORT uniquement, avec la tendance generale baissiere
        (EMA200), flirt avec la RESISTANCE + bougie ROUGE (baissiere) pour
        confirmer l entree. SL structurel sur rupture confirmee de la
        resistance (deja generalise, voir _structural_sl_broken). Levier
        dynamique 2-5x sur une entree via flirt (voir
        _compute_accumulation_dynamic_leverage), x1 sur cassure fraiche.
        L ancienne logique (LONG+SHORT, fenetre de proximite precise,
        detecteur de range/mode range) est retiree d ici — le mode range
        devient son PROPRE mode independant (_check_range_signal)."""
        if self.cfg.get("ENTRY_ENGINE_MTF", 1):  # v4.299 — moteur top-down multi-unites de temps
            return self._mtf_entry("accumulation", symbol, ticker, price, rsi, prices, state, accum_state)
        if self._supsw_reserved(ticker):      # v4.375 : memes regles avec les anciens moteurs
            return
        if self.cfg.get("ENTRY_ENGINE_SIMPLE", 1):  # v4.289 — moteur simple (0 = ancienne chaine)
            return self._simple_entry("accumulation", symbol, ticker, price, rsi, prices, state, accum_state)
        cfg = self.cfg
        snap = {"ts": time.time(), "enabled": cfg.get("ACCUMULATION_ENABLED", False)}
        accum_state.accumulation_gate_snapshot = snap
        if not cfg.get("ACCUMULATION_ENABLED", False):
            snap["blocker"] = "mode desactive"
            return
        if not self._gate_active_or_auto_activate(ticker, 100, "accumulation"):
            snap["blocker"] = "actif non selectionne pour ce mode"
            return
        rules_ac = cfg.get("SITUATION_RULES_ENABLED", 1)
        _gate_v4276 = ((None if rules_ac else self._regime_blocks("accumulation")) or self._hours_block("accumulation")
                       or self._market_quality_block(ticker, state, "accumulation"))  # v4.276 / v4.281 / v4.286
        if _gate_v4276:
            snap["blocker"] = _gate_v4276
            return

        # v4.286 — SITUATION DE MARCHE (miroir de Spot-Accum) :
        #  baisse saine          -> toutes les voies, niveaux 1h ET 5 min
        #  rebond dans une baisse-> seulement "fin de rebond" : short sur la
        #                           resistance de STRUCTURE 1h, bougie baissiere
        #                           et flux vendeur (retournement)
        #  repli dans une hausse -> contre-tendance : resistance 5 min, flux
        #                           vendeur franc, marge au-dessus du support
        #                           1h, trailing resserre
        #  hausse saine          -> aucun short
        sit_ac = self.situation(state, price)
        situation_ac = sit_ac["name"]
        snap["situation"] = situation_ac
        finrebond_ac = countertrend_ac = False
        if rules_ac:
            if situation_ac == "hausse saine":
                snap["blocker"] = "hausse saine : fond (1h) ET court terme (5 min) haussiers"
                return
            finrebond_ac = situation_ac == "rebond dans une baisse"
            countertrend_ac = situation_ac == "repli dans une hausse"
            if countertrend_ac and not cfg.get("SITUATION_ALLOW_COUNTERTREND", 0):
                # v4.287 — SUR DEMANDE EXPLICITE : contre-tendance retiree
                snap["blocker"] = "repli dans une hausse : short a contre-tendance desactive"
                return
        snap["trend_down"] = trend_down
        breakout_lookback_ac = cfg.get("ACCUMULATION_BREAKOUT_LOOKBACK_CANDLES", 30)
        fresh_breakout_ac = self._detect_fresh_breakout(state, "short", breakout_lookback_ac)
        snap["fresh_breakout"] = fresh_breakout_ac

        # v4.219 — SUR DEMANDE EXPLICITE : chemin d entree DEDIE, base
        # UNIQUEMENT sur une forte confirmation de volume pendant une
        # consolidation DEJA detectee — permet d entrer PENDANT
        # l accumulation elle-meme, sans attendre qu une tendance ou une
        # cassure de prix se manifeste. Volontairement CONSERVATEUR : exige
        # (1) un range genuinement detecte (pas juste calme par hasard),
        # (2) un volume recent tres nettement eleve (1.5x, plus strict que
        # le bonus de confiance 1.15x), (3) le prix proche du bord
        # resistance de ce range (coherent avec Accumulation = short). Si
        # ces 3 conditions sont reunies, contourne l exigence de tendance
        # EMA200/stabilite/ADX (naturellement non etablies pendant une
        # vraie accumulation, par definition).
        is_ranging_ac_early = self._is_market_ranging(state, cfg.get("ACCUMULATION_ANTI_RANGE_MIN_PCT", 2.0), cfg.get("ACCUMULATION_ANTI_RANGE_LOOKBACK", 30))
        volume_breakout_ac = False
        if cfg.get("ACCUMULATION_VOLUME_ENTRY_ENABLED", True) and is_ranging_ac_early and support is not None and resistance is not None and resistance > support:
            vol_confirms_entry = self._volume_confirms_accumulation(state, min_ratio=cfg.get("ACCUMULATION_VOLUME_ENTRY_MIN_RATIO", 1.5))
            near_resistance_early = self._is_near_level_atr(state, price, resistance, cfg.get("ENTRY_ATR_PROXIMITY_MULTIPLIER", 1.0))
            volume_breakout_ac = bool(vol_confirms_entry) and near_resistance_early
        snap["volume_breakout"] = volume_breakout_ac

        # v4.221 — SUR DEMANDE EXPLICITE : cassure ratee, meme principe que
        # Forex — signal fort et INDEPENDANT de l EMA200/ADX.
        failed_breakout_ac = False
        if cfg.get("FAILED_BREAKOUT_DETECTION_ENABLED", True) and resistance is not None:
            failed_breakout_ac = self._detect_failed_breakout(state, "short", resistance, cfg.get("FAILED_BREAKOUT_LOOKBACK_CANDLES", 20), ticker=ticker)
        snap["failed_breakout"] = failed_breakout_ac

        # v4.227 — SUR DEMANDE EXPLICITE : etoile filante ROUGE confirmee
        # sur 30 min — meme esprit que la cassure ratee (signal fort,
        # INDEPENDANT de l EMA200/ADX), bypass ces exigences.
        shooting_star_ac = False
        if cfg.get("SHOOTING_STAR_DETECTION_ENABLED", True):
            shooting_star_ac = self._shooting_star_confirmed(state, ticker=ticker)
        snap["shooting_star_confirmed"] = shooting_star_ac

        # v4.240 — SUR DEMANDE EXPLICITE : pression directionnelle soutenue
        # (flux de transactions reel) — voir Forex pour le raisonnement
        # complet.
        trend_persistence_ac = False
        if cfg.get("TREND_PERSISTENCE_ENABLED", True):
            trend_persistence_ac = self._trend_persistence_confirmed(state, "short")
        snap["trend_persistence_confirmed"] = trend_persistence_ac

        # v4.277 — meme correctif que Spot-Accum : les voies de contournement
        # ne permettent plus de shorter CONTRE une tendance haussiere, et
        # passent elles aussi par le veto du flux.
        if finrebond_ac:  # v4.286 — en rebond, seule la "fin de rebond" sur la structure 1h est autorisee
            fresh_breakout_ac = volume_breakout_ac = failed_breakout_ac = shooting_star_ac = trend_persistence_ac = False
        bypass_ac = fresh_breakout_ac or volume_breakout_ac or failed_breakout_ac or shooting_star_ac or trend_persistence_ac
        # v4.279 — meme exception que Spot-Accum pour la cassure fraiche
        # (vers le bas) contre une EMA200 encore haussiere.
        fresh_counter_ac = False
        if (not trend_down and fresh_breakout_ac and cfg.get("ACCUMULATION_FRESH_BREAKOUT_COUNTER_TREND", 0)
                and cfg.get("ACCUMULATION_BYPASS_REQUIRE_TREND", 1)):
            regime_ac = self.market_regime().get("regime") if cfg.get("MARKET_REGIME_FILTER_ENABLED", 1) else "neutre"
            fp_fresh_ac = self._compute_trade_flow_pressure(ticker, price_now=price)
            min_fp = cfg.get("FRESH_BREAKOUT_COUNTER_TREND_MIN_FLOW", 0.2)
            snap["entry_flow_pressure"] = fp_fresh_ac
            if regime_ac in ("neutre", "baissier") and fp_fresh_ac is not None and fp_fresh_ac <= -min_fp:
                fresh_counter_ac = True
            else:
                why = (f"regime {regime_ac}" if regime_ac not in ("neutre", "baissier")
                       else f"flux vendeur insuffisant ({fp_fresh_ac:+.2f} > -{min_fp})" if fp_fresh_ac is not None
                       else "flux indisponible")
                snap["blocker"] = f"cassure fraiche contre la tendance EMA200 refusee ({why})"
                return
        snap["fresh_breakout_counter_trend"] = fresh_counter_ac
        if not trend_down and not finrebond_ac and not fresh_counter_ac and (cfg.get("ACCUMULATION_BYPASS_REQUIRE_TREND", 1) or not bypass_ac):
            snap["blocker"] = "pas de tendance baissiere (EMA200)" + (" — signal de cassure/rejet ignore contre la tendance" if bypass_ac else "")
            return
        if bypass_ac and cfg.get("ACCUMULATION_BYPASS_FLOW_VETO", 1) and cfg.get("ENTRY_FLOW_CONFIRM_ENABLED", True):
            fp_bypass_ac = self._compute_trade_flow_pressure(ticker, price_now=price)
            snap["entry_flow_pressure"] = fp_bypass_ac
            if fp_bypass_ac is not None and fp_bypass_ac >= cfg.get("ENTRY_FLOW_CONTRADICTION_THRESHOLD", 0.3):
                snap["blocker"] = f"flux acheteur contredit l entree par cassure/rejet (pression {fp_bypass_ac:+.2f})"
                return

        min_stability_cycles = cfg.get("ACCUMULATION_TREND_STABILITY_CYCLES", 24)
        snap["trend_down_streak"] = state.trend_down_streak
        snap["min_stability_cycles"] = min_stability_cycles
        if state.trend_down_streak < min_stability_cycles and not finrebond_ac and not fresh_breakout_ac and not volume_breakout_ac and not failed_breakout_ac and not shooting_star_ac and not trend_persistence_ac:
            snap["blocker"] = f"tendance trop recente ({state.trend_down_streak}/{min_stability_cycles} cycles)"
            return
        if support is None or resistance is None or support <= 0:
            snap["blocker"] = "support/resistance indisponible"
            return

        if cfg.get("ACCUMULATION_REQUIRE_ADX_CONFIRM", False) and not finrebond_ac and not fresh_breakout_ac and not volume_breakout_ac and not failed_breakout_ac and not shooting_star_ac and not trend_persistence_ac:
            adx_local = calc_adx(list(state.mtf_prices) if len(state.mtf_prices) >= (cfg.get("ADX_PERIOD", 14)*2+1) else prices, cfg.get("ADX_PERIOD", 14))
            adx_threshold = cfg.get("ADX_TREND_THRESHOLD", 25.0)
            snap["adx"] = round(adx_local, 1) if adx_local is not None else None
            snap["adx_threshold"] = adx_threshold
            if adx_local is None or adx_local < adx_threshold:
                snap["blocker"] = f"ADX {snap['adx']} < {adx_threshold} (tendance pas assez forte)"
                return

        is_ranging_ac = self._is_market_ranging(state, cfg.get("ACCUMULATION_ANTI_RANGE_MIN_PCT", 2.0), cfg.get("ACCUMULATION_ANTI_RANGE_LOOKBACK", 30))
        snap["is_ranging"] = is_ranging_ac
        # v4.279 — FIX CONTRADICTION : la voie "volume en consolidation" EXIGE
        # ce meme range (meme calcul, memes reglages) — elle ne pouvait donc
        # JAMAIS aboutir. Idem pour la cassure fraiche, qui sort d une
        # consolidation. Le filtre anti-range ne s applique plus a ces voies.
        if is_ranging_ac and not fresh_breakout_ac and not volume_breakout_ac:
            snap["blocker"] = self._anti_range_text(state, cfg.get("ACCUMULATION_ANTI_RANGE_MIN_PCT", 2.0), cfg.get("ACCUMULATION_ANTI_RANGE_LOOKBACK", 30))
            return

        dist_below_resistance_pct = (resistance - price) / (resistance - support) * 100 if resistance != support else 0
        snap["dist_below_resistance_pct"] = round(dist_below_resistance_pct, 2)
        if not fresh_breakout_ac and not failed_breakout_ac and not shooting_star_ac and not trend_persistence_ac:
            # v4.215 — SUR DEMANDE EXPLICITE : utilise desormais le S/R
            # ancre au dernier retournement confirme (tendance dynamique,
            # voir _update_dynamic_trend) au lieu du S/R sur fenetre
            # glissante fixe — celui-ci pouvait "bouger sous les pieds" du
            # prix sans rapport avec un vrai changement de marche (un
            # nouveau plus bas ailleurs dans la fenetre suffisait a
            # deplacer le niveau, meme si le prix lui-meme etait stable).
            # Repli sur l ancien calcul si la tendance dynamique n est pas
            # encore etablie (historique 1h insuffisant).
            # v4.286 — NIVEAUX CHOISIS SELON LA SITUATION (miroir de Spot-Accum)
            dyn_res = state.dynamic_trend_resistance if state.dynamic_trend_resistance is not None else resistance
            if finrebond_ac:
                cands_ac = [("resistance de structure 1h", self._structural_resistance(state))]
            elif countertrend_ac:
                cands_ac = [("resistance 5 min", self._short_level(state, "resistance"))]
            elif situation_ac == "baisse saine":
                cands_ac = [("resistance de tendance 1h", dyn_res), ("resistance descendante 1h", getattr(state, "falling_resistance", None)),
                            ("resistance 5 min", self._short_level(state, "resistance"))]
            else:
                cands_ac = [("resistance de tendance 1h", dyn_res)]
            level_label_ac, level_ac = self._first_near_level(state, price, cands_ac, "resistance")
            near_resistance = level_label_ac is not None
            snap["entry_level"] = f"{level_label_ac} ${level_ac:.6g}" if level_ac else None
            if volume_breakout_ac:
                near_resistance = True  # v4.279 — deja verifie par la voie "volume"
            snap["near_resistance"] = near_resistance
            if not near_resistance and situation_ac == "baisse saine" and cfg.get("CONTINUATION_ENABLED", 1):
                cont_ok, cont_detail = self._continuation_check(ticker, state, price, "short")
                snap["continuation_detail"] = cont_detail
                if cont_ok:
                    near_resistance = True
                    snap["continuation"] = True
            if not near_resistance:
                listed = " / ".join(f"{lab} ${lvl:.6g}" for lab, lvl in cands_ac if lvl)
                extra = f" — continuation refusee : {snap['continuation_detail']}" if snap.get("continuation_detail") else ""
                snap["blocker"] = f"pas assez proche d une resistance ({listed or 'aucun niveau disponible'}){extra}"
                return
            if cfg.get("REQUIRE_ENTRY_CANDLE_COLOR", True):
                bearish_now = self._is_candle_bearish_now(state)
                if bearish_now is False:
                    snap["blocker"] = "bougie actuelle non baissiere"
                    return
            # v4.246 — SUR DEMANDE EXPLICITE : confirmation IMMEDIATE par le
            # VRAI flux de transactions — une seule bougie rouge pres de la
            # resistance peut n etre qu un soubresaut ponctuel, pas un vrai
            # debut de baisse (confirme par un motif recurrent : entrees
            # Accumulation qui repartent immediatement a la hausse apres
            # ouverture, declenchant le plafond de securite). Contrairement
            # a la pression SOUTENUE (10 min) utilisee comme contournement
            # complet ailleurs, ceci est une lecture UNIQUE et immediate —
            # ne bloque QUE si le flux contredit CLAIREMENT la baisse
            # attendue (achat net agressif), sans exiger une confirmation
            # parfaite.
            if cfg.get("ENTRY_FLOW_CONFIRM_ENABLED", True):
                flow_pressure = self._compute_trade_flow_pressure(ticker, price_now=price)
                snap["entry_flow_pressure"] = flow_pressure
                if flow_pressure is not None and flow_pressure >= cfg.get("ENTRY_FLOW_CONTRADICTION_THRESHOLD", 0.3):
                    snap["blocker"] = f"flux de transactions contredit la baisse (pression achat {flow_pressure:+.2f})"
                    return
                # v4.266 — confirmation OPTIONNELLE (0 = desactivee) ; v4.269 — reglage propre au mode
                confirm_min = cfg.get("ACCUMULATION_ENTRY_FLOW_CONFIRM_MIN")
                if confirm_min is None:
                    confirm_min = cfg.get("ENTRY_FLOW_CONFIRM_MIN_PRESSURE", 0.0) or 0.0
                if confirm_min > 0:
                    if flow_pressure is None:
                        snap["blocker"] = "confirmation flux exigee mais flux insuffisant (activite recente trop faible)"
                        return
                    if flow_pressure > -confirm_min:
                        snap["blocker"] = f"flux vendeur insuffisant (pression {flow_pressure:+.2f}, requis <= {-confirm_min:+.2f})"
                        return
        snap["entered_via_flirt"] = not fresh_breakout_ac and not volume_breakout_ac and not failed_breakout_ac and not shooting_star_ac and not trend_persistence_ac

        # v4.203/225 — SUR DEMANDE EXPLICITE : confirmation par MACD 1h +
        # tendance dynamique (Accumulation = short) — ETAIT un blocage dur,
        # converti en BONUS DE CONFIANCE non-bloquant suite a une
        # regression observee (Spot-Accum devenu moins reactif sur de
        # vraies hausses crypto, la tendance dynamique horaire pouvant
        # rester temporairement dans le mauvais sens meme au sein d une
        # tendance de fond plus large deja favorable). Ne bloque plus
        # JAMAIS — ajoute simplement au score de confiance quand elle
        # confirme.
        # v4.286 — conditions renforcees selon la situation
        if finrebond_ac or countertrend_ac:
            need_ac = cfg.get("SITUATION_REVERSAL_MIN_FLOW", 0.2) if finrebond_ac else cfg.get("SITUATION_COUNTERTREND_MIN_FLOW", 0.3)
            fp_sit_ac = self._compute_trade_flow_pressure(ticker, price_now=price)
            snap["entry_flow_pressure"] = fp_sit_ac
            if fp_sit_ac is None or fp_sit_ac > -need_ac:
                snap["blocker"] = (f"{situation_ac} : flux vendeur insuffisant "
                                   f"({'indisponible' if fp_sit_ac is None else f'{fp_sit_ac:+.2f}'} > -{need_ac})")
                return
        if countertrend_ac:
            sup1h_ac = self._structural_support(state)
            room_ac = (price - sup1h_ac) / price * 100 if sup1h_ac and sup1h_ac < price else None
            need_room = cfg.get("SITUATION_MIN_ROOM_PCT", 1.0)
            if room_ac is not None and room_ac < need_room:
                snap["blocker"] = f"repli dans une hausse : support 1h trop proche ({room_ac:.2f}% < {need_room}%)"
                return
        dynamic_trend_confirms_ac = False
        if cfg.get("DYNAMIC_TREND_CONFIRM_ENABLED", True) and state.dynamic_trend_direction == "down":
            macd_line, signal_line = self._compute_macd_1h(state)
            snap["macd_1h"] = round(macd_line, 6) if macd_line is not None else None
            snap["macd_1h_signal"] = round(signal_line, 6) if signal_line is not None else None
            if macd_line is not None and signal_line is not None and macd_line < signal_line:
                dynamic_trend_confirms_ac = True
        snap["dynamic_trend_confirms"] = dynamic_trend_confirms_ac

        sr_range = resistance - support
        min_below_pct = cfg.get("ACCUMULATION_MIN_BELOW_RESISTANCE_PCT", 5.0)
        position_in_range_pct = ((resistance - price) / sr_range * 100) if sr_range > 0 else 50.0
        confidence = 65.0 + min(max(position_in_range_pct - min_below_pct, 0) / 50.0 * 20.0, 20.0)
        # v4.218 — SUR DEMANDE EXPLICITE : renfort de confiance si le VRAI
        # volume confirme un etat d accumulation genuine (volume recent
        # eleve vs base plus longue) — surtout pertinent pendant une phase
        # de range detectee (is_ranging_ac), ou l on veut distinguer une
        # vraie accumulation d une simple derive sans interet. N EST
        # JAMAIS un blocage — juste un bonus de confiance quand disponible
        # et confirme, sans effet si l historique 1h est insuffisant.
        volume_confirms = self._volume_confirms_accumulation(state) if is_ranging_ac else None
        snap["volume_confirms_accumulation"] = volume_confirms
        if volume_confirms:
            confidence += cfg.get("ACCUMULATION_VOLUME_CONFIRM_BONUS", 5.0)
        if dynamic_trend_confirms_ac:
            confidence += cfg.get("DYNAMIC_TREND_CONFIRM_BONUS", 5.0)
        # v4.248 — SUR DEMANDE EXPLICITE : bonus de confiance proportionnel
        # a l intensite de la cassure ratee, quand c est elle qui a valide
        # l entree — une cassure ratee franche merite plus de confiance
        # qu une a peine au-dessus du minimum requis.
        if failed_breakout_ac:
            confidence += getattr(state, "failed_breakout_intensity", 0.5) * cfg.get("FAILED_BREAKOUT_MAX_CONFIDENCE_BONUS", 10.0)
        confidence = min(confidence, 85.0)
        snap["confidence"] = round(confidence, 1)

        conf_threshold = self._get_confidence_threshold(ticker)
        snap["confidence_threshold"] = conf_threshold
        if confidence < conf_threshold:
            snap["blocker"] = f"confiance {confidence:.0f}% < seuil {conf_threshold:.0f}%"
            return

        snap["blocker"] = None

        if snap.get("continuation"):
            path_ac = f"continuation ({snap.get('continuation_detail')})"
        elif finrebond_ac:
            path_ac = f"fin de rebond sous {snap.get('entry_level')}"
        elif fresh_breakout_ac:
            path_ac = "cassure fraiche" + (" (contre-tendance, flux vendeur)" if snap.get("fresh_breakout_counter_trend") else "")
        elif failed_breakout_ac:
            path_ac = "fausse cassure"
        elif shooting_star_ac:
            path_ac = "etoile filante"
        elif trend_persistence_ac:
            path_ac = "tendance persistante"
        elif volume_breakout_ac:
            path_ac = "volume en consolidation"
        else:
            path_ac = f"proche de la {snap.get('entry_level') or 'resistance'}"
        if countertrend_ac:
            path_ac = f"repli dans une hausse (contre-tendance) — {path_ac}"
        path_ac += f" [situation : {situation_ac}]"
        reasons = [
            f"🎯 Accumulation (short) — voie : {path_ac}",
            f"{dist_below_resistance_pct:.2f}% sous la resistance, tendance baissiere confirmee",
            f"RSI {rsi:.1f}" if rsi is not None else "RSI ?",
        ]

        self._pending_accumulation_candidates.append({
            "symbol": symbol, "ticker": ticker, "state": accum_state, "signal": "short",
            "price": price, "confidence": confidence, "rsi": rsi, "rsi_mode": "accumulation",
            "reasons": reasons, "prices": prices, "conf_breakdown": {},
            "strategy": "accumulation", "entered_via_range": False, "countertrend": countertrend_ac,  # v4.286
            "force_paper": bool(snap.get("continuation")) and bool(cfg.get("CONTINUATION_PAPER_ONLY", 1)),  # v4.287
            "support": support, "resistance": resistance,
            "entered_via_flirt": snap.get("entered_via_flirt", False),
        })

    @staticmethod
    def _quality_gate_name(txt):
        """v4.346 — nom court du filtre de qualite de marche qui a bloque (pour le suivi des entrees bloquees)."""
        t = (txt or "").lower()
        if "spread" in t:
            return "quality_spread"
        if "trop actif" in t:
            return "quality_too_active"
        if "deserte" in t or "endormi" in t:
            return "quality_too_calm"
        return "quality_other"

    def _funding_shadow_note(self, ticker, direction, price, gate):
        """v4.346 — SUR DEMANDE EXPLICITE (LECTURE SEULE) : enregistre une entree Funding pour le suivi
        "signaux bloques" : gate = "candidat" (entree generee) ou le filtre qualite qui l a bloquee. Un stop
        hypothetique FUNDING_SHADOW_SL_PCT sert a mesurer "+1R avant SL" de facon identique pour les deux
        groupes. N a aucun effet sur les entrees."""
        try:
            sl_pct = float(self.cfg.get("FUNDING_SHADOW_SL_PCT", 0.7))
            long_side = direction == "long"
            sl = price * (1 - sl_pct / 100) if long_side else price * (1 + sl_pct / 100)
            plan = {"sl": sl, "tp": None, "risk_pct": sl_pct, "rr": None, "reward_pct": None}
            self._blocked_note("funding_contrarian", ticker, gate, long_side, price, None, None, None, None, plan=plan)
        except Exception:
            pass

    def _check_funding_contrarian_signal(self, symbol, ticker, price, rsi, prices, state):
        """v4.346 — enveloppe de suivi : quand le filtre de qualite du marche bloque une entree Funding, la
        fonction d origine est rejouee SANS ce filtre (tous les autres filtres inchanges) pour savoir si
        l entree aurait ete prise ; si oui, elle est enregistree comme "bloquee par le filtre" et son
        issue sera mesuree 2 h plus tard. Le diagnostic affiche reste la raison du blocage. Aucun effet sur
        les entrees (le candidat n est JAMAIS ajoute dans ce mode)."""
        cfg = self.cfg
        q_text = None
        if cfg.get("FUNDING_SHADOW_TRACKING", 1) and cfg.get("FUNDING_MODE_ENABLED", False):
            try:
                if not self._hours_block("funding_contrarian"):
                    q_text = self._market_quality_block(ticker, state, "funding_contrarian")
            except Exception:
                q_text = None
        if not q_text:
            return self._check_funding_contrarian_signal_impl(symbol, ticker, price, rsi, prices, state)
        try:
            self._check_funding_contrarian_signal_impl(symbol, ticker, price, rsi, prices, state,
                                                       skip_quality=self._quality_gate_name(q_text))
        except Exception as e:
            print(f"[FUNDING-SHADOW] erreur ignoree : {e}")
        finally:
            snap = getattr(state, "funding_gate_snapshot", None)
            if isinstance(snap, dict):
                snap["blocker"] = q_text       # le diagnostic montre toujours la vraie raison du blocage
                snap.pop("direction", None)

    def _check_funding_contrarian_signal_impl(self, symbol, ticker, price, rsi, prices, state, skip_quality=None):
        """v4.33 — Mode FUNDING CONTRARIAN : source de signal FONDAMENTALEMENT
        DIFFERENTE de RSI/MACD/EMA (deja integres dans les prix par des
        acteurs plus rapides que ce bot). Le funding rate reflete un vrai
        desequilibre de position entre traders a effet de levier — quand il
        est extreme, ca signale un positionnement sur-leverage dans un sens,
        propice a un retour a la moyenne : SHORT si funding tres positif
        (trop de LONG en levier paient les SHORT), LONG si tres negatif
        (l inverse). AUCUNE garantie que cet avantage soit reel ici — a
        valider par les resultats reels (voir strategy="funding_contrarian"
        dans l historique), pas suppose d avance.
        SECURITE EXPLICITE : reste cantonne au paper trading tant que
        FUNDING_MODE_LIVE_ALLOWED n est pas active manuellement (voir
        _finalize_open), meme si le bot tourne par ailleurs en mode live.
        """
        cfg = self.cfg
        # v4.289 — diagnostic : raison de chaque non-entree (Funding n en avait
        # aucun, impossible de savoir pourquoi il ne tradait plus)
        snap = {"ts": time.time()}
        state.funding_gate_snapshot = snap
        if not cfg.get("FUNDING_MODE_ENABLED", False):
            snap["blocker"] = "mode desactive"
            return
        if not self._gate_active_or_auto_activate(ticker, 100, "funding_contrarian"):
            snap["blocker"] = "actif non selectionne pour ce mode"
            return  # actif desactive (Marches) ou exclu manuellement
        _gate_f = self._hours_block("funding_contrarian") or (None if skip_quality else self._market_quality_block(ticker, state, "funding_contrarian"))
        if _gate_f:
            snap["blocker"] = _gate_f
            return  # v4.276 — hors plage horaire du mode ; v4.281 — qualite du marche

        if cfg.get("FUNDING_EXCLUDE_WEAK_COINS", 1) and ticker in (cfg.get("FUNDING_WEAK_COINS", ["GMX", "RENDER"]) or []):
            snap["blocker"] = "actif exclu du Funding (resultats negatifs, test v4.374)"
            return

        hourly_rate = self.funding_rates.get(ticker)
        if hourly_rate is None:
            snap["blocker"] = "taux de financement pas encore recu"
            return  # pas encore de donnee de funding pour cet actif

        annual_pct = hourly_rate * 24 * 365 * 100  # annualise, en %
        threshold = cfg.get("FUNDING_ANNUAL_THRESHOLD_PCT", 25.0)
        snap["annual_pct"] = round(annual_pct, 1)

        direction = None
        if annual_pct >= threshold:
            direction = "short"  # positionnement LONG sur-leverage -> contrarian SHORT
        elif annual_pct <= -threshold:
            direction = "long"   # positionnement SHORT sur-leverage -> contrarian LONG

        if direction is None:
            snap["blocker"] = f"taux {annual_pct:+.1f}%/an, pas assez extreme (seuil +/-{threshold:.0f}%)"
            return

        # v4.374 : un funding TRES extreme annonce souvent un mouvement qui continue contre le pari (tranche 40-60 % : -1,09 $ sur 86 trades)
        max_annual = float(cfg.get("FUNDING_MAX_ANNUAL_PCT", 40.0) or 0)
        if max_annual > 0 and abs(annual_pct) > max_annual:
            snap["blocker"] = f"funding {annual_pct:+.1f}%/an au-dessus du plafond {max_annual:.0f}%"
            if not skip_quality:
                self._funding_shadow_note(ticker, direction, price, f"funding > {max_annual:.0f} %/an")
            return

        # ── Score de confiance dedie : plus le funding est extreme, plus la
        # confiance est elevee (positionnement d autant plus deforme) ──────
        excess = abs(annual_pct) - threshold
        confidence = 65.0 + min(excess / threshold * 25.0, 25.0)  # jusqu a 90% pour un exces de 100% du seuil
        confidence = min(confidence, 90.0)

        conf_threshold = self._get_confidence_threshold(ticker)
        if confidence < conf_threshold:
            snap["blocker"] = f"confiance {confidence:.0f}% < seuil appris {conf_threshold:.0f}%"
            return

        # v4.155 — SUR DEMANDE EXPLICITE : meme protection que Normal — le
        # mode Funding n a aucun outil dedie pour bien trader un marche en
        # range, bloque donc l entree si c est le cas, meme avec un signal
        # de funding par ailleurs valide.
        if cfg.get("FUNDING_REQUIRE_ANTI_RANGE", True):
            is_ranging_funding = self._is_market_ranging(state, cfg.get("FUNDING_ANTI_RANGE_MIN_PCT", 2.0), cfg.get("FUNDING_ANTI_RANGE_LOOKBACK", 200))
            if is_ranging_funding:
                snap["blocker"] = self._anti_range_text(state, cfg.get("FUNDING_ANTI_RANGE_MIN_PCT", 2.0), cfg.get("FUNDING_ANTI_RANGE_LOOKBACK", 200))
                return

        # v4.256 — SUR DEMANDE EXPLICITE : renforce l entree avec le VRAI
        # flux de transactions — Funding parie qu un positionnement extreme
        # (taux) va se retourner, mais le taux SEUL ne dit rien sur si ce
        # retournement a deja commence. Si le flux montre encore une
        # pression NETTEMENT dans le sens OPPOSE a la these (ex: encore
        # fortement achete alors qu on parie sur un retournement baissier),
        # rejette l entree — le marche ne montre aucun signe naissant que
        # la these est en train de se realiser, reduisant le hasard d une
        # entree basee sur le taux seul.
        if cfg.get("FUNDING_ENTRY_FLOW_CONFIRM_ENABLED", True):
            flow_pressure_funding = self._compute_trade_flow_pressure(ticker, price_now=price)
            if flow_pressure_funding is not None:
                contradiction_threshold = cfg.get("FUNDING_ENTRY_FLOW_CONTRADICTION_THRESHOLD", 0.3)
                contradicts = (flow_pressure_funding >= contradiction_threshold) if direction == "short" else (flow_pressure_funding <= -contradiction_threshold)
                if contradicts:
                    snap["blocker"] = f"flux contredit le pari ({flow_pressure_funding:+.2f})"
                    return

        reasons = [
            f"💰 Funding Contrarian : {annual_pct:+.1f}% annualise (seuil ±{threshold:.0f}%)",
            f"RSI {rsi:.1f}" if rsi is not None else "RSI ?",
        ]

        if skip_quality:
            # v4.346 — entree qui AURAIT ete prise sans le filtre qualite : enregistree pour le suivi, jamais ouverte
            snap["shadow_entry"] = direction
            self._funding_shadow_note(ticker, direction, price, skip_quality)
            return
        snap["blocker"] = None
        snap["direction"] = direction
        self._funding_shadow_note(ticker, direction, price, "candidat")   # v4.346 : groupe de reference
        self._pending_funding_candidates.append({
            "symbol": symbol, "ticker": ticker, "state": state, "signal": direction,
            "price": price, "confidence": confidence, "rsi": rsi, "rsi_mode": "funding_contrarian",
            "reasons": reasons, "prices": prices, "conf_breakdown": {},
            "strategy": "funding_contrarian",
        })

    def _check_spot_accumulation_signal(self, symbol, ticker, price, support, resistance,
                                          rsi, trend_up, prices, state):
        """v4.43 — SUR DEMANDE EXPLICITE : mode SPOT-ACCUMULATION — achat
        d actif dans l esprit spot ("tant que l actif existe on peut esperer
        une hausse ou garder ses actifs"). LONG uniquement, avec la tendance
        generale haussiere (EMA200) ET a au moins SPOT_ACCUM_MIN_ABOVE_SUPPORT_PCT
        au-dessus du support — on achete une tendance deja engagee, pas un
        rebond pres d un plancher (contrairement a Accumulation classique).
        AUCUNE garantie de fonctionnement — a valider par les resultats.
        v4.55 — SUR DEMANDE EXPLICITE : capture un diagnostic detaille
        (state.spot_accum_gate_snapshot) a CHAQUE etape, expose via
        /api/entry-diagnostics (qui ne couvrait jusqu ici que le mode
        normal) — pour voir precisement quelle clause bloque, sans deviner.
        """
        if self.cfg.get("ENTRY_ENGINE_MTF", 1):  # v4.299 — moteur top-down multi-unites de temps
            return self._mtf_entry("spot_accumulation", symbol, ticker, price, rsi, prices, state, state)
        if self._supsw_reserved(ticker):      # v4.375 : memes regles avec les anciens moteurs
            return
        if self.cfg.get("ENTRY_ENGINE_SIMPLE", 1):  # v4.289 — moteur simple (0 = ancienne chaine)
            return self._simple_entry("spot_accumulation", symbol, ticker, price, rsi, prices, state, state)
        cfg = self.cfg
        snap = {"ts": time.time(), "enabled": cfg.get("SPOT_ACCUM_ENABLED", False)}
        state.spot_accum_gate_snapshot = snap
        if not cfg.get("SPOT_ACCUM_ENABLED", False):
            snap["blocker"] = "mode desactive"
            return
        if not self._gate_active_or_auto_activate(ticker, 100, "spot_accumulation"):
            snap["blocker"] = "actif non selectionne pour ce mode"
            return
        rules_sa = cfg.get("SITUATION_RULES_ENABLED", 1)
        _gate_v4276 = ((None if rules_sa else self._regime_blocks("spot_accumulation")) or self._hours_block("spot_accumulation")
                       or self._market_quality_block(ticker, state, "spot_accumulation"))  # v4.276 / v4.281 / v4.286
        if _gate_v4276:
            snap["blocker"] = _gate_v4276
            return
        # v4.285 — COHERENCE entree/sortie : pas d achat pendant qu une etoile
        # filante (signal BAISSIER, qui ferme les LONG) est en cours sur l actif.
        if cfg.get("SHOOTING_STAR_DETECTION_ENABLED", True) and getattr(state, "shooting_star_pending_close", None) is not None:
            snap["blocker"] = "etoile filante (signal baissier) en cours de confirmation"
            return
        # v4.286 — SITUATION DE MARCHE : fond (1h) x court terme (5 min).
        #  hausse saine          -> toutes les voies, niveaux 1h ET 5 min
        #  repli dans une hausse -> seulement "fin de repli" : achat sur le
        #                           support de STRUCTURE 1h, avec bougie
        #                           haussiere et flux acheteur (retournement)
        #  rebond dans une baisse-> contre-tendance : support 5 min, flux
        #                           acheteur franc, marge sous la resistance
        #                           1h, trailing resserre
        #  baisse saine          -> aucun achat
        sit_sa = self.situation(state, price)
        situation_sa = sit_sa["name"]
        snap["situation"] = situation_sa
        repli_sa = rebond_sa = False
        if rules_sa:
            if situation_sa == "baisse saine":
                snap["blocker"] = "baisse saine : fond (1h) ET court terme (5 min) baissiers"
                return
            repli_sa = situation_sa == "repli dans une hausse"
            rebond_sa = situation_sa == "rebond dans une baisse"
            if rebond_sa and not cfg.get("SITUATION_ALLOW_COUNTERTREND", 0):
                # v4.287 — SUR DEMANDE EXPLICITE : contre-tendance retiree
                # (les achats contre la tendance de fond ont aggrave les pertes)
                snap["blocker"] = "rebond dans une baisse : achat a contre-tendance desactive"
                return

        snap["trend_up"] = trend_up
        # v4.150 — SUR DEMANDE EXPLICITE : meme detecteur de cassure fraiche
        # qu Accumulation (nouveau plus haut sur 1h), applique ici
        # uniquement en LONG (Spot-Accum ne fait jamais de short) — permet
        # une prise de position DES la confirmation d un debut de
        # mouvement haussier, sans attendre l EMA200 ni la duree de
        # stabilite habituellement requis.
        breakout_lookback_sa = cfg.get("SPOT_ACCUM_BREAKOUT_LOOKBACK_CANDLES", cfg.get("ACCUMULATION_BREAKOUT_LOOKBACK_CANDLES", 30))
        fresh_breakout_sa = self._detect_fresh_breakout(state, "long", breakout_lookback_sa)
        snap["fresh_breakout"] = fresh_breakout_sa

        # v4.219 — SUR DEMANDE EXPLICITE : meme chemin d entree base sur le
        # volume qu Accumulation (miroir, cote support/long) — voir
        # commentaire detaille dans _check_accumulation_signal.
        is_ranging_sa_early = self._is_market_ranging(state, cfg.get("ACCUMULATION_ANTI_RANGE_MIN_PCT", 2.0), cfg.get("ACCUMULATION_ANTI_RANGE_LOOKBACK", 30))
        volume_breakout_sa = False
        if cfg.get("ACCUMULATION_VOLUME_ENTRY_ENABLED", True) and is_ranging_sa_early and support is not None and resistance is not None and resistance > support:
            vol_confirms_entry_sa = self._volume_confirms_accumulation(state, min_ratio=cfg.get("ACCUMULATION_VOLUME_ENTRY_MIN_RATIO", 1.5))
            near_support_early = self._is_near_level_atr(state, price, support, cfg.get("ENTRY_ATR_PROXIMITY_MULTIPLIER", 1.0))
            volume_breakout_sa = bool(vol_confirms_entry_sa) and near_support_early
        snap["volume_breakout"] = volume_breakout_sa

        # v4.221 — SUR DEMANDE EXPLICITE : cassure ratee (miroir Accumulation).
        failed_breakout_sa = False
        if cfg.get("FAILED_BREAKOUT_DETECTION_ENABLED", True) and support is not None:
            failed_breakout_sa = self._detect_failed_breakout(state, "long", support, cfg.get("FAILED_BREAKOUT_LOOKBACK_CANDLES", 20), ticker=ticker)
        snap["failed_breakout"] = failed_breakout_sa

        # v4.240 — SUR DEMANDE EXPLICITE : pression directionnelle soutenue
        # (flux de transactions reel) — voir Forex pour le raisonnement
        # complet.
        trend_persistence_sa = False
        if cfg.get("TREND_PERSISTENCE_ENABLED", True):
            trend_persistence_sa = self._trend_persistence_confirmed(state, "long")
        snap["trend_persistence_confirmed"] = trend_persistence_sa

        # v4.277 — FIX (entrees perdantes du 22-23/09) : les 4 voies de
        # contournement (cassure fraiche, volume, fausse cassure, tendance
        # persistante) laissaient Spot-Accum ACHETER CONTRE une tendance
        # baissiere (EMA200), sans veto du flux ni couleur de bougie ni
        # proximite du support — soit exactement les achats de rebonds
        # rates observes (SL en quelques minutes, pic quasi nul). Elles ne
        # dispensent plus que de la STABILITE et de l ADX, jamais du SENS.
        if repli_sa:  # v4.286 — en repli, seule la "fin de repli" sur la structure 1h est autorisee
            fresh_breakout_sa = volume_breakout_sa = failed_breakout_sa = trend_persistence_sa = False
        bypass_sa = fresh_breakout_sa or volume_breakout_sa or failed_breakout_sa or trend_persistence_sa
        # v4.279 — EXCEPTION CASSURE FRAICHE : sa raison d etre est de capter
        # le DEBUT d un retournement, avant que l EMA200 (lente) ne suive. Elle
        # peut donc entrer CONTRE l EMA200, mais a des conditions plus
        # strictes que les autres voies : flux acheteur FRANC exige (et non
        # simplement "pas contraire") et regime de marche neutre ou haussier.
        fresh_counter_sa = False
        if (not trend_up and fresh_breakout_sa and cfg.get("SPOT_ACCUM_FRESH_BREAKOUT_COUNTER_TREND", 0)
                and cfg.get("SPOT_ACCUM_BYPASS_REQUIRE_TREND", 1)):  # a 0, l ancien comportement (sans condition) s applique deja
            regime_sa = self.market_regime().get("regime") if cfg.get("MARKET_REGIME_FILTER_ENABLED", 1) else "neutre"
            fp_fresh_sa = self._compute_trade_flow_pressure(ticker, price_now=price)
            min_fp = cfg.get("FRESH_BREAKOUT_COUNTER_TREND_MIN_FLOW", 0.2)
            snap["entry_flow_pressure"] = fp_fresh_sa
            if regime_sa in ("neutre", "haussier") and fp_fresh_sa is not None and fp_fresh_sa >= min_fp:
                fresh_counter_sa = True
            else:
                why = (f"regime {regime_sa}" if regime_sa not in ("neutre", "haussier")
                       else f"flux acheteur insuffisant ({fp_fresh_sa:+.2f} < +{min_fp})" if fp_fresh_sa is not None
                       else "flux indisponible")
                snap["blocker"] = f"cassure fraiche contre la tendance EMA200 refusee ({why})"
                return
        snap["fresh_breakout_counter_trend"] = fresh_counter_sa
        if not trend_up and not repli_sa and not fresh_counter_sa and (cfg.get("SPOT_ACCUM_BYPASS_REQUIRE_TREND", 1) or not bypass_sa):
            snap["blocker"] = "pas de tendance haussiere (EMA200)" + (" — signal de cassure/rebond ignore contre la tendance" if bypass_sa else "")
            return  # exige la tendance generale haussiere (EMA200)
        if bypass_sa and cfg.get("SPOT_ACCUM_BYPASS_FLOW_VETO", 1) and cfg.get("ENTRY_FLOW_CONFIRM_ENABLED", True):
            fp_bypass_sa = self._compute_trade_flow_pressure(ticker, price_now=price)
            snap["entry_flow_pressure"] = fp_bypass_sa
            if fp_bypass_sa is not None and fp_bypass_sa <= -cfg.get("ENTRY_FLOW_CONTRADICTION_THRESHOLD", 0.3):
                snap["blocker"] = f"flux vendeur contredit l entree par cassure/rebond (pression {fp_bypass_sa:+.2f})"
                return
        # v4.75 — SUR DEMANDE EXPLICITE : la tendance doit aussi etre STABLE
        # depuis un moment (pas juste vraie a l instant du signal) — evite
        # d entrer juste avant/pendant un retournement deja amorce.
        min_stability_cycles = cfg.get("SPOT_ACCUM_TREND_STABILITY_CYCLES", 24)
        snap["trend_up_streak"] = state.trend_up_streak
        snap["min_stability_cycles"] = min_stability_cycles
        if state.trend_up_streak < min_stability_cycles and not repli_sa and not fresh_breakout_sa and not volume_breakout_sa and not failed_breakout_sa and not trend_persistence_sa:
            snap["blocker"] = f"tendance trop recente ({state.trend_up_streak}/{min_stability_cycles} cycles)"
            return
        if support is None or resistance is None or support <= 0:
            snap["blocker"] = "support/resistance indisponible"
            return

        # v4.53 — SUR DEMANDE EXPLICITE : confirmation ADX de la tendance
        # (pas seulement prix > EMA200, qui peut etre franchi de justesse) —
        # meme seuil que le reste du bot (ADX_TREND_THRESHOLD, 25 par
        # defaut), calcule ici localement (pas encore disponible a ce point
        # du cycle pour la logique normale).
        if cfg.get("SPOT_ACCUM_REQUIRE_ADX_CONFIRM", True) and not repli_sa and not fresh_breakout_sa and not failed_breakout_sa and not trend_persistence_sa:
            adx_local = calc_adx(list(state.mtf_prices) if len(state.mtf_prices) >= (cfg.get("ADX_PERIOD", 14)*2+1) else prices, cfg.get("ADX_PERIOD", 14))
            adx_threshold = cfg.get("ADX_TREND_THRESHOLD", 25.0)
            snap["adx"] = round(adx_local, 1) if adx_local is not None else None
            snap["adx_threshold"] = adx_threshold
            if adx_local is None or adx_local < adx_threshold:
                snap["blocker"] = f"ADX {snap['adx']} < {adx_threshold} (tendance pas assez forte)"
                return  # tendance pas assez forte pour etre consideree "claire"

        # v4.53 — SUR DEMANDE EXPLICITE : la fourchette support-resistance
        # doit avoir une amplitude minimale (ex: support=100 -> resistance
        # >= 103 pour 3%) — evite d entrer dans un range trop plat, ou meme
        # atteindre l objectif ne rapporterait quasiment rien.
        # v4.127 — SUR DEMANDE EXPLICITE : remplace par un detecteur de
        # range DIRECT (mouvement reel du prix, mtf_prices) — juge plus
        # fiable que l amplitude S/R (calcul/interpretation conteste).
        # Neutralise via SPOT_ACCUM_REQUIRE_SR_AMPLITUDE (False par defaut).
        if cfg.get("SPOT_ACCUM_REQUIRE_SR_AMPLITUDE", False):
            min_sr_amplitude_pct = cfg.get("SPOT_ACCUM_MIN_SR_AMPLITUDE_PCT", 3.0)
            sr_amplitude_pct = (resistance - support) / support * 100
            snap["sr_amplitude_pct"] = round(sr_amplitude_pct, 2)
            snap["min_sr_amplitude_pct"] = min_sr_amplitude_pct
            if sr_amplitude_pct < min_sr_amplitude_pct:
                snap["blocker"] = f"fourchette S/R trop etroite ({sr_amplitude_pct:.2f}% < {min_sr_amplitude_pct}%)"
                return  # fourchette trop etroite, pas assez de marge de mouvement

        # v4.127 — SUR DEMANDE EXPLICITE : detecteur de range DIRECT, EN PLUS
        # de la fenetre de proximite (conservee, verifiee plus bas).
        is_ranging_sa = self._is_market_ranging(state, cfg.get("SPOT_ACCUM_ANTI_RANGE_MIN_PCT", 2.0), cfg.get("SPOT_ACCUM_ANTI_RANGE_LOOKBACK", 30))
        snap["is_ranging"] = is_ranging_sa
        # v4.279 — FIX CONTRADICTION : la cassure fraiche (sortie d une
        # consolidation) et l entree "volume en consolidation" EXIGENT un
        # marche qui vient d etre en range — le filtre anti-range les
        # annulait donc presque toujours. Il ne s applique plus a ces voies.
        if is_ranging_sa and not fresh_breakout_sa and not volume_breakout_sa:
            snap["blocker"] = self._anti_range_text(state, cfg.get("SPOT_ACCUM_ANTI_RANGE_MIN_PCT", 2.0), cfg.get("SPOT_ACCUM_ANTI_RANGE_LOOKBACK", 30))
            return

        min_above_pct = cfg.get("SPOT_ACCUM_MIN_ABOVE_SUPPORT_PCT", 5.0)
        # v4.50 — FIX : aucun plafond n existait avant — l entree pouvait se
        # produire n importe ou entre le minimum et la resistance (parfois
        # a 50-75% de la fourchette), contrairement a l intention reelle du
        # mode ("ouvrir pres du support"). Ajoute une borne haute explicite.
        # v4.108 — FIX BUG CRITIQUE : desormais exprime en % de l AMPLITUDE
        # (comme le seuil structurel du trailing, 70% de l amplitude) — pas
        # du prix du support, incoherent et pouvant techniquement autoriser
        # une entree au-dela de la resistance sur une fourchette etroite.
        # v4.176 — SUR DEMANDE EXPLICITE : nouvelle philosophie unifiee —
        # remplace la fenetre de proximite precise (5-10% de l amplitude)
        # par un "flirt" simple avec le support + confirmation par bougie
        # verte (haussiere). Coherent avec Normal/Accumulation desormais.
        # L ancienne logique (achat d une tendance DEJA engagee, loin du
        # support) est abandonnee au profit de cette approche unifiee.
        dist_above_support_pct = (price - support) / (resistance - support) * 100 if resistance != support else 0
        snap["dist_above_support_pct"] = round(dist_above_support_pct, 2)
        if not fresh_breakout_sa and not failed_breakout_sa and not trend_persistence_sa:
            # v4.215 — SUR DEMANDE EXPLICITE : meme principe qu Accumulation
            # — S/R ancre au dernier retournement confirme, plus stable
            # qu une fenetre glissante fixe.
            # v4.286 — NIVEAUX CHOISIS SELON LA SITUATION (priorite dans l ordre) :
            #  repli dans une hausse : structure 1h seulement (le support 5 min
            #    descend avec la chute : l acheter = rattraper un couteau)
            #  rebond dans une baisse : support 5 min seulement (mouvement court)
            #  hausse saine : support de tendance 1h, support ascendant 1h,
            #    puis support 5 min (dernier creux court terme)
            rising = getattr(state, "rising_support", None) if cfg.get("SPOT_ACCUM_RISING_SUPPORT_ENABLED", 1) else None
            dyn_sup = state.dynamic_trend_support if state.dynamic_trend_support is not None else support
            if repli_sa:
                cands_sa = [("support de structure 1h", self._structural_support(state))]
            elif rebond_sa:
                cands_sa = [("support 5 min", self._short_level(state, "support"))]
            elif situation_sa == "hausse saine":
                cands_sa = [("support de tendance 1h", dyn_sup), ("support ascendant 1h", rising),
                            ("support 5 min", self._short_level(state, "support"))]
            else:
                cands_sa = [("support de tendance 1h", dyn_sup), ("support ascendant 1h", rising)]
            level_label_sa, level_sa = self._first_near_level(state, price, cands_sa, "support")
            near_support = level_label_sa is not None
            near_rising = level_label_sa == "support ascendant 1h"
            snap["rising_support"] = rising
            snap["entry_level"] = f"{level_label_sa} ${level_sa:.6g}" if level_sa else None
            if volume_breakout_sa:
                near_support = True  # v4.279 — deja verifie par la voie "volume"
            snap["near_support"] = near_support
            snap["near_rising_support"] = near_rising
            if not near_support and situation_sa == "hausse saine" and cfg.get("CONTINUATION_ENABLED", 1):
                # v4.287 — la distance au support ne suffit plus a refuser :
                # voie "continuation" si tous les autres signaux la justifient
                cont_ok, cont_detail = self._continuation_check(ticker, state, price, "long")
                snap["continuation_detail"] = cont_detail
                if cont_ok:
                    near_support = True
                    snap["continuation"] = True
            if not near_support:
                listed = " / ".join(f"{lab} ${lvl:.6g}" for lab, lvl in cands_sa if lvl)
                extra = f" — continuation refusee : {snap['continuation_detail']}" if snap.get("continuation_detail") else ""
                snap["blocker"] = f"pas assez proche d un support ({listed or 'aucun niveau disponible'}){extra}"
                return
            snap["entered_via_rising_support"] = near_rising
            if cfg.get("REQUIRE_ENTRY_CANDLE_COLOR", True):
                bullish_now = self._is_candle_bullish_now(state)
                if bullish_now is False:
                    snap["blocker"] = "bougie actuelle non haussiere"
                    return
            # v4.246 — SUR DEMANDE EXPLICITE : confirmation immediate par le
            # flux de transactions reel — voir Accumulation pour le
            # raisonnement complet (miroir, cote achat).
            if cfg.get("ENTRY_FLOW_CONFIRM_ENABLED", True):
                flow_pressure_sa = self._compute_trade_flow_pressure(ticker, price_now=price)
                snap["entry_flow_pressure"] = flow_pressure_sa
                if flow_pressure_sa is not None and flow_pressure_sa <= -cfg.get("ENTRY_FLOW_CONTRADICTION_THRESHOLD", 0.3):
                    snap["blocker"] = f"flux de transactions contredit la hausse (pression vente {flow_pressure_sa:+.2f})"
                    return
                # v4.266 — confirmation OPTIONNELLE (0 = desactivee)
                confirm_min_sa = cfg.get("SPOT_ACCUM_ENTRY_FLOW_CONFIRM_MIN")
                if confirm_min_sa is None:
                    confirm_min_sa = cfg.get("ENTRY_FLOW_CONFIRM_MIN_PRESSURE", 0.0) or 0.0
                if confirm_min_sa > 0:
                    if flow_pressure_sa is None:
                        snap["blocker"] = "confirmation flux exigee mais flux insuffisant (activite recente trop faible)"
                        return
                    if flow_pressure_sa < confirm_min_sa:
                        snap["blocker"] = f"flux acheteur insuffisant (pression {flow_pressure_sa:+.2f}, requis >= {confirm_min_sa:+.2f})"
                        return
        # v4.178 — SUR DEMANDE EXPLICITE : marque si cette entree qualifie
        # via le flirt S/R (pas via une cassure fraiche) — determine si le
        # levier dynamique 2-5x s applique (uniquement dans ce cas).
        snap["entered_via_flirt"] = not fresh_breakout_sa and not volume_breakout_sa and not failed_breakout_sa and not trend_persistence_sa

        # v4.203/225 — SUR DEMANDE EXPLICITE : confirmation par MACD 1h +
        # tendance dynamique (Spot-Accum = long) — ETAIT un blocage dur,
        # converti en BONUS DE CONFIANCE non-bloquant (voir la meme
        # correction miroir dans _check_accumulation_signal pour le
        # raisonnement complet). Ne bloque plus JAMAIS.
        # v4.286 — conditions renforcees selon la situation
        if repli_sa or rebond_sa:
            need_sa = cfg.get("SITUATION_REVERSAL_MIN_FLOW", 0.2) if repli_sa else cfg.get("SITUATION_COUNTERTREND_MIN_FLOW", 0.3)
            fp_sit_sa = self._compute_trade_flow_pressure(ticker, price_now=price)
            snap["entry_flow_pressure"] = fp_sit_sa
            if fp_sit_sa is None or fp_sit_sa < need_sa:
                snap["blocker"] = (f"{situation_sa} : flux acheteur insuffisant "
                                   f"({'indisponible' if fp_sit_sa is None else f'{fp_sit_sa:+.2f}'} < +{need_sa})")
                return
        if rebond_sa:
            res1h_sa = self._structural_resistance(state)
            room_sa = (res1h_sa - price) / price * 100 if res1h_sa and res1h_sa > price else None
            need_room = cfg.get("SITUATION_MIN_ROOM_PCT", 1.0)
            if room_sa is not None and room_sa < need_room:
                snap["blocker"] = f"rebond dans une baisse : resistance 1h trop proche ({room_sa:.2f}% < {need_room}%)"
                return
        dynamic_trend_confirms_sa = False
        if cfg.get("DYNAMIC_TREND_CONFIRM_ENABLED", True) and state.dynamic_trend_direction == "up":
            macd_line, signal_line = self._compute_macd_1h(state)
            snap["macd_1h"] = round(macd_line, 6) if macd_line is not None else None
            snap["macd_1h_signal"] = round(signal_line, 6) if signal_line is not None else None
            if macd_line is not None and signal_line is not None and macd_line > signal_line:
                dynamic_trend_confirms_sa = True
        snap["dynamic_trend_confirms"] = dynamic_trend_confirms_sa
        # v4.150 — SUR DEMANDE EXPLICITE : une cassure fraiche contourne
        # ces deux blocages — une vraie cassure depasse PAR DEFINITION la
        # resistance recente, ce que le blocage ci-dessus interdirait
        # normalement (pensé pour eviter une entree "trop tardive", mais
        # contre-productif face a une cassure qu on cherche justement a
        # capturer des sa confirmation).

        # ── Score de confiance dedie : plus on est loin du support (dans la
        # zone visee) sans depasser la resistance, plus la confiance est
        # elevee — une vraie continuation de tendance, pas un exces ──────
        sr_range = resistance - support
        position_in_range_pct = ((price - support) / sr_range * 100) if sr_range > 0 else 50.0
        confidence = 65.0 + min(max(position_in_range_pct - min_above_pct, 0) / 50.0 * 20.0, 20.0)
        if dynamic_trend_confirms_sa:
            confidence += cfg.get("DYNAMIC_TREND_CONFIRM_BONUS", 5.0)
        # v4.248 — SUR DEMANDE EXPLICITE : voir Accumulation pour le
        # raisonnement complet (miroir).
        if failed_breakout_sa:
            confidence += getattr(state, "failed_breakout_intensity", 0.5) * cfg.get("FAILED_BREAKOUT_MAX_CONFIDENCE_BONUS", 10.0)
        confidence = min(confidence, 85.0)
        snap["confidence"] = round(confidence, 1)

        conf_threshold = self._get_confidence_threshold(ticker)
        snap["confidence_threshold"] = conf_threshold
        if confidence < conf_threshold:
            snap["blocker"] = f"confiance {confidence:.0f}% < seuil {conf_threshold:.0f}%"
            return

        snap["blocker"] = None  # rien ne bloque, candidat genere ce cycle

        # v4.278 — la VOIE d entree figure dans les raisons (colonne "raisons
        # d entree" de l export) pour mesurer laquelle gagne ou perd.
        if snap.get("continuation"):
            path_sa = f"continuation ({snap.get('continuation_detail')})"
        elif repli_sa:
            path_sa = f"fin de repli sur {snap.get('entry_level')}"
        elif fresh_breakout_sa:
            path_sa = "cassure fraiche" + (" (contre-tendance, flux acheteur)" if snap.get("fresh_breakout_counter_trend") else "")
        elif failed_breakout_sa:
            path_sa = "fausse cassure"
        elif trend_persistence_sa:
            path_sa = "tendance persistante"
        elif volume_breakout_sa:
            path_sa = "volume en consolidation"
        elif snap.get("entered_via_rising_support"):
            path_sa = f"repli sur support ascendant ${snap.get('rising_support'):.6g}"
        else:
            path_sa = f"proche du {snap.get('entry_level') or 'support'}"
        if rebond_sa:
            path_sa = f"rebond dans une baisse (contre-tendance) — {path_sa}"
        path_sa += f" [situation : {situation_sa}]"
        reasons = [
            f"🌱 Spot-Accumulation — voie : {path_sa}",
            f"{dist_above_support_pct:.2f}% au-dessus du support, tendance haussiere confirmee",
            f"RSI {rsi:.1f}" if rsi is not None else "RSI ?",
        ]

        self._pending_spot_accum_candidates.append({
            "symbol": symbol, "ticker": ticker, "state": state, "signal": "long",
            "price": price, "confidence": confidence, "rsi": rsi, "rsi_mode": "spot_accumulation",
            "reasons": reasons, "prices": prices, "conf_breakdown": {},
            "strategy": "spot_accumulation", "countertrend": rebond_sa,  # v4.286
            "force_paper": bool(snap.get("continuation")) and bool(cfg.get("CONTINUATION_PAPER_ONLY", 1)),  # v4.287
            "support_at_entry": support, "resistance_at_entry": resistance,
            "entered_via_flirt": snap.get("entered_via_flirt", False),
        })

    def _finalize_open(self, cand):
        """v3.2 — Execution reelle d un candidat retenu (voir _process et la
        file d attente _pending_candidates). Contient exactement la logique
        d ouverture qui etait auparavant executee immediatement en fin de
        _process — inchangee, juste deplacee pour s executer apres le
        classement par confiance de fin de cycle."""
        cfg = self.cfg
        # v4.14 — SUR DEMANDE EXPLICITE : bloque UNIQUEMENT l ouverture de
        # NOUVEAUX trades quand le trading est desactive (bouton Arreter) —
        # les positions deja ouvertes continuent d etre gerees normalement
        # ailleurs (_manage_position_impl, jamais gate par trading_enabled).
        if not self.trading_enabled:
            return
        symbol, ticker, state, signal, price, confidence, rsi, rsi_mode, reasons, prices, conf_breakdown = (
            cand["symbol"], cand["ticker"], cand["state"], cand["signal"], cand["price"],
            cand["confidence"], cand["rsi"], cand["rsi_mode"], cand["reasons"], cand["prices"],
            cand.get("conf_breakdown", {})
        )
        strategy = cand.get("strategy", "forex")  # v4.8 — "forex" ou "accumulation"
        # v4.106 — SUR DEMANDE EXPLICITE : chaque mode peut desormais etre
        # arrete INDEPENDAMMENT des autres (bouton Marche/Arret par mode) —
        # meme principe que le bouton global (self.trading_enabled) :
        # bloque UNIQUEMENT l ouverture de NOUVEAUX trades pour ce mode
        # precis, les positions deja ouvertes de ce mode continuent d etre
        # gerees normalement jusqu a leur fermeture (_manage_position_impl
        # n est jamais gate par ce flag). True par defaut (aucun changement
        # de comportement tant que rien n est desactive).
        if not cfg.get("STRATEGY_TRADING_ENABLED", {}).get(strategy, True):
            return

        # v4.16 — SUR DEMANDE EXPLICITE : le mode Accumulation tourne
        # desormais en PARALLELE de la logique normale, evaluee independamment
        # chaque cycle (plus seulement en repli quand la logique normale ne
        # trouve rien). Consequence : les deux systemes peuvent proposer un
        # candidat sur le MEME actif au MEME cycle — un seul slot par actif
        # existant, le premier a etre finalise (normal, traite en premier)
        # gagne ; l autre est simplement abandonne ce cycle, sans erreur.
        if state.position:
            self.emit("log", {"msg": f"[{ticker}] Candidat {strategy} abandonne — slot deja pris ce cycle par l autre strategie.", "level": "dim"})
            return

        # v4.195/237 — SUR DEMANDE EXPLICITE — REIMPLANTE apres une perte
        # accidentelle en cours de session : Accumulation (short) et
        # Spot-Accum (long) peuvent desormais cibler le MEME actif en sens
        # OPPOSES — sur Hyperliquid, un seul et meme compte/position par
        # actif existe (le bot les traite en interne comme INDEPENDANTS,
        # mais l exchange les NETTE silencieusement l un contre l autre,
        # confirme par un cas reel : short Accumulation jamais visible sur
        # Hyperliquid, netté contre un long Spot-Accum deja ouvert). Ferme
        # PROACTIVEMENT la position opposee existante avant d ouvrir la
        # nouvelle — on ne peut pas avoir deux tendances inversees sur le
        # meme actif.
        # v4.237 — SUR DEMANDE EXPLICITE : au lieu de TOUJOURS donner la
        # priorite au nouveau signal (peu importe sa force), compare
        # desormais les scores de CONFIANCE des deux cotes — confirme par
        # un lot reel : 28% des trades Accumulation se terminaient par ce
        # conflit, souvent sur des signaux faibles des deux cotes. Si la
        # position EXISTANTE a une confiance egale ou superieure au NOUVEAU
        # candidat (au-dela d une marge minimale), le nouveau candidat est
        # mis en ATTENTE (abandonne ce cycle, sans fermer l existant) —
        # seul un signal CLAIREMENT plus fort peut desormais renverser une
        # position en cours.
        conflict_margin = cfg.get("CONFLICT_RESOLUTION_MIN_CONFIDENCE_MARGIN", 5.0)
        if strategy == "accumulation" and signal == "short":
            opposing_state = self.states.get(symbol)
            if opposing_state and opposing_state.position and opposing_state.position.get("strategy") == "spot_accumulation":
                existing_confidence = opposing_state.position.get("confidence", 0) or 0
                if existing_confidence >= confidence - conflict_margin:
                    self.emit("log", {"msg": f"[{ticker}] Conflit avec Spot-Accum (confiance {existing_confidence:.0f}% vs {confidence:.0f}%) — position existante jugee au moins aussi solide, nouveau signal short mis en attente.", "level": "dim"})
                    return
                self.emit("log", {"msg": f"[{ticker}] ⚠️ Conflit detecte : fermeture du long Spot-Accum existant (confiance {existing_confidence:.0f}%) avant d ouvrir le short Accumulation, plus solide (confiance {confidence:.0f}%).", "level": "warn"})
                close_price = state.current_price or price
                # v4.264 — ordre reel D ABORD : si la fermeture reelle echoue,
                # l ancienne position reste suivie et la nouvelle n est PAS
                # ouverte (plus de position abandonnee sans suivi).
                _result = self._safe_close_position(opposing_state, close_price, "CONFLIT SENS OPPOSE (Accumulation)", ticker, opposing_state.position, symbol, None)
                if _result is None:
                    return
                pnl, _, trade = _result
                self.emit("trade", trade)
                self._save_open_positions()
        elif strategy == "spot_accumulation" and signal == "long":
            opposing_accum_state = self.accum_states.get(symbol)
            if opposing_accum_state and opposing_accum_state.position and opposing_accum_state.position.get("strategy") == "accumulation":
                existing_confidence = opposing_accum_state.position.get("confidence", 0) or 0
                if existing_confidence >= confidence - conflict_margin:
                    self.emit("log", {"msg": f"[{ticker}] Conflit avec Accumulation (confiance {existing_confidence:.0f}% vs {confidence:.0f}%) — position existante jugee au moins aussi solide, nouveau signal long mis en attente.", "level": "dim"})
                    return
                self.emit("log", {"msg": f"[{ticker}] ⚠️ Conflit detecte : fermeture du short Accumulation existant (confiance {existing_confidence:.0f}%) avant d ouvrir le long Spot-Accum, plus solide (confiance {confidence:.0f}%).", "level": "warn"})
                close_price = opposing_accum_state.current_price or price
                _result = self._safe_close_position(opposing_accum_state, close_price, "CONFLIT SENS OPPOSE (Spot-Accum)", ticker, opposing_accum_state.position, symbol, None)
                if _result is None:
                    return
                pnl, _, trade = _result
                self.emit("trade", trade)
                self._save_open_positions()

        # v4.9 — Cooldown de reentree dans le MEME sens : si le dernier trade
        # ferme sur cet actif allait deja dans cette direction et que le
        # delai minimum n est pas ecoule, on abandonne ce candidat (silence,
        # il pourra retenter au prochain cycle si le signal persiste apres
        # le cooldown). Un signal OPPOSE (retournement) n est jamais bloque.
        cooldown_sec = cfg.get("REENTRY_COOLDOWN_SEC", 900)
        if (cooldown_sec and state.last_closed_at is not None
                and state.last_closed_direction == signal):
            elapsed = time.time() - state.last_closed_at
            if elapsed < cooldown_sec:
                remaining = int(cooldown_sec - elapsed)
                self.emit("log", {
                    "msg": f"[{ticker}] {signal.upper()} ignore — cooldown de reentree dans le meme sens ({remaining}s restantes sur {cooldown_sec}s)",
                    "level": "dim"
                })
                return

        # v4.269 — delai de re-entree APRES UNE PERTE sur le meme actif et le
        # meme sens (reglable par mode ; plus long que le cooldown general).
        loss_cooldown = {"accumulation": cfg.get("ACCUMULATION_LOSS_COOLDOWN_SEC", 3600),
                         "spot_accumulation": cfg.get("SPOT_ACCUM_LOSS_COOLDOWN_SEC", 0),
                         "funding_contrarian": cfg.get("FUNDING_LOSS_COOLDOWN_SEC", 0)}.get(strategy, 0) or 0
        if (loss_cooldown and getattr(state, "last_closed_was_loss", False) and state.last_closed_at is not None
                and state.last_closed_direction == signal and time.time() - state.last_closed_at < loss_cooldown):
            remaining = int(loss_cooldown - (time.time() - state.last_closed_at))
            self.emit("log", {"msg": f"[{ticker}] {signal.upper()} ignore — delai apres perte sur cet actif ({remaining // 60} min restantes)", "level": "dim"})
            return
        # v4.269 — limite de NOUVELLES entrees d un meme mode par fenetre
        # glissante : evite les rafales d entrees correlees (un seul
        # mouvement de marche = plusieurs positions perdantes a la fois).
        burst_max = {"accumulation": cfg.get("ACCUMULATION_MAX_ENTRIES_PER_WINDOW", 3),
                     "spot_accumulation": cfg.get("SPOT_ACCUM_MAX_ENTRIES_PER_WINDOW", 0)}.get(strategy, 0) or 0
        if burst_max:
            window_s = cfg.get("ENTRY_BURST_WINDOW_SEC", 600)
            recent = [t for t in self._recent_entries.get(strategy, []) if time.time() - t < window_s]
            self._recent_entries[strategy] = recent
            if len(recent) >= burst_max:
                self.emit("log", {"msg": f"[{ticker}] {signal.upper()} ignore — deja {len(recent)} entrees {strategy} dans les {window_s // 60} dernieres minutes (max {burst_max})", "level": "dim"})
                return

        # ── v4.1 — Dimensionnement par LOT (batch) ──────────────────────────
        # E est calcule UNE SEULE FOIS au debut d un lot (des qu aucune
        # position n est ouverte), a partir de l equite totale du moment
        # (CAPITAL_USD + PnL realise de la session), puis reste FIGE a cette
        # valeur pour toutes les positions ouvertes durant ce lot — jusqu a
        # MAX_OPEN_TRADES positions simultanees (5 par defaut). Des que le lot
        # se vide entierement (toutes les positions fermees), un nouveau lot
        # commence et E est recalcule sur la base du capital disponible a ce
        # moment-la : "chaque fois que le capital le permet".
        # v4.89 — SUR DEMANDE EXPLICITE : ne JAMAIS melanger capital virtuel
        # (paper) et capital reel (live) dans le dimensionnement — chaque
        # trade utilise EXCLUSIVEMENT le pot qui correspond a son propre
        # mode effectif. base_capital_live est synchronise depuis
        # Hyperliquid (voir sync_capital_from_hyperliquid) ; si jamais
        # synchronise, repli sur CAPITAL_USD (comportement d origine, cas
        # ou aucun mode n a encore ete bascule en live).
        mode_for_sizing = self._effective_mode(strategy)
        if cand.get("force_paper") or (strategy == "funding_contrarian" and not cfg.get("FUNDING_MODE_LIVE_ALLOWED", False)):
            mode_for_sizing = "paper"  # v4.297 — dimensionne dans le pot ou il sera REELLEMENT trade
        # v4.297 — FIX : positions ouvertes DU MEME POT (paper ou live),
        # Accumulation comprise (avant : toutes confondues, sans Accumulation)
        open_count = sum(1 for pool in (self.states, self.accum_states) for s in pool.values()
                         if s.position and s.position.get("effective_mode", "paper") == mode_for_sizing)
        if mode_for_sizing == "live":
            total_pnl = sum(s.live_pnl for s in self.states.values()) + sum(s.live_pnl for s in self.accum_states.values())
            base_capital = getattr(self, "live_capital_base", cfg["CAPITAL_USD"])
        else:
            total_pnl = sum(s.paper_pnl for s in self.states.values()) + sum(s.paper_pnl for s in self.accum_states.values())
            base_capital = cfg["CAPITAL_USD"]
        equity = base_capital + total_pnl
        # v4.297 — en live : valeur REELLE du compte Hyperliquid (relevee toutes
        # les 2 min par le controle de synchronisation)
        if mode_for_sizing == "live" and getattr(self, "live_equity_real", None) \
                and time.time() - getattr(self, "live_equity_ts", 0) < 600:
            equity = self.live_equity_real
        # v4.153 — FIX BUG CRITIQUE : ne comptait QUE self.states
        # (Normal/Funding/Spot-Accum), jamais self.accum_states —
        # capital_available ignorait donc completement ce qu Accumulation
        # avait deja engage, permettant un surengagement REEL du capital
        # au-dela de ce qui est reellement disponible sur le compte des
        # que plusieurs modes ont des positions ouvertes simultanement.
        capital_engaged = sum(
            s.position["size"] for s in self.states.values()
            if s.position and s.position.get("effective_mode", "paper") == mode_for_sizing
        ) + sum(
            s.position["size"] for s in self.accum_states.values()
            if s.position and s.position.get("effective_mode", "paper") == mode_for_sizing
        )
        capital_available = equity - capital_engaged

        if capital_available <= 0:
            self.emit("log", {"msg": f"[{ticker}] Capital insuffisant (${capital_available:.2f})", "level": "warn"})
            return

        # v4.252 — SUR DEMANDE EXPLICITE : verification PRECOCE, avant de
        # traverser le reste de la logique d entree (calcul SL/TP, mise a
        # jour du levier sur l exchange, etc.) — le levier final n est pas
        # encore connu ici. Seuil prudent (2$, pas 10$) pour ne pas
        # rejeter a tort un capital qui resterait viable avec un levier
        # dynamique eleve (jusqu a x5 pour Accumulation/Spot-Accum) — le
        # controle PRECIS (avec le levier REEL determine) reste en place
        # plus loin, pour trancher les cas limites correctement.
        if capital_available < 2.0:
            cooldown_sec = cfg.get("INSUFFICIENT_NOTIONAL_COOLDOWN_SEC", 180)
            last_attempt = getattr(state, "_last_insufficient_notional_attempt", 0)
            now_ts = time.time()
            if now_ts - last_attempt < cooldown_sec:
                return
            state._last_insufficient_notional_attempt = now_ts
            self.emit("log", {"msg": f"[{ticker}] Capital disponible (${capital_available:.2f}) trop faible pour atteindre le minimum Hyperliquid ($10), meme avec un levier eleve — capital probablement engage ailleurs, nouvelle tentative dans {cooldown_sec//60} min.", "level": "warn"})
            return

        # v4.297 — FIX : un lot PAR pot. Avant, un seul E etait partage : fige
        # par un trade PAPER, il dimensionnait aussi les trades LIVE (et
        # inversement), quel que soit le capital reel.
        if open_count == 0 or not self.batch_entry_sizes.get(mode_for_sizing):
            self.batch_entry_sizes[mode_for_sizing] = equity * cfg["POSITION_SIZE_PCT"] / 100
            save_batch_entry_size(self.batch_entry_sizes)
            self.emit("log", {"msg": f"Nouveau lot {mode_for_sizing.upper()} — E fige a ${self.batch_entry_sizes[mode_for_sizing]:.2f} ({cfg['POSITION_SIZE_PCT']:.0f}% de ${equity:.2f})", "level": "info"})

        size = min(self.batch_entry_sizes[mode_for_sizing], capital_available)
        # v4.46 — SUR DEMANDE EXPLICITE : taille INDEPENDANTE par mode, si
        # definie — contourne le batch_entry_size PARTAGE (fige pour tout le
        # lot, tous modes confondus) et calcule une taille dediee depuis
        # l equity actuelle. None (par defaut) = aucun changement.
        mode_size_key = {
            "accumulation": "ACCUMULATION_POSITION_SIZE_PCT",
            "funding_contrarian": "FUNDING_POSITION_SIZE_PCT",
            "spot_accumulation": "SPOT_ACCUM_POSITION_SIZE_PCT",
        }.get(strategy)
        if mode_size_key:
            mode_pct = cfg.get(mode_size_key)
            if mode_pct is not None:
                size = min(equity * mode_pct / 100, capital_available)
        # v4.102 — SUR DEMANDE EXPLICITE : pour Spot-Accum specifiquement,
        # remplace le pourcentage fixe (mode_pct) par un dimensionnement
        # DYNAMIQUE — toujours diviser le capital disponible par le nombre
        # de trades simultanes autorises, pour ce mode. Garantit que TOUT
        # le capital est utilise (ni sous-utilise, ni sur-engage), quel que
        # soit le reglage de "trades simultanes max" — evite aussi les
        # echecs "notionnel sous le minimum Hyperliquid de $10" observes
        # avec un pourcentage fixe trop petit pour le capital actuel.
        if strategy == "spot_accumulation":
            # v4.158 — SUR DEMANDE EXPLICITE : diviseur desormais base sur
            # la SOMME des trades simultanes des DEUX modes (Accumulation +
            # Spot-Accumulation combines), pas seulement celui de ce mode —
            # les deux partagent reellement le meme capital (voir
            # capital_engaged, deja corrige pour inclure les deux), la
            # taille par trade doit refleter cette coordination.
            max_combined_trades = max(cfg.get("ACCUMULATION_MAX_TRADES", 3), 1) + max(cfg.get("SPOT_ACCUM_MAX_TRADES", 3), 1)
            size = min(equity / max_combined_trades, capital_available)
        # v4.152 — SUR DEMANDE EXPLICITE : meme dimensionnement dynamique
        # pour Accumulation — garantit l utilisation complete du capital
        # dedie, sans jamais le depasser, au lieu de partager la taille
        # figee du lot avec le mode normal.
        if strategy == "accumulation":
            # v4.158 — SUR DEMANDE EXPLICITE : meme diviseur combine que
            # Spot-Accumulation ci-dessus.
            max_combined_trades = max(cfg.get("ACCUMULATION_MAX_TRADES", 3), 1) + max(cfg.get("SPOT_ACCUM_MAX_TRADES", 3), 1)
            size = min(equity / max_combined_trades, capital_available)
        if size <= 0:
            self.emit("log", {"msg": f"[{ticker}] Capital insuffisant pour E=${self.batch_entry_sizes.get(mode_for_sizing, 0):.2f} (disponible ${capital_available:.2f})", "level": "warn"})
            return

        # v4.194 — SUR DEMANDE EXPLICITE : penalise les mauvais actifs /
        # favorise les gagnants sur la TAILLE de position (en plus du
        # levier, deja fait) — s applique aux 4 modes, base sur la MEME
        # performance historique (confidence_thresholds) que le levier.
        if cfg.get("PERFORMANCE_SIZE_ADJUST_ENABLED", True):
            size_mult = self._compute_performance_size_multiplier(ticker)
            size = min(size * size_mult, capital_available)
            if size <= 0:
                self.emit("log", {"msg": f"[{ticker}] Capital insuffisant apres ajustement performance (x{size_mult})", "level": "warn"})
                return

        # v3.2 — le levier prudent doit etre connu AVANT le calcul du SL de
        # securite, puisque le notionnel reel (taille x levier) determine le
        # % de mouvement correspondant a un montant $ donne.
        # v4.178 — SUR DEMANDE EXPLICITE : Spot-Accumulation utilise
        # desormais un levier DYNAMIQUE (2-5x) quand l entree qualifie via
        # le flirt S/R + couleur de bougie — l ancien x1 systematique reste
        # applique pour une entree via cassure fraiche (mouvement deja bien
        # engage, moins besoin d amplifier), coherent avec l esprit
        # prudent d origine dans ce cas precis.
        if strategy == "spot_accumulation":
            if cand.get("entered_via_flirt", False):
                leverage = self._compute_spot_accum_dynamic_leverage(ticker)
            else:
                leverage = 1
        elif strategy == "accumulation":
            # v4.191 — SUR DEMANDE EXPLICITE : meme principe que Spot-Accum
            # (miroir exact), Accumulation etant desormais l oppose (short).
            if cand.get("entered_via_flirt", False):
                leverage = self._compute_accumulation_dynamic_leverage(ticker)
            else:
                leverage = 1
        else:
            leverage = self._compute_prudent_leverage(ticker, confidence, rsi_mode)
            if self._forex_crypto_eligible(ticker):   # v4.331 : plafond propre aux cryptos du mode Forex
                leverage = min(leverage, max(int(cfg.get("FOREX_CRYPTO_MAX_LEVERAGE", 5)), 1))
        if cand.get("engine") == "mtf":
            # v4.299 — taille calculee sur le RISQUE (perte au SL = % fixe du capital), levier 1
            leverage = 1
            size = min(cand["mtf_notional"], capital_available)
        notional = size * leverage

        # ── v4.24 — SL/TTP adaptatifs a l ATR reel (optionnel) ───────────────
        # v4.41 — SUR DEMANDE EXPLICITE : chaque valeur peut desormais etre
        # surchargee PAR ACTIF (ex: SL_PCT_OF_E_BY_SYMBOL={"SUSHI": 0.5}) —
        # tous les actifs ne se comportent pas de la meme facon (fluctuation
        # intra-bougie differente, voir calc_avg_candle_fluctuation). Repli
        # sur la valeur globale si aucune surcharge definie pour cet actif.
        # v4.42 — SUR DEMANDE EXPLICITE : le mode Accumulation peut desormais
        # avoir son PROPRE jeu de seuils SL/TTP (LONG et SHORT confondus, un
        # seul jeu pour les deux sens), separe du mode normal — via les cles
        # ACCUMULATION_SL_PCT_OF_E / ACCUMULATION_TTP_*. Tant qu aucune de
        # ces cles n est explicitement definie (valeur None par defaut),
        # Accumulation continue de se comporter EXACTEMENT comme avant
        # (retombe sur les memes valeurs que le mode normal, y compris ses
        # eventuelles surcharges par actif) — aucun changement de
        # comportement tant que vous ne reglez rien.
        if strategy == "accumulation":
            sl_pct_of_e   = cfg.get("ACCUMULATION_SL_PCT_OF_E")
            if sl_pct_of_e is None:
                sl_pct_of_e = cfg.get("SL_PCT_OF_E_BY_SYMBOL", {}).get(ticker, cfg.get("SL_PCT_OF_E", 1.0))
            ttp_arm1_pct  = cfg.get("ACCUMULATION_TTP_ARM1_PRICE_PCT")
            if ttp_arm1_pct is None:
                ttp_arm1_pct = cfg.get("TTP_ARM1_PRICE_PCT_BY_SYMBOL", {}).get(ticker, cfg.get("TTP_ARM1_PRICE_PCT", 1.0))
            ttp_lock1_pct = cfg.get("ACCUMULATION_TTP_LOCK1_PRICE_PCT")
            if ttp_lock1_pct is None:
                ttp_lock1_pct = cfg.get("TTP_LOCK1_PRICE_PCT_BY_SYMBOL", {}).get(ticker, cfg.get("TTP_LOCK1_PRICE_PCT", 0.8))
            ttp_arm2_pct  = cfg.get("ACCUMULATION_TTP_ARM2_PRICE_PCT")
            if ttp_arm2_pct is None:
                ttp_arm2_pct = cfg.get("TTP_ARM2_PRICE_PCT_BY_SYMBOL", {}).get(ticker, cfg.get("TTP_ARM2_PRICE_PCT", 1.3))
            ttp_gap_pct   = cfg.get("ACCUMULATION_TTP_TRAIL_GAP_PRICE_PCT")
            if ttp_gap_pct is None:
                ttp_gap_pct = cfg.get("TTP_TRAIL_GAP_PRICE_PCT_BY_SYMBOL", {}).get(ticker, cfg.get("TTP_TRAIL_GAP_PRICE_PCT", 0.3))
        elif strategy == "funding_contrarian":
            # v4.45 — SUR DEMANDE EXPLICITE : meme mecanisme que Accumulation,
            # pour que les 4 modes soient tous reglables independamment.
            sl_pct_of_e   = cfg.get("FUNDING_SL_PCT_OF_E")
            if sl_pct_of_e is None:
                sl_pct_of_e = cfg.get("SL_PCT_OF_E_BY_SYMBOL", {}).get(ticker, cfg.get("SL_PCT_OF_E", 1.0))
            ttp_arm1_pct  = cfg.get("FUNDING_TTP_ARM1_PRICE_PCT")
            if ttp_arm1_pct is None:
                ttp_arm1_pct = cfg.get("TTP_ARM1_PRICE_PCT_BY_SYMBOL", {}).get(ticker, cfg.get("TTP_ARM1_PRICE_PCT", 1.0))
            ttp_lock1_pct = cfg.get("FUNDING_TTP_LOCK1_PRICE_PCT")
            if ttp_lock1_pct is None:
                ttp_lock1_pct = cfg.get("TTP_LOCK1_PRICE_PCT_BY_SYMBOL", {}).get(ticker, cfg.get("TTP_LOCK1_PRICE_PCT", 0.8))
            ttp_arm2_pct  = cfg.get("FUNDING_TTP_ARM2_PRICE_PCT")
            if ttp_arm2_pct is None:
                ttp_arm2_pct = cfg.get("TTP_ARM2_PRICE_PCT_BY_SYMBOL", {}).get(ticker, cfg.get("TTP_ARM2_PRICE_PCT", 1.3))
            ttp_gap_pct   = cfg.get("FUNDING_TTP_TRAIL_GAP_PRICE_PCT")
            if ttp_gap_pct is None:
                ttp_gap_pct = cfg.get("TTP_TRAIL_GAP_PRICE_PCT_BY_SYMBOL", {}).get(ticker, cfg.get("TTP_TRAIL_GAP_PRICE_PCT", 0.3))
        else:
            sl_pct_of_e   = cfg.get("SL_PCT_OF_E_BY_SYMBOL", {}).get(ticker, cfg.get("SL_PCT_OF_E", 1.0))
            ttp_arm1_pct  = cfg.get("TTP_ARM1_PRICE_PCT_BY_SYMBOL", {}).get(ticker, cfg.get("TTP_ARM1_PRICE_PCT", 1.0))
            ttp_lock1_pct = cfg.get("TTP_LOCK1_PRICE_PCT_BY_SYMBOL", {}).get(ticker, cfg.get("TTP_LOCK1_PRICE_PCT", 0.8))
            ttp_arm2_pct  = cfg.get("TTP_ARM2_PRICE_PCT_BY_SYMBOL", {}).get(ticker, cfg.get("TTP_ARM2_PRICE_PCT", 1.3))
            ttp_gap_pct   = cfg.get("TTP_TRAIL_GAP_PRICE_PCT_BY_SYMBOL", {}).get(ticker, cfg.get("TTP_TRAIL_GAP_PRICE_PCT", 0.3))
        # v4.56 — FIX BUG CRITIQUE : tier0_arm_pct/tier0_gap_pct doivent
        # aussi etre calcules ICI (a l ouverture) pour pouvoir etre mis a
        # l echelle par le SL/TTP adaptatif ci-dessous et memorises sur la
        # position — avant ce fix, ils n existaient qu en lecture directe
        # de la config globale FIXE dans _manage_position_impl, jamais mis
        # a l echelle, causant un armement/desarmement en boucle sur les
        # actifs a faible ATR (voir le commentaire plus bas).
        tier0_arm_pct = cfg.get("TTP_TIER0_ARM_PRICE_PCT", 0.5)
        tier0_gap_pct = cfg.get("TTP_TIER0_GAP_PRICE_PCT", 0.42)
        # Calcule les seuils de CE trade precis a partir de l ATR actuel,
        # en conservant les MEMES proportions que les reglages (fixes ou
        # par-actif ci-dessus) — toujours en %. Si desactive (par defaut) ou
        # ATR indisponible, retombe integralement sur les valeurs fixes.
        adaptive_used = False
        if cfg.get("SL_TTP_ADAPTIVE_ENABLED", False):
            # v4.36 — vrai calcul (haut/bas/cloture), repli sur l ancien.
            _, atr_pct_entry = calc_true_range_atr(list(state.candle_history), cfg.get("ATR_PERIOD", 14))
            if atr_pct_entry is None:
                _, atr_pct_entry = calc_atr(prices, cfg.get("ATR_PERIOD", 14))
            if atr_pct_entry is not None and atr_pct_entry > 0:
                sl_min = cfg.get("SL_PCT_MIN", 0.3)
                sl_max = cfg.get("SL_PCT_MAX", 3.0)
                # v4.41 — multiplicateur ATR aussi surchargeable par actif.
                # v4.42 — et par strategie (Accumulation) si defini.
                # v4.45 — et Funding Contrarian, sur le meme modele.
                if strategy == "accumulation" and cfg.get("ACCUMULATION_SL_ATR_MULTIPLIER") is not None:
                    sl_atr_mult = cfg.get("ACCUMULATION_SL_ATR_MULTIPLIER")
                elif strategy == "funding_contrarian" and cfg.get("FUNDING_SL_ATR_MULTIPLIER") is not None:
                    sl_atr_mult = cfg.get("FUNDING_SL_ATR_MULTIPLIER")
                else:
                    sl_atr_mult = cfg.get("SL_ATR_MULTIPLIER_BY_SYMBOL", {}).get(ticker, cfg.get("SL_ATR_MULTIPLIER", 1.0))
                sl_dynamic = max(sl_min, min(sl_max, atr_pct_entry * sl_atr_mult))
                # v4.116 — FIX BUG CRITIQUE : la compensation levier (v4.112)
                # etait appliquee TROP TARD (uniquement sur le sl_pct_of_e
                # FINAL, apres coup) — tous les seuils DERIVES par ratio
                # (tier0_arm_pct, ttp_arm1_pct, etc., calcules juste en
                # dessous a partir de sl_dynamic) restaient bases sur la
                # valeur NON compensee, expliquant la persistance de
                # l armement precoce (tier0 arme a 0.07-0.21% observe,
                # malgre le correctif SL) — leverage etait deja disponible
                # a ce point, il suffisait de compenser sl_dynamic ICI,
                # AVANT que les ratios ne s appliquent, pour que TOUS les
                # seuils derives en heritent naturellement.
                if leverage > 1:
                    min_price_move_pct_early = cfg.get("SL_MIN_PRICE_MOVE_PCT", 0.3)
                    implied_price_move_early = sl_dynamic / leverage
                    if implied_price_move_early < min_price_move_pct_early:
                        sl_dynamic = min_price_move_pct_early * leverage
                # Proportions conservees telles que definies par les reglages fixes actuels
                ratio_arm1  = (ttp_arm1_pct  / sl_pct_of_e) if sl_pct_of_e else 1.0
                ratio_lock1 = (ttp_lock1_pct / sl_pct_of_e) if sl_pct_of_e else 0.8
                ratio_arm2  = (ttp_arm2_pct  / sl_pct_of_e) if sl_pct_of_e else 1.3
                ratio_gap   = (ttp_gap_pct   / sl_pct_of_e) if sl_pct_of_e else 0.3
                # v4.56 — FIX BUG CRITIQUE : tier0_arm_pct/tier0_gap_pct
                # restaient FIXES (jamais mis a l echelle) alors que
                # ttp_arm1_pct l etait — sur un actif a faible ATR,
                # ttp_arm1_pct pouvait finir SOUS le tier0_arm_pct fixe
                # (0.5% par defaut), inversant leur relation logique :
                # le trade s armait en tier 1 a, par exemple, 0.30%, puis
                # etait IMMEDIATEMENT redescendu en tier 0 des le cycle
                # suivant (0.30% <= 0.5%) — armement/desarmement en boucle,
                # plusieurs fois par minute (observe sur TIA). Applique
                # desormais le MEME ratio de mise a l echelle.
                # v4.68 — FIX BUG CRITIQUE : le ratio etait CALCULE depuis
                # tier0_arm_pct (TOUJOURS la valeur GLOBALE, jamais
                # surchargeable par mode) divise par sl_pct_of_e (qui, LUI,
                # peut etre une surcharge par mode tres differente, ex:
                # FUNDING_SL_PCT_OF_E=0.2% contre le defaut global 0.5% pour
                # tier0) — un SL Funding delibrement bas gonflait le ratio a
                # 2.5 au lieu de ~0.5, poussant tier0_arm final a 0.75-2.00%
                # au lieu de rester une fraction raisonnable du SL adaptatif
                # (observe : pic +0.58% jamais arme). Utilise desormais un
                # ratio FIXE et VOULU, jamais derive de deux valeurs
                # independantes qui peuvent diverger entre modes.
                ratio_tier0_arm = cfg.get("TIER0_ARM_RATIO_OF_SL", 0.5)
                ratio_tier0_gap = cfg.get("TIER0_GAP_RATIO_OF_SL", 0.42)
                sl_pct_of_e   = round(sl_dynamic, 4)
                ttp_arm1_pct  = round(sl_dynamic * ratio_arm1, 4)
                ttp_lock1_pct = round(sl_dynamic * ratio_lock1, 4)

                ttp_arm2_pct  = round(sl_dynamic * ratio_arm2, 4)
                ttp_gap_pct   = round(sl_dynamic * ratio_gap, 4)
                tier0_arm_pct = round(sl_dynamic * ratio_tier0_arm, 4)
                tier0_gap_pct = round(sl_dynamic * ratio_tier0_gap, 4)
                # v4.66 — SUR DEMANDE EXPLICITE : plancher STRICT et DIRECT
                # sur les seuils d armement eux-memes (pas seulement sur
                # sl_dynamic dont ils derivent par ratio) — evite les trades
                # microscopiques (pic 0.06%, gain $0.00-0.01) observes sur
                # des actifs a tres faible ATR (INJ, TIA, POL). Le plancher
                # sur sl_dynamic seul ne suffisait pas car applique AVANT la
                # multiplication par un ratio < 1 (tier0 ~0.5x, par exemple).
                min_arm_pct = cfg.get("TTP_MIN_ARM_PCT_FLOOR", 0.3)
                tier0_arm_pct = max(tier0_arm_pct, min_arm_pct * 0.5)
                ttp_arm1_pct = max(ttp_arm1_pct, min_arm_pct)
                adaptive_used = True

        # ── v4.10 — le SL Hyperliquid (filet de securite) est un MULTIPLE du
        # Stop Loss du bot (EXCHANGE_SAFETY_SL_MULT, defaut x2), lui-meme en
        # % de E (perte $ plafonnee). Comme les deux sont exprimes en % de E
        # (donc de CETTE position precise), le filet de securite reste
        # toujours proportionnel au Stop Loss reel, quelle que soit la
        # taille ou le levier utilises — d ou la conversion via le notionnel
        # (taille x levier) pour obtenir le % de mouvement de prix correct.
        # v4.105 — FIX BUG CRITIQUE : Spot-Accum n avait AUCUN traitement
        # dedie ici, retombant sur sl_pct_of_e generique (adaptatif ATR,
        # souvent 0.3-3%) — completement deconnecte de son PROPRE plafond
        # dur (SPOT_ACCUM_HARD_SL_PCT, 5% du PnL par defaut). A leverage x1,
        # ce filet Hyperliquid pouvait etre PLUS SERRE que le plafond dur
        # interne, fermant la position REELLE avant que le bot n ait la
        # moindre raison de le faire — desynchronisation confirmee (trades
        # fermes sur Hyperliquid, restes ouverts dans le suivi du bot).
        # Utilise desormais explicitement le plafond dur de Spot-Accum
        # (avec la meme marge EXCHANGE_SAFETY_SL_MULT), garantissant que le
        # filet Hyperliquid reste TOUJOURS plus large que ce que la logique
        # interne du bot utiliserait pour fermer en premier.
        if strategy == "spot_accumulation":
            spot_accum_reference_sl_pct = cfg.get("SPOT_ACCUM_HARD_SL_PCT", 5.0)
            print(f"[SL-EXCHANGE-DIAG] Spot-Accum {ticker} : SPOT_ACCUM_HARD_SL_PCT={spot_accum_reference_sl_pct} | EXCHANGE_SAFETY_SL_MULT={cfg.get('EXCHANGE_SAFETY_SL_MULT', 2.0)}")
            safety_sl_pct = spot_accum_reference_sl_pct * cfg.get("EXCHANGE_SAFETY_SL_MULT", 2.0)
        else:
            safety_sl_usd = size * sl_pct_of_e / 100 * cfg.get("EXCHANGE_SAFETY_SL_MULT", 2.0)
            safety_sl_pct = (safety_sl_usd / notional * 100) if notional > 0 else 2.0
            # v4.269 — le filet Hyperliquid doit rester PLUS LARGE que le
            # plafond du bot, sinon elargir ACCUMULATION_SL_CAP_PCT ferait
            # sortir la position par le SL de securite natif avant.
            if strategy == "accumulation":
                cap_ac = cfg.get("ACCUMULATION_SL_CAP_PCT") or cfg.get("STRUCTURAL_SL_HARD_CAP_PCT", 0.5)
                safety_sl_pct = max(safety_sl_pct, cap_ac * cfg.get("EXCHANGE_SAFETY_SL_MULT", 2.0))
        sl_p = price * (1 - safety_sl_pct/100) if signal == "long" else price * (1 + safety_sl_pct/100)
        # tp_p conserve uniquement a titre informatif / pour le bouton manuel TP
        # du dashboard — plus jamais envoye a Hyperliquid ni utilise pour fermer
        # automatiquement (remplace par le Trailing TP en $ de _manage_position).
        tp_pct = cfg.get("SYMBOL_TP_PCT", {}).get(ticker, cfg["TAKE_PROFIT_PCT"])
        tp_p = price * (1 + tp_pct/100) if signal == "long" else price * (1 - tp_pct/100)
        if cand.get("engine") == "mtf":
            # v4.299 — SL sur la structure (aussi pose en ordre natif) et objectif sur la zone opposee
            sl_p, tp_p = cand["mtf_sl"], cand["mtf_tp"]

        # v4.8 — FIX : "label"/"action" DOIT rester exactement "LONG"/"SHORT"
        # (sans prefixe) car c est la valeur utilisee pour retrouver le trade
        # a sa fermeture (db.get_open_trade_id_by_coin_action, correspondance
        # EXACTE coin+action). Le tag Accumulation est affiche separement
        # dans les logs (strat_tag_log) et transmis via le champ "strategy".
        label = "LONG" if signal == "long" else "SHORT"
        strat_tag_log = "🎯 ACCUMULATION " if strategy == "accumulation" else ("💰 FUNDING " if strategy == "funding_contrarian" else "")
        # v4.36 — vrai calcul (haut/bas/cloture), repli sur l ancien.
        _, atr_at_entry = calc_true_range_atr(list(state.candle_history), cfg.get("ATR_PERIOD", 14))
        if atr_at_entry is None:
            _, atr_at_entry = calc_atr(prices, cfg.get("ATR_PERIOD", 14))
        atr_str = f"ATR {atr_at_entry:.3f}%" if atr_at_entry else "ATR ?"
        lev_str = f"x{leverage}" if leverage > 1 else "x1"

        stop_loss_usd_here = size * sl_pct_of_e / 100
        adaptive_tag = " 🎚️ADAPTATIF" if adaptive_used else ""
        self.emit("log", {"msg": f"[{ticker}] {strat_tag_log}{label} @ ${price:.2f} | RSI:{rsi:.1f}({rsi_mode}) | {atr_str} | {' | '.join(reasons)} | ${size:.2f} {lev_str} | Stop Loss -${stop_loss_usd_here:.2f} ({sl_pct_of_e:.2f}% de E{adaptive_tag}) | TTP arme des {ttp_arm1_pct:.2f}% | SL secu {safety_sl_pct:.2f}% [PERP]", "level": "signal"})

        # v4.33 — SECURITE EXPLICITE : un candidat "funding_contrarian" ne
        # passe JAMAIS d ordre reel tant que FUNDING_MODE_LIVE_ALLOWED n est
        # pas active manuellement — meme si le bot tourne par ailleurs en
        # mode live. Les autres strategies (normal, accumulation) ne sont pas
        # affectees.
        # v4.87 — SUR DEMANDE EXPLICITE : chaque mode peut desormais
        # basculer independamment entre paper et live — voir _effective_mode.
        # Le garde-fou Funding ci-dessous reste actif en plus, inchange.
        effective_mode_open = self._effective_mode(strategy)
        if cand.get("force_paper") and effective_mode_open == "live":
            # v4.287 — voie en phase de test : simulee meme si le mode est en live
            effective_mode_open = "paper"
            self.emit("log", {"msg": f"[{ticker}] Entree \"continuation\" simulee (paper) — voie en test, le mode reste en live pour les autres entrees.", "level": "dim"})
        funding_live_blocked = strategy == "funding_contrarian" and not cfg.get("FUNDING_MODE_LIVE_ALLOWED", False)
        if funding_live_blocked and effective_mode_open == "live":
            self.emit("log", {"msg": f"[{ticker}] 💰 Trade Funding Contrarian simule (paper) malgre le mode live — deverrouillez FUNDING_MODE_LIVE_ALLOWED pour l autoriser en reel.", "level": "warn"})
        # v4.264 — mode REEL de ce trade (un Funding bloque reste paper, il
        # ne doit pas etre enregistre "live" : sa fermeture tenterait sinon
        # un ordre reel sur une position qui n existe pas).
        recorded_mode = "paper" if funding_live_blocked else effective_mode_open
        # v4.273 — Hyperliquid ne tient qu UNE position perp par actif : pas
        # d ouverture live sur un actif deja tenu en live manuellement.
        if recorded_mode == "live" and getattr(self, "manual", None) is not None and self.manual.has_live_perp(ticker):
            self.emit("log", {"msg": f"[{ticker}] Entree live ignoree — position MANUELLE live deja ouverte sur cet actif.", "level": "dim"})
            return
        # v4.264 — INDEXATION A L OUVERTURE : identifiant unique cree AVANT
        # tout ordre, portant le mode source et l heure d ouverture, envoye
        # a Hyperliquid comme cloid et ecrit en base de facon synchrone.
        trade_uid = make_trade_uid(strategy)
        action_label = "LONG" if signal == "long" else "SHORT"
        is_accum_slot = state is self.accum_states.get(symbol)

        if effective_mode_open == "live" and self.exchange and not funding_live_blocked:
            # v3.2 — applique le levier PRUDENT specifique a ce trade sur
            # Hyperliquid (remplace/surcharge le levier uniforme applique au
            # demarrage) — chaque trade peut donc avoir un levier different
            # selon sa confiance/mode/etat de penalite.
            # v4.163 — SUR DEMANDE EXPLICITE : les marches HIP-3 (forex,
            # namespace "xyz:") exigent la marge ISOLEE — la marge croisee,
            # utilisee pour les cryptos, n est pas supportee sur ces marches.
            # v4.171 — FIX : PAXG est un actif natif Hyperliquid (marge
            # croisee normale), contrairement aux vrais marches HIP-3
            # (prefixe "xyz:", marge isolee obligatoire) — ne verifier que
            # le prefixe, pas l appartenance a FOREX_SYMBOLS dans son
            # ensemble (qui inclut aussi PAXG pour l isolation de MODE
            # uniquement, pas pour la marge).
            is_cross_margin = not ticker.startswith("xyz:")
            # v4.166 — meme nom qualifie "dex:coin" que pour le passage d ordre.
            lev_ticker = ticker
            try:
                self.exchange.update_leverage(leverage, lev_ticker, is_cross=is_cross_margin)
            except Exception as e:
                self.emit("log", {"msg": f"[{ticker}] Echec application levier prudent x{leverage} : {e} — poursuite avec le levier deja en place.", "level": "warn"})
            # tp_price=None : plus d ordre TP fixe sur Hyperliquid, la prise de
            # profit est entierement geree par le bot (Quick Profit / Trailing)
            # v4.251 — SUR DEMANDE EXPLICITE, FIX : verifie le notionnel
            # minimal AVANT d appeler place_order (pas apres son echec) —
            # evite de retenter la MEME commande vouee a l echec a CHAQUE
            # cycle quand le capital disponible est insuffisant (confirme
            # par un cas reel : LINK, 7+ tentatives identiques en moins de
            # 2 minutes, meme notionnel $1.91 a chaque fois, tant que le
            # capital reste bloque dans d autres positions). Cooldown de
            # quelques minutes avant de retenter CE ticket specifiquement,
            # le temps qu un autre trade se ferme et libere du capital.
            projected_notional = size * max(leverage, 1)
            # v4.296 — trades trop petits pour Hyperliquid (Funding : ~6 $ de
            # notionnel) : taille relevee juste au-dessus du minimum de 10 $,
            # si le capital disponible le permet (option, activee par defaut).
            if projected_notional < 10.0 and cfg.get("LIVE_MIN_NOTIONAL_BUMP", 1):
                bumped_size = cfg.get("LIVE_MIN_NOTIONAL_TARGET_USD", 10.5) / max(leverage, 1)
                if bumped_size <= capital_available:
                    self.emit("log", {"msg": f"[{ticker}] Notionnel ${projected_notional:.2f} sous le minimum Hyperliquid : taille relevee a ${bumped_size * max(leverage, 1):.2f}.", "level": "dim"})
                    size = bumped_size
                    projected_notional = size * max(leverage, 1)
            if projected_notional < 10.0:
                cooldown_sec = cfg.get("INSUFFICIENT_NOTIONAL_COOLDOWN_SEC", 180)
                last_attempt = getattr(state, "_last_insufficient_notional_attempt", 0)
                now_ts = time.time()
                if now_ts - last_attempt < cooldown_sec:
                    return
                state._last_insufficient_notional_attempt = now_ts
                self.emit("log", {"msg": f"[{ticker}] Notionnel projete ${projected_notional:.2f} sous le minimum Hyperliquid de $10 — capital probablement engage ailleurs, nouvelle tentative dans {cooldown_sec//60} min.", "level": "warn"})
                return
            # Trace durable ecrite AVANT l ordre : si le process est coupe
            # juste apres l execution, le trade reste identifiable au
            # redemarrage (mode source + heure), jamais orphelin.
            try:
                db.register_pending_trade(trade_uid, ticker, action_label, strategy, symbol,
                                          recorded_mode, price, is_accum_slot)
            except Exception as e_reg:
                print(f"[TRADE-INDEX] Enregistrement prealable {trade_uid} impossible : {e_reg}")
            ok, order_err, real_fill_price = place_order(self.exchange, ticker, signal == "long", size, price, cfg, sl_price=sl_p, tp_price=None, leverage=leverage, trade_uid=trade_uid)
            if not ok:
                self._discard_pending_trade(trade_uid, ticker)
                self.emit("log", {"msg": f"[{ticker}] Ordre non execute — {order_err or 'raison inconnue'}", "level": "warn"})
                return
            # v4.226 — SUR DEMANDE EXPLICITE, FIX BUG CRITIQUE : utilise le
            # VRAI prix de remplissage confirme par Hyperliquid (quand
            # disponible) comme point de reference pour CE trade, au lieu
            # du prix VISE par le bot avant l ouverture — elimine l ecart
            # de PnL entre le bot et Hyperliquid observe sur des trades
            # reels (slippage jamais reflete auparavant), essentiel pour un
            # timing SL/TTP precis base sur UNE SEULE source de verite.
            if real_fill_price is not None and real_fill_price > 0:
                price = real_fill_price
            # v4.160 — FIX BUG CRITIQUE : update_leverage() ne verifiait
            # jamais si le levier REELLEMENT applique par Hyperliquid
            # correspondait a celui demande — pour certains actifs
            # (souvent plus risques/moins liquides), Hyperliquid peut
            # plafonner silencieusement a un levier inferieur, sans lever
            # d erreur. Le bot continuait alors a calculer son PnL/afficher
            # un badge base sur le levier DEMANDE, jamais celui REELLEMENT
            # utilise — observe concretement : x3 affiche, x2 reel sur
            # Hyperliquid, PnL du bot sous-evalue de moitie. Verifie
            # desormais apres coup et corrige si un ecart est detecte.
            try:
                # v4.264 — FIX BUG CRITIQUE : lisait uniquement le DEX natif ;
                # une position forex (xyz:) reellement ouverte n etait jamais
                # "confirmee" -> suivi annule -> position ORPHELINE. Lit
                # desormais le bon DEX, avec quelques relectures (l etat du
                # compte peut avoir un leger retard juste apres l execution).
                position_confirmed_on_exchange = False
                exch_pos = None
                for _attempt in range(3):
                    exch_all = fetch_exchange_positions(self.info, cfg["WALLET_ADDRESS"], [ticker])
                    exch_pos = (exch_all or {}).get(ticker)
                    if exch_pos and exch_pos["szi"] != 0:
                        position_confirmed_on_exchange = True
                        break
                    time.sleep(0.7)
                if exch_pos:
                    real_leverage = exch_pos.get("leverage")
                    if real_leverage and real_leverage != leverage:
                        self.emit("log", {"msg": f"[{ticker}] ⚠️ Levier reellement applique par Hyperliquid (x{real_leverage}) different de celui demande (x{leverage}) — correction du suivi interne.", "level": "warn"})
                        leverage = real_leverage
                # v4.239 — SUR DEMANDE EXPLICITE, FIX BUG CRITIQUE : place_order
                # peut retourner ok=True (statuses non vide) alors que la
                # position n existe PAS reellement sur Hyperliquid (ex:
                # rejet partiel dans un batch d ordres, race condition) —
                # confirme par un cas reel (5 positions Accumulation
                # visibles cote bot en mode live, mais JAMAIS ouvertes sur
                # Hyperliquid). Verifie desormais ACTIVEMENT la presence
                # reelle de la position avant d enregistrer le suivi
                # interne — sans cette confirmation, le bot croirait gerer
                # une position reelle (SL/TTP, PnL) qui n existe nulle part
                # ailleurs que dans sa propre memoire.
                if not position_confirmed_on_exchange:
                    self._discard_pending_trade(trade_uid, ticker)
                    self.emit("log", {"msg": f"[{ticker}] ⚠️ ANNULE : ordre signale reussi mais AUCUNE position reelle trouvee sur Hyperliquid — suivi interne non enregistre pour eviter une position fantome.", "level": "error"})
                    return
            except Exception as e:
                print(f"[LEVERAGE-VERIF] Impossible de verifier le levier reel pour {ticker} : {e}")

        state.open_position(signal, price, sl_p, tp_p, size, confidence=confidence, leverage=leverage, strategy=strategy)
        # v4.176 — SUR DEMANDE EXPLICITE : memorise si cette entree a
        # qualifie specifiquement via le mode "trader le range" d
        # Accumulation — exclu du nouveau SL structurel (garde son propre
        # SL % de prix classique), toutes les autres entrees suivent
        # desormais le SL structurel (rupture confirmee du support/
        # resistance).
        state.position["entered_via_range"] = cand.get("entered_via_range", False)
        # v4.89 — SUR DEMANDE EXPLICITE : memorise le mode REEL (paper/live)
        # de CE trade precis au moment de son ouverture — close_position le
        # relit pour alimenter le bon pot (paper_pnl vs live_pnl), jamais
        # melanges. effective_mode_open deja calcule plus haut (utilise pour
        # decider si un ordre reel est passe).
        state.position["effective_mode"] = recorded_mode
        state.position["trade_uid"] = trade_uid      # v4.264 — identifiant exact
        self._recent_entries.setdefault(strategy, []).append(time.time())  # v4.269
        state.position["slot_key"] = symbol          # v4.264
        if cand.get("countertrend"):
            state.position["countertrend"] = True    # v4.286 — trailing resserre
        if cand.get("engine") == "mtf":              # v4.299 — gestion top-down
            state.position.update({"engine": "mtf", "sl": cand["mtf_sl"], "tp": cand["mtf_tp"],
                                   "mtf_r": abs(state.position["entry"] - cand["mtf_sl"]),
                                   "mtf_zone_bound": cand.get("mtf_zone_bound"),
                                   "mtf_path": cand.get("mtf_path", "zone"),  # v4.321 — "zone" ou "tendance"
                                   "mtf_trend_exit_off": bool(cand.get("mtf_trend_exit_off")),  # v4.348
                                   "mtf_swing": bool(cand.get("mtf_swing")), "swing_info": cand.get("swing_info")})   # v4.359
        # v4.24 — memorise les seuils REELLEMENT appliques a CE trade (fixes
        # ou adaptatifs a l ATR) — _manage_position_impl les relit ici en
        # priorite, avec repli sur les valeurs fixes globales si absents
        # (positions ouvertes avant ce fix, ou mode adaptatif desactive).
        # v4.112 — FIX BUG CRITIQUE : sl_pct_of_e est un plafond en % DE E
        # (donc en $, independant du levier par conception — voir plus haut)
        # — mais le MOUVEMENT DE PRIX reellement necessaire pour l atteindre
        # est sl_pct_of_e/levier, qui devient microscopique a fort levier
        # (0.10% a x3 avec le plancher adaptatif de 0.3%) — largement dans
        # le bruit normal du marche, causant des sorties quasi-aleatoires
        # independamment de la qualite de l entree (confirme sur un lot de
        # 47 trades, 46 pertes, pic moyen 0.30% avant SL). Compense
        # desormais sl_pct_of_e A LA HAUSSE si le levier > x1, pour garantir
        # un mouvement de prix minimum (SL_MIN_PRICE_MOVE_PCT, 0.3% par
        # defaut) avant que le SL ne puisse se declencher — quel que soit
        # le levier utilise.
        min_price_move_pct = cfg.get("SL_MIN_PRICE_MOVE_PCT", 0.3)
        if leverage > 1:
            implied_price_move = sl_pct_of_e / leverage
            if implied_price_move < min_price_move_pct:
                sl_pct_of_e = min_price_move_pct * leverage
        state.position["sl_pct_of_e"]   = sl_pct_of_e
        state.position["ttp_arm1_pct"]  = ttp_arm1_pct
        state.position["ttp_lock1_pct"] = ttp_lock1_pct
        state.position["ttp_arm2_pct"]  = ttp_arm2_pct
        state.position["ttp_gap_pct"]   = ttp_gap_pct
        state.position["adaptive_sl_ttp"] = adaptive_used
        # v4.81 — FIX BUG CRITIQUE : le plancher (v4.66) ne vivait QUE dans
        # la branche adaptative — si l ATR etait indisponible a l ouverture
        # (candle_history pas encore assez fourni, cas observe : 62/100
        # bougies), tout le recalcul adaptatif etait saute, y COMPRIS le
        # plancher, laissant passer des seuils tier0/arm1 microscopiques
        # (pics de 0.06-0.09% observes sur des trades pourtant ouverts
        # APRES le deploiement du plancher). Applique desormais le plancher
        # de facon INCONDITIONNELLE, juste avant la memorisation finale sur
        # la position — protege quel que soit le chemin de calcul emprunte.
        min_arm_pct_final = cfg.get("TTP_MIN_ARM_PCT_FLOOR", 0.3)
        tier0_arm_pct = max(tier0_arm_pct, min_arm_pct_final * 0.5)
        ttp_arm1_pct  = max(ttp_arm1_pct, min_arm_pct_final)
        # v4.56 — memorise les seuils tier0 REELLEMENT appliques a CE trade
        # (mis a l echelle si adaptatif) — _manage_position_impl les relit
        # ici en priorite, exactement comme sl_pct_of_e/ttp_arm1_pct.
        state.position["tier0_arm_pct"] = tier0_arm_pct
        state.position["tier0_gap_pct"] = tier0_gap_pct
        state.position["ttp_arm1_pct"]  = ttp_arm1_pct
        # v4.43 — SUR DEMANDE EXPLICITE : Spot-Accumulation memorise le
        # support/resistance mesures a l ENTREE (pas recalcules plus tard,
        # la structure de marche a pu changer) pour calculer l objectif a
        # SPOT_ACCUM_TARGET_SR_PCT (80% par defaut) de cette distance —
        # reinitialise aussi le suivi du trailing pour CETTE position.
        if strategy == "spot_accumulation":
            support_at_entry = cand.get("support_at_entry")
            resistance_at_entry = cand.get("resistance_at_entry")
            target_pct = cfg.get("SPOT_ACCUM_TARGET_SR_PCT", 80.0)
            target_price = None
            if support_at_entry is not None and resistance_at_entry is not None:
                # v4.43 — FIX : calcule depuis le SUPPORT, pas depuis
                # l entree — sinon l objectif peut depasser la resistance
                # elle-meme si l entree est deja significativement au-dessus
                # du support (verifie numeriquement : entree=105,
                # support=100, resistance=120 -> ancienne formule donnait
                # 121$, au-dela de la resistance !).
                target_price = support_at_entry + (target_pct / 100.0) * (resistance_at_entry - support_at_entry)
                # v4.43 — Protection : si l entree est deja proche de la
                # resistance, l objectif a 80% peut tomber SOUS le prix
                # d entree — fermerait la position immediatement apres
                # ouverture. Dans ce cas, pas d objectif fixe : on retombe
                # uniquement sur le trailing (3%/0.5%) pour cette position.
                if target_price <= price:
                    target_price = None
            # v4.49 — SUR DEMANDE EXPLICITE : second seuil de declenchement
            # du trailing, base sur la STRUCTURE du marche (pas juste un %
            # de PnL fixe) — support + 70% de la distance support-resistance
            # (= 30% avant la resistance, memes points). S ADDITIONNE au
            # seuil de PnL existant (SPOT_ACCUM_TTP_ARM_PCT, 3% par defaut) :
            # le trailing s arme des que L UN DES DEUX est atteint, celui
            # qui arrive en premier. Meme protection anti-depassement.
            trailing_arm_price = None
            if support_at_entry is not None and resistance_at_entry is not None:
                arm_pct = cfg.get("SPOT_ACCUM_TRAILING_ARM_SR_PCT", 70.0)
                trailing_arm_price = support_at_entry + (arm_pct / 100.0) * (resistance_at_entry - support_at_entry)
                if trailing_arm_price <= price:
                    trailing_arm_price = None
            state.position["support_at_entry"] = support_at_entry
            state.position["resistance_at_entry"] = resistance_at_entry
            state.position["target_price"] = target_price
            state.position["trailing_arm_price"] = trailing_arm_price
            state.spot_accum_armed = False
            state.spot_accum_peak_pnl_pct = None
        self._save_open_positions()  # v3.2 : sauvegarde en live ET en paper

        # ── v4.7 web : evenement structure pour l API (table trades / signaux) ──
        # tp1/tp2 sont deduits des seuils du TTP (arm1/arm2), DESORMAIS de
        # vrais % de mouvement de prix (v4.7) — plus besoin de les convertir
        # via le notionnel/levier comme avant, ce sont deja des % de prix.
        arm1_price_pct = cfg.get("TTP_ARM1_PRICE_PCT", 1.0)
        arm2_price_pct = cfg.get("TTP_ARM2_PRICE_PCT", 1.3)
        tp1_price = price * (1 + arm1_price_pct/100) if signal == "long" else price * (1 - arm1_price_pct/100)
        tp2_price = price * (1 + arm2_price_pct/100) if signal == "long" else price * (1 - arm2_price_pct/100)
        try:  # v4.281 — qualite du marche AU MOMENT de l entree (calibrage des filtres)
            quality = self.market_quality(ticker, self.states.get(symbol) or state)
        except Exception:
            quality = {"vol_ratio": None, "activity_ratio": None, "flow": None, "spread_pct": None}
        opened_event = {
            "vol_ratio": quality["vol_ratio"], "activity_ratio": quality["activity_ratio"], "flow_at_entry": quality["flow"],
            "spread_at_entry": quality.get("spread_pct"),  # v4.282
            "trade_uid": trade_uid,     # v4.264
            "slot_key": symbol,         # v4.264
            "is_accum_slot": is_accum_slot,
            "coin": ticker,
            "action": label,
            "confidence": round(confidence, 1),
            "leverage": leverage,
            "position_size_pct": cfg["POSITION_SIZE_PCT"],
            "size_usd": round(size, 4),  # v4.28 — taille reelle en $ de CE trade (E)
            # v4.30 — seuils SL/TTP REELLEMENT appliques (fixes ou adaptatifs a l ATR)
            "sl_pct_used": ((cand.get("swing_info") or {}).get("risk_pct") if cand.get("mtf_swing") else sl_pct_of_e),   # v4.359 : risque (% du prix) = 1 R
            "ttp_arm1_pct_used": ttp_arm1_pct,
            "adaptive_sl_ttp": adaptive_used,
            "strategy": strategy,  # v4.8 — "forex" ou "accumulation"
            "trade_mode": recorded_mode,  # v4.90/v4.264 — mode reel (paper/live) de CE trade
            # v4.10 — ratio informatif "mouvement de prix TP / % de E du SL" :
            # a levier x1 c est le vrai ratio gain/risque $. Au-dela, le gain
            # $ est amplifie par le levier (TP en % de prix) alors que la
            # perte $ reste plafonnee (SL en % de E) — ce chiffre SOUS-ESTIME
            # donc le vrai ratio $ reel des que le levier depasse x1 (le
            # ratio reel s ameliore avec le levier, puisque seul le gain grossit).
            "risk_reward": round(arm1_price_pct * max(leverage, 1) / sl_pct_of_e, 2) if sl_pct_of_e else None,  # v4.324 : x levier (le SL est en % de E, le TTP en % de prix)
            "timeframe": "SWING_D1H4" if cand.get("mtf_swing") else cfg.get("PROFILE", "swing"),   # v4.359 : etiquette du profil Swing
            "entry": price,
            "stop_loss": sl_p,
            "take_profit1": tp1_price,
            "take_profit2": tp2_price,
            "rsi": round(rsi, 1) if rsi is not None else None,
            "entry_reasons": " | ".join(reasons),
            "confidence_breakdown": json.dumps(conf_breakdown),
        }
        # v4.264 — ecriture SYNCHRONE en base (plus de dependance a la file d
        # evenements, qui pouvait etre perdue si le process etait coupe).
        try:
            db.upsert_open_trade(opened_event)
            opened_event["persisted"] = True
        except Exception as e_up:
            print(f"[TRADE-INDEX] Ecriture synchrone {trade_uid} impossible ({e_up}) — repli sur la file d evenements.")
        self.emit("trade_opened", opened_event)
        if cand.get("mtf_swing") and cand.get("swing_key"):          # v4.359 : le signal journalise devient pris
            try:
                db.swing_event_mark_taken(cand["swing_key"][0], cand["swing_key"][1], cand["swing_key"][2], trade_uid)
            except Exception as _e_sw:
                print(f"[SWING-JOURNAL] marquage pris ignore : {_e_sw}")

    def _finalize_pending_candidates(self):
        """v3.2 — Appelee une fois par cycle, APRES avoir evalue tous les
        symboles : classe les candidats valides par confiance decroissante
        et remplit les slots disponibles (MAX_OPEN_TRADES) en priorite avec
        les meilleurs scores. Les candidats laisses de cote ce cycle (faute
        de place) resteront candidats aux cycles suivants si leur signal
        persiste toujours.
        v4.8 — MAX_OPEN_TRADES ne compte desormais que les positions de
        strategie "forex" : le mode Accumulation a son propre plafond
        independant (voir _finalize_pending_accumulation_candidates),
        les deux pools de slots ne se disputent plus la meme limite."""
        if not self._pending_candidates:
            return
        self._publish_opportunities("forex", self._pending_candidates)  # v4.273 — trading manuel
        cfg = self.cfg
        self._pending_candidates.sort(key=lambda c: c["confidence"], reverse=True)
        max_open = cfg.get("MAX_OPEN_TRADES")
        for cand in self._pending_candidates:
            open_count = sum(1 for st in self.states.values() if st.position and st.position.get("strategy", "forex") == "forex")
            if max_open is not None and open_count >= max_open:
                self.emit("log", {
                    "msg": f"[{cand['ticker']}] Slot plein ({open_count}/{max_open}) — candidat a {cand['confidence']:.0f}% laisse de cote ce cycle (meilleurs scores prioritaires).",
                    "level": "dim"
                })
                continue
            self._finalize_open(cand)
        self._pending_candidates = []

    def _finalize_pending_accumulation_candidates(self):
        """v4.8 — Equivalent de _finalize_pending_candidates, mais pour les
        candidats du mode Accumulation : plafond independant
        (ACCUMULATION_MAX_TRADES), classes eux aussi par confiance
        decroissante."""
        if not self._pending_accumulation_candidates:
            return
        self._publish_opportunities("accumulation", self._pending_accumulation_candidates)  # v4.273 — trading manuel
        cfg = self.cfg
        self._pending_accumulation_candidates.sort(key=lambda c: c["confidence"], reverse=True)
        max_acc = cfg.get("ACCUMULATION_MAX_TRADES", 3)
        for cand in self._pending_accumulation_candidates:
            # v4.121 — FIX BUG CRITIQUE : comptait depuis self.states, mais
            # Accumulation a desormais son PROPRE emplacement
            # (self.accum_states) — sans ce correctif, ce plafond restait
            # TOUJOURS a 0/max_acc, permettant un nombre ILLIMITE de trades
            # Accumulation simultanes, quel que soit le reglage.
            open_count = sum(1 for st in self.accum_states.values() if st.position)
            if open_count >= max_acc:
                self.emit("log", {
                    "msg": f"[{cand['ticker']}] 🎯 Slot Accumulation plein ({open_count}/{max_acc}) — candidat a {cand['confidence']:.0f}% laisse de cote ce cycle.",
                    "level": "dim"
                })
                continue
            self._finalize_open(cand)
        self._pending_accumulation_candidates = []

    def _finalize_pending_funding_candidates(self):
        """v4.33 — Equivalent de _finalize_pending_candidates, pour les
        candidats du mode Funding Contrarian : plafond independant
        (FUNDING_MODE_MAX_TRADES)."""
        if not self._pending_funding_candidates:
            return
        self._publish_opportunities("funding_contrarian", self._pending_funding_candidates)  # v4.273 — trading manuel
        cfg = self.cfg
        self._pending_funding_candidates.sort(key=lambda c: c["confidence"], reverse=True)
        max_funding = cfg.get("FUNDING_MODE_MAX_TRADES", 3)
        for cand in self._pending_funding_candidates:
            open_count = sum(1 for st in self.states.values() if st.position and st.position.get("strategy") == "funding_contrarian")
            if open_count >= max_funding:
                self.emit("log", {
                    "msg": f"[{cand['ticker']}] 💰 Slot Funding Contrarian plein ({open_count}/{max_funding}) — candidat a {cand['confidence']:.0f}% laisse de cote ce cycle.",
                    "level": "dim"
                })
                continue
            self._finalize_open(cand)
        self._pending_funding_candidates = []

    def _finalize_pending_spot_accum_candidates(self):
        """v4.43 — Equivalent de _finalize_pending_candidates, pour les
        candidats Spot-Accumulation : plafond independant (SPOT_ACCUM_MAX_TRADES)."""
        if not self._pending_spot_accum_candidates:
            return
        self._publish_opportunities("spot_accumulation", self._pending_spot_accum_candidates)  # v4.273 — trading manuel
        cfg = self.cfg
        self._pending_spot_accum_candidates.sort(key=lambda c: c["confidence"], reverse=True)
        max_spot = cfg.get("SPOT_ACCUM_MAX_TRADES", 3)
        for cand in self._pending_spot_accum_candidates:
            open_count = sum(1 for st in self.states.values() if st.position and st.position.get("strategy") == "spot_accumulation")
            if open_count >= max_spot:
                self.emit("log", {
                    "msg": f"[{cand['ticker']}] 🌱 Slot Spot-Accumulation plein ({open_count}/{max_spot}) — candidat a {cand['confidence']:.0f}% laisse de cote ce cycle.",
                    "level": "dim"
                })
                continue
            self._finalize_open(cand)
        self._pending_spot_accum_candidates = []


# ─────────────────────────────────────────────
#  DASHBOARD TKINTER
# ─────────────────────────────────────────────
