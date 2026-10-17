# HyperBot Web — déploiement GitHub + Railway

Version web (sans interface Tkinter) du bot de trading, avec l'interface
`index.html` fournie branchée sur une vraie API FastAPI et une base SQLite.

Version courante : **4.299** (visible dans `/health` et dans les logs de démarrage).

## 1. Structure du projet

```
hyperbot_web/
├── api.py           # API FastAPI (tous les endpoints consommés par index.html)
├── bot_engine.py     # Moteur de trading (extrait du bot Tkinter, sans dépendance graphique)
├── db.py            # Persistance SQLite (utilisateurs, trades, config)
├── auth.py          # Hash de mot de passe + tokens de session (JWT)
├── index.html        # Interface web (React, servie directement par l API)
├── requirements.txt
├── Procfile          # Commande de démarrage Railway
└── .gitignore
```

## 2. Tester en local avant de déployer

```bash
python3 -m venv venv
source venv/bin/activate          # Windows : venv\Scripts\activate
pip install -r requirements.txt

export HYPERBOT_SECRET_KEY="change-moi-en-une-longue-chaine-aleatoire"
export HYPERBOT_DATA_DIR="./data"

uvicorn api:app --reload --port 8000
```

Ouvre `http://localhost:8000` — tu dois voir l'écran de connexion. Crée ton
compte (premier et unique compte autorisé, voir section 5), connecte-toi,
puis clique sur DÉMARRER pour vérifier que le bot tourne (regarde les logs
dans le terminal et dans l'onglet Logs de l'interface).

## 3. Déploiement GitHub

```bash
cd hyperbot_web
git init
git add .
git commit -m "HyperBot Web v1"
git branch -M main
git remote add origin https://github.com/TON_COMPTE/hyperbot-web.git
git push -u origin main
```

Le `.gitignore` exclut déjà la base SQLite, les fichiers de capital/session
et les logs — ils ne doivent jamais être versionnés (données personnelles +
état runtime, pas du code).

## 4. Déploiement Railway

1. Sur [railway.app](https://railway.app), **New Project → Deploy from GitHub repo** → sélectionne `hyperbot-web`.
2. Railway détecte le `Procfile` automatiquement (sinon Settings → Start Command : `uvicorn api:app --host 0.0.0.0 --port $PORT`).
3. **Variables d'environnement** (Settings → Variables) :

| Variable | Rôle |
|---|---|
| `HYPERBOT_SECRET_KEY` | **Obligatoire (32 caractères minimum).** Le serveur refuse de démarrer sans elle. Générer : `python -c "import secrets; print(secrets.token_urlsafe(48))"` |
| `HYPERBOT_DATA_DIR` | `/data` (voir volume ci-dessous) |
| `HYPERBOT_PRIVATE_KEY` | Clé privée Hyperliquid (mode live) |
| `HYPERBOT_WALLET_ADDRESS` | Adresse du wallet Hyperliquid |
| `HYPERBOT_FINNHUB_API_KEY` | Clé Finnhub (optionnel, blackout CPI) |

4. **Volume persistant (important)** — Settings → Volumes → *Add Volume*, monte-le sur `/data`. Sans ça, la base SQLite, le capital et les logs sont **remis à zéro à chaque redéploiement**. Avec le volume monté sur `/data` et `HYPERBOT_DATA_DIR=/data`, tout survit aux redéploiements.
5. Déploie. Railway te donne une URL (`https://xxxx.up.railway.app`) — c'est ton dashboard.

## 4bis. Clé API Hyperliquid — désormais obligatoire (paper ET live)

Depuis cette version, `HYPERBOT_PRIVATE_KEY` et `HYPERBOT_WALLET_ADDRESS` sont
**obligatoires**, y compris en mode paper : le bot n'utilise plus jamais de
prix simulés en interne. En mode paper, les prix et le flux WebSocket
viennent bien de Hyperliquid — seule la **passation d'ordres réels** reste
désactivée (aucun ordre n'est envoyé à l'exchange tant que `trading_mode`
n'est pas `live`). Sans ces deux variables, `/api/bot/start` répond
immédiatement avec une erreur 400 explicite plutôt que de démarrer un bot
qui échouerait silencieusement.

## 4ter. Le bot tourne côté serveur, indépendamment du navigateur

Le bot tourne **dans le process Railway**, pas dans ton navigateur : une
fois démarré, il continue de tourner même si tu fermes l'onglet ou éteins
ton PC. La page web n'est qu'une télécommande à distance.

**Redémarrage automatique après un redéploiement** — l'état souhaité
(DÉMARRÉ / ARRÊTÉ) est mémorisé en base à chaque clic sur DÉMARRER/ARRÊTER.
Au démarrage du process (redéploiement Railway, crash-restart, etc.), le
bot relit cet état :
- s'il était **DÉMARRÉ**, il redémarre automatiquement tout seul (aucune
  action requise sur la page web) ;
- s'il était **ARRÊTÉ** (tu as explicitement cliqué ARRÊTER), il reste
  arrêté même après un redéploiement, jusqu'à ce que tu recliques sur
  DÉMARRER.

Cliquer sur ARRÊTER depuis la page web arrête bien le vrai bot qui tourne
sur Railway (il n'y a qu'une seule instance, pas de simulation côté
navigateur) — et cet arrêt persiste across redéploiements, comme décrit
ci-dessus.

## 4quater. Vérifier que la persistance fonctionne vraiment

Va sur `https://TON_URL.up.railway.app/health` — tu verras :

```json
{
  "data_dir": "/data",
  "data_dir_configured": true,
  "boot_count": 3,
  ...
}
```

- `data_dir_configured: false` → tu n'as pas défini `HYPERBOT_DATA_DIR` sur Railway → rien ne persiste.
- `boot_count` doit **augmenter** (1, 2, 3...) à chaque redéploiement. S'il repart toujours à 1, le Volume n'est pas monté correctement même si la variable est définie (vérifie le point de montage dans Settings > Volumes).

## 4quinquies. Garder les actifs actifs/inactifs sans Volume

Comme pour les clés Hyperliquid/Finnhub, tu peux fixer les actifs actifs via
une variable d'environnement Railway plutôt que via l'interface web (ce qui
nécessiterait un Volume pour survivre à un redéploiement) :

| Variable | Exemple | Rôle |
|---|---|---|
| `HYPERBOT_ACTIVE_COINS` | `BTC,ETH,xyz:EUR` | Liste des actifs actifs, séparés par des virgules, parmi ceux de `SYMBOLS` (cryptos + forex `xyz:EUR`, `xyz:JPY`, `xyz:KRW`, `xyz:DXY`). La casse est normalisée (`XYZ:eur` → `xyz:EUR`). Absente = liste par défaut. |

Un changement fait depuis l'interface web reste prioritaire tant que le
process ne redémarre pas, mais **sans Volume, il est perdu au prochain
redéploiement** — la variable d'environnement, elle, survit toujours.

## 4sexies. Temps de fonctionnement réel (hors arrêts)

Le "temps de trading" affiché dans le bilan ne compte plus le temps écoulé
depuis un reset, mais le **temps réellement passé avec le bot démarré**
(`running_seconds`) — les périodes où il était arrêté (manuellement ou
suite à un redéploiement en attente de redémarrage) ne sont pas comptées.
Ce compteur est stocké en base (`total_running_seconds` / `running_since`)
et **nécessite donc lui aussi un Volume pour survivre aux redéploiements** —
sans Volume, il repart de zéro à chaque redéploiement comme le reste des
données dynamiques (trades, etc.), contrairement aux clés API qui peuvent
être fixées par variable d'environnement.

## 4septies. Indexation des trades et reprise après redémarrage

Chaque trade reçoit à l'ouverture un **identifiant unique** (`trade_uid`) qui
encode son **mode source** (Forex, Accumulation, Spot-Accum, Funding) et son
**heure d'ouverture**. Il est :

- écrit en base **avant** l'envoi de l'ordre (statut `pending`), puis complété
  dès l'ouverture confirmée (statut `open`) ;
- envoyé à Hyperliquid comme identifiant client de l'ordre d'entrée (`cloid`).

Au redémarrage, la reprise se fait dans cet ordre :

1. **Positions sauvegardées** (`hyperbot_positions.json`) restaurées à
   l'identique (mode, heure, niveaux, état du trailing). Une position live
   absente d'Hyperliquid est clôturée comme « fermée pendant la coupure ».
2. **Positions réelles non couvertes** : rattachées à leur trade via la base,
   puis via le `cloid` dans l'historique d'ordres Hyperliquid (fonctionne même
   si la base locale a été perdue).
3. En dernier recours seulement, une position inconnue est suivie en mode
   forex avec une **alerte explicite** dans les logs.

Si Hyperliquid est illisible au démarrage, rien n'est supprimé ni déduit.
Le fichier de positions est écrit de façon atomique et n'est plus vidé à la
lecture. Les DEX natif **et** `xyz` (forex) sont interrogés partout.

## 4octies. Fermetures réelles vérifiées

En live, toute fermeture (SL, TTP, retournement, objectif, conflit, fermeture
manuelle) envoie **d'abord** l'ordre à Hyperliquid et vérifie ensuite que la
position a réellement disparu. Le suivi interne n'est fermé qu'à cette
condition ; sinon la position reste suivie et la fermeture est retentée au
cycle suivant. Le mode paper/live est celui **de l'ouverture** de la position,
même si la stratégie a été rebasculée depuis.

## 4nonies. Suivi et export des trades Spot-Accum / Accumulation / Funding

Onglet **Historique** → bouton **📥 Télécharger le suivi (CSV)** (fichier
lisible directement par Excel : séparateur `;`, virgule décimale).

Pour chaque trade : mode, actif, sens, paper/live, heures d'ouverture et de
fermeture (heure de Paris), durée, motif de sortie, levier, marge et
notionnel, prix d'entrée et de sortie (avec leur source), mouvement de prix,
pic, PnL brut, frais, PnL net, puis le **comportement du prix après la
sortie** : prix à +30 et +60 min, meilleur mouvement dans l'heure et, pour les
trades perdants, si le prix est revenu au niveau d'entrée dans l'heure.

- Le suivi après sortie est calculé automatiquement ~1 h après chaque
  fermeture à partir des bougies Hyperliquid, dans un fil séparé (aucun
  impact sur le trading). Les trades déjà en base (jusqu'à ~16 jours) sont
  complétés rétroactivement, par lots, après le déploiement.
- En live, les frais et le PnL réels proviennent des remplissages
  Hyperliquid ; à défaut, les frais sont estimés (0,09 % du notionnel par
  aller-retour) et signalés comme tels.
- Depuis la 4.265, le PnL d'une sortie live est calculé sur le **prix
  réellement exécuté** par Hyperliquid (colonne « source prix sortie » =
  `hyperliquid`), et non plus sur le prix vu par le bot au moment de décider.

## 4decies. Flux de transactions (pression acheteurs / vendeurs)

Spot-Accum et Accumulation utilisent le flux WebSocket `trades` d'Hyperliquid
pour mesurer la pression directionnelle (−1 = 100 % vendeurs agressifs, +1 =
100 % acheteurs agressifs), calculée sur une **fenêtre de temps fixe**
(`TRADE_FLOW_WINDOW_SEC`, 180 s par défaut). Le flux est réabonné à chaque
reconnexion WebSocket ; s'il ne reçoit plus rien (`TRADE_FLOW_WS_DEAD_SEC`),
le bot bascule automatiquement sur des requêtes REST.

Réglages (onglet Réglages avancés) :

| Réglage | Défaut | Rôle |
|---|---|---|
| `ENTRY_FLOW_CONTRADICTION_THRESHOLD` | 0.3 | Veto : refuse l'entrée si le flux est nettement contraire |
| `ENTRY_FLOW_CONFIRM_MIN_PRESSURE` | 0 (désactivé) | Exige un flux favorable à l'entrée (ex. 0.1 : achat net ≥ +0,1 pour Spot-Accum, vente nette ≤ −0,1 pour Accumulation) |
| `TRADE_FLOW_WINDOW_SEC` | 180 | Fenêtre d'analyse du flux |

## 4undecies. Trailing take profit du mode Funding

Le TTP Funding s'arme dès **+1 %** de mouvement de prix favorable
(`FUNDING_TTP_ARM_PCT`). Une fois armé, quand le prix se replie, le **flux de
transactions** décide de la patience :

| Flux au moment du repli | Repli toléré depuis le pic | Motif affiché |
|---|---|---|
| Favorable au trade (vendeurs dominants pour un short) | 0,9 % (patience) | `(Funding, après patience)` |
| Neutre ou indisponible | 0,5 % | `(Funding)` |
| Contraire au trade | 0,25 % (sortie anticipée) | `(Funding, flux contraire)` |

Quelle que soit la décision, un trade armé ne redescend jamais sous **+0,3 %**
de gain (`FUNDING_TTP_MIN_LOCK_PCT`, motif `(Funding, plancher)`). Tous ces
seuils sont modifiables dans les réglages avancés.

## 4duodecies. Réglages par mode et simulation du SL (v4.269)

**Simulation « SL plus large ».** Pour chaque trade sorti par stop loss, le
suivi rejoue la minute par minute l'heure qui suit la sortie et indique, pour
un SL hypothétique à 0,75 %, 1 % et 1,5 %, si le prix serait d'abord **revenu
au niveau d'entrée** ou aurait **touché ce SL** (bougie ambiguë = SL touché,
par prudence). Colonnes « si SL à … : issue » de l'export CSV. L'export
indique aussi les **raisons d'entrée** de chaque trade, et les heures sont
dans le fuseau de votre navigateur.

**Réglages par mode** (onglet Réglages avancés, vide = réglage général) :

| Réglage | Défaut |
|---|---|
| Spot-Accum / Accumulation — SL plafond (% de prix) | 0,5 (hérité) |
| Spot-Accum / Accumulation — confirmation flux à l'entrée | réglage général (0) |
| Accumulation — délai après perte sur un actif | **3600 s** |
| Accumulation — entrées max par fenêtre de 10 min | **3** |
| Spot-Accum / Funding — délai après perte, Spot-Accum — entrées max | 0 (désactivé) |

**Correctif important :** jusqu'à la 4.268, le profil (SWING/SCALP) était
appliqué **après** les réglages enregistrés, et écrasait à chaque redémarrage
37 réglages avancés. Depuis la 4.269, vos réglages sont conservés.

## 4terdecies. Patience du SL Spot-Accum pilotée par le flux (v4.270)

Quand un trade Spot-Accum atteint son SL plafond (0,5 % par défaut), le bot
lit le flux de transactions :

- **acheteurs dominants** (pression ≥ +0,2) **et tendance de fond intacte**
  (prix au-dessus de l'EMA200, v4.271) → il **attend** au lieu de couper,
  dans la limite d'une perte de **1 %** et de **15 minutes** ;
- flux neutre ou vendeur → sortie immédiate, comme avant.

Pendant l'attente, la sortie se fait dès que le flux devient défavorable, que
la perte atteint 1 % ou que les 15 minutes sont écoulées. Motifs distincts :
`STOP LOSS (flux defavorable)`, `(tendance cassee)`, `(plafond patience)`,
`(patience expiree)` ;
un trade sauvé qui finit autrement porte la mention `· apres patience SL`.
Réglages avancés : seuil de pression, perte maximale, durée maximale
(0 = patience désactivée).

## 4quaterdecies. Mode Forex recalibré (v4.272)

Le mode Forex (lignes « 💱 FOREX LONG / SHORT » du diagnostic) ne trade que
les actifs de `FOREX_MODE_SYMBOLS` (devises `xyz:` et PAXG). Deux seuils
calibrés pour les cryptos l'empêchaient d'entrer :

| Réglage | Avant | Après |
|---|---|---|
| `FOREX_ANTI_RANGE_MIN_PCT` (mouvement minimal sur 30 bougies 5 min) | 2 % | 0,25 % |
| `FOREX_LONG_TERM_MOMENTUM_MIN_CHANGE_PCT` (repli de la confirmation de tendance) | 2 % | 0,4 % |

Le diagnostic affiche désormais le blocage « marché en range » pour les
devises, et « non concerné » pour les cryptos.

## 4quindecies. Onglet « ✋ Trading Manuel » (v4.273)

1. **Opportunités** : le bot publie en continu les entrées que ses modes
   détectent (y compris celles qu'il ne prend pas, faute de place). Une
   opportunité reste choisissable tant que le bot la reconfirme (moins de
   `MANUAL_OPPORTUNITY_TTL_SEC` = 120 s), et **uniquement dans le sens prévu**.
2. **Proposition** : prix, taille (15 $ de notionnel par défaut), levier, SL
   (logique du mode source), TP (80 % de la fourchette support/résistance) et
   trailing (réglages du mode source). **Tout est modifiable.**
3. **Exécution** : maintenant, ou programmée (prix ≥, prix ≤, ou heure), avec
   expiration (24 h par défaut) et annulation automatique si le bot ne
   confirme plus l'opportunité au déclenchement (option).
4. **Paper / Live** : le live exige une confirmation explicite. **Perps ou
   Spot** : le spot n'autorise que l'achat, sans levier ; les USDC doivent
   être disponibles côté Spot.
5. **Suivi** : chaque position manuelle est gérée par le bot selon SES
   paramètres (SL, TP, trailing), modifiables à tout moment, avec fermeture
   manuelle. Historique dédié, strategie « Manuel » dans l'export CSV.

### Ordre libre (v4.343)

Carte « ✏️ Ordre libre » en haut de l'onglet : choisissez **n'importe quel actif suivi par le bot**
(crypto ou Forex `xyz:`), le sens, puis « Préparer l'ordre ». Le ticket s'ouvre avec des valeurs
par défaut (`MANUAL_DEFAULT_NOTIONAL_USD`, `MANUAL_DEFAULT_SL_PCT`), sans opportunité du bot, et
**tout est modifiable** : paper ou live, perp ou spot, notionnel, levier, entrée (maintenant, prix,
heure), SL, TP, trailing, et deux options ajoutées :

- **SL dynamique (verrou progressif)** : dès que le gain maximal atteint le seuil choisi (+X %), le
  SL monte pour garantir la part choisie (Y %) de ce gain maximal, et ne redescend jamais (miroir en
  short). Sortie « STOP DYNAMIQUE (manuel) ». En perp live, le SL natif Hyperliquid est mis à jour
  (écart minimal `MANUAL_LOCK_PUSH_MIN_STEP_PCT` = 0,05 %, dans un thread). Sans effet sur un SL
  fixe : le verrou ne fait que relever le SL existant. Modifiable en cours de route.
- **Critères de tendance H4 (choix manuels)** : *condition d'entrée* (haussière, baissière ou
  neutre — l'ordre est refusé si la tendance diffère, est indisponible ou si les bougies sont
  périmées ; pour un ordre programmé, il est annulé au déclenchement) et *règle de sortie* (« si la
  tendance devient opposée » ou « si elle n'est plus celle du sens du trade », vérifiée toutes les
  5 s au plus, uniquement sur des bougies H4 à jour). Même calcul que le top-down.

**Sécurités** : Hyperliquid ne tient qu'une position perp par actif — un ordre
manuel live est refusé si le bot (ou vous) détient déjà une position live sur
l'actif, et le bot n'ouvre pas en live sur un actif tenu manuellement. En perp
live, le SL est aussi posé en ordre natif sur Hyperliquid (mis à jour si vous
le modifiez). En spot, la protection repose sur le bot seul. Les ordres
programmés et positions manuelles survivent aux redémarrages.

## 4sedecies. Régime de marché et plages horaires (v4.276)

**Régime de marché** (recalculé toutes les 5 min) : il est **haussier
confirmé** quand BTC en 1 h est au-dessus de son EMA200 (d'au moins 0,2 %)
avec l'EMA50 au-dessus de l'EMA200, **et** qu'au moins 60 % des actifs suivis
sont au-dessus de leur propre EMA200 **en 1 h** (même unité de temps que
BTC — depuis la 4.280) ; **baissier confirmé** dans le cas
inverse ; **neutre** sinon, y compris tant que la largeur de marché n'est pas
mesurable (au moins 8 actifs).

À ne pas confondre avec la tendance propre à chaque mode : Spot-Accum et
Accumulation exigent en plus la tendance **court terme** de l'actif (EMA200 sur
des points de 2 min, soit ~6 h 40). Un marché peut donc être haussier de fond
(régime) tout en étant en repli à court terme sur de nombreux actifs.

- Marché baissier confirmé → **Spot-Accum** (achats) n'ouvre plus rien.
- Marché haussier confirmé → **Accumulation** (shorts) n'ouvre plus rien.
- Neutre → les deux modes fonctionnent normalement.

Le régime s'affiche en tête du diagnostic d'entrée et dans l'onglet
Historique ; chaque changement de régime est journalisé.

**Plages horaires par mode** (réglages avancés, heures UTC, 0-24 = toujours) :
Spot-Accum, Accumulation et Funding. Le tableau « Résultats par tranche
horaire » de l'onglet Historique (tranches de 4 h, en UTC et à votre heure
locale) aide à les choisir.

## 4septdecies. Entrées par cassure/rebond encadrées (v4.277)

Spot-Accum et Accumulation ont des voies d'entrée « de contournement »
(cassure fraîche, cassure sur volume, fausse cassure, tendance persistante,
étoile filante pour Accumulation). Elles dispensaient de TOUS les filtres
principaux, y compris le sens de la tendance EMA200 et le veto du flux.
Depuis la 4.277, elles ne dispensent plus que de la stabilité de tendance et
de l'ADX : elles doivent respecter le sens de la tendance et passer le veto
du flux. Réglages avancés `*_BYPASS_REQUIRE_TREND` / `*_BYPASS_FLOW_VETO`
(1 = oui, 0 = ancien comportement).

## 4octodecies. Spot-Accum : achat sur repli (v4.278)

En hausse régulière, le prix s'éloigne du support d'origine de la tendance.
Spot-Accum suit désormais aussi un **support ascendant** : le dernier creux de
repli « plus haut que le précédent » sur les bougies 1 h (creux validé par 2
bougies de chaque côté, sur les 3 derniers jours). Un repli sur ce niveau est
une entrée valide, avec les mêmes protections (bougie haussière, veto du flux,
tendance EMA200, régime de marché). Si le creux est cassé en clôture, le
support ascendant disparaît.

La **voie d'entrée** (proche du support, repli sur support ascendant, cassure
fraîche, fausse cassure, tendance persistante, volume, étoile filante…) figure
maintenant dans les raisons d'entrée — colonne « raisons d'entrée » de
l'export CSV — pour Spot-Accum et Accumulation.

## 4novodecies. Cassure fraîche et cohérence des voies d'entrée (v4.279)

**Cassure fraîche contre la tendance** : elle peut de nouveau capter le début
d'un retournement avant que l'EMA200 ne suive, mais seulement si le flux le
confirme franchement (pression ≥ +0,2 pour Spot-Accum, ≤ −0,2 pour
Accumulation) et si le régime de marché n'est pas opposé. Les autres voies de
contournement restent soumises au sens de l'EMA200.

**Contradictions corrigées** :
- le filtre anti-range bloquait les voies « cassure fraîche » (qui sort d'une
  consolidation) et « volume en consolidation » (qui l'exige) — pour
  Accumulation, la voie volume ne pouvait même jamais aboutir (même calcul,
  mêmes réglages) ; ces deux voies en sont désormais exemptées ;
- la voie volume subissait deux critères de proximité différents (support de
  la consolidation puis support dynamique) : un seul est conservé.

## 4vicies. Qualité du marché et anti-range relatif (v4.281)

**Anti-range relatif à chaque actif.** Le seuil absolu (2 % de mouvement pour
toutes les cryptos, 0,25 % pour le Forex) est remplacé par un seuil adapté à
l'actif : amplitude horaire **habituelle** de l'actif (médiane sur 7 jours de
bougies 1 h) × 0,6 × racine de la durée de la fenêtre en heures. Exemples :
sur 1 h, ~0,36 % pour un actif calme comme BTC, ~1,2 % pour un actif nerveux
comme WIF. Les seuils absolus ne servent plus que de repli tant que
l'habitude de l'actif n'est pas connue. Réglages : `ANTI_RANGE_RELATIVE_ENABLED`,
`ANTI_RANGE_REL_MULT`.

**Qualité du marché** (tous les modes), mesurée par rapport aux habitudes de
l'actif et affichée au diagnostic :
- **volatilité** : amplitude de la dernière heure / amplitude horaire habituelle ;
- **activité** : volume $ de la dernière heure / volume horaire habituel ;
- **flux** : pression acheteurs/vendeurs ;
- **spread** (v4.282) : écart entre meilleure offre d'achat et de vente du
  carnet d'ordres, en % du prix — coût d'un aller-retour au marché en plus
  des frais (lu à la demande, mis en cache 30 s par actif).

Ces quatre mesures sont enregistrées à **chaque entrée** (colonnes de l'export
CSV). Les filtres correspondants (« marché endormi », « marché déserté »,
« flux sans conviction » par mode, « spread trop large ») sont prêts mais **désactivés (0)** en
attendant d'être calibrés sur les résultats réels.

## 4unvicies. Indicateurs vérifiables sur le graphique Hyperliquid (v4.283)

Hyperliquid ne fournit aucun indicateur calculé (EMA, support/résistance…) :
le bot les calcule, désormais **entièrement à partir des vraies bougies
Hyperliquid** :

| Indicateur | Calcul | À vérifier sur le graphique |
|---|---|---|
| Tendance de chaque mode | **EMA 80 sur bougies 5 min** (~6 h 40, même horizon que l'ancienne EMA200 sur points internes de 2 min) | ajouter une EMA de longueur 80 en unité 5 min |
| Support / résistance (entrées) | plus bas / plus haut des **40 dernières bougies 5 min** (~3 h 20) | |
| Support / résistance Accumulation | **288 bougies 5 min** (~24 h) | |
| Régime de marché | EMA200 / EMA50 sur bougies 1 h | EMA 200 en unité 1 h |

**Depuis la 4.284, les bougies 5 min et 1 h arrivent en temps réel par
WebSocket** (canal `candle` d'Hyperliquid, 2 flux par actif) : chaque bougie
est prise en compte à l'instant où elle se clôture — les indicateurs sont
calculés sur la dernière bougie clôturée (« l'avant-dernière » du graphique),
jamais sur la bougie en cours. Le REST ne sert plus qu'au chargement initial de
l'historique, à une resynchronisation de sécurité toutes les 6 h, après chaque
reconnexion (bougies manquées) et en repli si le flux d'un actif reste muet.
Le diagnostic affiche les
valeurs exactes (« 📐 Niveaux ») pour comparaison directe avec le graphique.

## 4duovicies. Situations de marché et choix des niveaux (v4.286)

Chaque actif est classé en croisant sa tendance de **fond** (prix / EMA200
1 h, ~8 jours) et sa tendance **court terme** (prix / EMA80 5 min, ~6 h 40) :

| Situation | Spot-Accum (achat) | Accumulation (short) | Niveaux utilisés |
|---|---|---|---|
| **Hausse saine** (fond ↑, court ↑) | toutes les voies | ❌ | 1 h puis 5 min |
| **Repli dans une hausse** (fond ↑, court ↓) | **fin de repli** : achat sur le support de structure 1 h + bougie haussière + flux ≥ +0,2 | ❌ contre-tendance désactivée (v4.287) | Spot : 1 h |
| **Baisse saine** (fond ↓, court ↓) | ❌ | toutes les voies | 1 h puis 5 min |
| **Rebond dans une baisse** (fond ↓, court ↑) | ❌ contre-tendance désactivée (v4.287) | **fin de rebond** : short sous la résistance de structure 1 h + bougie baissière + flux ≤ −0,2 | Accu : 1 h |
| Fond neutre | règles court terme habituelles | règles court terme habituelles | 1 h |

Règle de choix des niveaux : **dans le sens du fond**, le bot utilise la
structure 1 h (support ascendant / résistance descendante, ou plus bas / plus
haut de la tendance 1 h) ; **à contre-fond**, il utilise les niveaux 5 min
(mouvement court) et exige de la marge avant le niveau 1 h opposé, qui sert
de plafond / plancher. Le diagnostic affiche la situation de chaque actif et
les deux jeux de niveaux. Ces règles remplacent le blocage par le régime
global (`SITUATION_RULES_ENABLED` = 0 pour revenir à l'ancien comportement).

**Depuis la 4.287, les trades à contre-tendance sont désactivés** (achat dans
un rebond baissier, short dans un repli haussier, cassure fraîche contre l'EMA
de tendance) : chaque mode ne trade que dans le sens de la tendance de fond.
Réactivables via `SITUATION_ALLOW_COUNTERTREND` et
`*_FRESH_BREAKOUT_COUNTER_TREND` (réglages avancés) lorsque des données
montreront qu'ils sont gagnants.

## 4trevicies. Voie « continuation » (v4.288)

La distance à un support (Spot-Accum) ou à une résistance (Accumulation) n'est
plus à elle seule une raison de refuser une entrée en **tendance saine**. La
voie « continuation » autorise l'entrée loin d'un niveau si **toutes** ces
conditions sont réunies :

1. situation **hausse saine** (Spot-Accum) ou **baisse saine** (Accumulation) ;
2. flux franc **et confirmé** : ≥ +0,3 (ou ≤ −0,3) sur la lecture actuelle ET la précédente ;
3. marché actif : activité ≥ 1× l'habitude de l'actif ;
4. pas d'excès : écart à l'EMA80 5 min ≤ 1,5 ATR 5 min ;
5. place pour gagner : prochain niveau opposé (5 min et 1 h) à ≥ 2× le SL ;
6. bougie dans le sens du trade.

**Phase de test : ces entrées sont simulées en paper même si le mode est en
live** (`CONTINUATION_PAPER_ONLY` = 1), et marquées « continuation » dans
l'export. Le diagnostic indique la condition manquante quand la voie est
refusée.

## 4quattuorvicies. Nettoyage : moteur d'entrée simple (v4.289)

Spot-Accum et Accumulation n'utilisent plus la longue chaîne de conditions
éliminatoires, mais trois étapes :

1. **Garde-fous** : mode actif, actif sélectionné, plage horaire, qualité du
   marché (si activée), situation compatible (jamais à contre-tendance de
   fond), étoile filante en cours (achats).
2. **Un signal parmi trois** : proche d'un niveau (1 h ou 5 min — 1 h
   seulement en fin de repli / de rebond), cassure fraîche, continuation.
3. **Une confirmation** : le flux ne contredit pas l'entrée (et confirme le
   retournement en fin de repli / de rebond).

Supprimés : stabilité 12 cycles, ADX, anti-range, couleur de bougie,
fourchette S/R minimale, seuil de confiance appris, voies fausse cassure,
volume, tendance persistante et étoile filante (doublons). Restent à
l'ouverture : plafond de positions, délai après perte, limite de rafales,
conflit entre modes. Ancienne chaîne : `ENTRY_ENGINE_SIMPLE` = 0.

**Funding** a maintenant sa ligne au diagnostic (« 💰 FUNDING »), qui indique
pourquoi il n'entre pas (taux pas assez extrême, flux contraire, range…).

## 4quinvicies. Analyse statistique des stop loss (v4.290)

Après chaque SL (Spot-Accum, Accumulation, Funding, Manuel), le bot suit le
prix **minute par minute pendant 2 h** et enregistre :
- si un SL à 0,75 / 1 / 1,5 / 2 % aurait tenu (prix revenu à l'entrée d'abord)
  ou aurait été touché ;
- le **délai** avant que le prix revienne au niveau d'entrée ;
- le **pire recul** (depuis l'entrée) subi avant ce retour — la largeur de SL
  qui aurait permis de patienter ;
- le prix 2 h après, par rapport à l'entrée.

Onglet **Historique → 🛑 Analyse des stop loss** : synthèse par mode (part des
SL suivis d'un retour à l'entrée en 30 min / 1 h / 2 h, délai médian, reculs
médian / 75 % / 90 %, et pour chaque SL simulé le gain estimé en points de %),
**téléchargeable en CSV**. Le détail trade par trade est dans l'export « Suivi »
(4 colonnes « après SL » supplémentaires). Les SL des 16 derniers jours sont
complétés rétroactivement.

## 4sexvicies. Synchronisation bot ↔ Hyperliquid (v4.292)

Toutes les 2 min (fil séparé), le bot compare ses positions **live** (tous
modes, trading manuel compris) avec celles réellement tenues sur Hyperliquid
(DEX natif et xyz), actif par actif :

| Écart | Correction automatique (après 2 contrôles consécutifs) |
|---|---|
| Position **fantôme** (bot : ouverte / Hyperliquid : aucune) | clôturée côté bot au prix réel de sortie |
| Position **orpheline** (Hyperliquid : ouverte / bot : aucune) | reprise par le bot (identification par cloid) |
| **Taille** ou **prix d'entrée** différents | alignés sur Hyperliquid |
| **SL natif** absent | reposé immédiatement |
| **Sens opposé** | jamais corrigé : alerte, vérification manuelle |

Carte « 🔄 Synchronisation bot ↔ Hyperliquid » en haut de l'onglet de
trading (bouton « Vérifier maintenant »), avec la position des deux côtés, le
prix de liquidation, le PnL latent et la valeur du compte. Chaque écart est
aussi journalisé. Réglages : `LIVE_SYNC_INTERVAL_SEC`, `LIVE_SYNC_AUTO_FIX`.

## 4septemvicies. Moteur « top-down » multi-unités de temps (v4.299)

Spot-Accum (achats) et Accumulation (ventes) utilisent désormais l'approche
**top-down** sur les vraies bougies Hyperliquid (`mtf_analysis.py`) :

1. **Unité majeure (H4 par défaut, ou Daily)** : tendance de fond (clôture /
   EMA50 / EMA200) et **zones clés** (sommets et creux de marché regroupés ;
   un ancien plafond cassé devient un plancher). Spot-Accum n'achète qu'en
   tendance haussière, Accumulation ne vend qu'en tendance baissière.
2. **Attente** que le prix entre dans une zone de support (achat) ou de
   résistance (vente), avec une marge de 0,25 ATR.
3. **Unité inférieure (M15 par défaut, ou H1)** : signal de bougie de
   retournement sur la dernière bougie clôturée — avalement, marteau / étoile
   filante, étoile du matin / du soir — dont la mèche a touché la zone.
4. **Plan de trade** : SL au-delà de la zone et du motif (+ 0,15 ATR), posé
   aussi en ordre natif ; objectif = zone opposée ; rapport gain/risque ≥ 1,5.
   **Taille calculée sur le risque** : perte au SL = 0,5 % du capital
   (notionnel plafonné à 30 $), levier 1.
5. **Gestion** : SL remonté au prix d'entrée à +1R, puis SL suiveur à 1R du
   meilleur prix au-delà de +2R ; sortie à l'objectif.

Le diagnostic affiche pour chaque actif la ligne « 📊 TOP-DOWN » (tendance,
zones, position du prix). Tous les paramètres sont réglables (réglages
avancés « Top-down »). Moteur précédent : `ENTRY_ENGINE_MTF` = 0.

## 5. Premier lancement

Va sur l'URL Railway, clique "Créer un compte" et crée **ton unique compte**.
**L'inscription se ferme automatiquement dès qu'un compte existe** — personne
d'autre ne pourra créer de compte même si l'URL fuite. Si tu dois un jour
recréer un compte, connecte-toi à la base et vide la table `users`.

## 6. Ce qui a changé par rapport au bot Tkinter

- **Aucune fenêtre graphique** : tout se pilote depuis `index.html`, servi
  directement par l'API sur `/`.
- **Persistance réelle** : utilisateurs, trades, réglages custom vivent dans
  `hyperbot.db` (SQLite) sur le volume Railway.
- **Un seul `BotEngine`** créé au démarrage du serveur (pas recréé à chaque
  clic DÉMARRER comme dans la version Tkinter) — le PnL cumulé de session
  (`total_pnl`) survit donc à tous les démarrages/arrêts tant que le
  **process** ne redémarre pas (redeploy Railway = nouveau process).

## 7. Correspondances et simplifications assumées

L'interface `index.html` a été conçue pour un bot plus riche que le nôtre.
Voici comment chaque champ est réellement branché :

| Champ interface | Branché sur | Note |
|---|---|---|
| `position_pct` | `POSITION_SIZE_PCT` | ✅ direct |
| `max_loss_usd` | `SL_PCT_OF_E` | ⚠️ converti en % de la taille de position (E) au moment de l'enregistrement |
| `quick_profit_usd` | `TTP_ARM1_PRICE_PCT` | ⚠️ converti en % de mouvement de prix au moment de l'enregistrement |
| `max_open_trades` | `MAX_OPEN_TRADES` (nouveau garde-fou ajouté) | ✅ |
| `filter_hours` | `CRYPTO_OFFPEAK_ENABLED` (heures creuses 2h-6h UTC) | ✅ correspond à une vraie fonctionnalité |
| `filter_weekend` | `FOREX_SYMBOLS` (fermeture Forex sur PAXG) | ✅ correspond à une vraie fonctionnalité |
| `filter_macro` | `CPI_BLACKOUT_ENABLED` (blackout CPI Finnhub) | ✅ correspond à une vraie fonctionnalité |
| `active_coins` | `ACTIVE_COINS` | ✅ tous les actifs de `SYMBOLS` (cryptos et forex HIP-3) |
| Signal `take_profit1` / `take_profit2` | Prix équivalents calculés depuis les seuils $ (Quick Profit / Trailing) | ⚠️ nôtre bot ne raisonne pas en % fixe — conversion informative au moment de l'entrée, pas un vrai ordre TP1/TP2 |
| `leverage` par signal | `CONFIG["LEVERAGE"]` | ⚠️ toujours le même (pas de levier variable par trade) |
| `risk_reward` | `QUICK_PROFIT_ARM_USD / MAX_LOSS_USD` | ⚠️ ratio informatif, pas un vrai calcul de risk/reward par trade |
| Toggle "IA continue" | Stocké, **sans effet** | ❌ pas d'équivalent — ce bot n'a pas de moteur de génération de signaux IA continu |
| Login email/mot de passe | Table `users` réelle (PBKDF2 + JWT) | ✅ mais inscription limitée à un seul compte (voir section 5) |

## 8. Sécurité

- `HYPERBOT_SECRET_KEY` est obligatoire : sans elle (ou si elle fait moins de
  32 caractères) le serveur ne démarre pas. Un token n'est accepté que si le
  compte existe toujours en base.
- Préfère renseigner `HYPERBOT_PRIVATE_KEY` en variable d'environnement
  Railway plutôt que via le formulaire de l'interface (`/api/config/hyperliquid`)
  — ce formulaire écrit la clé dans le fichier SQLite du volume, ce qui est
  acceptable si le volume est privé, mais une variable d'environnement reste
  la manière la plus sûre de gérer un secret.
- Le changement de wallet/clé API ne prend effet qu'au **prochain démarrage**
  du bot (arrêt puis démarrage) — reconnecter l'exchange à chaud n'est pas
  géré.

## v4.362 — Swing Support (crypto, paper)

Sous-mode PAPER, éteint par défaut (`SUPSW_ENABLED`). Actifs : BTC, ETH, HYPE, TAO, SUI (`SUPSW_SYMBOLS`).
- **Entrée** : tendance Daily haussière + prix dans un support Daily + (retest du support tenu sur les `SUPSW_WINDOW` dernières bougies 1 h, ou balayage de liquidité puis reprise), dans les `SUPSW_SIGNAL_MAX_AGE_SEC` qui suivent la clôture de la bougie 1 h.
- **Sortie** : objectif `SUPSW_TP_PCT` (2 %), stop de sécurité `SUPSW_SL_PCT` (5 %), durée max, et **sortie anticipée** si une bougie 1 h clôture sous (bas de zone − tampon) ET flux < `SUPSW_EXIT_FLOW`. `SUPSW_EXIT_REQUIRE_H4_END=1` exige en plus la tendance H4 finie. Aucun breakeven.
- **Contexte enregistré sans filtrer** : tendance de BTC, tendance Weekly, funding annualisé, flux.
- **Journal** (`supsw_events`) : chaque signal (pris ou refusé) mesuré à 24 h / 72 h ; chaque sortie anticipée suivie à 1 h / 4 h / 24 h, avec « l'objectif ou le stop aurait-il été touché ensuite ? » (`too_early`).
- **API** : `/api/supsw/status`, `/api/supsw/stats?days=`, `/api/supsw/journal/export.csv`. **UI** : carte « Swing Support » dans TRADES P/L › Swing ; ligne « 🧱 SWING SUPPORT » dans le diagnostic.
- Fichiers : `swing_support.py` (nouveau), `bot_engine.py`, `db.py`, `api.py`, `index.html`.
- Le trade occupe le slot de l'actif tant qu'il est ouvert (les autres modes ne peuvent pas trader cet actif).

### v4.363 — Swing Support : niveau de sortie anticipée
Constat (HYPE, TAO) : sur une zone Daily large, le bas de zone est sous le stop à 5 % → la sortie anticipée était inatteignable. Désormais (`SUPSW_EXIT_LEVEL=1`, défaut) le niveau est le **plus bas du retest** (plus bas des bougies 1 h du signal), borné entre `SUPSW_EXIT_MIN_PCT` (0,5 %) et `SUPSW_EXIT_MAX_PCT` (3 %) sous l'entrée. `SUPSW_EXIT_LEVEL=0` rétablit le bas de zone. Le niveau est stocké sur la position et dans le journal (colonne `inval`) ; les positions déjà ouvertes gardent le bas de zone. Migration automatique de la table `supsw_events`.

## v4.365 — Tendance confirmée (module à part, paper)

Nouveau fichier : `trend_follow.py` (à déposer à côté de `bot_engine.py`). Fichiers modifiés : `bot_engine.py`, `db.py`, `api.py`, `index.html`.

- **Entrée** dès que le Daily ET le H4 vont dans le même sens (LONG ou SHORT), avec momentum 1 h (clôture du bon côté de sa moyenne 20) et flux de transactions dans le sens. Option « cassure » (`TRENDF_ENTRY_MODE=1`).
- **On reste tant que la tendance tient** : aucun objectif ; stop suiveur = 3 × ATR H4 (borné 0,8–6 %) ; sortie si le Daily n'est plus dans le sens (bougie clôturée), si le H4 se retourne (`TRENDF_EXIT_H4_REVERSE`), au stop, ou après 30 jours.
- **Module à part** (stratégie `trend_follow`, code 09, sous-onglet « 📈 Tendance »), paper uniquement, désactivé par défaut (`TRENDF_ENABLED`). Un actif = une seule position, tous modes confondus.
- **Traçabilité** : table `trendf_events` ; chaque entrée (prise ou refusée) est mesurée à 24 h / 72 h, avec son **retard** (distance à la moyenne Daily en ATR) ; chaque sortie est suivie 1 h / 4 h / 24 h. Statistiques `/api/trend/stats`, export `/api/trend/journal/export.csv`.
- Actifs par défaut : BTC, ETH, HYPE, TAO, SUI, PAXG, xyz:EUR, xyz:JPY (`TRENDF_SYMBOLS`).

### v4.366 — Swing Support : stop et objectif selon le levier (optionnel)

Réglage `SUPSW_LEVER_SCALE` (0 par défaut = comportement inchangé). À 1 : stop (en % du prix) = `SUPSW_MARGIN_RISK_PCT` (7,5 % de la marge) / levier, plafonné à `SUPSW_SL_PCT` et plancher `SUPSW_MIN_SL_PCT` (1,5 %) ; objectif : `SUPSW_TP_LEV1` / `LEV2` / `LEV3` = 2 / 1,5 / 1 % pour un levier ×1 / ×2 / ×3 et plus. Le levier se règle avec `SUPSW_LEVERAGE` (1 à 10). La taille reste dimensionnée par le risque (≈ 1 % du capital au stop) ; le niveau de sortie anticipée est toujours maintenu au-dessus du stop. Les positions de la carte affichent « prix X % · marge Y % ».

### v4.367 — Trades fermés Swing Support / Tendance : sens et pic

Chaque ligne de « Trades fermés » indique maintenant LONG / SHORT et le pic atteint : « pic prix +X % · marge +Y % · +Z $ » (ou « jamais en gain »).

### v4.368 — Badge de stratégie sur les trades ouverts

La liste « Trades ouverts » affiche maintenant la stratégie de chaque position : 🧱 SWING SUPPORT, 📈 TENDANCE, 🌊 SWING FX, ⚡ SCALP FX, 🧭 TOP-DOWN (moteur principal), en plus de ACCUMULATION et FUNDING.

### v4.369 — Libellés des réglages Swing Support

Les réglages avancés Swing Support portent maintenant un groupe explicite après « Swing Support · » : ACTIVATION, STOP FIXE, OBJECTIF FIXE, TAILLE, LIMITE, SIGNAL, DURÉE, SORTIE ANTICIPÉE, LEVIER, STOP ET OBJECTIF SELON LE LEVIER, SELON LE LEVIER. Aucun changement de comportement.

### v4.370 — Swing Support : alertes de dégradation (mesure seule)

Aucune sortie ne change. Chaque minute, pour chaque position Swing Support ouverte, le bot compte 7 signes de dégradation : flux vendeur, support cassé en clôture 1 h, H4 baissier, BTC baissier, sous le prix d'entrée depuis 2 h, aucun gain ≥ 0,3 % après 3 h, prix à mi-chemin du stop. Le maximum atteint et le compte à la sortie sont enregistrés dans `supsw_events` (colonnes `alerts_max`, `alerts_exit`, `alerts_json` avec l'historique), migration automatique. La carte Swing Support compare gagnants et perdants (`/api/supsw/stats`, bloc `alerts`) ; l'export CSV contient les nouvelles colonnes. Les positions déjà ouvertes commencent à être mesurées dès le déploiement. Un seuil de sortie ne sera proposé que si les perdants montent nettement plus haut que les gagnants (environ 20 trades fermés).

## v4.371 — Scalp Forex : deux sorties anticipées

Fichiers modifiés : `scalp_forex.py`, `bot_engine.py` (valeurs par défaut), `api.py` (réglages et validation), `README.md`. Actifs par défaut (1) ; chaque règle s'éteint par un réglage à 0.

- **SANS PROGRES** (`FOREX_SCALP_STALE_*`) : après 10 min (`STALE_MIN`), si le pic de gain est resté sous 0,05 % du prix (`STALE_PEAK_PCT`), le trade sort au lieu d'attendre les 20 min du temps max. Motif affiché : « SCALP SANS PROGRES ».
- **ECHEC PRECOCE** (`FOREX_SCALP_EARLYFAIL_*`) : un trade qui n'a jamais dépassé 0,03 % de gain (`EARLYFAIL_PEAK_PCT`) et a perdu la moitié de son stop (`EARLYFAIL_RISK_FRAC`) sort sans attendre le stop complet (au plus tôt 1 min après l'entrée, jamais après le breakeven). Motif : « SCALP ECHEC PRECOCE ».
- Constat à l'origine (30 trades, 04-09/10) : 6 stops simples jamais en gain (−0,247 $), 12 « temps max » à −0,159 $. Les sorties « flux contraire » (8/8 gagnantes) et le breakeven ne sont pas touchés. Les positions déjà ouvertes appliquent les nouvelles règles dès le redémarrage.

## v4.372 — Smartphone : application installable, mise en page mobile, chargement plus rapide

Fichiers : `api.py`, `index.html` (généré), `index_src.html` (source), `README.md`.

- **Installation sur l'écran d'accueil** : iPhone (Safari → Partager → « Sur l'écran d'accueil ») et Android (Chrome → menu → « Installer l'application »). L'application s'ouvre en plein écran avec son icône. Les icônes, le manifeste et le service worker sont servis par `api.py` (`/manifest.webmanifest`, `/sw.js`, `/icon-192.png`, `/icon-512.png`, `/apple-touch-icon.png`) : aucun fichier image à déposer. Le service worker ne met rien en cache : c'est toujours la dernière version en ligne.
- **Mise en page petit écran** (≤ 640 px) : grilles de cartes à 2 colonnes (1 colonne pour les paires), champs à 16 px (plus de zoom automatique sur iPhone), boutons plus hauts, marges de sécurité (encoche et barre du bas), plus de défilement horizontal de la page, en-tête et horloge qui passent à la ligne.
- **Chargement plus rapide** : `index.html` est maintenant **précompilé** (le navigateur n'a plus à charger ni exécuter Babel, environ 3 Mo, à chaque ouverture). La source reste dans `index_src.html` : c'est elle qui sert aux futures modifications, et `index.html` en est regénéré. **Les deux fichiers sont à déposer dans le dépôt**, mais seul `index.html` est servi.
- Les notifications ne sont pas incluses (iOS ne les permet qu'à l'application installée et via un service d'envoi côté serveur) ; à reprendre plus tard.

## v4.373 — FIX CRITIQUE : positions perdues au redéploiement

Fichiers : `bot_engine.py`, `db.py`, `README.md`.

- **Cause** (confirmée par les logs Railway du 10/10 à 13:40 UTC) : la sauvegarde automatique des positions (toutes les 5 s) démarrait **avant** la reprise des positions. Quand le démarrage dépassait 5 s, elle écrasait `hyperbot_positions.json` avec 0 position ; la reprise relisait alors un fichier vide (« Chargement OK : 0 position »). Les trades Paper (Swing Support, Spot-Accum…) disparaissaient de la mémoire du bot ; leurs lignes restaient « ouvertes » en base.
- **Correctif** : aucune écriture du fichier tant que la reprise n'est pas terminée ; copie de sécurité `hyperbot_positions.json.bak` avant chaque reprise ; verrou et fichier temporaire unique par fil (corrige aussi l'erreur « No such file … .tmp »).
- **Rattrapage** : au démarrage, les trades **Paper Swing Support et Spot-Accum** encore « ouverts » en base depuis moins de 4 jours et absents du fichier sont **reconstruits** avec leurs niveaux d'origine (entrée, stop, objectif, taille, levier). Le suivi du pic et l'armement repartent de zéro ; la sortie anticipée de Swing Support garde son niveau d'invalidation. Un emplacement déjà occupé n'est jamais touché ; les vieilles lignes « ouvertes » (plus de 4 jours) sont ignorées. Message dans les logs : « RECONSTRUITE depuis la base ».

## v4.374 — Funding : plafond d'entrée, actifs exclus, sortie « sans progrès »

Fichiers : `bot_engine.py`, `api.py`, `README.md`. Les trois règles sont actives par défaut ; chacune a un réglage (groupes « Funding · PLAFOND D'ENTREE », « ACTIFS EXCLUS », « SANS PROGRES »).

Constat (346 trades Funding fermés, 24/09 → 10/10) : depuis le 28/09 +0,54 $ net (68 % de réussite), mais gains moyens +0,04 $ pour des pertes moyennes −0,077 $ et frais = 72 % du gain brut.

- **Plafond d'entrée** `FUNDING_MAX_ANNUAL_PCT` (40 %, 0 = sans plafond) : pas d'entrée quand |funding annualisé| dépasse ce niveau. Tranche 40–60 % : −0,55 $ sur 34 trades (depuis le 28/09), −1,09 $ sur 86 trades au total. L'entrée refusée est suivie en lecture seule (« signaux bloqués »).
- **Actifs exclus** `FUNDING_EXCLUDE_WEAK_COINS` (1) : plus d'entrée Funding sur GMX et RENDER (−0,40 $ / 18 trades et −0,21 $ / 6 depuis le 28/09). Liste `FUNDING_WEAK_COINS` dans le code.
- **Sans progrès** `FUNDING_STALE_ENABLED` (1), `FUNDING_STALE_MIN` (20 min), `FUNDING_STALE_PEAK_PCT` (0,15 %) : un trade dont le pic est resté sous 0,15 % après 20 min sort (motif « FUNDING SANS PROGRES ») au lieu de continuer vers le stop. Sur l'historique : 22 trades dans ce cas, 21 finissaient au stop (−1,41 $).

## v4.375 — Actifs de Swing Support réservés

Fichiers : `bot_engine.py`, `api.py`, `README.md`. Réglage `SUPSW_RESERVE_ASSETS` (1 par défaut, 0 = ancien comportement), dans la carte Swing Support.

- **Constat** : BTC, ETH, HYPE, TAO et SUI (liste `SUPSW_SYMBOLS`) sont souvent occupés par Swing Support ; Tendance, Accumulation et Spot-Accum y étaient bloqués (« actif déjà ouvert dans un autre mode »).
- **Changement** : ces trois modes **n'évaluent plus** les actifs de `SUPSW_SYMBOLS` (diagnostic : « actif réservé à Swing Support »). Ils gardent tous les autres actifs. Funding, Scalp et Forex ne changent pas.
- Sans effet si Swing Support est éteint. Les positions déjà ouvertes continuent d'être gérées normalement.

## v4.376 — Alertes sur téléphone (notifications push)

Fichiers : **nouveau `push_alerts.py`**, `api.py`, `manual_trading.py`, `index.html`, `index_src.html`, `README.md`.

- **À vérifier avant le déploiement** : le fichier `requirements.txt` doit contenir la ligne `cryptography` (seule dépendance utilisée, aucun service externe). Sans elle, le bot démarre normalement et le panneau affiche « Alertes indisponibles ».
- **Activation** : onglet **Trading Manuel**, panneau « 🔔 ALERTES TÉLÉPHONE » → *Activer les alertes sur cet appareil* (un toucher est obligatoire sur iPhone). Sur iPhone : d'abord installer l'app (Safari > Partager > Sur l'écran d'accueil), l'ouvrir depuis l'icône, puis activer. iOS 16.4 ou plus.
- **Événements** (chacun activable) : trade ouvert · trade fermé avec son résultat · perte du jour au-delà d'un seuil (2 $ par défaut, tous modes, journée locale America/Marigot, une seule alerte par jour) · connexion Hyperliquid perdue depuis plus de 3 min (puis « rétablie ») · **opportunités du bot** (désactivé par défaut ; confiance minimale 70 %, une même opportunité n'est annoncée qu'une fois par 30 min).
- Les alertes partent du serveur Railway : elles arrivent app fermée et téléphone verrouillé. Maximum 30 alertes par 10 minutes.
- Les clés et les appareils abonnés sont dans `/data/hyperbot_push.json` (survivent aux redéploiements). Si iOS invalide un abonnement, le panneau permet de se réabonner.
- Service worker : ajout de l'affichage des notifications et de l'ouverture de l'app au toucher.

## v4.377 — Alertes d'opportunités : choix du type

Fichiers : `push_alerts.py`, `bot_engine.py`, `index.html`, `index_src.html`, `README.md`.

- Dans le panneau « 🔔 ALERTES TÉLÉPHONE » (Trading Manuel), la case **Opportunités du bot** affiche maintenant des boutons pour choisir les types annoncés : **Swing Support**, Accumulation, Spot-Accum, Funding, Forex. Par défaut : les quatre types déjà existants (Swing Support à cocher).
- **Swing Support** : alerte à chaque setup valide (retest tenu ou balayage de liquidité), qu'il soit ouvert par le bot ou non (dans ce cas le motif est indiqué : nombre maximal atteint, pause après perte, arrêt…). Les signaux 1 h trop anciens et les spreads trop larges n'envoient rien. Pas de seuil de confiance pour ce type (le seuil ne s'applique qu'aux autres).
- Tendance, Swing Forex et Scalp Forex n'ont pas d'alerte d'opportunité pour l'instant.

## v4.378 — Alertes Swing Support : tous les actifs et note de qualité réglable

Fichiers : `bot_engine.py`, `push_alerts.py`, `swing_support.py`, `index.html`, `index_src.html`, `README.md`.

- **Tous les actifs crypto** : avec l'alerte « Swing Support » cochée (Trading Manuel > Alertes téléphone > Opportunités), le bot cherche aussi des setups Swing Support (tendance Daily haussière, prix dans un support Daily, retest tenu ou balayage de liquidité) sur **les autres actifs crypto** de la liste `SYMBOLS`, 6 actifs par passage (toutes les 30 s). Pour ces actifs : **alerte seulement**, jamais d'ouverture, rien dans le journal ni dans la carte Swing Support. Les 5 actifs du mode restent traités comme avant (pris par le bot ou non, avec le motif).
- **Note de qualité 0-100** (réglage « Note de qualité minimale », 60 par défaut ; 40 = souple, 60 = normal, 80 = strict) : Daily 20 (repli en tendance : 10) · Weekly 15 · BTC 15 · H4 haussier 10 · contacts de la zone 15 · flux acheteur 15 · funding 10. Un élément inconnu (flux, funding) vaut une note partielle. La note et le détail figurent dans l'alerte. **Elle ne sert qu'à filtrer les alertes** (aucun trade n'en dépend) et n'est pas calibrée sur des résultats : elle sera à ajuster à l'usage.
- La confiance minimale existante (70 % par défaut) continue de s'appliquer à Accumulation, Spot-Accum, Funding et Forex.

## v4.379 — Historique des alertes

Fichiers : `push_alerts.py`, `api.py`, `index.html`, `index_src.html`, `README.md`.

- Sur iPhone, **toucher une notification la retire du centre de notifications** (comportement d'iOS, non modifiable par l'application). Les 100 dernières alertes sont donc conservées côté serveur (`/data/hyperbot_push.json`) et consultables dans Trading Manuel > Alertes téléphone > « Voir l'historique des alertes » (titre, détail, heure locale).
- Route `GET /api/push/history`. L'historique se remplit même si aucun appareil n'est abonné.

## v4.380 — Historique des alertes : sélection et suppression

Fichiers : `push_alerts.py`, `api.py`, `index.html`, `index_src.html`, `README.md`.

- Dans l'historique des alertes (Trading Manuel > Alertes téléphone), chaque alerte a une case : **Tout sélectionner**, **Effacer la sélection (n)** et **Tout effacer** (avec confirmation).
- Route `POST /api/push/history/delete` (`ids` ou `everything`). Les anciennes alertes reçoivent un identifiant au chargement.

## v4.381 — Préparer un trade depuis une alerte d'opportunité

Fichiers : `manual_trading.py`, `push_alerts.py`, `api.py`, `index.html`, `index_src.html`, `README.md`.

- Dans l'historique des alertes, chaque alerte d'opportunité a un bouton **🧾 Préparer le trade** : le ticket de Trading Manuel s'ouvre (et l'écran défile jusqu'à lui). Rien n'est envoyé tant que tu ne valides pas ; paper ou live au choix, comme avant.
- **Opportunité encore valable** (Accumulation, Spot-Accum, Funding, Forex) : ticket de son mode, comme depuis la liste d'opportunités. **Opportunité expirée** : ticket libre (taille, stop et objectif par défaut), avec un message.
- **Swing Support** (nouveau type de ticket, n'importe quel actif crypto, LONG uniquement) : stop, objectif, levier et taille du mode Swing Support (selon le levier si activé), sans opportunité du bot à confirmer, pas de trailing par défaut. La sortie anticipée du mode ne s'applique pas aux trades manuels.
- **Toucher la notification** d'une opportunité ouvre l'application sur l'onglet Trading Manuel (application fermée ou déjà ouverte).

## v4.382 — Pas d'opportunité sur un actif déjà occupé

Fichiers : `manual_trading.py`, `bot_engine.py`, `api.py`, `index.html`, `index_src.html`, `README.md`.

- Une opportunité sur un actif où une **position est déjà ouverte** (n'importe quel mode du bot, paper ou live, ou un trade manuel) n'est plus : proposée dans la liste de Trading Manuel, annoncée par alerte (Accumulation, Spot-Accum, Funding, Forex et Swing Support), ni préparable en ticket. Si une position s'ouvre pendant qu'une opportunité est affichée, elle disparaît de la liste.
- Dans l'historique des alertes, le bouton « Préparer le trade » est remplacé par « Position déjà ouverte sur … » quand l'actif est occupé.

## v4.383 — Trades manuels ouverts dans « Tous »

Fichiers : `api.py`, `index.html`, `index_src.html`, `README.md`.

- Les trades ouverts en **Manuel** apparaissent dans Trades P/L > **Tous**, avec les autres trades ouverts, taggés « ✋ MANUEL » (entrée, prix actuel, PnL, SL/TP, bouton Fermer). Ils sont comptés dans la tuile TRADES OUVERTS.
- Ils restent affichés dans l'onglet de leur mode (shorts → Accumulation, longs → Spot-Accum), dans le bloc « TRADES MANUELS OUVERTS » existant depuis la v4.358, sans doublon.
- Ils ne sont **pas inclus** dans le solde ni dans le PnL ouvert du bot.
- API : `paper_portfolio` renvoie en plus `manual_open`.

## v4.384 — Performance : Swing Support et Tendance

Fichiers : `index.html`, `index_src.html`, `README.md` (aucun changement serveur).

- Trades P/L > **Performance** affiche désormais aussi les cartes **🧱 Swing Support** et **📈 Tendance** (ouverts/total, win rate, net, frais), en plus de Forex, Scalp, Accumulation, Funding et Spot-Accum. Même calcul que les autres modes (net après frais si l'option est active, depuis le point de réinitialisation des statistiques).

## v4.385 — Alerte Swing Support : sens indiqué

Fichiers : `bot_engine.py`, `README.md`.

- Le titre de l'alerte Swing Support indique maintenant le sens : « 🧱 Swing Support · 📈 LONG (achat) ETH · note 75/100 ». Le mode est LONG uniquement (achat sur support).
- Le statut en fin de message est plus clair : « le bot n'a pas ouvert ce trade (nombre maximal de swings support simultanes atteint) ».

## v4.386 — Alertes horodatées et fraîcheur des opportunités

Fichiers : `push_alerts.py`, `bot_engine.py`, `api.py`, `index.html`, `index_src.html`, `README.md`.

- La notification d'une opportunité (y compris Swing Support) commence par l'heure locale : « 🕐 18:06 · … ».
- Dans l'historique des alertes : âge en couleur (vert ≤ 15 min, orange ≤ 1 h, rouge au-delà avec « probablement dépassée »), et « prix à l'alerte X → maintenant Y · +n % dans ton sens / contre toi » (sens LONG/SHORT pris en compte).
- Le prix à l'alerte est conservé avec chaque alerte ; les anciennes alertes n'affichent que l'âge. `GET /api/push/history` renvoie `now_price`.

## v4.387 — Journal Swing Support : contexte de marché (BTC 24 h, H4)

Fichiers : `bot_engine.py`, `db.py`, `api.py`, `README.md`.

- Trois colonnes sont ajoutées au journal des signaux Swing Support et à son export CSV : `btc_chg24` (variation de BTC sur 24 h, en %), `btc_h4` (tendance H4 de BTC) et `h4_trend` (tendance H4 de l'actif). Elles sont enregistrées seulement : aucune décision du bot n'en dépend.
- Les colonnes sont créées automatiquement au démarrage dans la base existante ; les anciens signaux restent vides pour ces colonnes.
- Objectif : tester dans quelques semaines un filtre « ne pas entrer quand BTC chute » sur des données réelles.

## v4.388 — Forex : suivi du creux de PnL pendant le trade

Fichiers : `bot_engine.py`, `README.md`.

- Les trades du mode Forex enregistrent désormais, comme Funding, le chemin de leur PnL (lecture seule, aucun effet sur une décision) : creux atteint pendant le trade, creux après l'armement du trailing, pic au moment de ce creux et délai jusqu'à l'armement. Ces valeurs apparaissent dans l'export « Suivi … Forex » (colonnes « PnL minimal pendant le trade % », « PnL minimal APRES avoir touche +0,5 % », « pic atteint au moment de ce minimum % », « delai jusqu a +0,5 % (s) »).
- **Attention au libellé** : pour le Forex, le seuil de « contact » est l'armement du trailing (`FOREX_TTP_ARM_PCT`, 0,15 %), pas +0,5 % comme l'indique l'en-tête de colonne (conservé pour Funding).
- « suivi complet du trade » = oui seulement si le trade a été ouvert après le déploiement ; un trade déjà ouvert garde un creux partiel.
- Objectif : tester dans quelques semaines une sortie « sans progrès » (stop de plafond de −0,5 % touché par des trades qui n'ont jamais bougé dans le bon sens).

## v4.389 — Swing Support : laisser courir après l'objectif + réentrée après un gain

Fichiers : `bot_engine.py`, `db.py`, `api.py`, `index.html`, `index_src.html`, `README.md`.

- **Laisser courir** (`SUPSW_TRAIL_ON`, 1 par défaut) : à l'objectif (+1 %), le trade n'est plus fermé. Le stop remonte aussitôt à +0,5 % au-dessus de l'entrée (`SUPSW_TRAIL_LOCK_PCT`, gain verrouillé), puis suit le plus haut atteint à 0,5 % en dessous (`SUPSW_TRAIL_PCT`). Sortie : « SWING SUPPORT TRAILING OBJECTIF ». La durée max (5 j) reste active ; la sortie anticipée « CASSE (flux) » est suspendue une fois le gain verrouillé. `SUPSW_TRAIL_ON = 0` rétablit l'objectif fixe. Redémarrage : le niveau du stop suiveur est sauvegardé (au plus une fois par minute) ; une position reconstruite au démarrage repart du stop d'origine et se ré-arme si le prix est au-dessus de l'objectif.
- **Réentrée après un gain** (`SUPSW_REENTRY_ON`, 1 par défaut) : pendant 12 h (`SUPSW_REENTRY_HOURS`) après une sortie gagnante sur un actif, un nouveau signal n'est accepté que si le prix est au moins à (prix de sortie − 0,2 %) (`SUPSW_REENTRY_TOL_PCT`), donc « ça continue de monter ». Si le prix a baissé, le bot attend la fin de la baisse : point bas fait depuis au moins 2 bougies 1 h et dernière bougie 1 h verte. Le refus apparaît dans le journal (« baisse apres un gain … ») et la carte du mode.
- **Journal / export** : deux colonnes ajoutées (`peak_pct` = plus haut atteint pendant le trade en %, `trail_armed`), pour comparer chaque trade au gain qu'aurait donné l'objectif fixe. Les colonnes sont créées automatiquement au démarrage.
- Les 6 réglages sont modifiables dans Réglages ; le texte du mode (Trading > Swing Support) indique le trailing et la réentrée.
- Limite : l'export historique ne contient pas le chemin du prix, l'effet du trailing se mesure donc sur les trades à venir.

## v4.390 — Trades manuels ouverts : pic atteint et modification du TP

Fichiers : `api.py`, `index.html`, `index_src.html`, `README.md`.

- La carte d'un trade manuel ouvert (onglet « Tous » et volet du mode) affiche maintenant le **pic** : « pic +2,50 % (prix · gain en $) », c'est-à-dire le meilleur gain atteint depuis l'ouverture, en % du prix, en prix et en dollars.
- Bouton **« Modifier le TP »** sur la même carte : saisie du nouveau prix puis Enregistrer ; champ vide = retirer le TP. Le serveur refuse un TP du mauvais côté du prix actuel (message affiché). Le TP est géré par le bot (il n'est pas posé sur Hyperliquid), donc la modification est immédiate en paper comme en live.
- SL, trailing, verrou et règle de tendance restent modifiables dans l'onglet Trading Manuel (inchangé).
- `GET /api/paper/portfolio` : `manual_open` renvoie aussi `peak_pct`, `qty`, `armed`. Aucun changement de moteur.

## v4.391 — Swing Support : entrée seulement loin de la résistance, objectif = la résistance

Fichiers : `bot_engine.py`, `db.py`, `api.py`, `index.html`, `index_src.html`, `README.md`.

- **Distance à la résistance** : pour chaque signal, le bot calcule la prochaine résistance Daily AU-DESSUS de la zone de support et du prix (mêmes zones que le reste du bot) et enregistre `res_low` (niveau) et `res_dist_pct` (distance en % du prix) dans le journal et dans l'export CSV, pour TOUS les signaux, pris ou refusés. Les colonnes sont créées au démarrage. Sans résistance au-dessus, l'entrée n'est pas limitée.
- **Filtre d'entrée** (`SUPSW_RES_FILTER_ON`, 1 par défaut) : entrée refusée si la résistance est à moins de `SUPSW_RES_MIN_PCT` (3 %) au-dessus du prix : « resistance Daily trop proche … retournement probable ». Les signaux refusés sont quand même mesurés à 24 h / 72 h : après quelques jours on pourra voir, avec les colonnes MFE/MAE de ces lignes, s'ils auraient perdu (et donc si un SHORT à la résistance aurait gagné).
- **Viser la résistance** (`SUPSW_RIDE_ON`, 1 par défaut) : l'objectif devient (résistance − 0,3 %), plafonné à 6 % (`SUPSW_RIDE_MAX_PCT`), seulement s'il est plus loin que l'objectif fixe de 1 %. À +1 % le gain est verrouillé (stop à +0,5 %, comme en v4.389), puis le stop suit le plus haut à 1 % (`SUPSW_RIDE_TRAIL_PCT`, large pour ne pas être sorti par le bruit) jusqu'à la résistance. Une fois la résistance atteinte, le stop se resserre à 0,5 % sous le plus haut (`SUPSW_TRAIL_PCT`). Sortie : « SWING SUPPORT TRAILING OBJECTIF ».
- Après un redémarrage, une position reconstruite repart avec l'objectif = résistance comme seuil de verrou (le verrou à +1 % n'est pas rejoué).
- **SHORT à la résistance** : NON implémenté. Il est mesuré d'abord via les signaux refusés (voir ci-dessus).
- Limite : ces règles ne peuvent pas être testées sur l'historique (les exports n'ont pas de bougies) ; elles sont mesurées sur les prochains signaux. Si peu d'entrées passent le filtre, baisser `SUPSW_RES_MIN_PCT`.

## v4.392 — Swing Support : porte BTC et contrôle des dernières heures

Fichiers : `bot_engine.py`, `db.py`, `api.py`, `README.md`.

- **Porte BTC** (`SUPSW_BTC_GATE_ON`, 1 par défaut) : entrée refusée si la variation de BTC sur 24 h est inférieure à `SUPSW_BTC24_MIN` (0 %) ou si la tendance H4 de BTC n'est pas conforme à `SUPSW_BTC_H4_MODE` (0 = ignorée, 1 = refus si baissière, **2 = haussière exigée**, valeur par défaut). Si la donnée manque, l'entrée n'est pas bloquée.
- **Dernières heures** (`SUPSW_RECENT_ON`, 1 par défaut) : entrée refusée pendant une chute en cours : l'actif a perdu plus de 1,5 % (`SUPSW_RECENT_ASSET_MIN_PCT`) ou BTC plus de 0,5 % (`SUPSW_RECENT_BTC_MIN_PCT`) sur les 4 dernières heures (`SUPSW_RECENT_HOURS`), mesuré sur les bougies 1 h clôturées.
- **Journal / export** : deux colonnes ajoutées, `chg_recent` (actif) et `btc_chg_recent` (BTC) sur la fenêtre, enregistrées pour tous les signaux (pris ou refusés) afin de calibrer les seuils. Les signaux refusés restent mesurés à 24 h / 72 h.
- Ordre des contrôles : pause après perte → BTC 24 h → H4 de BTC → chute actif → chute BTC → résistance proche (v4.391) → réentrée après gain (v4.389).
- Rejeu sur l'export du 14/10 (15 trades pris avec contexte BTC) : BTC 24 h ≥ 0 seul garde 7 trades (−7,5 % au lieu de −18,2 %) ; avec H4 haussière exigée, 4 trades (−6,2 %). Les seuils « chute en cours » ne peuvent pas être rejoués (colonne nouvelle). Ces deux sous-ensembles restent négatifs : l'échantillon est petit et issu d'une même phase de baisse.
- Critère d'arrêt proposé : environ 20 trades avec ces règles ; si le total net de frais n'est pas positif, Swing Support est abandonné. Si trop peu d'entrées passent, assouplir `SUPSW_BTC_H4_MODE` (2 → 1) en premier.

## v4.393 — Top-down (Spot-Accum, Accumulation, Swing) : verrou de gain en % du prix

Fichiers : `bot_engine.py`, `api.py`, `README.md`.

- **Constat** (cas UNI) : le verrou existant (v4.340) ne s'arme qu'à +1R. Avec un stop large (R de 3 à 4 % du prix), il n'armait qu'à +3 % de gain : les pics à +1,4 % et +1,9 % n'avaient aucune protection et ont fini en perte (« STRUCTURE CASSEE »), et le pic de +4,15 % n'a été rendu que pour moitié.
- **Nouveau verrou en %** (`MTF_PCT_LOCK_ON`, 1 par défaut) : dès que le meilleur gain atteint `MTF_PCT_LOCK_ARM_PCT` (1 % du prix), le SL monte pour garantir 60 % du pic (`MTF_PCT_LOCK_FRACTION`), 75 % au-delà d'un pic de 3 % (`MTF_PCT_LOCK_BIG_PCT` / `MTF_PCT_LOCK_BIG_FRACTION`), et jamais moins que +0,15 % de l'entrée (`MTF_PCT_LOCK_FLOOR_PCT`, frais couverts). Il ne fait que monter (long) / descendre (short), se combine avec le verrou en R et le suiveur existants (le plus favorable l'emporte), et marque la position « SL remonté » : la sortie s'appelle « STOP SUIVEUR (top-down) ». En live, le SL natif est mis à jour par le mécanisme existant.
- **Estimation sur l'export du 12/10** (82 trades top-down Spot-Accum / Accumulation, net actuel −12,6 $) : armement à 1 % et 60 % du pic ≈ +4 $ ; armement à 0,8 % ≈ +4,6 $ ; à 0,6 % ≈ +6,9 $. C'est une **borne haute** : l'export donne le pic et la sortie, pas le chemin ; un stop plus serré peut aussi couper des trades qui seraient repartis, et les frais ne sont pas retranchés.
- Tous les moteurs top-down sont concernés (pas seulement le profil Swing). Pour désactiver : `MTF_PCT_LOCK_ON = 0`.

## v4.394 — Affichage du PnL des trades manuels : « $ (%) » comme sur Hyperliquid

Fichiers : `index.html`, `index_src.html`, `README.md`.

- Dans les trades manuels ouverts (onglet Trading Manuel, onglet « Tous » et volet du mode), le PnL s'affiche désormais **d'abord en $, puis en % entre parenthèses**, comme sur Hyperliquid : « +0,351 $ (+2,06 %) ». Avant, l'onglet Trading Manuel affichait le % avant le $ et le volet du mode aussi.
- Le % est maintenant le **% de marge (ROE = mouvement de prix × levier)**, comme Hyperliquid et comme les trades du bot ; à levier ×1 il est identique au mouvement de prix. Le pic affiché suit la même règle : « 🏔️ pic +0,41 $ (+1,44 %) · prix … ».
- Les trades du bot et les historiques étaient déjà au format « $ (%) » : inchangés.

## v4.395 — Trades manuels : « laisser courir au TP » (trailing après l'objectif)

Fichiers : `manual_trading.py`, `api.py`, `index.html`, `index_src.html`, `README.md`.

- **Constat** : le trailing de la v4.389 ne concerne que le Swing Support du bot (paper). Un ticket manuel préparé depuis une alerte Swing Support n'avait qu'un TP fixe (sans TTP), qui fermait tout le trade à l'objectif.
- **Nouvelle option « LAISSER COURIR AU TP »** sur tout ticket manuel qui a un TP (case dans le ticket, sous le TP). Elle est **cochée par défaut pour les tickets Swing Support** (verrou +0,5 % et suiveur 0,5 %, repris de `SUPSW_TRAIL_LOCK_PCT` / `SUPSW_TRAIL_PCT`) ; décochée pour les autres, à cocher à la main.
- **Fonctionnement** : quand le prix touche le TP, le trade n'est plus fermé. Le SL monte aussitôt à +verrou % au-dessus de l'entrée (en SHORT : en dessous), puis suit le plus haut atteint à N % de distance. La sortie s'appelle « TRAILING TAKE PROFIT (manuel) ». LONG et SHORT, paper et live. En **live**, le SL natif Hyperliquid est mis à jour à chaque palier (au moins `MANUAL_LOCK_PUSH_MIN_STEP_PCT`, 0,05 %), donc le stop reste protégé même si le bot s'arrête ; le TP, lui, n'est pas un ordre natif (inchangé).
- **Trades déjà ouverts** : bouton « Modifier » (onglet Trading Manuel) : case « laisser courir au TP » + verrou et suiveur. Le verrou doit rester inférieur à la distance du TP. Retirer le TP désactive l'effet.
- Le trade ouvert affiche « laisser courir » puis « armé » une fois l'objectif atteint. Aucun changement pour les ordres sans cette option.

## v4.396 — Trades manuels : alerte si « laisser courir au TP » n'est pas pris en compte

Fichiers : `index.html`, `index_src.html`, `README.md`.

- Après « Enregistrer » dans la modification d'un trade manuel ouvert, si la case « laisser courir au TP » était cochée et que le serveur ne la confirme pas, un message l'explique. Cause typique : seuls les fichiers de l'interface (`index.html`) ont été déposés, pas `api.py` et `manual_trading.py` de la v4.395 : l'ancien serveur ignore silencieusement les champs qu'il ne connaît pas, et la case revient décochée à la réouverture.
- Aucun changement côté serveur.

## v4.397 — Version affichée par /health

Fichiers : `bot_engine.py`, `README.md`.

- `BOT_VERSION` (4.299 jusque-là, jamais mis à jour) et `BOT_BUILD` reflètent désormais la version déployée : `GET /health` renvoie `"version": "4.397"` et `"build": "2026-10-14-a"`. Ouvrir `https://<ton-app>/health` permet de confirmer que Railway fait bien tourner le dernier code (utile quand une nouveauté de l'interface ne semble pas prise en compte côté serveur).
- Vérifié de bout en bout (requête HTTP simulée, route `PUT /api/manual/positions/{id}`) : « laisser courir au TP » est bien enregistré et renvoyé par le serveur de la v4.395 ; si ce n'est pas le cas chez toi, c'est que le serveur déployé n'est pas à jour.

## v4.398 — « Laisser courir au TP » : message clair quand il n'y a pas de TP
- Cette option n'a de sens qu'avec un TP. Sans TP, le serveur refuse avec un message explicite (avant : option ignorée en silence) et la case est grisée dans le formulaire (« saisis d'abord un TP »).
- Fichiers : manual_trading.py, bot_engine.py (version), index_src.html, index.html, README.md

## v4.399 — Forex : sortie « SANS PROGRES »
- Constat (export 15/10, 248 trades Forex) : 48 trades restés > 2 h sans jamais atteindre +0,15 % de pic finissent à −0,21 % en moyenne (−3,55 $ bruts). Les frais estimés (6,8 $) dépassent le PnL brut (−0,64 $).
- Nouveau : si, après `FOREX_STALE_MIN` (90 min), le pic du trade reste sous `FOREX_STALE_PEAK_PCT` (0,15 %, = seuil d'armement du suiveur), le trade est fermé. Motif affiché : « FOREX SANS PROGRES ». Interrupteur `FOREX_STALE_ENABLED` (1/0) dans les réglages (libellés « Forex · SANS PROGRES »).
- Ne touche ni l'entrée, ni le suiveur, ni le stop dynamique, ni les autres modes. Version affichée par /health : 4.399.
- À mesurer : comparer, après ~1–2 semaines, le net des sorties « FOREX SANS PROGRES » à celui des anciens trades > 2 h sans armement. Le gain réel n'est pas chiffrable à l'avance (l'export n'a pas le prix pendant le trade).
- Fichiers : bot_engine.py, api.py, README.md

## v4.400 — Ordres programmés modifiables
- Bouton **Modifier** sur chaque ligne de « ⏰ Ordres programmés » (Trading Manuel) : prix ou heure de déclenchement, taille, levier, SL %, TP %, « laisser courir au TP » (verrou + suiveur), TTP, et prolongation de l'expiration (« expire dans … h »).
- Mêmes validations qu'à la création (SL 0,05–50 %, TP 0,05–500 %, levier max, 10 $ mini en live, heure dans le futur, « laisser courir » exige un TP). En cas d'erreur, rien n'est modifié.
- Le sens, l'actif, le marché, le mode (paper/live) et le type de déclenchement ne se modifient pas : annuler puis recréer l'ordre.
- Fichiers : manual_trading.py, api.py, bot_engine.py (version), index_src.html, index.html, README.md

## v4.401 — Ordres programmés : changer le sens (LONG/SHORT) et le mode (PAPER/LIVE)
- Dans « Modifier » d'un ordre programmé : listes **SENS** et **MODE**.
- Passage à **LIVE** : confirmation obligatoire (fenêtre d'avertissement), connexion Hyperliquid requise, 10 $ de notionnel minimum, et aucune autre position live sur l'actif. Retour à PAPER : sans confirmation.
- Inverser le sens d'un ordre issu d'une opportunité du bot en fait un ordre libre (il ne suit plus l'opportunité). Le spot reste LONG uniquement.
- Une position déjà **ouverte** ne peut pas changer de sens ni de mode : la fermer puis ouvrir l'autre.
- Fichiers : manual_trading.py, api.py, bot_engine.py (version), index_src.html, index.html, README.md

## v4.402 — Ordres programmés : critères d'entrée modifiables
- Dans « Modifier » d'un ordre programmé : type de déclenchement (prix ≥, prix ≤, heure précise) avec son prix ou son heure, **condition de tendance H4 à l'entrée** (aucune / haussière / baissière / neutre), case « annuler si l'opportunité du bot n'est plus valable au déclenchement » (ordres issus d'une opportunité), ainsi que verrou progressif et règle de sortie sur la tendance.
- Mêmes validations qu'à la création ; en cas d'erreur rien n'est modifié.
- Fichiers : manual_trading.py, api.py, bot_engine.py (version), index_src.html, index.html, README.md

## v4.403 — « Laisser courir au TP » : champs pré-remplis
- Cocher « laisser courir au TP » remplit automatiquement les champs manquants avec les valeurs par défaut : TP = 1 % (ticket et ordre programmé) ou ±1 % autour du prix pour une position ouverte, gain verrouillé = +0,5 %, suiveur = 0,5 %. Une valeur déjà saisie n'est jamais écrasée.
- La case n'est plus grisée quand le TP est vide : elle crée le TP par défaut.
- Fichiers : bot_engine.py (version), index_src.html, index.html, README.md

## v4.404 — TP1 / TP2 (clôture partielle) pour les trades manuels
- **TP1** : à ce niveau, une part de la position (5–95 %, 50 % par défaut) est clôturée au marché ; le **TP existant devient le TP2** (le reste). Option (cochée par défaut) : le SL remonte au seuil de rentabilité (entrée + frais) après le TP1. « Laisser courir au TP » s'applique au TP2.
- Disponible : ticket (case « TP1 — clôture partielle », pré-remplie à la moitié du TP), ordres programmés (Modifier), positions ouvertes (Modifier ; non modifiable une fois encaissé). Affichage « TP1 … / TP2 … », « TP1 ✔ encaissé +x $ ».
- Le PnL affiché et le PnL final incluent la part encaissée au TP1 ; frais comptés au prorata. Motif de sortie « STOP APRES TP1 (manuel) » si le stop touche après le TP1.
- Spot LIVE : refusé (clôture partielle non gérée) ; paper et perp live acceptés.
- Live perp : la clôture partielle utilise un ordre au marché sur la quantité exacte et le SL natif est renvoyé avec la nouvelle taille. **À valider d'abord avec une petite position live** (non testé sur Hyperliquid depuis cet environnement).
- En cas d'échec de la clôture partielle : position inchangée, nouvel essai après 60 s.
- Fichiers : manual_trading.py, api.py, bot_engine.py (version), index_src.html, index.html, README.md

## v4.405 — TP1/TP2 : saisie en prix
- Ticket : le TP1 a maintenant un champ « prix » à côté du « % » (comme le SL et le TP), les deux se convertissent l'un en l'autre.
- Ordre programmé (Modifier) : SL, TP/TP2 et TP1 acceptent le prix ou le %. Référence du calcul : le prix de déclenchement (ordres sur prix) ou le prix actuel (ordres sur heure).
- Les niveaux restent stockés en % de l'entrée : si l'ordre s'exécute au marché à un prix légèrement différent du déclenchement, les prix réels de SL/TP/TP1 sont décalés d'autant.
- Fichiers : bot_engine.py (version), index_src.html, index.html, README.md

## v4.406 — Résumé instantané d'un actif (fenêtre)
- **Clic sur un actif** (tuiles « Prix en temps réel », position du tableau de bord, tickers de Trading Manuel : opportunités, positions, ordres programmés, historique, bouton « 📊 Résumé de l'actif » de l'ordre libre) → **fenêtre** avec bouton **✕ Fermer** (clic à l'extérieur ou Échap aussi).
- Contenu, actualisé toutes les 15 s : prix, variations 1 h / 4 h / 24 h / 7 j, plages 24 h et 7 j, et pour **H1, H4 et H24 (D1)** : tendance (EMA50/200 + prix live), RSI, ATR, **support et résistance** les plus proches (distance en %, « dans la zone » si le prix y est). Contexte : tendance BTC H4, funding annualisé.
- **Conseil LONG / SHORT / ATTENDRE** : score à règles explicites (tendance H4 ×2, D1 ×1, H1 ×1, RSI H4 en surachat/survente, prix sur support/résistance H4/H1, BTC, funding extrême). ≥ +3 → LONG, ≤ −3 → SHORT, sinon ATTENDRE (avec le côté vers lequel ça penche). Les raisons sont listées, avec repères de SL/objectif/gain-risque tirés des zones H4. **Indicatif, pas une garantie.**
- Boutons **▲ Préparer un LONG / ▼ Préparer un SHORT** : ouvrent Trading Manuel avec un ticket libre pré-rempli (valeurs par défaut, rien n'est envoyé tant que vous ne validez pas) et **ferment la fenêtre**.
- Nouvelle route lecture seule : `GET /api/asset/summary?ticker=…`.
- Fichiers : manual_trading.py, api.py, bot_engine.py (version), index_src.html, index.html, README.md

## v4.407 — Résumé d'actif : « ATTENDRE » expliqué + situations programmables
- Le conseil **ATTENDRE** dit maintenant **pourquoi** (signaux partagés, score insuffisant, ou bonne direction mais entrée immédiate trop chère).
- **Nouveau critère** : un LONG/SHORT dont le gain/risque d'une entrée immédiate (stop et objectif sur les zones H4) est < 1,2 devient ATTENDRE. Seuil de score inchangé (±3).
- Bloc **« Situations à attendre »** (zones H4) : LONG sur rebond du support, SHORT sur rejet de la résistance, LONG sur cassure de la résistance, SHORT sur cassure du support. Chacune : prix de déclenchement (et distance), stop au-delà de la zone, objectif sur la zone suivante, gain/risque, mention « contre-tendance » si elle va contre la tendance H4.
- Bouton **⏱ Programmer** : ouvre le ticket libre avec déclenchement (prix ≤ / ≥), SL et TP déjà remplis, validité 72 h. Rien n'est envoyé avant votre validation.
- Fichiers : manual_trading.py, bot_engine.py (version), index_src.html, index.html, README.md

## v4.408 — Résumé d'actif : arguments « pour / contre » par scénario
- Chaque situation à attendre (rebond de support, rejet de résistance, cassure haussière, cassure baissière) affiche un **bilan « n pour · n contre »** et la liste des arguments.
- Critères : tendance H4 et D1 (dans le sens ou à contre-courant), RSI H4 (survente/surachat pour un rebond, élan sans excès pour une cassure), nombre de contacts de la zone (confirmée 2–4, usée ≥ 5), même zone retrouvée en H1/D1, bougie de rejet H4 (mèche) pour les rebonds, volume H4 récent et compression de volatilité pour les cassures, tendance BTC H4, funding extrême.
- Règles usuelles d'analyse technique, **non validées statistiquement** ; elles n'entrent pas dans le score du conseil. À confronter aux résultats réels des trades programmés. Le retest d'une zone cassée n'est pas détecté.
- Fichiers : manual_trading.py, bot_engine.py (version), index_src.html, index.html, README.md

## v4.409 — Pré-programmation unique, choisie sur l'historique de l'actif
- La fenêtre « résumé d'actif » ne met plus les quatre situations au même niveau : elle **retient la plus favorable** (carte ⭐) avec un bouton **⏱ Pré-programmer** (ticket rempli : déclenchement, SL, TP, 72 h). Les autres sont repliées avec la raison de leur non-sélection.
- **Méthode** : chaque situation (rebond de support, rejet de résistance, cassure haussière, cassure baissière) est rejouée sur jusqu'à ~1000 bougies H4 (~6 mois) de l'actif : zones recalculées à chaque bougie avec les seules données connues alors, déclenchement dans les 72 h, puis stop / objectif / sortie à 96 h (stop prioritaire si les deux sont touchés dans la même bougie), frais 0,09 % déduits. Un même épisode n'est compté qu'une fois. Résultat : nombre de cas, % d'objectifs atteints, espérance nette en R.
- **Sélection** : espérance nette > 0, ≥ 8 cas, gain/risque actuel ≥ 1 ; classement par espérance pondérée par la taille de l'échantillon (+ léger bonus au contexte pour/contre). Sinon : « Aucune situation ne ressort — ne rien programmer est un choix valable ».
- Calcul mis en cache par bougie H4 ; mêmes règles de niveaux pour le direct et l'historique (`_levels`).
- Limites : échantillon souvent petit, le passé ne garantit rien, l'historique est propre à l'actif (pas de filtre de tendance).
- Fichiers : manual_trading.py, bot_engine.py (version), index_src.html, index.html, README.md

## v4.410 — Pré-programmation : historique plus court (90 j) et confirmation récente
- Profondeur de l'historique rejoué ramenée de ~6 mois à **90 jours par défaut**, réglable (réglage `ASSET_BT_DAYS`, 30 à 180 jours). Les 150 bougies H4 précédentes ne servent qu'à calculer les zones.
- **Confirmation récente** : une situation n'est retenue que si son espérance nette n'est pas négative sur la **moitié la plus récente** de la fenêtre (dès 4 cas) ; sinon rejet « le régime a peut-être changé ». Le résultat de la moitié récente est affiché.
- Le nombre de jours affiché est celui de la fenêtre jugée.
- Fichiers : manual_trading.py, api.py, bot_engine.py (version + réglage), index_src.html, index.html, README.md

## v4.411 — Sortie manuelle LIVE : vrai prix Hyperliquid retrouvé
- Constat : quand le **stop natif** Hyperliquid se déclenchait avant que le bot ne ferme, la position était déjà absente → le bot reprenait son propre prix du moment et affichait « sortie estimée par le bot », alors que la sortie avait bien eu lieu sur Hyperliquid.
- Correctif : à la fermeture, si aucun remplissage n'est renvoyé, le bot lit les **remplissages du compte** (dernier groupe de clôture sur l'actif) : prix moyen réel, frais réels. La sortie est alors « confirmée par Hyperliquid ».
- **Rattrapage** : une fois par heure, les trades manuels live fermés (14 derniers jours) marqués « estimée » sont recontrôlés ; si le remplissage existe, prix de sortie, PnL et source sont corrigés (fiche manuelle et table des trades). Un seul essai par trade.
- Si le remplissage reste introuvable, le libellé devient « estimée par le bot (remplissage Hyperliquid introuvable) ».
- Fichiers : manual_trading.py, db.py, bot_engine.py (version), index_src.html, index.html, README.md

## v4.412 — Levier refusé par Hyperliquid : le suivi prend le levier réel
- Constat : le SDK ne lève pas d erreur quand Hyperliquid refuse un levier, il renvoie un statut « err ». Le refus passait inaperçu : l'ordre partait avec le levier déjà en place sur Hyperliquid, mais le trade restait suivi avec le levier demandé (marge, % de marge et PnL faux).
- Correctif (trades manuels live perp, ticket et ordres programmés) :
  1. refus détecté et journalisé avec la raison donnée par Hyperliquid ;
  2. si le levier demandé dépasse le maximum de l'actif, nouvel essai au **levier maximal** de l'actif ;
  3. après l'ouverture, le **levier réel de la position** lu sur Hyperliquid devient le levier du trade (le notionnel ne change pas ; la marge utilisée en dépend).
- Affichage : « ⚠ demandé x5 » à côté du levier réel dans les positions ouvertes (survol = explication).
- Fichiers : manual_trading.py, bot_engine.py (version), index_src.html, index.html, README.md
