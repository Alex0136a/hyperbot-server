# HyperBot AI — Serveur Autonome

Bot de trading autonome 24h/24 sur Hyperliquid, propulsé par Claude.

## Déploiement sur Railway

### Étape 1 — Préparer GitHub
1. Créez un nouveau repo GitHub `hyperbot-server`
2. Uploadez tous les fichiers de ce dossier :
   - `main.py`
   - `requirements.txt`
   - `Procfile`
   - `railway.toml`
   - `static/index.html`

### Étape 2 — Déployer sur Railway
1. Allez sur **railway.app**
2. **New Project** → **Deploy from GitHub repo**
3. Sélectionnez `hyperbot-server`
4. Railway détecte automatiquement Python et déploie

### Étape 3 — Obtenir votre URL
1. Dans Railway → votre projet → **Settings** → **Domains**
2. Cliquez **Generate Domain**
3. Votre bot est accessible sur `https://hyperbot-server-xxx.railway.app`

### Étape 4 — Utiliser le bot
1. Ouvrez votre URL Railway
2. Créez un compte (email + mot de passe)
3. Dans Paramètres → entrez votre clé API Anthropic et wallet
4. Cliquez **DÉMARRER**
5. Le bot tourne 24h/24 sur le serveur !

## Fonctionnalités
- ✅ Authentification sécurisée
- ✅ Bot autonome 24h/24 (indépendant du navigateur)
- ✅ Signaux sauvegardés en base de données
- ✅ Accessible depuis n'importe quel appareil
- ✅ Analyse IA via Claude (Anthropic)
- ✅ Données live Hyperliquid
- ✅ 12 marchés configurables

## Structure
```
hyperbot-server/
├── main.py          # Serveur FastAPI + moteur de scan
├── requirements.txt # Dépendances Python
├── Procfile         # Config Railway
├── railway.toml     # Config déploiement
└── static/
    └── index.html   # Interface web avec login
```
