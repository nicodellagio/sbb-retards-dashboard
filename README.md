# Analyse des Retards Ferroviaires SBB

> **Portfolio Data Analyst** — Pipeline ETL complet sur données réelles Swiss Federal Railways (SBB/CFF), de l'extraction brute à un dashboard décisionnel interactif.

---

## 🚉 Contexte

**Analyse des flux voyageurs et de l'impact des retards sur le réseau ferroviaire suisse.**

Les données istdaten de l'Open Data Platform Mobility Switzerland (`opentransportdata.swiss`) contiennent les passages réels de tous les trains suisses, à la seconde près. Ce projet traite **50 jours de données réelles SBB** (janvier–février 2026), couvrant **658 gares** et plusieurs centaines de milliers de passages enregistrés.

---

## 🎯 Objectif

**Transformer des données brutes en indicateurs décisionnels.**

À partir d'un export brut difficile à exploiter (format CSV volumineux, erreurs de saisie, champs manquants), construire un pipeline de nettoyage robuste et un tableau de bord interactif permettant à un décideur de comprendre en 30 secondes où concentrer les efforts d'amélioration.

---

## ⚙️ Pipeline

Architecture ETL en 4 étapes, reproduisant la logique Power Query / Power BI en Python pur :

| Étape | Description | Outil |
|---|---|---|
| **Extract** | Téléchargement en streaming via API REST (`opentransportdata.swiss`), filtrage SBB + `PRODUKT_ID='Zug'` + `AN_PROGNOSE_STATUS='REAL'` | `requests` |
| **Transform** | Calcul du retard réel : `AN_PROGNOSE − ANKUNFTSZEIT` (en secondes → minutes), agrégation par Gare × Date | `pandas` |
| **Clean** | Suppression des doublons, imputation des valeurs manquantes (médiane), correction des outliers (retards négatifs → 0), catégorisation métier | `pandas` |
| **Load** | Sauvegarde CSV local pré-agrégé (~30k lignes), lecture instantanée par le dashboard | CSV / `st.cache_data` |

---

## 📊 Résultats

**Dashboard interactif Streamlit** accessible en ligne — [▶ Voir le dashboard](#)

- **KPIs métier** : Total voyageurs, retard moyen, taux de ponctualité (97.4% sur données réelles)
- **KPIs pipeline** : Doublons supprimés, valeurs imputées, anomalies corrigées, % de lignes affectées
- **Visualisations** : Évolution temporelle des retards, distribution par catégorie, top gares
- **Recommandations opérationnelles** : Gare prioritaire, période problématique, type d'incident dominant — générés dynamiquement selon les filtres actifs

---

## 🧠 Compétences démontrées

| Domaine | Détail |
|---|---|
| **Python & pandas** | Manipulation de datasets, agrégations, parsing datetime, chaînes de traitement |
| **Nettoyage de données** | Détection et suppression de doublons, imputation par médiane, correction d'outliers |
| **ETL & API** | Extraction par streaming (rate-limiting géré), transformation, chargement local |
| **Dataviz** | Dashboard Streamlit + Plotly : graphiques interactifs, filtres dynamiques |
| **Logique ETL** | Pipeline reproductible, documenté, traçable (métriques de qualité exposées en KPIs) |
| **Reporting** | Section "Recommandations opérationnelles" générée dynamiquement pour un décideur non-technique |

---

## 🚀 Lancer le projet

```bash
# Cloner le dépôt
git clone https://github.com/Dellagiovanna/sbb-retards-dashboard.git
cd sbb-retards-dashboard

# Installer les dépendances
pip install -r requirements.txt

# Configurer le token API (opentransportdata.swiss)
echo '[opentransportdata]\ntoken = "VOTRE_TOKEN"' > .streamlit/secrets.toml

# Télécharger les données (une seule fois, ~15 min)
python3 fetch_sbb_data.py

# Lancer le dashboard
streamlit run app.py
```

> **Token gratuit** : inscription sur [opentransportdata.swiss](https://opentransportdata.swiss/en/register) — plan Formation, 20 000 appels/jour.

---

## 📁 Structure

```
.
├── app.py                  # Dashboard Streamlit principal
├── fetch_sbb_data.py       # Script batch de collecte des données
├── data/
│   └── sbb_retards_2026.csv   # Données pré-agrégées (généré par fetch_sbb_data.py)
├── requirements.txt
└── .streamlit/
    └── secrets.toml        # Token API (ne pas committer)
```

---

*Données source : [opentransportdata.swiss — istdaten](https://data.opentransportdata.swiss/en/dataset/istdaten) · Licence ODbL*
