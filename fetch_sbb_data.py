"""
fetch_sbb_data.py
─────────────────────────────────────────────────────────────────────────────
Script batch : télécharge les données SBB istdaten depuis opentransportdata.swiss
pour chaque jour depuis le 1er janvier 2026, agrège par Gare×Date et
sauvegarde dans data/sbb_retards_2026.csv (lu ensuite par app.py).

Usage :
    python3 fetch_sbb_data.py

Durée estimée : ~15–20 min (rate limit 5 calls/min)
"""

import io
import os
import re
import time
import requests
import pandas as pd
from datetime import date, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
TOKEN      = "eyJvcmciOiI2NDA2NTFhNTIyZmEwNTAwMDEyOWJiZTEiLCJpZCI6IjI4YjM0YzlmNmQyNjQ2N2Y4OGI4YjcwYmM1NjVkNzQ4IiwiaCI6Im11cm11cjEyOCJ9"
HEADERS    = {"Authorization": TOKEN}
DATASET_ID = "0edc74a3-ad4d-486e-8657-f8f3b34a0979"
START_DATE = date(2026, 1, 1)
END_DATE   = date(2026, 2, 26)   # Dernier jour disponible
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "sbb_retards_2026.csv")
MAX_ROWS   = 400_000              # Lignes max par fichier (streaming)
RATE_LIMIT = 5                   # Appels max par minute


def get_resource_map() -> dict[str, str]:
    """Scrape les resource IDs depuis les pages HTML du dataset."""
    pattern = r'/resource/([0-9a-f\-]{36})/download/(2026-\d{2}-\d{2})_istdaten\.csv'
    resource_map = {}
    for page in range(1, 5):   # On scanne 4 pages pour couvrir toute l'année
        url = f"https://data.opentransportdata.swiss/en/dataset/istdaten?page={page}"
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            break
        matches = re.findall(pattern, r.text)
        new = {d: rid for rid, d in matches}
        if not new:
            break
        resource_map.update(new)
        print(f"  Page {page} : {len(new)} ressources trouvées")
        time.sleep(0.5)
    return resource_map


def build_url(day_str: str, resource_id: str) -> str:
    return (
        f"https://data.opentransportdata.swiss/dataset/{DATASET_ID}"
        f"/resource/{resource_id}/download/{day_str}_istdaten.csv"
    )


def fetch_and_aggregate(day_str: str, resource_id: str) -> pd.DataFrame | None:
    """Télécharge un jour, filtre SBB trains réels, calcule les retards et agrège."""
    url = build_url(day_str, resource_id)
    try:
        r = requests.get(url, headers=HEADERS, timeout=120, stream=True)
        if r.status_code != 200:
            print(f"  ⚠️  {day_str} : HTTP {r.status_code}")
            return None

        lines = []
        for i, line in enumerate(r.iter_lines()):
            lines.append(line.decode("utf-8", errors="replace"))
            if i >= MAX_ROWS:
                break

        df = pd.read_csv(io.StringIO("\n".join(lines)), sep=";", low_memory=False)

        # Filtres métier : trains SBB réels uniquement
        df = df[
            (df["BETREIBER_ABK"] == "SBB") &
            (df["PRODUKT_ID"] == "Zug") &
            (df["AN_PROGNOSE_STATUS"] == "REAL")
        ].dropna(subset=["ANKUNFTSZEIT", "AN_PROGNOSE"])

        if df.empty:
            return None

        # Calcul du retard réel (secondes → minutes)
        df["ANKUNFTSZEIT"] = pd.to_datetime(df["ANKUNFTSZEIT"], format="%d.%m.%Y %H:%M",    errors="coerce")
        df["AN_PROGNOSE"]  = pd.to_datetime(df["AN_PROGNOSE"],  format="%d.%m.%Y %H:%M:%S", errors="coerce")
        df["Retard_Minutes"] = (
            (df["AN_PROGNOSE"] - df["ANKUNFTSZEIT"]).dt.total_seconds() / 60
        ).clip(lower=0).round(1)

        # Agrégation par Gare
        df["Date"] = pd.Timestamp(day_str)
        agg = df.groupby(["Date", "HALTESTELLEN_NAME"]).agg(
            Voyageurs_Impactes=("AN_PROGNOSE", "count"),
            Retard_Minutes=("Retard_Minutes", "mean"),
        ).reset_index().rename(columns={"HALTESTELLEN_NAME": "Gare"})
        agg["Retard_Minutes"] = agg["Retard_Minutes"].round(1)

        return agg

    except Exception as e:
        print(f"  ❌ {day_str} : {e}")
        return None


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=" * 60)
    print("  SBB Istdaten Fetcher — Janvier → Février 2026")
    print("=" * 60)

    # Récupération dynamique des resource IDs
    print("\n📋 Récupération des resource IDs depuis le site…")
    resource_map = get_resource_map()
    print(f"  Total : {len(resource_map)} jours trouvés\n")

    # Filtrage sur la plage de dates voulue
    days_to_fetch = []
    d = START_DATE
    while d <= END_DATE:
        day_str = d.strftime("%Y-%m-%d")
        if day_str in resource_map:
            days_to_fetch.append((day_str, resource_map[day_str]))
        else:
            print(f"  ⚠️  {day_str} : pas de resource ID (jour peut-être absent)")
        d += timedelta(days=1)

    print(f"📥 {len(days_to_fetch)} jours à télécharger\n")
    print(f"⏱️  Durée estimée : ~{len(days_to_fetch) / RATE_LIMIT:.0f} min (rate limit {RATE_LIMIT}/min)\n")

    # Chargement et agrégation
    all_frames = []
    for i, (day_str, resource_id) in enumerate(days_to_fetch):
        print(f"[{i+1:2d}/{len(days_to_fetch)}] {day_str}… ", end="", flush=True)
        frame = fetch_and_aggregate(day_str, resource_id)
        if frame is not None:
            all_frames.append(frame)
            print(f"✅ ({len(frame)} gares, retard moyen: {frame['Retard_Minutes'].mean():.1f} min)")
        else:
            print("⏭️  skipped")

        # Respect du rate limit (5 appels/min = 1 appel toutes les 12s)
        if (i + 1) % RATE_LIMIT == 0:
            print(f"  ⏸️  Pause rate limit…")
            time.sleep(65)   # 65s pour être safe

    # Sauvegarde
    if not all_frames:
        print("\n❌ Aucune donnée récupérée !")
        return

    df_final = pd.concat(all_frames, ignore_index=True)
    df_final.to_csv(OUTPUT_CSV, index=False)

    print(f"\n✅ Données sauvegardées dans : {OUTPUT_CSV}")
    print(f"   Lignes : {len(df_final):,}")
    print(f"   Gares  : {df_final['Gare'].nunique():,}")
    print(f"   Période: {df_final['Date'].min().date()} → {df_final['Date'].max().date()}")
    print(f"   Retard moyen global: {df_final['Retard_Minutes'].mean():.2f} min")


if __name__ == "__main__":
    main()
