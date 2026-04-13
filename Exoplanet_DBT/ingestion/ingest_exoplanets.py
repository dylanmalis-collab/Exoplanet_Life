import os
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery


# =========================================================
# CONFIGURATION
# =========================================================

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")

credentials_path = (BASE_DIR.parent / "credentials" / "cle_bigquery.json").resolve()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_path)

PROJECT_ID = "exoplanet-491915"
DATASET_ID = "raw_data"
TABLE_NAME = "raw_exoplanets"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

NASA_URL = (
    "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
    "?query=select+*+from+pscomppars"
    "&format=csv"
)

ENRICHMENT_URL = (
    "https://raw.githubusercontent.com/dylanmalis-collab/Exoplanet_Life/"
    "refs/heads/main/exoplanet_hosts_spectype_clean.csv"
)

EXPORT_DIR = BASE_DIR / "data_exports"
EXPORT_FILE = EXPORT_DIR / "raw_exoplanets_enriched.csv"


# =========================================================
# FONCTIONS UTILITAIRES
# =========================================================

def map_spectral_type(value) -> str:
    if pd.isna(value):
        return "Unknown"

    value = str(value).upper()

    if "WD" in value or "DQ" in value or "DC" in value:
        return "White Dwarf"

    if "O" in value:
        return "O"
    if "B" in value:
        return "B"
    if "A" in value:
        return "A"
    if "F" in value:
        return "F"
    if "G" in value:
        return "G"
    if "K" in value:
        return "K"
    if "M" in value:
        return "M"

    return "Other"


def impute_planets(df: pd.DataFrame, max_iter: int = 10) -> pd.DataFrame:
    df = df.copy()

    cols_check = ["Gravity_G_earth_calc", "pl_dens_calc", "pl_bmasse_calc"]

    if "pl_bmasse_calc" not in df.columns:
        df["pl_bmasse_calc"] = df["pl_bmasse"]

    if "Gravity_G_earth_calc" not in df.columns:
        df["Gravity_G_earth_calc"] = df["Gravity_G_earth"]

    if "pl_dens_calc" not in df.columns:
        df["pl_dens_calc"] = df["pl_dens"]

    for _ in range(max_iter):
        before_na = df[cols_check].isna().sum().sum()

        mask_phys = df["pl_bmasse_calc"].notna() & df["pl_rade"].notna()

        mask_g = mask_phys & df["Gravity_G_earth_calc"].isna()
        df.loc[mask_g, "Gravity_G_earth_calc"] = (
            df.loc[mask_g, "pl_bmasse_calc"] /
            (df.loc[mask_g, "pl_rade"] ** 2)
        )

        mask_d = mask_phys & df["pl_dens_calc"].isna()
        df.loc[mask_d, "pl_dens_calc"] = (
            (df.loc[mask_d, "pl_bmasse_calc"] / (df.loc[mask_d, "pl_rade"] ** 3)) * 5.51
        )

        mask_m_from_density = (
            df["pl_dens_calc"].notna() &
            df["pl_rade"].notna() &
            df["pl_bmasse_calc"].isna()
        )
        df.loc[mask_m_from_density, "pl_bmasse_calc"] = (
            (df.loc[mask_m_from_density, "pl_dens_calc"] / 5.51) *
            (df.loc[mask_m_from_density, "pl_rade"] ** 3)
        )

        mask_m_from_gravity = (
            df["Gravity_G_earth_calc"].notna() &
            df["pl_rade"].notna() &
            df["pl_bmasse_calc"].isna()
        )
        df.loc[mask_m_from_gravity, "pl_bmasse_calc"] = (
            df.loc[mask_m_from_gravity, "Gravity_G_earth_calc"] *
            (df.loc[mask_m_from_gravity, "pl_rade"] ** 2)
        )

        after_na = df[cols_check].isna().sum().sum()
        if after_na == before_na:
            break

    return df


def compute_habitability(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    base_ok = (
        (df["pl_type"] == "Rocheuse") &
        (df["eau_possible"] == 1) &
        (df["Gravity_G_earth_calc"] >= 0.8) &
        (df["pl_bmasse_calc"].between(0.6, 4))
    )

    gravity = df["Gravity_G_earth_calc"]
    flux = df["pl_insol_calc"]

    stellar_factor = np.where(
        df["st_spectype_group_en"] == "M",
        1.2,
        1.0
    )

    risk = flux * stellar_factor
    df["atm_survival_score"] = gravity / risk
    df["atm_ok"] = df["atm_survival_score"] > 0.8

    is_m = df["st_spectype_group_en"] == "M"
    is_gkf = df["st_spectype_group_en"].isin(["G", "K", "F"])

    df["pt_habitable"] = (
        base_ok &
        (
            (is_m & df["atm_ok"]) |
            is_gkf
        )
    ).astype(int)

    return df


# =========================================================
# EXTRACTION
# =========================================================

def fetch_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    print("Téléchargement des données NASA...")
    df_nasa = pd.read_csv(NASA_URL, low_memory=False)

    print("Téléchargement des données d’enrichissement GitHub...")
    df_enrichment = pd.read_csv(ENRICHMENT_URL)

    return df_nasa, df_enrichment


# =========================================================
# TRANSFORMATION
# =========================================================

def transform_data(df_nasa: pd.DataFrame, df_enrichment: pd.DataFrame) -> pd.DataFrame:
    print("Fusion des datasets...")

    df = df_nasa.merge(
        df_enrichment[["hostname", "st_spectype"]],
        on="hostname",
        how="left",
        suffixes=("", "_new")
    )

    df["st_spectype"] = df["st_spectype"].fillna(df["st_spectype_new"])
    df = df.drop(columns=["st_spectype_new"])

    df = df.drop_duplicates()

    df["st_spectype_group"] = df["st_spectype"].apply(map_spectral_type)
    df["st_spectype_group_en"] = df["st_spectype_group"].replace("Unknown", "M")

    df["st_lum_lin"] = 10 ** df["st_lum"]

    df["st_lum_lin_calc"] = df["st_lum_lin"].fillna(
        df.groupby("st_spectype_group_en")["st_lum_lin"].transform("median")
    )

    df["pl_insol_calc"] = df["pl_insol"].fillna(
        df["st_lum_lin_calc"] / (df["pl_orbsmax"] ** 2)
    )
    df["pl_eqt_calc"] = df["pl_eqt"].fillna(
        255 * (df["pl_insol_calc"] ** 0.25)
    )

    df["pl_orbsmax_calc"] = df["pl_orbsmax"].fillna(
        np.sqrt(df["st_lum_lin_calc"] / df["pl_insol_calc"])
    )
    df["pl_orbsmax_calc"] = df["pl_orbsmax_calc"].fillna(
        np.sqrt(df["st_lum_lin_calc"] / ((df["pl_eqt_calc"] / 255) ** 4))
    )

    df["pl_insol_calc"] = df["pl_insol_calc"].fillna(
        df["st_lum_lin_calc"] / (df["pl_orbsmax_calc"] ** 2)
    )
    df["pl_eqt_calc"] = df["pl_eqt_calc"].fillna(
        255 * (df["pl_insol_calc"] ** 0.25)
    )

    df["pl_orbsmax_calc"] = df["pl_orbsmax_calc"].fillna(
        np.sqrt(df["st_lum_lin_calc"] / df["pl_insol_calc"])
    )
    df["pl_orbsmax_calc"] = df["pl_orbsmax_calc"].fillna(
        np.sqrt(df["st_lum_lin_calc"] / ((df["pl_eqt_calc"] / 255) ** 4))
    )

    df["Gravity_G_earth"] = df["pl_bmasse"] / (df["pl_rade"] ** 2)

    df = impute_planets(df)

    df["masse_10_24kg"] = df["pl_bmasse"] * 5.972
    df["masse_10_24kg_calc"] = df["pl_bmasse_calc"] * 5.972

    df["Score_roch"] = df["pl_dens_calc"] * (1 / df["pl_rade"]) ** 3

    df["pl_type"] = np.where(
        (df["Score_roch"] > 2.2) & (df["pl_rade"] <= 1.7),
        "Rocheuse",
        "Gazeuse/Mixte"
    )

    df["sim_earth"] = (
        (1 - np.abs((df["pl_rade"] - 1) / (df["pl_rade"] + 1))) ** 0.57 *
        (1 - np.abs((df["pl_dens_calc"] - 5.51) / (df["pl_dens_calc"] + 5.51))) ** 1.07 *
        (1 - np.abs((df["Gravity_G_earth_calc"] - 1) / (df["Gravity_G_earth_calc"] + 1))) ** 0.70 *
        (1 - np.abs((df["pl_eqt_calc"] - 288) / (df["pl_eqt_calc"] + 288))) ** 5.58
    ) ** 0.25

    df["eau_possible"] = df["pl_insol_calc"].between(0.5, 1.5).astype(int)

    df = compute_habitability(df)

    df["ingestion_timestamp"] = pd.Timestamp.utcnow()

    df = df.replace([np.inf, -np.inf], np.nan)

    print(f"Transformation terminée : {len(df)} lignes prêtes à être chargées.")
    return df


# =========================================================
# EXPORT CSV
# =========================================================

def export_to_csv(df: pd.DataFrame) -> None:
    """Exporte le DataFrame enrichi dans le repo."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(EXPORT_FILE, index=False, encoding="utf-8")
    print(f"CSV exporté : {EXPORT_FILE}")


# =========================================================
# CHARGEMENT
# =========================================================

def load_to_bigquery(df: pd.DataFrame) -> None:
    print(f"Chargement dans BigQuery : {TABLE_ID}")

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    load_job = client.load_table_from_dataframe(
        df,
        TABLE_ID,
        job_config=job_config,
    )

    load_job.result()

    print(f"Succès : {len(df)} lignes chargées dans {TABLE_ID}.")


# =========================================================
# PIPELINE PRINCIPAL
# =========================================================

def main() -> None:
    df_nasa, df_enrichment = fetch_data()
    df_final = transform_data(df_nasa, df_enrichment)
    export_to_csv(df_final)
    load_to_bigquery(df_final)


if __name__ == "__main__":
    main()