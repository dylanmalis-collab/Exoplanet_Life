#Les Imports
import pandas as pd
import numpy as np
import requests

#requête API en CSV à transformer en requête API JSON
url = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query=select+*+from+pscomppars&format=csv"
df1 = pd.read_csv(url)
df1.head()

#récupération du CSV d'enrichissement depuis GitHub
url2 = "https://raw.githubusercontent.com/dylanmalis-collab/Exoplanet_Life/refs/heads/main/exoplanet_hosts_spectype_clean.csv"
df2 = pd.read_csv(url2)

#merge des 2 dfs
df = df1.merge(
    df2[['hostname', 'st_spectype']],
    on='hostname',
    how='left',
    suffixes=('', '_new') #création temporaire de df['st_spectype_new']
)

#Remplacer uniquement les NaN de df1 par les valeurs de df2
df['st_spectype'] = df['st_spectype'].fillna(df['st_spectype_new'])

#Supprimer la colonne temporaire
df = df.drop(columns=['st_spectype_new'])

#drop des doublons
df = df.drop_duplicates()

#fonction de mapping des types d'étoiles
def map_spectral_type(x):
    if pd.isna(x):
        return "Unknown"
    
    x = str(x).upper()

    if "WD" in x or "DQ" in x or "DC" in x:
        return "White Dwarf"

    if "O" in x:
        return "O"
    if "B" in x:
        return "B"
    if "A" in x:
        return "A"
    if "F" in x:
        return "F"
    if "G" in x:
        return "G"
    if "K" in x:
        return "K"
    if "M" in x:
        return "M"

    return "Other"

#application de la fonction map_spectral_type sur une nouvelle colonne df["st_spectype_group"]
df["st_spectype_group"] = df["st_spectype"].apply(map_spectral_type)

#création d'une colonne df["st_spectype_group_en"] enrichie par le type M (80% des étoiles dans l'univers visible + le plus restrictif des types)
df["st_spectype_group_en"] = df["st_spectype_group"].fillna("M")

#df["st_lum"] est un logarithme, conversion dans une colonne en valeur brut linéaire
df["st_lum_lin"] = 10 ** df["st_lum"]

#création de df["st_lum_lin_calc"] complèté avec la median des st_lum_lin recensé par type d'étoile
df["st_lum_lin_calc"] = df["st_lum_lin"].fillna(df.groupby("st_spectype_group_en")["st_lum_lin"].transform("median"))

#1er calcule des pl_insol manquant avec st_lum_lin_calc et pl_orbsmax pour complèter pl_eqt_calc
df["pl_insol_calc"] = df["pl_insol"].fillna(df["st_lum_lin_calc"] / (df["pl_orbsmax"] ** 2))
df["pl_eqt_calc"] = df["pl_eqt"].fillna(255 * (df["pl_insol_calc"] ** 0.25))

#1er tentative de calcules des pl_orbsmax manquant avec 2 méthodes
df["pl_orbsmax_calc"] = df["pl_orbsmax"].fillna(np.sqrt(df["st_lum_lin_calc"] / df["pl_insol_calc"]))
df["pl_orbsmax_calc"] = df["pl_orbsmax_calc"].fillna(np.sqrt(df["st_lum_lin_calc"] / ((df["pl_eqt_calc"] / 255) ** 4)))

#2ème calcule des pl_insol manquant avec st_lum_lin_calc et pl_orbsmax_calc pour complèter pl_eqt_calc
df["pl_insol_calc"] = df["pl_insol_calc"].fillna(df["st_lum_lin_calc"] / (df["pl_orbsmax_calc"] ** 2))
df["pl_eqt_calc"] = df["pl_eqt_calc"].fillna(255 * (df["pl_insol_calc"] ** 0.25))

#2ème tentative de calcules des pl_orbsmax_calc manquant avec les 2 méthodes
df["pl_orbsmax_calc"] = df["pl_orbsmax_calc"].fillna(np.sqrt(df["st_lum_lin_calc"] / df["pl_insol_calc"]))
df["pl_orbsmax_calc"] = df["pl_orbsmax_calc"].fillna(np.sqrt(df["st_lum_lin_calc"] / ((df["pl_eqt_calc"] / 255) ** 4)))

# calcule de la gravité terrestre en G (1g = terre)
df["Gravity_G_earth"] = df["pl_bmasse"] / (df["pl_rade"] ** 2)

#fonction calculant la gravité, la densité et la masse manquante gâce a une règle de 3 et propage les résultat dans 3 colonnes distinct
def impute_planets(df, max_iter=10):
    df = df.copy()

    cols_check = ["Gravity_G_earth_calc", "pl_dens_calc", "pl_bmasse_calc"]

    # ----------------------------
    # initialisation
    # ----------------------------
    if "pl_bmasse_calc" not in df:
        df["pl_bmasse_calc"] = df["pl_bmasse"]

    if "Gravity_G_earth_calc" not in df:
        df["Gravity_G_earth_calc"] = df["Gravity_G_earth"]

    if "pl_dens_calc" not in df:
        df["pl_dens_calc"] = df["pl_dens"]

    for _ in range(max_iter):

        before_na = df[cols_check].isna().sum().sum()

        # =====================================================
        # 1. M + R → g + densité (FIX BUG ICI)
        # =====================================================
        mask_phys = df["pl_bmasse_calc"].notna() & df["pl_rade"].notna()

        # --- GRAVITÉ ---
        mask_g = mask_phys & df["Gravity_G_earth_calc"].isna()

        df.loc[mask_g, "Gravity_G_earth_calc"] = (
            df.loc[mask_g, "pl_bmasse_calc"] /
            (df.loc[mask_g, "pl_rade"] ** 2)
        )

        # --- DENSITÉ ---
        mask_d = mask_phys & df["pl_dens_calc"].isna()

        df.loc[mask_d, "pl_dens_calc"] = (
            (df.loc[mask_d, "pl_bmasse_calc"] /
            (df.loc[mask_d, "pl_rade"] ** 3)) * 5.51
        )

        # =====================================================
        # 2. densité + rayon → masse
        # =====================================================
        mask_m = (
            df["pl_dens_calc"].notna() &
            df["pl_rade"].notna() &
            df["pl_bmasse_calc"].isna()
        )

        df.loc[mask_m, "pl_bmasse_calc"] = (
            (df.loc[mask_m, "pl_dens_calc"] / 5.51) *
            (df.loc[mask_m, "pl_rade"] ** 3)
        )

        # =====================================================
        # 3. gravité + rayon → masse
        # =====================================================
        mask_m2 = (
            df["Gravity_G_earth_calc"].notna() &
            df["pl_rade"].notna() &
            df["pl_bmasse_calc"].isna()
        )

        df.loc[mask_m2, "pl_bmasse_calc"] = (
            df.loc[mask_m2, "Gravity_G_earth_calc"] *
            (df.loc[mask_m2, "pl_rade"] ** 2)
        )

        # =====================================================
        # convergence
        # =====================================================
        after_na = df[cols_check].isna().sum().sum()

        if after_na == before_na:
            break

    return df

# utilisation de impute_planets sur le df
df = impute_planets(df)

#calcule du poid des planetes en yottakilogramme (Ykg)
df["Masse_10^24kg"] = df["pl_bmasse"].apply(lambda x: x * 5.972)
df["Masse_10^24kg_calc"] = df["pl_bmasse_calc"].apply(lambda x: x * 5.972)

# création d'un score de roche (un peu) simplicite mais qui donne de bon résultat
df["Score_roch"] = df["pl_dens_calc"] * (1/df["pl_rade"])**3

#création du type (rocheux ou non) en fonction du score 2.2 pour ne pas penalisé les super-terre, mais avec un rayon < 1.7
df["pl_type"] = np.where(
    (df["Score_roch"] > 2.2) &
    (df["pl_rade"] <= 1.7),
    "Rocheuse",
    "Gazeuse/Mixte"
)

# Score de similarité a la terre (version simplifié du Earth Similarity Index (ESI)) proposé vers 2011 par Abel Méndez et collaborateurs
df["sim_earth"] = (
    (1 - np.abs((df["pl_rade"] - 1) / (df["pl_rade"] + 1))) ** 0.57 *
    (1 - np.abs((df["pl_dens_calc"] - 5.51) / (df["pl_dens_calc"] + 5.51))) ** 1.07 *
    (1 - np.abs((df["Gravity_G_earth_calc"] - 1) / (df["Gravity_G_earth_calc"] + 1))) ** 0.70 *
    (1 - np.abs((df["pl_eqt_calc"] - 288) / (df["pl_eqt_calc"] + 288))) ** 5.58
) ** 0.25

# création de la colonne eau_possible (1 = oui, 0 = non)
df["eau_possible"] = df["pl_insol_calc"].between(0.5, 1.5).astype(int)

# création de la fonction permettant de definir l'habitabilité (avec un malus de 20% sur les étoile M pour simulé les flares)
def compute_habitability(df):
    df = df.copy()

    # -----------------------
    # 1. Conditions universelles
    # -----------------------
    base_ok = (
        (df["pl_type"] == "Rocheuse") &
        (df["eau_possible"] == 1) &
        (df["Gravity_G_earth_calc"] >= 0.8) &
        (df["pl_bmasse_calc"].between(0.6, 4))
    )

    # -----------------------
    # 2. Nouveau critère M (physique)
    # -----------------------
    g = df["Gravity_G_earth_calc"]
    flux = df["pl_insol_calc"]

    stellar_factor = np.where(
        df["st_spectype_group_en"] == "M",
        1.2,
        1.0
    )

    risk = flux * stellar_factor

    df["atm_survival_score"] = g / risk

    df["atm_ok"] = (df["atm_survival_score"] > 0.8)

    # -----------------------
    # 3. Types stellaires
    # -----------------------
    is_M = df["st_spectype_group_en"] == "M"
    is_GKF = df["st_spectype_group_en"].isin(["G", "K", "F"])

    # -----------------------
    # 4. Habitabilité finale
    # -----------------------
    df["pt_habitable"] = (
        base_ok &
        (
            (is_M & df["atm_ok"]) |
            (is_GKF)
        )
    ).astype(int)

    return df

# Application de compute_habitability au df
df = compute_habitability(df)

