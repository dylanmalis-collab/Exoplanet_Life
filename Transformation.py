#Les Imports
import pandas as pd
import numpy as np
import requests

#requête API en CSV a transformer en requête API JSON
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