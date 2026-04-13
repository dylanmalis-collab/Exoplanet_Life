{{ config(schema='intermediate') }}

with source as (

    select * from {{ ref('stg_exoplanets') }}

)

select distinct
    systeme_sk,
    nom_etoile_hote,
    nom_hd,
    nom_hipparcos,
    id_tic,
    id_gaia_dr2,
    id_gaia_dr3,
    nb_etoiles_systeme,
    nb_planetes_systeme,
    nb_lunes_systeme,
    systeme_binaire_flag,

    ascension_droite_str,
    ascension_droite,
    declinaison_str,
    declinaison,

    distance_systeme,
    magnitude_visible,
    magnitude_k,
    magnitude_gaia
from source