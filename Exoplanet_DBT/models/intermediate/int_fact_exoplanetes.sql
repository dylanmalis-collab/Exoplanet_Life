{{ config(schema='intermediate') }}

with source as (

    select * from {{ ref('stg_exoplanets') }}

)

select
    planete_sk,
    systeme_sk,
    methode_sk,
    instrument_sk,
    type_etoile_sk,
    annee_decouverte,

    periode_orbitale_jours,
    rayon_terre,
    rayon_jupiter,
    masse_terre,
    masse_jupiter,
    methode_masse,
    densite_planete,
    excentricite_orbite,
    flux_energie_recu,
    temperature_equilibre,
    temps_milieu_transit,
    variation_temps_transit_flag,
    profondeur_occultation,

    temperature_etoile,
    rayon_etoile,
    masse_etoile,
    metallicite_etoile,
    ratio_metallicite,
    luminosite_etoile,
    gravite_surface_etoile,
    densite_etoile,

    ascension_droite_str,
    ascension_droite,
    declinaison_str,
    declinaison

from source