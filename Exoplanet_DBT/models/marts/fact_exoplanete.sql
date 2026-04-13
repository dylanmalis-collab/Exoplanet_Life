{{ config(materialized='table') }}

select
    planete_sk as fact_exoplanete_sk,
    planete_sk,
    systeme_sk,
    methode_sk,
    instrument_sk,
    type_etoile_sk,
    annee_decouverte,

    periode_orbitale_jours,
    demi_grand_axe_ua,
    demi_grand_axe_ua_calc,

    rayon_terre,
    rayon_jupiter,

    masse_terre,
    masse_jupiter,
    methode_masse,
    masse_terre_calc,
    masse_10_24_kg,
    masse_10_24_kg_calc,

    densite_planete,
    densite_planete_calc,
    gravite_surface_terre_relative,
    gravite_surface_terre_relative_calc,

    excentricite_orbite,
    flux_energie_recu,
    flux_energie_recu_calc,
    temperature_equilibre,
    temperature_equilibre_calc,

    temps_milieu_transit,
    variation_temps_transit_flag,
    profondeur_occultation,

    score_rocheux,
    score_similarite_terre,
    eau_possible_flag,
    score_survie_atmosphere,
    atmosphere_stable_flag,
    potentiellement_habitable_flag,

    ascension_droite_str,
    ascension_droite,
    declinaison_str,
    declinaison,
    date_ingestion

from {{ ref('int_fact_exoplanetes') }}