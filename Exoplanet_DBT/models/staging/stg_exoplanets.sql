{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key([
        'pl_name'
    ]) }} as planete_sk,

    pl_name as nom_planete,

    {{ dbt_utils.generate_surrogate_key([
        'hostname'
    ]) }} as systeme_sk,

    hostname as nom_etoile_hote,
    hd_name as nom_hd,
    hip_name as nom_hipparcos,
    tic_id as id_tic,
    gaia_dr2_id as id_gaia_dr2,
    gaia_dr3_id as id_gaia_dr3,

    sy_snum as nb_etoiles_systeme,
    sy_pnum as nb_planetes_systeme,
    sy_mnum as nb_lunes_systeme,

    cb_flag as systeme_binaire_flag,

    {{ dbt_utils.generate_surrogate_key([
        'discoverymethod'
    ]) }} as methode_sk,

    discoverymethod as methode_decouverte,
    disc_year as annee_decouverte,

    {{ dbt_utils.generate_surrogate_key([
        'disc_facility'
    ]) }} as instrument_sk,

    disc_facility as instrument_decouverte,

    pl_controv_flag as planete_controversee_flag,
    pl_orbper as periode_orbitale_jours,
    pl_orbsmax as demi_grand_axe_ua,
    pl_orbsmax_calc as demi_grand_axe_ua_calc,

    pl_rade as rayon_terre,
    pl_radj as rayon_jupiter,

    pl_bmasse as masse_terre,
    pl_bmassj as masse_jupiter,
    pl_bmassprov as methode_masse,
    pl_bmasse_calc as masse_terre_calc,
    masse_10_24kg as masse_10_24_kg,
    masse_10_24kg_calc as masse_10_24_kg_calc,

    pl_dens as densite_planete,
    pl_dens_calc as densite_planete_calc,

    pl_orbeccen as excentricite_orbite,

    pl_insol as flux_energie_recu,
    pl_insol_calc as flux_energie_recu_calc,

    pl_eqt as temperature_equilibre,
    pl_eqt_calc as temperature_equilibre_calc,

    Gravity_G_earth as gravite_surface_terre_relative,
    Gravity_G_earth_calc as gravite_surface_terre_relative_calc,

    pl_tranmid as temps_milieu_transit,
    ttv_flag as variation_temps_transit_flag,
    pl_occdep as profondeur_occultation,

    {{ dbt_utils.generate_surrogate_key([
        'st_spectype'
    ]) }} as type_etoile_sk,

    st_spectype as type_spectral_etoile,
    st_spectype_group as type_spectral_groupe,
    st_spectype_group_en as type_spectral_groupe_enrichi,

    st_teff as temperature_etoile,
    st_rad as rayon_etoile,
    st_mass as masse_etoile,
    st_met as metallicite_etoile,
    st_metratio as ratio_metallicite,
    st_lum as luminosite_etoile,
    st_lum_lin as luminosite_etoile_lineaire,
    st_lum_lin_calc as luminosite_etoile_lineaire_calc,
    st_logg as gravite_surface_etoile,
    st_dens as densite_etoile,

    Score_roch as score_rocheux,
    pl_type as type_planete,
    sim_earth as score_similarite_terre,
    eau_possible as eau_possible_flag,
    atm_survival_score as score_survie_atmosphere,
    atm_ok as atmosphere_stable_flag,
    pt_habitable as potentiellement_habitable_flag,

    rastr as ascension_droite_str,
    ra as ascension_droite,
    decstr as declinaison_str,
    dec as declinaison,

    sy_dist as distance_systeme,
    sy_vmag as magnitude_visible,
    sy_kmag as magnitude_k,
    sy_gaiamag as magnitude_gaia,

    ingestion_timestamp as date_ingestion

from {{ source('raw', 'raw_exoplanets') }}