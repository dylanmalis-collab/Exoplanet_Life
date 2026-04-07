with Fact_mesures_planet as (
    select *
    from `Exoplanet_DBT`.`DataFrameFinal`
),

Fact_mesures_planet_renamed as (
    select
        pl_name as nom_planete,
        hostname as nom_etoile_hote,
        discoverymethod as methode_decouverte,
        pl_orbper as periode_orbitale,
        pl_orbperlim as periode_orbitale_ind_limite,
        pl_orbsmax as demi_grand_axe_orbite,
        pl_rade as rayon_planete_terre,
        pl_radj as rayon_planete_jupiter,
        pl_bmasse as masse_planete_terre_ou_msin_i,
        pl_bmassj as masse_planete_jupiter_ou_msin_i,
        pl_bmassprov as provenance_masse_planete,
        pl_dens as densite_planete,
        pl_orbeccen as excentricite_orbite,
        pl_eqt as temperature_equilibre,
        pl_tranmid as milieu_transit,
        ttv_flag as ind_variations_temps_transit,
        pl_occdep as profondeur_occultation,
        `Masse_(10^24kg)` as masse_10_24kg,
        `Gravity_(G_earth)` as gravite_terre,
        Score_roch as score_roch,
        pl_type as type_planete
    from Fact_mesures_planet
)

select *
from Fact_mesures_planet_renamed