{{ config(schema='intermediate') }}

with source as (

    select * from {{ ref('stg_exoplanets') }}

)

select
    type_etoile_sk,
    type_spectral_etoile,
    type_spectral_groupe,
    type_spectral_groupe_enrichi,

    avg(temperature_etoile) as temperature_etoile,
    avg(rayon_etoile) as rayon_etoile,
    avg(masse_etoile) as masse_etoile,
    avg(metallicite_etoile) as metallicite_etoile,
    any_value(ratio_metallicite) as ratio_metallicite,
    avg(luminosite_etoile) as luminosite_etoile,
    avg(luminosite_etoile_lineaire) as luminosite_etoile_lineaire,
    avg(luminosite_etoile_lineaire_calc) as luminosite_etoile_lineaire_calc,
    avg(gravite_surface_etoile) as gravite_surface_etoile,
    avg(densite_etoile) as densite_etoile

from source
group by
    type_etoile_sk,
    type_spectral_etoile,
    type_spectral_groupe,
    type_spectral_groupe_enrichi