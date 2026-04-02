{{ config(schema='intermediate') }}

with source as (

    select * from {{ ref('stg_exoplanets') }}

)

select distinct
    planete_sk,
    nom_planete,
    planete_controversee_flag
from source