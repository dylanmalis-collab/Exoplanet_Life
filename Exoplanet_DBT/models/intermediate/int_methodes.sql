{{ config(schema='intermediate') }}

with source as (

    select * from {{ ref('stg_exoplanets') }}

)

select distinct
    methode_sk,
    methode_decouverte
from source