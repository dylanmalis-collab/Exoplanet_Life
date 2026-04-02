{{ config(schema='intermediate') }}

with source as (

    select * from {{ ref('stg_exoplanets') }}

)

select distinct
    type_etoile_sk,
    type_spectral_etoile
from source