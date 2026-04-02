{{ config(schema='intermediate') }}

with source as (

    select * from {{ ref('stg_exoplanets') }}

)

select distinct
    instrument_sk,
    instrument_decouverte
from source
where instrument_decouverte is not null