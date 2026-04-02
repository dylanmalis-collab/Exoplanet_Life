{{ config(materialized='table') }}

select *
from {{ ref('int_fact_exoplanetes') }}