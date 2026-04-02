{{ config(materialized='table') }}

select *
from {{ ref('int_instruments') }}