{{ config(materialized='table') }}

select *
from {{ ref('int_types_etoile') }}