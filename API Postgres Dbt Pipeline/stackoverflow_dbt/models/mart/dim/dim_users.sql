{{ config(materialized='table') }}

select
    user_id,
    site,
    user_site_pk,
    display_name,
    reputation,
    extracted_at
from {{ ref('stg_users') }}