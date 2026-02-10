with source as (
    select *
    from {{ source('stackoverflow_raw', 'answers') }}
)
select
    answer_id,
    question_id,
    site,
    creation_date,
    extracted_at,
    (data->>'is_accepted')::boolean          as is_accepted,
    (data->>'score')::int                   as score,
    (data->'owner'->>'user_id')::bigint     as owner_user_id,
    (data->'owner'->>'display_name')        as owner_display_name,
    (data->'owner'->>'reputation')::int     as owner_reputation,
    (data->'owner'->>'account_id')::bigint	as owner_account_id,
    data
from source