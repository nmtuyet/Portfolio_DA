with source as (
    select *
    from {{ source('stackoverflow_raw', 'questions') }}
)
select
    question_id,
    site,
    creation_date,
    extracted_at,
    (data->>'title')                        as title,
    (data->>'link')                         as link,
    (data->>'is_answered')::boolean         as is_answered,
    (data->>'view_count')::int              as view_count,
    (data->>'answer_count')::int            as answer_count,
    (data->>'score')::int                   as score,
    (data->'owner'->>'user_id')::bigint     as owner_user_id,
    (data->'owner'->>'display_name')        as owner_display_name,
    (data->'owner'->>'reputation')::int     as owner_reputation,
    (data->'owner'->>'account_id')::bigint	as owner_account_id,
    data
from source