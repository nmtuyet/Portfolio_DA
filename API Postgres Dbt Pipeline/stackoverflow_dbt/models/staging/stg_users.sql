with users_from_questions as (
    select
        (data->'owner'->>'user_id')::bigint     as user_id,
        site,
        (data->'owner'->>'display_name')        as display_name,
        (data->'owner'->>'reputation')::int     as reputation,
        extracted_at
    from {{ source('stackoverflow_raw', 'questions') }}
    where data->'owner'->>'user_id' is not null
),
users_from_answers as (
    select
        (data->'owner'->>'user_id')::bigint     as user_id,
        site,
        (data->'owner'->>'display_name')        as display_name,
        (data->'owner'->>'reputation')::int     as reputation,
        extracted_at
    from {{ source('stackoverflow_raw', 'answers') }}
    where data->'owner'->>'user_id' is not null
),
unioned as (
    select * from users_from_questions
    union all
    select * from users_from_answers
),
ranked as (
    select
        *,
        row_number() over (
            partition by user_id, site
            order by extracted_at desc
        ) as rn
    from unioned
)
select
    user_id,
    site,
    md5(coalesce(user_id::text, '') || '-' ||coalesce(site, '')) as user_site_pk,
    display_name,
    reputation,
    extracted_at
from ranked
where rn = 1
