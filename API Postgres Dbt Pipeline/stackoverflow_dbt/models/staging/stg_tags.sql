with source as (
    select *
    from {{ source('stackoverflow_raw', 'tags') }}
)
select
    tag_name,
    site,
    extracted_at,
    (data->>'count')::int           as question_count,
    (data->>'has_synonyms')::boolean as has_synonyms,
    (data->>'is_moderator_only')::boolean as is_moderator_only,
    (data->>'is_required')::boolean as is_required,
    data
from source