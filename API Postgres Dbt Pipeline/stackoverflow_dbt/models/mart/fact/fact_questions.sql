{{ config(materialized='table') }}

select
    q.question_id,
    q.creation_date,
    q.owner_user_id as user_id,
    q.score,
    q.view_count,
    q.answer_count,
    q.is_answered,
    q.title,
    q.link
from {{ ref('stg_questions') }} q
