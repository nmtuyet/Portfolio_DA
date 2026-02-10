{{ config(materialized='table') }}

select
    u.user_id,
    count(distinct q.question_id) as total_questions,
    count(distinct a.answer_id)   as total_answers,
    count(distinct c.comment_id)  as total_comments
from {{ ref('stg_users') }} u
left join {{ ref('stg_questions') }} q
    on u.user_id = q.owner_user_id
left join {{ ref('stg_answers') }} a
    on u.user_id = a.owner_user_id
left join {{ ref('stg_comments') }} c
    on u.user_id = c.owner_user_id
group by u.user_id
