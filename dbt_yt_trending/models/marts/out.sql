with latest as (
    select * 
    from {{ ref('latest') }}
), 
previous as (
    select * 
    from {{ ref('previous') }}
)

select 
    previous.id,
    previous.snippet_title,
    previous.snippet_channeltitle,
    previous.snippet_thumbnails_default_url,
    previous.statistics_viewcount,
    previous.rank
from previous
left join latest
on previous.id = latest.id
where latest.id is null