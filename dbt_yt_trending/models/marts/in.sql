with latest as (
    select * 
    from {{ ref('latest') }}
), 
previous as (
    select * 
    from {{ ref('previous') }}
)

select 
    latest.id,
    latest.snippet_title,
    latest.snippet_channeltitle,
    latest.snippet_thumbnails_default_url,
    latest.statistics_viewcount,
    latest.rank
from latest
left join previous
on previous.id = latest.id
where previous.id is null


