with not_latest as (
    select * 
    from {{ref('stg_youtube_snapshots')}}
    where snapshot_ts not in (
        select snapshot_ts
        from {{ref('latest')}}
    )
)
select *
from not_latest
where snapshot_ts = (select max(snapshot_ts) from not_latest)