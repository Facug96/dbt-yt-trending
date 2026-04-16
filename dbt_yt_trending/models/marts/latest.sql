select *
from {{ref('stg_youtube_snapshots')}}
where snapshot_ts = (select max(snapshot_ts) from {{ref('stg_youtube_snapshots')}})