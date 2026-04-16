select
    id,
    snippet_title,
    snippet_channeltitle,
    snippet_publishedat,
    snippet_categoryid,
    snippet_thumbnails_default_url,
    statistics_viewcount,
    statistics_likecount,
    statistics_commentcount,
    snapshot_ts,
    region_code,
    rank
from {{ source('rds', 'youtube_snapshots') }}