with log_update_ts(update_ts) as (
    select update_ts 
    from dds.wf_settings
    where entity_name = 'dm_users'
)
, stg_dm_users as (
select 
    object_id user_id
    , object_value::jsonb->>'name' user_name
    , object_value::jsonb->>'login' user_login
    , osu.update_ts
from stg.ordersystem_users osu, log_update_ts luts
where
    osu.update_ts > luts.update_ts
)
, last_update_ts as (
select max(update_ts) update_ts
from stg_dm_users
)
, insert_dm_users as (
insert into dds.dm_users as du (user_id, user_name, user_login)
select user_id, user_name, user_login
from stg_dm_users
on conflict (user_id) do update set 
    user_name = excluded.user_name
    , user_login = excluded.user_login
returning user_id
)
insert into dds.wf_settings (entity_name, update_ts)
select 'dm_users', update_ts
from insert_dm_users iu, last_update_ts
limit 1
on conflict (entity_name) do update set
    update_ts = excluded.update_ts
returning entity_name, update_ts