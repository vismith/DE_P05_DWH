with log_update_ts(log_update_ts) as (
    select update_ts 
    from dds.wf_settings
    where entity_name = 'dm_restaurants'
)
, stg_dm_restaurants as (
select 
    object_id restaurant_id
    , object_value::jsonb->>'name' restaurant_name
    , update_ts active_from
    , '2099-12-31'::timestamp active_to
from stg.ordersystem_restaurants osr, log_update_ts luts
where osr.update_ts > luts.log_update_ts
)
, load_update_ts as (
select max(active_from) update_ts 
from stg_dm_restaurants
)
, insert_dm_restaurants as (
insert into dds.dm_restaurants
    (restaurant_id, restaurant_name, active_from, active_to)
select 
    restaurant_id 
    , restaurant_name 
    , active_from 
    , active_to
from stg_dm_restaurants
on conflict (restaurant_id) do update set
    restaurant_name = excluded.restaurant_name
returning id, restaurant_id
)
insert into dds.wf_settings (entity_name, update_ts)
select 
    'dm_restaurants'
    , luts.update_ts
from insert_dm_restaurants idmr, load_update_ts luts
limit 1
on conflict (entity_name) do update set
update_ts = excluded.update_ts
returning entity_name, update_ts