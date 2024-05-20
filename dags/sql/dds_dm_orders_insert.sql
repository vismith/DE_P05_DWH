with log_update_ts(log_update_ts) as (
select 
    update_ts 
from dds.wf_settings
where entity_name = 'dm_orders'
)
, order_statuses(status) as (
values ('CLOSED'), ('CANCELLED')
)
, stg_orders as (
select 
    object_value :: jsonb ->> '_id' order_key
    , oss.status order_status
    , object_value :: jsonb #>> '{user, id}' user_id
    , object_value :: jsonb #>> '{restaurant, id}' restaurant_id
    , (object_value :: jsonb ->> 'update_ts')::timestamp ts
from log_update_ts luts, order_statuses oss
join stg.ordersystem_orders oo on oo.object_value::jsonb->>'final_status' = oss.status
where (object_value :: jsonb ->> 'update_ts')::timestamp > log_update_ts
)
, insert_dm_orders as (
insert into dds.dm_orders (
    order_key
    , order_status
    , user_id
    , restaurant_id
    , timestamp_id
    )
select 
    order_key
    , order_status
    , du.id user_id 
    , dr.id restaurant_id 
    , dts.id timestamp_id
from stg_orders so
join dds.dm_users du using(user_id)
join dds.dm_restaurants dr using(restaurant_id)
join dds.dm_timestamps dts using(ts)
on conflict do nothing
returning timestamp_id id
)
insert into dds.wf_settings (entity_name, update_ts)
select 
    'dm_orders'
    , dt.ts update_ts
from insert_dm_orders
join dds.dm_timestamps dt using(id)
order by dt.ts desc 
limit 1
on conflict (entity_name) do update set 
    update_ts = excluded.update_ts
returning entity_name, update_ts