with dds_log_update_ts(log_update_ts) as (
select update_ts
from dds.wf_settings
where entity_name = 'fct_deliveries'
)
, stg_dlvs as (
select 
    object_value->>'delivery_id' delivery_id
    , object_value ->> 'order_id' order_api_id
    , object_value ->> 'courier_id' courier_id
    , (object_value ->> 'order_ts')::timestamp order_ts
    , (object_value ->> 'rate')::int rate
    , (object_value ->> 'tip_sum')::numeric tip_sum
from stg.deliveries sd, dds_log_update_ts luts
where (object_value->>'order_ts')::timestamp > log_update_ts
)
, upsert_fct_deliveries as (
insert into dds.fct_deliveries (delivery_id, order_id, courier_id, rate, tip_sum)
select 
    delivery_id
    , dos.id  order_id
    , dc.id courier_id
    , rate
    , tip_sum
from stg_dlvs sds
left join dds.dm_orders dos on dos.order_key = sds.order_api_id
left join dds.dm_couriers dc using(courier_id) 
on conflict (delivery_id) do nothing 
returning id
)
insert into dds.wf_settings (entity_name, update_ts)
select
    'fct_deliveries' entity_name, 
    order_ts update_ts
from stg_dlvs
order by order_ts desc
limit 1
on conflict (entity_name) do update set 
    update_ts = excluded.update_ts
returning entity_name, update_ts


