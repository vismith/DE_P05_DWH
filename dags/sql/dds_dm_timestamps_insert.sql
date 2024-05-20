with log_update_ts(log_update_ts) as (
select update_ts
from dds.wf_settings
where entity_name = 'dm_timestamps'
)
, order_statuses(status) as (
values ('CLOSED'), ('CANCELLED')
)
, stg_orders_timestamps as (
select
    distinct 
    (object_value::jsonb ->> 'update_ts')::timestamp ts
from log_update_ts luts, order_statuses oss
join stg.ordersystem_orders oo 
    on oo.object_value::jsonb->>'final_status' = oss.status
where 
    oo.update_ts > luts.log_update_ts
)
, insert_dm_timestamps as (
insert into dds.dm_timestamps(ts)
select ts
from stg_orders_timestamps
order by ts
on conflict (ts) do nothing
returning ts
)
insert into dds.wf_settings (entity_name, update_ts)
select 
    'dm_timestamps'
    , ts
from insert_dm_timestamps
order by ts desc 
limit 1
on conflict (entity_name) do update set 
update_ts = excluded.update_ts
returning entity_name, update_ts