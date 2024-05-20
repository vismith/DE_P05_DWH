with stg_couriers as (
--
select 
    courier_id courier_id
    , object_value ->>'name' courier_name
from stg.couriers c 
full join dds.dm_couriers dc using(courier_id)
except
select 
    courier_id 
    , courier_name 
from dds.dm_couriers dc 
)
, dds_upsert as (
insert into dds.dm_couriers (courier_id, courier_name)
--
    select courier_id, courier_name
    from stg_couriers
--
on conflict (courier_id) do update set 
    courier_name = excluded.courier_name
returning id
)
insert into dds.wf_settings (entity_name, update_ts)
values ('dm_couriers', current_timestamp at time zone 'utc')
on conflict (entity_name) do update set 
    update_ts = excluded.update_ts
returning entity_name, update_ts