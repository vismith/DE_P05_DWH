with log_update_ts(log_update_ts) as (
select update_ts
from dds.wf_settings
where entity_name = 'dm_products'
)
, stg_products as (
with restaurants as (
select 
    object_value::jsonb->>'_id' restaurant_id
    , object_value::jsonb->'menu' menu
    , (object_value::jsonb->>'update_ts')::timestamp update_ts
    , log_update_ts
from stg.ordersystem_restaurants osr, log_update_ts luts
where (object_value::jsonb->>'update_ts')::timestamp > luts.log_update_ts
)
, products as (
select 
    restaurant_id
    , product->>'_id' product_id
    , product->>'name' product_name
    , (product->'price')::numeric product_price
    , update_ts
from (
    select 
        restaurant_id
        , update_ts
        , jsonb_array_elements(menu) 
    from restaurants
    ) as products(restaurant_id, update_ts, product)
)
select 
    dr.id restaurant_id
    , product_id
    , product_name
    , product_price
    , ps.update_ts active_from
    , '2099-12-31'::timestamp active_to
from products ps
join dds.dm_restaurants dr using(restaurant_id)
)
, insert_dm_products as (
insert into dds.dm_products (restaurant_id,
    product_id, product_name, product_price,
    active_from, active_to)
select 
    restaurant_id,
    product_id, product_name, product_price,
    active_from, active_to
from stg_products
on conflict (restaurant_id, product_id) do update set 
    product_name = excluded.product_name
    , product_price = excluded.product_price
    , active_from = excluded.active_from
returning active_from
)
insert into dds.wf_settings (entity_name, update_ts)
select 
    'dm_products'
    , active_from
from insert_dm_products
order by active_from desc
limit 1
on conflict (entity_name) do update set 
update_ts = excluded.update_ts
returning entity_name, update_ts