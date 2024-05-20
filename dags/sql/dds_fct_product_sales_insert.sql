with log_update_ts(log_update_ts) as (
select update_ts
from dds.wf_settings 
where entity_name = 'fct_product_sales'
)
, order_product_payment as (
select 
    event_value::jsonb->>'order_id' order_key
    , jsonb_array_elements(
        event_value::jsonb->'product_payments'
        )->>'product_id' product_id
    , (jsonb_array_elements(
        event_value::jsonb->'product_payments'
        )->>'price')::numeric price
    , (jsonb_array_elements(
        event_value::jsonb->'product_payments'
        )->>'quantity')::int "count"
    , (jsonb_array_elements(
        event_value::jsonb->'product_payments'
        )->>'bonus_payment')::numeric  bonus_payment
    , (jsonb_array_elements(
        event_value::jsonb->'product_payments'
        )->>'bonus_grant')::numeric  bonus_grant
    , event_ts
from log_update_ts luts, stg.bonussystem_events be 
where event_type = 'bonus_transaction'
    and be.event_ts > luts.log_update_ts
)
, stg_product_sales as (
select 
    o.id order_id
    , p.id product_id
    , opp."count"
    , opp.price
    , opp."count" * opp.price total_sum
    , opp.bonus_payment
    , opp.bonus_grant
    , event_ts
from dds.dm_orders o
join order_product_payment opp using(order_key)
join dds.dm_products p using(product_id)
)
, insert_fct_product_sales as (
insert into dds.fct_product_sales (
order_id, product_id , "count" , price , total_sum ,
bonus_payment , bonus_grant )
select 
order_id, product_id , "count" , price , total_sum ,
bonus_payment , bonus_grant
from stg_product_sales
on conflict do nothing
returning product_id
)
insert into dds.wf_settings (entity_name, update_ts)
select 
    'fct_product_sales'
    , event_ts
from insert_fct_product_sales fps
join stg_product_sales sps on sps.product_id = fps.product_id
order by event_ts desc 
limit 1
on conflict (entity_name) do update set 
update_ts = excluded.update_ts
returning entity_name, update_ts