with
log_update_ts(log_update_ts) as (
select update_ts
from cdm.wf_settings
where entity_name = 'dm_settlement_report'
)
, products_sales as (
select 
    dr.restaurant_id restaurant_id 
    , dr.restaurant_name restaurant_name
    , dt.ts dttm
    , date_trunc('day', dt.ts)::date settlement_date
    , dor.id order_id
    , fps.total_sum products_total_sum
    , fps.bonus_payment 
    , fps.bonus_grant 
from log_update_ts, dds.fct_product_sales fps 
join dds.dm_products dp on dp.id = fps.product_id 
join dds.dm_restaurants dr on dr.id = dp.restaurant_id 
join dds.dm_orders dor on dor.id = fps.order_id 
join dds.dm_timestamps dt on dt.id = dor.timestamp_id 
where dor.order_status = 'CLOSED'
    and dt.ts > log_update_ts
    )
, report as (
with report_day_order as (
select 
    restaurant_id
    , restaurant_name
    , settlement_date
    , order_id -- orders_count
    , sum(products_total_sum) orders_total_sum
    , sum(bonus_payment) orders_bonus_payment_sum
    , sum(bonus_grant) orders_bonus_granted_sum
    , sum(products_total_sum)*0.25 order_processing_fee
    , sum(products_total_sum)*0.75 - sum(bonus_payment) restaurant_reward_sum
from products_sales
group by restaurant_id, restaurant_name, settlement_date, order_id
)
select 
    restaurant_id
    , restaurant_name
    , settlement_date
    , count(order_id) orders_count
    , sum(orders_total_sum) orders_total_sum
    , sum(orders_bonus_payment_sum) orders_bonus_payment_sum
    , sum(orders_bonus_granted_sum) orders_bonus_granted_sum
    , sum(orders_total_sum)*0.25 order_processing_fee
    , sum(orders_total_sum)*0.75 - sum(orders_bonus_payment_sum) restaurant_reward_sum
from report_day_order
group by restaurant_id, restaurant_name, settlement_date
)
, insert_report as (
insert into cdm.dm_settlement_report as r (
restaurant_id, restaurant_name, settlement_date, orders_count,
orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
select restaurant_id, restaurant_name, settlement_date, orders_count,
orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum
from report
on conflict (restaurant_id, settlement_date) do update set 
    orders_count = r.orders_count + excluded.orders_count
    , orders_total_sum = r.orders_total_sum + excluded.orders_total_sum
    , orders_bonus_payment_sum = r.orders_bonus_payment_sum + excluded.orders_bonus_payment_sum 
    , order_processing_fee = r.order_processing_fee + excluded.order_processing_fee
    , restaurant_reward_sum = r.restaurant_reward_sum + excluded.restaurant_reward_sum
returning restaurant_id
)
insert into cdm.wf_settings (entity_name, update_ts)
select 
    'dm_settlement_report'
    , dttm update_ts
from insert_report ir
join products_sales ps on ps.restaurant_id = ir.restaurant_id
order by dttm desc 
limit 1
on conflict (entity_name) do update set 
update_ts = excluded.update_ts
returning entity_name, update_ts