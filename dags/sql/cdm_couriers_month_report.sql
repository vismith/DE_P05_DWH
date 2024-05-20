with report_deliveries as (
select 
    ts 
    , courier_id 
    , year settlement_year
    , month settlement_month
    , order_id
    , rate
    , tip_sum
from dds.fct_deliveries fd 
left join dds.dm_orders dos on fd.order_id = dos.id 
left join dds.dm_timestamps dt on dt.id = dos.timestamp_id 
where year = %(year)s 
    and month = %(month)s 
    and order_status = 'CLOSED'
)
, orders_total_sum as (
select 
    order_id
    , sum(total_sum) order_total_sum 
from report_deliveries rdls
left join dds.fct_product_sales fps using(order_id)
group by order_id
)
, courier_counts as (
select 
    courier_id
    , settlement_year
    , settlement_month
    , count(order_id) orders_count
    , avg(rate) rate_avg
    , sum(tip_sum) courier_tips_sum
    , sum(order_total_sum) orders_total_sum
    , sum(order_total_sum)*0.25 order_processing_fee
from report_deliveries 
join orders_total_sum using(order_id)
group by courier_id, settlement_year, settlement_month
)
, courier_order_sum as (
select 
    courier_id
    , settlement_year
    , settlement_month
    , (case 
        when rate_avg < 4 then greatest(orders_total_sum*0.05, 100)
        when rate_avg >= 4 and rate_avg < 4.5 then greatest(orders_total_sum*0.07, 150)
        when rate_avg >= 4.5 and rate_avg < 4.9 then greatest(orders_total_sum*0.08, 175)
        else greatest(orders_total_sum*0.1, 200)
        end)::numeric courier_order_sum
from courier_counts
)
, couriers_upsert as (
insert into cdm.dm_courier_ledger as dcl (
    courier_id
    , courier_name
    , settlement_year
    , settlement_month
    , orders_count
    , orders_total_sum
    , rate_avg
    , order_processing_fee
    , courier_order_sum
    , courier_tips_sum
    , courier_reward_sum
)
select 
    dc.courier_id courier_id
    , dc.courier_name courier_name
    , settlement_year
    , settlement_month
    , orders_count
    , orders_total_sum
    , rate_avg
    , order_processing_fee
    , courier_order_sum
    , courier_tips_sum
    , courier_order_sum + courier_tips_sum*0.95 courier_reward_sum
from courier_counts cc
join courier_order_sum cos using(courier_id, settlement_year, settlement_month)
left join dds.dm_couriers dc on dc.id = cc.courier_id
on conflict (courier_id, settlement_year, settlement_month) do update set
    orders_count = excluded.orders_count
    , orders_total_sum = excluded.orders_total_sum
    , rate_avg = excluded.rate_avg
    , order_processing_fee = excluded.order_processing_fee
    , courier_order_sum = excluded.courier_order_sum
    , courier_tips_sum = excluded.courier_tips_sum
    , courier_reward_sum = excluded.courier_reward_sum
returning id
)
insert into cdm.wf_settings (entity_name, update_ts)
values ('dm_courier_ledger', current_timestamp)
on conflict (entity_name) do update set
    update_ts = excluded.update_ts
returning entity_name, update_ts