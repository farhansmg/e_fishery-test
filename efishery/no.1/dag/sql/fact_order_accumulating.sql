-- CREATE TABLE IF NOT EXISTS fact_order_accumulating as
SELECT
dd.id as order_date_id,
dd.id as invoice_date_id,
dd.id as payment_date_id,
dc.id as customer_id,
ol.order_number,
i.invoice_number,
pm.payment_number,
sum(ol.quantity) as total_order_quantity,
sum(ol.usd_amount)as total_order_usd_amount,
(i.date - o.date) as order_to_invoice_lag_days,
(pm.date - i.date) as invoice_to_payment_lag_days
FROM order_lines ol
left join orders o on ol.order_number = o.order_number
LEFT join dim_date dd on o.date = dd.date
left join dim_customer dc on o.customer_id = dc.id
left join invoices i on i.order_number = o.order_number
LEFT join payments pm on pm.invoice_number = i.invoice_number
group by dd.id, dc.id, ol.order_number, i.invoice_number, pm.payment_number, o.date