-- CREATE TABLE IF NOT EXISTS dim_date AS
SELECT ROW_NUMBER() OVER (ORDER BY o.date) AS id, o.date, EXTRACT(MONTH FROM o.date) as month, EXTRACT(YEAR FROM o.date) AS year, 
CASE
	WHEN EXTRACT(MONTH from o.date) in (1,2,3) then 1
    WHEN EXTRACT(MONTH from o.date) in (4,5,6) then 2
    WHEN EXTRACT(MONTH from o.date) in (7,8,9) then 3
    WHEN EXTRACT(MONTH from o.date) in (10,11,12) then 4    
end as quarter_of_year,
case
	WHEN EXTRACT(isodow from o.date) IN (6,7) then TRUE
    else FALSE
end as is_weekend
from orders o 
left join invoices i on o.order_number = i.order_number
left join payments pm on pm.invoice_number = i.invoice_number