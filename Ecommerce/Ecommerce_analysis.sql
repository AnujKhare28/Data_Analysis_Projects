create database ecommerce;

select distinct customer_city from ecommerce.customers;

select count(order_id) from ecommerce.orders where year(order_purchase_timestamp)=2017;

select ecommerce.products.product_category category, round(sum(ecommerce.payments.payment_value),2) sales
from ecommerce.products join ecommerce.order_items 
on products.product_id = order_items.product_id
join ecommerce.payments
on payments.order_id = order_items.order_id
group by category;

select (sum(case when payment_installments >= 1 then 1 else 0 end))/count(*)*100 
from ecommerce.payments;

select customer_state, count(customer_id)
from ecommerce.customers group by customer_state;

select monthname(order_purchase_timestamp) months, count(order_id) order_count
from ecommerce.orders where year(order_purchase_timestamp) = 2018
group by months;



WITH count_per_order AS (
    SELECT 
        o.order_id, 
        o.customer_id, 
        COUNT(oi.order_id) AS oc
    FROM ecommerce.orders AS o
    JOIN ecommerce.order_items AS oi
        ON o.order_id = oi.order_id
    GROUP BY 
        o.order_id, 
        o.customer_id
)
SELECT 
    c.customer_city, 
    ROUND(AVG(cpo.oc),2) AS avg_items_per_order
FROM ecommerce.customers AS c
JOIN count_per_order AS cpo
    ON c.customer_id = cpo.customer_id
GROUP BY 
    c.customer_city;
    
    
    
SELECT 
    p.product_category AS category,
    ROUND(
        (SUM(pay.payment_value) / 
         (SELECT SUM(payment_value) FROM ecommerce.payments)
        ) * 100, 2
    ) AS sales_percentage
FROM ecommerce.products AS p
JOIN ecommerce.order_items AS oi 
    ON p.product_id = oi.product_id
JOIN ecommerce.payments AS pay
    ON pay.order_id = oi.order_id
GROUP BY p.product_category
ORDER BY sales_percentage DESC;

