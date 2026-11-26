-- The shop's order volume per day (#80 point 8).
CREATE MATERIALIZED VIEW daily_orders AS
SELECT order_date, count(*) AS orders, sum(quantity) AS units
FROM bronze.shop.order_lines
GROUP BY order_date;
