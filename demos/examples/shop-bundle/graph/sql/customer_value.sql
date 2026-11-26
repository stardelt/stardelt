-- The shop's customers by country and segment (#80 point 8).
CREATE MATERIALIZED VIEW customer_value AS
SELECT c.country, c.segment, count(*) AS orders, sum(o.quantity) AS units
FROM bronze.shop.order_lines o
JOIN bronze.shop.customers c ON c.id = o.customer_id
GROUP BY c.country, c.segment;
