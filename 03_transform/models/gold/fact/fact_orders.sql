-- One row for each order item.
--
-- There is no employee_id column. An order has no employee in this schema, so a
-- key for dim_employees does not belong at this grain. See obt_b.sql.

SELECT
    order_id,
    order_item_id,
    product_id,
    store_id,
    customer_id,
    total_amount,
    quantity,
    unit_price,
    line_amount
FROM
    {{ ref('obt_b') }}
