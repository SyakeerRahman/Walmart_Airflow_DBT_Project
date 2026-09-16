-- Employees come straight from the silver technical layer, not from obt_b.
--
-- An order has no employee. The orders table carries customer_id and store_id
-- only, so a join through obt_b would attach every employee of the store to
-- every order. The column names keep the employee_ prefix, because
-- snapshots/dim_employees.yml reads employee_updated_timestamp.

SELECT
    employee_id,
    store_id,
    first_name AS employee_first_name,
    last_name AS employee_last_name,
    email AS employee_email,
    job_title,
    salary,
    created_timestamp AS employee_created_timestamp,
    updated_timestamp AS employee_updated_timestamp,
    is_active AS employee_is_active,
    processed_at AS employee_processed_at,
    CURRENT_TIMESTAMP() AS employee_gold_processed_at
FROM
    {{ ref('employees_t') }}
