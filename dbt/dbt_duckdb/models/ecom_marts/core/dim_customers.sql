{{ config(
    materialized='view',
    tags=['marts', 'dimensions']
) }}

SELECT
    -- Customer Base Info
    c.customer_id,
    c.email,
    c.first_name,
    c.last_name,
    c.age,
    c.gender,
    c.annual_income,
    -- Location Info Denormalized
    l.city,
    l.state,
    l.country,
    -- Type Fields Denormalized
    et.education_type,
    ms.status_type AS marital_status,
    -- Metrics
    COALESCE(o.total_orders, 0) AS total_orders,
    COALESCE(o.total_spent, 0) AS total_spent,
    COALESCE(o.total_spent / NULLIF(o.total_orders, 0), 0) AS avg_order_value,
    o.first_order_date,
    o.last_order_date,
    -- Derived Fields
    CASE 
        WHEN o.last_order_date >= CURRENT_DATE - INTERVAL '3 months' THEN 'Active'
        WHEN o.last_order_date >= CURRENT_DATE - INTERVAL '6 months' THEN 'At Risk'
        ELSE 'Churned'
    END AS customer_status,
    DATE_DIFF('day', COALESCE(o.first_order_date, c.signup_date), CURRENT_DATE) AS customer_lifetime_days,
    -- Additional Info
    c.signup_date,
    c.last_login,
    c.is_active,
    c.created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ source('ecom_intermediate', 'customers_enriched') }} c
LEFT JOIN {{ source('ecom_intermediate', 'locations') }} l 
    ON c.location_id = l.location_id
LEFT JOIN {{ source('ecom_intermediate', 'education_types') }} et 
    ON c.education_id = et.education_id
LEFT JOIN {{ source('ecom_intermediate', 'marital_statuses') }} ms 
    ON c.marital_status_id = ms.marital_status_id
LEFT JOIN (
    SELECT 
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_amount) AS total_spent,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date
    FROM {{ source('ecom_intermediate', 'orders') }}
    GROUP BY customer_id
) o 
    ON c.customer_id = o.customer_id
