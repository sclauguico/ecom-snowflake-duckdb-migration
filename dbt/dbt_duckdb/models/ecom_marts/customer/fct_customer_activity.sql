{{ config(
    materialized='incremental',
    tags=['marts', 'customers']
) }}

SELECT
    c.customer_id,
    c.email,
    DATE_TRUNC('month', CAST(i.event_date AS DATE)) AS activity_month,
    
    -- Page Views
    COUNT(CASE WHEN i.event_type = 'view' THEN 1 END) AS total_views,
    COUNT(DISTINCT CASE WHEN i.event_type = 'view' THEN i.product_id END) AS unique_products_viewed,

    -- Cart Activity 
    COUNT(CASE WHEN i.event_type = 'cart_add' THEN 1 END) AS cart_adds,
    COUNT(DISTINCT CASE WHEN i.event_type = 'cart_add' THEN i.product_id END) AS unique_products_added,

    -- Purchase Activity
    COUNT(CASE WHEN i.event_type = 'purchase' THEN 1 END) AS purchases,
    COUNT(DISTINCT CASE WHEN i.event_type = 'purchase' THEN i.product_id END) AS unique_products_purchased,

    -- Session Info
    COUNT(DISTINCT i.session_id) AS total_sessions,
    COUNT(DISTINCT i.device_type) AS devices_used,

    -- Timestamps
    CURRENT_TIMESTAMP AS updated_at
FROM {{ source('ecom_intermediate', 'customers_enriched') }} c
LEFT JOIN {{ source('ecom_intermediate', 'customer_interactions') }} i 
    ON c.customer_id = i.customer_id
WHERE CAST(i.event_date AS DATE) IS NOT NULL
GROUP BY 
    c.customer_id, 
    c.email, 
    DATE_TRUNC('month', CAST(i.event_date AS DATE))