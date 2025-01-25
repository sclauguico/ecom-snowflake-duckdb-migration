{{ config(
    materialized='view',
    tags=['marts', 'dimensions']
) }}


WITH category_hierarchy AS (
    SELECT DISTINCT
        c.category_id,
        c.category_name,
        STRING_AGG(DISTINCT s.subcategory_name, ', ' ORDER BY s.subcategory_name) as subcategories
    FROM {{ source('ecom_intermediate', 'categories_enriched') }} c
    LEFT JOIN {{ source('ecom_intermediate', 'subcategories_enriched') }} s 
        USING (category_id)
    GROUP BY 
        c.category_id,
        c.category_name
)

SELECT
    c.category_id,
    c.category_name,
    ch.subcategories,
    c.created_at
FROM {{ source('ecom_intermediate', 'categories_enriched') }} c
LEFT JOIN category_hierarchy ch 
    USING (category_id)