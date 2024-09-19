WITH inv_request AS (
  SELECT
    inventory_vel_barcode,
    SUM(set_requested_quantity) AS inventory_total_requested,
    MIN(inventory_quantity) AS inventory_quantity,
    (MIN(inventory_quantity) - SUM(set_requested_quantity)) AS seed_difference
  FROM
    `bcs-csp-analytics.dataform.greenlight_material_seed_check_raw`
  GROUP BY 1
),

current_request_inventory AS (
  SELECT 
  inventory_vel_barcode, 
  set_id,
  CASE WHEN GL_set_id IS NOT NULL THEN 'GL' ELSE 'Not GL' END AS GL_or_not,
  RTE_date,
  CASE WHEN filled_date IS NULL THEN 'Not Filled' END AS Filled_or_not,
  set_requested_quantity,
  inventory_quantity
  FROM `bcs-csp-analytics.dataform.greenlight_material_seed_check_raw`
  WHERE (GL_set_id IS NOT NULL AND filled_date IS NULL)
  OR (GL_set_id IS NULL AND filled_date IS NULL)
  ORDER BY 1,2
)

SELECT 
current_request_inventory.inventory_vel_barcode,
ARRAY_AGG(STRUCT(set_id,GL_or_not,RTE_date,Filled_or_not,set_requested_quantity)) AS sets,
MIN(inventory_total_requested) AS inventory_total_requested,
SUM(set_requested_quantity) AS inventory_current_requested,
MIN(current_request_inventory.inventory_quantity) AS inventory_quantity
FROM current_request_inventory
JOIN inv_request
ON current_request_inventory.inventory_vel_barcode = inv_request.inventory_vel_barcode
GROUP BY 1





