WITH GL_Filled AS (
  SELECT 
  VCR,
  experiment_name,
  experiment_id,
  inventory_vel_barcode, 
  set_id,
  CASE WHEN GL_set_id IS NOT NULL THEN 'GL' ELSE 'Not GL' END AS GL_or_not,
  RTE_date,
  CASE WHEN filled_date IS NOT NULL THEN 'Filled' ELSE 'Not Filled' END AS Filled_or_not,
  set_requested_quantity,
  set_requested_packet,
  inventory_quantity
  FROM `bcs-csp-analytics.dataform.greenlight_experiment_seed_check_raw`
  ORDER BY experiment_id,inventory_vel_barcode),

  GL_exp AS (
    SELECT experiment_id,
    MAX(RTE_date) AS RTE_date
    FROM GL_Filled
    WHERE GL_or_not = 'GL'
    GROUP BY 1
  ),

  group_exp_inv AS (
    SELECT VCR,
    experiment_name,
    experiment_id,
    inventory_vel_barcode,
    SUM(CASE WHEN GL_or_not = 'GL' AND Filled_or_not = 'Filled' THEN set_requested_quantity ELSE 0 END) AS GL_filled_requested_seed,
    SUM(CASE WHEN GL_or_not = 'GL' AND Filled_or_not = 'Filled' THEN set_requested_packet ELSE 0 END) AS GL_filled_requested_packet,
    SUM(CASE WHEN GL_or_not = 'GL' AND Filled_or_not = 'Not Filled' THEN set_requested_quantity ELSE 0 END) AS GL_notfilled_requested_seed,
    SUM(CASE WHEN GL_or_not = 'GL' AND Filled_or_not = 'Not Filled' THEN set_requested_packet ELSE 0 END) AS GL_notfilled_requested_packet,
    SUM(CASE WHEN GL_or_not = 'Not GL' AND Filled_or_not = 'Not Filled' THEN set_requested_quantity ELSE 0 END) AS notGL_notfilled_requested_seed,
    SUM(CASE WHEN GL_or_not = 'Not GL' AND Filled_or_not = 'Not Filled' THEN set_requested_packet ELSE 0 END) AS notGL_notfilled_requested_packet,
    MIN(inventory_quantity) AS inventory_quantity
    FROM GL_Filled
    GROUP BY 1,2,3,4
 )

SELECT 
group_exp_inv.VCR,
group_exp_inv.experiment_name,
GL_exp.experiment_id,
GL_exp.RTE_date,
group_exp_inv.inventory_vel_barcode,
group_exp_inv.GL_notfilled_requested_seed,
group_exp_inv.GL_notfilled_requested_packet,
group_exp_inv.inventory_quantity,
CASE 
  WHEN group_exp_inv.inventory_quantity >= group_exp_inv.GL_notfilled_requested_seed THEN 'True' ELSE 'False' END AS enough_seed_for_GL,
CASE 
  WHEN group_exp_inv.inventory_quantity >= (group_exp_inv.GL_notfilled_requested_seed+notGL_notfilled_requested_seed) THEN 'True' ELSE 'False' END AS enough_seed_for_all
FROM GL_exp
LEFT JOIN group_exp_inv
ON GL_exp.experiment_id = group_exp_inv.experiment_id
ORDER BY 
  RTE_date DESC,
  experiment_id,
  inventory_vel_barcode


 