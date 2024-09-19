WITH
  all_inventory_sets AS (
    SELECT DISTINCT
      sa.setId,
      ese.entryid,
      ese.sets_material_id AS barcode,
      sa.audit_status,
      sa.audit_time_stamp,
      cr.crops
    FROM
      `bcs-breeding-datasets.velocity.set_audits` sa
    LEFT JOIN
      `bcs-breeding-datasets.velocity.experiment_sets_entries` ese
    ON sa.setId = ese.setid
    JOIN UNNEST(ese.material_fulfillment_groups) AS material_fulfillment_groups
    LEFT JOIN `bcs-breeding-datasets.velocity.capacity_request` cr
    ON cr.capacity_request_id = ese.capacity_request_id
    LEFT JOIN UNNEST(cr.environment) cre
    WHERE
      1=1
      
      AND planting_year = 2024
      AND (material_fulfillment_groups = 'HAZELWOOD-SEED-CENTER'
        OR material_fulfillment_groups = 'HUXLEY-SEED-CENTER')
  ),

  GL_sets AS (
    SELECT DISTINCT 
      setID AS GL_set_id,
      MAX(EXTRACT(Date FROM all_inventory_sets.audit_time_stamp)) AS RTE_date
    FROM all_inventory_sets
    WHERE audit_status = 'Material Fulfillment: Ready to Execute'
    GROUP BY 1
  ),

  pkt_unnest AS (
    SELECT entryid, allocatedInventoryBarcode, filledDate 
    FROM `bcs-csw-core.velocity.packet` pkt,
    UNNEST(pkt.entryids) as entryid
  ),

  entry_filled AS (
    SELECT *, all_inventory_sets.entryid AS entry_id
    FROM all_inventory_sets
    LEFT JOIN pkt_unnest
    ON all_inventory_sets.entryid = pkt_unnest.entryid
  ),

  entry_filled_GL_sets AS (
    SELECT *
    FROM entry_filled 
    LEFT JOIN GL_sets
    ON entry_filled.setId = GL_sets.GL_set_id
  ),

  reqQuan AS (
  SELECT 
  entryid,
  MAX(CAST((CASE WHEN si.question_code = 'PACK_ENT' THEN si.value END) AS numeric)) AS requested_packet,
  MAX(CAST((CASE WHEN si.question_code = 'PACK_ENT' THEN si.value END) AS numeric)) *  
  MAX(CAST((CASE WHEN si.question_code = 'SEEDS_PACK' THEN si.value END) AS numeric)) 
  AS requested_quantity
  FROM `bcs-breeding-datasets.velocity.experiment_sets_entries` ese
  JOIN UNNEST(ese.set_instructions) as si
  GROUP BY 1  
  ),

  entry_barcode AS (
  SELECT es.entryid,CAST(mt.materialid AS numeric) AS barcode
  FROM `bcs-csw-core.velocity.sets`
  JOIN UNNEST(entries) es
  JOIN UNNEST(es.materials) mt
  WHERE mt.producttype = 'inventory'
  ),

  entry_barcode_inv AS (
  SELECT
    eb.entryid,
    eb.barcode,
    inv.quantity,
    inv.uom    
  FROM
    entry_barcode eb
  JOIN
  `bcs-csw-core.velocity.inventory` inv
  ON 
  eb.barcode = inv.barcode
  ),


  combined AS (
  SELECT
    DISTINCT 
    entry_barcode_inv.barcode,
    entry_filled_GL_sets.setid,
    entry_filled_GL_sets.GL_set_id,
    entry_filled_GL_sets.RTE_date,
    EXTRACT(DATE FROM entry_filled_GL_sets.filledDate) AS filledDate,
    entry_filled_GL_sets.entry_id,    
    reqQuan.requested_quantity,
    reqQuan.requested_packet,
    entry_barcode_inv.quantity,
    entry_barcode_inv.uom
  FROM
    entry_filled_GL_sets
  LEFT JOIN 
    reqQuan
  ON 
    entry_filled_GL_sets.entry_id = reqQuan.entryid
  LEFT JOIN
    entry_barcode_inv
  ON
    entry_filled_GL_sets.entry_id = entry_barcode_inv.entryid
  WHERE entry_barcode_inv.barcode IS NOT NULL
  )



SELECT
  CAST(barcode AS numeric) AS inventory_vel_barcode,
  setid AS set_id,
  GL_set_id,
  RTE_date,
  filledDate AS filled_date,
  SUM(CAST(requested_quantity AS numeric)) AS set_requested_quantity,
  SUM(CAST(requested_packet AS numeric)) AS set_requested_packet,
  MIN(CAST(quantity AS numeric)) AS inventory_quantity,
FROM
  combined
GROUP BY
  1,2,3,4,5




