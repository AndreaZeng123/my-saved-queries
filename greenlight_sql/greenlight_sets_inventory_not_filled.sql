WITH
  set_RTE AS (
    SELECT
    sa.setId,
    ese.capacity_request_id AS VCR,
    ese.experiment_name,
    ese.experiment_id,
    ese.entryid,
    cre.homesite_name,
    sg.site_name,
    sg.field_name,
    cr.crops,
    MAX(EXTRACT(Date FROM sa.audit_time_stamp)) AS RTE_date
  FROM
    `bcs-breeding-datasets.velocity.set_audits` sa
  LEFT JOIN
    `bcs-breeding-datasets.velocity.experiment_sets_entries` ese
  ON sa.setId = ese.setid
  JOIN UNNEST(ese.material_fulfillment_groups) AS material_fulfillment_groups
  LEFT JOIN `bcs-breeding-datasets.velocity.capacity_request` cr
  ON cr.capacity_request_id = ese.capacity_request_id
  LEFT JOIN UNNEST(cr.environment) cre
  LEFT JOIN `bcs-breeding-datasets.velocity.sets_geography` sg
  ON sg.entryid = ese.entryid
  WHERE
    --sa.audit_status = 'Mapping: Ready to Execute'
    sa.audit_status = 'Material Fulfillment: Ready to Execute'
    AND EXTRACT(DATE FROM sa.audit_time_stamp) >= '2024-07-31'
    AND (material_fulfillment_groups = 'HAZELWOOD-SEED-CENTER'
      OR material_fulfillment_groups = 'HUXLEY-SEED-CENTER')
  GROUP BY
    sa.setId,
    ese.capacity_request_id,
    ese.experiment_name,
    ese.experiment_id,
    ese.entryid,
    cre.homesite_name,
    sg.site_name,
    sg.field_name,
    cr.crops),
  
  pkt_unnest AS (
    SELECT entryid, allocatedInventoryBarcode, filledDate 
    FROM `bcs-csw-core.velocity.packet` pkt,
    UNNEST(pkt.entryids) as entryid
  ),

  entry_filled AS (
    SELECT *, set_RTE.entryid AS entry_id
    FROM set_RTE
    LEFT JOIN pkt_unnest
    ON set_RTE.entryid = pkt_unnest.entryid
  )

SELECT DISTINCT
setid AS set_id,
allocatedInventoryBarcode AS inventory_not_filled,
VCR,
experiment_name,
experiment_id,
homesite_name,
site_name,
field_name,
crops AS crop
FROM entry_filled 
WHERE setid IN (
  SELECT set_id
  FROM `bcs-csp-analytics.dataform.greenlight_sets` 
  WHERE filled_pct > 90 
  AND filled_pct < 100
)
AND filledDate IS NULL
