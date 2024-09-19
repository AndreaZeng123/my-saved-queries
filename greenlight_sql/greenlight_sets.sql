WITH
  set_RTE AS (
    SELECT
    sa.setId,
    ese.capacity_request_id AS VCR,
    ese.experiment_name,
    ese.experiment_id,
    ese.entryid,
    COALESCE(sg.homesite_name, cre.homesite_name) AS homesite_name,
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
    AND EXTRACT(Date FROM sa.audit_time_stamp) >= '2024-07-31'
    AND (material_fulfillment_groups = 'HAZELWOOD-SEED-CENTER' --from ese unnest
      OR material_fulfillment_groups = 'HUXLEY-SEED-CENTER')
  GROUP BY
    sa.setId,
    ese.capacity_request_id,
    ese.experiment_name,
    ese.experiment_id,
    ese.entryid,
    homesite_name,
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


    SELECT
      setId AS set_id,
      VCR,
      experiment_name,
      experiment_id,
      RTE_date,
      homesite_name,
      site_name,
      field_name,
      crops AS crop,
      COUNT(entry_id) AS total_entries,
      SUM(CASE WHEN filledDate IS NOT NULL THEN 1 ELSE 0 END) AS filled_entries,
      CASE
        WHEN (SUM(CASE WHEN filledDate IS NOT NULL THEN 1 ELSE 0 END)*100.0 / COUNT(entry_id)) = 0 THEN "Not Start"
        WHEN (SUM(CASE WHEN filledDate IS NOT NULL THEN 1 ELSE 0 END)*100.0 / COUNT(entry_id)) > 0
        AND (SUM(CASE WHEN filledDate IS NOT NULL THEN 1 ELSE 0 END)*100.0 / COUNT(entry_id)) < 100 THEN "In Progress"
        WHEN (SUM(CASE WHEN filledDate IS NOT NULL THEN 1 ELSE 0 END)*100.0 / COUNT(entry_id)) = 100 THEN "Completed"
        END AS fill_status,
      
      ROUND((SUM(CASE WHEN filledDate IS NOT NULL THEN 1 ELSE 0 END)*100.0 / COUNT(entry_id)),2) AS filled_pct

    FROM
      entry_filled
    
    GROUP BY 1,2,3,4,5,6,7,8,9
    ORDER BY 
      RTE_date DESC,
      experiment_id,
      setid 


