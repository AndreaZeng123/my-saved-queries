-- Set constants
WITH declares AS (
  SELECT
    DATE '2024-01-01' AS start_date,
    ARRAY<STRING> ['WIB'] AS sites,
    ARRAY<STRING> ['WILLIAMSBURG-SEED-OPS-PACKAGE'] AS fulfillment_group,
),
 
find_fts_code AS (
  SELECT
    inv.barcode,
    CASE
      WHEN matexc.sender_inv_barcode IS NOT NULL THEN sender_inv_barcode
      ELSE matexc.receiver_inv_barcode
  END
    AS fts_code
  FROM
    `bcs-breeding-datasets.velocity.inventory` inv
  JOIN
    `bcs-csw-core.exadata.midas_material_exchange` matexc
  ON
    matexc.receiver_inv_barcode = inv.legacy_barcode
    OR matexc.sender_inv_barcode = inv.legacy_barcode),
 
entry_packet_date AS (
 
SELECT
entryid,
MIN(filledDate) AS start_date,
MAX(filledDate) AS end_date
FROM `bcs-csw-core.velocity.packet` pkt
CROSS JOIN UNNEST(pkt.entryids) AS entryid
WHERE fillStation LIKE CONCAT((SELECT site FROM declares, UNNEST(sites) AS site), '%')
AND EXTRACT(date FROM filledDate) >= (SELECT start_date FROM declares)
GROUP BY entryid
ORDER BY entryid),
 
inv_process_date AS (
  SELECT
  DISTINCT s3.inv_bid,
  inv.legacy_barcode,
  find_fts_code.fts_code,
  COALESCE(inv.barcode,find_fts_code.barcode) AS barcode,
  datetime_start,
  datetime_finish
FROM
  `bcs-breeding-datasets.breeding_operations.mactracker_shellmatic_3` s3
JOIN
  `bcs-breeding-datasets.velocity.inventory` inv
ON
  s3.inv_bid = inv.legacy_barcode
JOIN
  find_fts_code
ON
  find_fts_code.fts_code = s3.inv_bid
WHERE machine_location IN (SELECT site FROM declares, UNNEST(sites) AS site)
AND EXTRACT(DATE FROM datetime_start) >= (SELECT start_date FROM declares)
)
 
SELECT ese.setid,
ese.experiment_id,
MIN(inv_process_date.datetime_start) AS process_start,
MAX(inv_process_date.datetime_finish) AS process_end,
MAX(inv_process_date.datetime_start) AS inventory_available,
MIN(entry_packet_date.start_date) AS packet_fill_start,
MAX(entry_packet_date.end_date) AS packet_fill_end
FROM `bcs-breeding-datasets.velocity.experiment_sets_entries` ese
JOIN inv_process_date
ON inv_process_date.barcode = ese.sets_material_id
JOIN entry_packet_date
ON entry_packet_date.entryid = ese.entryid
CROSS JOIN UNNEST(ese.material_fulfillment_groups) AS material_fulfillment_group
WHERE material_fulfillment_group IN (SELECT fulfillment_group FROM declares, UNNEST(fulfillment_group) AS fulfillment_group)
GROUP BY ese.setid,ese.experiment_id
