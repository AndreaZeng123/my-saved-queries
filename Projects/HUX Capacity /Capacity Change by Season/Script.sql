-- Sum of packets by crop, by date

-- Set constants
WITH declares AS (
  SELECT
    -- DATE '2024-01-01' AS start_date,
    ARRAY<STRING> ['2022','2023','2024'] AS years,
    ARRAY<STRING> ['HAZ', 'WIB', 'SOG', 'KIE', 'KUI', 'JDZ', 'TLA', 'RAG', 'SIA'] AS sites,
    --2024 AS target_planting_year,
    -- ARRAY<STRING> ['CARMAN-CANOLA-PACKAGING', 'NORLA-BREEDING-CORN-NURSERY-PACKAGING', 'VEG-RD-INVENTORY-STEWARDS-NAM-PAYETTE', 'WILLIAMSBURG-SEED-OPS-PACKAGE'] AS fulfillment_group,
    -- ARRAY<STRING> ['Corn', 'Cotton', 'Wheat', 'Soybean', 'Soybeans', 'Canola', 'OSR-Canola'] AS crops,
    ARRAY<STRING> ['Corn', 'Soybean', 'Soybeans'] AS crops,
    -- ARRAY<STRING> ['Canola', 'OSR-Canola', 'Wheat'] AS non_mactracker_crops
),

-- all dates
date_range AS (
  SELECT DATE AS day
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-12-30', INTERVAL 1 DAY)) AS date
),

-- get all NSP/YTP packets
all_packets AS (
  SELECT DATE(datetime_child_scan, 'America/Chicago') AS date, 
        batch_packet_barcode AS packetBarcode,
        machine_location AS site,
        record_id
  FROM `bcs-breeding-datasets.breeding_operations.mactracker_nsp` AS mactracker_nsp
  WHERE mactracker_nsp.machine_location IN (SELECT sites FROM declares, UNNEST(sites) AS sites)
  AND mactracker_nsp.batch_source_barcode <> 'TOTALIZER'
  AND REGEXP_CONTAINS(batch_packet_barcode, r'^P0|^\d')
  AND CAST(EXTRACT(YEAR FROM mactracker_nsp.datetime_source_scan) AS STRING) IN (SELECT years FROM declares, UNNEST(years) AS years)

  UNION ALL

  SELECT DATE(datetime_child_scan, 'America/Chicago') AS date, 
        batch_packet_barcode AS packetBarcode,
        machine_location AS site,
        record_id
  FROM `bcs-breeding-datasets.breeding_operations.mactracker_ytp` AS mactracker_ytp
  WHERE mactracker_ytp.machine_location IN (SELECT sites FROM declares, UNNEST(sites) AS sites)
  AND mactracker_ytp.batch_source_barcode <> 'TOTALIZER'
  AND REGEXP_CONTAINS(batch_packet_barcode, r'^P0|^\d')
  AND CAST(EXTRACT(YEAR FROM mactracker_ytp.datetime_source_scan) AS STRING) IN (SELECT years FROM declares, UNNEST(years) AS years)
),
  
-- velocity connection
velocity_packets AS (
  WITH vpackets AS (
    SELECT date, packetBarcode, site, entryIds
    FROM `bcs-csw-core.velocity.packet` AS p
    CROSS JOIN UNNEST(p.entryIds) AS entryIds
    INNER JOIN (SELECT date, CAST(packetBarcode AS INT64) AS packetBarcode, site
                FROM all_packets
                WHERE REGEXP_CONTAINS(packetBarcode, r'^\d')) USING (packetBarcode)
  )

  SELECT vpackets.date, COUNT(vpackets.packetBarcode) AS n_packets, cr.crops AS crop, vpackets.site, 'velocity' AS source,
  extract(year from vpackets.date) as year,
  CASE WHEN EXTRACT(MONTH FROM vpackets.date)<7 THEN 'Q1/Q2'
        WHEN EXTRACT(MONTH FROM vpackets.date)>=7 THEN 'Q3/Q4'
      ELSE NULL END AS season
  FROM vpackets
  LEFT JOIN (
  SELECT entryid, capacity_request_id
  FROM `bcs-breeding-datasets.velocity.experiment_sets_entries` 
  ) ese
  ON vpackets.entryIds = ese.entryid
  LEFT JOIN (
    SELECT capacity_request_id, crops --could get material stage, etc. from here
    FROM `bcs-breeding-datasets.velocity.capacity_request`
    WHERE CAST(planting_year AS STRING) IN (SELECT years FROM declares, UNNEST(years) AS years)
  ) cr
  ON ese.capacity_request_id = cr.capacity_request_id
  GROUP BY vpackets.date, cr.crops, vpackets.site
),

-- FTS connection
fts_packets AS (
  SELECT date, 
        COUNT(DISTINCT(record_id)) as n_packets, 
        ops_experiment_entries.crop, 
        site,
        'FTS' AS source,
        extract(year from date) as year,
        CASE WHEN EXTRACT(MONTH FROM date)<7 THEN 'Q1/Q2'
        WHEN EXTRACT(MONTH FROM date)>=7 THEN 'Q3/Q4'
      ELSE NULL END AS season
  FROM all_packets
  INNER JOIN (
    SELECT plot_barcode, planting_year, crop
      FROM `bcs-breeding-datasets.breeding_pipeline.ops_experiment_entries`
      WHERE crop IN (SELECT crops FROM declares, UNNEST(crops) AS crops)
      AND CAST(planting_year AS STRING) IN (SELECT years FROM declares, UNNEST(years) AS years)
  ) AS ops_experiment_entries
  ON all_packets.packetBarcode = ops_experiment_entries.plot_barcode
  GROUP BY date, crop, site
),

all_data AS (
  SELECT * FROM velocity_packets
  UNION ALL
  SELECT * FROM fts_packets
)

SELECT site,year,season,crop,source,SUM(n_packets) as n_packets
  FROM all_data
  WHERE crop IN (SELECT crops FROM declares, UNNEST(crops) AS crops)
  GROUP BY 1,2,3,4,5
  ORDER BY site,season,crop,source,year
