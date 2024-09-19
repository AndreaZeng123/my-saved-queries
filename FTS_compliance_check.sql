SELECT barcode, 
isolation_category_id as seed_category,
asort1 AS event,
CASE
  WHEN seed_treatment IS NULL
  THEN
  'No Microbial Treatment'
  ELSE
 'Treated with Microbials'
  END
  AS microbial_status
          FROM `bcs-breeding-datasets.obs.breeding_midas_inventory`
where barcode='IJN800000879564322938168'
order by 1
--LIMIT 1000
 