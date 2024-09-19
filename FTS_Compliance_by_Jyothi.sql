/* Formatted on 12/11/2016 12:17:27 PM (QP5 v5.215.12089.38647) */
SELECT DISTINCT
       I.barcode AS BARCODE,
       I.ISOLATION_CATEGORY_ID AS "SEED_CATEGORY",
       CASE
          WHEN microbial_inventories.treated_inventory IS NULL
          THEN
             'No Microbial Treatment'
          ELSE
             'Treated with Microbials'
       END
          AS MICROBIAL_STATUS,
          MICROBIAL_INVENTORIES.CHEM_NAME as "CHEM_NAME",
       CASE
          WHEN G.EVENT_DISPLAY IS NULL THEN 'No Event'
          ELSE g.event_display
       END
          AS EVENT_DISPLAY,
          'United States of America' as GROWING_COUNTRY
  FROM MIDAS.INVENTORY i,
       MIDAS.genetic_material gm,
       midas.germplasm g,
       (SELECT DISTINCT I.BARCODE AS TREATED_INVENTORY,
                        P.BARCODE AS PARENT_INVENTORY,
                        CP.SUPPLIER_PROD_NBR,
                        CN.CHEM_NAME
          FROM MIDAS.TREATMENT T,
               MIDAS.APPLICATION_GROUP_TREATMENT AGT,
               MIDAS.APPLICATION_GROUP AG,
               MIDAS.INV_CNT_APPL_GROUP ICAG,
               MIDAS.INVENTORY_CONTAINER IC,
               MIDAS.INVENTORY I,
               MIDAS.PARENT_INVENTORY PI,
               MIDAS.INVENTORY P,
               MATERIAL.CHEM_NAME CN,
               MATERIAL.CHEM_PRODUCT CP,
               MATERIAL.CHEM_LOT CL
         WHERE     T.TREATMENT_ID = AGT.TREATMENT_ID
               AND AGT.APPLICATION_GROUP_ID = AG.APPLICATION_GROUP_ID
               AND AG.APPLICATION_GROUP_ID = ICAG.APPLICATION_GROUP_ID
               AND ICAG.INVENTORY_CONTAINER_ID = IC.INVENTORY_CONTAINER_ID
               AND IC.INVENTORY_ID = I.INVENTORY_ID
               AND PI.INVENTORY_ID = I.INVENTORY_ID
               AND P.INVENTORY_ID = PI.PARENTAL_INVENTORY_ID
               AND CN.CHEM_NAME_ID = T.CHEMICAL_NAME_IDENTIFIER
               AND CP.PROD_ID = CN.PROD_ID
               AND CN.CHEM_NAME_ID = CL.CHEM_NAME_ID
               AND (CN.CHEM_NAME LIKE 'MON 20%' OR CN.CHEM_NAME LIKE 'MON 21%')
union
               select distinct inventory.barcode as TREATED_INVENTORY, 'N/A' as PARENT_INVENTORY, 'N/A' as CUPPLIER_PROD_NBR
,NVL(Substr( inventory.asort1, instr(inventory.asort1,'MON 20') , 10),Substr( inventory.asort1, instr(inventory.asort1,'MON 21') , 10)) as CHEM_NAME
               from midas.inventory, midas.crop, midas.br_prog
               where inventory.ASORT1 is not null
               and (inventory.asort1 like '%MON 20%' OR inventory.asort1 like '%MON 21%')
               and inventory.br_prog_id = br_prog.br_prog_id
               and BR_PROG.CROP_ID = crop.crop_id
               and crop.name = 'Wheat') MICROBIAL_INVENTORIES
WHERE     I.GENETIC_MATERIAL_ID = GM.GENETIC_MATERIAL_ID(+)
       AND I.BARCODE = MICROBIAL_INVENTORIES.TREATED_INVENTORY(+)
       AND GM.GERMPLASM_ID = g.germplasm_id(+)
       AND i.barcode = ?