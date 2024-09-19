with plot_data as (
select distinct germ.legacy_attributes.pedigree,
germ.legacy_attributes.origin AS origin,
rep.tracking_id,
ts.crop_name,
ts.set_name,
inv.legacy_attributes.barcode AS barcode,
plot.plot_barcode  as plot_barcode,
rep.repetition_number,
rep.midas_field_id,
rep.originator_program,
rep.grower_program,
  fields.grower_name,
  plot.absolute_range,
  plot.absolute_column,
  plot.plot_number,
  ent.entry_num,
  germ.legacy_attributes.source AS source,
  ts.regulated_type,
  observation_ref_cd,
  coalesce (cast(dsr_value as string), cast(num_value as string), cast(str_value as string), cast(date_value as string) ,null) as value
FROM`product360-datasets.performance.TestSetsV1` ts
INNER JOIN`product360-datasets.performance.RepetitionsV1` rep
ON ts.midas_test_set_id = rep.midas_test_set_id
INNER JOIN`product360-datasets.performance.PlotsV1` plot
ON plot.midas_repetition_id = rep.midas_repetition_id
LEFT JOIN`product360-datasets.pipeline.InventoriesV1` inv
ON inv.legacy_attributes.inventory_id = plot.midas_inventory_id
LEFT JOIN`product360-datasets.pipeline.GermplasmV1` germ
ON germ.legacy_attributes.genetic_material_id = inv.legacy_attributes.genetic_material_id
left join product360-datasets.performance.FieldsV1 fields
on   fields.midas_field_id = rep.midas_field_id
LEFT JOIN`product360-datasets.performance.EntriesV1` ent
ON ent.midas_test_set_entry_id = plot.midas_entry_id
left join `product360-datasets.performance.PlotObservationsV1` po on po.midas_plot_id= plot.midas_plot_id
where germ.legacy_attributes.pedigree ='WADE220009' and germ.legacy_attributes.origin ='JIDB1832/JIDB1822'  and  rep.tracking_id ='0001'
and inv.legacy_attributes.barcode ='I00879564750035940010001' and plot.plot_barcode ='P00000000879565341416574'
and observation_ref_cd  in (
  'PHT',  'TSIR', 'CMNTS',  'ERSC',  'S50D',  'EHT',  'P50D'
)
)
select * from
plot_data
pivot (string_agg(value) for observation_ref_cd in(  'PHT',  'TSIR', 'CMNTS',  'ERSC',  'S50D',  'EHT',  'P50D'));