SELECT
  ordinal_position,
  column_name,
  data_type,
  udt_schema,
  udt_name,
  is_nullable,
  character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'qc_coversheet'
  AND table_name   = 'contact'
ORDER BY ordinal_position;