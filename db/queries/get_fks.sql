select constraint_name,
       ORDINAL_POSITION,
       table_name,
       column_name,
       referenced_table_name,
       referenced_column_name
from information_schema.key_column_usage
where referenced_table_name is not null
  and TABLE_SCHEMA=?
order by CONSTRAINT_NAME, ORDINAL_POSITION
;