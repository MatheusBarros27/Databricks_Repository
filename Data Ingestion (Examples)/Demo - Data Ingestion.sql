-- Databricks notebook source
USE CATALOG 'new_sample';


-- COMMAND ----------

select current_catalog(), current_schema();

-- COMMAND ----------

list '/Volumes/new_sample/default/db_class_matheus'

-- COMMAND ----------

select * from parquet.`/Volumes/new_sample/default/db_class_matheus/users_10000_with_10pct_null_emails.parquet`

-- COMMAND ----------

select * from read_files(
    '/Volumes/new_sample/default/db_class_matheus/Array JSON com 10 objetos.json',
    format => 'json'
)

-- COMMAND ----------

select *
from 
    read_files(
        '/Volumes/new_sample/default/db_class_matheus/users_10000_with_10pct_null_emails.parquet',
        format => 'parquet'
    )
    limit 10;

-- COMMAND ----------

drop table if exists historical_users_bronze_ctas_rf;

create table historical_users_bronze_ctas_rf as
select 
    *
from
    read_files(
        '/Volumes/new_sample/default/db_class_matheus/users_10000_with_10pct_null_emails.parquet',
        format => 'parquet'
    );

select * from historical_users_bronze_ctas_rf limit 100;


-- COMMAND ----------

describe table extended historical_users_bronze_ctas_rf;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # CTAS USING PYTHON INSTEAD SQL
-- MAGIC # 1. Read the Parquet files from the volume into a Spark DataFrame
-- MAGIC my_catalog = "new_sample"
-- MAGIC df = (spark
-- MAGIC     .read
-- MAGIC     .format("parquet")
-- MAGIC     .load("/Volumes/new_sample/default/db_class_matheus/users_10000_with_10pct_null_emails.parquet")
-- MAGIC )
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # 2. Write to the DataFrame to a UC table (overwrite the table if it exists)
-- MAGIC (df
-- MAGIC     .write
-- MAGIC     .mode("overwrite")
-- MAGIC     .saveAsTable(f"{my_catalog}.default.historical_users_bronze_python")
-- MAGIC )
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # 3. Read and view the table
-- MAGIC users_bronze_table = spark.table(f"{my_catalog}.default.historical_users_bronze_python")
-- MAGIC users_bronze_table.display()

-- COMMAND ----------

drop table if exists historical_users_bronze_ci;

create table historical_users_bronze_ci (
user_id BIGINT,
user_first_touch_timestamp timestamp_ntz
);


-- COMMAND ----------


COPY INTO historical_users_bronze_ci
 FROM '/Volumes/new_sample/default/db_class_matheus/users_10000_with_10pct_null_emails.parquet'
 FILEFORMAT = PARQUET
 COPY_OPTIONS ('mergeSchema' = 'true'); -- Merge the schema of each file for tables that we have the number of columns different the original

-- COMMAND ----------

select * from historical_users_bronze_ci limit 100;
describe table extended historical_users_bronze_ci;

-- COMMAND ----------

-- Exemplo para pegar a colunas Metadata de uma tabela lida de um arquivo ou fonte cloud
select *,
    _metadata.file_name,
    _metadata.file_modification_time
from historical_users_bronze_ci limit 100;

-- COMMAND ----------

--SELECT schema_of_json('{"name":"Matheus","age":26}')
select 
schema_of_json('{
                "id": 1,
                "name": "User 1",
                "email": "user1@example.com",
                "age": 20,
                "city": "New York",
                "isActive": false,
                "score": 50.5
                }'
)

-- COMMAND ----------

create TABLE json_columns_ex1 
(
    LINHA int,
    JSON_COLUMNs_sample string

);

-- COMMAND ----------

insert into json_columns_ex1 (LINHA, JSON_COLUMNs_sample) values
(1, '
  {
    "id": 1,
    "name": "User 1",
    "email": "user1@example.com",
    "age": 20,
    "city": "New York",
    "isActive": false,
    "score": 50.5
  }'),
(2, '
  {
    "id": 2,
    "name": "User 2",
    "email": "user2@example.com",
    "age": 25,
    "city": "London",
    "isActive": true,
    "score": 57.8
  }'),
(3, '
  {
    "id": 3,
    "name": "User 3",
    "email": "user3@example.com",
    "age": 30,
    "city": "Tokyo",
    "isActive": true,
    "score": 65.1
  }'),
(4, '
  {
    "id": 4,
    "name": "User 4",
    "email": "user4@example.com",
    "age": 35,
    "city": "Berlin",
    "isActive": false,
    "score": 72.4
  }'),
(5, '
  {
    "id": 5,
    "name": "User 5",
    "email": "user5@example.com",
    "age": 40,
    "city": "Sydney",
    "isActive": true,
    "score": 79.7
  }'),
(6, '
  {
    "id": 6,
    "name": "User 6",
    "email": "user6@example.com",
    "age": 45,
    "city": "Paris",
    "isActive": true,
    "score": 87.0
  }'),
(7, '
  {
    "id": 7,
    "name": "User 7",
    "email": "user7@example.com",
    "age": 50,
    "city": "Moscow",
    "isActive": false,
    "score": 94.3
  }'),
(8, '
  {
    "id": 8,
    "name": "User 8",
    "email": "user8@example.com",
    "age": 55,
    "city": "Beijing",
    "isActive": true,
    "score": 101.6
  }'),
(9, '
  {
    "id": 9,
    "name": "User 9",
    "email": "user9@example.com",
    "age": 60,
    "city": "São Paulo",
    "isActive": true,
    "score": 108.9
  }'),
(10, '
  {
    "id": 10,
    "name": "User 10",
    "email": "user10@example.com",
    "age": 65,
    "city": "Mumbai",
    "isActive": false,
    "score": 116.2
  }')
;

-- COMMAND ----------

select * from new_sample.default.json_columns_ex1

-- COMMAND ----------

--tableas criadas somente com CTAS tem que usar este code abaixo para mudar as configuracoes e funcionar o DROP Column
ALTER TABLE new_sample.default.json_columns_ex1 SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

ALTER TABLE new_sample.default.json_columns_ex1
ADD COLUMN structured_column STRUCT<age: BIGINT, city: STRING, email: STRING, id: BIGINT, isActive: BOOLEAN, name: STRING, score: DOUBLE>;

-- COMMAND ----------

-- exemplo adicionando uma coluna STRUCT de uma outra coluna que era string mas tinha um json nela usando UPDATE e SET 
UPDATE new_sample.default.json_columns_ex1
SET structured_column = from_json(
  JSON_COLUMNs_sample,
  'STRUCT<age: BIGINT, city: STRING, email: STRING, id: BIGINT, isActive: BOOLEAN, name: STRING, score: DOUBLE>'
);


-- COMMAND ----------

--SELECT from_json(json_col, 'STRUCT<name: STRING, age: INT>')
--LENDO O RESULTADO

select *
from new_sample.default.json_columns_ex1;



-- COMMAND ----------

-- transformando em uma view consultavel
create view json_columns_ex1_silver as
SELECT
  structured_column.id,
  structured_column.name,
  structured_column.email,
  structured_column.age,
  structured_column.city,
  structured_column.isActive,
  structured_column.score
FROM new_sample.default.json_columns_ex1;

-- COMMAND ----------

-- lendo os resultados

select * from json_columns_ex1_silver

-- COMMAND ----------

-- EXEMPLO usando tipo VARIANT muito versatil no databricks

ALTER TABLE new_sample.default.json_columns_ex1
ADD COLUMN VARIANT_column VARIANT;

-- exemplo adicionando uma coluna VARIANT de uma outra coluna que era string mas tinha um json nela usando UPDATE e SET 
UPDATE new_sample.default.json_columns_ex1
SET VARIANT_column = parse_json(JSON_COLUMNs_sample) ;


-- COMMAND ----------

--mostrando os resultados

SELECT *
FROM json_columns_ex1;

-- COMMAND ----------

drop table if exists Id_user_table_ref_bronze;

create table Id_user_table_ref_bronze as
select 
    *
from
    read_files(
        '/Volumes/new_sample/default/db_class_matheus/Id_user_table_ref_bronze.csv',
        format => 'csv'
    );

select * from Id_user_table_ref_bronze;


-- COMMAND ----------

--MERGE INTO Example
drop table if exists Id_user_table_ref_bronze_updated;

create table Id_user_table_ref_bronze_updated as
select 
    *
from
    read_files(
        '/Volumes/new_sample/default/db_class_matheus/Id_user_table_ref_bronze_updated.csv',
        format => 'csv'
    );

select * from Id_user_table_ref_bronze_updated;

-- COMMAND ----------

--MERGE INTO Example
merge into new_sample.default.id_user_table_ref_bronze as old
using new_sample.default.Id_user_table_ref_bronze_updated as new
on old.user_id = new.user_id
when matched and new.status = 'delete' then delete
when matched and new.status = 'update' then update set *
when not matched then insert *;

-- COMMAND ----------

select * from new_sample.default.id_user_table_ref_bronze order by user_id asc