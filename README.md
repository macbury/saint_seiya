# Kopiowanie danych

```
aws s3 cp s3://cdstawsprez/flat/fakedata.csv s3://saint-seiya/ --recursive
```

```
SELECT AVG(height) as avg_height, country FROM prezka.countries GROUP BY country;
```

# FLAT

first_name string, last_name string, country string, height float

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS default.cdst_flat (
  `first_name` string,
  `last_name` string,
  `country` string,
  `height` float
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cdstawsprez/flat/'
TBLPROPERTIES ('has_encrypted_data'='false');
```

```sql
SELECT * FROM cdst_flat LIMIT 1;
```

```sql
SELECT count(*) AS cnt FROM cdst_flat;
```

```sql
SELECT AVG(height) AS height FROM cdst_flat;
```

```sql
SELECT AVG(height) AS height, country FROM cdst_flat GROUP BY country;
```

```sql
SELECT AVG(height) AS height, country FROM cdst_flat WHERE country LIKE 'Poland' GROUP BY country;
```

# PARTITIONED

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS default.cdst_partitioned (
  `first_name` string,
  `last_name` string,
  `height` float
) PARTITIONED BY (
  country string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cdstawsprez/partitioned/'
TBLPROPERTIES ('has_encrypted_data'='false');
```

```sql
SELECT * FROM cdst_partitioned LIMIT 1;
```

```sql
SELECT count(*) AS cnt FROM cdst_partitioned;
```

```sql
SELECT AVG(height) AS height FROM cdst_partitioned;
```

```sql
SELECT AVG(height) AS height, country FROM cdst_partitioned GROUP BY country;
```

```sql
SELECT AVG(height) AS height, country FROM cdst_partitioned WHERE country LIKE 'Poland' GROUP BY country;
```
