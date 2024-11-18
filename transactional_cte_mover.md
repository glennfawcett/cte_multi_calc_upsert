# Transactional CTEs with CockroachDB
More examples of transactional CTEs.

## Data Mover 

```sql

WITH copybatch AS (
    DELETE FROM oldtable
        WHERE 1=1
	LIMIT 1000
	        RETURNING oldtable.*
	)
	INSERT INTO newtable
	SELECT * FROM copybatch;

```

## Data Trimmer to record deletes

```sql

CREATE TABLE delete_tracker (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP DEFAULT now(),
    tablename string,
    rows_deleted int
);

WITH deletebatch AS (
    DELETE FROM mytable
        WHERE 1=1
        ts < now() - INTERVAL '24h'
	LIMIT 1000
	        RETURNING mytable.*
	)
	INSERT INTO delete_tracker(tablename, rows_deleted) 
	SELECT 'mytable', count(*) FROM deletebatch;
	
```