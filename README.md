# Optimizing Upserts and Calcluations with CockroachDB 
This example is meant to show how you can optimize the insertion of values to multiple tables while calculating aggregrated summary information.  CockroachDB is a distributed SQL database that uses *serializable* isolation to ensure data consistency.  Serializable isolation is achieved using an *optimistic* model without locking.  If any modified data has been changed by another transaction before the a *commit* then a *serializable* failure might occur and the transaction must be re-tried.  With multiple transactions trying to *upsert* calcluations to the same row, this can be less than ideal.

This example shows how *batching* values into a *single* CTE statement can help to improve scaling of such operations.

Consider the following *orders* and  *lineitem* tables:

```sql
create table orders
(
  oid int primary key,
  sum_item_quantity int
);

create table lineitem
(
  lid int,
  oid int,
  quantity int,
  primary key(lid, oid)
);
```

There can be multiple `lineitem` entries for an `order`.  The `sum_item_quantity` value in the `orders` table must be re-calculated as more `lineitem` rows are added.  This is typically done using multiple SQL statements within a transaction.  For example, this is the traditional multi-statement approach.

### Multiple Statements
These 5 statements are all kicked off within the code to insert new `orders` and `lineitem` values:

```sql
BEGIN;
  insert into orders values (1,0);
COMMIT;
```

Next a batch of 5 inserts into `lineitem` table are run to populate the order and calculate the summary values for the `order`:
```sql 
BEGIN; 
  insert into lineitem values (1,1,1)
  update orders set sum_item_quantity = (select sum(quantity) from lineitem where oid=1)::int where oid=1;
COMMIT;
```
```sql
BEGIN;
  insert into lineitem values (2,1,1)
  update orders set sum_item_quantity = (select sum(quantity) from lineitem where oid=1)::int where oid=1;
COMMIT;
```
```sql
BEGIN;
  insert into lineitem values (3,1,1)
  update orders set sum_item_quantity = (select sum(quantity) from lineitem where oid=1)::int where oid=1;
COMMIT;
```
```sql
BEGIN;
  insert into lineitem values (4,1,2)
  update orders set sum_item_quantity = (select sum(quantity) from lineitem where oid=1)::int where oid=1;
COMMIT;
```
```sql
BEGIN;
  insert into lineitem values (5,1,3)
  update orders set sum_item_quantity = (select sum(quantity) from lineitem where oid=1)::int where oid=1;
COMMIT;
```
This requires multiple threads, commits, and possibly retries to ensure data is properly calculated.  To optimize this batching all the values is the first step.

### Multiple Statements (Batching Values)

```sql
BEGIN;
  insert into orders values (1,0);
  insert into lineitem values (1,1,1),(2,1,1),(3,1,1),(4,1,2),(5,1,3);
  update orders set sum_item_quantity = (select sum(quantity) from lineitem where oid=1)::int where oid=1;
COMMIT;
```

While this can certainly improve the performance, there is one further thing that can be done with CockroachDB to use the *returning* clause within a *CTE* to upsert values in a single statement.

### CTE single statement UPSERT Calculation
The following statement is able to insert all values and calculate the results.

```sql
-- CTE upsert single statement
--
with input_cte as (
   select column1 as lid, column2 as oid, column3 as quantity
   from (values(1,1,1),(2,1,1),(3,1,1),(4,1,2),(5,1,3))
),
insert_li as (
  insert into lineitem
     select 
       (case when l.lid is null then i.lid else l.lid end),
       (case when l.oid is null then i.oid else l.oid end),
       i.quantity
       --(case when l.quantity is null then i.quantity else i.quantity end) 
     from input_cte as i
     full outer join lineitem as l on (l.oid=i.oid and l.lid=i.lid)
    on conflict (lid, oid)
    do update set quantity= case when excluded.quantity is null then lineitem.quantity else excluded.quantity end
  returning (lid),(oid),(quantity)
)
insert into orders select oid, sum(quantity)::int from insert_li where oid=1 group by oid
  on conflict(oid)
  do update set sum_item_quantity = excluded.sum_item_quantity
returning (oid),(sum_item_quantity);
```

## Summary
You can make transactions retry automatically with CockroachDB using a single CTE.  This is more efficient and reduces round-trips between the application and DB.

Additionally, a CTE you be a efficient way to move data between tables inside a single statement as shown by this [example](./transactional_cte_mover.md).