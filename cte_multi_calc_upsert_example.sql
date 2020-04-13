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
  primary key(oid,lid)
);

-- create table lineitem
-- (
--   lid int,
--   oid int,
--   quantity int,
--   primary key(lid,oid)
-- );
create table orders2
(
  oid int primary key,
  sum_item_quantity int
);

create table lineitem2
(
  lid int,
  oid int,
  quantity int,
  primary key(oid,lid)
);

-- Multi-Statement transaction /w optimistic locking
--
BEGIN;

  insert into orders values (1,0) on conflict(oid) do nothing;
  insert into lineitem values (1,1,1),(2,1,1),(3,1,1),(4,1,2),(5,1,3) on conflict(lid,oid) do nothing;
  update orders set sum_item_quantity = (select sum(quantity) from lineitem where oid=1)::int where oid=1;
  
COMMIT;

-- Multi-Statement transaction /w SFU
--
BEGIN;

select o.oid, o.sum_item_quantity, l.lid 
from orders as o 
join lineitem as l
  on (o.oid = l.oid) for update;

  insert into orders values (1,0) on conflict(oid) do nothing;
  insert into lineitem values (1,1,1),(2,1,1),(3,1,1),(4,1,2),(5,1,3) on conflict(lid,oid) do nothing;
  update orders set sum_item_quantity = (select sum(quantity) from lineitem where oid=1)::int where oid=1;
  
COMMIT;


-- Check Results
--
select * from orders;
select * from lineitem;

-- RESET
--
delete from orders where oid=1;
delete from lineitem where oid=1;

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

-- Check Results
--
select * from orders;
select * from lineitem;


-- CTE upsert single statement... add one lineitem Value
--
with input_cte as (
   select column1 as lid, column2 as oid, column3 as quantity
   from (values(99,1,34))
),
insert_li as (
  insert into lineitem
     select 
       (case when l.lid is null then i.lid else l.lid end),
       (case when l.oid is null then i.oid else l.oid end),
       i.quantity
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

-- Check Results
--
select * from orders;
select * from lineitem;
