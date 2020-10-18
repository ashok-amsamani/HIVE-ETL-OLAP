Create database if not exists custdb;

use custdb;

create table if not exists customer(custno int, firstname varchar(20), lastname varchar(20), age int,profession varchar(20))
row format delimited fields terminated by ',';

load data local inpath '/home/hduser/hive/data/custs' into table customer; 

create database if not exists retail;

use retaildb

create table if not exists txnrecords(txnno INT, txndate STRING, custno INT, amount DOUBLE, category varchar(20), product varchar(20), city varchar(20), state varchar(20), spendby varchar(20))
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;

load data local inpath '/home/hduser/hive/data/txns' into table txnrecords;

use usecase;

create external table if not exists cust_trxn (custno int,fullname varchar(20),age int,profession varchar(20),amount double,product varchar(20),spendby varchar(20),agecat varchar(100),modifiedamout float) 
row format delimited fields terminated by ',' location '/user/hduser/custtxns';

insert into table cust_trxn 
select a.custno,upper(concat(a.firstname,a.lastname)),a.age,a.profession,b.amount,b.product,b.spendby,
case when age<30 then 'low' 
when age>=30 and age < 50 then 'middle'
when age>=50 then 'old'
else 'others' end as agecat,
case when spendby= 'credit' then b.amount+(b.amount*0.05) else b.amount end as modifiedamount
from custdb.customer a JOIN retail.txnrecords b
ON a.custno = b.custno; 


create external table if not exists cust_trxn_aggr (seqno int,product varchar(20), profession varchar(20),level varchar(20), sumamt double, avgamount double,maxamt double,avgage int,currentdate date)
row format delimited fields terminated by ',';

insert overwrite table cust_trxn_aggr
select row_number() over(),product, 
profession,agecat, cast(sum(amount) as decimal(8,3)),cast(avg(amount) as decimal(8,3)),cast(max(amount) as decimal(8,3)),cast(avg(age) as decimal(8,3)),current_date()
from cust_trxn group by product,profession, agecat, current_date(); 

select count(*) from cust_trxn_aggr;


/****** Wok on below, it has error ********/
create external table if not exists cust_trxn_aggr1 (seqno int,product varchar(20), product_count int, profession varchar(20),level varchar(20), sumamt double, avgamount double,maxamt double,avgage int,currentdate date)
row format delimited fields terminated by ',';

insert overwrite table cust_trxn_aggr1
select row_number() over(),product, 
count(*) over (partition by product order by product asc), profession,agecat, cast(sum(amount) as decimal(8,3)),cast(avg(amount) as decimal(8,3)),cast(max(amount) as decimal(8,3)),cast(avg(age) as decimal(8,3)),current_date()
from cust_trxn group by profession, agecat, current_date(); 






