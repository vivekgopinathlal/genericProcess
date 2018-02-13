
-- MySQL  Table creation
/*create table categories1 (category_id INTEGER UNIQUE, category_department_id INTEGER, category_name VARCHAR(22));

create table customers (
customer_id INTEGER UNIQUE,
customer_fname VARCHAR(30),
customer_lname VARCHAR(30),
customer_email VARCHAR(80),
customer_password VARCHAR(30),
customer_street VARCHAR(120),
customer_city VARCHAR(80),
customer_state VARCHAR(30),
customer_zipcode VARCHAR(30)
);*/

CREATE TABLE cdcAttrVal(
batch_id INT(6) NOT NULL,
run_id INT(6) NOT NULL,
prog_id INT(6) NOT NULL,
source_table_name varchar(20)NOT NULL,
attribute_name varchar(250) NOT NULL,
attribute_val varchar(250) NOT NULL,
as_of_date int(6)
);

CREATE TABLE progDetails(
program_id INT(6) NOT NULL  PRIMARY KEY,
program_type varchar(20)NOT NULL,
program_location varchar(250) NOT NULL,
script_name varchar(50) NOT NULL
);

CREATE TABLE jobDependency(
job_dep_id INT(6) NOT NULL,
job_name VARCHAR(15) NOT NULL,
program_id INT(6) NOT NULL, 
exec_order_seq INT(4) NOT NULL,
arguments VARCHAR(250)
);

CREATE TABLE runStatus(
run_id INT(6) NOT NULL,
batch_id INT(6) NOT NULL,
program_id INT(6) NOT NULL,
program_status varchar(10),
createdTime DATETIME,
updatedTime DATETIME,
error varchar(500),
log_path varchar(250),
debug_log varchar(250),
stats_log varchar(250)
);

CREATE TABLE reportBatches(
batch_id INT(6) NOT NULL AUTO_INCREMENT PRIMARY KEY,
date DATETIME,
as_of_date DATE,
job_name VARCHAR(50)
);

/*
insert into jobDependency (job_dep_id, job_name, program_id, exec_order_seq, arguments) values (1,'Products',101,1,' --sourceTable products_demo  --destFile ingestProducts -e Products -b 12345 -p 101 --asOfDate 99991231 -r 000 ');
insert into jobDependency (job_dep_id, job_name, program_id, exec_order_seq, arguments) values (1,'Products',102,2,'-b products_base -d products_stg1 -e Products -k product_category_id,product_id -p product_category_id -u product_name --batchId 12345 --progId 102:000 --asOfDate 99991231');
insert into jobDependency (job_dep_id, job_name, program_id, exec_order_seq, arguments) values (1,'Products',103,3,NULL);


Insert into progDetails(program_id, program_type, program_location, script_name) Values(101,'pyspark','<<DIRLOC>>','mysql_pull.py');
Insert into progDetails(program_id, program_type, program_location, script_name) Values(102,'pyspark','<<DIRLOC>>','generic_cdc_process.py');
Insert into progDetails(program_id, program_type, program_location, script_name) Values(103,'hive','<<DIRLOC>>','hive_part_exchg.hql');
*/
