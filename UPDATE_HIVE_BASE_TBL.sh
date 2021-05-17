############ EXECUTE SQOOP INCREMENTAL JOB ######################

sqoop job --meta-connect jdbc:hsqldb:hsql://metaconnecnt.host.name:16000/sqoop -exec INC_TEST_APPEND

###### test_base creation ########
CREATE TABLE dm_labware_live_db.test_base 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'  
STORED AS ORC 
location '/xxx/xxxx/test_base'
tblproperties("orc.compress"="zlib")
 AS 
SELECT * FROM test_stg;


########### HIVE ETL TO UPDATE AND INSERT RECORDS ##############-------------

CREATE VIEW test_update AS 
WITH CTE AS 
(
SELECT * ,
row_number() over(PARTITION BY test_number ORDER BY changed_on desc ) Indexs
FROM test_stg
)
select * from cte where Indexs=1


###########--- CONVERTING MANAGED TABLE TO EXTERNAL TABLE ---#############
 
 ALTER TABLE test_base SET TBLPROPERTIES('EXTERNAL'='TRUE');
 

#################  Add  index column to test_base table ###########
ALTER TABLE TEST_BASE ADD COLUMNS(INDEXES INT);


### INSERT OVERITE BASE TABLE WITH UPDATED RECORDS FROM STAGING TABLE #############

INSERT OVERWRITE TABLE  dm_labware_live_db.test_base  
select * from test_update;

SELECT count(*) FROM dm_labware_live_db.test_base;

#########--- check the count of before and after update of base table ############

SELECT count(*) FROM hive_db.test_stg;
SELECT count(*) FROM hive_db.test_base;
