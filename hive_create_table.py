#
#

from pyhive import hive
import pandas as pd
import numpy as np
import os

#

hive_conn = hive.connect( host = '{myip}', port = {myport}, auth = 'NOSASL' )
hive_curs = hive_conn.cursor()
hive_curs.execute("set hive.execution.engine = spark")

#

hive_curs.execute("create database if not exists temp")
hive_curs.execute("create database if not exists mydb")

hive_curs.execute("show databases")
print( hive_curs.fetchall() )

#

str_sql = "create table if not exists temp.mytable (" + \
    "col1  char(4)         " + "," + \
    "col2  decimal(5)      " + "," + \
    "col3  varchar(100)    " + "," + \
    "col4  double          " + "," + \
    "col5  decimal(18,10)  " + "," + \
    "col6  timestamp       " + "," + \
    "date  char(10)        " + \
    ")" + \
    " row format delimited" + \
    " fields terminated by '\t'" + \
    " lines terminated by '\n'" + \
    " NULL defined as 'NULL'" + \
    " stored as textfile"

hive_curs.execute(str_sql)


str_sql = "create table if not exists mydb.mytable (" + \
    "col1  char(4)         " + "," + \
    "col2  decimal(5)      " + "," + \
    "col3  varchar(100)    " + "," + \
    "col4  double          " + "," + \
    "col5  decimal(18,10)  " + "," + \
    "col6  timestamp       " + \
    ")" + \
    " partitioned by (date char(10))" + \
    " clustered by (col1) into 4 buckets" + \
    " stored as orc tblproperties ('transactional' = 'true')"

hive_curs.execute(str_sql)


###
###
###

df = pd.read_sql("select * from ecoper.order_main where orddt = '2016/11/06' limit 5", hive_conn)

hive_curs.execute("delete from ecoper.order_main where substr(trim(orddt), 1, 4) != '2016'")
hive_curs.execute("use test1")
hive_curs.execute("analyze table test1.order_detail partition (orddt = '2016/04/05') compute statistics")
hive_curs.execute("desc formatted test1.order_detail")
hive_curs.execute("alter table test1.order_detail drop if exists partition (orddt = '__HIVE_DEF'), partition (orddt = '__HIVE_DEFAULT_PARTITION__')")
print( hive_curs.fetchall() )

#
#
#
#
#
#
#