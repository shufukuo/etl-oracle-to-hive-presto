#
#

from hive import presto
from hive import hive
import pandas as pd
import numpy as np
import time
import datetime
import os
import request as req
import json

#

db_host = '{myip}'

presto_conn = presto.connect( host = db_host, port = {myport1}, username = "{username}", catalog = 'hive', schema = 'default' )
presto_curs = presto_conn.cursor()

hive_conn = hive.connect( host = db_host, port = {myport2}, auth = 'NOSASL' )
hive_curs = hive_conn.cursor()
hive_curs.execute("set hive.execution.engine = spark")


presto_curs.execute("create schema if not exists mydb")
presto_curs.fetchall()
presto_curs.execute("show schemas")
presto_curs.fetchall()

#

tmp_dt = datetime.datetime(2018,12,31)

tmp_log = open('tmp_log_myjob.txt', 'w')
tmp_log.close()

t0 = time.time()

tmp_log = open('tmp_log_myjob.txt', 'a')
tmp_log.write( str(datetime.datetime.now()) + "\n" )
tmp_log.close()

is_1st = 1


while tmp_dt >= datetime.datetime.strptime("2018/01/01", "%Y/%m/%d"):
    t01 = time.time()
    
    tmp_year = tmp_dt.year
    tmp_date = tmp_dt.strftime("%Y/%m/%d")
    
    tmp_log = open('tmp_log_myjob.txt', 'a')
    tmp_log.write( tmp_date + "\n" )
    tmp_log.write( str(datetime.datetime.now()) + " - start\n" )
    tmp_log.close()
    
    
    str_sql = """
        select t.*, '$date$' as date
        from
            (
            select rownum as rn,
                col1,
                col2,
                col3,
                col4,
                col5,
                col6
            from db.order
            where id between '$dt$$id_from$' and '$dt$$id_to$' and trim(id) != ' '
            ) t
        where rn between $lb$ and $ub$
        """
    
    
    data_txt = open("data.txt", "w")
    data_txt.close()
    
    
    t02 = time.time()
    
    for i in range(1,6):
        j = 1
        
        while True:
            row_lb = 10000 * (j-1) + 1
            row_ub = 10000 * j
            
            tmp_sql = str_sql \
                .replace("$date$", tmp_date) \
                .replace("$year$", str(tmp_year)) \
                .replace("$dt$", tmp_dt.strftime("%Y%m%d")) \
                .replace("$id_from$", "{:0>6d}".format(20000*(i-1))) \
                .replace("$id_to$", "{:0>6d}".format(20000*i-1)) \
                .replace("$lb$", str(row_lb)) \
                .replace("$ub$", str(row_ub))
            
            t00 = time.time()
            
            res = req.post("http://{ip}/{CallAPI}?method=OracleQuery.Query",
                data = json.dumps({"sqlcmd": tmp_sql, "is_transform_addr_isfloor": "false"})
                )
            
            tmp_log = open('tmp_log_myjob.txt', 'a')
            tmp_log.write( str(time.time() - t00) + " - end query $i$, rows $lb$-$ub$\n".replace("$i$", str(i)).replace("$lb$", str(row_lb)).replace("$ub$", str(row_ub)) )
            tmp_log.close()
            
            str_res = res.json()["QueryResult"].encode('utf8').decode('utf8')
            
            if str_res == "":
                break
            
            t00 = time.time()
            
            data_txt = open("data.txt", "a")
            data_txt.write(str_res)
            data_txt.close()
            
            tmp_log = open('tmp_log_myjob.txt', 'a')
            tmp_log.write( str(time.time() - t00) + " - end write data.txt $i$, rows $lb$-$ub$\n".replace("$i$", str(i)).replace("$lb$", str(row_lb)).replace("$ub$", str(row_ub)) )
            tmp_log.close()
            
            j += 1
    
    
    tmp_log = open('tmp_log_myjob.txt', 'a')
    tmp_log.write( str(time.time() - t02) + " - end wrtie data.txt\n" )
    tmp_log.close()
    
    
    data_txt = open("data.txt", "a")
    
    if data_txt.tell() == 0:
        data_txt.close()
        tmp_dt = tmp_dt - datetime.timedelta(days = 1)
        continue
    
    data_txt.close()
    
    
    os.system( "cp -p data.txt {mydir}/data_" + tmp_dt.strftime("%Y%m%d") + ".txt" )
    os.system( "sudo sshpass -p {password} scp -r data.txt {username}@" + db_host + ":/{db_dir}/" )
    
    
    str_sql = "load data local inpath '/{db_dir}/data.txt' overwrite into table temp.mytable"
    
    t00 = time.time()
    
    hive_curs.execute(str_sql)
    
    tmp_log = open('tmp_log_myjob.txt', 'a')
    tmp_log.write( str(time.time() - t00) + " - done - load data into temp.mytable\n" )
    tmp_log.close()
    
    
    if is_1st:
        str_sql = "drop table if exists mydb.mytable"
        presto_curs.execute(str_sql)
        presto_curs.fetchall()
        
        str_sql = """
            create table mydb.mytable
            with (
                format = 'ORC',
                partitioned_by = ARRAY['date']
                )
            as
            select * from temp.mytable
            """
        
        is_1st = 0
    else:
        str_sql = """
            insert into mydb.mytable
            select * from temp.mytable
            """
    
    
    t00 = time.time()
    
    presto_curs.execute(str_sql)
    presto_curs.fetchall()
    
    tmp_log = open('tmp_log_myjob.txt', 'a')
    tmp_log.write( str(time.time() - t00) + " - done - insert into mydb.mytable\n" )
    tmp_log.close()
    
    tmp_dt = tmp_dt - datetime.timedelta(days = 1)
    
    tmp_log = open('tmp_log_myjob.txt', 'a')
    tmp_log.write( str(time.time() - t01) + " - end $dt$\n".replace("$dt$", tmp_date) )
    tmp_log.close()


tmp_log = open('tmp_log_myjob.txt', 'a')
tmp_log.write( str(time.time() - t0) + "\n" )
tmp_log.close()


quit()

#
#
#
#
#
#
#
