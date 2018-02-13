# Global Functions
# Author: Vivek
# Version: 1.0 common_functions.py
from __future__ import print_function

import os, sys, csv, base64, logging, cStringIO, traceback, MySQLdb, subprocess
sys.path.append(".")
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext as sqlContext, HiveContext
from pyspark.sql.functions import unix_timestamp
from time import gmtime, strftime
import credentials_b64 as cb64,log
from credentials_b64 import *

mysqlConnectionString='jdbc:mysql://mcdhpoc3:3306/bbiuserDB'
mysqlUser1='bbiuser'
mysqlHost1='mcdhpoc3'
mysqlPasswd1=base64.b64decode(cb64.spark_jdbc_poc_b64password)
mysqlDB1='bbiuserDB'

mysqlConnectionString=mysql_conn_string
mysqlUser=mysql_user
mysqlHost=mysql_host
mysqlPasswd=base64.b64decode(spark_jdbc_poc_b64password)
mysqlDB=mysql_db

runStatus_tbl_nm='runStatus'
batch_tbl_nm='reportBatches'
jobDep_tbl_nm='jobDependency'

def getCurTime(format="%Y-%m-%d %H:%M:%S"):
        return strftime(format, gmtime())

def setup_logger(logger_name, log_file, level=logging.INFO):
    l = logging.getLogger(logger_name)
    #formatter = logging.Formatter('%(log_color)s%(asctime)s%(reset)s | %(name)s | %(log_color)s%(levelname)s%(reset)s | %(message)s')
    #formatter = logging.Formatter('%(log.FAIL)s%(asctime)s%(log.ENDC)s | %(name)s | %(log.FAIL)s%(levelname)s%(log.ENDC)s | %(message)s')
    #formatter = ColoredFormatter(formatter)
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    fileHandler = logging.FileHandler(log_file, mode='w')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    l.setLevel(level)
    l.addHandler(fileHandler)
    l.addHandler(streamHandler)

def insertBatchReports(sc, as_of_date, program_id):
        try:
          curTime=getCurTime()
          data=[curTime, as_of_date, program_id]
          df = sc.parallelize([data]).toDF(['date', 'as_of_date', 'program_id'])

          #passwd=base64.b64decode(cb64.spark_jdbc_poc_b64password)
          df.write.format('jdbc').options(url=mysqlConnectionString, dbtable='reportBatches',
          user=mysqlUser, password=mysqlPasswd).mode('append').save()
          return "Insert Successful..."

        except:
          print("InsertBatchReports Exception : Type | " , sys.exc_info()[0])
          print("InsertBatchReports Exception : Value | ",sys.exc_info()[1])
          print(repr(traceback.format_tb(sys.exc_info()[2])))
          quit()

def getBatchId(as_of_date, job_nm):
	conn = mySqlDBconnect()
        cursor = conn.cursor()
        curTime = getCurTime()
        insertStmt = "INSERT INTO "+batch_tbl_nm+" (date, as_of_date, job_name) VALUES(%s, %s, %s)"
        #selectStmt = "SELECT BATCH_ID FROM "+batch_tbl_nm+" WHERE as_of_date = '"+as_of_date+"' and date ='"+curTime+"'"
        try:
          cursor.execute(insertStmt,(curTime, as_of_date, job_nm))
          print("Lastrowid : ",cursor.lastrowid)
          conn.commit()
          batch_id=cursor.lastrowid
        except:
          print("Failed at getBatchId:")
	  logExcep('getBatchId',sys.exc_info())
          quit()
        finally:
          cursor.close()
          conn.close()
	return int(batch_id)	  

def updateRunStatus(batch_id, program_id, status, excp="", run_id=1):
        conn = mySqlDBconnect()
        cursor = conn.cursor ()
        curTime = getCurTime()
	#runId = int(selValues(runStatus_tbl_nm, 'max(run_id)', "batch_id = "+str(batch_id), fcond=False)[0])
	runId = run_id
	if excp:
	  updtStmt = "UPDATE "+runStatus_tbl_nm+" SET program_status = '"+status+"', updatedTime ='"+curTime+"', error ='"+excp+"' WHERE batch_id = "+str(batch_id)+" AND program_id = "+str(program_id)+" AND run_id = "+str(runId)
	else:
          updtStmt = "UPDATE "+runStatus_tbl_nm+" SET program_status = '"+status+"', updatedTime ='"+curTime+"' WHERE batch_id = "+str(batch_id)+" AND program_id = "+str(program_id)+" AND run_id = "+str(runId)
        print("URS: "+updtStmt)
        try:
          cursor.execute(updtStmt)
	  print("URS: Completed")
        except:
	  logExcep('updateRunStatus',sys.exc_info())
        finally:
          cursor.close ()
          conn.close ()

        return "Update Completed"
		
def mySqlDBconnect(mysqlHost=mysqlHost, mysqlUser=mysqlUser, mysqlPasswd=mysqlPasswd, mysqlDB=mysqlDB):
		return MySQLdb.connect (host = mysqlHost, user = mysqlUser, passwd = mysqlPasswd, db = mysqlDB)

def insertIntoTbl(tbl,stmt):
        conn = mySqlDBconnect()
        cursor = conn.cursor()
	try:
	  cursor.execute(stmt)
	except:
	  logExcep('insertIntoTbl | '+tbl, sys.exc_info())
	  return "Insert | "+tbl+" Failed"
	finally:
          cursor.close ()
          conn.close ()
        return 'Insert Completed'

def dfInsertIntoTbl(tbl, frame, mysqlHost=mysqlHost, mysqlUser=mysqlUser, mysqlPasswd=mysqlPasswd, mysqlDB=mysqlDB):
	import pandas as pd
	conn = mySqlDBconnect(mysqlHost=mysqlHost.replace('\n',''), mysqlUser=mysqlUser.replace('\n',''), mysqlPasswd=mysqlPasswd.replace('\n',''), mysqlDB=mysqlDB.replace('\n',''))
	cursor = conn.cursor()
	try:
	  cols=frame.dtypes.index
	  col_names=', '.join(cols)
	  wildcard=','.join(['%s']*len(frame.columns))
	  stmt="INSERT INTO %s (%s) VALUES (%s)"%(tbl, col_names, wildcard)
	  print(stmt)
	  data=[tuple([None if pd.isnull(v) else v for v in rw]) for rw in frame.values]
	  print(data[0])
	  print(cursor.executemany(stmt,data))
          #for result in cursor.execute(stmt,data, multi=True):
          #for result in cursor.executemany(stmt,data):
             #print(result.statement)
	     #print(cursor.lastrowid)
        except:
          logExcep('dfInsertIntoTbl | '+tbl, sys.exc_info())
          return "DataFrame Insert | "+tbl+" Failed"
        finally:
          cursor.close ()
          conn.close ()
        return 'DataFrame Insert Completed'

def insertRunStatus(batch_id,runId,job_name,as_of_date):
	conn = mySqlDBconnect()
	cursor = conn.cursor()
	curTime = getCurTime("%Y%m%d%H%M%S")
	path = "$HDP_COMMON_DATA_LOG/"
	prog_id_list=[]
	suffix = "_"+str(batch_id)+"_"+str(as_of_date)+"_"+str(curTime)+"."
	insertStmt = "INSERT INTO "+runStatus_tbl_nm+" (batch_id, run_id, program_id, program_status, createdTime, log_path, debug_log, stats_log) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
	try:
	  cursor.execute("SELECT * FROM "+jobDep_tbl_nm+" WHERE job_name = '"+job_name+"' ORDER BY exec_order_seq")
	  for row in cursor.fetchall():
	    job_name=row[1]
	    program_id=str(row[2])
	    file_nm=job_name+"_"+program_id
	    cursor.execute(insertStmt,(batch_id, runId, program_id, "Initiated", curTime, path+file_nm+suffix+"log", path+file_nm+suffix+"deb", path+file_nm+suffix+"stat" ))
	    prog_id_list.append(file_nm+suffix)
	except:
	  logExcep('insertRunStatus',sys.exc_info())
          return "Insert | runStatus Failed"
        finally:
          cursor.close ()
          conn.close ()
	return prog_id_list

def selValues(tbl_nm, attr_list, cond='1=1', fcond=False):
	conn = mySqlDBconnect()
	cursor = conn.cursor()
	curTime = getCurTime()
        row = []
	selStmt = "SELECT "+attr_list+" FROM "+tbl_nm+" WHERE "+cond
        print(selStmt)
	try:
	  cursor.execute(selStmt)
	  #print(cursor.fetchone())
	  if fcond:
	    #row.append(cursor.fetchall())
            x=cursor.fetchall()
	  else:
	    #row.append(cursor.fetchone())
            x=cursor.fetchone()
	except:
	  logExcep('Select Values', sys.exc_info())
	  quit()
	finally:
	  cursor.close ()
          conn.close ()
	return x

def executeCommand(cmd,wfh,batch_id=0,prog_id=0,runId=1):
    print(cmd+'|'+str(batch_id)+'|'+str(prog_id))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out, err = p.communicate() 
    if p.returncode:
      wfh.write("EC:"+str(err))
      updateRunStatus(batch_id,prog_id,'Failed',err,runId)
      wfh.write("\t=========Error==========\n\t\t"+err)
    else:
      wfh.write("EC:"+str(out))
      updateRunStatus(batch_id,prog_id,'Success','Process Completed Successfully',runId)
      wfh.write("\t=========Output===========\n\t\t"+out)
    result = out.split('\n')
    for lin in result:
      if not lin.startswith('#'):
        print(lin)

def logExcep(module,excp,batch_id=0,prog_id=0,status='Failed',track=False):
	log.rerr("************************Exception Message**************************")
	log.warn(module+" : ")
	log.err(str(excp[0]))
	log.err(str(excp[1]),batch_id,prog_id,status,track)
	log.warn("Traceback : ")
	log.err(str(repr(traceback.format_tb(excp[2]))))
	#log.err(str(repr(traceback.print_exception(excp[0],excp[1],excp[2],limit=2, file=sys.stdout))))
	log.rerr("*******************************************************************")
	return 1

def logStatus(module,info, batch_id=0,prog_id=0,status='Success',track=False):
    log.infog("**********************************************************************")
    log.warn(module+" : ")
    log.info(info, batch_id,prog_id,status,track)
    log.infog("**********************************************************************")
    return 0
