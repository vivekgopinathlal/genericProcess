from __future__ import print_function
import os, sys, csv, logging, argparse
sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0])))
sys.path.append("/home/mapr/hadoop/BBI/DEV_VAL/SparkStreaming/conf")
import Conf, log
from credentials_b64 import *
from colorlog import ColoredFormatter
from common_functions import getBatchId, insertRunStatus as irs, updateRunStatus as urs, getCurTime, setup_logger, logExcep
from common_functions import *
from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext, SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import max


def printAval(flg,msgPart):
    if flg:
       msg=msgPart+" validated Successfully \n"
    else:
       msg=msgPart+" failed, Kindly validated \n"

    return msg

def genWhereClause(delKey, sourceTable):
    cond=[]
    reqKeys=[]
    for keyList in delKey.split(","):
	print(keyList)
        val=selValues('cdcAttrVal', 'attribute_val', cond='attribute_name="'+keyList+'" and source_table_name="'+sourceTable+'" order by batch_id desc, run_id desc', fcond=False)
        if val is None:
          reqKeys.append(keyList)
	else:
	  cond.append(keyList+'>'+val[0])
    return cond,reqKeys

def main(sc, hc, sqlContext, args):

        #Initialize the variables
        entity=args.entity
        sourceTable=args.sourceTable
        destFile=args.destFile
        dataBase=args.db
        programId=args.programId
	runId=args.runId
	deltaKey=args.deltaKey
	asOfDate=args.asOfDate
	batchId=args.batchId
	
	#destFile='ingest'+sourceTable
		
        setup_logger(entity+'_log', Conf.HDP_COMMON_DATA_LOG +'/MySql_Info.log',logging.INFO)
        setup_logger(entity+'_debug', Conf.HDP_COMMON_DATA_LOG +'/MySql_Debug.log',logging.DEBUG)
        setup_logger(entity+'_stats', Conf.HDP_COMMON_DATA_LOG +'/'+entity+'_Stats_'+asOfDate+'_'+batchId+'_'+programId+'_'+runId+'.log')

        infoLog = logging.getLogger(entity+'_log')
        debugLog = logging.getLogger(entity+'_debug')
        statsLog = logging.getLogger(entity+'_stats')

        print(args)
        try:
	  condi,keyList=genWhereClause(deltaKey, sourceTable)
	  query="(SELECT * FROM "+sourceTable+" WHERE 1=1) "+sourceTable+"_alias"
	  whereClause=''.join(condi) if len(condi)==1 else ' AND '.join(condi)
	  print(whereClause)
	  if deltaKey!='FULLSCAN' and len(condi)>0:
 	     query="(SELECT * FROM "+sourceTable+" WHERE 1=1 AND "+whereClause+") "+sourceTable+"_alias"
          #hc.sql("use "+dataBase)
  	  statsLog.info('Job Name | Batch Id | Run Id | Prog Id | Prog type | Type of Data | Dataset Name | Rec count')
	  hc1 = HiveContext(sc)
	  sql_df=sqlContext.read.format("jdbc").options(url="jdbc:mysql://mcdhpoc3:3306/bbiuserDB", dbtable=query, user="bbiuser", password="BBIuser@123").load()
	  #sql_df=sqlContext.read.format("jdbc").options(url=mysql_conn_string, dbtable=query, user=mysql_user, password=spark_jdbc_poc_b64password).load()
	  sql_df.registerTempTable("temp_"+sourceTable)
	  #sqlContext.sql("SELECT * FROM temp_"+sourceTable).write.parquet(Conf.HDP_COMMON_DATA_STG+'/'+destFile+'_'+str(batch_id)+'_'+str(programId)+'_'+str(as_of_date))
	  
	  if deltaKey!='FULLSCAN' and sql_df.count()>1:
	   keyList=deltaKey.split(",")
	   des=sql_df.describe(keyList)
	   desMax=des.filter(des['summary']=='max')
	   for cols in keyList:
	    for x in desMax.select(cols).collect():
	       stmt='insert into cdcAttrVal (batch_id , run_id , prog_id , source_table_name , attribute_name , attribute_val , as_of_date) values ('+batchId+','+runId+','+programId+",'"+sourceTable+"','"+cols+"',"+x[0]+","+asOfDate+')'
	       print(stmt)
	       insertIntoTbl('cdcAttrVal',stmt)

	  #hc.sql("use "+dataBase)
          hc_df=hc.sql("SELECT * FROM temp_"+sourceTable)
	  #sql_df=sqlContext.read.format("jdbc").options(url="jdbc:mysql://mcdhpoc3:3306/bbiuserDB", user="bbiuser", password="BBIuser@123").sql("SELECT * FROM "+sourceTable+" WHERE 1=1").load()
	  print("Rec Cnt Written HCDF : %s",hc_df.count())
          statsLog.info(entity+'|'+batchId+'|'+runId+'|'+programId+'|PySpark|Input|'+sourceTable+'|'+str(hc_df.count()))
	  #sql_df.saveAsParquetFile(Conf.HDP_COMMON_DATA_STG+'/'+destFile+'_'+batch_id+'_'+programId+'_'+as_of_date+'.dat')
	  sql_df.write.mode('overwrite').parquet(Conf.HDP_COMMON_DATA_STG+'/'+destFile+'_'+str(programId))
	  print("Rec Cnt Written : %s",sql_df.count())
	  statsLog.info(entity+'|'+batchId+'|'+runId+'|'+programId+'|PySpark|Output|'+destFile+'_'+str(programId)+'|'+str(sql_df.count()))
          #hc_df.write.saveAsTable(dataBase+"."+destFile)

        except:
	  infoLog.info("Un handled exception :"+str(sys.exc_info()[1]))
	  logExcep(entity,sys.exc_info())
	  return 1
	  quit()
	logStatus(entity,asOfDate+"s run Completed.. Kindly Validate",batchId,programId,"Success",True)
        return 0

def parseArguments(parser):
    # Create argument parser
    #parser = argparse.ArgumentParser()

    # Positional mandatory arguments
    parser.add_argument("-s", "--sourceTable", help="Source Table")
    parser.add_argument("-d", "--destFile", help="Destination File")
    parser.add_argument("-e", "--entity", help="Entity Name")
    parser.add_argument("-b", "--batchId", help="Batch Id")
    parser.add_argument("-p", "--programId", help="Program Id")
    parser.add_argument("-a", "--asOfDate", help="As of date")

    # Optional arguments
    parser.add_argument("-m", "--db", help="Default Schema", default='bbiuserdb')
    parser.add_argument("-r", "--runId", help="Nth run for the batch", default=1)
    parser.add_argument("-k", "--deltaKey", help="Delta Key to determine if full table or recently inserted or modified records to be pulled", default='FULLSCAN')
    # Print version
    parser.add_argument("--version", action="version", version='%(prog)s - Version 1.0')

    # Parse arguments
    args = parser.parse_args()

    return args

if __name__ == '__main__':
    # Parse the arguments
    parser = argparse.ArgumentParser("Pull data from MySQL DB tables")
    args = parseArguments(parser)

    # Raw print arguments
    for a in args.__dict__:
        if args.__dict__[a]:
           print("Values provided for : "+ str(a) + ": " + str(args.__dict__[a]))
        else:
           parser.print_help()
	   logExcep(args.entity,sys.exc_info(),batch_id=args.batchId, prog_id=args.programId, track=True)
           sys.exit(0)
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    hc = SparkSession.builder.enableHiveSupport().getOrCreate()
    sys.exit(main(sc, hc, sqlContext, args))

