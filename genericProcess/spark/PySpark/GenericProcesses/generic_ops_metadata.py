import os, sys, csv, logging, argparse
sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0])))
sys.path.append(".")
import  log
from common_functions import *
from Conf import *

from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext, SparkSession
#from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, DoubleType

import time, datetime
from datetime import datetime
from threading import Timer

def generateCommandLine(progId,batchId,jobName,asOfDate,runId):
    progDetails=selValues("progDetails","program_id,program_type,program_location,script_name,arguments,jar_files","program_id="+str(progId),True)
    argDetails=selValues("jobDependency","arguments","job_name='"+jobName+"' and program_id="+str(progId),True)
    options={'pyspark': framePySpark,'hive':frameHive,'mysql-pull':frameMySQLPull}
    print progDetails
    programType, progLoc, scriptName, args, jarFiles = str(progDetails[0][1]).split(":")[-1], str(progDetails[0][2]), str(progDetails[0][3]), str(argDetails[0][0]), str(progDetails[0][5])
    #programType, progLoc, scriptName, args, jarFiles = str(progDetails[0][1]), str(progDetails[0][2]), str(progDetails[0][3]), str(progDetails[0][4]), str(progDetails[0][5])
    if args:
        args=args.replace('12345',str(batchId)).replace('101',str(progId)).replace('99991231',asOfDate).replace('000',str(runId))
    #print(options[programType](progLoc,scriptName,args,jarFiles))
    return options[programType](progLoc,scriptName,batchId,runId,progId,asOfDate,args,jarFiles)

def framePySpark(progLoc,scriptName,batchId,runId,progId,asOfDate,args="",jarFiles=""):
    progNm="spark-submit "+progLoc+"/"+scriptName
    jarDetails=" "
    if jarFiles=="":
      jarDetails=" --jars "+jarFiles

    return progNm+" "+args+jarDetails

def frameHive(progLoc,scriptName,batchId,runId,progId,asOfDate,args="",jarFiles=""):
    progNm="hive -f "+HDP_COMMON_DATA_TRAN+"/"+scriptName.replace('12345',str(batchId)).replace('99991231',asOfDate).replace('000',str(runId))
    return (progNm)    

def frameMySQLPull(progLoc,scriptName,batchId,runId,progId,asOfDate,args="",jarFiles=""):
    progNm="spark-submit "+progLoc+"/"+scriptName
    
    #--sourceTable products_demo  --destFile ingestProducts -e Products -b 12345 -p 101 -f abcd.dat --db bbiuserdb --asOfDate 99991231 -r 000
    #--sourceTable --destFile 'ingest'+entity -e jobName -b batchId -p progId --asOfDate asOfDate -r runId

def parseArguments(parser):
    # Create argument parser
    # Positional mandatory arguments
    parser.add_argument("-b", "--batchId", help="Batch Id")
    parser.add_argument("-j", "--jobName", help="Job Name")
    parser.add_argument("-a", "--asOfDate", help="As of Date")

    # Optional arguments
    parser.add_argument("-m", "--db", help="Default Schema", default='bbiuserdb')
    parser.add_argument("-l", "--log", help="Default Log File dir & name", default='generic_process_')

    # Print version
    parser.add_argument("--version", action="version", version='%(prog)s - Version 1.0')

    # Parse arguments
    args = parser.parse_args()

    return args

def main(args):
    #Populate Batch id
    print("Inside OpsMetadata")
    batchId=args.batchId
    jobName=args.jobName
    asOfDate=args.asOfDate
    logDetails=args.log
    wfh=open(logDetails,'w+')
    runId=1
    status=False
    posStatus=['Initiated','Failed']
    print("BatchId "+str(batchId))
    if int(batchId)==0:
      batchId=getBatchId(asOfDate,jobName)
      print("BatchId"+str(batchId))
    else:
      runId=int(selValues("runStatus","max(run_id)+1","batch_id="+str(batchId),False)[0])
    insertRunStatus(batchId,runId,jobName,asOfDate)
    for progIdList,order_seq in selValues("jobDependency","cast(program_id as unsigned),cast(exec_order_seq as unsigned)","job_name='"+jobName+"' ORDER BY exec_order_seq", True):
      wfh.write("Program Id: "+str(progIdList)+" Order Seq: "+str(order_seq)+" Current Status: "+str(status))
      print int(progIdList),int(order_seq),status
      cmd=generateCommandLine(int(progIdList),batchId,jobName,asOfDate,runId)
      print("My CMD: ",cmd)
      wfh.write("\n\t |-- Command being executed: \n\t"+ cmd)
      print("===================================================="+str(int(progIdList))+"=================================================================")
      if order_seq==1 or status:
        executeCommand(cmd,wfh,batchId,int(progIdList),runId)
        status=str(selValues("runStatus","program_status","batch_id="+str(batchId)+" and run_id="+str(runId)+" and program_id="+str(progIdList), True)[0][0]) not in posStatus
      print("=============================================================="+str(status)+"========================================================================")
      wfh.write("=============================================================="+str(status)+"========================================================================")
    wfh.close()
if __name__ == '__main__':
    # Parse the arguments
    parser = argparse.ArgumentParser("Pull data from MySQL DB tables")
    args = parseArguments(parser)
    for a in args.__dict__:
        if args.__dict__[a]:
           print("Values provided for : "+ str(a) + ": " + str(args.__dict__[a]))
        else:
           parser.print_help()
           sys.exit(0)
    try:
      main(args)
    except:
     print(sys.exc_info()[0])
     logExcep('OpsMetadata',sys.exc_info(),1,999,'Failure',False)
     sys.exit(1) 
#Populate Batch id
#Populate run status
