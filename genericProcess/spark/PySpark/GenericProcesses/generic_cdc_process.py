from __future__ import print_function
import os, sys, csv, logging, argparse, numpy
sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0])))
#sys.path.append("/home/mapr/hadoop/BBI/DEV_VAL/SparkStreaming/conf")
import Conf, log
'''import sys'''
from Conf import *
from common_functions import getBatchId, insertRunStatus as irs, updateRunStatus as urs, getCurTime, setup_logger, logExcep
from common_functions import *
from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext, SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, DoubleType

def setup_logger(logger_name, log_file, level=logging.INFO):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    fileHandler = logging.FileHandler(log_file, mode='w')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)

    l.setLevel(level)
    l.addHandler(fileHandler)
    l.addHandler(streamHandler)    
def printAval(flg,msgPart):
    if flg:
       msg=msgPart+" validated Successfully \n"
    else:
       msg=msgPart+" failed, Kindly validated \n"
    
    return msg

def validateColumns(productsHCBase, productsHCStg, myArgs):
    base_df=set(productsHCBase.columns)
    delta_df=set(productsHCStg.columns)
    flg=""
    msg=""
    updateAttr=myArgs.updateAttr.split(",")
    partAttr=myArgs.partAttr.split(",")
    keys=myArgs.key.split(",")
    attrList=sorted(base_df.intersection(delta_df))
    updateAttr=attrList if updateAttr == ['NONE'] else updateAttr
    print("Update Attr",updateAttr)
    flg=len(attrList) == len(base_df) & len(attrList) == len(delta_df)
    if flg:
       msg="Base Table Attr in line with Delta Table Attr \n"
    else:
       msg="Mismatch in the attribute list between  Base Table and Delta Table \n"

    uflag=len(set(updateAttr).intersection(attrList))== len(updateAttr)
    msg=msg+printAval(uflag,"Update Attributes")
    pflag=len(set(partAttr).intersection(attrList))== len(partAttr)
    msg=msg+printAval(pflag,"Partition Attributes")
    kflag=len(set(keys).intersection(attrList))== len(keys)
    flg=flg & uflag & pflag & kflag
    msg=msg+printAval(kflag,"Key Attributes")

    return flg,msg,attrList,updateAttr

def framePart(pattr,part):
    cnt=0
    l=[]
    for partCol in pattr:
      l.append(partCol+"="+str(part[cnt]))
      cnt=cnt+1
    return l

def frameTuple(rec,dList):
    l=[]
    for item in dList: 
      l.append(rec[item])
    return tuple(l)

def compPart(rec,pattr,partitionLists):
    #tuple to compare
    #(numpy.array(frameTuple(rec[1],pattr)) == partitionLists
    for partitions in partitionLists:
      #print(partitions)
      if ((numpy.array(frameTuple(rec[1],pattr)) == partitions).all()):
        return True

def diffRec(rec1,rec0,uattrList):
    flg='UC'
    cnt=0
    recDisp=rec0
    for rec1Data in uattrList:
        if(rec1[rec1Data] != rec0[rec1Data]):
            flg='U'
            recDisp=rec1
    return flg,recDisp

def main(sc, hc, args):

	#Initialize the variables
	entity=args.entity
	baseTable=args.baseTable
	deltaTable=args.deltaTable
	dataBase=args.db
	batchId=args.batchId
	progId,runId=args.progId.split(":")
	asOfDate=args.asOfDate
	uattr=args.updateAttr.split(",")
	pattr=args.partAttr.split(",")
	keys=args.key.split(",")

	setup_logger(entity+'_log', Conf.HDP_COMMON_DATA_LOG +'/'+entity+'_CDC_Info_'+asOfDate+'_'+batchId+'_'+progId+'.log',logging.INFO)
	setup_logger(entity+'_debug', Conf.HDP_COMMON_DATA_LOG +'/'+entity+'_CDC_Debug_'+asOfDate+'_'+batchId+'_'+progId+'.log',logging.DEBUG)
	setup_logger(entity+'_stats', Conf.HDP_COMMON_DATA_LOG +'/'+entity+'_Stats_'+asOfDate+'_'+batchId+'_'+progId+'_'+runId+'.log')
	infoLog = logging.getLogger(entity+'_log')
        debugLog = logging.getLogger(entity+'_debug')
        statsLog = logging.getLogger(entity+'_stats')

	print(args)
	print(entity,baseTable,deltaTable,dataBase,uattr,pattr,keys)
	hc.sql("use "+dataBase)
	hc.sql("show databases").show()
        hc.sql("drop table IF EXISTS "+dataBase+".temptable_"+deltaTable)
        productsHCBase=hc.sql("select * from "+dataBase+"."+baseTable)
        productsHCStg=hc.sql("select * from "+dataBase+"."+deltaTable)
	flg,msg,attrList,updateAttr=validateColumns(productsHCBase, productsHCStg, args)
	print("Flag :",flg)
	print("Message :", msg)
	print(attrList)
	if flg:
	 print(attrList)
	else:
	 infoLog.info("Attribute validation failed")
	 infoLog.info(msg)
	 quit()
 
        productsBaseRDD=productsHCBase.rdd
        productsRDD=productsHCStg.rdd
        infoLog.info("RDD - read operation completed for productsBase and products dataset")
	cols=productsHCBase.columns

	statsLog.info('Job Name | Batch Id | Run Id | Prog Id | Prog type | Type of Data | Dataset Name | Rec count')
	statsLog.info(entity+'|'+batchId+'|'+runId+'|'+progId+'|PySpark|Input1|'+baseTable+'|'+str( productsBaseRDD.count()))
	statsLog.info(entity+'|'+batchId+'|'+runId+'|'+progId+'|PySpark|Input2|'+deltaTable+'|'+str( productsRDD.count()))
        #Key Value Pair of the datasets
        #kv_base=productsBaseRDD.map(lambda brec: ((brec.product_id,brec.product_category_id),brec))
	kv_base=productsBaseRDD.map(lambda brec: (frameTuple(brec,keys),brec ))
        #kv_new=productsRDD.map(lambda nrec: ((nrec.product_id,nrec.product_category_id),nrec))
        kv_new=productsRDD.map(lambda nrec: (frameTuple(nrec,keys), nrec ))
        infoLog.info("Key Value Pair has been created")

        #Full Outer Join
        fullOuterJn_base_new=kv_base.fullOuterJoin(kv_new)
        #fullOuterJn_base_new.map(lambda rec: (int(rec[0][0]),rec)).sortByKey(False).collect()
        infoLog.info("Performing fullouter join Base on Delta/New")

        #Identify the Inserts, Deletes & Updates
        insertRec_base_new=fullOuterJn_base_new.filter(lambda rec: rec[1][0]==None).map(lambda rec: ("I",rec[1][1]))
        infoLog.info("Identified the Insert Records & captured in Debug Log : "+Conf.HDP_COMMON_DATA_LOG +'/'+entity+'CDC_Debug_'+asOfDate+'_'+batchId+'_'+progId+'.log')
        debugLog.debug("Following are the list of records identified for Insert : \n %s",insertRec_base_new.collect())

        deleteRec_base_new=fullOuterJn_base_new.filter(lambda rec: rec[1][1]==None).map(lambda rec: ("D",rec[1][0]))
        infoLog.info("Identified the Records to be Deleted")
        debugLog.debug("Following are the list of records identified for deletion : \n %s",deleteRec_base_new.collect())

        possibleChangesRec=fullOuterJn_base_new.filter(lambda rec: (rec[1][0]!=None and rec[1][1]!=None)).map(lambda rec: rec[1])
        infoLog.info("Identified the possible Update and UnChanged data")

        infoLog.info("Mark the Updates and UnChanged records")
        updateRec_base_new=possibleChangesRec.map(lambda rec: diffRec(rec[1],rec[0], updateAttr))
        debugLog.debug("Following is the list of records identified to be updated : \n %s ",updateRec_base_new.filter(lambda rec: rec[0]=='U').collect())

        #Identify the distinct partitions in update file
        #identifyPartitions=updateRec_base_new.filter(lambda urec: urec[0]=='U').map(lambda rec: rec[1][1])
        identifyPartitions=updateRec_base_new.filter(lambda urec: urec[0]=='U').map(lambda rec: frameTuple(rec[1],pattr))
        partitionLists=sorted(set(identifyPartitions.collect()))
        debugLog.debug("Identified the distinct Partitions %s", partitionLists)

	#df=insertRec_base_new.union(updateRec_base_new).map(lambda x : x[1]).toDF()
	#myrdd=insertRec_base_new.union(updateRec_base_new.filter(lambda rec: (numpy.array(frameTuple(rec[1],pattr)) == partitionLists).all()))
	myrdd=insertRec_base_new.union(updateRec_base_new.filter(lambda rec: compPart(rec,pattr,partitionLists)))
	infoLog.info("Combined both Insert and Update records")

	#Identify the distinct partitions in update file
        identifyPartitions=myrdd.filter(lambda urec: urec[0]=='U' or urec[0]=='I' ).map(lambda rec: frameTuple(rec[1],pattr))
        partitionLists=sorted(set(identifyPartitions.collect()))
        debugLog.debug("Identified the distinct Partitions %s", partitionLists)

	statsLog.info(entity+'|'+batchId+'|'+runId+'|'+progId+'|PySpark|Process (Post Join)|Inserts|'+str(insertRec_base_new.count()))
	statsLog.info(entity+'|'+batchId+'|'+runId+'|'+progId+'|PySpark|Process (Post Join)|Deletes|'+str(deleteRec_base_new.count()))
	statsLog.info(entity+'|'+batchId+'|'+runId+'|'+progId+'|PySpark|Process (Post Join)|Updates|'+str(updateRec_base_new.filter(lambda rec: rec[0]=='U').count()))
	myrdd=myrdd.map(lambda rec: frameTuple(rec[1],cols))
	#debugLog.debug("MyRdd %s",myrdd.collect())
	#reading the schema
	#product_id, product_category_id, product_name, product_description, product_price, product_image
	#structInfo = StructType([StructField("product_id", IntegerType(), True),
	#StructField("product_category_id", IntegerType(), True),
	#StructField("product_name", StringType(), True),
	#StructField("product_description", StringType(), True),
	#StructField("product_price", DoubleType(), True),
	#StructField("product_image", StringType(), True)])
	#df=hc.createDataFrame(myrdd.map(lambda x : x[1]),structInfo)
        structInfo = productsHCBase.schema
        df=myrdd.toDF(schema=structInfo)
	df.write.partitionBy(pattr[0:]).saveAsTable(dataBase+".temptable_"+deltaTable)
	#df.write.partitionBy("product_category_id", "product_id").saveAsTable(dataBase+".temptable_"+deltaTable)
	infoLog.info("Created a temp partition table in same db")
	debugLog.debug('df.write.partitionBy('+",".join(pattr[0:])+').saveAsTable(dataBase+".temptable_"+deltaTable)')
	dpHDP="use "+dataBase+";\n"
	epHDP="\n"
	
	statsLog.info(entity+'|'+batchId+'|'+runId+'|'+progId+'|PySpark|Output (In DF)|DataFrame|'+str(df.count()))
	with open(HDP_COMMON_DATA_TRAN+'/'+'hive-cdc_'+asOfDate+'_'+batchId+'_'+runId+'.hql','a+')as f:
	 f.write(dpHDP)
	 for part in partitionLists:
	  #dp="ALTER TABLE "+baseTable+" DROP IF EXISTS PARTITION (product_category_id="+str(part)+"); \n\n"
	  dp="ALTER TABLE "+baseTable+" DROP IF EXISTS PARTITION ("+",".join(framePart(pattr,part))+"); \n\n"
	  dpHDP=dpHDP+dp
	  f.write(dpHDP)
	  dpHDP=""
	  #hc.sql(dp)
	  debugLog.debug('Executed Drop Partition : %s ',dp)
	  infoLog.info('Drop Partition completed for %s partition',part)
	  ep="ALTER TABLE "+baseTable+" EXCHANGE PARTITION ("+",".join(framePart(pattr,part))+") WITH TABLE temptable_"+deltaTable+"; \n\n"
	  epHDP=ep+epHDP
	  f.write(ep)
	  #hc.sql(ep)
	  debugLog.debug('Executed Exchane Partition : %s ', ep)
	  infoLog.info('Exchange Partition completed for %s partition',part)
	 #f.read()
	 f.close()

	#Collecting the stats
	statsLog.info("Total number of records : ")
	statsLog.info("	        to be inserted : %s", insertRec_base_new.count())
	statsLog.info("	        to be updated  : %s", updateRec_base_new.filter(lambda rec: rec[0]=='U').count())
	statsLog.info("	        to be deleted  : %s", deleteRec_base_new.count())
	
	logStatus("CDC-"+entity,asOfDate+" CDC Process Completed Scuccessfully",batchId,progId,'Success',True)
	return 0


def parseArguments(parser):
    # Create argument parser
    #parser = argparse.ArgumentParser()

    # Positional mandatory arguments
    parser.add_argument("-b", "--baseTable", help="BaseTable")
    parser.add_argument("-d", "--deltaTable", help="Delta Table")
    parser.add_argument("-e", "--entity", help="Entity Name")
    parser.add_argument("-k", "--key", help="Key list")
    parser.add_argument("-p", "--partAttr", help="Attributes on which BaseTable is partitioned")
    parser.add_argument("-r", "--batchId", help="Get Batch Id from the operational tables")
    parser.add_argument("-o", "--progId", help="Get Prog Id from the operational tables")
    parser.add_argument("-a", "--asOfDate", help="Get As Of Date from the operational tables")


    # Optional arguments
    parser.add_argument("-s", "--db", help="Default Schema", default='bbiuserdb')
    parser.add_argument("-u", "--updateAttr", help="Identifies if any changes are on these attributes", default='NONE')

    # Print version
    parser.add_argument("--version", action="version", version='%(prog)s - Version 1.0')


    # Parse arguments
    args = parser.parse_args()

    return args

if __name__ == '__main__':
    # Parse the arguments
    parser = argparse.ArgumentParser("Perform CDC between Base and Staging table")
    args = parseArguments(parser)

    # Raw print arguments
    print("You are running the script with arguments: ")
    print(args.__dict__)
    for a in args.__dict__:
	if args.__dict__[a]:
	   print("Values provided for : "+ str(a) + ": " + str(args.__dict__[a]))
	else:
	   parser.print_help()
	   sys.exit(0)
    sc = SparkContext()
    hc = SparkSession.builder.enableHiveSupport().getOrCreate()
    try:
      main(sc, hc, args)
    except:
      logExcep(args.entity,sys.exc_info(),args.batchId,args.progId,'Failure',True)
      sys.exit(1)

