import os, sys, csv, argparse, re

from Conf import *
from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext, SparkSession

def identifyByHeader(f,colDel):
  return len(next(csv.reader(f, delimiter=colDel)))

def parseData(f,colDel):
  arr={}
  for rec in csv.reader(f, delimiter=colDel):
    arr[len(rec)]=arr.setdefault(len(rec),0)+1
  return max(arr,key=arr.get)

def adjustNewline(wh,reader,numOfAttr,colDel,recDel,ignoreHdr):
  jnval=':jn:'
  jn=[jnval]
  hdrLine=next(reader) if ignoreHdr else reader
  for rec in reader:
    irec=rec
    while len(irec)<numOfAttr:
      irec=irec+jn+next(reader)
      irec=(((colDel.join(irec)).replace(colDel+jnval+colDel,'')).replace(colDel+jnval,'').replace(jnval+colDel,'')).split(colDel)
    wh.write(colDel.join(irec)+recDel)
  return

def getDataFrame(sqlContext, myList, headerExist):
  header_rec = myList[0] if headerExist else []
  data_rec = myList[1:] if headerExist else myList
  df = sqlContext.createDataFrame(data_rec, header_rec)
  #myrdd = spark.parallelize(data_rec)
  #df = myrdd.toDF(header_rec)
  return df

def readCSVWriteHive(sqlContext, fileHandle, sppversion, colDel, dataBase, hiveTable, removeChar='\n', headerExist=True):
  dataList=[]
  for row in csv.reader(fileHandle, delimiter=colDel, quotechar='"'):
    dataList.append([re.sub("[^A-Z0-9a-z :;,.<>?/'*&^+-}{\]\[%$#@!|\~`\"]","",w).replace(removeChar,'') for w in row])
  print(dataList)
  df = getDataFrame(sqlContext,dataList,headerExist)
  #df.write.mode('overwrite').saveAsTable(dataBase+".temptable_"+hiveTable)
  df.write.mode('overwrite').parquet(HDP_COMMON_DATA_STG+'/'+hiveTable)
  

def main(args, sc, hc):
  absFile=args.fileToClean
  hdrBool=args.headerAvail
  colDel=args.delimiter
  ignoreHdr=args.ignoreHeader
  tmpFile=args.outFile
  recDel='\n'
  dataBase='bbiuserdb'
  hiveTable='ingest'+tmpFile
  fh=open(absFile,'rb')
  numOfAttr=identifyByHeader(fh,colDel) if hdrBool else parseData(fh,colDel)
  fh.close()
  fh=open(absFile,'rb')
  sqlContext = SQLContext(sc)
  sqlContext.sql("drop table IF EXISTS "+dataBase+".temptable_"+hiveTable)
  readCSVWriteHive(sqlContext, fh, 1.6, colDel=colDel, headerExist=hdrBool, dataBase=dataBase, hiveTable=hiveTable) 
  #wh.close()
  fh.close()

def parseArguments(parser):
    # Positional mandatory arguments
    parser.add_argument("-f", "--fileToClean", help="Absolute FileName - File to be cleansed")
    parser.add_argument("-t", "--headerAvail", help="True/False - Data to be cleansed contains header record (Attribute Names)")
    parser.add_argument("-d", "--delimiter", help="[,:|] Column Delimiter of the data file")
    # Optional arguments
    parser.add_argument("-o", "--outFile", help="Output File", default='tmp_file')
    parser.add_argument("-i", "--ignoreHeader", help="Ignore the First Record", default=True)
    # Print version
    parser.add_argument("--version", action="version", version='%(prog)s - Version 1.0')
    # Parse arguments
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    # Parse the arguments
    parser = argparse.ArgumentParser("Perform Cleansing of data specific to Client")
    args = parseArguments(parser)

    # Raw print arguments
    print("You are running the script with arguments: ")
    for a in args.__dict__:
        if args.__dict__[a]:
           print("Values provided for : "+ str(a) + ": " + str(args.__dict__[a]))
        else:
           parser.print_help()
           sys.exit(0)
    sc = SparkContext()
    hc = SparkSession.builder.enableHiveSupport().getOrCreate()

    main(args,sc,hc)
