import pandas as pd
import os, sys, csv, argparse, re, base64
from pandas import ExcelWriter, ExcelFile  
from common_functions import *
from credentials_b64 import *

mysqlHost=mysql_host
mysqlUser=mysql_user
mysqlPasswd=base64.b64decode(spark_jdbc_poc_b64password)
mysqlDB=mysql_db

def main(args):
    xlsFP = args.xlsFilePath
    jd_df = pd.read_excel(xlsFP,sheet_name='jobDependency')
    pd_df = pd.read_excel(xlsFP,sheet_name='progDetails')
    cols = pd_df.dtypes.index   
    dfInsertIntoTbl('progDetails',pd_df,mysqlHost=mysqlHost, mysqlUser=mysqlUser, mysqlPasswd=mysqlPasswd, mysqlDB=mysqlDB)
    dfInsertIntoTbl('jobDependency',jd_df,mysqlHost=mysqlHost, mysqlUser=mysqlUser, mysqlPasswd=mysqlPasswd, mysqlDB=mysqlDB)
    #xls = pd.ExcelFile('/data/apps/landing/Operational_Data_Entries.xlsx')
    #xls.sheet_names

def parseArguments(parser):
    # Positional mandatory arguments
    parser.add_argument("-x", "--xlsFilePath", help="Absolute Xls FileName - File to be parsed")
    # Optional arguments
    parser.add_argument("-d", "--db", help="DB details", default='bbiuserDB')
    # Print version
    parser.add_argument("--version", action="version", version='%(prog)s - Version 1.0')
    # Parse arguments
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    # Parse the arguments
    parser = argparse.ArgumentParser("Read Excel Content and Load to Operational Table")
    args = parseArguments(parser)

    # Raw print arguments
    print("You are running the script with arguments: ")
    for a in args.__dict__:
        if args.__dict__[a]:
           print("Values provided for : "+ str(a) + ": " + str(args.__dict__[a]))
        else:
           parser.print_help()
           sys.exit(0)
    main(args)

