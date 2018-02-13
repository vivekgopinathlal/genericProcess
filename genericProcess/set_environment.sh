export HDP_COMMON_DATA_LOG=/apps/data/mapr/schneider/poc/genericProcess/log
export HDP_COMMON_DATA_TEMP=/apps/data/mapr/schneider/poc/genericProcess/temp
export HDP_COMMON_DATA_ERR=/apps/data/mapr/schneider/poc/genericProcess/error
export HDP_COMMON_DATA_STG=/apps/data/mapr/schneider/poc/genericProcess/data/staging
export HDP_COMMON_DATA_TRAN=/apps/data/mapr/schneider/poc/genericProcess/data/transform
export HDP_COMMON_DATA_REP=/apps/data/mapr/schneider/poc/genericProcess/data/report
. /opt/mapr/spark/spark-2.1.0/conf/spark-env.sh
. /opt/mapr/hive/hive-2.1/conf/hive-env.sh
. /opt/mapr/hadoop/hadoop-0.20.2/conf/hadoop-env.sh
export PYSPARK_PYTHON=/usr/bin/python
export LANDING_DIR=<<landingdir>>
export SRC_LANDING_DIR=/data/apps/src_landing
export SBOX_DIR=<<sbpath>>
export HDP_CONF=<<sbpath>>/conf
export HDP_SQOOP=<<sbpath>>/sqoop
export HDP_SQL=<<sbpath>>/sql
export HDP_SCRIPTS=<<sbpath>>/scripts
export HDP_PIG=<<sbpath>>/pig
export HDP_HIVE=<<sbpath>>/hive
export HDP_FLUME=<<sbpath>>/flume
export HDP_DOC=<<sbpath>>/doc
export HDP_DML=<<sbpath>>/dml
export HDP_SPARK=<<sbpath>>/spark
