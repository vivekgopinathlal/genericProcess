#!/bin/sh

LANDING_DIR=${1:-"/data/apps/landing/watchDog"}
filePattern=${2:-"trigger.*.trig"}
logDir=<<logDir>>
sb=<<sbpath>>
curTime=$(date +"%Y%m%d%H%M%S")
startTime=$(date +"%Y%m%d080000")

cond=true

if [[ $curTime -lt $startTime ]]
then
	endTime=$(date +"%Y%m%d075900")
else
	endTime=$(date +"%Y%m%d075900" --date="tomorrow")
fi

echo $startTime" : "$curTime" : "$endTime >> ${logDir}/filePolling.log
delta=endTime-curTime

while(${cond})
do
if [[ $curTime -ge $endTime ]]
then
  echo "Time to exit..." | tee -a ${logDir}/filePolling.log
  mv ${logDir}/nohup_filepolling.log ${logDir}/nohup_filepolling_${curTime}.log
  exit 0
fi

for files in `find ${LANDING_DIR} -type f -name "${filePattern}"`
do

  echo "$files" | tee -a ${logDir}/filePolling.log
  echo $startTime" : "$curTime" : "$endTime >> ${logDir}/filePolling.log
  jobName=`echo $files | cut -d"." -f2`
  batchId=`echo $files | cut -d"." -f3`
  asofdate=`echo $files | cut -d"." -f4`
  echo "Trigg File has been registered :- moving it as COMPLETED"
  mv $files ${files}.COMPLETED
  echo "spark-submit ${sb}/generic_ops_metadata.py -b ${batchId} -j ${jobName} -a ${asofdate} -l ${logDir}/generic_process_${jobName}_${asofdate}_${curTime}.log 2>>${logDir}/generic_process_${jobName}_${asofdate}_${curTime}.log & >>${logDir}/generic_process_${jobName}_${asofdate}_${curTime}.log"
  spark-submit ${sb}/generic_ops_metadata.py -b ${batchId} -j ${jobName} -a ${asofdate} -l ${logDir}/generic_process_${jobName}_${asofdate}_${curTime}.log 2>>${logDir}/generic_process_${jobName}_${asofdate}_${curTime}.err & >>${logDir}/generic_process_${jobName}_${asofdate}_${curTime}.log
  #cond=false
done
sleep 5s
curTime=$(date +"%Y%m%d%H%M%S")
done
