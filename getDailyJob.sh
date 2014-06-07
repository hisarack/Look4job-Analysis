#!/bin/bash

curYYMMDDHH=`/bin/date +"%y%m%d%H"`

tmpFolder=/usr/share/hisarack-jobfinder/tmp/
tmpFile="104-${curYYMMDDHH}.json"
dataFolder=/usr/share/hisarack-jobfinder/data/
dataFile="104-${curYYMMDDHH}.jfile"

url="http://www.104.com.tw/i/apis/jobsearch.cfm"
param="cat=2007001006&area=6001001000&role=1"

recordCount=`curl "$url?$param&fmt=2" | tr -d '\r'`
pageCount=$(($recordCount / 200))
pageCount=$(($pageCount + 1))

pageIndex=1
rowIndex=0
while [ $pageIndex -le $pageCount ]
do
   cols="ADDRESS,APPEAR_DATE,C,DESCRIPTION,J,JOB,JOBCAT_DESCRIPT,JOB_ADDRESS,LANGUAGE1,LANGUAGE2,LANGUAGE3,LINK,NAME,PRODUCT,SAL_MONTH_HIGHT,SAL_MONTH_LOW,STARTBY,PROFILE,WELFARE"
   curl  "$url?$param&page=$pageIndex&pgsz=200&fmt=8&cols=$cols" > $tmpFolder$tmpFile.$pageIndex
   java -cp /usr/share/hisarack-jobfinder/JobFinder-assembly-1.0.jar JSONSerializer $tmpFolder$tmpFile.$pageIndex $dataFolder$dataFile $rowIndex $curYYMMDDHH 
   pageIndex=$(($pageIndex + 1))
   rowIndex=$(($rowIndex + 200))
done

if [ ! -f ${dataFolder}summary.csv ]; then
   touch ${dataFolder}summary.csv
fi
echo "$dataFile,$recordCount" >> ${dataFolder}summary.csv

java -cp /usr/share/hisarack-jobfinder/JobFinder-assembly-1.0.jar JobAnalysis $dataFolder$dataFile

rm -rf $tmpFolder$tmpFile.*
