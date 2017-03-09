#!/bin/sh

set -e -x

online_conf_path="/home/deploy/data/recom/beibei/ad-search-qp/trunk/query2cat"
if [ -s ${online_conf_path}/common_config.sh ]; then 
    source ${online_conf_path}/common_config.sh 
else 
    source /home/deni.li/dev/recom/ad-search-qp/trunk/query2cat/common_config.sh
fi

echo "==========================================================================================================================="
echo "BEGIN_OF_${bizdate} ======================================================================================================="
while [ 1 -eq 1 ] 
do
    status=`partition_exist ${ods_ods_bb09_category_s_d} ${bizdate} ${final_status}`
    if [ $status == 0 ]; then 
        break;
    fi
    sleep 1200 # wait 20 mins
done

hive -e "
set mapreduce.job.queuename=root.bigdata;
drop table if exists ${termweight_cate_data_tmp1};
create table if not exists ${termweight_cate_data_tmp1} as 
select distinct 
    id as cate_id,
    name as cate_name,
    cate_type
from ${ods_ods_bb09_category_s_d} 
where pt = '${bizdate}' 
    and status = 1 
;"

rm -rf ${raw_cate_data}

hive -e "
select distinct 
    '2' as raw_type,
    concat_ws('_', string(cate_id), string(cate_type)) as raw_info,
    cate_name 
from ${termweight_cate_data_tmp1}
where cate_name is not null 
    and lower(cate_name) != 'null'
    and length(cate_name) > 0
;" > ${raw_cate_data} 

rm -rf ${termweight_cate_data_file}.tmp  
cd /home/deni.li/termweight2.5.x/java 
make clean; make; 
time java -Xmx1g -Xms1g -cp bin -Djava.library.path=bin com.tw.Test ${raw_cate_data} > ${termweight_cate_data_file}.tmp; cd -
if [ $? -ne 0 ]; then
    echo "ERROR"
fi

rm -rf ${termweight_cate_data_file}  && \
cat ${termweight_cate_data_file}.tmp | awk -F'\t' 'NF==5 {print $0}' > ${termweight_cate_data_file} && \
cate_cnt=`cat ${termweight_cate_data_file} | wc -l` && \

# create cate dict table
hive -e "
create table if not exists ${termweight_cate_data} ( 
    raw_type    string,
    raw_info    string,
    raw_cate    string, 
    term_cate   string, 
    tw_cate     string 
) partitioned by (pt bigint) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
;"

hive -e "
alter table ${termweight_cate_data} drop if exists partition (pt= ${bizdate});
load data local inpath '${termweight_cate_data_file}' overwrite into table ${termweight_cate_data} partition (pt = ${bizdate});"

hadoop fs -touchz /user/deni.li/done/${termweight_cate_data}.done.${bizdate}


del_done=`date -d"${bizdate} -180 days" '+%Y%m%d'`
hive -e "alter table ${termweight_cate_data} drop if exists partition (pt = ${del_done});"
hadoop fs -rm -f /user/deni.li/done/${termweight_cate_data}.done.${del_done}

echo "END_OF_${bizdate} ========================================================================================================="
echo "==========================================================================================================================="

exit 0
