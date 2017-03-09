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
    status=`partition_exist ${ods_ods_bb09_brand_detail_s_d} ${bizdate} ${final_status}`
    if [ $status == 0 ]; then 
        break;
    fi
    sleep 1200 # wait 20 mins
done

hive -e "
set mapreduce.job.queuename=root.bigdata;
drop table if exists ${termweight_brand_data_tmp1};
create table if not exists ${termweight_brand_data_tmp1} as 
select 
    id as brand_id, 
    name as brand_name, 
    type as brand_type  -- 0默认，1国际知名品牌，2国内知名品牌，3天猫知名品牌，4天猫普通品牌，5其他  
from ${ods_ods_bb09_brand_detail_s_d} 
where pt = ${bizdate} 
;"

rm -rf ${raw_brand_data}

hive -e "
select distinct 
    '4' as raw_type,
    concat_ws('_', string(brand_id), string(brand_type)) as raw_info,
    brand_name as raw_brand 
from ${termweight_brand_data_tmp1}
where brand_name is not null 
    and lower(brand_name) != 'null'
    and length(brand_name) > 0
;" > ${raw_brand_data} 

rm -rf ${termweight_brand_data_file}.tmp  
cd /home/deni.li/termweight2.5.x/java 
make clean; make; 
time java -Xmx1g -Xms1g -cp bin -Djava.library.path=bin com.tw.Test ${raw_brand_data} > ${termweight_brand_data_file}.tmp; cd -
if [ $? -ne 0 ]; then
    echo "ERROR"
fi

rm -rf ${termweight_brand_data_file}  && \
cat ${termweight_brand_data_file}.tmp | awk -F'\t' 'NF==5 {print $0}' > ${termweight_brand_data_file} && \
query_cnt=`cat ${termweight_brand_data_file} | wc -l` && \

# create brand dict table
hive -e "
drop table if exists ${termweight_brand_data};
create table if not exists ${termweight_brand_data} ( 
    raw_type    string,
    raw_info    string,
    raw_brand   string,
    term_brand  string,
    tw_brand    string 
) partitioned by (pt bigint) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
;"

hive -e "
alter table ${termweight_brand_data} drop if exists partition (pt= ${bizdate}); 
load data local inpath '${termweight_brand_data_file}' overwrite into table ${termweight_brand_data} partition (pt = ${bizdate});"

hadoop fs -touchz /user/deni.li/done/${termweight_brand_data}.done.${bizdate}   


del_done=`date -d"${bizdate} -180 days" '+%Y%m%d'`
hive -e "alter table ${termweight_brand_data} drop if exists partition (pt = ${del_done});"
hadoop fs -rm -f /user/deni.li/done/${termweight_brand_data}.done.${del_done}
echo "END_OF_${bizdate} ========================================================================================================="
echo "==========================================================================================================================="

exit 0
