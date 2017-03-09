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
    status=`partition_exist ${ods_ods_bb09_product_s_d} ${bizdate} ${final_status}`
    if [ $status == 0 ]; then 
        break;
    fi
    sleep 1200 # wait 20 mins
done

# create doc dict table
hive -e "
create table if not exists ${termweight_doc_data} ( 
    raw_type    string,
    raw_info    string,
    raw_doc     string,
    term_doc    string,
    tw_doc      string 
) partitioned by (pt bigint) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE
TBLPROPERTIES('LIFECYCLE'='200d')
;"

for idx in `seq 0 179` 
do
    dt=`date -d"${bizdate} -${idx} days" '+%Y%m%d'`
    partitions=`hive -e "show partitions ${termweight_doc_data}"`
    status=0
    for part in ${partitions}
    do
        if [ "${part}" == "pt=${dt}" ]; then
            status=1
            break
        fi
    done
    if [[ $status -eq 1 ]]; then
        continue
    fi
    hive -e "
    set mapreduce.job.queuename=root.bigdata;
    drop table if exists ${termweight_doc_data_tmp1};
    create table if not exists ${termweight_doc_data_tmp1} as 
    select distinct 
         id,
         cid,
         title
    from ${ods_ods_bb09_product_s_d} 
    where pt = ${dt} 
        and status = 1 
    ;"

    rm -rf ${raw_doc_data}

    hive -e "
    select distinct  
        '3' as raw_type,
        concat_ws('_', string(id), string(cid)) as raw_info,
        title 
    from ${termweight_doc_data_tmp1}
    where title is not null 
        and lower(title) != 'null'
        and length(title) > 0
    ;" > ${raw_doc_data} 

    rm -rf ${termweight_doc_data_file}.tmp  
    if [ ! -s ${online_conf_path}/common_config.sh ]; then 
        cd /home/deni.li/dev/recom/ad-search-qp/trunk/termWeight 
        rm -rf termWeight2.5.x 
        tar -zxvf termweight2.5.x.tar 
        cd -
        cd /home/deni.li/dev/recom/ad-search-qp/trunk/termWeight/termweight2.5.x/java
        make clean 
        make 
        time java -Xmx1g -Xms1g -cp bin -Djava.library.path=bin com.tw.Test ${raw_doc_data} > ${termweight_doc_data_file}.tmp 
        cd -
    else 
        cd /home/deploy/data/recom/beibei/ad-search-qp/trunk/termWeight
        #rm -rf termWeight2.5.x 
        #tar -zxvf termweight2.5.x.tar 
        cd -
        cd /home/deploy/data/recom/beibei/ad-search-qp/trunk/termWeight/termweight2.5.x/java-online
        #make 
        time java -Xmx1g -Xms1g -cp bin -Djava.library.path=bin com.tw.Test ${raw_doc_data} > ${termweight_doc_data_file}.tmp 
        cd -
    fi

    rm -rf ${termweight_doc_data_file}  && \
    cat ${termweight_doc_data_file}.tmp | awk -F'\t' 'NF==5 {print $0}' > ${termweight_doc_data_file} && \
    query_cnt=`cat ${termweight_doc_data_file} | wc -l` && \

    hive -e "
    alter table ${termweight_doc_data} drop if exists partition (pt= ${dt}); 
    load data local inpath '${termweight_doc_data_file}' overwrite into table ${termweight_doc_data} partition (pt = ${dt});"
    hadoop fs -touchz /user/deni.li/done/${termweight_doc_data}.done.${dt}   
done

del_done=`date -d"${bizdate} -180 days" '+%Y%m%d'`
hive -e "alter table ${termweight_doc_data} drop if exists partition (pt = ${del_done});"
hadoop fs -rm -f /user/deni.li/done/${termweight_doc_data}.done.${del_done}

echo "END_OF_${bizdate} ========================================================================================================="
echo "==========================================================================================================================="

exit 0
