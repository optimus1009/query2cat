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
    status=`partition_exist ${bbdw_dwd_search_log_i_d} ${bizdate} ${final_status}`
    if [ $status == 0 ]; then 
        break;
    fi
    sleep 1200 # wait 20 mins
done

# create query dict table
hive -e "
create table if not exists ${termweight_query_data} ( 
    raw_type    string,
    raw_info    string,
    raw_query   string,
    term_query  string,
    tw_query    string 
) partitioned by (pt bigint) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE
TBLPROPERTIES('LIFECYCLE'='200d')
;"

for idx in `seq 0 179` 
do
    dt=`date -d"${bizdate} -${idx} days" '+%Y%m%d'`
    partitions=`hive -e "show partitions ${termweight_query_data}"`
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
    drop table if exists ${termweight_query_data_tmp1};
    create table if not exists ${termweight_query_data_tmp1} as 
    select 
        coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])) as key_word 
        ,count(1) as show_pv
        ,count(distinct concat(imei,imsi,device_id,userid)) as show_uv
    from ${bbdw_dwd_search_log_i_d} 
    where pt = ${dt} 
        and event_type = 'list_show'
        and (
           kv['page'] in ('搜索结果页') 
           or kv['name'] in ('搜索结果页_商品list_曝光')
           or kv['e_name'] in ('搜索结果页_商品list_曝光','分类搜索结果页_商品list_曝光')
           or kv['e_name'] in ('搜索无结果页_推荐商品list_曝光','搜索结果页_零结果_推荐商品list_曝光') 
           or kv['e_name'] in ('搜索结果页_少结果_推荐商品list_曝光')
         )
        and ( kv['keyWord'] is not null 
            or kv['key_word'] is not null
            or kv['keyword'] is not null
                     )
        and coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])) != '' 
    group by 
        coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])) 
    order by show_pv desc 
    ;"

    rm -rf ${raw_query_data}

    hive -e "
    select distinct 
        '1' as raw_type,
        '1' as raw_info,
        key_word
    from ${termweight_query_data_tmp1}
    where key_word is not null 
        and lower(key_word) != 'null'
        and length(key_word) > 0
    ;" > ${raw_query_data} 
    rm -rf ${termweight_query_data_file}.tmp  
    if [ ! -s ${online_conf_path}/common_config.sh ]; then 
        cd /home/deni.li/dev/recom/ad-search-qp/trunk/termWeight/termweight2.5.x/java
        make clean 
        make 
        time java -Xmx1g -Xms1g -cp bin -Djava.library.path=bin com.tw.Test ${raw_query_data} > ${termweight_query_data_file}.tmp 
        cd -
    else 
        cd /home/deploy/data/recom/beibei/ad-search-qp/trunk/termWeight
        #rm -rf termWeight2.5.x 
        #tar -zxvf termweight2.5.x.tar 
        cd -
        cd /home/deploy/data/recom/beibei/ad-search-qp/trunk/termWeight/termweight2.5.x/java-online
        #make 
        time java -Xmx1g -Xms1g -cp bin -Djava.library.path=bin com.tw.Test ${raw_query_data} > ${termweight_query_data_file}.tmp 
        cd -
    fi

    rm -rf ${termweight_query_data_file}  && \
    cat ${termweight_query_data_file}.tmp | awk -F'\t' 'NF==5 {print $0}' > ${termweight_query_data_file} && \
    query_cnt=`cat ${termweight_query_data_file} | wc -l` && \

    hive -e "
    alter table ${termweight_query_data} drop if exists partition (pt= ${dt}); 
    load data local inpath '${termweight_query_data_file}' overwrite into table ${termweight_query_data} partition (pt = ${dt});"
    hadoop fs -touchz /user/deni.li/done/${termweight_query_data}.done.${dt}   
done

del_done=`date -d"${bizdate} -180 days" '+%Y%m%d'`
hive -e "alter table ${termweight_query_data} drop if exists partition (pt = ${del_done});"
hadoop fs -rm -f /user/deni.li/done/${termweight_query_data}.done.${del_done}

echo "END_OF_${bizdate} ========================================================================================================="
echo "==========================================================================================================================="

exit 0
