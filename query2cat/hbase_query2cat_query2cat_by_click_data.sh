#!/bin/sh 

set -e -x

online_conf_path="/home/deploy/data/recom/beibei/ad-search-qp/trunk/query2cat"
if [ -s ${online_conf_path}/common_config.sh ]; then 
    source ${online_conf_path}/common_config.sh 
else 
    source ./common_config.sh
fi

# define hbase table  
<<!
hbase shell << EOF
disable '${query2cat_by_click_data_hbase}';
drop '${query2cat_by_click_data_hbase}';
create '${query2cat_by_click_data_hbase}', {NAME => 'rank_features', BLOOMFILTER => 'ROW', VERSIONS => 3, COMPRESSION => 'SNAPPY', TTL => 864000, BLOCKCACHE => true};
EOF
!

hive -e " 
create table if not exists ${query2cat_by_click_data}_backup (
    rowkey      string comment 'base64_termquery',
    cate_info   string 
) partitioned by (pt bigint) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
TBLPROPERTIES('LIFECYCLE'='30d')
;"

hive -e "
add file ${CURRENT_DIR}/app/query2cat_query2cat_by_click_entropy.py;
alter table ${query2cat_by_click_data}_backup add if not exists partition (pt =${bizdate});
insert overwrite table ${query2cat_by_click_data}_backup partition (pt = ${bizdate}) 
select transform(base64_term_query, leaf_cate_score, cate2_score, cate1_score)
using 'python query2cat_query2cat_by_click_entropy.py'
from ${query2cat_by_click_data_tmp2} 
;"

hive -e "
drop table if exists ${query2cat_by_click_data};
create external table if not exists ${query2cat_by_click_data} (
    rowkey      string comment 'base64_termquery',
    cate_info   string 
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties ('hbase.columns.mapping' = ':key,
	rank_features:cate_info#b') TBLPROPERTIES ('hbase.table.name' = '${query2cat_by_click_data_hbase}')
;"

# query2cat by click  
hive -e "
set mapreduce.job.name=${mapreduce_job_name_prefix}${query2cat_by_click_data};
add file ${CURRENT_DIR}/app/query2cat_query2cat_by_click_entropy.py;
insert overwrite table ${query2cat_by_click_data} 
select
    rowkey,
    cate_info 
from ${query2cat_by_click_data}_backup 
where pt = ${bizdate} 
;
"

exit 0 
