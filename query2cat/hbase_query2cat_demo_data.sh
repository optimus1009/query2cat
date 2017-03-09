#!/bin/sh 

set -e -x

online_conf_path="/home/deploy/data/recom/beibei/ad-search-qp/trunk/query2cat"
if [ -s ${online_conf_path}/common_config.sh ]; then 
    source ${online_conf_path}/common_config.sh 
else 
    source ./common_config.sh
fi

<<!
# load uid_eventid iid sim_score data into hbase
hbase shell << EOF
disable '${query2cat_random_query_demo_data_hbase}';
drop '${query2cat_random_query_demo_data_hbase}';
create '${query2cat_random_query_demo_data_hbase}', {NAME => 'rank_features', BLOOMFILTER => 'ROW', VERSIONS => 3, COMPRESSION => 'SNAPPY', TTL => 259200, BLOCKCACHE => true};
EOF
!

hive -e "
drop table if exists ${query2cat_random_query_demo_data};
create external table if not exists ${query2cat_random_query_demo_data} (
    rowkey string comment 'random', 
    iid_info string comment 'query,query'
) 
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties ('hbase.columns.mapping' = ':key,
	rank_features:query_info#b') TBLPROPERTIES ('hbase.table.name' = '${query2cat_random_query_demo_data_hbase}')
;
"

hive -e "
insert into table ${query2cat_random_query_demo_data} 
select 
    string('random_id') as rowkey,
    concat_ws(',', collect_set(term_query)) as query_info
from (
        select 
            t3.term_query,
            row_number() over (order by sum_click_score) as rank
        from 
        (
            select 
                t2.term_query, 
                sum(t2.click_score) as sum_click_score 
            from 
                (
                    select 
                        t1.term_query,
                        t1.click_score, 
                        row_number() over (partition by t1.term_query order by t1.click_score desc) as rank
                    from ${query_item_click_data_tmp3} t1 
                ) t2 
            where t2.rank <= 5
            group by t2.term_query 
        ) t3
    ) t 
where rank <= 60000 
    and rank%10 = 7 
;"

exit 0
