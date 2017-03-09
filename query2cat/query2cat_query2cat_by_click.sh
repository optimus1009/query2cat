#!/bin/sh 

set -e -x

online_conf_path="/home/deploy/data/recom/beibei/ad-search-qp/trunk/query2cat"
if [ -s ${online_conf_path}/common_config.sh ]; then 
    source ${online_conf_path}/common_config.sh 
else 
    source ./common_config.sh
fi

# query2cat by click  
hive -e "
set mapreduce.job.name=${mapreduce_job_name_prefix}${query2cat_by_click_data_tmp1};
drop table if exists ${query2cat_by_click_data_tmp1};
create table if not exists ${query2cat_by_click_data_tmp1} as 
select 
    t1.term_query,
    t1.leaf_cate_score, 
    t2.cate2_score, 
    t3.cate1_score 
from 
    ( -- 叶子类目
        select
            t.term_query, 
            concat_ws(',', collect_set(concat_ws(':', string(t.leaf_cate_id), string(t.leaf_cate_name), string(t.click_score)))) as leaf_cate_score
        from (
                select 
                    term_query, 
                    leaf_cate_id, 
                    leaf_cate_name, 
                    click_score 
                from ${query_item_click_data_tmp3} 
                order by 
                    term_query, 
                    click_score desc
            ) t 
        group by t.term_query 
    ) t1 
left outer join 
    ( -- 二级类目
        select
            t.term_query,
            concat_ws(',', collect_set(concat_ws(':', string(t.cate2_id), string(t.cate2_name), string(t.click_score)))) as cate2_score
        from ( 
                select 
                    term_query, 
                    cate2_id, 
                    cate2_name, 
                    click_score 
                from ${query_item_click_data_tmp5} 
                order by 
                    term_query, 
                    click_score desc
            ) t 
        group by t.term_query 
    ) t2
on t1.term_query = t2.term_query 
left outer join 
    ( -- 一级类目
        select
            t.term_query,
            concat_ws(',', collect_set(concat_ws(':', string(t.cate1_id), string(t.cate1_name), string(t.click_score)))) as cate1_score
        from (
                select 
                    term_query, 
                    cate1_id, 
                    cate1_name, 
                    click_score 
                from ${query_item_click_data_tmp4} 
                order by 
                    term_query, 
                    click_score desc
            ) t 
        group by t.term_query 
    ) t3 
on t1.term_query = t3.term_query 
;
"

hive -e "
add jar ${CURRENT_DIR}/app/base64UDF-1.0-SNAPSHOT.jar;
create temporary function encodeBase64 as 'com.beibei.bigdata.udf.base64UDF.base64Encode'; 
set mapreduce.job.name=${mapreduce_job_name_prefix}${query2cat_by_click_data_tmp2};
drop table if exists ${query2cat_by_click_data_tmp2};
create table if not exists ${query2cat_by_click_data_tmp2} as 
select 
    encodebase64(t1.term_query) as base64_term_query, 
    t1.leaf_cate_score, 
    t2.cate2_score, 
    t3.cate1_score 
from 
    ( -- 叶子类目
        select
            t.term_query, 
            -- concat_ws(',', collect_set(concat_ws(':', string(t.leaf_cate_id), string(t.click_score)))) as leaf_cate_score
            concat_ws(',', collect_set(concat_ws(':', string(t.leaf_cate_id), string(t.leaf_cate_name), string(t.click_score)))) as leaf_cate_score
        from (
                select 
                    term_query, 
                    leaf_cate_id, 
                    leaf_cate_name, 
                    click_score 
                from ${query_item_click_data_tmp3} 
                order by 
                    term_query, 
                    click_score desc
            ) t 
        group by t.term_query 
    ) t1 
left outer join 
    ( -- 二级类目
        select
            t.term_query,
            -- concat_ws(',', collect_set(concat_ws(':', string(t.cate2_id), string(t.click_score)))) as cate2_score
            concat_ws(',', collect_set(concat_ws(':', string(t.cate2_id), string(t.cate2_name), string(t.click_score)))) as cate2_score
        from ( 
                select 
                    term_query, 
                    cate2_id, 
                    cate2_name, 
                    click_score 
                from ${query_item_click_data_tmp5} 
                order by 
                    term_query, 
                    click_score desc
            ) t 
        group by t.term_query 
    ) t2
on t1.term_query = t2.term_query 
left outer join 
    ( -- 一级类目
        select
            t.term_query,
            -- concat_ws(',', collect_set(concat_ws(':', string(t.cate1_id), string(t.click_score)))) as cate1_score
            concat_ws(',', collect_set(concat_ws(':', string(t.cate1_id), string(t.cate1_name), string(t.click_score)))) as cate1_score
        from (
                select 
                    term_query, 
                    cate1_id, 
                    cate1_name, 
                    click_score 
                from ${query_item_click_data_tmp4} 
                order by 
                    term_query, 
                    click_score desc
            ) t 
        group by t.term_query 
    ) t3 
on t1.term_query = t3.term_query 
;
"

exit 0
