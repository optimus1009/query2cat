#!/bin/sh 

#
set -e -x

online_conf_path="/home/deploy/data/recom/beibei/ad-search-qp/trunk/query2cat"
if [ -s ${online_conf_path}/common_config.sh ]; then 
    source ${online_conf_path}/common_config.sh 
else 
    source ./common_config.sh
fi

# new click data 
hive -e "
drop table if exists ${query2cat_query_brand_cate_info_tmp1};
create table if not exists ${query2cat_query_brand_cate_info_tmp1} as 
select 
    t1.brand as brand_id,
    t2.brand_name,
    t1.cid as leaf_cate_id,
    t1.prod_num
from 
    (
        select
            brand, 
            cid,
            count(distinct product_id) as prod_num
        from ods.ods_ba09_item_s_d 
        where pt = '${bizdate}' 
            and status = 1 
            and id is not null 
            and id > 0
            and product_id is not null 
            and product_id > 0  
            and cid is not null 
            and cid > 0 
            and brand is not null 
            and brand > 0 
        group by brand, cid
    ) t1 
join (
        select distinct 
            id as brand_id, 
            lower(raw_name) as brand_name 
        from  ods.ods_bb09_brand_detail_s_d
        lateral view explode(split(name, '\/')) col as raw_name
        where pt = '${bizdate}'
            and id is not null 
            and id > 0
            and name is not null 
            and name <> ''
            and raw_name is not null 
            and raw_name <> ''
    ) t2 
on t1.brand = t2.brand_id 
;
"

# 计算所属的叶子类目
hive -e "
drop table if exists ${query2cat_query_brand_cate_info_tmp2};
create table if not exists ${query2cat_query_brand_cate_info_tmp2} as 
select
    t1.brand_name,
    t1.leaf_cate_id,
    t2.leaf_cate_name, 
    t1.prod_num 
from ${query2cat_query_brand_cate_info_tmp1} t1
join ${cate_level_data} t2
on t1.leaf_cate_id = t2.leaf_cate_id 
distribute by t1.brand_name 
;
"

# 计算所属的2级类目
hive -e "
drop table if exists ${query2cat_query_brand_cate_info_tmp3};
create table if not exists ${query2cat_query_brand_cate_info_tmp3} as 
select
    t1.brand_name,
    t2.cate2_id,
    t2.cate2_name, 
    sum(t1.prod_num) as prod_num  
from ${query2cat_query_brand_cate_info_tmp1} t1
join ${cate_level_data} t2
on t1.leaf_cate_id = t2.leaf_cate_id 
group by t1.brand_name, t2.cate2_id, t2.cate2_name  
;
"

# 计算所属的1级类目
hive -e "
drop table if exists ${query2cat_query_brand_cate_info_tmp4};
create table if not exists ${query2cat_query_brand_cate_info_tmp4} as 
select
    t1.brand_name,
    t2.cate1_id,
    t2.cate1_name, 
    sum(t1.prod_num) as prod_num  
from ${query2cat_query_brand_cate_info_tmp1} t1
join ${cate_level_data} t2
on t1.leaf_cate_id = t2.leaf_cate_id 
group by t1.brand_name, t2.cate1_id, t2.cate1_name  
;
"

hive -e "
add jar ${CURRENT_DIR}/app/base64UDF-1.0-SNAPSHOT.jar;
create temporary function encodeBase64 as 'com.beibei.bigdata.udf.base64UDF.base64Encode'; 
drop table if exists ${query2cat_query_brand_cate_info_tmp5};
create table if not exists ${query2cat_query_brand_cate_info_tmp5} as 
select 
    encodebase64(t1.brand_name) as base64_brand_name, 
    t1.leaf_cate_score, 
    t2.cate2_score, 
    t3.cate1_score 
from 
    ( -- 叶子类目
        select
            t.brand_name, 
            concat_ws(',', collect_set(concat_ws(':', string(t.leaf_cate_id), string(t.leaf_cate_name), string(t.prod_num)))) as leaf_cate_score
        from (
                select 
                    brand_name, 
                    leaf_cate_id, 
                    leaf_cate_name, 
                    prod_num 
                from ${query2cat_query_brand_cate_info_tmp2} 
                order by 
                    brand_name, prod_num desc
            ) t 
        group by t.brand_name
    ) t1 
left outer join 
    ( -- 二级类目
        select
            t.brand_name,
            concat_ws(',', collect_set(concat_ws(':', string(t.cate2_id), string(t.cate2_name), string(t.brand_name)))) as cate2_score
        from ( 
                select 
                    brand_name, 
                    cate2_id, 
                    cate2_name, 
                    prod_num
                from ${query2cat_query_brand_cate_info_tmp3} 
                order by 
                    brand_name, prod_num desc
            ) t 
        group by t.brand_name 
    ) t2
on t1.brand_name = t2.brand_name 
left outer join 
    ( -- 一级类目
        select
            t.brand_name,
            concat_ws(',', collect_set(concat_ws(':', string(t.cate1_id), string(t.cate1_name), string(t.brand_name)))) as cate1_score
        from (
                select 
                    brand_name, 
                    cate1_id, 
                    cate1_name, 
                    prod_num 
                from ${query2cat_query_brand_cate_info_tmp4} 
                order by 
                    brand_name, prod_num desc
            ) t 
        group by t.brand_name
    ) t3 
on t1.brand_name = t3.brand_name 
;
"
#正常退出标志
exit 0
