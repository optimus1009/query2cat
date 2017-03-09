#!/bin/sh 

set -e -x

online_conf_path="/home/deploy/data/recom/beibei/ad-search-qp/trunk/query2cat"
if [ -s ${online_conf_path}/common_config.sh ]; then 
    source ${online_conf_path}/common_config.sh 
else 
    source ./common_config.sh
fi

<<!
hive -e "
create table if not exists ${query_item_exp_data} (
    key_word string,
    pid      string,
    cid      string,
    uv_cnt   bigint 
) partitioned by (pt bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS SEQUENCEFILE
;"
!

<<!
hive -e "
set mapreduce.job.name=${mapreduce_job_name_prefix}${query_item_exp_data_tmp1};
drop table if exists ${query_item_exp_data_tmp1};
create table if not exists ${query_item_exp_data_tmp1} as 
select
    trim(split_item_id) as item_id
    ,coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])) as key_word 
    ,count(1) as show_pv
    ,count(distinct concat(imei,imsi,device_id,userid)) as show_uv
from ${bbdw_dwd_search_log_i_d}
lateral view explode(split(kv['ids'],',')) col as split_item_id
where pt='${bizdate}' 
    and ((version >= '4.1' and os = 'android') or (version >= '4.0' and os='ios'))
    and event_type = 'list_show'
    and (
           kv['page'] in ('搜索结果页') 
           or kv['name'] in ('搜索结果页_商品list_曝光')
           or kv['e_name'] in ('搜索结果页_商品list_曝光','分类搜索结果页_商品list_曝光')
           or kv['e_name'] in ('搜索无结果页_推荐商品list_曝光','搜索结果页_零结果_推荐商品list_曝光') 
           or kv['e_name'] in ('搜索结果页_少结果_推荐商品list_曝光')
        )
    and ( 
            kv['keyWord'] is not null 
            or kv['key_word'] is not null
            or kv['keyword'] is not null
        )
    and coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])) != '' 
    -- and kv['ids'] != '' 
    -- and split_item_id != '' 
group by 
    trim(split_item_id) 
    ,coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])) 
;
"

# raw click data
hive -e "
set mapreduce.job.name=${mapreduce_job_name_prefix}${query_item_click_data_tmp1};
drop table if exists ${query_item_click_data_tmp1};
create table if not exists ${query_item_click_data_tmp1} as 
select 
    coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])) as key_word
    ,trim(kv['item_id']) as item_id
    ,concat(imei,imsi,device_id,userid) as raw_uid
    ,count(1) as raw_pv_cnt
    ,min(round(ts/1000,0)) as ts
from ${bbdw_dwd_search_log_i_d}  
where pt='${bizdate}' 
   and ((version >= '4.1' and os = 'android') or (version >= '4.0' and os='ios'))
   and (event_type = 'event_click' and  kv['item_id'] != '') -- 4.2之后改成点击时必须带上item_id ,确保是商品点击事件
   and (
        kv['page'] in ('搜索结果页') 
        or kv['name'] in ('搜索结果页_商品list_点击')
        or kv['e_name'] in ('搜索结果页_商品list_点击','分类搜索结果页_商品list_点击')
        or kv['e_name'] in ('搜索结果页_零结果_推荐商品list_点击','搜索无结果页_推荐商品list_点击') -- ios 曝光和点击 android点击
        or kv['e_name'] in ('搜索结果页_少结果_推荐商品list_点击')
      )
   and ( kv['keyWord'] is not null 
         or kv['key_word'] is not null
         or kv['keyword'] is not null
    		 )
   and (kv['item_id']) != ''
group by 
    coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])) 
    ,trim(kv['item_id']) 
    ,concat(imei,imsi,device_id,userid) 
;
"
!

# new click data 
hive -e "
set mapreduce.job.name=${mapreduce_job_name_prefix}${query_item_click_data_tmp1};
drop table if exists ${query_item_click_data_tmp1};
create table if not exists ${query_item_click_data_tmp1} as 
select 
    coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])) as key_word
    ,trim(kv['item_id']) as item_id
    ,concat(imei,imsi,device_id,userid) as raw_uid
    ,ts
    ,count(1) over (partition by coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])), concat(imei,imsi,device_id,userid), trim(kv['item_id'])) as raw_pv_cnt
    ,min(ts) over (partition by coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])), concat(imei,imsi,device_id,userid), trim(kv['item_id'])) as min_ts
    ,max(ts) over (partition by coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])), concat(imei,imsi,device_id,userid)) as max_ts
from ${bbdw_dwd_search_log_i_d}  
where pt='${bizdate}' 
   and ((version >= '4.1' and os = 'android') or (version >= '4.0' and os='ios'))
   and (event_type = 'event_click' and  kv['item_id'] != '') -- 4.2之后改成点击时必须带上item_id ,确保是商品点击事件
   and (
        kv['page'] in ('搜索结果页') 
        or kv['name'] in ('搜索结果页_商品list_点击')
        or kv['e_name'] in ('搜索结果页_商品list_点击','分类搜索结果页_商品list_点击')
        or kv['e_name'] in ('搜索结果页_零结果_推荐商品list_点击','搜索无结果页_推荐商品list_点击') -- ios 曝光和点击 android点击
        or kv['e_name'] in ('搜索结果页_少结果_推荐商品list_点击')
      )
   and ( kv['keyWord'] is not null 
         or kv['key_word'] is not null
         or kv['keyword'] is not null
    		 )
   and (kv['item_id']) != ''
   and coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])) != '' 
;
"

<<!
# click data
hive -e "
set mapreduce.job.name=${mapreduce_job_name_prefix}${query_item_click_data_tmp2};
drop table if exists ${query_item_click_data_tmp2};
create table if not exists ${query_item_click_data_tmp2} as 
select
    key_word,
    item_id,
    sum(raw_pv_cnt) as click_pv,
    count(distinct raw_uid) as click_uv
from ${query_item_click_data_tmp1} 
group by
    key_word,
    item_id
;
"

# 提取query的候选类目数据: 通过query下曝光的商品来选取
hive -e "
set mapreduce.job.name=${mapreduce_job_name_prefix}${query_cat_exp_data_tmp1};
drop table if exists ${query_cat_exp_data_tmp1};
create table if not exists ${query_cat_exp_data_tmp1} as 
select
    t3.tw_query
    ,t2.cid
    ,sum(t1.show_pv) as show_pv
    ,sum(t1.show_uv) as show_uv 
from ${query_item_exp_data_tmp1} t1
join ${ods_ods_ba09_item_s_d} t2 
on t2.pt = ${bizdate} 
    and t2.id is not null 
    and t2.cid is not null 
    and t2.id > 0 
    and t2.cid > 0 
    and t1.item_id = t2.id
join ${termweight_query_data} t3
on t3.pt = ${bizdate} 
    and t1.key_word = t3.raw_query  
group by 
    t3.tw_query
    ,t2.cid
;"
!

exit 0
