#!/bin/sh 

set -e -x

online_conf_path="/home/deploy/data/recom/beibei/ad-search-qp/trunk/query2cat"
if [ -s ${online_conf_path}/common_config.sh ]; then 
    source ${online_conf_path}/common_config.sh 
else 
    source ./common_config.sh
fi

# new click data 
hive -e "
set mapreduce.job.name=${mapreduce_job_name_prefix}${query_item_click_data_tmp1};
drop table if exists ${query_item_click_data_tmp1};
create table if not exists ${query_item_click_data_tmp1} as 
select 
    pt
    ,coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])) as key_word
    ,trim(kv['item_id']) as item_id
    ,concat(imei,imsi,device_id,userid) as raw_uid
    ,ts
    ,count(1) over (partition by pt, coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])), concat(imei,imsi,device_id,userid), trim(kv['item_id'])) as raw_pv_cnt
    ,min(ts) over (partition by pt, coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])), concat(imei,imsi,device_id,userid), trim(kv['item_id'])) as min_ts
    ,max(ts) over (partition by pt, coalesce(trim(kv['keyword']),trim(kv['key_word']),trim(kv['keyWord'])), concat(imei,imsi,device_id,userid)) as max_ts
from ${bbdw_dwd_search_log_i_d}  
where unix_timestamp(string(pt),'yyyyMMdd') > (unix_timestamp('${bizdate}','yyyyMMdd') - 180 * 24 * 3600) 
   and unix_timestamp(string(pt),'yyyyMMdd') <=  (unix_timestamp('${bizdate}','yyyyMMdd') -   0 * 24 * 3600) 
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

hive -e "
create table if not exists ${query_item_click_data_partition_tmp2} ( 
    key_word        string,
    term_query      string, 
    tw_query        string, 
    cid             bigint, 
    click_score     double
) partitioned by (pt bigint) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
TBLPROPERTIES('LIFECYCLE'='5d')
;"

for idx in `seq 0 1` 
do
    dt=`date -d"${bizdate} -${idx} days +${idx}days " '+%Y%m%d'`
    partitions=`hive -e "show partitions ${query_item_click_data_partition_tmp2}"`
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
    set mapreduce.job.name=${mapreduce_job_name_prefix}${query_item_click_data_tmp2};
    drop table if exists ${query_item_click_data_tmp2};
    create table if not exists ${query_item_click_data_tmp2} as 
    select 
        t1.key_word,
        t3.term_query,
        t3.tw_query,
        t2.cid,
        sum(click_score) as click_score
    from 
        (
            select 
                t1.key_word
                ,t1.item_id
                ,sum(( t1.click_wt + t1.last_click_wt ) *
                        (case   when t1.diff_days >= 180 then 0.01 
                                when t1.diff_days <= 0.0 then 0.99  
                                else (1.0-1.0/(1+exp(log(99.0)*(1.0-t1.diff_days/90.0)))) 
                        end)
                    ) as click_score
            from
                (
                    select 
                        key_word
                        ,item_id
                        ,raw_uid
                        ,((unix_timestamp()*1000-ts)/(1000*24*3600)) as diff_days 
                        ,double(10.0) as click_wt
                        ,(case when ts == max_ts then 50.0 else 0.0 end) as last_click_wt
                    from ${query_item_click_data_tmp1}   
                    distribute by key_word 
                ) t1
            group by 
                t1.key_word
                ,t1.item_id
        ) t1
    join ${ods_ods_ba09_item_s_d} t2 
    on t2.pt = ${dt}
        and t2.id is not null 
        and t2.cid is not null 
        and t2.id > 0 
        and t2.cid > 0 
        and t1.item_id = t2.id 
    join ${termweight_query_data} t3 
    on      unix_timestamp(string(t3.pt),'yyyyMMdd') >   (unix_timestamp('${dt}','yyyyMMdd') - 180 * 24 * 3600) 
        and unix_timestamp(string(t3.pt),'yyyyMMdd') <=  (unix_timestamp('${dt}','yyyyMMdd') -   0 * 24 * 3600) 
        and t3.raw_type = 1 
        and t3.term_query != '' 
        and t3.tw_query != ''
        and t1.key_word = t3.raw_query
    group by 
        t1.key_word,
        t3.term_query,
        t3.tw_query,
        t2.cid 
    ;"

    hive -e "
    alter table ${query_item_click_data_partition_tmp2} drop if exists partition (pt = ${dt}); 
    alter table ${query_item_click_data_partition_tmp2} add if not exists partition (pt = ${dt}); 
    insert into table ${query_item_click_data_partition_tmp2} partition (pt = ${dt}) 
    select * from ${query_item_click_data_tmp2};"
done


# 计算所属的叶子类目
hive -e "
set mapreduce.job.name=${mapreduce_job_name_prefix}${query_item_click_data_tmp3};
drop table if exists ${query_item_click_data_tmp3};
create table if not exists ${query_item_click_data_tmp3} as 
select 
    -- t1.key_word,
    t1.term_query,
    -- t1.tw_query,
    t2.leaf_cate_id,
    t2.leaf_cate_name,
    sum(t1.click_score) as click_score
from ${query_item_click_data_tmp2} t1 
join ${cate_level_data} t2 
on  t1.cid = t2.leaf_cate_id 
group by 
    -- t1.key_word,
    t1.term_query,
    -- t1.tw_query,
    t2.leaf_cate_id,
    t2.leaf_cate_name
sort by 
    t1.term_query,
    click_score desc 
;
"

# 计算所属的一级类目
hive -e "
set mapreduce.job.name=${mapreduce_job_name_prefix}${query_item_click_data_tmp4};
drop table if exists ${query_item_click_data_tmp4};
create table if not exists ${query_item_click_data_tmp4} as 
select 
    -- t1.key_word,
    t1.term_query,
    -- t1.tw_query,
    t2.cate1_id,
    t2.cate1_name,
    sum(t1.click_score) as click_score 
from ${query_item_click_data_tmp2} t1 
join ${cate_level_data} t2 
on t2.cate1_id <> '0' 
    and t2.cate1_name <> '-1' 
    and t1.cid = t2.leaf_cate_id 
group by 
    -- t1.key_word,
    t1.term_query,
    -- t1.tw_query,
    t2.cate1_id,
    t2.cate1_name
sort by 
    t1.term_query,
    click_score desc 
;
"

# 计算所属的二级类目
hive -e "
set mapreduce.job.name=${mapreduce_job_name_prefix}${query_item_click_data_tmp5};
drop table if exists ${query_item_click_data_tmp5};
create table if not exists ${query_item_click_data_tmp5} as 
select 
    -- t1.key_word,
    t1.term_query,
    -- t1.tw_query,
    t2.cate2_id,
    t2.cate2_name,
    sum(t1.click_score) as click_score  
from ${query_item_click_data_tmp2} t1 
join ${cate_level_data} t2 
on t1.cid = t2.leaf_cate_id 
    and t2.cate2_id <> '0' 
    and t2.cate2_name <> '-1' 
group by 
    -- t1.key_word,
    t1.term_query,
    -- t1.tw_query,
    t2.cate2_id,
    t2.cate2_name
sort by 
    t1.term_query,
    click_score desc 
;
"

exit 0
