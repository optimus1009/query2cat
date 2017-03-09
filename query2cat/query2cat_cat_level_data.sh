#!/bin/sh

set -e -x

online_conf_path="/home/deploy/data/recom/beibei/ad-search-qp/trunk/query2cat/"
if [ -s ${online_conf_path}/common_config.sh ]; then 
    source ${online_conf_path}/common_config.sh 
else 
    source ./common_config.sh
fi

hive -e "
set mapreduce.job.name=${mapreduce_job_name_prefix}${cate_level_data};
drop table if exists ${cate_level_data};
create table if not exists ${cate_level_data} as 
select 
    nvl(t3.id, nvl(t2.id, t1.id)) as leaf_cate_id, 
    nvl(t3.name, nvl(t2.name, t1.name)) as leaf_cate_name, 
    t1.id as cate1_id,
    t1.name as cate1_name,
    nvl(t2.id, 0) as cate2_id,
    nvl(t2.name, '-1') as cate2_name,
    nvl(t3.id, 0) as cate3_id,
    nvl(t3.name, '-1') as cate3_name
from 
    (-- 选取1级类目: 父类目为0
        select id, name, cate_type, parent_id, is_parent 
        from ${ods_ods_bb09_category_s_d} 
        where pt = ${bizdate} 
        and id is not null and id > 0 
        and (parent_id is null or parent_id = 0) 
    ) t1 
left outer join 
    (-- 选取2级类目：父类目=1级类目
        select id, name, cate_type, parent_id, is_parent
        from ${ods_ods_bb09_category_s_d} 
        where pt = ${bizdate} 
        and id is not null and id > 0 
        and (parent_id is not null and parent_id > 0) 
    ) t2 
on t1.id = t2.parent_id 
left outer join 
    (-- 选取3级类目：父类目=2级类目
        select id, name, cate_type, parent_id, is_parent
        from ${ods_ods_bb09_category_s_d} 
        where pt = ${bizdate} 
        and id is not null and id > 0 
        and (parent_id is not null and parent_id > 0) 
    ) t3 
on t2.id = t3.parent_id 
;
"

exit 0 
