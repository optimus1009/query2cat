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
add file ${CURRENT_DIR}/app/query2cat_query2cat_by_click_entropy_topcat.py;
drop table if exists ${query2cat_query_topleafcat_data};
create table if not exists ${query2cat_query_topleafcat_data} as  
select transform(term_query, leaf_cate_score, cate2_score, cate1_score)
using 'python query2cat_query2cat_by_click_entropy_topcat.py'
from ${query2cat_by_click_data_tmp1} 
;
"

exit 0 
