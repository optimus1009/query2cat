#!/bin/sh

set -e -x 

online_conf_path="/home/deploy/data/recom/beibei/ad-search-qp/trunk/query2cat"
if [ -s ${online_conf_path}/common_config.sh ]; then 
    source ${online_conf_path}/common_config.sh 
else 
    source /home/deni.li/dev/recom/ad-search-qp/trunk/query2cat/common_config.sh
fi

while [ 1 -eq 1 ] 
do
    status=`partition_exist ${termweight_query_data} ${bizdate}`
    if [ $status == 0 ]; then 
        break;
    fi
    sleep 600 # wait 10 mins
done

exit 0
