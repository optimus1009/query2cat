#!/bin/sh

set -e -x 

debug=0

export CURRENT_DIR="/home/deploy/data/recom/beibei/ad-search-qp/trunk/query2cat/"
if [ ! -s ${CURRENT_DIR}/common_config.sh ]; then
    export HOME=/home/deni.li
    export CURRENT_DIR="`pwd $(dirname $0)`"
    export bizdate=`date -d"-1 days" '+%Y%m%d'`
fi 

export PROJ_NAME="query2cat"
export DB_NAME="bbalgo"
export TABLE_PREFIX="${DB_NAME}.${PROJ_NAME}" 

export DATA_PATH="${CURRENT_DIR}/data"
export TMP_DATA_PATH="${CURRENT_DIR}/tmp_data"
mkdir -p ${DATA_PATH} 
mkdir -p ${TMP_DATA_PATH}

export mapreduce_job_name_prefix="deni.li#query2cat#"
#export mapreduce_job_queuename="root.bigdata"

# set mapreduce.job.queuename=root.default;
# set mapreduce.job.queuename=root.production;
# set mapreduce.job.queuename=root.bigdata;
# set mapred.max.split.size=1000000000;
# set mapred.min.split.size.per.node=1000000000;
# set mapred.min.split.size.per.rack=1000000000;
# set mapred.reduce.tasks=100;

#####################################################################################################################
#-----------------------------------COMMON TABLE --------------------------------------------------------------------
export dwd_dwd_flow_app_log_i_d="dwd.dwd_flow_app_log_i_d"
export dwb_dwb_flow_app_log_show_i_d="dwb.dwb_flow_app_log_show_i_d"
export ods_ods_ba09_item_s_d="ods.ods_ba09_item_s_d"
export ods_ods_bb09_product_s_d="ods.ods_bb09_product_s_d"
export bbdw_dwd_search_log_i_d="bbdw.dwd_search_log_i_d" 
export ods_ods_bb09_category_s_d="ods.ods_bb09_category_s_d"
export ods_ods_bb09_brand_detail_s_d="ods.ods_bb09_brand_detail_s_d"
#####################################################################################################################

#####################################################################################################################
#-----------------------------------DEFINE TABLE NAME---------------------------------------------------------------- 
export query_item_exp_data_tmp1="${TABLE_PREFIX}_query_item_exp_data_tmp1"
export query_cat_exp_data_tmp1="${TABLE_PREFIX}_query_cate_exp_data_tmp1"
export query_item_exp_data="${TABLE_PREFIX}_query_item_exp_data"
export query_item_click_data_tmp1="${TABLE_PREFIX}_query_item_click_data_tmp1"
export query_item_click_data_tmp2="${TABLE_PREFIX}_query_item_click_data_tmp2"
export query_item_click_data_partition_tmp2="${TABLE_PREFIX}_query_item_click_data_partition_tmp2"
export query_item_click_data_tmp3="${TABLE_PREFIX}_query_item_click_data_tmp3"
export query_item_click_data_tmp4="${TABLE_PREFIX}_query_item_click_data_tmp4"
export query_item_click_data_tmp5="${TABLE_PREFIX}_query_item_click_data_tmp5"
export query_item_action_data="${TABLE_PREFIX}_query_item_action_data"
export termweight_query_data_tmp1="${TABLE_PREFIX}_termweight_query_data_tmp1"
export termweight_query_data="${TABLE_PREFIX}_termweight_query_data"
export termweight_cate_data_tmp1="${TABLE_PREFIX}_termweight_cate_data_tmp1"
export termweight_cate_data="${TABLE_PREFIX}_termweight_cate_data"
export termweight_doc_data_tmp1="${TABLE_PREFIX}_termweight_doc_data_tmp1"
export termweight_doc_data="${TABLE_PREFIX}_termweight_doc_data"
export termweight_brand_data_tmp1="${TABLE_PREFIX}_termweight_brand_data_tmp1"
export termweight_brand_data="${TABLE_PREFIX}_termweight_brand_data"
export cate_level_data="${TABLE_PREFIX}_cate_level_data"
export query2cat_by_click_data_tmp1="${TABLE_PREFIX}_query2cat_by_click_data_tmp1"
export query2cat_by_click_data_tmp2="${TABLE_PREFIX}_query2cat_by_click_data_tmp2"
export query2cat_by_click_data="${TABLE_PREFIX}_query2cat_by_click_data"
export query2cat_by_click_data_hbase="${PROJ_NAME}_query2cat_by_click_data_hbase"
export query2cat_random_query_demo_data="${TABLE_PREFIX}_query2cat_random_query_demo_data"
export query2cat_random_query_demo_data_hbase="${PROJ_NAME}_query2cat_random_query_demo_data"
export query2cat_query_brand_cate_info_tmp1="${TABLE_PREFIX}_query2cat_query_brand_cate_info_tmp1"
export query2cat_query_brand_cate_info_tmp2="${TABLE_PREFIX}_query2cat_query_brand_cate_info_tmp2"
export query2cat_query_brand_cate_info_tmp3="${TABLE_PREFIX}_query2cat_query_brand_cate_info_tmp3"
export query2cat_query_brand_cate_info_tmp4="${TABLE_PREFIX}_query2cat_query_brand_cate_info_tmp4"
export query2cat_query_brand_cate_info_tmp5="${TABLE_PREFIX}_query2cat_query_brand_cate_info_tmp5"
export query2cat_query_brand_cate_info_tmp6="${TABLE_PREFIX}_query2cat_query_brand_cate_info_tmp6"
export query2cat_query_topleafcat_data="${TABLE_PREFIX}_query2cat_query_top_leaf_cat_data"
#####################################################################################################################

#####################################################################################################################
#----------------------------------DEFINE DATA FILE------------------------------------------------------------------
export raw_query_data="${DATA_PATH}/raw_query_data"
export raw_cate_data="${DATA_PATH}/raw_cate_data"
export raw_doc_data="${DATA_PATH}/raw_doc_data"
export raw_brand_data="${DATA_PATH}/raw_brand_data"
export termweight_query_data_file="${DATA_PATH}/termweight_query_data"
export termweight_cate_data_file="${DATA_PATH}/termweight_cate_data"
export termweight_doc_data_file="${DATA_PATH}/termweight_doc_data"
export termweight_brand_data_file="${DATA_PATH}/termweight_brand_data"
#####################################################################################################################

#####################################################################################################################
#----------------------------------DEFINE FUNCTION------------------------------------------------------------------
function partition_exist() {
    tb_name=$1
    pt_name=$2
    partitions=`hive -e "show partitions ${tb_name}" | tail -n 5`
    final_status=1   # not exists 
    for part in ${partitions}
    do
        if [ "${part}" == "pt=${pt_name}" ]; then
            final_status=0
            break
        fi
    done
    echo $final_status
}
#####################################################################################################################
