#!/usr/bin/python
#-*- coding:utf-8 -*-
################################

import sys
import json
import math
from collections import defaultdict

def process_cateinfo(cateinfo):
    min_score = 0.0
    max_score = 0.0
    total_score = 0.0
    res_list = []
    if cateinfo and cateinfo != 'NULL':
        tmp_ls = cateinfo.strip().split(',')
        if len(tmp_ls) > 0:
            for item in tmp_ls:
                item_ls = item.strip().split(':')
                if len( item_ls ) == 3:
                    origin_score = round(float(item_ls[2]), 4)
                    tmp_dict = {}
                    tmp_dict['id'] = item_ls[0]
                    tmp_dict['name'] = item_ls[1] + ""
                    tmp_dict['wt_clk'] = origin_score
                    res_list.append(tmp_dict)
            res_list = sorted(res_list, key=lambda tmp_item: tmp_item["wt_clk"], reverse=True)
            res_list = res_list[:15]
            for item in res_list:
                origin_score = item['wt_clk']
                if min_score == 0.0:
                    min_score = origin_score
                else:
                    if origin_score < min_score:
                        min_score = origin_score
                if max_score == 0.0:
                    max_score = origin_score
                else:
                    if origin_score > max_score:
                        max_score = origin_score
                total_score += origin_score

    click_entropy = 0.0
    for item in res_list:
        if min_score >= max_score:
            normed_cate_score = 1.0
        else:
            normed_cate_score = ( item.get('wt_clk') - min_score )/( max_score - min_score )
        item['nm_wt_clk'] = normed_cate_score
        cat_click_percent = item.get('wt_clk')*1.0/total_score
        click_entropy += (-1.0*cat_click_percent)*math.log(cat_click_percent, 2.0)
    if len(res_list) == 1:
        res_list[0]['cat_lv'] = 'A'
        res_list[0]['is_peak'] = 'Y'
    if len(res_list) > 1:
        res_list = sorted(res_list, key=lambda tmp_item: tmp_item["wt_clk"], reverse=True)
        top_n = math.ceil(pow(2.0, click_entropy))
        #top_n = math.ceil(click_entropy)
        if top_n > len(res_list):
            top_n = len(res_list)
        has_peak = 'Y'
        if top_n > 1:
            has_peak = 'N'
        for i in range(len(res_list)):
            if i < top_n:
                res_list[i]['cat_lv'] = 'A'
                res_list[i]['is_peak'] = 'N'
                if has_peak == 'Y':
                    res_list[i]['is_peak'] = 'Y'
            else:
                res_list[i]['cat_lv'] = 'B'
                if (i+1) == len(res_list):
                    res_list[i]['cat_lv'] = 'C'
                res_list[i]['is_peak'] = 'N'

    return res_list

def main():
    # input
    for line in sys.stdin:
        if len(line.strip('\n').split('\t')) < 4:
            continue
        base64_term_query, leaf_cate_score, cate2_score, cate1_score = line.strip('\n').split('\t')
        s_dict = {}
        # 处理叶子类目得分
        leaf_cate_list = []
        if leaf_cate_score and leaf_cate_score != 'NULL':
            leaf_cate_list = process_cateinfo( leaf_cate_score )
        s_dict['leaf_cate'] = leaf_cate_list
        cate2_score_list = []
        if cate2_score and cate2_score != 'NULL':
            cate2_score_list = process_cateinfo( cate2_score )
        s_dict['cate2'] = cate2_score_list
        cate1_score_list = []
        if cate1_score and cate1_score != 'NULL':
            cate1_score_list = process_cateinfo( cate1_score )
        s_dict['cate1'] = cate1_score_list

        print '\t'.join( [ base64_term_query, json.dumps(s_dict, ensure_ascii=False) ] )

main()

