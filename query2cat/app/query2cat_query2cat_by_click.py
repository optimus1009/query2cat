#!/usr/bin/python
#-*- coding:utf-8 -*-
################################

import sys
import json
from collections import defaultdict

def process_cateinfo(cateinfo):
    cnt = 0
    min_score = 0.0
    max_score = 0.0
    total_score = 0.0
    res_dict = {}
    res_list = []
    if cateinfo and cateinfo != 'NULL':
        tmp_ls = cateinfo.strip().split(',')
        if len(tmp_ls) > 0:
            for item in tmp_ls:
                if cnt >= 5:  #控制输出top5
                    break
                item_ls = item.strip().split(':')
                if len( item_ls ) == 3:
                    origin_score = round(float(item_ls[2]), 4)
                    tmp_dict = {}
                    tmp_dict['id'] = item_ls[0]
                    tmp_dict['name'] = item_ls[1] + ""
                    tmp_dict['wt_clk'] = origin_score
                    res_list.append(tmp_dict)
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
                    cnt += 1

    for item in res_list:
        if min_score >= max_score:
            normed_cate_score = 1.0
        else:
            normed_cate_score = ( item.get('wt_clk') - min_score )/( max_score - min_score )
        cat_click_percent = item.get('wt_clk')*1.0/total_score
        if cat_click_percent >= 0.7 and cat_click_percent <= 1.0:
            cate_level = 'A'
            is_peak = 'Y'
        else:
            if cat_click_percent >= 0.4 and cat_click_percent < 0.7:
                cate_level = 'B'
                is_peak = 'N'
            else:
                cate_level = 'C'
                is_peak = 'N'
        #补充
        if normed_cate_score >= 0.6:
            cate_level = 'A'
        item['nm_wt_clk'] = normed_cate_score
        item['cat_lv'] = cate_level
        item['is_peak'] = is_peak
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

