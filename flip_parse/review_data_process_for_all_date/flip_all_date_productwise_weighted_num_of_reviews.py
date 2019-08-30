# -*- coding:utf-8 -*-
# ALL_DATE_PREPROCESSOR의 결과를 바탕으로, 주차별 리뷰 가중합과, 주차별 리뷰 수를 나타냄

import os

import pandas as pd

from main.flip_global_variables import *
from datetime import datetime


# 내용이 없는 첫 행 제거
def trim_first_empty_row(dataframe):
    dataframe.drop(0, inplace=True)


# 한 기종의 주별 리뷰 수를 세서 list로 리턴
# 80주 초과는 고려하지 않는다 - 무의미
def count_reviews_per_week(week_list):
    global NUM_OF_LIST_ENTRY

    count_list = [0] * (NUM_OF_LIST_ENTRY + 1)
    for week_data in week_list:
        if week_data < NUM_OF_LIST_ENTRY:
            count_list[week_data] += 1

    return count_list


# Num_past_weeks 컬럼을 기반으로 가중합을 구해 리턴
# ex. 33주차에 크롤링한 데이터의 경우 32주차의 리뷰 수 * 1, 31주차의 리뷰 수 * 0.95...... 의 합을 구해 리턴
# 가중치는 음수가 될 수 있다
def calculate_weighted_num_of_reviews(dataframe):
    # 상수
    DECREASE_CONSTANT_FOR_A_WEEK = 0.05

    total_weighted_sum = 0.0
    for num_past_weeks in dataframe['Num_past_weeks']:
        # print(num_past_weeks)
        # 1주당 0.05씩 가중치가 줄어들도록 설정
        if num_past_weeks >= 1:
            # 1주마다 0.05씩 가중치를 줄여 합에 반영
            weighted_sum_for_a_row = 1 - DECREASE_CONSTANT_FOR_A_WEEK * (num_past_weeks - 1)
            if weighted_sum_for_a_row >= 0:
                total_weighted_sum += weighted_sum_for_a_row
        elif num_past_weeks == 0:
            total_weighted_sum += 1
        else:
            raise ValueError('Num_past_weeks의 내용이 >= 0인 자연수가 아니거나 컬럼이 없음')

    return total_weighted_sum


# 제품명이 ')' 로 끝남을 이용하여 제품명을 리턴
def get_product_name_from_filename(filename):
    return filename[:filename.rfind(')') + 1]  # 범위 끝이 exclusive이므로 + 1


# 크롤링된 주차와 리뷰 주차의 차이들을 구한 컬럼을 list로 리턴
def get_num_past_weeks(dataframe):
    # preprocessor에 있는 크롤링 시점 구하는 함수 사용
    global today

    # 상수
    WEEKS_IN_A_YEAR = 52

    review_week_num_list = []
    for review_date in dataframe['Days_of_overall']:
        # 리뷰 일자가 크롤링된 주차와 몇 주차 차이가 나는지를 계산
        converted_date = datetime.strptime(str(review_date), '%Y%m%d').date()
        week_num_gap = WEEKS_IN_A_YEAR * today.isocalendar()[0] + today.isocalendar()[1]\
                       - WEEKS_IN_A_YEAR * converted_date.isocalendar()[0] - converted_date.isocalendar()[1]
        review_week_num_list.append(week_num_gap)
        if week_num_gap < 0:
            raise ValueError('두 기간의 주 차이가 음수임')

    dataframe['Num_past_weeks'] = review_week_num_list
    return review_week_num_list


if __name__ == '__main__':

    # 몇 주까지의 리뷰로 종합을 할 것인가
    NUM_OF_LIST_ENTRY = 80 + 1  # 0주부터 세므로

    # 오늘의 날짜 계산
    today = datetime.today()

    filename_list = os.listdir(preprocessed_directory)

    # 모든 제품의 주차 별 리뷰 수와 가중합을 저장할 리스트 생성
    productwise_number_of_reviews_over_weeks_and_weighted_sum = []

    for filename in filename_list:
        print('Processing ' + filename + ' .........')\

        review_data = pd.read_csv(preprocessed_directory + '\\' + filename, usecols=column_name_list, encoding='UTF-8')

        week_list = get_num_past_weeks(review_data)

        # 이 파일의 리뷰 일자 / 가중치 데이터를 리스트에 추가함
        productwise_number_of_reviews_over_weeks_and_weighted_sum.append \
            ([get_product_name_from_filename(filename)] + count_reviews_per_week(week_list)+ [calculate_weighted_num_of_reviews(review_data)])

    # 리스트를 이용하여 모든 파일의 리뷰 일자 / 가중치 데이터를 요약한 파일 생성
    print('Saving PRODUCTWISE.csv .........')
    pd.DataFrame(data=productwise_number_of_reviews_over_weeks_and_weighted_sum, \
                 columns=['Product_name'] + [('Week' + str(_)) for _ in range(0, NUM_OF_LIST_ENTRY + 1)] + ['Weight_sum']) \
                 .to_csv(weighted_sum_directory + '\\' + 'PRODUCTWISE_' + today.strftime('%Y-%m-%d') + '.csv', index=False, encoding='utf-8')
    print('Parsing complete.')
