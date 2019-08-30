# -*- coding:utf-8 -*-
# source 내 폴더들을 돌며, 일자별 크롤링 데이터들을 기종별 파일에 모아 처리할 수 있도록 함
# 엑셀 2016 버전(최대 행 개수가 1,048,576 행)을 가정함

# 크롤링 데이터가 주어졌을 때, 모든 리뷰 일자에 대해 전처리함.
# 단, 리뷰 일자가 'month ago' 단위인 경우에는 해당하는 달의 랜덤한 일자로 변경함
# 크롤링 시점과 리뷰 시점의 시차(인도<->한국)를 고려함

import calendar
import os
import random
import string
from datetime import datetime

import pandas as pd
from dateutil import parser
from dateutil.relativedelta import relativedelta

from main.flip_global_variables import *


# region of overall 열의 지역 데이터만을 추출하여 해당 열에 대체
def parse_region(dataframe):
    length = len('certified buyer')
    parsed_region_list = []

    for region_data in dataframe['Region_of_overall']:
        if isinstance(region_data, float):  # 결측치(NaN, float type)인 경우
            parsed_region_list.append('== NOT AVAILABLE ==')
            continue
        elif len(region_data) > length:  # Certified Buyer,에 뒤따르는 도시가 있는 경우
            separated_region_data = region_data.split(', ')
            parsed_region_list.append(separated_region_data[1])
        else:  # 데이터가 깨진 경우 - 없긴 함
            parsed_region_list.append('== NOT AVAILABLE ==')

    dataframe['Region_of_overall'] = parsed_region_list


# date 객체를 YYYYMMDD 형식의 문자열로 리턴
def convert_date_to_string(date):
    # assert isinstance(date, datetime)
    return date.strftime('%Y%m%d')


# 상대적 일자(ex. 3일 전)를 절대적 일자(ex. 5월 27일)로 바꿈
# 일자에 결측값은 없다고 가정
# 문자열 비교 시 대소문자 변환 안하고 비교
# TODO : 성능 최적화
def convert_ago_to_absolute_date(dataframe, filename):
    global TIME_DIFFERENCE_BETWEEN_KOREA_AND_INDIA
    india_local_date = get_crawling_time(filename) - TIME_DIFFERENCE_BETWEEN_KOREA_AND_INDIA
    india_local_date = datetime.strptime(india_local_date.strftime('%Y%m%d'), '%Y%m%d').date()

    converted_date_list = []
    for ago_date in dataframe['Days_of_overall']:
        if 'Today' in ago_date:
            converted_date_list.append(convert_date_to_string(india_local_date))
        elif 'day' in ago_date:
            converted_date_list.append(
                convert_date_to_string(india_local_date - relativedelta(days=int(''.join(filter(str.isdigit, ago_date))))))
        elif 'month' in ago_date:
            # 일 자리를 랜덤화할 날짜
            to_be_random = india_local_date - relativedelta(months=int(''.join(filter(str.isdigit, ago_date))))

            # 일 자리를 랜덤화
            days_range = calendar.monthrange(to_be_random.year, to_be_random.month)
            random_day_date = to_be_random.replace(day=random.choice([_ for _ in range(days_range[0] + 1, days_range[1] + 1)]))

            converted_date_list.append(convert_date_to_string(random_day_date))
        else:  # 1년 이상이 지난 리뷰는 Aug, 2018의 형식임
            # 일 자리를 랜덤화할 날짜
            # 월 단위 랜덤화이기 때문에 시차 고려하지 않음
            to_be_random = parser.parse(ago_date)

            # 일 자리를 랜덤화
            days_range = calendar.monthrange(to_be_random.year, to_be_random.month)
            random_day_date = to_be_random.replace(day=random.choice([_ for _ in range(days_range[0] + 1, days_range[1] + 1)]))

            converted_date_list.append(convert_date_to_string(random_day_date))

    dataframe['Days_of_overall'] = converted_date_list


# 파일 이름으로부터 크롤링한 시간을 연 월 일 까지만 고려하여 datetime 형식으로 리턴
def get_crawling_time(filename):
    return datetime.strptime(filename[filename.rfind(')') + 1:].strip()[:7 + 1], '%Y%m%d').date()


# 평범한 영어 문장에 쓰이는 글자 외에 모두 공백으로 바꿈
def delete_dirty_characters_in_reviews(dataframe):
    allowed_characters = string.printable  # 숫자, 영문 대소문자, 공백, 문장 기호

    two_columns = ['Content_of_overall', 'Title_of_overall']
    for col_name in two_columns:
        clean_review_list = []
        for review in dataframe[col_name]:
            if isinstance(review, float):  # 결측치(NaN, float type)인 경우
                clean_review_list.append('== NOT AVAILABLE ==')
                continue

            clean_review = ''
            for char in review:
                if char in allowed_characters:
                    clean_review += char
                else:
                    clean_review += ' '
            clean_review_list.append(clean_review)
        dataframe[col_name] = clean_review_list


# pandas에서 유니코드 오류가 나지 않는 파일명으로 변경
def convert_filename_to_non_error_filename(filename):
    allowed_characters = string.printable  # 숫자, 영문 대소문자, 공백, 문장 기호

    new_name = ''
    for char in filename:
        if char in allowed_characters:
            new_name += char

    return new_name


if __name__ == '__main__':

    TIME_DIFFERENCE_BETWEEN_KOREA_AND_INDIA = relativedelta(hours=3, minutes=30)

    # 유니코드 오류 나지 않도록 파일명 적절히 변경
    for subdir in os.walk(source_directory):
        if len(subdir[1]) == 0:
            for to_be_renamed_to_non_error_name in subdir[2]:
                # 파일명에 (1)이 있음 -> 단순 복사본이기 때문에 제외
                if to_be_renamed_to_non_error_name.find('(1)') == -1:
                    try:
                        os.rename(subdir[0] + '\\' + to_be_renamed_to_non_error_name,
                                subdir[0] + '\\' + convert_filename_to_non_error_filename(to_be_renamed_to_non_error_name))
                    except FileExistsError:
                        pass

    # 하위 디렉토리 내 모든 엑셀 파일의 디렉토리를 포함한 파일 이름을 저장
    # 하위 디렉토리 명이 정렬되었다고 가정함
    filename_with_dir_list = []
    for subdir in os.walk(source_directory):
        if len(subdir[2]) >= 1:
            for filename_with_dir in subdir[2]:
                # 파일명에 (1)이 있음 -> 단순 복사본이기 때문에 제외
                if filename_with_dir.find('(1)') == -1:
                    filename_with_dir_list.append(subdir[0] + '\\' + filename_with_dir)

    # 파일 이름이 ')'로 끝남을 이용하여 제품 이름 추출
    product_name_list = []
    for filename_with_dir in filename_with_dir_list:
        filename = filename_with_dir.split('\\')[-1]
        product_name_list.append(filename[:filename.rfind(')') + 1])

    # 제품별로 통합된 정보가 기록될 엑셀 파일 생성
    for product_name in product_name_list:
        print('Creating repository for ' + product_name + '..........')
        pd.DataFrame(columns=column_name_list)\
            .to_csv(dest_directory + '\\' + product_name + '__PREPROCESSED.csv', encoding='UTF-8', index=False)

    # 빈 파일이 주어진 경우가 있음. 체크리스트로 마지막에 출력
    empty_file_check_list = []

    # 하위 디렉토리 내 파일들을 탐색하며, 제품별로 엑셀 파일에 통합 시작
    for filename_with_dir in filename_with_dir_list:
        print('Processing ' + filename_with_dir + ' ..........')

        # 파일 이름이 ')'로 끝남을 이용하여 제품 이름 추출
        filename = filename_with_dir.split('\\')[-1]
        product_name = filename[:filename.rfind(')') + 1]

        try:
            total = pd.read_csv(dest_directory + '\\' + product_name + '__PREPROCESSED.csv',
                                usecols=column_name_list, encoding='UTF-8')
        except UnicodeError:
            total = pd.read_csv(dest_directory + '\\' + product_name + '__PREPROCESSED.csv',
                                usecols=column_name_list, encoding='ANSI')

        try:
            right = pd.read_csv(filename_with_dir, usecols=column_name_list, encoding='UTF-8')

        except UnicodeError:
            right = pd.read_csv(filename_with_dir, usecols=column_name_list, encoding='ANSI')
        except pd.errors.EmptyDataError:  # 가끔 아예 빈 엑셀 파일 있음. 이때는 스킵하도록
            continue


        # 리뷰 데이터 중, overall 데이터가 있는 행만 남겨놓기 위해 내용이 모두 NaN인 행 제거
        # 남은 데이터 중 결측치도 제거(ex. 갤럭시 J 크롤링 중 컬럼이 1개씩 좌측으로 밀려 기록된 경우가 드물게 존재)
        right = right.dropna(how='any')

        # 지역 데이터 parse 후 저장
        parse_region(right)

        # 현재 기준의 날짜를 절대적 날짜로 수정
        convert_ago_to_absolute_date(right, filename)

        # 리뷰 제목과 본문에서 정상 문자만 남겨둠
        delete_dirty_characters_in_reviews(right)

        try:
            total = pd.concat([total, right], sort=False)\
                    .drop_duplicates(subset=column_name_list_for_drop_duplicate)

        except ValueError:
            empty_file_check_list.append(product_name)
            continue
        total.to_csv(dest_directory + '\\' + product_name + '__PREPROCESSED.csv', encoding='UTF-8', index=False)

    print('Please check these files are empty : ' + str(empty_file_check_list))
    print('Preprocessing done. sorting ........')

    preprocessed_filename_list = os.listdir(preprocessed_directory)
    for preprocessed_filename in preprocessed_filename_list:
        to_be_sorted = pd.read_csv(preprocessed_directory + '\\' + preprocessed_filename,
                                   usecols=column_name_list, encoding='UTF-8')
        to_be_sorted.sort_values('Days_of_overall', inplace=True, ascending=False)
        to_be_sorted.to_csv(preprocessed_directory + '\\' + product_name + '__PREPROCESSED.csv', encoding='UTF-8', index=False)

    print('Preprocessing complete.')