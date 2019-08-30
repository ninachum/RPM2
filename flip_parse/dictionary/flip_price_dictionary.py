# 크롤링 데이터와 전처리 데이터를 바탕으로 제품별 가격 사전 구축

import difflib
import os

import pandas as pd

from review_data_process_for_all_date.flip_all_date_productwise_weighted_num_of_reviews import \
    get_product_name_from_filename
from main.flip_global_variables import *
from datetime import datetime

def get_filename_from_directory(filename_with_dir):
    return filename_with_dir.split(sep='\\')[-1]


if __name__ == '__main__':
    price_dictionary = []

    raw_source_directory = source_directory

    # 하위 디렉토리 명이 오름차순으로 정렬되었다고 가정, 그때의 맨 마지막 일자를 기준으로 가격을 구함
    # 중요 : difflib은 정상 작동함

    # 하위 파일 돌면서 (파일이름<->파일네임 with dir 쌍 추가)
    # 비슷한 파일 이름 찾아 디렉터리를 열게 한 후, 숫자를 읽는다
    # 하위 디렉토리 내 모든 엑셀 파일의 디렉토리를 포함한 파일 이름을 저장
    # 하위 디렉토리 명이 정렬되었다고 가정함
    filename_with_dir_list = []
    filename_with_dir_dictionary = {}
    for subdir in os.walk(source_directory):
        if len(subdir[2]) >= 1:
            for filename_with_dir in subdir[2]:
                # 파일명에 (1)이 있음 -> 단순 복사본이기 때문에 제외
                if filename_with_dir.find('(1)') == -1:
                    full_directory = subdir[0] + '\\' + filename_with_dir
                    filename_with_dir_list.append(get_filename_from_directory(full_directory))
                    filename_with_dir_dictionary[get_filename_from_directory(full_directory)] = full_directory

    print('Generating price dictionary.....')
    for preprocessed_filename in os.listdir(preprocessed_directory):
        # print('processing ' + preprocessed_filename + ' .............')
        # 전처리된 파일의 이름과 가장 비슷한 최근 일자의 제품 파일명을 구함
        product_name = preprocessed_filename.split(sep='__')[0]
        most_similar_source_file_name = difflib.get_close_matches(product_name, filename_with_dir_list, cutoff=0.6)[0]
        # print(product_name)
        # print(most_similar_source_file_name)

        # 이름 정확성 보장
        try:
            assert product_name == get_product_name_from_filename(most_similar_source_file_name)
        except AssertionError('ERROR ->> please check direcrtory settings in filp_price_dictionary.py'):  # TODO: iphone 6 gold 64gb -> iphone 6s gold 64gb?
            most_similar_source_file_name = difflib.get_close_matches(product_name, filename_with_dir_list)[1]

        try:
            source_file = pd.read_csv(filename_with_dir_dictionary[most_similar_source_file_name],
                                usecols=['DP', 'MRP'], encoding='UTF-8')
        except UnicodeError:
            source_file = pd.read_csv(filename_with_dir_dictionary[most_similar_source_file_name],
                                usecols=['DP', 'MRP'], encoding='ANSI')

        # 첫 번째 행 제거
        source_file = source_file.dropna(how='all')

        # 빈 파일은 무시
        if source_file.shape[0] == 0:
            continue

        try:
            current_price = source_file['DP'].iloc[0]
        except IndexError:
            current_price = source_file['MRP'].iloc[0]

        # 가격 정확성 보장
        assert current_price != 0

        price_dictionary.append([product_name, current_price])

    print('Done. saving...')
    pd.DataFrame(data=price_dictionary, columns=['Product_name', 'Current_price']).to_csv\
        (price_dictionary_directory + '\\' + 'PRICE_DICTIONARY_' + datetime.today().strftime('%Y-%m-%d') + '.csv', index=False, encoding='utf-8')
    print('Complete.')