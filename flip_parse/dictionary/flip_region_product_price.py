# 어느 지역에서, 어느 시간에, 어느 제품이 몇 개 팔렸는지 종합
# 추가로 Monthly-GDP 컬럼, MGDP와 제품가격의 차이 컬럼도 기록

import os

import pandas as pd
from sklearn import preprocessing

from review_data_process_for_all_date.flip_all_date_productwise_weighted_num_of_reviews import \
    get_product_name_from_filename
from main.flip_global_variables import *
from datetime import datetime


def get_true_row_num(pandas_series):
    for i in range(0, len(pandas_series)):
        if pandas_series[i]:  # T of F 값을 가짐
            return i


def get_quarter_series_from_date_series(pandas_series):
    quarter_list = []
    for i in range(0, len(pandas_series)):
        ith_date = str(int(pandas_series[i]))
        assert len(ith_date) == 8  # YYYYMMDD 형식까지는 아니어도 길이로 어느정도 정확성 확보

        year = ith_date[:4]
        month = ith_date[4:6]

        month_only_quarter = ''
        if month <= '03':  # 1분기
            month_only_quarter = '1Q'
        elif month <= '06': # 2분기
            month_only_quarter = '2Q'
        elif month <= '09':  # 3분기
            month_only_quarter = '3Q'
        elif month <= '12':  # 4분기
            month_only_quarter = '4Q'
        else:
            raise ValueError('value of month is ' + month)

        quarter_list.append(year + ' ' + month_only_quarter)

    return quarter_list


def add_coordinates_from_location_dictionary(dataframe):
    global location_dictionary

    latitude_column = []
    longitude_column = []
    for region in dataframe['Region']:
        try:
            latitude_column.append(location_dictionary[region][0])
            longitude_column.append(location_dictionary[region][1])
        except KeyError:
            latitude_column.append(13.535496)
            longitude_column.append(82.297328)

    dataframe['Latitude'] = latitude_column
    dataframe['Longitude'] = longitude_column


def construct_price_dictionary(dataframe):
    dictionary = {}
    for current_product_name, current_price in zip(dataframe['Product_name'], dataframe['Current_price']):
        dictionary[current_product_name] = current_price

    return dictionary


def construct_location_dictionary(dataframe):
    dictionary = {}
    for location, latitude, longitude in zip(dataframe['Location'], dataframe['Latitude'], dataframe['Longitude']):
        dictionary[location] = (latitude, longitude)
    # dictionary['== NOT AVAILABLE =='] = (13.535496, 82.297328)  # 인도 주변의 아무 바다

    return dictionary


def construct_major_area_income_dictionary(dataframe):
    dictionary = {}
    for region, income in zip(dataframe['Region'], dataframe['Income']):
        dictionary[region] = income

    return dictionary


def add_vendor_column(dataframe, product_name):
    vendor_name = product_name.split()[0]
    if vendor_name == 'Redmi':
        vendor_name = 'Xiaomi'
    elif vendor_name == 'Mi':
        vendor_name == 'Xiaomi'
    elif vendor_name == 'Honor':
        vendor_name = 'Huawei'
    elif vendor_name == 'POCO':
        vendor_name == 'Xiaomi'
    elif vendor_name == 'OnePlus':
        vendor_name == 'OPPO'

    MAJOR = ['Huawei', 'Samsung', 'Xiaomi', 'Apple', 'OPPO', 'Realme']

    if vendor_name in MAJOR:
        dataframe['Vendor'] = [vendor_name] * dataframe.shape[0]
    else:
        dataframe['Vendor'] = ['Others'] * dataframe.shape[0]


def add_price_category_column(dataframe):
    global price_dictionary

    price_category_column = []
    for price in dataframe['Current_price']:
        if price <= 8000:
            price_category_column.append('Low')
        elif price <= 15000:
            price_category_column.append('Mid_1')
        elif price <= 20000:
            price_category_column.append('Mid_2')
        elif price <= 30000:
            price_category_column.append('Mid_3')
        elif price <= 40000:
            price_category_column.append('High')
        else:
            price_category_column.append('Extreme')

    dataframe['Price_category'] = price_category_column


def expand_count(dataframe):
    data = []
    for index, row in dataframe.iterrows():
        for i in range(0, row['Count']):
            data.append(row)
    expanded_dataframe = pd.DataFrame(data=data, columns=dataframe.columns)
    expanded_dataframe['Count'] = 1
    return expanded_dataframe


# State 별 첫 번째 레코드의 좌표를 기준으로 다른 레코드들의 좌표 통일
def merge_coordinates_by_state_wise(dataframe):
    coordinates_dictionary = {}
    latitude_column = []
    longitude_column = []
    for state, latitude, longitude in zip(dataframe['Region'], dataframe['Latitude'], dataframe['Longitude']):
        # State의 첫 레코드를 만났을 경우
        if coordinates_dictionary.get(state) is None:
            coordinates_dictionary[state] = (latitude, longitude)
            latitude_column.append(latitude)
            longitude_column.append(longitude)
        # State의 첫 레코드를 이미 만난 경우
        else:
            latitude_column.append(coordinates_dictionary[state][0])
            longitude_column.append(coordinates_dictionary[state][1])

    dataframe['Latitude'] = latitude_column
    dataframe['Longitude'] = longitude_column


# 소득은 2017-2018 회계연도 1인당 State-wise GDP (MOSPI 자료)
if __name__ == '__main__':

    QUARTER_MODE = False                        # True : 분기 단위 표시, False : 일자 단위 표시
    MAJOR_CITY_ONLY_MODE = True                 # True : 주요 City만 고려하여 State로 변환. 그리고 State에 해당하는 소득 컬럼 추가
    COUNT_EXPAND_MODE = True                    # True : Count를 1짜리 여러 행으로 나누어 출력
    MERGE_ADJACENT_COORDINATES_MODE = True      # True : 주요 City들이 인접하다는 가정 하에, State 단위로 좌표 통합하여 출력

    try:
        price_dictionary_file = pd.read_csv(price_dictionary_directory + '\\' + os.listdir(price_dictionary_directory)[-1],
                                            usecols=['Product_name', 'Current_price'], encoding='utf-8')
    except UnicodeError:
        price_dictionary_file = pd.read_csv(price_dictionary_directory + '\\' + os.listdir(price_dictionary_directory)[-1],
                                            usecols=['Product_name', 'Current_price'], encoding='ANSI')

    try:
        location_dictionary_file = pd.read_csv(location_dictionary_directory + '\\' + os.listdir(location_dictionary_directory)[-1],
                                               usecols=['Location', 'Latitude', 'Longitude'], encoding='utf-8')
    except UnicodeError:
        location_dictionary_file = pd.read_csv(location_dictionary_directory + '\\' + os.listdir(location_dictionary_directory)[-1],
                                               usecols=['Location', 'Latitude', 'Longitude'], encoding='ANSI')

    if MAJOR_CITY_ONLY_MODE:
        try:
            major_area_income_dictionary_file = pd.read_csv(state_income_dictionary + '\\' + os.listdir(state_income_dictionary)[-1],
                                                            usecols=['Region', 'Income', 'State'], encoding='utf-8')
        except UnicodeError:
            major_area_income_dictionary_file = pd.read_csv(state_income_dictionary + '\\' + os.listdir(state_income_dictionary)[-1],
                                                            usecols=['Region', 'Income', 'State'], encoding='ANSI')

    price_dictionary = construct_price_dictionary(price_dictionary_file)
    location_dictionary = construct_location_dictionary(location_dictionary_file)
    exported_dataframe = pd.DataFrame()

    for preprocessed_filename in os.listdir(preprocessed_directory):
        print('Processing ' + preprocessed_filename + ' ..........')
        # 전처리된 파일 이름으로부터 제품명 구함
        product_name = get_product_name_from_filename(preprocessed_filename)

        # 불러온 직후 컬럼명 변경됨에 유의
        try:
            current_preprocessed_file = pd.read_csv(preprocessed_directory + '\\' + preprocessed_filename, usecols=['Region_of_overall', 'Days_of_overall'], encoding='utf-8')
        except UnicodeError:
            current_preprocessed_file = pd.read_csv(preprocessed_directory + '\\' + preprocessed_filename, usecols=['Region_of_overall', 'Days_of_overall'], encoding='ANSI')
        current_preprocessed_file = current_preprocessed_file.rename(columns={'Region_of_overall': 'Region'})

        # 빈 파일은 무시
        if current_preprocessed_file.shape[0] == 0:
            continue

        if QUARTER_MODE:
            # 분기 추가
            current_preprocessed_file['Quarter'] = get_quarter_series_from_date_series(current_preprocessed_file['Days_of_overall'])
            # 날짜 삭제
            del current_preprocessed_file['Days_of_overall']

        # 가격 사전에서 해당하는 제품을 찾아 리턴
        current_price_for_this_product = price_dictionary[product_name]

        # Product_name 컬럼 추가
        row_len = current_preprocessed_file.shape[0]
        current_preprocessed_file['Product_name'] = [product_name] * row_len

        # [제품명, 지역, 분기명]으로 [갯수] 컬럼을 group_by로 생성
        # 첫 번째 줄은 수를 세기 위한 무의미한 컬럼 추가(추후 재생성됨)
        current_preprocessed_file['Current_price'] = [current_price_for_this_product] * row_len
        if QUARTER_MODE:
            current_preprocessed_file = current_preprocessed_file.groupby(['Product_name', 'Quarter', 'Region'])['Current_price'].count().reset_index(name='Count')
        else:
            current_preprocessed_file = current_preprocessed_file.groupby(['Product_name', 'Days_of_overall', 'Region'])['Current_price'].count().reset_index(name='Count')

        #>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> 'Product_name', 'Quarter'/'Days_of_overall', 'Region', 'Count 외의 모든 컬럼 추가는 반드시 이곳부터 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        # 가격 사전에 해당하는 지역을 찾아 컬럼 추가
        after_groupby_row_len = current_preprocessed_file.shape[0]
        current_preprocessed_file['Current_price'] = [current_price_for_this_product] * after_groupby_row_len

        # Vendor 컬럼 추가
        add_vendor_column(current_preprocessed_file, product_name)

        # 가격 수준 레이블 컬럼 추가
        add_price_category_column(current_preprocessed_file)

        # 지역 사전에서 해당하는 위도/경도를 찾아 리턴
        add_coordinates_from_location_dictionary(current_preprocessed_file)

        # TODO: Trend 컬럼 추가 검토

        # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> 'Product_name', 'Quarter'/'Days_of_overall', 'Region', 'Count 외의 모든 컬럼 추가는 반드시 여기까지만 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        # 주요 지역만을 남겨두고, 주요 지역의 소득 자료와 이를 이용한 컬럼들도 추가
        # 주요 지역 이름을 모두 주 이름으로 바꿈
        if MAJOR_CITY_ONLY_MODE:
            # TODO: 최적화 필요.. 사전을 사용하는 모든 로직을 성능을 고려한 join으로 변경하면 훨씬 간결한 코드 작성 가능
            # Monthly GDP, (MGDP - price) 컬럼 각각 추가
            current_preprocessed_file['MGDP'] = current_preprocessed_file.join(major_area_income_dictionary_file.set_index('Region'), on='Region', lsuffix='_left', rsuffix='_right', how='inner')['Income']\
                                                          / 12
            current_preprocessed_file['MGDP_price_diff'] = current_preprocessed_file['MGDP'] - current_preprocessed_file['Current_price']

            # Region 내 값을 State 단위로 변경
            current_preprocessed_file['Region'] = current_preprocessed_file.join(major_area_income_dictionary_file.set_index('Region'), on='Region', lsuffix='_left', rsuffix='_right', how='inner')['State']
            current_preprocessed_file.dropna(how='any', inplace=True)

        # 결과에 합침
        exported_dataframe = pd.concat([exported_dataframe, current_preprocessed_file], sort=False)

    if MERGE_ADJACENT_COORDINATES_MODE and MAJOR_CITY_ONLY_MODE:
        merge_coordinates_by_state_wise(exported_dataframe)

    if not COUNT_EXPAND_MODE:
        exported_dataframe.to_csv(region_product_price_directory + '\\' + datetime.today().strftime('%Y-%m-%d') + '.csv', index=False, encoding='utf-8')
    else:
        exported_dataframe = expand_count(exported_dataframe)
        exported_dataframe.to_csv(region_product_price_directory + '\\' + datetime.today().strftime('%Y-%m-%d') + '_COUNT_EXPANDED.csv', index=False, encoding='utf-8')
    print('Complete.')