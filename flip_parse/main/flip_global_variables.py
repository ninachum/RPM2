column_name_list = ['Category_of_overall', 'Ratings_of_overall', 'Title_of_overall',
                    'Content_of_overall', 'Customer_of_overall', 'Region_of_overall',
                    'Days_of_overall', 'Good_of_overall', 'Bad_of_overall']

# 리뷰의 추천/비추천 수는 고려하지 않는다. (크롤링 일자에 따라 바뀔 수 있기 때문)
column_name_list_for_drop_duplicate = ['Ratings_of_overall', 'Title_of_overall',
                                       'Content_of_overall', 'Customer_of_overall', 'Region_of_overall']

# 디렉토리들 - 사용하는 PC에 맞게 변경하세요
main_directory = 'C:\\Users\\USER\\Desktop\\pms'

source_directory = main_directory + '\\' + 'sources'
weighted_sum_directory = main_directory + '\\' + 'productwise'

dest_directory = main_directory + '\\' + 'preprocessed'
preprocessed_directory = main_directory + '\\' + 'preprocessed'

price_dictionary_directory = main_directory + '\\' + 'price_dictionary'
region_product_price_directory = main_directory + '\\' + 'region_product_price'
location_dictionary_directory = main_directory + '\\' + 'location_dictionary'
income_dictionary_directory = main_directory + '\\' + 'income_dictionary'
major_location_dictionary = main_directory + '\\' + 'major_location_dictionary'
crawler_base_file_directory = main_directory + '\\' + 'crawler_base_file'
state_income_dictionary = main_directory + '\\' + 'state_income_dictionary'

# RANGED_*에만 적용
START_DATE = '20190722'
END_DATE = '20190825'  # 최종 데이터 기준