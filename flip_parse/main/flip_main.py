import subprocess

# 점검사항 1. /main/flip_global_varables.py 내 변수 설정하고 진행할 것
# 점검사항 2. main_directory에 chromedriver 필요함
#
# 실행 전 세팅 1. /main/global_variables.py의 START_DATE와 END_DATE를 원하는 일자로 수정했는지
# 실행 전 세팅 2. /dictionary/flip_region_product_price의 MODE 변수들 원하는 대로 설정했는지


if __name__ == '__main__':
    # subprocess.call('python ../crawler/flipkart_crawler_page_link_star_duplicated.py')
    # subprocess.call('python ../crawler/flipkart_crawler_reviews.py')
    subprocess.call('python ../review_data_process_for_range/filp_RANGED_preprocessor.py')
    subprocess.call('python ../dictionary/flip_price_dictionary.py')
    subprocess.call('python ../review_data_process_for_range/flip_RANGED_productwise_weighted_num_of_reviews.py')
    subprocess.call('python ../dictionary/flip_major_area_income_and_location.py')
    subprocess.call('python ../dictionary/flip_region_product_price.py')
    print('--------------- ALL DONE ------------------')
