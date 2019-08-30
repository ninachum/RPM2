# 주의: /major_area의 주요 도시들의 위도와 경도 데이터를 PM 님으로부터 최신화해서 받아와야 유의미한 데이터가 나옴

# /income_dictionary 내, 주요 리뷰 지역의 State-GDP 사전 사용
# /major_area_location_dictionary 내, 주요 리뷰 지역이 위도와 경도 사전 사용
# OUTPUT : /major_area 내, 주요 리뷰 지역의 위도와 경도사전에 State 컬럼과 income 컬럼 추가

from selenium import webdriver

from dictionary.flip_price_dictionary import *


if __name__ == '__main__':
    driver_directory = main_directory + '\\' + 'chromedriver.exe'
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    options.add_argument('disable-gpu')

    driver = webdriver.Chrome(driver_directory, chrome_options=options)
    driver.get('https://www.latlong.net/Show-Latitude-Longitude.html')

    major_location_dictionary_file = pd.read_csv(major_location_dictionary + '\\' + os.listdir(major_location_dictionary)[-1], encoding='UTF-8')
    major_location_dictionary_file.dropna(how='any')

    # 'Odisha' == 'Orissa'
    state_list = ['Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar', 'Chhattisgarh', 'Goa', 'Gujarat',
                        'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir', 'Jharkhand', 'Karnataka', 'Kerala',
                        'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Orissa',
                        'Punjab', 'Rajasthan', 'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh', 'Uttarakhand',
                        'West Bengal', 'Andaman & Nicobar Islands', 'Chandigarh', 'Delhi', 'Puducherry']
    state_column = []

    print('Processing coordinates of major cities........')
    for lat, long in zip(major_location_dictionary_file['Latitude'], major_location_dictionary_file['Longitude']):
        driver.find_element_by_id('latitude').clear()
        driver.find_element_by_id('longitude').clear()
        driver.find_element_by_id('latitude').send_keys(str(lat))
        driver.find_element_by_id('longitude').send_keys(str(long))
        driver.find_element_by_xpath('/html/body/main/div[2]/div[1]/form/div[1]/div[3]/button').click()
        count = 0
        address = driver.find_element_by_id('address').text
        # print(address)
        for state in state_list:
            if state in address:
                count += 1
                state_column.append(state)
        if count != 1:
            raise ValueError

    major_location_dictionary_file['State'] = state_column

    # major_location_dictionary_file.to_csv(major_location_dictionary + '\\' + os.listdir(major_location_dictionary)[-1], encoding='UTF-8', index=False)
    driver.quit()
    right = pd.read_csv(income_dictionary_directory + '\\' + os.listdir(income_dictionary_directory)[-1])
    joined = major_location_dictionary_file.join(right.set_index('State'), how='inner', lsuffix='_left', rsuffix='_right', on='State')
    # print(major_location_dictionary_file, right, joined)
    joined.to_csv(state_income_dictionary + '\\' + 'state_income_dictionary.csv', encoding='UTF-8', index=False)


