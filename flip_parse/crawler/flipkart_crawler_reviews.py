import multiprocessing
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup

from main.flip_global_variables import *

mobile_page='https://www.flipkart.com/mobiles/pr?sid=tyy%2C4io&p%5B%5D=facets.serviceability%5B%5D%3Dfalse&otracker=categorytree&p%5B%5D=facets.type%255B%255D%3DSmartphones&page='

global_crawling_start_date_for_folder_name = datetime.now().strftime('%Y-%m-%d')
global_product_name=''
global_total_star=''
global_total_ratings=''
global_total_reviews=''
global_five=''
global_four=''
global_three=''
global_two=''
global_one=''
global_camera_ratings=''
global_battery_ratings=''
global_display_ratings=''
global_vfm_ratings=''
global_performance_ratings=''
global_reviews_category_num=0
global_price=''
global_net_price=''

flipkart_csv=pd.read_csv(crawler_base_file_directory + '\\' + "flipkart.csv")

title_txt=open(crawler_base_file_directory + '\\' + "title_date.txt", 'r')
flipkart_pagelink_title=title_txt.readline()
title_txt.close()
flipkart_csv2=pd.read_csv(crawler_base_file_directory + '\\' + flipkart_pagelink_title)
page_link_list=list(flipkart_csv2['Link'])
len_link_list=len(page_link_list)
csv_product_name=''

global_csv_row=0

def flip_spider(x):

    sub_gather_link_product_main(page_link_list[x])


# def sub_gather_product_list(data_url):
#     source_code = requests.get(data_url)
#     soup = BeautifulSoup(source_code.text, "html.parser")
#     soup_url=soup.findAll('a',{'class' : '_31qSD5'})
#     for temp_link in soup_url :
#         next_link='https://www.flipkart.com'+temp_link.get('href')
#         sub_gather_link_product_main(next_link)


def sub_gather_link_product_main(data_url):

    source_code = requests.get(data_url)
    soup = BeautifulSoup(source_code.text, "html.parser")
    global global_net_price
    global global_price
    global csv_product_name


    global global_product_name
    if soup.find('span',{'class' : '_35KyD6'})!=None :
        title=soup.find('span',{'class' : '_35KyD6'}).text
        global_product_name=title
        print(title)
        now = datetime.now()
        csv_product_name=title+' '+str(now.year)+str('%02d'%now.month)+str('%02d'%now.day)+str('%02d'%now.hour)+str('%02d'%now.minute)+str('.csv')
    
    if soup.find('div',{'class' : '_1vC4OE _3qQ9m1'})!=None :
        global_price=soup.find('div',{'class' : '_1vC4OE _3qQ9m1'}).text.replace("₹","").replace(",","")
        print(global_price)
    
    if soup.find('div',{'class' : '_3auQ3N _1POkHg'})!=None :
        global_net_price=soup.find('div',{'class' : '_3auQ3N _1POkHg'}).text.replace("₹","").replace(",","")
        print(global_net_price)

    is_ratings_reviews=soup.find('span',{'class' : '_1IcGRZ'})
    if is_ratings_reviews==None :
        if soup.find('span',{'class' : '_38sUEc'})!=None :
            ratings_reviews=soup.find('span',{'class' : '_38sUEc'}).text
            star=soup.find('div',{'class' : 'hGSR34'}).text
            global global_total_star
            global_total_star=star
            print(star)
            ratings=str(ratings_reviews).split("&")[0]
            reviews=str(ratings_reviews).split("&")[1]
            ratings_n=ratings.split(" ")[0].replace(",","").strip()
            ratings_t=ratings.split(" ")[1].strip()
            reviews_n=reviews.split(" ")[0].replace(",","").strip()
            reviews_t=reviews.split(" ")[1].strip()
            global global_total_ratings
            global_total_ratings=ratings_n
            global global_total_reviews 
            global_total_reviews=reviews_n
            print(ratings_n,ratings_t)    
            print(reviews_n,reviews_t)
    if soup.findAll('div',{'class' : 'CamDho'}) !=None :        
        stars=soup.findAll('div',{'class' : 'CamDho'})
        print("stars length : ",len(stars))
        global global_five
        if len(stars)==5 :
            global_five=stars[0].text.replace(",","").strip()
        global global_four
        if len(stars)==5 :
            global_four=stars[1].text.replace(",","").strip()
        global global_three
        if len(stars)==5 :
            global_three=stars[2].text.replace(",","").strip()
        global global_two
        if len(stars)==5 :
            global_two=stars[3].text.replace(",","").strip()
        global global_one
        if len(stars)==5 :
            global_one=stars[4].text.replace(",","").strip()
        for s in stars :
            if len(stars)==5 :
                print(s.text.replace(",","").strip())
    if soup.findAll('text',{'class' : 'PRNS4f'}) !=None :             
        ratings_atr_score=soup.findAll('text',{'class' : 'PRNS4f'})
        ratings_atr_name=soup.findAll('div',{'class' : '_3wUVEm'})
        global global_camera_ratings
        global global_battery_ratings
        global global_display_ratings
        global global_vfm_ratings
        global global_performance_ratings

        print("scores length : ",len(ratings_atr_score))
        for atr in range(0,len(ratings_atr_score))  :
            if ratings_atr_name[atr].text == 'Camera' :
                global_camera_ratings=ratings_atr_score[atr].text
            if ratings_atr_name[atr].text == 'Battery' :
                global_battery_ratings=ratings_atr_score[atr].text
            if ratings_atr_name[atr].text == 'Display' :
                global_display_ratings=ratings_atr_score[atr].text
            if ratings_atr_name[atr].text == 'Value for Money' :
                global_vfm_ratings=ratings_atr_score[atr].text
            if ratings_atr_name[atr].text == 'Performance' :
                global_performance_ratings=ratings_atr_score[atr].text

        # if len(ratings_atr_score)==4 :
        #     global_camera_ratings=ratings_atr_score[0].text
            
        # if len(ratings_atr_score)==4 :
        #     global_battery_ratings=ratings_atr_score[1].text
        # if len(ratings_atr_score)==4 :
        #     global_display_ratings=ratings_atr_score[2].text
            
        # if len(ratings_atr_score)==4 :
        #     global_vfm_ratings=ratings_atr_score[3].text
        for i in range(0,len(ratings_atr_score)) :
            print(ratings_atr_name[i].text)
            print(ratings_atr_score[i].text)
    global global_csv_row
    global flipkart_csv
    flipkart_csv=pd.read_csv(crawler_base_file_directory + '\\' + "flipkart.csv")
    flipkart_csv.to_csv(source_directory + '\\' + global_crawling_start_date_for_folder_name + '\\' + csv_product_name, index=False, encoding='utf-8')
    global_csv_row=len(flipkart_csv.index-1)
    if soup.find('span',{'class' : '_35KyD6'})!=None :
        flipkart_csv.ix[global_csv_row,0]=global_product_name
    if soup.find('div',{'class' : '_1vC4OE _3qQ9m1'})!=None :
        flipkart_csv.ix[global_csv_row,1]=global_price
    if soup.find('div',{'class' : '_3auQ3N _1POkHg'})!=None :
        flipkart_csv.ix[global_csv_row,2]=global_net_price  
    if is_ratings_reviews==None :
        if soup.find('span',{'class' : '_38sUEc'})!=None :
            flipkart_csv.ix[global_csv_row,3]=global_total_star
            flipkart_csv.ix[global_csv_row,4]=global_total_ratings
            flipkart_csv.ix[global_csv_row,5]=global_total_reviews
    if soup.findAll('div',{'class' : 'CamDho'}) !=None :        
        if len(stars)==5 :
            flipkart_csv.ix[global_csv_row,6]=global_five
            flipkart_csv.ix[global_csv_row,7]=global_four
            flipkart_csv.ix[global_csv_row,8]=global_three
            flipkart_csv.ix[global_csv_row,9]=global_two
            flipkart_csv.ix[global_csv_row,10]=global_one
    if soup.findAll('text',{'class' : 'PRNS4f'}) !=None :
        for atr in range(0,len(ratings_atr_score))  :
            if ratings_atr_name[atr].text == 'Camera' :
                flipkart_csv.ix[global_csv_row,11]=global_camera_ratings
            if ratings_atr_name[atr].text == 'Battery' :
                flipkart_csv.ix[global_csv_row,12]=global_battery_ratings
            if ratings_atr_name[atr].text == 'Display' :
                flipkart_csv.ix[global_csv_row,13]=global_display_ratings
            if ratings_atr_name[atr].text == 'Value for Money' :
                flipkart_csv.ix[global_csv_row,14]=global_vfm_ratings
            if ratings_atr_name[atr].text == 'Performance' :
                flipkart_csv.ix[global_csv_row,15]=global_performance_ratings            
        # if len(ratings_atr_score)==4 :
        #     flipkart_csv.ix[global_csv_row,11]=global_camera_ratings
        #     flipkart_csv.ix[global_csv_row,12]=global_battery_ratings
        #     flipkart_csv.ix[global_csv_row,13]=global_display_ratings
        #     flipkart_csv.ix[global_csv_row,14]=global_vfm_ratings
        #     flipkart_csv.ix[global_csv_row,15]=global_performance_ratings

    flipkart_csv.to_csv(source_directory + '\\' + global_crawling_start_date_for_folder_name + '\\' + csv_product_name,index=False,encoding='utf-8')
    if soup.find('div',{'class' : 'col _39LH-M'})!=None :
        reviews_link='https://www.flipkart.com'+soup.find('div',{'class' : 'col _39LH-M'}).findAll('a')[len(soup.find('div',{'class' : 'col _39LH-M'}).findAll('a'))-1].get('href')
        sub_gather_link_reviews(reviews_link)
    elif soup.find('div',{'class' : 'col _390CkK'})!=None :
        for reviews in soup.findAll('div',{'class' : 'col _390CkK'}) :
            if reviews.find('div',{'class' : 'hGSR34 E_uFuv'})!=None :
                flipkart_csv.ix[global_csv_row,17]=reviews.find('div',{'class' : 'hGSR34 E_uFuv'}).text
            elif reviews.find('div',{'class' : 'hGSR34 _1x2VEC'})!=None :
                flipkart_csv.ix[global_csv_row,17]=reviews.find('div',{'class' : 'hGSR34 _1x2VEC'}).text
            elif reviews.find('div',{'class' : 'hGSR34 _1nLEql E_uFuv'})!=None :
                flipkart_csv.ix[global_csv_row,17]=reviews.find('div',{'class' : 'hGSR34 _1nLEql E_uFuv'}).text
            if reviews.find('p',{'class' : '_2xg6Ul'})!=None :
                flipkart_csv.ix[global_csv_row,18]=reviews.find('p',{'class' : '_2xg6Ul'}).text
            if reviews.find('div',{'class' : 'qwjRop'})!=None :
                flipkart_csv.ix[global_csv_row,19]=reviews.find('div',{'class' : 'qwjRop'}).text
            if reviews.find('p',{'class' : '_3LYOAd _3sxSiS'})!=None :
                flipkart_csv.ix[global_csv_row,20]=reviews.find('p',{'class' : '_3LYOAd _3sxSiS'}).text
            if reviews.find('p',{'class' : '_19inI8'})!=None : 
                flipkart_csv.ix[global_csv_row,21]=reviews.find('p',{'class' : '_19inI8'}).text
            if reviews.findAll('p',{'class' : '_3LYOAd'})!=None :
                flipkart_csv.ix[global_csv_row,22]=reviews.findAll('p',{'class' : '_3LYOAd'})[1].text
            if reviews.find('div',{'class' : '_2ZibVB'})!=None :
                flipkart_csv.ix[global_csv_row,23]=reviews.find('div',{'class' : '_2ZibVB'}).find('span').text
            if reviews.find('div',{'class' : '_2ZibVB _1FP7V7'})!=None :
                flipkart_csv.ix[global_csv_row,24]=reviews.find('div',{'class' : '_2ZibVB _1FP7V7'}).find('span').text
            global_csv_row+=1
            flipkart_csv.to_csv(source_directory + '\\' + global_crawling_start_date_for_folder_name + '\\' + csv_product_name,index=False,encoding='utf-8')

def sub_gather_link_reviews(data_url):

    source_code = requests.get(data_url)
    soup = BeautifulSoup(source_code.text, "html.parser")
    reviews_all_href=[]
    reviews_all_name=[]
    if soup.find('div',{'class' : '_36Dmoj'})!=None :
        reviews_=soup.find('div',{'class' : '_36Dmoj'})
        reviews_all_href=reviews_.findAll('a')
        reviews_all_name=reviews_.findAll('span')
    reviews_link_list=[]
    reviews_link_list_category=[]
    global global_reviews_category_num
    if len(reviews_all_href) == 6 :
        for i in range(0,6) :
            
            reviews_link_list.append('https://www.flipkart.com'+reviews_all_href[i].get('href'))
            reviews_link_list_category.append(reviews_link_list[i].replace("MOST_HELPFUL","MOST_RECENT"))
            # reviews_link_list_category.append(reviews_link_list[i].replace("MOST_HELPFUL","MOST_RECENT"))
            # reviews_link_list_category.append(reviews_link_list[i].replace("MOST_HELPFUL","POSITIVE_FIRST"))
            # reviews_link_list_category.append(reviews_link_list[i].replace("MOST_HELPFUL","NEGATIVE_FIRST"))


        global_reviews_category_num=len(reviews_link_list_category)
        if global_reviews_category_num==6 :
            sub_gather_link_reviews_each(0,reviews_link_list_category[0])
            sub_gather_link_reviews_each(1,reviews_link_list_category[1])
            sub_gather_link_reviews_each(2,reviews_link_list_category[2])
            sub_gather_link_reviews_each(3,reviews_link_list_category[3])
            sub_gather_link_reviews_each(4,reviews_link_list_category[4])
            sub_gather_link_reviews_each(5,reviews_link_list_category[5])
    else :
        if reviews_all_href == [] :
            sub_gather_link_reviews_each(0,data_url)
        else :
            for i in range(0,len(reviews_all_href)) :
                reviews_link_list.append('https://www.flipkart.com'+reviews_all_href[i].get('href'))
                reviews_link_list_category.append(reviews_link_list[i].replace("MOST_HELPFUL","MOST_RECENT"))

            for j in range(0,len(reviews_all_name)) :
                if reviews_all_name[j].text=='Overall' :
                    sub_gather_link_reviews_each(0,reviews_link_list_category[j])
                elif reviews_all_name[j].text=='Camera' :
                    sub_gather_link_reviews_each(1,reviews_link_list_category[j])
                elif reviews_all_name[j].text=='Battery' :
                    sub_gather_link_reviews_each(2,reviews_link_list_category[j])
                elif reviews_all_name[j].text=='Display' :
                    sub_gather_link_reviews_each(3,reviews_link_list_category[j])
                elif reviews_all_name[j].text=='Value for Money' :
                    sub_gather_link_reviews_each(4,reviews_link_list_category[j])
                elif reviews_all_name[j].text=='Performance' :
                    sub_gather_link_reviews_each(5,reviews_link_list_category[j])




def sub_gather_link_reviews_each(col_num,data_url):
    col=col_num
    source_code = requests.get(data_url)
    soup = BeautifulSoup(source_code.text, "html.parser")
    if soup.find('div',{'class' : '_2zg3yZ _3KSYCY'})!=None :
        if soup.find('div',{'class' : '_2zg3yZ _3KSYCY'}).find('span') != None :
            pages=int(str(soup.find('div',{'class' : '_2zg3yZ _3KSYCY'}).find('span').text).split(' ')[3].replace(",", ""))
            if soup.find('a',{'class' : '_2Xp0TH'}) !=None :
                pages_link=soup.find('a',{'class' : '_2Xp0TH'}).get('href')
                pages_link='https://www.flipkart.com'+pages_link[:(len(pages_link)-1)]
                for i in range(1,pages+1) :
                    if i>999 :
                        break
                    sub_gather_link_reviews_each_pages(col,pages_link+str(i))



def sub_gather_link_reviews_each_pages(col_num,data_url):

    global global_csv_row
    global flipkart_csv
    source_code = requests.get(data_url)
    soup = BeautifulSoup(source_code.text, "html.parser")
    col=16
    category='MOST_RECENT'
    if col_num==0 :
        col=16
    elif col_num==1  :
        col=25
    elif col_num==2  : 
        col=34
    elif col_num==3  :
        col=43
    elif col_num==4  :
        col=52   
    elif col_num==5  :
        col=61                      

    if soup.findAll('div',{'class':'col _390CkK _1gY8H-'})!=None :
        review_box=soup.findAll('div',{'class':'col _390CkK _1gY8H-'})
        for reviews in review_box :
            flipkart_csv.ix[global_csv_row,col]=category
            if reviews.find('div',{'class' : 'hGSR34 E_uFuv'})!=None :
                print(reviews.find('div',{'class' : 'hGSR34 E_uFuv'}).text) #star 5 4 3
                flipkart_csv.ix[global_csv_row,col+1]=reviews.find('div',{'class' : 'hGSR34 E_uFuv'}).text
            elif reviews.find('div',{'class' : 'hGSR34 _1x2VEC'})!=None :
                print(reviews.find('div',{'class' : 'hGSR34 _1x2VEC'}).text) #star 2
                flipkart_csv.ix[global_csv_row,col+1]=reviews.find('div',{'class' : 'hGSR34 _1x2VEC'}).text
            elif reviews.find('div',{'class' : 'hGSR34 _1x2VEC E_uFuv'})!=None :
                print(reviews.find('div',{'class' : 'hGSR34 _1x2VEC E_uFuv'}).text) #star 2
                flipkart_csv.ix[global_csv_row,col+1]=reviews.find('div',{'class' : 'hGSR34 _1x2VEC E_uFuv'}).text
            elif reviews.find('div',{'class' : 'hGSR34 _1nLEql E_uFuv'})!=None :
                print(reviews.find('div',{'class' : 'hGSR34 _1nLEql E_uFuv'}).text) #star 1
                flipkart_csv.ix[global_csv_row,col+1]=reviews.find('div',{'class' : 'hGSR34 _1nLEql E_uFuv'}).text

            if reviews.find('p',{'class' : '_2xg6Ul'})!=None : 
                print(reviews.find('p',{'class' : '_2xg6Ul'}).text) #title
                flipkart_csv.ix[global_csv_row,col+2]=reviews.find('p',{'class' : '_2xg6Ul'}).text
            if reviews.find('div',{'class' : 'qwjRop'}).find('div').find('div')!=None : 
                #print(reviews.find('div',{'class' : 'qwjRop'}).find('div').find('div').text) #content
                flipkart_csv.ix[global_csv_row,col+3]=reviews.find('div',{'class' : 'qwjRop'}).find('div').find('div').text
            if reviews.find('p',{'class' : '_3LYOAd _3sxSiS'})!=None :
                print(reviews.find('p',{'class' : '_3LYOAd _3sxSiS'}).text) #customer name
                flipkart_csv.ix[global_csv_row,col+4]=reviews.find('p',{'class' : '_3LYOAd _3sxSiS'}).text
            if reviews.find('p',{'class' : '_19inI8'})!=None :
                print(reviews.find('p',{'class' : '_19inI8'}).text) # Buyer, region
                flipkart_csv.ix[global_csv_row,col+5]=reviews.find('p',{'class' : '_19inI8'}).text
            if reviews.findAll('p',{'class' : '_3LYOAd'})!=None :
                print(reviews.findAll('p',{'class' : '_3LYOAd'})[1].text) # days ago
                flipkart_csv.ix[global_csv_row,col+6]=reviews.findAll('p',{'class' : '_3LYOAd'})[1].text
            if reviews.find('div',{'class' : '_2ZibVB'})!=None :
                print(reviews.find('div',{'class' : '_2ZibVB'}).find('span').text) # good
                flipkart_csv.ix[global_csv_row,col+7]=reviews.find('div',{'class' : '_2ZibVB'}).find('span').text
            if reviews.find('div',{'class' : '_2ZibVB _1FP7V7'})!=None :
                print(reviews.find('div',{'class' : '_2ZibVB _1FP7V7'}).find('span').text) #bad
                flipkart_csv.ix[global_csv_row,col+8]=reviews.find('div',{'class' : '_2ZibVB _1FP7V7'}).find('span').text
            global_csv_row+=1
            flipkart_csv.to_csv(source_directory + '\\' + global_crawling_start_date_for_folder_name + '\\' + csv_product_name,index=False,encoding='utf-8')

   

try:
    if __name__ == '__main__':

        if os.path.exists(source_directory + '\\' + global_crawling_start_date_for_folder_name):
            print('Crawling data in ' + source_directory + '\\' + global_crawling_start_date_for_folder_name + ' already exists.')
            print('----- CANNOT PROCEED -----')
            exit(1)
        else:
            os.mkdir(source_directory + '\\' + global_crawling_start_date_for_folder_name)
            print('... Created folder named ' + source_directory + '\\' + global_crawling_start_date_for_folder_name)

        pool = multiprocessing.Pool(processes=16)
        pool.map(flip_spider, range(0, 2))  # TODO : 임시수정 제거할 것(원래 200)
except:
    pass