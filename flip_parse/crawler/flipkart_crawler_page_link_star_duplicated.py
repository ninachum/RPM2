import pandas as pd
import requests
from bs4 import BeautifulSoup

from main.flip_global_variables import *

mobile_page='https://www.flipkart.com/mobiles/pr?sid=tyy%2C4io&p%5B%5D=facets.serviceability%5B%5D%3Dfalse&otracker=categorytree&p%5B%5D=facets.type%255B%255D%3DSmartphones&page='

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
global_page_link=''
now = datetime.now()
title='flipkart_pagelink_'+str(now.year)+str('%02d'%now.month)+str('%02d'%now.day)+str('%02d'%now.hour)+str('%02d'%now.minute)+str('.csv')
flipkart_csv=pd.read_csv(crawler_base_file_directory + '\\' + "flipkart_pagelink_sample.csv")
flipkart_csv.to_csv(crawler_base_file_directory + '\\' + title, index=False, encoding='utf-8')

csv_product_name=''
global_rank=1

global_csv_row=0
global_raw_row=0


def flip_spider():

    for i in range(1,2) :  # TODO: 원상복귀 시킬 것(원래 51)
        print("########",i,"########")
        mobile_page_list=mobile_page+str(i)
        sub_gather_product_list(mobile_page_list)


def sub_gather_product_list(data_url):    
    source_code = requests.get(data_url)
    soup = BeautifulSoup(source_code.text, "html.parser")
    global global_rank
    global global_product_name
    global global_total_star
    global global_total_ratings
    global global_total_reviews
    global global_price
    global global_net_price
    global global_page_link
    global global_csv_row
    global title
    flipkart_csv=pd.read_csv(crawler_base_file_directory + '\\' + title)
    global_csv_row=len(flipkart_csv.index-1)
    remarks=''
    if soup.findAll('div',{'class' : '_1UoZlX'})!=None :
        product_list=soup.findAll('div',{'class' : '_1UoZlX'})
        for product_list_each in product_list :
            flipkart_csv=pd.read_csv(crawler_base_file_directory + '\\' + title)
            global_csv_row=len(flipkart_csv.index-1)
            flipkart_csv.ix[global_csv_row,0]=global_rank

            if product_list_each.find('a',{'class' : '_31qSD5'}) != None :
                global_page_link='https://www.flipkart.com'+product_list_each.find('a',{'class' : '_31qSD5'}).get('href')
                flipkart_csv.ix[global_csv_row,8]=global_page_link
                print(global_page_link)
            if product_list_each.find('div',{'class' : '_3wU53n'}) != None :
                global_product_name=product_list_each.find('div',{'class' : '_3wU53n'}).text
                flipkart_csv.ix[global_csv_row,1]=global_product_name
                print(global_product_name)
            if product_list_each.find('div',{'class' : 'hGSR34'}) != None :
                global_total_star=product_list_each.find('div',{'class' : 'hGSR34'}).text
                flipkart_csv.ix[global_csv_row,2]=global_total_star
                print(global_total_star)
            if product_list_each.find('span',{'class' : '_38sUEc'}) != None :
                if product_list_each.find('span',{'class' : '_38sUEc'}).text.split("&")[0] != None :
                    global_total_ratings=product_list_each.find('span',{'class' : '_38sUEc'}).text.split("&")[0].split(" ")[0].replace(",","").strip()
                    flipkart_csv.ix[global_csv_row,3]=global_total_ratings
                    print(global_total_ratings)
                if len(product_list_each.find('span',{'class' : '_38sUEc'}).text.split("&"))==2 :
                    global_total_reviews=product_list_each.find('span',{'class' : '_38sUEc'}).text.split("&")[1].split(" ")[0].replace(",","").strip()
                    flipkart_csv.ix[global_csv_row,4]=global_total_reviews
                    print(global_total_reviews)
            if product_list_each.find('div',{'class' : '_1vC4OE _2rQ-NK'}) != None :
                global_price=product_list_each.find('div',{'class' : '_1vC4OE _2rQ-NK'}).text.replace("₹","").replace(",","")
                flipkart_csv.ix[global_csv_row,5]=global_price
                print(global_price)
            if product_list_each.find('div',{'class' : '_3auQ3N _2GcJzG'}) != None :
                if product_list_each.find('div',{'class' : '_3auQ3N _2GcJzG'}).text != None :
                    global_net_price=product_list_each.find('div',{'class' : '_3auQ3N _2GcJzG'}).text.replace("₹","").replace(",","")
                    flipkart_csv.ix[global_csv_row,6]=global_net_price
                    print(global_net_price)
            if product_list_each.find('div',{'class' : '_1lkeIA'}) != None :
                remarks=product_list_each.find('div',{'class' : '_1lkeIA'}).text
                flipkart_csv.ix[global_csv_row,7]=remarks
                print(remarks)
            global_rank+=1
            flipkart_csv.to_csv(crawler_base_file_directory + '\\' + title, index=False, encoding='utf-8')



flip_spider()
flipkart_csv=pd.read_csv(crawler_base_file_directory + '\\' + title)
flipkart_csv=flipkart_csv.drop(0)
flipkart_csv.to_csv(crawler_base_file_directory + '\\' + title, index=False, encoding='utf-8')

flipkart_csv=pd.read_csv(crawler_base_file_directory + '\\' + title)
now = datetime.now()
title='flipkart_pagelink_duplicated_'+str(now.year)+str('%02d'%now.month)+str('%02d'%now.day)+str('%02d'%now.hour)+str('%02d'%now.minute)+str('.csv')
flipkart_csv=flipkart_csv.loc[(flipkart_csv.loc[:,"Ratings"].duplicated()&flipkart_csv.loc[:,"Num_ratings"].duplicated()&flipkart_csv.loc[:,"Num_reviews"].duplicated())==False,:]
# flipkart_csv=flipkart_csv.loc[(flipkart_csv.loc[:,"Ratings"].duplicated()&flipkart_csv.loc[:,"Num_ratings"].duplicated()&flipkart_csv.loc[:,"Num_reviews"].duplicated()&flipkart_csv.loc[:,"DP"].duplicated())==False,:]

flipkart_csv.to_csv(crawler_base_file_directory + '\\' + title, index=False, encoding='utf-8')


###############################-*-title_date-*-###############################
title_txt=open(crawler_base_file_directory + '\\' + "title_date.txt", 'w')
title_txt.write(title)
title_txt.close()
###############################-*-star-*-###############################



flipkart_csv=pd.read_csv(crawler_base_file_directory + '\\' + "flipkart_pagelink_star_sample.csv")

flipkart_csv2=pd.read_csv(crawler_base_file_directory + '\\' + title)
page_link_list=list(flipkart_csv2['Link'])
len_link_list=len(page_link_list)
now = datetime.now()
title='flipkart_pagelink_star_'+str(now.year)+str('%02d'%now.month)+str('%02d'%now.day)+str('%02d'%now.hour)+str('%02d'%now.minute)+str('.csv')
flipkart_csv.to_csv(crawler_base_file_directory + '\\' + title, index=False, encoding='utf-8')


def star_flip_spider(rank):
    global global_raw_row
    global_raw_row = rank
    star_sub_gather_link_product_main(page_link_list[rank])


def star_sub_gather_link_product_main(data_url):

    source_code = requests.get(data_url)
    soup = BeautifulSoup(source_code.text, "html.parser")
    global title
    global csv_product_name
    global global_csv_row
    global global_raw_row
    global flipkart_csv
    global flipkart_csv2
    flipkart_csv=pd.read_csv(crawler_base_file_directory + '\\' + title)
    global_csv_row=len(flipkart_csv.index-1)
    flipkart_csv.ix[global_csv_row,0]=flipkart_csv2.iloc[global_raw_row,0]
    flipkart_csv.ix[global_csv_row,2]=flipkart_csv2.iloc[global_raw_row,2]
    flipkart_csv.ix[global_csv_row,3]=flipkart_csv2.iloc[global_raw_row,3]
    flipkart_csv.ix[global_csv_row,4]=flipkart_csv2.iloc[global_raw_row,4]
    flipkart_csv.ix[global_csv_row,5]=flipkart_csv2.iloc[global_raw_row,5]
    flipkart_csv.ix[global_csv_row,6]=flipkart_csv2.iloc[global_raw_row,6]
    flipkart_csv.ix[global_csv_row,17]=flipkart_csv2.iloc[global_raw_row,7]
    flipkart_csv.ix[global_csv_row,18]=flipkart_csv2.iloc[global_raw_row,8]
    
    if soup.find('span',{'class' : '_35KyD6'})!=None :
        flipkart_csv.ix[global_csv_row,1]=soup.find('span',{'class' : '_35KyD6'}).text
        print(soup.find('span',{'class' : '_35KyD6'}).text)

    if soup.findAll('div',{'class' : 'CamDho'}) !=None :        
        stars=soup.findAll('div',{'class' : 'CamDho'})
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
    if soup.findAll('text',{'class' : 'PRNS4f'}) !=None :             
        ratings_atr_score=soup.findAll('text',{'class' : 'PRNS4f'})
        ratings_atr_name=soup.findAll('div',{'class' : '_3wUVEm'})
        global global_camera_ratings
        global global_battery_ratings
        global global_display_ratings
        global global_vfm_ratings
        global global_performance_ratings

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

        for i in range(0,len(ratings_atr_score)) :
            print(ratings_atr_name[i].text)
            print(ratings_atr_score[i].text)

    if soup.findAll('div',{'class' : 'CamDho'}) !=None :        
        if len(stars)==5 :
            flipkart_csv.ix[global_csv_row,7]=global_five
            flipkart_csv.ix[global_csv_row,8]=global_four
            flipkart_csv.ix[global_csv_row,9]=global_three
            flipkart_csv.ix[global_csv_row,10]=global_two
            flipkart_csv.ix[global_csv_row,11]=global_one
    if soup.findAll('text',{'class' : 'PRNS4f'}) !=None :
        for atr in range(0,len(ratings_atr_score))  :
            if ratings_atr_name[atr].text == 'Camera' :
                flipkart_csv.ix[global_csv_row,12]=global_camera_ratings
            if ratings_atr_name[atr].text == 'Battery' :
                flipkart_csv.ix[global_csv_row,13]=global_battery_ratings
            if ratings_atr_name[atr].text == 'Display' :
                flipkart_csv.ix[global_csv_row,14]=global_display_ratings
            if ratings_atr_name[atr].text == 'Value for Money' :
                flipkart_csv.ix[global_csv_row,15]=global_vfm_ratings
            if ratings_atr_name[atr].text == 'Performance' :
                flipkart_csv.ix[global_csv_row,16]=global_performance_ratings            

    flipkart_csv.to_csv(crawler_base_file_directory + '\\' + title, index=False, encoding='utf-8')

for rank in range(0, len_link_list) :
    star_flip_spider(rank)
flipkart_csv=pd.read_csv(crawler_base_file_directory + '\\' + title)
flipkart_csv=flipkart_csv.drop(0)
flipkart_csv.to_csv(crawler_base_file_directory + '\\' + title, index=False, encoding='utf-8')



