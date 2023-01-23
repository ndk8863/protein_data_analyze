import requests
import numpy as np
import pandas as pd
import json
import datetime
import re

# 商品名から分析に不要な文字列を削除する
# params ItemNameSeries Series型 
# return result Series型 文字列を修正したobjectを返す
def remove_extra_string_on_item_name(ItemNameSeries):
    item_name_list = ItemNameSeries.tolist()
    temp_data =""
    temp_data_list = []
    for i in range(0,len(item_name_list)):
        temp_data = item_name_list[i]
        temp_data = re.sub(r'\【.*?\】','',temp_data)
        temp_data = re.sub(r'\＼.*?\／','',temp_data)
        temp_data_list.append(temp_data)
        
    result = pd.Series(temp_data_list)
    return result

APP_ID = '1067554345477542293'
secret ='92fa5786d838475b2d0d5bbe3f36b0829f9c76c8'
URL = "https://app.rakuten.co.jp/services/api/IchibaItem/Search/20170706"
search_keyword = "プロテイン"
ng_keyword = ""
page = 0

search_params = {
    "format" : "json",
    "keyword" : search_keyword,
#     "NGKeyword":ng_keyword,
    "applicationId" : APP_ID,
    "availability" : 1,
    "hasReviewFlag":1,
    "hits" : 30,
    "page" : page,
    "field":1,
    "availability":1,
    "sort" : "-reviewCount",
    "postageFlag":1
}
# hitsは最大30件まで
# pageを増やすとどうなるか→複数ページにわたる検索結果を取得可能
# →大量の商品情報を取得できる

# 商品情報をリストで取得
extracted_item_list = []
max_page = 100
for page in range(1,max_page + 1):
    search_params['page'] = page
    print(page)
    response = requests.get(URL,search_params)
    result = response.json()
    if ('error_description' in result) is True:
        print("continueします")
        continue
    
    #resultから必要な情報を抜き出した後dictを作る
    item_key = [
        'itemCode',
        'itemName',
        'catchcopy',
        'itemCaption',
        'itemPrice',
        'itemUrl',
        'mediumImageUrls',
        'shopCode',
        'shopName',
        'shopUrl',
        'reviewAverage',
        'reviewCount',
        'pointRate',
        'asurakuFlag',
        'asurakuArea',
        'startTime',
        'endTime',
        'giftFlag',
        'taxFlag',
        'postageFlag',
        'creditCardFlag',
        'shopOfTheYearFlag',
        'shipOverseasFlag'
    ]
    
    for i in range(0,len(result['Items'])):
        insert_item_row = {}
        item = result['Items'][i]['Item']
        for key,value in item.items():
            if key in item_key:
                insert_item_row[key] = value
        extracted_item_list.append(insert_item_row.copy())


extracted_date = datetime.datetime.now()
extracted_date = extracted_date.date()        

df = pd.DataFrame(extracted_item_list)
df['extracted_date'] = extracted_date

df = df.reindex(columns=[
    'itemCode',
    'itemName',
    'catchcopy',
    'itemCaption',
    'itemPrice',
    'itemUrl',
    'mediumImageUrls',
    'shopCode',
    'shopName',
    'shopUrl',
    'reviewAverage',
    'reviewCount',
    'pointRate',
    'startTime',
    'endTime',
    'asurakuFlag',
    'asurakuArea',
    'giftFlag',
    'taxFlag',
    'postageFlag',
    'creditCardFlag',
    'shopOfTheYearFlag',
    'shipOverseasFlag',
    'extracted_date'
    ])
df.columns = [
    '商品コード',
    '商品名',
    'キャッチコピー',
    '説明文',
    '価格',
    '商品URL',
    '商品画像',
    '店舗コード',
    '店舗名',
    '店舗URL',
    'レビュー平均',
    'レビュー数',
    'ポイント率',
    'タイムセール開始時刻',
    'タイムセール終了時刻',
    'あす楽対象フラグ',
    'あす楽配送対象地域',
    'ギフト対応',
    '税込み/税抜き',
    '送料込/送料別',
    'クレジットカード対応',
    'ショップオブザイヤーフラグ',
    '海外配送の対応',
    '抽出日'
    ]
index = df.index + 1

# 商品名から余計な文字を取り除く
df['商品名'] = remove_extra_string_on_item_name(df['商品名'])
print(df.count)
# CSVで保存

df.to_csv(str(extracted_date) + "rakuten_protein_data.csv")


