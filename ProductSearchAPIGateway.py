import requests
import numpy as np
import pandas as pd
import json
import datetime
import re
from GCPModules import StorageGateway

class RakutenItemGateway:
    """
    楽天の商品検索APIと接続し、データをデータフレームに保存、
    別のフォルダまたはクラウドストレージに保存するクラス
    """

    API_ID = ""
    secret = ""
    url = ""
    search_params = {}


    def __init__(self,json_file_path):
        """
        インスタンス作成時にローカルに保存されたjsonファイルから楽天WEBServiceのAPIキーを取得し、
        インスタンス変数のAPI_ID,secret,urlにセットする
        args: 
            params json_file_path string型 /id,secret,urlが記載されているファイル 
        returns:
            無し
        """
        print(json_file_path)
        apikey_json_open = open(json_file_path,'r')
        apikey_json_load = json.load(apikey_json_open)
        self.API_ID = apikey_json_load['rakuten_api_key']['api_id']
        self.secret = apikey_json_load['rakuten_api_key']['secret']
        self.url = apikey_json_load['rakuten_api_key']['url']

    def remove_extra_string_on_item_name(self,ItemNameSeries):
        """
        商品名から分析に不要な文字列を削除する
        args: 
            params ItemNameSeries Series型 
        returns:
            return result Series型 文字列を修正したobjectを返す
        """
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


    def search_params_setter(self,search_keyword,ng_keyword):
        """
        APIによる商品検索に必要なパラメータをセットする
        args: 
            params ng_keyword string型 /検索から除外したいキーワード
            params api_id string型 /リクエストに必要なapiキー
        returns:
            無し
        """

        self.search_params = {
            "format" : "json",
            "keyword" : search_keyword,
            # "NGKeyword":ng_keyword,
            "applicationId" : self.API_ID,
            "availability" : 1,
            "hasReviewFlag":1,
            "hits" : 30,
            "page" : 0,
            "field":1,
            "availability":1,
            "sort" : "-reviewCount",
            "postageFlag":1
        }


    def request_api(self,max_page_count,target_key):
        """
        APIにアクセスして、抽出したい情報を抽出し、データフレームに格納して返す
        args: 
            params max_page_count int型 /検索するページ数を設定する
            target_key list型 /抽出したデータから、リストに該当するキーのデータをdfに格納する
        returns:
            return df DataFlame型 /抽出したデータを格納するデータ
        """
        extracted_item_list = []
        for page in range(1,max_page_count + 1):
            self.search_params['page'] = page
            response = requests.get(self.url,self.search_params)
            result = response.json()
            if ('error_description' in result) is True:
                print("continueします")
                continue
            
            for i in range(0,len(result['Items'])):
                insert_item_row = {}
                item = result['Items'][i]['Item']
                for key,value in item.items():
                    if key in target_key:
                        insert_item_row[key] = value
                extracted_item_list.append(insert_item_row.copy())
        df = pd.DataFrame(extracted_item_list)
        return df

    def fix_df(self,df,new_index,new_columns_name):
        """
        datalakeにデータを格納する前に、データの形を整える。
        args: 
            params df DataFrame /修正したいdf
            params new_index list /新しいカラムの並びのリスト
            params new_cloumns_name /新しいカラムの名前のリスト
        returns:
            return df DataFrame /修正後のDataFlame
        """

        extracted_date = datetime.datetime.now()
        extracted_date = extracted_date.date()       
        df['extracted_date'] = extracted_date.strftime('%Y-%m-%d')


        df = df.reindex(columns=new_index)
        df.columns = new_columns_name
        index = df.index + 1

        # df['商品名'] = remove_extra_string_on_item_name(df['商品名'])
        return df


    def save_to_cloud_storage_as_csv(self,df):
        """
        データフレームをcsvの形に変換し、
        クラウドストレージに保存する
        args: 
            params df DataFrame型/保存するdf
        returns:
            無し
        """

        extracted_date = datetime.datetime.now()
        extracted_date = extracted_date.date()

        file_name = str(extracted_date) + "rakuten_protein_data.csv"
        csv_data = df.to_csv()
        # print(csv_data)
        bucket_name = "protein_datalake999"

        json_file_path = 'local_apikey/gcp_key.json'
        c = StorageGateway(json_file_path)
        c.upload_string_to_bucket(file_name,csv_data,bucket_name)

    def save_to_cloud_storage_as_json(self,df):
        """
        データフレームをcsvの形に変換し、
        クラウドストレージに保存する
        args: 
            params df DataFrame型/保存するdf
        returns:
            無し
        """

        extracted_date = datetime.datetime.now()
        extracted_date = extracted_date.date()

        print(df)
        json_data = df.to_json(orient='index',force_ascii=False)
        print(json_data)
        file_name = str(extracted_date) + "rakuten_protein_data.json"
        bucket_name = "protein_datalake999"

        json_file_path = 'local_apikey/gcp_key.json'
        c = StorageGateway(json_file_path)
        c.upload_string_to_bucket(file_name,json_data,bucket_name)
