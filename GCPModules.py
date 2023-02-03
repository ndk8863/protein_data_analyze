# 接続の手順
# APIとサービスに移動→cloudstorageに関するapiが有効になっているかチェック
# サービスアカウントの作成、ここでapiの認証をチェックする　フルアクセスにする模様 

import os
from google.cloud import storage
from google.cloud import bigquery


class StorageGateway:
    """
    google cloud storageに接続し、ファイルのアップロードとダウンロードを行うクラス
    """

    storage_client = None

    def __init__(self,json_file_path):
        """
        引数として受け取ったパスから、ローカルに保存されたjsonfileを読み込み、
        サービスアカウントの秘密鍵情報をもったストレージクライアントのインスタンスを生成する。
        args: 
            blob_name string型 バケットに保存する際のblob名
            file_path string型 ローカルのアップロードしたいファイルのパス
            bucket_name string型 保存したいバケットの名前
        
        returns:
            boolean
        """
        self.storage_client = storage.Client.from_service_account_json(json_file_path)

  
    def upload_to_bucket(self,blob_name,file_path,bucket_name):
        """
        バケットにファイルをアップロードする
        args: 
            blob_name string型 バケットに保存する際のblob名
            file_path string型 ローカルのアップロードしたいファイルのパス
            bucket_name string型 保存したいバケットの名前
        
        returns:
            boolean
        """
        try:
            my_bucket = self.storage_client.get_bucket(bucket_name)
            my_blob = my_bucket.blob(blob_name)
            my_blob.upload_from_filename(file_path)
            return True
        except Exception as e:
            print(e)
            return False


    def upload_string_to_bucket(self,blob_name,string_data,bucket_name):
        """
        バケットにcsvデータをアップロードする
        args: 
            blob_name string型 バケットに保存する際のblob名
            string_data string型 アップロードしたい文字列のデータ
            bucket_name string型 保存したいバケットの名前
        
        returns:
            boolean
        """
        try:
            my_bucket = self.storage_client.get_bucket(bucket_name)
            my_blob = my_bucket.blob(blob_name)
            my_blob.upload_from_string(string_data,content_type='application/json')
            return True
        except Exception as e:
            print(e)
            return False

    def download_string_from_bucket(self,blob_name,bucket_name):
        """
        バケットから文字列データをダウンロードする
        args: 
            blob_name string型 バケットに保存する際のblob名
            csv_data string型 アップロードしたいcsvのデータ
            bucket_name string型 保存したいバケットの名前
        
        returns:
            csv_data ダウンロードしたcsvデータ
        """
        try:
            my_bucket = self.storage_client.get_bucket(bucket_name)
            my_blob = my_bucket.blob(blob_name)
            string_data = my_blob.download_as_string()
            # print(string_data)
            return string_data
        except Exception as e:
            print(e)
            return False

def insert_to_bigquery(table_id,data):
    '''
      指定したtable_idのテーブルに、dataを追加する
      args:
        params table_id string型 データを追加したいテーブルのid
        params data dict型 追加したいデータ
      returns
        無し
    '''
    client = bigquery.Client()

    # rows_to_insert = [
    #     data,
    # ]
    table = client.get_table(table_id)
    errors = client.insert_rows_json(table, data, row_ids=[None] * len(data))
    if errors == []:
      print("New rows have been added.")
    else:
      print("Encountered errors while inserting rows: {}".format(errors))
