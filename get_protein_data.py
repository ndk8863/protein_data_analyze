from ProductSearchAPIGateway import RakutenItemGateway
# import CloudStorageConnecter as c
import os
import datetime

if __name__ == "__main__":
    r = RakutenItemGateway('my_apikey.json')
    r.test()

    # request_url = "https://app.rakuten.co.jp/services/api/IchibaItem/Search/20170706"
    search_keyword = "プロテイン"
    ng_keyword = ""

    r.search_params_setter(search_keyword,ng_keyword)
    max_page_count = 3

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

    df = r.request_api(max_page_count,item_key)

    # print(df)

    new_index = [
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
        ]

    new_columns_name =  [
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

    df = r.fix_df(df,new_index,new_columns_name)
    r.save_to_cloud_storage(df)

    # extracted_date = datetime.datetime.now()
    # extracted_date = extracted_date.date()
    # file_name = str(extracted_date) + "rakuten_protein_data.json"
    # # df.to_csv(file_name)
    # csv_data = df.to_csv()
    # print(csv_data)
    # print(df)
    # df.to_json(os.path.join(os.getcwd(),file_name),orient='index',force_ascii=False)
    # extracted_json_data = df.to_json(orient='index')
    # debug
    # print(extracted_json_data)