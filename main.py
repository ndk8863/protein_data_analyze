from ProductSearchAPIGateway import RakutenItemGateway
from GCPModules import insert_to_bigquery
# import CloudStorageConnecter as c
# import os
# import datetime

if __name__ == "__main__":
    r = RakutenItemGateway('local_apikey/my_apikey.json')

    search_keyword = "プロテイン"
    ng_keyword = ""

    r.search_params_setter(search_keyword,ng_keyword)
    max_page_count = 3

    item_key = [
        'itemCode',
        'itemName',
        'catchcopy',
        'itemCaption',
        'mediumImageUrls',
        'shopCode',
        'shopName',
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
        'shopCode',
        'shopName',
        'extractedDate',
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
        'shipOverseasFlag'
        ]

    new_columns_name =  [
        'code',
        'name',
        'catchcopy',
        'item_caption',
        'price',
        'shop_code',
        'shop_name',
        'review_average',
        'review_count',
        'point_rate',
        'extracted_date',
        'start_time',
        'end_time',
        'asuraku_flag',
        'asuraku_area',
        'gift_flag',
        'tax_flag',
        'postage_flag',
        'credit_card_flag',
        'shop_of_the_year_flag',
        'ship_overseas_flag'
        ]

    df = r.fix_df(df,new_index,new_columns_name)
    r.save_to_cloud_storage_as_json(df)

    
    df = df.drop(columns=[
        'catchcopy',
        'item_caption',
        'start_time',
        'end_time'
        ])

    data = df.to_dict(orient="records")

    table_id = "ecdataanalyze.analyze_protein_data.rakuten_item_table"
    # insert_to_bigquery(table_id,data)


    # print(df)
    # r.save_to_cloud_storage_as_json(df)
