from fastapi import FastAPI
import boto3
import botocore
from boto3.dynamodb.conditions import Key
import pandas as pd
import re

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/recommendations/{user_id}")
async def read_item(user_id):
    dynamodb = boto3.resource('dynamodb'
                              )
    recommendation_table = dynamodb.Table('Recommendations')
    news_collection_table = dynamodb.Table('NewsCollection2')
    try:
        response = recommendation_table.query(KeyConditionExpression=Key('user_id').eq(user_id))
        if response['Items']:
            feed = [news_collection_table.query(KeyConditionExpression=Key('id').eq(news_id))['Items'] for news_id in response['Items'][0]['feed']]
            response['Items'][0]['feed'] = filter_columns(feed)
            return response['Items']
        else:
            response = recommendation_table.query(KeyConditionExpression=Key('user_id').eq('default'))
            feed = [news_collection_table.query(KeyConditionExpression=Key('id').eq(news_id))['Items'] for news_id
                    in response['Items'][0]['feed']]
            response['Items'][0]['feed'] = feed
    except botocore.exceptions.ClientError as e:
        print(e.response['Error']['Message'])
        return e.response['Error']['Message']

@app.get("/history/{user_id}")
async def read_item(user_id):
    dynamodb = boto3.resource('dynamodb'
                              )
    ut_interactions_table = dynamodb.Table('UserItemInteraction')
    news_collection_table = dynamodb.Table('NewsCollection2')
    try:
        response = ut_interactions_table.scan()
        if response['Items']:
            df_ut_interactions = pd.DataFrame(response['Items'])
            items_read_by_user = df_ut_interactions.loc[df_ut_interactions['user_id'] == user_id]['item_id'].tolist()
            return filter_columns([news_collection_table.query(KeyConditionExpression=Key('id').eq(news_id))['Items'] for news_id in items_read_by_user])
    except botocore.exceptions.ClientError as e:
        print(e.response['Error']['Message'])
        return e.response['Error']['Message']

def filter_columns(list_of_items):
    # print(list_of_items)
    keys_to_filter = ['title', 'summary', 'published', 'link']
    return [{key: re.compile(r'<.*?>').sub('', (item[0][key])) for key in keys_to_filter if key in item[0]} for item in list_of_items]