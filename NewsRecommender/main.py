import datetime
import logging
import re
from decimal import Decimal

import boto3
import botocore
import nltk
import numpy as np
import pandas as pd
import simplejson as json
from boto3.dynamodb.conditions import Key

import colab_based
import content_based


def build_recommendations():
    dynamodb = boto3.resource('dynamodb'
                              )
    client = boto3.client('dynamodb'
                          )
    news_collection_table = dynamodb.Table('NewsCollection2')

    logging.info("Fetching News Data for past 3 days")
    news = []
    for day in range(0, 3):
        for i in range(0, 999):
            try:
                response = news_collection_table.query(KeyConditionExpression=Key('id').eq(
                    (datetime.date.today() - datetime.timedelta(days=day)).strftime("%d-%m-%Y") + "-" + str(i).rjust(3,
                                                                                                                     '0')
                ))
                if response['Items']:
                    news.append(response['Items'][0])
            except botocore.exceptions.ClientError as e:
                print(e.response['Error']['Message'])
                break
    df_news = pd.DataFrame(news).sort_values(by=['id'])
    logging.info("Fetched News Data")

    ut_interactions_table = dynamodb.Table('UserItemInteraction')
    df_ut_interactions = pd.DataFrame()
    try:
        response = ut_interactions_table.scan()
        if response['Items']:
            df_ut_interactions = pd.DataFrame(response['Items'])
    except botocore.exceptions.ClientError as e:
        print(e.response['Error']['Message'])
        raise

    emptyTable(client, dynamodb, 'Recommendations')
    recommendation_table = dynamodb.Table('Recommendations')

    recommendations = get_recommendations(df_ut_interactions, df_news)
    logging.info("loading Recommendations to DB...")
    for recom in recommendations:
        print(recom)
        recommendation_table.put_item(Item=recom)
    print("loaded")


def get_recommendations(df_ut_interactions=None, df_news=None, a=0.4, b=0.4, c=0.2):
    if df_ut_interactions is None:
        df_ut_interactions = pd.read_csv("ut_interactions.csv")
    if df_news is None:
        df_news = pd.read_csv("data.csv").sort_values(by=['id'])
    df_ut_interactions = df_ut_interactions.replace({"type": {"view": 1}})
    df_ut_interactions_pivoted = df_ut_interactions.pivot(index="user_id", columns="item_id", values="type").fillna(0)

    df_news = preprocess_news(df_news)
    df_content_based = content_based.content_based_score(df_ut_interactions, df_news)
    df_colab_based = colab_based.colab_based(df_ut_interactions_pivoted)

    logging.info("creating popularity scores")
    df_popular = df_ut_interactions.groupby(["item_id"])["user_id"].count().reset_index(name="count").sort_values(
        ['count'], ascending=False)
    df_popular['count_normalized'] = df_popular["count"] / df_popular["count"].max()
    users = df_ut_interactions.user_id.unique()
    final = [json.loads(json.dumps(
        {"user_id": 'default',
         "feed": [x for x in df_popular.sort_values(by=['count_normalized']).item_id.values.tolist()[:50]]}
    ), parse_float=Decimal)]

    logging.info("getting Recommendations for Users")
    for user_id in users:
        final_df = pd.merge(df_content_based[["id", user_id]], df_colab_based[[user_id]], how='left', left_on='id',
                            right_on='item_id').replace(np.nan, 0)
        final_df = pd.merge(final_df, df_popular, how='left', left_on='id', right_on='item_id').replace(np.nan, 0)
        final_df['total_score'] = float(a) * final_df[str(user_id) + "_x"] + float(b) * final_df[
            str(user_id) + "_y"] + float(c) * final_df['count_normalized']
        final_df = final_df[['id', 'total_score']].sort_values(by=['total_score'], ascending=False)
        items_read_by_user = df_ut_interactions.loc[df_ut_interactions['user_id'] == user_id]['item_id'].tolist()
        feed = [x for x in final_df.id.values.tolist()[:50] if x not in items_read_by_user]
        final.append(json.loads(json.dumps({"user_id": user_id, "feed": feed}, ignore_nan=True), parse_float=Decimal))
    return final


def extract_from_url(url):
    tokens = re.split('/|-|\.', url)
    return ' '.join(tokens)


def preprocess_news(df_news):
    stoplist = {'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself',
                'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself',
                'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that',
                'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
                'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as',
                'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through',
                'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off',
                'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how',
                'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not',
                'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should',
                'now'}
    stoplist.update(["https", "news", "com", "net", "www", "html", ":"])
    df_news['link_cleaned'] = df_news['link'].apply(
        lambda url: ' '.join([token for token in nltk.word_tokenize(extract_from_url(url)) if token not in stoplist]))
    df_news['title_cleaned'] = df_news['title'].apply(
        lambda text: ' '.join([token for token in nltk.word_tokenize(text) if token not in stoplist]))
    df_news['summary_cleaned'] = df_news['summary'].fillna("").apply(lambda text: ' '.join(
        [token for token in nltk.word_tokenize(re.compile(r'<.*?>').sub('', text)) if token not in stoplist]))
    df_news['combined_text'] = df_news[['link_cleaned', 'title_cleaned', 'summary_cleaned']].apply(
        lambda row: ' '.join(row.values.astype(str)), axis=1)
    return df_news


def deleteTable(client, table_name):
    logging.info('deleting table')
    return client.delete_table(TableName=table_name)


def createTable(client, dynamodb, table_name):
    waiter = client.get_waiter('table_not_exists')
    waiter.wait(TableName=table_name)
    logging.info('creating table')
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'user_id',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'user_id',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        },
        StreamSpecification={
            'StreamEnabled': False
        }
    )


def emptyTable(client, dynamodb, table_name):
    try:
        deleteTable(client, table_name)
    except botocore.errorfactory.ResourceNotFoundException as e:
        logging.error(table_name + " not found")
    finally:
        createTable(client, dynamodb, table_name)


if __name__ == '__main__':
    build_recommendations()
