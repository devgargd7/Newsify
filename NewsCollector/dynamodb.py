import boto3
from email.utils import parsedate_tz
import datetime

from botocore.exceptions import ClientError


def get_all_news_from_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb'
        )

    table = dynamodb.Table('NewsCollection2')

    try:
        response = table.scan()
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Items']


def load_news(news_list, index=0, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('NewsCollection2')
    news_count = 0
    today = datetime.date.today().strftime("%d-%m-%Y")
    for news in news_list:
        try:
            try:
                publication_date_name = 'published' if 'published' in news else 'pubDate'
                published = datetime.datetime(*(parsedate_tz(news[publication_date_name]))[:6]).isoformat()
                news[publication_date_name] = published
            except Exception as e:
                print("cannot parse the date due to: ", e, news)
            news['id'] = today + "-" + str(news_count + index).rjust(3, '0')
            print(news)
            table.put_item(Item=news)
            news_count = news_count+1
        except Exception:
            pass
    print("Loaded to DB count: ", news_count)

def create_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    # try:
    #     dynamodb.delete_table(TableName='NewsCollection')
    # except Exception:
    #     pass
    table = dynamodb.create_table(
        TableName='NewsCollection',
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'published',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'published',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'id',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table


if __name__ == '__main__':
    movie_table = create_table()
    print("Table status:", movie_table.table_status)
