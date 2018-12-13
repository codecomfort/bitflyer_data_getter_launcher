# 将来的には Step Functions や SNS も試したい
# AWSのLambdaからLambdaを呼んで、Slackにメッセージを送信する - Qiita
# https://qiita.com/seisyu1985/items/76a7a10543ea38dd07de
import base64
import boto3
from datetime import datetime, timedelta
import gzip
import json
import os
from pprint import pprint
from pytz import timezone
import requests
from tzlocal import get_localzone

date_format = "%Y/%m/%d %H:%M"
discord_post_url = os.environ["DISCORD_POST_URL"]
local_zone = get_localzone()


def decode_event_data(event_data):
    # イベントデータは base64 エンコーディング＆圧縮されてるのでもとに戻す
    gziped = base64.b64decode(event_data)
    unpacked = gzip.decompress(gziped).decode('utf-8')
    json_data = json.loads(unpacked)
    return json_data


def get_last_no(logEvent):
    bflog = json.loads(logEvent["message"].split("\t")[-1])
    # bitflyer_data_getter の log 例
    # {
    #     "name": "bitflyer executions",
    #     "first": 1,
    #     "last": 1000,
    #     "state": "completed"
    # }
    last = int(bflog["last"])
    return last


def post_to_discord(message):

    post_data = {
        "content": message
    }

    try:
        response = requests.post(discord_post_url, data=json.dumps(post_data),
                                 headers={'Content-Type': "application/json"})
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print('Request failed: {}'.format(e))
        # log.error('Request failed: {}'.format(e))


# 名前は Lambda の設定名に合わせる
def lambda_handler(event, context):
    """
    CloudWatch のロググループ「bitflyer_data_getter」 にてサブスクリプション設定し、
    completed を含むログをストリーム配信して本 Lambda を起動

    - 初回起動方法
        最初の1回は、bitflyer_data_getter のテスト機能を利用して起動
    - 以降の起動
        は、bitflyer_data_getter のログを本 lambda(bitflyer_data_getter_launcher) で
        サブスクリプションする(ストリーム配信される)
        ログメッセージを抜粋
        {
            "name": "bitflyer executions",
            "first": 1,
            "last": 1000,
            "state": "completed"
        }
        本 lambda にて last 番号から次の first 番号を算出し、再び bitflyer_data_getter を invoke する
    - 終了条件
        次の first 番号が、(bitflyer から取得した)最新の番号より大きければ終了
    - 無限ループの検知

    - 無限ループの強制停止
        万が一の無限ループに陥ったら、以下のいずれかの方法で停止させることができる
        - CloudWatch 側でストリーム配信設定を削除するか、
        - bitflyer_data_getter_launcher 側でトリガー設定している CloudWatch Logs を無効にする
        - bitflyer_data_getter の msg の "state": "completed" 行をコメントアウトしてアップロード

    """
    latest_execution_no = 2000000  # 最新の約定番号

    # pprint(event)
    json_data = decode_event_data(event['awslogs']['data'])
    logEvent_latest = json_data["logEvents"][-1]
    last = get_last_no(logEvent_latest)

    symbol = "BTC_JPY"
    next_first = last + 1
    if latest_execution_no < next_first:
        jst_now = datetime.now(local_zone).strftime(date_format)
        msg = '[{}] {} までの取得が終わりました'.format(jst_now, latest_execution_no)
        print(msg)
        post_to_discord(msg)
        return msg

    next_range = 10000   # 次の bitflyer_data_getter で取得する件数
    next_last = last + next_range
    if latest_execution_no < next_last:
        next_last = latest_execution_no

    params = {
        "symbol": symbol,
        "first": next_first,
        "last": next_last,
    }

    lambda_client = boto3.client("lambda")
    lambda_client.invoke(
        FunctionName="bitflyer_data_getter",
        # 非同期呼び出し(呼び出し先の lambda の完了を待たない)
        InvocationType="Event",
        Payload=json.dumps(params)
    )

    jst_now = datetime.now(local_zone).strftime(date_format)
    msg = '[{}] bitflyer_data_getter invoked (symbol: {}, first: {}, last: {})'.format(
        jst_now, symbol, next_first, next_last)
    print(msg)
    post_to_discord(msg)


if __name__ == '__main__':
    event = {
        'awslogs': {
            'data': 'H4sIAAAAAAAAAG1Qy27bMBD8lYDo0aq4fInUzUCcIEDSHKxTTUOg5JUjQA9DopsGhv89Kze59Ti7szOzc2E9znM4YvFxQpaz+3WxLl822+36ccNWbHwfcKKxVUpKxZ1zKqNxNx4fp/F8ok0a3ue0C311CGnVxqb7wKk8hBjKI8ZIxzf2Nk4YeqILDjYFkYJMdz+e18VmW+yt0HVd2UoAgrK2slwLMsxQOqxQ1CQxn6u5ntpTbMfhoe1Id2b5jj3ffP+Jl/9zL7twHuo3irG/5dj8wSEupxfWHiiOVOSjtBZacSk1l8YI7rjgmXVKWe7AaecMgDEgDSghpDScK4oUW2ouhp5KAK2UyTIJJgOz+m6U5P2we/r18Lr3cfk7AZGALLjOOeTa/CTybx+pVmykkEmDTiUAaBOrg0iU42iFQxka5ePFsyH06Fl+59n3p3f4F+vz0sns2YoWTTvNceHAgrrwBTjnC6a08UuhHvtThxEPnl39wK776yedOnBhCQIAAA=='
        }
    }
    lambda_handler(event, None)
