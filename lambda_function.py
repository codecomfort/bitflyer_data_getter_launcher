# 将来的には Step Functions や SNS も試したい
# AWSのLambdaからLambdaを呼んで、Slackにメッセージを送信する - Qiita
# https://qiita.com/seisyu1985/items/76a7a10543ea38dd07de
import base64
import boto3
from datetime import datetime, timedelta
from distutils.util import strtobool
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
    #     "state": "completed",
    #     "invoke_next": "false"
    # }
    last = int(bflog["last"])
    invoke_next = bflog["invoke_next"]
    return last, invoke_next


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
    """
    if event.get("manual") is not None:
        manual = event["manual"]

        last = int(manual["first"]) - 1
        invoke_next = manual["invoke_next"]
        next_range = int(manual["range"])
        symbol = manual["symbol"]
        next_first = int(manual["first"])
        latest_execution_no = next_first + next_range
    else:
        json_data = decode_event_data(event['awslogs']['data'])
        logEvent_latest = json_data["logEvents"][-1]

        last, invoke_next = get_last_no(logEvent_latest)
        # 次の bitflyer_data_getter で取得する件数
        next_range = int(os.environ["RANGE"])
        # リトライ失敗してラムダがコケた場合、この単位で取得し直ししなるので
        # あまり大きい値も微妙
        symbol = os.environ["SYMBOL"]
        next_first = last + 1
        latest_execution_no = int(os.environ["LATEST_EXECUTION_NO"])

    # 終了条件
    # 最終番号を超えようとしているか、
    # 手動実行でなく且つinvokeNext=Falseが指定されている場合
    if (latest_execution_no < next_first) or (event.get("manual") is None and not bool(strtobool(invoke_next))):
        jst_now = datetime.now(local_zone).strftime(date_format)
        msg = '[{}] {} までの取得が終わりました'.format(jst_now, latest_execution_no)
        print(msg)
        post_to_discord(msg)
        return msg

    next_last = last + next_range
    if latest_execution_no < next_last:
        next_last = latest_execution_no

    params = {
        "symbol": symbol,
        "first": next_first,
        "last": next_last,
        "invoke_next": invoke_next
    }

    lambda_client = boto3.client("lambda")
    lambda_client.invoke(
        FunctionName=os.environ["INVOKE_FUNCTION_NAME"],
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
    # CloudWatch logs からのイベントは以下のような形式でくる
    # event = {
    #     'awslogs': {
    #         'data': 'H4sIAAAAAAAAAG1Qy2rrMBT8lSK6jGs9Lcm7wE1LoY9FvGocjGwf55rrR7CUPgj59x4nTVcXtBBz5syZmSPpwXu3g+xrDyQlf5bZsnherdfLhxVZkPFjgAlhI6UQklprpUa4G3cP03jY4yR2Hz7uXF/WLi7b0HRfMBW1C67YQQi4fGavwwSuRzqnzMSMx0zEm9unZbZaZ9sqUY3iYGSlhATDrHBJXQsjagWlZRQl/KH01dTuQzsO922Hup6kG/J0vnsRL/53vejcYaj+oo3t2cfqHYYwrx5JW6MdIaU0SluphLBGGqutYIlk+EsoN4nQmBxjc6a0oFwraekcAS2FFpsLrscSmJJSc41PKbu4Norym8eX+9dtHuaViPGIiYzZlPKU6jukvuXBioonxjZR01AbMQYmKpsEImO5MCWjxkqeh2NOBtdDTtKbnFxz3sAnVIe5EZ+TBQ6advJh5nCKJimbsc79QoJSOkNoOvxIVWO/7yBAfRFoh/fxHxQDfIbLvHGdR+opH8hpe/oGd1JKeiwCAAA='
    #     }
    # }
    # テスト実行のパラメータは以下の形式
    event = {
        "manual": {
            "symbol": "BTC_JPY",
            # 取得開始の id
            "first": 3000001,
            # first からの増分
            "range": 10000,
            "invoke_next": "true"
        }
    }
    lambda_handler(event, None)
