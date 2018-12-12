# 将来的には Step Functions や SNS も試したい
# AWSのLambdaからLambdaを呼んで、Slackにメッセージを送信する - Qiita
# https://qiita.com/seisyu1985/items/76a7a10543ea38dd07de
import boto3
from datetime import datetime, timedelta
import json
from pprint import pprint
from pytz import timezone
from tzlocal import get_localzone

date_format = "%Y/%m/%d %H:%M"
local_zone = get_localzone()


# 名前は Lambda の設定名に合わせる
def lambda_handler(event, context):

    lambda_client = boto3.client("lambda")
    params = {
        "symbol": "BTC_JPY",
        "first": 20001,
        "last": 20500,
        "step": 500
    }
    response = lambda_client.invoke(
        FunctionName="bitflyer_data_getter",
        # 同期実行
        InvocationType="RequestResponse",
        Payload=json.dumps(params)
    )

    res_payload = json.loads(response["Payload"].read())
    pprint(res_payload)

    # 今回のバッチ実行
    # 月１の定期実行
    # ccxt で最新を取得し、その番号と比較して実行判断


if __name__ == '__main__':
    lambda_handler(None, None)
