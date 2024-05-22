from finnlp.data_sources.social_media._base import Social_Media_Downloader

import requests
import pandas as pd
from tqdm import tqdm
import json

class Stocktwits_Streaming(Social_Media_Downloader):

    def __init__(self, args = {}):
        super().__init__(args)
        self.dataframe = pd.DataFrame()
        self.json = None

    def reset(self):
        self.dataframe = pd.DataFrame()
        self.json = None

    def download_streaming_stock(self, stock = "AAPL", rounds = 3):
        url = f"https://api.stocktwits.com/api/2/streams/symbol/{stock}.json"
        headers = {
            'accept': 'application/json',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'authorization': 'OAuth 8a881f43cbc7af061ec2aa35deec9b44f7e3cc09',
            'dnt': '1',
            'origin': 'https://stocktwits.com',
            'referer': 'https://stocktwits.com/',

            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:126.0) Gecko/20100101 Firefox/126.0',
            'cookie': 'auto_log_in=1; enw=1; anonymous_user_id=anonymous-US-WEB-1; mp_7517f2f4e72502c521b258f3bcee9025_mixpanel=%7B%22distinct_id%22%3A%20%22%24device%3A18f8f0c61292d7e-04c2fac999c6908-42282e32-384000-18f8f0c61292d7e%22%2C%22%24device_id%22%3A%20%2218f8f0c61292d7e-04c2fac999c6908-42282e32-384000-18f8f0c61292d7e%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%2C%22__mps%22%3A%20%7B%7D%2C%22__mpso%22%3A%20%7B%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5D%2C%22__mpap%22%3A%20%5B%5D%7D; _ga=GA1.1.1194578238.1716091904; _ga_PPZNBXBSPJ=GS1.1.1716101788.2.0.1716101788.0.0.0; _clck=6hbyy3%7C2%7Cflw%7C0%7C1600; _ga_GX96K6BSVJ=GS1.2.1716091931.1.0.1716092071.0.0.0; OptanonConsent=isGpcEnabled=0&datestamp=Sat+May+18+2024+21%3A13%3A49+GMT-0700+(Pacific+Daylight+Time)&version=202403.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=d88335ab-dd6a-416c-80c6-d9461c19a598&interactionCount=2&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CSPD_BG%3A1%2CC0005%3A1%2CC0002%3A1%2CC0004%3A1&intType=3; afUserId=d1f918e5-9e65-41a4-9374-34afa0821426-p; _lc2_fpi=99ca3b449c05--01hy7gt3aaqfnprdhg42avygba; _lc2_fpi_meta=%7B%22w%22%3A1716091948362%7D; cookie=610b9234-983a-4295-8006-c8aea6a117ba; cookie_cst=1izpLMgsJw%3D%3D; _au_1d=AU1D-0100-001716091949-QC506UGO-2BUC; panoramaId_expiry=1716696749731; _cc_id=f400382967d93b793f3228f61e6057c0; panoramaId=8319804da7b4ab8e5c715beaeeaf185ca02c844993dc402d91eb22d5cadff58c; __qca=P0-149400111-1716091949213; cto_bundle=bZ6WsF8wZzhuQTFvVDRuZ0RLZXBQa1ZnY0l3S1Ryckdha1V4UXNaVEdkVHVxckZSdU5CTHpmcnVXbHp6eENoSVVqeHBTSk5yN0FmWG8yUmE2RFVwMTZNRHR2UUhKeHZsWVRCcFVwT244c2lwb1lleWdiSldXb0JyTCUyRlhNTjNndyUyQjh4ZTFQTnFWSVNZeiUyRjZmcyUyQnlkZ0J3QnQ3ZXJZSXg0MWRRTUpwY2Q0R2p0NFpMSHN0QXhTaTZwT0NYY3htOXIzWTIzUE1aVUZpT3NVQmtUcHhwZTBkMHBoSVElM0QlM0Q; cto_bidid=s1gPYV93bGVNeENvNGtmZSUyRlhURUZIc2ZzNmxwV1BxJTJGTSUyRlRqYkhOUXlleGZ1N2Z3TFV5amJGaHJxaG05enBnNE9xdXo3TUdGUXRoRE9zaUxlbWczOFZRUSUyRkV6a2xabXV6SmFPb1FDRTlqZmd1TW83aTV6b0pkeUF6Smx5algxUUlBU1FE; __gads=ID=406258176982b970:T=1716091951:RT=1716091951:S=ALNI_MY5DrJr0oID5w5dIDD_iyoQjh0jyg; __gpi=UID=00000e075b6d95bf:T=1716091952:RT=1716091952:S=ALNI_MY7YWz7TPMP7v6Qk-PJsOuLpzLbKw; __eoi=ID=09c67dc124f4ed75:T=1716091952:RT=1716091952:S=AA-AfjY3bbMqhsz0kGV6dFl8Eo84; __ssid=461b8a74147b0b219286662f0ead806; OptanonAlertBoxClosed=2024-05-19T04:13:49.350Z; _ga_FVWZ0RM4DH=GS1.1.1716101788.1.0.1716101788.60.0.0',
            'host': 'api.stocktwits.com',
            'if-none-match': 'W/"c63fe0e11e9c95f8a798c5161790f822"',
            'priority': 'u=1',
        }
        for i in tqdm(range(rounds)):
            if i == 0:
                params = {
                "filter":"top",
                "limit":1000,
                # "max":410000000,
                }
            else:
                params = {
                "filter":"top",
                "limit":1000,
                "max":max,
                }
            response = self._request_get(url = url, headers=headers, params=params)
            if response is None:
                print(f"Fetch data fail. Please check your stock name :{stock} and connections. You may raise an issue if you can't solve this problem")
                continue
            else:
                res = json.loads(response.text)
                if self.json is None:
                    self.json = res
                else:
                    self.json['messages'].extend(res['messages'])
                max = res["cursor"]["since"]
                res = pd.DataFrame(res["messages"])
                self.dataframe = pd.concat([self.dataframe, res])

        self.dataframe = self.dataframe.reset_index(drop = True)
