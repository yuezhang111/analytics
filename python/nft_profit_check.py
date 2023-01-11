import time
import requests
from requests.auth import HTTPBasicAuth

tg_host = "192.168.100.151"
tg_port = 9000
graph_name = "nft_profit"
tg_user = "data_analyst"
tg_passwd = "uhdBEJa4zroToFr4Y8VbNJzsynHX9YeUXoCTi9MRe"


class Client(object):
    def __init__(self, host: str, rest_port: int, user: str, passwd: str, graph_name: str):
        self.host = host
        self.rest_port = rest_port
        self.user = user
        self.passwd = passwd
        self.graph_name = graph_name
        self.token = ""
        self.expiration: int = 0

    def __request_token(self):
        url = 'http://{}:{}/requesttoken'.format(self.host, self.rest_port)
        data = {'graph': self.graph_name}
        result = requests.post(url, json=data, auth=HTTPBasicAuth(self.user, self.passwd))
        result = result.json()
        print(result)
        self.token = result['results']['token']
        self.expiration = result['expiration']

    @staticmethod
    def with_token(fn):
        def wrap(self, *args, **kwargs):
            if time.time() >= self.expiration - 60:  # 提前60s认为过期
                self.__request_token()
            return fn(self, *args, **kwargs)

        return wrap

    @with_token
    def run_query(self, query_name: str, **params):
        url = 'http://{}:{}/query/{}/{}'.format(self.host, self.rest_port, self.graph_name, query_name)
        headers = {
            'Authorization': 'Bearer {}'.format(self.token)
        }
        result = requests.post(url, json=params, headers=headers)
        return result.json()


if __name__ == '__main__':
    c = Client(tg_host, tg_port, tg_user, tg_passwd, 'nft_profit')
    res = c.run_query('nft_profit_calculation', address="0xbb34d62e24def6543470a9fd1d05f70375ce5ec5")
    print(res)
