import os
import time
import requests
from functools import wraps
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
load_dotenv()

tg_test_host = os.getenv("TIGERGRAPH_TEST_HOST")
tg_test_port = os.getenv("TIGERGRAPH_TEST_REST_SERVER_PORT")
tg_user = os.getenv("TIGERGRAPH_USERNAME")
tg_pwd = os.getenv("TIGERGRAPH_ANALYST_PASSWORD")


def with_token(fn):
    @wraps(fn)
    def wrap(self, *args, **kwargs):
        if time.time() >= self.expiration - 60:  # 提前60s认为过期
            self.request_token()
        return fn(self, *args, **kwargs)

    return wrap


class TigergraphClient(object):
    def __init__(self, graph_name: str, host=tg_test_host, rest_port=tg_test_port, user=tg_user, passwd=tg_pwd):
        self.host = host
        self.rest_port = rest_port
        self.user = user
        self.passwd = passwd
        self.graph_name = graph_name
        self.token = ""
        self.expiration: int = 0

    def request_token(self):
        url = 'http://{}:{}/requesttoken'.format(self.host, self.rest_port)
        data = {'graph': self.graph_name}
        result = requests.post(url, json=data, auth=HTTPBasicAuth(self.user, self.passwd))
        result = result.json()
        print(result)
        self.token = result['results']['token']
        self.expiration = result['expiration']

    @with_token
    def run_query(self, query_name: str, **params):
        url = 'http://{}:{}/query/{}/{}'.format(self.host, self.rest_port, self.graph_name, query_name)
        headers = {
            'Authorization': 'Bearer {}'.format(self.token)
        }
        result = requests.post(url, json=params, headers=headers)
        return result.json()


if __name__ == '__main__':
    c = TigergraphClient('nft_profit')
    print()
    # res = c.run_query('nft_profit_calculation', address="0xbb34d62e24def6543470a9fd1d05f70375ce5ec5")
    # print(res)
