import os
import json
import pymysql
import requests
import dataclasses
from requests.auth import HTTPBasicAuth

def check_and_parse(result: requests.Response):
    if result.status_code != 200:
        logger.error("code: {}, body: {}", result.status_code, result.text)
        raise Exception(result.status_code)
    data = json.loads(result.text, strict=False)
    x = data.get('error', False)
    if x == 'false':
        x = False
    if x:
        raise Exception(result.text)
    return data


class DorisClient(object):
    def __init__(self,host,protocol,user,passwd,db='dw'):
        self.host = host
        self.protocol = int(protocol)
        self.user = user
        self.passwd = passwd
        self.db = db
    
    def doris_cursor(self):
        mysql_host = self.host
        mysql_prot = self.protocol
        mysql_user = self.user
        mysql_pd = self.passwd
        mysql_db = self.db
        connection = pymysql.connect(host=mysql_host, user=mysql_user, password=mysql_pd, db=mysql_db, port=mysql_prot, charset='utf8')
        cursor = connection.cursor()
        return cursor
    
    def run_sql(self,sql):
        """
            only run_sql without return results
        """
        cursor = self.doris_cursor()
        try:
            cursor.execute(sql)
            return True
        except:
            return False

    def read_sql(self,sql):
        """
            only run_sql with results and field_names
        """
        cursor = self.doris_cursor()
        cursor.execute(sql)
        res_rows = cursor.fetchall()
        field_names = [i[0] for i in cursor.description]
        return res_rows,field_names


@dataclasses.dataclass
class TGToken:
    token: str = ''
    expiration: int = 0

    def is_expired(self):
        return time.time() >= self.expiration


class TigerGraphClient(object):
    def __init__(self, host: str, rest_port: int, gsql_port: int, user: str, passwd: str, graph_name: str):
        self.host = host
        self.rest_port = rest_port
        self.gsql_port = gsql_port
        self.user = user
        self.passwd = passwd
        self.graph_name = graph_name
        self.token = TGToken()
        
    def __request_token(self):
        url = 'http://{}:{}/requesttoken'.format(self.host, self.rest_port)
        data = {'graph': self.graph_name}
        result = requests.post(url, json=data, auth=HTTPBasicAuth(self.user, self.passwd))
        result = check_and_parse(result)
        self.token.token = result['results']['token']
        self.token.expiration = result['expiration']
        
    def with_token(fn):
        def wrap(self, *args, **kwargs):
            if self.token.is_expired():
                self.__request_token()
            return fn(self, *args, **kwargs)

        return wrap
    
    @with_token
    def insert(self, entities=None, opt = None):
        url = 'http://{}:{}/graph/{}'.format(self.host, self.rest_port, self.graph_name)
        headers = {
            'Authorization': 'Bearer {}'.format(self.token.token)
        }
        result = requests.post(url, params=opt, json=entities, headers=headers)
        return check_and_parse(result)
