import os
import json
import pymysql
import requests
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
    

class TgGraphClient(object):
    def __init__(self, ip, user, passwd, graph_name, protocol="http",):
        self.protocol = protocol
        self.ip = ip
        self.user = user
        self.passwd = passwd
        self.graph_name = graph_name
        
    def insert(self, entities):
        url = '%s://%s:9000/graph/%s' % (self.protocol, self.ip, self.graph_name)
        result = requests.post(url, json=entities, auth=HTTPBasicAuth(self.user, self.passwd))
        return check_and_parse(result)
