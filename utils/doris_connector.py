import os
import pymysql
from dotenv import load_dotenv
load_dotenv()

doris_host = os.getenv("doris_host")
doris_prot = os.getenv("doris_prot")
doris_user = os.getenv("doris_user")
doris_pwd = os.getenv("doris_pwd")


class DorisClient(object):
    def __init__(self,db: str,
                 host=doris_host,protocol=int(doris_prot),
                 user=doris_user, passwd=doris_pwd,):
        self.host = host
        self.protocol = protocol
        self.user = user
        self.passwd = passwd
        self.db = db

    @property
    def doris_cursor(self):
        mysql_host = self.host
        mysql_prot = self.protocol
        mysql_user = self.user
        mysql_pd = self.passwd
        mysql_db = self.db
        connection = pymysql.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_pd,
            db=mysql_db,
            port=mysql_prot,
            charset='utf8'
        )
        cursor = connection.cursor()
        return cursor

    def run_sql(self, sql):
        """
            only run_sql without return results
        """
        cursor = self.doris_cursor
        try:
            cursor.execute(sql)
            return True
        except ConnectionError:
            return False

    def read_sql(self, sql):
        """
            only run_sql with results and field_names
        """
        cursor = self.doris_cursor
        cursor.execute(sql)
        res_rows = cursor.fetchall()
        field_names = [i[0] for i in cursor.description]
        return res_rows, field_names
