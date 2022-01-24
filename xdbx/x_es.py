# #!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time : 2022/1/24 17:14
# @Author : BruceLong
# @FileName: x_es.py
# @Email   : 18656170559@163.com
# @Software: PyCharm
# @Blog ：http://www.cnblogs.com/yunlongaimeng/
import hashlib
import json

import pandas as pd
from elasticsearch import Elasticsearch

from config import ES_HOST_PORT_LIST
from x_single import SingletonType
from itertools import chain


class XES(metaclass=SingletonType):
    def __init__(self, host_port_list=ES_HOST_PORT_LIST):
        self.host_port_list = host_port_list.split(';') if ES_HOST_PORT_LIST else []
        self.index = None
        self.batch_size = 100000
        self.max_request_timeout = 60
        self.batch_id = ''
        pass

    def connect(self):
        es = Elasticsearch(self.host_port_list)
        return es

    # def query(self, index: str, query_json: str, to_excel: bool = False):
    def query(self, query_json: str, to_excel: bool = False):
        es = self.connect()
        self.batch_id = hashlib.md5((str(query_json)).encode('utf8')).hexdigest()
        query_body = json.loads(query_json)
        size = query_body.get('size')
        if size > self.batch_size:
            query_body['size'] = self.batch_size
        contents = es.search(index=self.index, body=query_body, request_timeout=self.max_request_timeout)
        hits_result = [i['_source'] for i in contents.get('hits').get('hits') if i]
        count = contents.get('hits').get('total')
        # aggregations_temp = contents.get('aggregations') if contents.get('aggregations') else []
        # aggregations_result = [aggregations_temp[key] for key in aggregations_temp.keys()]
        # print(aggregations_result, count)
        # print(hits_result)

        if to_excel:
            # self.__init_excel()
            df = pd.DataFrame(hits_result)
            df.fillna('', inplace=True)
            df.to_excel(f'es_result_{self.batch_id}.xlsx', encoding='utf_8_sig', engine='xlsxwriter')
            # self.writer.save()
        pass

    def __get_data(self):
        pass

    def __init_excel(self):
        self.file_name = f'es_result_{self.batch_id}.xlsx'
        self.writer = pd.ExcelWriter(self.file_name)

    def run(self):
        pass


x_es = XES()
x_es.index = 'colordata_eureka'
x_es.host_port_list = ['120.92.150.128:9200']
query_json = '''{
  "size": 100000, 
  "query": {
    "bool": {
      "must": [
        {
          "range": {
            "replyDate": {
              "gte": "2022-01-12 00:00:00",
              "lte": "2022-01-18 00:00:00",
              "format": "yyyy-MM-dd HH:mm:ss"
            }
          }
        }
      ]
    }
  },"aggs": {
    "CliendIds": {
      "terms": {
        "field": "ClientID",
        "size": 1000
      },"aggs": {
        "days": {
          "date_histogram": {
            "field": "replyDate",
            "interval": "day"
          }
        }
      }
    }
  }
}'''
x_es.query(query_json=query_json, to_excel=True)
