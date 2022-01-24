# #!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time : 2022/1/24 16:38
# @Author : BruceLong
# @FileName: x_kafka.py
# @Email   : 18656170559@163.com
# @Software: PyCharm
# @Blog ：http://www.cnblogs.com/yunlongaimeng/
import json
import threading

from kafka import KafkaProducer
from kafka.errors import KafkaError
from .config import KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC


class SingletonType(type):
    _instance_lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            with SingletonType._instance_lock:
                if not hasattr(cls, "_instance"):
                    cls._instance = super(SingletonType, cls).__call__(*args, **kwargs)
        return cls._instance


class XKafka(metaclass=SingletonType):

    def __init__(self, host: str = KAFKA_HOST, port: str = KAFKA_PORT, kafka_topic=KAFKA_TOPIC):
        self.host = host
        self.port = port
        self.kafka_topic = kafka_topic
        self.__producer = KafkaProducer(bootstrap_servers='{kafka_host}:{kafka_port}'.format(
            kafka_host=self.host,
            kafka_port=self.port,
        ))

    def insert(self, item, **kwargs):
        '''
        topic, value=None, key=None, partition=None, timestamp_ms=None
        :param item:
        :param kwargs:
        :return:
        '''
        parmas_message = json.dumps(dict(item))
        try:
            self.__producer.send(topic=self.kafka_topic, value=parmas_message.encode('utf-8'), **kwargs)
            self.__producer.flush()
            return item
        except KafkaError as e:
            raise e


x_kafka = XKafka()
