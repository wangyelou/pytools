#!/usr/bin/evn python
# -*- coding: UTF-8 -*-
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *
import traceback
import time


class HbaseClient():
    rety = 1

    def __init__(self, nodes, timeout = 30000):
        self.nodes = nodes
        self.timeout = timeout
        self.__connect()

    def __connect(self):
        """
        hbase 连接
        """
        for index in range(0, len(self.nodes)):
            try:
                host, port = self.nodes[index].split(':')
                for i in range(0, self.rety):
                    try:
                        transport = TSocket.TSocket(host, port)
                        transport.setTimeout(self.timeout)
                        self.transport = TTransport.TBufferedTransport(transport)
                        self.client = Hbase.Client(
                            TBinaryProtocol.TBinaryProtocol(self.transport)
                        )
                        self.transport.open()
                        break
                    except:
                        if i + 1 >= self.rety:
                            raise Exception('cannot connect hbase, info: %s' % traceback.format_exc())

                break
            except Exception as e:
                if index >= len(self.nodes):
                    raise Exception(e.message)


    def close(self):
        """
        连接关闭
        """
        if self.transport.isOpen():
            self.transport.close()

    def __reconnect(self):
        self.close()
        self.__connect()

    def createTable(self, tableName, columnsFamily):
        """
        创建表
        param tableName
        param columnsFamily [
            {"name" : "c1"}
        ]
        """
        for i in range(0, self.rety):
            try:
                list = []
                for item in columnsFamily:
                    name = item.get("name") if item.get("name") else ""
                    maxVersions = item.get("maxVersions") if item.get("maxVersions") else 3
                    list.append(ColumnDescriptor(name, maxVersions))

                self.client.createTable(tableName, list)
                break
            except:
                self.__reconnect()
                if i + 1 >= self.rety:
                    raise Exception(traceback.format_exc())

    def put(self, tableName, row, timestamp, **kwargs):
        """
        推入数据
        :param tableName
        :param timestamp
        :param kwargs BasicInfo = {"col1" : "b1"}
        """
        for i in range(0, self.rety):
            try:
                mutations = []
                for columnFamily, item in kwargs.items():
                    for column, value in item.items():
                        mutations.append(
                            Mutation(
                                column = "%s:%s" % (columnFamily, column),
                                value = str(value) if value is not None else ''
                            )
                        )

                self.client.mutateRowTs(tableName, row, mutations, long(timestamp))

                break
            except:
                self.__reconnect()
                if i + 1 >= self.rety:
                    raise Exception(traceback.format_exc())

    def getRow(self, tableName, row):
        """
        获取一条数据
        """
        for i in range(0, self.rety):
            try:

                return self.client.getRow(tableName, row)

                break
            except:
                self.__reconnect()
                if i + 1 >= self.rety:
                    raise Exception(traceback.format_exc())





if __name__ == '__main__':

    client = HbaseClient(
        ["127.0.0.1:9090",],
        5000
    )

    client.put('test', 'row2', 1234564, BasicInfo={"a":"b"})






