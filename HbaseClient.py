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

    def __init__(self, host, **kwargs):
        self.host = host if host != '' else '127.0.0.1'
        self.port = int(kwargs.get('port')) if kwargs.get('port') is not None else 9090
        self.timeout = int(kwargs.get('timeout')) * 1000 if kwargs.get('timeout') is not None else 30000
        self.__connect()
        
        
    def __connect(self):
        """
        hbase 连接
        """
        for i in range(0, self.rety):
            try:
                transport = TSocket.TSocket(self.host, self.port)
                transport.setTimeout(self.timeout)
                self.transport = TTransport.TBufferedTransport(transport)
                self.client = Hbase.Client(
                    TBinaryProtocol.TBinaryProtocol(self.transport)
                )
                self.transport.open()
                break
            except:
                if i+1 >= self.rety:
                    raise Exception('cannot connect hbase, info: %s' % traceback.format_exc())
    
    def close(self):
        """
        连接关闭
        """
        if self.transport:
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
                if i+1 >= self.rety:
                    raise Exception(traceback.format_exc())
        
        
    def put(self, tableName, row, datas):
        """
        推入数据
        param tableName
        param datas
        [
            ["col_fam", "c1", "v1"]
        ]
        """
        for i in range(0, self.rety):
            try:
                mutations = []
                for item in datas:
                    if len(item) == 3:
                        mutations.append(
                            Mutation(
                                column = "%s:%s" % (item[0], item[1]),
                                value = item[2]
                            )
                        )
                
                self.client.mutateRow(tableName, row, mutations)
                
                break
            except:
                self.__reconnect()
                if i+1 >= self.rety:
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
                if i+1 >= self.rety:
                    raise Exception(traceback.format_exc())
    
    
    
    
    
    
    
    
    
    
    
    