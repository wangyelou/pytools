#!/usr/bin/evn python
# -*- coding: UTF-8 -*-
"""
 pip install thrift
 pip install hbase-thrift
"""
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *
import traceback
import time
import threading
import hashlib
import random
import copy



class HbaseClientException(Exception):
    pass

class HbaseClientCollections():

    __trans = None
    __client = None
    __server = None

    def __init__(self, trans, client, server):
        self.__trans = trans
        self.__client = client
        self.__server = server
        
    def getClient(self):
        return self.__client
        
    def getServer(self):
        return self.__server
        
    def getTrans(self):
        return self.__trans

class HbaseClient():

    __hasusefulserverfirst = False
    __check_table = None
    retry = 3

    def __init__(self, nodes, timeout = 30000, check_table = 'hbase:namespace'):
        self.nodes = nodes
        self.timeout = timeout
        self.__useful_thrift_servers = {}
        self.__unuseful_thrift_nodes = nodes
        self.__check_table = check_table
        self.__connect()
        while True:
            time.sleep(1)
            if self.__hasusefulserverfirst is True:
                break
        

    def __connect(self):
        """
        hbase 连接
        创建线程维护可用的thrift server
        """
        def check():
            while True:
                self.__checkUnusefulClient()
                self.__checkUsefuleClient()
                time.sleep(2)
        
        self.threading = threading.Thread( target = check )
        self.threading.setDaemon(True)
        self.threading.start()


    def __checkUnusefulClient(self):
        """
        检验unuseful client
        """
        tmp = copy.deepcopy(self.__unuseful_thrift_nodes)
        for server in tmp:
            try:
                self.__log("\033[31mcheck unuseful thrift server %s\033[0m" % server)
                host, port = server.split(':')
                transport = TSocket.TSocket(host, port)
                transport.setTimeout(self.timeout)
                trans = TTransport.TBufferedTransport(transport)
                client = Hbase.Client(
                    TBinaryProtocol.TBinaryProtocol(trans)
                )
                trans.open()
                
                self.__useful_thrift_servers[self.__generateClientKey(server)] = HbaseClientCollections(trans, client, server)
                self.__unuseful_thrift_nodes.remove(server)
                self.__hasusefulserverfirst = True
            except:
                self.__log(traceback.format_exc())
                    
    def __checkUsefuleClient(self):
        """
        检验useful client
        """
        tmp = self.__useful_thrift_servers.copy()
        for key, server in tmp.iteritems():
            self.__log("\033[34mcheck useful thrift server %s\033[0m" % server.getServer())
            if self.__isUseful(server) is False:
                self.__dealUnusefulServer(server)

    def __getUsefulClient(self):
        """
        获取随机可用client
        """
        start = time.time()
        for i in range(0, self.retry - 1):
            usefulclientkeys = self.__useful_thrift_servers.keys()
            if len(usefulclientkeys) > 0:
                return self.__useful_thrift_servers.get(random.choice(usefulclientkeys), None)
            else:
                if self.threading.isAlive() is False:
                    self.__connect()
                if i <= self.retry - 1:
                    time.sleep(3)
        raise HbaseClientException('has no useful thrift server')        
        
    def createTable(self, tableName, columnsFamily):
        """
        创建表
        :param tableName
        :param columnsFamily [
            {"name" : "c1"}
        ]
        """
        for i in range(0, self.retry):
            try:
                client = None
                list = []
                for item in columnsFamily:
                    name = item.get("name") if item.get("name") else ""
                    maxVersions = item.get("maxVersions") if item.get("maxVersions") else 3
                    list.append(ColumnDescriptor(name, maxVersions))

                client = self.__getUsefulClient()
                return client.getClient().createTable(tableName, list)
                break
            except HbaseClientException as e:
                raise HbaseClientException(e)
            except:
                self.__dealException(client, traceback.format_exc(), i)


    def put(self, tableName, row, timestamp, **kwargs):
        """
        推入数据
        :param tableName
        :param timestamp
        :param kwargs BasicInfo = {"col1" : "b1"}
        """
        for i in range(0, self.retry):
            try:
                client = None
                mutations = []
                for columnFamily, item in kwargs.items():
                    for column, value in item.items():
                        mutations.append(
                            Mutation(
                                column = "%s:%s" % (columnFamily, column),
                                value = str(value) if value is not None else ''
                            )
                        )
                        
                client = self.__getUsefulClient()
                return client.getClient().mutateRowTs(tableName, row, mutations, long(timestamp))
                break
            except HbaseClientException as e:
                raise HbaseClientException(e)
            except:
                self.__dealException(client, traceback.format_exc(), i)
                
    def getRow(self, tableName, rowKey):
        """
        推入数据
        :param tableName
        :param rowKey
        """
        for i in range(0, self.retry):
            try:
                client = None
                client = self.__getUsefulClient()
                result = client.getClient().getRow(tableName, rowKey)
                return result
                break
            except HbaseClientException as e:
                raise HbaseClientException(e)
            except:
                self.__dealException(client, traceback.format_exc(), i)

    def __dealException(self, client, msg, i):
        """
        异常处理
        """
        if client is not None:
            msg = "%s, %s" % (client.getServer(), msg)
            self.__log(msg)
            if self.__isUseful(client) is True or i >= self.retry - 1:
                raise HbaseClientException(msg)
            else:
                self.__dealUnusefulServer(client)
        else:
            self.__log(msg)
            raise HbaseClientException(msg)

    def __dealUnusefulServer(self, client):
        """
        处理失效thrfit server
        """
        #连接关闭
        if client.getTrans().isOpen():
            client.getTrans().close()
        #删除可用server
        key = self.__generateClientKey(client.getServer())
        if self.__useful_thrift_servers.has_key(key):
            del self.__useful_thrift_servers[key]
            
        #新增失效server
        self.__unuseful_thrift_nodes.append(client.getServer())
        
        
    def __isUseful(self, client):
        """
        客户端是否失效
        :param HbaseClientCollections
        :return bool
        """
        try:
            client.getClient().isTableEnabled(self.__check_table)
            return True
        except:
            return False
        return False

    def __log(self, msg):
        results = msg.strip().split("\n")
        if len(results) > 1:
            print("%s   %s" % (results[0], results.pop()))
        elif len(results) == 1:
            print(results.pop())

    def __generateClientKey(self, server):
        """
        创建key
        :param server
        """
        hl = hashlib.md5()
        hl.update(server)
        return hl.hexdigest()



if __name__ == '__main__':

    client = HbaseClient(
        ["172.17.0.4:9090","172.17.0.3:9090","172.17.0.2:9090"],
        5000
    )
    
    """
    client.createTable('test', [
        {"name" : "cf1"}
    ])
    """
    
    while True:
        try:
            result = client.put('test', 'row2', 12, cf1 = {"col1" : "b1"})
            #print(result)
            #time.sleep(1)
            
        except:
            print(traceback.format_exc())






