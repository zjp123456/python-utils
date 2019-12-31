#!/usr/bin/env python


import sys

project_dir='/'.join(sys.path[0].split("/")[0:-1])
sys.path.insert(0, project_dir)

from thrift.transport import TSocket
from thrift.transport import TSSLSocket
from thrift.transport import THttpClient

from thrift_hive_metastore import ThriftHiveMetastore
from thrift_hive_metastore.ttypes import *

#pip install thrift_hive_metastore==1.0.1
class HiveMetaStoreClient():
    def __init__(self,host='0.0.0.0',port=9083):
        self.host = host
        self.port = port
        self.uri = ''
        self.framed = False
        self.ssl = False
        self.http = False
        self.client=None
        self.transport=None

    def get_instance(self):
        if self.http:
            self.transport = THttpClient.THttpClient(self.host, self.port, self.uri)
        else:
            socket = TSSLSocket.TSSLSocket(self.host, self.port, validate=False) if self.ssl else TSocket.TSocket(self.host, self.port)
        if self.framed:
            self.transport = TTransport.TFramedTransport(socket)
        else:
            self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = ThriftHiveMetastore.Client(protocol)
        self.transport.open()

        return self.client

    def close(self):
        self.transport.close()


if __name__ == "__main__":
        client=HiveMetaStoreClient(host='127.0.0.1',port=9083).get_instance()


        databases=client.get_databases("*")
        for db in databases:
           print db
