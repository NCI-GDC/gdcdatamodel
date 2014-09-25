import os, imp, json, pprint

from sqlalchemy import create_engine, select, MetaData, Table, Column, Integer

currentDir = os.path.dirname(os.path.realpath(__file__))
basePath   = os.path.join(currentDir, 'base.py')
base       = imp.load_source('Export', basePath)

class Export(base.Export):

    def initialize(self, **kwargs):

        self.postgresSettings = {}
        database = kwargs.get('database', None)
        user     = kwargs.get('user', None)
        password = kwargs.get('password', None)
        host     = kwargs.get('host', None)
        port     = kwargs.get('port', None)

        self.table = kwargs.get('table', '')

        conn_str = 'postgresql://%s:%s@%s/%s' % (user, password, host, database)
        self.engine = create_engine(conn_str)
        self.metadata = MetaData()
        self.table = Table(self.table, self.metadata, autoload=True, autoload_with=self.engine)

    def export(self, doc, schedulerDetails, conversionDetails, **kwargs):

        docType = kwargs.get('type', None)

        table = self.table
        ins = table.insert().returning(table.c.id).values(type=docType, doc=doc)
        conn = self.engine.connect()
        result = conn.execute(ins).fetchall()
        conn.close()

        print schedulerDetails, conversionDetails
        pass

    def close(self, **kwargs):
        pass
        
