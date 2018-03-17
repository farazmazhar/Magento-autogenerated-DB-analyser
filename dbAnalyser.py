# -*- coding: utf-8 -*-
"""
Created on Sat Mar 17 18:16:32 2018

@author: faraz
"""

import dbInfo
import pymysql
import pandas as pd

class mysqlDBanalyser:
    connection = None
    
    db_name = ""
    mysql_username = ""
    mysql_password = ""
    ip = ""
    port = ""
    
    def __init__(self):
        self.db_name = dbInfo.getDBInfo()[0]
        self.mysql_username = dbInfo.getDBInfo()[1]
        self.mysql_password = dbInfo.getDBInfo()[2]
        self.ip = dbInfo.getDBInfo()[3]
        self.port = dbInfo.getDBInfo()[4]
    
    def establishConnection(self):
        self.connection = pymysql.connect(host = self.ip,
                                     user = self.mysql_username,
                                     password = self.mysql_password,
                                     db = self.db_name,
                                     charset = 'utf8mb4',
                                     cursorclass = pymysql.cursors.DictCursor)
        
    def closeConnection(self):
        self.connection.close()
        
    def getTableRows(self, table_name, columns = "*"):
        with self.connection.cursor() as cursor:
            query = 'SELECT * FROM ' + table_name
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        
    def getTableNames(self, columns = "*"):
        with self.connection.cursor() as cursor:
            query = "SELECT " + columns + " FROM information_schema.tables WHERE TABLE_SCHEMA = '" + self.db_name +"'"
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        
    def getNonEmptyTables(self, columns = "*"):
        with self.connection.cursor() as cursor:
            query = "SELECT " + columns + " FROM information_schema.tables WHERE TABLE_SCHEMA = '" + self.db_name +"' AND NOT TABLE_ROWS = 0"
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        
    def getColumnNames(self, columns = "*"):
        with self.connection.cursor() as cursor:
            query = "SELECT " + columns + " FROM information_schema.columns WHERE TABLE_SCHEMA = '" + self.db_name +"'"
            cursor.execute(query)
            result = cursor.fetchall()
            return result
    
    def generateCommonColumnTableDict(self):
        cctDict = {}
        
        for item in self.getColumnNames():
            try:
                cctDict[item['COLUMN_NAME']].append(item['TABLE_NAME'])
            except:
                cctDict[item['COLUMN_NAME']] = [item['TABLE_NAME']]
                
        return cctDict
            
