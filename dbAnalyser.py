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
        
    def getTableRows(self, table_name):
        with self.connection.cursor() as cursor:
            sql = 'SELECT * FROM ' + table_name
            cursor.execute(sql)
            result = cursor.fetchall()
            return result
            
def main():
    obj = mysqlDBanalyser()
    obj.establishConnection()
    df = pd.DataFrame(data = obj.getTableRows('admin_role'))
#    with open("tableRows.csv", "w") as file:
#        file.write(df.to_csv())
    
    print(df)
    obj.closeConnection()
    
if __name__ == "__main__":
    main()
    