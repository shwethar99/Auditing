import mysql.connector
from mysql.connector import errorcode
import pandas as pd

class AuditTable:
	def __init__(self,tablename, opfilename):
		var=None
		self.tablename=tablename
		self.opfilename=opfilename
	
	def execute(self):
		try:
			dbname=self.tablename.split('.')[0]
			table=self.tablename.split('.')[1]
			cnx = mysql.connector.connect(user='root', password='',
                              	host='127.0.0.1',
                              	database=dbname)
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				print("Something is wrong with your user name or password")
			elif err.errno == errorcode.ER_BAD_DB_ERROR:
				print("Database does not exist")
			else:
				print(err)
			var="FAILED"
			return var
		else:
			try:
				df = pd.read_sql('SELECT * FROM '+table, con=cnx)
				df.to_csv(self.opfilename,index=False,header="None")
			except:
				var="FAILED"
				return var
