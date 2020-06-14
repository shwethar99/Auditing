import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import traceback
class AuditTable:
	def __init__(self,tablename, opfilename,run_date):
		var=None
		self.tablename=tablename
		self.opfilename=opfilename
		self.run_date=run_date
	def execute(self):
		try:
			dbname=self.tablename.split('.')[0]
			table=self.tablename.split('.')[1]
			cnx = mysql.connector.connect(user='root', password='',
                              	host='127.0.0.1')
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
				df = pd.read_sql('SELECT * FROM '+self.tablename, con=cnx)
				df.to_csv(self.opfilename,index=False,header="None")
				cursor=cnx.cursor()
				rep_db="reporting_db"
				stmt = "select count(*) from information_schema.tables where table_schema=%s and table_name like %s"
				cursor.execute(stmt,(rep_db,table))
				result = cursor.fetchone()
				rep_table="reporting_db."+table
				if result[0]!=1:
					cr_query="create table "+rep_table+" like "+self.tablename
					cursor.execute(cr_query)
					ins_query="insert into "+rep_table+" select * from "+self.tablename
					cursor.execute(ins_query)
					cnx.commit()
					alt_query="alter table "+rep_table+" add run_date varchar(255)"
					cursor.execute(alt_query)
					cnx.commit()
					upd_query="update "+rep_table+" set run_date =%s where 1"
					value=(str(self.run_date),)
					cursor.execute(upd_query,value)
					rowcount=cursor.rowcount
					cnx.commit()
				else:
					upd_query="update "+rep_table+" set run_date =%s where 1"
					value=(str(self.run_date),)
					cursor.execute(upd_query,value)
					rowcount=cursor.rowcount
					cnx.commit()
				return [rowcount,rep_table]
			except Exception as e:
				print(traceback.format_exc())
				var="FAILED"
				return var



