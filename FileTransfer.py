import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import sys,socket,datetime,os
from AuditTable import AuditTable
from AuditFile import AuditFile


#ASSUMES id IS A PRIMARY KEY IN THE TABLE WITH auto_increment OPTION ENABLED.



class FileTransfer:

#checks for format of arguments, initialises mysql connection params and class variables.
	def __init__(self,args):
		if len(args)<3:
			print("Usage: python3 file_transfer_python.py <input_file_path or input_table> <run_date(dd/mon/yy)>, file path should be complete")
			exit(1)
		else:
			self.input_file_path_or_table=args[1]
			self.run_date=args[2]
			format = "%d/%b/%y"
			try:
				date=datetime.datetime.strptime(self.run_date, format)
			except ValueError:
				print("Please enter run_date in the format 'dd/mon/yy' eg: 31/Dec/19")
				print(self.run_date)
				exit(1)
			if date>datetime.datetime.now():
				print("No future dates allowed!")
				exit(1)
		
		try:
			self.cnx = mysql.connector.connect(user='root', password='',
                              	host='127.0.0.1', database='auditDB')
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				print("Something is wrong with your user name or password")
				exit(1)	
			elif err.errno == errorcode.ER_BAD_DB_ERROR:
				print("Database does not exist")
				exit(1)
			else:
				print(err)
				exit(1)

		else:
			self.id=None
			self.input_table_name=None
			self.input_file_path=None
			self.file_name=None
			self.input_file_delimiter=None
			self.file_extension=None
			self.table_or_file=None
			self.new_date=None
			self.status=None
			self.formattedtime=None
			self.cursor=self.cnx.cursor()
			self.execute()


#updates initial progress
	def initial_commit(self):
		print("Process started.")
		insert_query="insert into audit (status,updated_by) values (%s,%s)"
		hostname=socket.gethostname()
		val=("IN-PROGRESS",hostname)
		self.cursor.execute(insert_query,val)
		self.cnx.commit()
		print("Process status updated- IN-PROGRESS")
		select_query="select max(id) from auditDB.audit";
		self.cursor.execute(select_query)
		id1=self.cursor.fetchone()
		self.id=id1[0]
		print("Fetched ID")
		print(self.id)


#determines type of argument and sets class variables and table entries accordingly.
	def find_if_file_or_table(self):
		str_list=self.input_file_path_or_table.split('/')
		if len(str_list)==1:
			print("Type detected: table")
			print(self.id)
			self.input_table_name=self.input_file_path_or_table
			self.input_file_path=None
			self.file_name=None
			self.file_extension=None
			self.input_file_delimiter=None
			self.table_or_file="table"
		else:
			self.file_name=str_list[-1]
			file_parts=self.file_name.split('.')
			self.file_extension=file_parts[-1]
			self.input_table_name=None
			self.input_file_path=self.input_file_path_or_table
			self.input_file_delimiter=None #can be changed later
			self.table_or_file="file"

		
		update_query="update audit set input_table_name=%s, input_file_path=%s, input_file_name=%s, file_type=%s, input_file_delimiter=%s where id=%s"
		val=(self.input_table_name, self.input_file_path, self.file_name, self.file_extension, self.input_file_delimiter, self.id)
		self.cursor.execute(update_query,val)
		print("Updated input file parameters.")
		self.cnx.commit()
		print("Rows updated:",self.cursor.rowcount)
		self.check_status()
	

		
#Converts and updates run_date in specified format into the table
	def update_run_date(self):
		self.new_date=datetime.datetime.strptime(self.run_date,"%d/%b/%y")
		self.new_date=self.new_date.strftime("%d-%m-%Y")
		print(self.new_date)
		update_query="update audit set run_date=%s where id=%s"
		val=(self.new_date, self.id)
		self.cursor.execute(update_query,val)
		self.cnx.commit()
		print("Updated run date.")


#sets last updated time in table with required format.
	def time(self):
		now=datetime.datetime.now()
		self.formattedtime=now.strftime("%d-%m-%Y %H:%M:%S")
		update_query="update audit set last_updated_time=%s where id=%s"
		val=(self.formattedtime,self.id)
		self.cursor.execute(update_query,val)
		print("Updated modified time")
		self.cnx.commit()


#checks if table/file exists and changes status if it doesn't exist
	def check_status(self):
		if self.input_table_name!=None:
			db=self.input_table_name.split('.')[0]
			table=self.input_table_name.split('.')[1]
			stmt = "select count(*) from information_schema.tables where table_schema like %s and table_name like %s"
			self.cursor.execute(stmt,(db,table))
			result = self.cursor.fetchone()
			if result[0]!=1:
				self.status="FAILED"
				query="update audit set status=%s, op_file_path=%s, run_date=%s where id=%s"
				val=(self.status,None,None,self.id)
				self.cursor.execute(query,val)
				self.cnx.commit()
				self.time()
				print("No such table exists")
				exit(1)
			
		else:
			if os.path.isfile(self.input_file_path)==False:
				self.status="FAILED"
				query="update audit set status=%s, op_file_path=%s, run_date=%s where id=%s"
				val=(self.status,None,None,self.id)
				self.cursor.execute(query,val)
				self.cnx.commit()
				self.time()
				print("No such file exists")
				exit(1)



		
#constructs output filename		
	def define_op_filename(self):
		op_filename=os.path.dirname(__file__)+"/op_path/"
		if self.table_or_file=="table":
			table=self.input_table_name.split('.')
			file_name=table[-1]
			op_filename+=file_name+"_"+self.new_date+".csv"
			self.run_table(self.input_table_name,op_filename)
		elif self.table_or_file=="file":
			file_name=self.file_name.split('.')[0]
			op_filename+=file_name+"_"+self.new_date+".csv"
			self.run_file(self.input_file_path, op_filename)
		query="update audit set op_file_path=%s where id=%s"
		val=(op_filename,self.id)
		self.cursor.execute(query,val)
		self.cnx.commit()
		print("Output filename updated.")


#if table, run the table module
	def run_table(self,table,op_filename):
		runningtable=AuditTable(table,op_filename)
		stat=runningtable.execute()
		self.update_status(stat)

#if file, run the file module		
	def run_file(self,filepath, op_filename):
		runningfile=AuditFile(filepath,op_filename)
		stat=runningfile.execute()
		self.update_status(stat)


#post running, update status
	def update_status(self,stat):
		if stat==None:
			query="update audit set status=%s where id=%s"
			self.status="COMPLETED"
			val=(self.status,self.id)
			self.cursor.execute(query,val)
			self.cnx.commit()
			self.time()
			print("Status updated.")
		elif stat=="FAILED":
			query="update audit set status=%s,op_file_path=%s,run_date=%s where id=%s"
			self.status="FAILED"
			val=(self.status,None,None,self.id)
			self.cursor.execute(query,val)
			self.cnx.commit()
			self.time()
			print("Status updated.")
		
		
	
#calls above defined methods.
	def execute(self):
		self.initial_commit()
		self.find_if_file_or_table()
		self.update_run_date()
		self.define_op_filename()
		self.time()
		self.cnx.close()


