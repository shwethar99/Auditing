import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import sys,socket,datetime,os
from AuditTable import AuditTable
from AuditFile import AuditFile
import logging

#ASSUMES id IS A PRIMARY KEY IN THE TABLE WITH auto_increment OPTION ENABLED.



class FileTransfer:

#checks for format of arguments, initialises mysql connection params and class variables.
	def __init__(self,args):
		
		logging.basicConfig(filename='output.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
		self.logger=logging.getLogger('server_logger')
		self.logger.info('Process started')
		if len(args)<4:
			logging.error('Usage: python3 file_transfer_python.py <input_file_path or input_table> <run_date(dd/mon/yy)> <input_file_delimiter>, file path should be complete')
			exit(1)
		else:
			self.param_dict={"input_table_name":None, "input_file_name":None, "input_file_path":None, "file_type":None, "input_file_delimiter":args[3], "input_file_size":None, "input_row_count":None, "status":None, "op_file_path":None, "output_table_name":None, "output_row_count":None, "run_date":args[2],"last_updated_time":None, "id":None} 
			
			self.table_or_file=None
			self.input_file_path_or_table=args[1]

			self.format = "%d/%b/%y"
			try:
				
				date=datetime.datetime.strptime(self.param_dict['run_date'], self.format)
			except ValueError:
				self.logger.error("Please enter run_date in the format 'dd/mon/yy' eg: 31/Dec/19")
				exit(1)
			if date>datetime.datetime.now():
				self.logger.error("No future dates allowed!")
				exit(1)
		
		try:
			self.cnx = mysql.connector.connect(user='root', password='',
                              	host='127.0.0.1', database='auditDB', buffered=True)
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				self.logger.error("Something is wrong with your MySQL user name or password")
				exit(1)	
			elif err.errno == errorcode.ER_BAD_DB_ERROR:
				self.logger.error("MySQL Database does not exist")
				exit(1)
			else:
				self.logger.error(err)
				exit(1)

		else:
			
			self.cursor=self.cnx.cursor()
			self.execute()


#updates initial progress
	def initial_commit(self):
		
		insert_query="insert into audit (input_table_name,input_file_path,status,updated_by,last_updated_time) values (%s,%s,%s,%s,%s)"
		hostname=socket.gethostname()
		self.param_dict["status"]="IN-PROGRESS"
		val=(self.param_dict['input_table_name'],self.param_dict['input_file_path'],self.param_dict["status"],hostname,self.param_dict['last_updated_time'])
		self.cursor.execute(insert_query,val)
		self.cnx.commit()
		self.logger.info("Process status updated- IN-PROGRESS")
		select_query="select max(id) from auditDB.audit";
		self.cursor.execute(select_query)
		id1=self.cursor.fetchone()
		self.param_dict["id"]=id1[0]


#determines type of argument and sets class variables and table entries accordingly.
	def find_if_file_or_table(self):
		str_list=self.input_file_path_or_table.split('/')
		if len(str_list)==1:
			self.param_dict['input_table_name']=self.input_file_path_or_table
			self.table_or_file="table"
		else:
			self.param_dict["input_file_name"]=str_list[-1]
			file_parts=self.param_dict['input_file_name'].split('.')
			self.param_dict["file_type"]=file_parts[-1]
			self.param_dict["input_file_path"]=self.input_file_path_or_table
			self.table_or_file="file"
		self.time()
		self.initial_commit()
		self.check_status()
	

		
#Converts and updates run_date in specified format into the table
	def update_run_date(self):
		new_date=datetime.datetime.strptime(self.param_dict["run_date"],"%d/%b/%y")
		self.param_dict["run_date"]=new_date.strftime("%d-%m-%Y")


#sets last updated time in table with required format.
	def time(self):
		now=datetime.datetime.now()
		formattedtime=now.strftime("%d-%m-%Y %H:%M:%S")
		self.param_dict["last_updated_time"]=formattedtime


#checks if table/file exists and changes status if it doesn't exist
	def check_status(self):
		if self.param_dict['input_table_name']!=None:
			db=self.param_dict['input_table_name'].split('.')[0]
			table=self.param_dict['input_table_name'].split('.')[1]
			stmt = "select count(*) from information_schema.tables where table_schema like %s and table_name like %s"
			self.cursor.execute(stmt,(db,table))
			result = self.cursor.fetchone()
			if result[0]!=1:
				self.param_dict['status']="FAILED"
				self.time()
				self.update_audit()
				self.logger.error("No such table exists")
				exit(1)
			
		else:
			if os.path.isfile(self.param_dict['input_file_path'])==False:
				self.param_dict['status']="FAILED"
				self.time()
				self.update_audit()
				self.logger.error("No such file exists")
				exit(1)
			else: 
				file_size=os.path.getsize(self.param_dict['input_file_path'])
				self.param_dict["input_file_size"]= file_size/1024

		
#constructs output filename		
	def define_op_filename(self):
		op_filename=os.path.dirname(__file__)+"/op_path/"
		if self.table_or_file=="table":
			table=self.param_dict['input_table_name'].split('.')
			file_name=table[-1]
			op_filename+=file_name+"_"+self.param_dict['run_date']+".csv"
			self.param_dict['op_file_path']=op_filename
			query="select * from "+self.param_dict['input_table_name']
			self.cursor.execute(query)
			self.param_dict['input_row_count']=self.cursor.rowcount
			self.run_table(self.param_dict['input_table_name'],self.param_dict['op_file_path'],self.param_dict['run_date'])
		elif self.table_or_file=="file":
			file_name=self.param_dict['input_file_name'].split('.')[0]
			op_filename+=file_name+"_"+self.param_dict['run_date']+".csv"
			self.param_dict['op_file_path']=op_filename
			self.run_file(self.param_dict['input_file_path'], self.param_dict['op_file_path'],self.param_dict["input_file_delimiter"],self.param_dict["file_type"])
		


#if table, run the table module
	def run_table(self,table,op_filename,run_date):
		runningtable=AuditTable(table,op_filename,run_date)
		stat=runningtable.execute()
		self.update_status(stat)

#if file, run the file module		
	def run_file(self,filepath, op_filename, delim, ext):
		runningfile=AuditFile(filepath,op_filename,delim,ext)
		stat=runningfile.execute()
		self.update_status(stat)


#post running, update status
	def update_status(self,stat):
		if stat!="FAILED":
			self.param_dict['status']="COMPLETED"
			self.param_dict['output_row_count']=stat[0]
			if self.table_or_file=='table':
				self.param_dict['output_table_name']=stat[1]
			else:
				self.param_dict['input_row_count']=stat[0]
				self.param_dict['output_row_count']=stat[1]
			self.time()
			
		else:
			self.param_dict['status']="FAILED"
			self.time()
			self.param_dict['op_file_path']=None
			self.param_dict['output_table_name']=None
			self.param_dict['output_row_count']=None
			self.param_dict['run_date']=None
			
	
	def update_audit(self):
		self.time()
		query="update audit set input_table_name=%s, input_file_name=%s, input_file_path=%s, file_type=%s, input_file_delimiter=%s, input_file_size=%s, input_row_count=%s, status=%s, op_file_path=%s, output_table_name=%s, output_row_count=%s, run_date=%s, last_updated_time=%s where id=%s"
		keyvalues=self.param_dict.values()
		val=tuple(keyvalues)
		self.cursor.execute(query,val)
		self.cnx.commit()
		self.logger.info("Process over- with status- "+self.param_dict['status'])
		self.logger.info("Rows updated: "+str(self.cursor.rowcount))
		
		 
#calls above defined methods.
	def execute(self):
		
		self.find_if_file_or_table()
		self.update_run_date()
		self.define_op_filename()
		self.time()
		self.update_audit()
		self.cnx.close()


