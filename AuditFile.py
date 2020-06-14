import pandas as pd
import gzip
class AuditFile:
	def __init__(self,ipfile,opfile,delim,ext):
		var=None
		self.ipfile=ipfile
		self.opfile=opfile
		self.delim=delim 
		self.ext=ext
	def execute(self):
		try:
			if self.ext!="gz":
				df=pd.read_csv(self.ipfile,sep=self.delim,header=None)
				ip_rowcount=df.shape[0]
				df.to_csv(self.opfile,sep="^",header=None)
				op_rowcount=df.shape[0]
			else:
				with gzip.open(self.ipfile) as f:
					df=pd.read_csv(f,sep=self.delim,header=None)
					ip_rowcount=df.shape[0]
					df.to_csv(self.opfile,sep="^",header=None)
					op_rowcount=df.shape[0]
			return [ip_rowcount,op_rowcount]
		except Exception as e:
			print(e)
			var="FAILED"
			return var

