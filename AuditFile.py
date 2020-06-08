import pandas as pd
class AuditFile:
	def __init__(self,ipfile,opfile):
		var=None
		self.ipfile=ipfile
		self.opfile=opfile
	def execute(self):
		try:
			df=pd.read_csv(self.ipfile,header=None)
			df.to_csv(self.opfile,index=False,header=None)
		except Exception as e:
			print(e)
			var="FAILED"
			return var
