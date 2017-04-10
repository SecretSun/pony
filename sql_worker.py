#!/usr/bin/python2.7
# -*- coding: UTF-8 -*-

import json
import sys,os
import random
import datetime,time
import multiprocessing
import socket
import affinity
from configobj import ConfigObj

from log_class import *
import mysql_class 

reload(sys)
sys.setdefaultencoding("utf-8")

CONF_FILE=os.getcwd()+"/pony.ini"
if os.path.exists(CONF_FILE)==False:
        print "Configuration File Does Not Exist !!!"
        sys.exit()

config = ConfigObj(CONF_FILE, encoding="utf8")

WORKER = int(config['SqlWorker']['Worker'])
WORKER_HOST = config['SqlWorker']['Host']
WORKER_BASE_PORT = int(config['SqlWorker']['BasePort'])
WORKER_LOG = config['SqlWorker']['WorkerLog']

BINLOG_INFO_INDEX_FILE = config['BaseLog']['BinlogInfoIndexFile'] 
BINLOG_INFO_FILE = config['BaseLog']['BinlogInfoFile']
BINLOG_RUNING_LOG = config['BaseLog']['BinlogRunningLog']

if os.path.exists(BINLOG_INFO_FILE):
	if os.path.getsize(BINLOG_INFO_FILE) <> 0:
		file = open(BINLOG_INFO_FILE)
		list_lines = file.read().strip().split(' ')
		file.close()
		START_LOG_FILE = list_lines[0]
		START_LOG_POS = int(list_lines[1])
	else:
		START_LOG_FILE = config['BinLog']['FileName']
		START_LOG_POS = int(config['BinLog']['FilePos'])
else:
	START_LOG_FILE = config['BinLog']['FileName']
	START_LOG_POS = int(config['BinLog']['FilePos'])


class sql_runner:
	def __init__(self, host, port):
		self.host = host
		self.port = port
		self.logger_runner = Logger("%s.%d"%(WORKER_LOG, self.port), logging.INFO, logging.INFO)
		if not os.path.exists("%s.%d"%(WORKER_LOG, self.port)):os.mknod("%s.%d"%(WORKER_LOG, self.port))

		try:
			self.logdb = mysql_class.MySQL(config['LogDBServer']['Host'],config['LogDBServer']['User'],config['LogDBServer']['Password'],int(config['LogDBServer']['Port']))
			self.logdb.select_db(config['LogDBServer']['Db'])
		except Exception, e:
			self.logdb.close()
			self.logger_runner.error('LogDB Error: %s'%e)
			sys.exit(1)

		try:
			self.proxydb = mysql_class.MySQL(config['DbProxy']['Host'],config['DbProxy']['User'],config['DbProxy']['Password'],int(config['DbProxy']['Port']))
		except Exception, e:
			self.proxydb.close()
			self.logger_runner.error('PorxyDB Error: %s'%e)
			sys.exit(1)
		
		try:
			self.logger_runner.info('Work Process START:%s %d'%(self.host, self.port))
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.bind((self.host, self.port))
			s.listen(10)
			self.logger_runner.info('Work Process WORKING:%s %d'%(self.host, self.port))
		except socket.error, e:
			self.logdb.close()
			self.logger_runner.error('Work Process Not WORKING:%s %d %s'%(self.host, self.port, e))

		try:
			while True:
				conn,addr = s.accept()
				self.data = eval(conn.recv(20480))
				self.logger_runner.info('SQL Runner %d Connected by %s. Package: %s'%(self.port, addr, self.data))
				conn.sendall('Done')
				conn.close()

				self.binlog_host = self.data['binlog_host']
				self.binlog_port = self.data['binlog_port']
				self.binlog_datetime = self.data['binlog_datetime']
				self.through_datetime = self.data['through_datetime']
				self.binlog_file_name = self.data['binlog_file_name']
				self.binlog_pos = self.data['binlog_pos']
				self.binlog_datetime = self.data['binlog_datetime']
				self.schema_name = self.data['schema_name']
				self.table_name = self.data['table_name']
				self.table_id = self.data['table_id']
				self.primary_key = self.data['primary_key']
				self.before_vals = self.data['before_vals']
				self.vals = self.data['vals']
				self.before_sql_type = self.data['before_sql_type']
				self.sql_type = self.data['sql_type']

				self.runner()

		except socket.error, e:
			self.logdb.close()
			self.logger_runner.error('SQL Runner %d Client Connect Problem:%s %s %s'%(self.port, e, self.binlog_file_name, self.binlog_pos))

		self.proxydb.close()
		self.logdb.close()

	def runner(self):
		b_sql = self.transform_sql(self.schema_name, self.table_name, self.primary_key, self.before_vals, self.before_sql_type)
		sql = self.transform_sql(self.schema_name, self.table_name, self.primary_key, self.vals, self.sql_type)

		logdb_sql_data = (
			{
				'binlog_host':self.binlog_host,
				'binlog_port':self.binlog_port,
				'binlog_datetime':self.binlog_datetime,
				'through_datetime':self.through_datetime,
				'binlog_schema':self.schema_name,
				'binlog_table':self.table_name,
				'table_pk':self.primary_key,
				'table_id':self.table_id,
				'binlog_pos':self.binlog_pos,
				'binlog_file_name':self.binlog_file_name,
				'query_json':str(self.vals),
				'real_sql':sql,
				'query_before_json':str(self.before_vals),
				'real_before_sql':b_sql
			}
		)
		try:
			self.logger_runner.info('SQL Runner %d. %s %s STAERT'%(self.port, self.binlog_file_name, self.binlog_pos))
			self.proxydb.select_db(self.schema_name)
			self.proxydb.query(sql)
			self.proxydb.commit()
                        self.write_file("%s.%s.%d"%('sql_worker', BINLOG_INFO_FILE, self.port), "%s %s"%(self.binlog_file_name, self.binlog_pos), "Replace")
			self.logdb.insert('bin_log', logdb_sql_data)
			self.logdb.commit()
                        self.write_file("%s.%s.%d"%('sql_worker', BINLOG_INFO_INDEX_FILE, self.port), "%s %s"%(self.binlog_file_name, self.binlog_pos), "Write")
			self.logger_runner.info('SQL Runner %d. %s %s SUCCEED'%(self.port, self.binlog_file_name, self.binlog_pos))
		except Exception, e:
			self.proxydb.rollback()
			self.proxydb.close()
			self.logdb.rollback()
			self.logdb.close()
			self.logger_runner.error('SQL Runner %d. %s %s UNSUCCEED'%(self.port, self.binlog_file_name, self.binlog_pos))
			sys.exit(1)
		
	def write_file(self, file_name, message, write_type):
		if (write_type == "Replace"):
			f = open(file_name,'w')
		elif (write_type == "Write"):
			f = open(file_name,'a')
		f.write(message + '\n')
		f.close()	
	
	def transform_sql(self, schema_name, table_name, pk_name, value_json, sql_type):
		sql = "None"
		if (sql_type == 'Delete'):
			sql = "DELETE FROM %s.%s WHERE %s in ('%s')"%(schema_name, table_name, pk_name, value_json[pk_name])
		elif (sql_type == 'Update'):
			update_str = ''
			for k in value_json:
				if k != pk_name:
					update_str = update_str + str(k) + "='" + str(value_json[k]) + "',"
				else:
					pass
			update_str = update_str[:-1]
			sql = "UPDATE %s.%s SET %s WHERE %s in ('%s')" %(schema_name, table_name, update_str, pk_name, value_json[pk_name])
		elif (sql_type == 'Insert'):
			column_name = ''
			data_value = ''
			for k in value_json:
				column_name = column_name + "," + str(k)
				data_value = data_value + ",'" + str(value_json[k]) + "'"
			column_name = column_name[1:]
			data_value = data_value[1:]
			sql = "INSERT INTO %s.%s (%s) VALUE (%s)"%(schema_name, table_name, column_name, data_value)
		else:
			pass

		return sql


def main():
	#logger = Logger(WORKER_LOG, logging.INFO, logging.INFO)
	#if not os.path.exists(WORKER_LOG):os.mknod(WORKER_LOG)
	process_record = []
	for w in range(WORKER):
		work_port = WORKER_BASE_PORT + w
		process = multiprocessing.Process(target = sql_runner, args = (WORKER_HOST, work_port))
		process.daemon = True
		process_record.append(process)
		process.start()
		pid = process.pid
		cpuid = w + 1
        	#affinity.set_process_affinity_mask(pid, cpuid)
		#logger.info('Work Process START:%d'%work_port)

	for process in process_record:
		process.join()

if __name__ == "__main__":
	main()
