#!/usr/bin/python2.7
# -*- coding: UTF-8 -*-

import json
import sys,os
import random
import datetime,time
import socket
import Queue

from configobj import ConfigObj

from log_class import *
import mysql_class 

#import sys
#reload(sys)
#sys.setdefaultencoding("utf-8")

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import (
	XidEvent,
	RotateEvent,
	ExecuteLoadQueryEvent,
	BeginLoadQueryEvent,
)
from pymysqlreplication.row_event import (
	DeleteRowsEvent,
	UpdateRowsEvent,
	WriteRowsEvent,
)

CONF_FILE=os.getcwd()+"/pony.ini"
if os.path.exists(CONF_FILE)==False:
        print "Configuration File Does Not Exist !!!"
        sys.exit()

config = ConfigObj(CONF_FILE, encoding="utf8")

MYSQL_SETTINGS = {
	'host': config['BinLogDBServer']['Host'],
	'port': int(config['BinLogDBServer']['Port']),
	'user': config['BinLogDBServer']['User'],
	'passwd': config['BinLogDBServer']['Password'],
	'charset': "utf8"
}

SCHEMA_LIST = config['Rule']['Schema']
TABLE_LIST = config['Rule']['Table']

BINLOG_INFO_INDEX_FILE = config['BaseLog']['BinlogInfoIndexFile'] 
BINLOG_INFO_FILE = config['BaseLog']['BinlogInfoFile']
BINLOG_RUNING_LOG = config['BaseLog']['BinlogRunningLog']

WORKER = int(config['SqlWorker']['Worker'])
WORKER_HOST = config['SqlWorker']['Host']
WORKER_BASE_PORT = int(config['SqlWorker']['BasePort'])
WORKER_LOG = config['SqlWorker']['WorkerLog']

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

def write_file(file_name,message,write_type):
	if (write_type == "Replace"):
		f = open(file_name,'w')
	elif (write_type == "Write"):
		f = open(file_name,'a')
	f.write(message + '\n')
	f.close()

def main():
	logger = Logger(BINLOG_RUNING_LOG, logging.INFO, logging.INFO)
	if not os.path.exists(BINLOG_RUNING_LOG):os.mknod(BINLOG_RUNING_LOG)
	
	try:
		stream = BinLogStreamReader (
			connection_settings = MYSQL_SETTINGS,
			server_id = random.randint(0, 99999),
			resume_stream = START_LOG_POS,
			log_file = START_LOG_FILE,
			log_pos = START_LOG_POS,
			only_schemas = SCHEMA_LIST,
			only_tables = TABLE_LIST,
			blocking = True,
			only_events = [
				DeleteRowsEvent,
				WriteRowsEvent,
				UpdateRowsEvent,
			]
		)
	except Exception, e:
		logger.error('BinlogServer Error: %s'%e)
		stream.close()
		sys.exit(1)

	for binlogevent in stream:
		binlog_file_name = stream.log_file
		binlog_pos = stream.log_pos
		binlog_datetime = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(binlogevent.timestamp))
		through_datetime = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
		prefix = "BinlogDate:%s ThroughDate:%s Schema:%s Table:%s PrimaryKey:%s TableId:%s BinLogName:%s LogPosition:%s"%(
				binlog_datetime,
				through_datetime,
				binlogevent.schema,
				binlogevent.table,
				binlogevent.primary_key,
				binlogevent.table_id,
				binlog_file_name,
				binlog_pos,
		)

		logger.info('BinLogName:%s LogPosition:%s START'%(binlog_file_name, binlog_pos))
		for row in binlogevent.rows:
			if isinstance(binlogevent, DeleteRowsEvent):
				before_vals = row['values']
				vals = row['values']
				before_sql_type = 'Insert'
                                sql_type = 'Delete'
			elif isinstance(binlogevent, UpdateRowsEvent):
				before_vals = row['before_values']
				vals = row['after_values']
				before_sql_type = 'Update'
                                sql_type = 'Update'
			elif isinstance(binlogevent, WriteRowsEvent):
				before_vals = row["values"]
				vals = row["values"]
				before_sql_type = 'Delete'
                                sql_type = 'Insert'
			else:
				pass
			
			sql_package = dict()
			sql_package['binlog_host'] = config['BinLogDBServer']['Host']
			sql_package['binlog_port'] = config['BinLogDBServer']['Port']
			sql_package['binlog_datetime'] = binlog_datetime
			sql_package['through_datetime'] = through_datetime
			sql_package['binlog_file_name'] = binlog_file_name
			sql_package['binlog_pos'] = binlog_pos
			sql_package['binlog_datetime'] = binlog_datetime
			sql_package['schema_name'] = binlogevent.schema
			sql_package['table_name'] = binlogevent.table
			sql_package['table_id'] = binlogevent.table_id
			sql_package['primary_key'] = binlogevent.primary_key
			sql_package['before_vals'] = before_vals
			sql_package['vals'] = vals
			sql_package['before_sql_type'] = before_sql_type
			sql_package['sql_type'] = sql_type

			try:
				port = WORKER_BASE_PORT + hash(binlogevent.table)%WORKER
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				s.connect((WORKER_HOST, port))
				logger.info("Send SQL Package to %s:%d START"%(WORKER_HOST, port))
				s.sendall(str(sql_package))
				returns_data=s.recv(1024)
				logger.info("Send SQL Package to %s:%d SUCCEED (%s)"%(WORKER_HOST, port, returns_data))
				s.close()
			except socket.gaierror, e:
				logger.error("Connecting to SQL-Runner Error: %s %d %s"%(WORKER_HOST, port, e))
				s.close()
				stream.close()
				sys.exit(1)
			try:
				#*保留最近 文件信息
				#Keep the most recent file information
				write_file(BINLOG_INFO_FILE, "%s %s"%(binlog_file_name, binlog_pos), "Replace")
				#增量保存 文件信息
				#Incremental save of file information
				write_file(BINLOG_INFO_INDEX_FILE, "%s %s"%(binlog_file_name, binlog_pos), "Write")
				#增量保存 操作信息
				#Increases the operation information incrementally
				logger.info(prefix)
				logger.info('QueryJsonPackage: %s'%(sql_package))
				logger.info('BinLogName:%s LogPosition:%s SUCCEED'%(binlog_file_name, binlog_pos))
			except Exception, e:
				logger.error("TransForm Error: %s"%e)
				logger.error(prefix)
				logger.error('QueryJsonPackage: %s'%(vals))
				logger.error('BinLogName:%s LogPosition:%s UNSUCCEED'%(binlog_file_name, binlog_pos))
				stream.close()
				sys.exit(1)
			#print ""
	stream.close()
if __name__ == "__main__":
	main()
