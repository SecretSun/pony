# -*- coding:utf-8 -*-
import logging,os

'''
	Log Level
		logging.DEBUG
		logging.INFO
		logging.WARNING
		logging.ERROR
		logging.CRITICAL

        logyyx = Logger('yyx.log',logging.ERROR,logging.DEBUG)
        logyyx.debug('debug message')
        logyyx.info('info message')
        logyyx.war('warning message')
        logyyx.error('error message')
        logyyx.cri('critical message')

'''
class Logger:
	def __init__(self, path, clevel, Flevel):
		#Create logger
		self.logger = logging.getLogger(path)
		self.logger.setLevel(Flevel)

		#Set handler output format
		formatter = logging.Formatter('[%(asctime)s] - [%(levelname)s] - %(message)s', '%Y-%m-%d %H:%M:%S')

		#Set handler output to console
		ch = logging.StreamHandler()
		ch.setFormatter(formatter)
		ch.setLevel(clevel)

		#Set the file log
		fh = logging.FileHandler(path)
		fh.setFormatter(formatter)
		fh.setLevel(Flevel)

		#Add logger handler
		self.logger.addHandler(ch)
		self.logger.addHandler(fh)

	def debug(self,message):
		self.logger.debug(message)

	def info(self,message):
		self.logger.info(message)

	def warn(self,message):
		self.logger.warn(message)

	def error(self,message):
		self.logger.error(message)

	#Critical
	def cri(self,message):
		self.logger.critical(message)

