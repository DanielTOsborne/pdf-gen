import logging
from java.util.logging import Logger, Level

class JULHandler(logging.Handler):
	def __init__(self, logger=None):
		logging.Handler.__init__(self)
		if logger is None:
			logger = Logger.getLogger("")
		self.logger = logger
		
	def createLock(self):
		pass

	def acquire(self):
		pass
	
	def release(self):
		pass
	
	def flush(self):
		pass

	def emit(self, record):
		try:
			level = record.levelno
			if self.logger.isLoggable(Level.SEVERE) and level >= logging.ERROR:
				msg = self.format(record)
				self.logger.severe(msg)
			elif self.logger.isLoggable(Level.WARNING) and level >= logging.WARNING:
				msg = self.format(record)
				self.logger.warning(msg)
			elif self.logger.isLoggable(Level.INFO) and level >= logging.INFO:
				msg = self.format(record)
				self.logger.info(msg)
			elif self.logger.isLoggable(Level.FINE) and level >= logging.DEBUG:
				msg = self.format(record)
				self.logger.fine(msg)
			elif self.logger.isLoggable(Level.FINEST) and level >= logging.NOTSET:
				msg = self.format(record)
				self.logger.finest(msg)
		except (KeyboardInterrupt, SystemExit):
			raise
		except:
			self.handleError(record)