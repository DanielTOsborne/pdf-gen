'''
This module allows Jython scripts to access RADAR data via a DBAPI compatible interface.


Version History

   21.2  26May2022   Original version derived from DBAPI.py

'''


#  Copyright (c) 2022
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import platform
IS_CPYTHON = True if platform.python_implementation() == "CPython" else False

import threading
import json, urllib, csv

if IS_CPYTHON:
	basestring = str
	import urllib as urllib2
else:
	import urllib2

import traceback
from io import StringIO

from hec.db                 import ConnectionFactory, DbConnection, DbIoException
from hec.io                 import TimeSeriesContainer
from hec.heclib.util        import HecTime
from hec.lang               import Const
from java.sql               import Timestamp
from java.text              import SimpleDateFormat
from java.util              import Calendar, Date, TimeZone, Collections
from rma.util               import RMAIO
from wcds.dbi.oracle        import CwmsDaoServiceLookup
from hec.db.cwms            import CwmsTimeSeriesDao, CwmsCatalogDao, CwmsRatingDao

from mil.army.usace.hec.data.timeseries.math import TimeSeriesTemplate
from mil.army.usace.hec.metadata.timeseries import TimeSeriesIdentifierFactory
from mil.army.usace.hec.metadata import OfficeId, DataSetIllegalArgumentException
from mil.army.usace.hec.metadata.resourceservices import ParameterLookup
from java.time import ZoneId, LocalDateTime, Instant, DateTimeException, ZonedDateTime
from java.time.format import DateTimeFormatter
from mil.army.usace.hec.metadata import Units
from hec.data.tx import DataSetTx, DescriptionTx
from hec.hecmath import TimeSeriesMath
from hec.data.cwmsRating import RatingSet, RatingSetFactory
from hec.data.cwmsRating.io import RatingSetContainer
from mil.army.usace.hec.cwms.rating.io.xml import RatingXmlFactory
from hec.data import RatingException
from hec.data.rating import JDomRatingSpecification

from functools import wraps

from usace.metrics.services import MetricsService
from usace.metrics.services.config import MetricsConfigBuilder

from wcds.dbi.client import TokenAuthKeyProvider
from mil.army.usace.hec.cwms.http.client import ApiConnectionInfoBuilder, HttpRequestBuilderImpl

import DBAPI
import logging

logger = logging.getLogger(__name__)

if not logging.root.handlers and not logger.handlers:
	logging.basicConfig()

logger.info("RADARAPI module loaded")

######### Logging Information ##########
# Users of this class should configure the python logging module so that it logs
# to their desired destination.  See the logging module documentation at:
# https://docs.python.org/2.7/library/logging.html
# Or other online resources such as: https://realpython.com/python-logging/
# A client application might include the following to write errors to a
# rotating log file and write all messages to stdout:
# 			ERROR_LOG_FILENAME = ".RADARAPI-errors.log"
# 			LOGGING_CONFIG = {
# 				"version": 1,
# 				"disable_existing_loggers": False,
# 				"formatters": {
# 					"default": {  # The formatter name, can be anything
# 						"format": "%(asctime)s:%(name)s:%(lineno)d " "%(levelname)s %(message)s",
# 						"datefmt": "%Y-%m-%d %H:%M:%S",  # How to display dates
# 					},
# 					"simple": {  # The formatter name
# 						"format": "%(message)s",  # As simple as possible!
# 					}
# 				},
# 				"handlers": {
# 					"logfile": {  # The handler name
# 						"formatter": "default",  # Refer to the formatter defined above
# 						"level": "ERROR",  # FILTER: Only ERROR and CRITICAL logs
# 						"class": "logging.handlers.RotatingFileHandler",  # OUTPUT: Which class to use
# 						"filename": ERROR_LOG_FILENAME,
# 						"backupCount": 2,
# 					},
# 					"verbose_output": {  # The handler name
# 						"formatter": "default",  # Refer to the formatter defined above
# 						"level": "DEBUG",  # FILTER: All logs
# 						"class": "logging.StreamHandler",  # OUTPUT: Which class to use
# 						"stream": "ext://sys.stdout",  # Param for class above. It means stream to console
# 					},
# 				},
# 				"loggers": {
# 					"RADARAPI": {  #Name should match the module
# 						"level": "INFO",  # IF this is set at INFO it means INFO and above (so not DEBUG)
# 						"handlers": [
# 							"verbose_output",  # The handler defined above
# 						],
# 					},
# 				},
# 				"root": {  # All loggers (including RADARAPI)
# 					"level": "INFO",  # FILTER: only INFO logs onwards
# 					"handlers": [
# 						"logfile",  # Refer the handler defined above
# 					]
# 				}
# 			}
# 			logging.config.dictConfig(LOGGING_CONFIG)
#
# Another way that RADARAPI users can configure logging is to use the provided JULHandler class.
# This class redirects python log messages into the standard java.util.logging framework.
# For example, a client could use the following:
# 			import JULHandler
# 			logging.getLogger().addHandler(JULHandler.JULHandler())
# The user should then also configure the java.util.logging package in the standard ways.  One
# popular method to configure java.util.logging is to create a properties file and then reference
# the file from the java command line using the -Djava.util.logging.config.file=<path to file>
# command line argument.
# For example:
#   "C:\Program Files\Java\jdk1.8.0_202\bin\java.exe" -classpath "J:\Workspaces\git\hec-cwms-data
#   -access\jython-db-api\target\jar\metrics-codahale-2.0.jar;J:\Workspaces\git\hec-cwms-data-acc
#   ess\jython-db-api\target\jar\*;J:\Workspaces\git\hec-cwms-data-access\jython-db-api\src\main\
#   resources;J:\Workspaces\git\hec-cwms-data-access\jython-db-api\src\test\scripts" -Dmetrics-en
#   abled=true -Dcwms.dbi.ConnectUsingUrl="http://localhost:7000/swt-data/" -DCWMS_HOME="J:\Works
#   paces\git\hec-cwms-data-access\jython-db-api\src\test" -Djava.util.logging.config.file=loggin
#   g.properties  org.python.util.jython J:\Workspaces\git\hec-cwms-data-access\jython-db-api\src
#   \test\scripts\test_RADAR.py
#
######### Logging Information ##########

'''
Values that specify the database connection method.  These don't all make sense for RADARAPI but
are included for compatibility with DBAPI.
'''
CREDENTIALS_SPECIFIED_1  = "CREDENTIALS_SPECIFIED_1" # All credentials specified (url, user, password, office)
CREDENTIALS_SPECIFIED_2  = "CREDENTIALS_SPECIFIED_2" # All credentials except url specified
NETWORKED_CLIENT         = "NETWORKED_CLIENT"        # No credentials specified, existing connection used
CWMS_PRODUCTION_ACCOUNT  = "CWMS_PRODUCTION_ACCOUNT" # No credentials specified, taken from environment, production dbi.properties, and production dbi.conf
CWMS_SERVICE_ACCOUNT     = "CWMS_SERVICE_ACCOUNT"    # No credentials specified, taken from environment, production dbi.properties, and local dbi.conf
CWMS_VUE_1               = "CWMS_VUE_1"              # No credentials specified, existing connection used
CWMS_VUE_2               = "CWMS_VUE_2"              # No credentials specified, existing connection used
SERVER_SUITE_WO_DIALOG   = "SERVER_SUITE_WO_DIALOG"  # No credentials specified, existing connection used
SERVER_SUITE_W_DIALOG    = "SERVER_SUITE_W_DIALOG"   # No credentials specified, login dialog used
APP_NAME                 = "DBAPI"                   # Application name

'''
Version Information
'''
VERSION_NUMBER = "21.2"
VERSION_DATE   = "26May2022"


offices = {
#	EROC :  OFC
	"B0" : "MVD",
	"B1" : "MVM",
	"B2" : "MVN",
	"B3" : "MVS",
	"B4" : "MVK",
	"B5" : "MVR",
	"B6" : "MVP",
	"E0" : "NAD",
	"E1" : "NAB",
	"E3" : "NAN",
	"E4" : "NAO",
	"E5" : "NAP",
	"E6" : "NAE",
	"G0" : "NWDP",
	"G2" : "NWP",
	"G3" : "NWS",
	"G4" : "NWW",
	"G5" : "NWK",
	"G6" : "NWO",
	"G7" : "NWDM",
	"H0" : "LRD",
	"H1" : "LRH",
	"H2" : "LRL",
	"H3" : "LRN",
	"H4" : "LRP",
	"H5" : "LRB",
	"H6" : "LRC",
	"H7" : "LRE",
	"H8" : "LRDG",
	"J0" : "POD",
	"J3" : "POH",
	"J4" : "POA",
	"K0" : "SAD",
	"K2" : "SAC",
	"K3" : "SAJ",
	"K5" : "SAM",
	"K6" : "SAS",
	"K7" : "SAW",
	"L0" : "SPD",
	"L1" : "SPL",
	"L2" : "SPK",
	"L3" : "SPN",
	"L4" : "SPA",
	"M0" : "SWD",
	"M2" : "SWF",
	"M3" : "SWG",
	"M4" : "SWL",
	"M5" : "SWT",
	"Q0" : "HEC",
	"S0" : "CPC",
}

metrics_config = MetricsConfigBuilder().withSystemPropertyOptions().build()
metrics_service = MetricsService(metrics_config)

def _getMetricsService():
	return metrics_service
	
def _createMetrics(parameters):
	strings = ["CWMS/Metrics","RADARAPI"]
	for param in parameters:
		strings.append(str(param))
	
	return _getMetricsService().createMetrics(strings)
	

def _timed(func):
	@wraps(func)
	def wrapper(*args, **kwargs):
		timer = _createMetrics([func.__name__]).createTimer()
		
		ctx = timer.start()
		try:
			result = func(*args, **kwargs)
			return result
		finally:
			ctx.close()
	return wrapper

##############################################################################
@_timed
def isUndefined(value) :
	'''
	Determine if a value is undefined
	'''
	return DBAPI.isUndefined(value)

@_timed
def nonVersionedDate() :
	'''
	Return a date string for non-versioned dates
	'''
	return DBAPI.nonVersionedDate()
	
@_timed
def isNonVersioned(dateStr) :
	'''
	Determine if date string is the non-versioned date string
	'''
	return DBAPI.isNonVersioned(dateStr)

@_timed
def isTsId(id) :
	'''
	Determine if an identifier looks like a CWMS Times Sries Identifier
	loc-subLoc.param-subParam.paramType.intvl.dur.ver
	'''
	return DBAPI.isTsId(id)

@_timed
def isRatingId(id) :
	'''
	Determine if an identifier looks like a CWMS Rating Identifier
	loc.indParams;depParam.templateVer.specVer
	'''
	return DBAPI.isRatingId(id)

@_timed
def getCwmsHome() :
	'''
	Returns the CWMS_HOME value from the System properites or environment
	'''
	return DBAPI.getCwmsHome()

@_timed
def getCwmsDbiProperty(key) :
	'''
	Returns a property in the $CWMS_HOME/config/properties/dbi.properties file
	'''
	return DBAPI.getCwmsDbiProperty(key)

@_timed
def getPassAndUrl(passwordFilename, userName, dbUrl) :
	'''
	Returns the password and case-corrected database URL from a specified password file for a specified user and database URL.
	The password is passed back in the first position of a 2-string array; the URL is passed back in
	the second position.  If the specified file name is not a valid password file, or if no match
	is found in the password file, the password and URL are both returned as null.
	'''
	return DBAPI.getPassAndUrl(passwordFilename, userName, dbUrl)

@_timed
def getDbiDbUserAndPass() :
	'''
	Returns the DBI user and password for the CWMS database URL
	'''
	return DBAPI.getDbiDbUserAndPass()

@_timed
def getApiConnectionInfo(dbUrl, token=None):
	builder = ApiConnectionInfoBuilder(dbUrl)
	if token is not None:
		authProvider = TokenAuthKeyProvider(token)
		builder = builder.withAuthorizationKeyProvider(authProvider)
		
	return builder.build()

# This getConnectionFactory is not in the RadarAccess class its more of a utility builder function
@_timed
def getConnectionFactory(dbUrl, dbOfc, apiConnectionInfo=None) :
	'''
	Returns a connection factory for the specified database, and office.
	'''

	dbConnection = DbConnection(DbConnection.DB_RADAR)
	dbConnection.setDatabaseUrl(dbUrl.lower())
	
	dbConnection.setSessionOfficeId(dbOfc)
	dbConnection.setShowSecurityLogin(False)
	
	factory = ConnectionFactory.newInstance(dbConnection, apiConnectionInfo)
	
	return factory

@_timed
def getTimeWindow(*args) :
	tw = DBAPI.getTimeWindow(*args)
	return tw

@_timed
def removeConnectionFactoryInstance() :
	'''
	Removes the current connection factory instance so it won't automatically
	be used by the next zero-argument call to DBAPI.open()
	'''
	try    :
		ConnectionFactory.removeInstance(ConnectionFactory.getInstances().iterator().next().getDbConnection())
	except : pass

##############################################################################

class OracleTypes :
	'''
	Functional equivalent of oracle.jdbc.OracleTypes
	'''
	ARRAY             =  2003
	BFILE             =   -13
	BIGINT            =    -5
	BINARY            =    -2
	BINARY_DOUBLE     =   101
	BINARY_FLOAT      =   100
	BIT               =    -7
	BLOB              =  2004
	BOOLEAN           =    16
	CHAR              =     1
	CLOB              =  2005
	CURSOR            =   -10
	DATALINK          =    70
	DATE              =    91
	DECIMAL           =     3
	DOUBLE            =     8
	FIXED_CHAR        =   999
	FLOAT             =     6
	INTEGER           =     4
	INTERVALDS        =  -104
	INTERVALYM        =  -103
	JAVA_OBJECT       =  2000
	JAVA_STRUCT       =  2008
	LONGVARBINARY     =    -4
	LONGVARCHAR       =    -1
	NULL              =     0
	NUMBER            =     2
	NUMERIC           =     2
	OPAQUE            =  2007
	OTHER             =  1111
	PLSQL_INDEX_TABLE =   -14
	RAW               =    -2
	REAL              =     7
	REF               =  2006
	ROWID             =    -8
	SMALLINT          =     5
	STRUCT            =  2002
	TIME              =    92
	TIMESTAMP         =    93
	TIMESTAMPLTZ      =  -102
	TIMESTAMPNS       =  -100
	TIMESTAMPTZ       =  -101
	TINYINT           =    -6
	TRACE             = False
	VARBINARY         =    -3
	VARCHAR           =    12


##############################################################################


class RadarAccess :
	'''
	Class that parallels the DbAccess from DBAPI
	'''
	@_timed
	def __init__(self, *args, **kwargs) :
		'''
		Construct a RadarAccess object.
		'''
		self._clientLock = threading.RLock()
		self._clientLock.acquire()
		try :
			self._initValues_()
			#----------------------------------#
			# process the positional arguments #
			#----------------------------------#
			argsLen = len(args)
			if argsLen > 0 :
				self._url = args[0]
				if argsLen > 1 :
					self._officeId     = args[1]
				if argsLen > 2 :
					self._startTimeStr = args[2]
				if argsLen > 3 :
					self._endTimeStr   = args[3]
				if argsLen > 4 :
					raise ValueError("Invalid parameters to RadarAccess constructor")
			#-------------------------------#
			# process the keyword arguments #
			#-------------------------------#
			for key in kwargs.keys() :
				if key == "url" :
					if self._url :
						raise ValueError("URL already specified in positional parameters")
					self._url = kwargs[key]
				elif key == "office" :
					if self._officeId :
						raise ValueError("Office already specified in positional parameters")
					self._officeId = kwargs[key]
				elif key == "startTime" :
					if self._startTimeStr :
						raise ValueError("Start time already specified in positional parameters")
					self._startTimeStr = kwargs[key]
				elif key == "endTime" :
					if self._endTimeStr :
						raise ValueError("End time already specified in positional parameters")
					self._endTimeStr = kwargs[key]
				else :
					raise ValueError('Invalid keyword parameter "%s"' % key)
			#---------------------------------#
			# validate parameter combinations #
			#---------------------------------#
			if (self._startTimeStr is None) != (self._endTimeStr is None) :
				raise ValueError("Start time and end time must be specified together.")
			#----------------------------------#
			# set the time window if specified #
			#----------------------------------#
			if self._startTimeStr :
				self.setTimeWindow(self._startTimeStr, self._endTimeStr)
			#----------------------------------#
			# finally, connect to the database #
			#----------------------------------#
			if self._url :
				self._connectionInfo = getApiConnectionInfo(self._url)
				self._factory = getConnectionFactory(self._url, self._officeId, self._connectionInfo)
				self._connectionMethod = CREDENTIALS_SPECIFIED_1
			else :
				self._url = getCwmsDbiProperty("cwms.dbi.ConnectUsingUrl")
				self._connectionInfo = getApiConnectionInfo(self._url)
				self._factory = getConnectionFactory(self._url, self._officeId, self._connectionInfo)
				self._connectionMethod = CREDENTIALS_SPECIFIED_2
				
			self._isOpen = self._factory is not None
			if not self._isOpen:
				raise Exception("Could not connect to database")
		finally :
			self._clientLock.release()

	@_timed
	def _initValues_(self):
		'''
		Initialize the member variable values.
		'''
		self._isOpen = False
		self._url = None
		self._factory = None
		self._connectionMethod = None
		self._connectionInfo = None
		self._startTimeStr = None
		self._startTime = None
		self._endTimeStr = None
		self._endTime = None
		self._officeId = None
		self._unitSystem = "English"
		self._storeRule = Const.Replace_All
		self._utcTimeZone = TimeZone.getTimeZone("UTC")
		self._timeZone = self._utcTimeZone
		self._trim = True
		self._startTimeInclusive = True
		self._endTimeInclusive = True
		self._getPrevious = False
		self._getNext = False
		self._overrideProtection = False
		self._maxVersion = True
		self._parameterUnits = None
		self._ratingLoadMethod = "LAZY"
		self._utcCal = Calendar.getInstance()
		self._utcCal.setTimeZone(self._utcTimeZone)
		self._sdf = SimpleDateFormat('yyyy/MM/dd HHmm')
		self._sdfDATE = SimpleDateFormat('yyyy-MM-dd HH:mm:ss')
		self._hts = HecTime(HecTime.SECOND_INCREMENT)
		self._sdf.setTimeZone(self._utcTimeZone)
		
		self.resetVersionDate()
	
	# Sets the DataAccessFactory to use.
	@_timed
	def setConnectionFactory(self, factory):
		self._factory = factory
	
	@_timed
	def setApiConnectionInfo(self, connectionInfo):
		self._connectionInfo = connectionInfo

	@_timed
	def lock(self):
		'''
		Lock the client lock
		'''
		self._clientLock.acquire()

	@_timed
	def unlock(self):
		'''
		Unlock the client lock
		'''
		self._clientLock.release()
	
	@_timed
	def isOpen(self):
		'''
		Return whether we can use the dbapi object
		'''
		self.lock()
		try:
			return self._isOpen
		finally:
			self.unlock()
	
	@_timed
	def getConnection(self) :
		'''
		Unsupported method that would get a database connection for external use.
		'''
		raise Exception("RADARAPI does not support getConnection")
	
	@_timed
	def getFileName(self) :
		'''
		Get the default start time string
		'''
		self.lock()
		try     : return self._url
		finally : self.unlock()
	
	@_timed
	def setTrimMissing(self, state) :
		'''
		Set the default time series trim state
		'''
		self.lock()
		try     : self._trim = bool(state)
		finally : self.unlock()
	
	@_timed
	def getTrimMissing(self) :
		'''
		Get the default time series trim state
		'''
		self.lock()
		try     : return self._trim
		finally : self.unlock()
	
	@_timed
	def getStartTime(self) :
		'''
		Get the default start time string
		'''
		self.lock()
		try     : return self._startTimeStr
		finally : self.unlock()
	
	@_timed
	def getEndTime(self) :
		'''
		Get the default start time string
		'''
		self.lock()
		try     : return self._endTimeStr
		finally : self.unlock()

	@_timed
	def setTimeZone(self, tz) :
		'''
		Set the default time zone
		'''
		self.lock()
		try :
			if isinstance(tz, TimeZone) :
				self._timeZone = tz
			elif isinstance(tz, basestring) :
				if   tz in ("EST", "CDT") : tz = "Etc/GMT+5"
				elif tz in ("CST", "MDT") : tz = "Etc/GMT+6"
				elif tz in ("MST", "PDT") : tz = "Etc/GMT+7"
				elif tz == "PST"          : tz = "Etc/GMT+8"
				self._timeZone = TimeZone.getTimeZone(tz)
			else :
				raise TypeError("Parameter tz must be a string or a TimeZone object")
		finally :
			self.unlock()
	
	@_timed
	def getTimeZone(self) :
		'''
		Get the default time zone
		'''
		self.lock()
		try     : return self._timeZone
		finally : self.unlock()
	
	@_timed
	def getTimeZoneName(self) :
		'''
		Get the default time zone
		'''
		self.lock()
		try     : return self._timeZone.getID()
		finally : self.unlock()
	
	@_timed
	def setStartTimeInclusive(self, state) :
		'''
		Set the default start time inclusive state
		'''
		self.lock()
		try     : self._startTimeInclusive = bool(state)
		finally : self.unlock()
	
	@_timed
	def getStartTimeInclusive(self) :
		'''
		Get the default start time inclusive state
		'''
		self.lock()
		try     : return self._startTimeInclusive
		finally : self.unlock()
	
	@_timed
	def setEndTimeInclusive(self, state) :
		'''
		Set the default end time inclusive state
		'''
		self.lock()
		try     : self._endTimeInclusive = bool(state)
		finally : self.unlock()
	
	@_timed
	def getEndTimeInclusive(self) :
		'''
		Get the default end time inclusive state
		'''
		self.lock()
		try     : return self._endTimeInclusive
		finally : self.unlock()
	
	@_timed
	def setRetrievePrevious(self, state) :
		'''
		Set the default time series get previous state
		'''
		self.lock()
		try     : self._getPrevious = bool(state)
		finally : self.unlock()
	
	@_timed
	def getRetrievePrevious(self, state) :
		'''
		Get the default time series get previous state
		'''
		self.lock()
		try     : return self._getPrevious
		finally : self.unlock()
	
	@_timed
	def setRetrieveNext(self, state) :
		'''
		Set the default time series get next state
		'''
		self.lock()
		try     : self._getNext = bool(state)
		finally : self.unlock()
	
	@_timed
	def getRetrieveNext(self) :
		'''
		Get the default time series get next state
		'''
		self.lock()
		try     : return self._getNext
		finally : self.unlock()
	
	@_timed
	def setRatingLoadMethod(self, loadMethodStr) :
		'''
		Sets the default rating load method.  Ignored in RADARAPI
		'''
		if loadMethodStr.upper() in ("EAGER", "LAZY", "REFERENCE") :
			self._ratingLoadMethod = loadMethodStr.upper()
		else :
			raise ValueError('Invalid rating load method: "%s"' % loadMethodStr)
	
	@_timed
	def getRatingLoadMethod(self) :
		'''
		Gets the default rating load method. Ignored in RADARAPI
		'''
		return self._ratingLoadMethod

	@_timed
	def setOfficeId(self, officeId):
		'''
		Set the default office ID
		'''
		self.lock()
		try:
			_officeId = officeId.upper()
		finally:
			self.unlock()

	@_timed
	def getOfficeId(self):
		'''
		Get the default office ID
		'''
		self.lock()
		try:
			return self._officeId
		finally:
			self.unlock()
	
	@_timed
	def setDefaultVerticalDatum(self, DefaultVertDatum) :
		'''
		Set the default vertical datum for elevations
		'''
		raise Exception("setDefaultVerticalDatum not implemented in RADARAPI")
	
	@_timed
	def getDefaultVerticalDatum(self) :
		'''
		Retrieve the default vertical datum for elevations
		'''
		raise Exception("getDefaultVerticalDatum not implemented in RADARAPI")
	
	@_timed
	def getLocationVerticalDatum(self, locationId, officeId=None) :
		'''
		Retrieve the identified vertical datum for a location
		'''
		raise Exception("getLocationVerticalDatum not implemented in RADARAPI")
	
	@_timed
	def setVerticalDatumOffset(self, locationId, verticalDatumId1, verticalDatumId2, value, unit) :
		'''
		Set the offset from vertical datum id 1 to vertical datum id 2 at a location
		'''
		raise Exception("setVerticalDatumOffset not implemented in RADARAPI")
	
	@_timed
	def getVerticalDatumOffset1(self, locationId, verticalDatumId1, verticalDatumId2, unit, officeId=None) :
		'''
		Retrieve the offset from vertical datum id 1 to vertical datum id 2 at a location
		'''
		raise Exception("getVerticalDatumOffset1 not implemented in RADARAPI")
	
	@_timed
	def getVerticalDatumOffset2(self, locationId, unit=None, officeId=None) :
		'''
		Retrieve the offset from the location's identified vertical datum to the default
		vertical datum or datum specified in the unit spec (if any)
		'''
		raise Exception("getVerticalDatumOffset2 not implemented in RADARAPI")
	
	@_timed
	def getVerticalDatumOffset(self, *args, **kwargs) :
		'''
		Forwarder to getVerticalDatumOffset1 or getVerticalDatumOffset2
		'''
		argCount = len(args)
		kwargCount = len(kwargs)
		totArgCount = argCount + kwargCount
		if totArgCount not in (2, 4) :
			raise Exception('getVerticalDatumOffset expects 2 or 4 arguments, received %d' % totArgCount)
		locationId       = None
		unit             = None
		verticalDatumId1 = None
		verticalDatumId2 = None
		officeId         = None
		#---------------------------#
		# parse the positional args #
		#---------------------------#
		if   argCount == 5 :
			locationId, verticalDatumId1, verticalDatumId2, unit, officeId = args
		elif argCount == 4 :
			locationId, verticalDatumId1, verticalDatumId2, unit = args
		elif argCount == 3 :
			locationId, unit, officeId = args
		elif argCount == 2 :
			locationId, unit = args
		elif argCount == 1 :
			locationId, = args
		#------------------------#
		# parse the keyword args #
		#------------------------#
		try :
			for argName in kwargs.keys() :
				if   argName == 'locationId' :
					assert locationId is None
					locationId = kwargs[argName]
				elif argName == 'unitSpec' :
					assert unitSpec is None
					unitSpec = kwargs[argName]
				elif argName == 'verticalDatumId1' :
					assert verticalDatumId1 is None
					verticalDatumId1 = kwargs[argName]
				elif argName == 'verticalDatumId2' :
					assert verticalDatumId2 is None
					verticalDatumId2 = kwargs[argName]
				elif argName == 'unitId' :
					assert unitId is None
					unitId = kwargs[argName]
				elif argName == 'officeId' :
					assert officeId is None
					unitId = kwargs[argName]
				else :
					raise Exception('getVerticalDatumOffset: invalid argument name: %s' % argName)
		except AssertionError :
			raise Exception('getVerticalDatumOffset: argument %s is multiply defined' % argName)
		
		if locationId and verticalDatumId1 and verticalDatumId2 and unit :
			return self.getVerticalDatumOffset1(locationId, verticalDatumId1, verticalDatumId2, unit, officeId)
		if locationId and unit :
			return self.getVerticalDatumOffset2(locationId, unit, officeId)
		raise Exception('getVerticalDatumOffset: invalid parameters')
	
	@_timed
	def setDefaultVerticalDatum(self, DefaultVertDatum) :
		'''
		Set the default vertical datum for elevations
		'''
		raise Exception("Cannot set default vertical datum")
	
	@_timed
	def canWrite(self) :
		self.lock()
		try:
			if self.isOpen() and self._factory is not None:
				# Might have to set "hec.cwms.data.access.radar.writeable" to true for RadarDataAccessFactory
				writable = self._factory.isWritable()
				return writable
			
			return False
		finally:
			self.unlock()
	
	@_timed
	def setRetrieveMaxVersionDate(self, state) :
		'''
		Set the default time series get max version state
		'''
		self.lock()
		try     : self._maxVersion = bool(state)
		finally : self.unlock()
	
	@_timed
	def getRetrieveMaxVersionDate(self, state) :
		'''
		Get the default time series get max version state
		'''
		self.lock()
		try     : return self._maxVersion
		finally : self.unlock()

	@_timed
	def setVersionDate(self, dateStr=None):
		'''
		Set the default time series version date
		'''
		self.lock()
		try:
			if dateStr and not isNonVersioned(dateStr):
				self._hts.set(dateStr)
				self._sdfDATE.setTimeZone(self._utcTimeZone)
				self._versionDate = self._sdfDATE.format(self._hts.getTimeInMillis())
			else:
				self._versionDate = dateStr
		finally:
			self.unlock()

	@_timed
	def resetVersionDate(self):
		'''
		Resets the default time series version date to "non-versioned"
		'''
		self.setVersionDate('')

	@_timed
	def unsetVersionDate(self):
		'''
		Resets the default time series version date to "non-versioned"
		'''
		self.resetVersionDate()

	@_timed
	def getVersionDate(self):
		'''
		Get the default time series version date
		'''
		self.lock()
		try:
			return self._versionDate
		finally:
			self.unlock()

	@_timed
	def setTimeWindow(self, *args):
		'''
		Set the default time window for future database accesses.
		'''
		try:
			self._startTime, self._endTime = getTimeWindow(*args)
		except Exception as e:
			logger.exception("Error setting the time window from args:%s" % args)
			self._startTimeStr = None
			self._endTimeStr = None
		else:
			self._startTime.showTimeAsBeginningOfDay(True)
			self._startTimeStr = self._startTime.dateAndTime(4)
			self._endTimeStr = self._endTime.dateAndTime(4)

	@_timed
	def getTimeWindow(self):
		'''
		Get the default time window for future database accesses.
		'''
		self.lock()
		try:
			return self._startTimeStr, self._endTimeStr
		finally:
			self.unlock()
	
	@_timed
	def setOverrideProtection(self, state):
		'''
		Set the default override protection version state
		'''
		self.lock()
		try:
			self._overrideProtection = bool(state)
		finally:
			self.unlock()
	
	@_timed
	def getOverrideProtection(self) :
		'''
		Get the default override protection version state
		'''
		self.lock()
		try     : return self._overrideProtection
		finally : self.unlock()

	@_timed
	def setUnitSystem(self, system):
		'''
		Set the working units system to SI or English, used for reads only.
		'''
		self.lock()
		try:
			if system.lower() == "english":
				self._unitSystem = "English"
			elif system.lower() == "si":
				self._unitSystem = "SI"
			else:
				raise ValueError("System must be English or SI.")
		finally:
			self.unlock()

	@_timed
	def getUnitSystem(self):
		'''
		Get the working units system to SI or English, used for reads only.
		'''
		self.lock()
		try:
			return self._unitSystem
		finally:
			self.unlock()
	
	@_timed
	def setStoreRule(self, storeRule) :
		'''
		Sets the default store rule, used for writes only.
		'''
		self.lock()
		try     : self._storeRule = storeRule
		finally : self.unlock()
	
	@_timed
	def getStoreRule(self) :
		'''
		Gets the default store rule, used for writes only.
		'''
		self.lock()
		try     : return self._storeRule
		finally : self.unlock()

	@_timed
	def commit(self) :
		'''
		This is now provided only to not break old code. Any method than stores or deletes
		database data peforms its own commit() before returning
		'''
		pass

	@_timed
	def close(self):
		'''
		Release any resources and make this object unusable.
		'''
		self.lock()
		try:
			remove_connection = True
		
			if remove_connection and self._connectionInfo is not None:
				logger.info("Closing %s" % self._connectionInfo)
				removeConnectionFactoryInstance()
				self._connectionInfo = None
				self._factory = None
		
			self._initValues_()
		finally:
			self.unlock()

	@_timed
	def delete(self, ids) :
		if not self.canWrite() :
			raise Exception("Cannot write to database %s" % conn.getMetaData().getURL())
		
		if type(ids) not in (type([]), type(())) :
			self.delete([ids])
		else :
			tsIds = []
			ratingIds = []
			
			for id in ids :
				if isTsId(id) :
					tsIds.append(id)
				elif isRatingId(id) :
					ratingIds.append(id)
			
			if len(tsIds) > 0 :
				for ts_id in tsIds :
					self.deleteTimeSeries(ts_id)
			
			if len(ratingIds) > 0 :
				self.deleteRatings(ratingIds)
		

	@_timed
	def deleteRatings(self, ratingIds) :
		'''
		delete ratings (ratingId and data) from the database
		'''
		
		if not self.canWrite() :
			raise Exception("Cannot write to database %s" % conn.getMetaData().getURL())
			
		if isinstance(ratingIds, basestring) :
			ratingIds = [ratingIds]
			
		self.lock()
		try :
			db_conn = self._factory.getDbConnection()
			rating_dao = CwmsDaoServiceLookup.getDao(CwmsRatingDao, db_conn)
		
			dak = self._factory.getDataAccessKey("deleteRatings")
			
			office = self.getOfficeId()
			
			# DBAPI does this:
			#     stmt = conn.prepareCall('begin cwms_rating.delete_specs(:1, cwms_util.delete_all, :2); end;')
			
			# Have to decide which of these we're calling:
			#  void deleteRatingCurve(DataAccessKey dataAccessKey, IRatingSpecification iRatingSpecification,
			# 						   Date effectiveDateStart, Date effectiveDateEnd) throws DbIoException;
			#
			# 	void deleteRatingSpecification(DataAccessKey dataAccessKey, IRatingSpecification iRatingSpecification, String deleteAction)throws DbIoException;
			#
			# 	void deleteRatingTemplate(DataAccessKey dataAccessKey, IRatingTemplate iRatingTemplate, String deleteAction) throws DbIoException;
			
			# the deleteActions are 'DELETE ALL', 'DELETE KEY', or 'DELETE DATA'
			
			for rating_id in ratingIds:
				i_rating_specification = JDomRatingSpecification(office, rating_id)
				
				try:
					rating_dao.deleteRatingSpecification(dak,  i_rating_specification, 'DELETE ALL')
					
				except DbIoException as e:
					raise Exception("Failed to delete rating specification", e)
		finally :
			self.unlock()
		
	@_timed
	def deleteTimeSeries(self, tsId) :
		'''
		delete time series (id and data) from the database
		'''
		db_conn = self._factory.getDbConnection()
		ts_dao = CwmsDaoServiceLookup.getDao(CwmsTimeSeriesDao, db_conn)
		
		dak = self._factory.getDataAccessKey("deleteTimeSeries")
		locationIdTz = None
		officeId = self.getOfficeId()
		
		if IS_CPYTHON:
			time_series_identifier = TimeSeriesIdentifierFactory.from_(OfficeId(officeId), tsId, locationIdTz)
		else:
			time_series_identifier = getattr(TimeSeriesIdentifierFactory, "from")(OfficeId(officeId), tsId, locationIdTz)
		
		try:
			ts_dao.deleteTimeSeriesData(dak,  time_series_identifier)
			ts_dao.deleteTimeSeriesIdentifier(dak,  time_series_identifier)
		except DbIoException as e:
			raise Exception("Failed to delete timeseries", e)
	
	@_timed
	def refreshTsCatalog(self) :
		'''
		No-op retained for backward compatibility. Not needed in current schema
		'''
		pass

	@_timed
	def done(self) :
		'''
		Release any resources and make this object unusable.
		'''
		self.close()

	def __del__(self) :
		'''
		Destructor (only called when last reference disappears)
		'''
		try:
			self.close()
		except:
			# This can be called after the JVM shuts down under JPype
			pass

	@_timed
	def getTimeSeriesContainer(
			self,
			tsId,
			startTimeStr = None,
			endTimeStr = None,
			units = None,
			timeZone = None,
			trim = None,
			startInclusive = None,
			endInclusive = None,
			getPrevious = None,
			getNext = None,
			versionDate = None,
			maxVersion = None,
			officeId = None) :
		'''
		Read a time-series from the database and return it in a TimeSeriesContainer object.
		'''
		
		#----------------------------#
		# get data from the database #
		#----------------------------#
		tsc = None
		self.lock()
		try:
			#----------------------#
			# handle the arguments #
			#----------------------#
			if (not startTimeStr) != (not endTimeStr):
				raise ValueError("Start time and end time must be specified together")
			if not startTimeStr:
				startTimeStr = self._startTimeStr
			if not endTimeStr:
				endTimeStr = self._endTimeStr
			if not startTimeStr or not endTimeStr:
				raise ValueError("No default or explicit time window")
			if not units:
				unitSystem = self._unitSystem[:2].upper()
				tsParts = tsId.split(".")
				parameter = None
				if tsParts and len(tsParts) > 1:
					parameter = tsParts[1]
					
				if parameter:
					parameter_units = self._getParameterUnits_()
					if parameter_units and unitSystem in parameter_units:
						parameters_in_units = parameter_units[unitSystem]
						if parameter in parameters_in_units:
							units = parameters_in_units[parameter]
						else:
							logger.warning("Parameter %s not found in units %s." % (parameter, unitSystem))
					else:
						logger.warning("Parameter units not found for unit system %s." % unitSystem)
			if timeZone is not None:
				timeZone = TimeZone.getTimeZone(timeZone)
			else:
				timeZone = self._timeZone
			if trim is not None:
				trim = bool(trim)
			else:
				trim = self._trim
			if startInclusive is not None:
				startTimeInclusive = bool(startInclusive)
			else:
				startTimeInclusive = self._startTimeInclusive
			if endInclusive is not None:
				endTimeInclusive = bool(endInclusive)
			else:
				endTimeInclusive = self._endTimeInclusive
			if getPrevious is not None:
				getPrevious = bool(getPrevious)
			else:
				getPrevious = self._getPrevious
			if getNext is not None:
				getNext = bool(getNext)
			else:
				getNext = self._getNext
			if versionDate is None:
				versionTimeStr = self._versionDate
			else:
				versionTimeStr = versionDate
			if maxVersion is not None:
				maxVersion = bool(maxVersion)
			else:
				maxVersion = self._maxVersion
			if officeId is not None:
				officeId = officeId.upper()
			else:
				officeId = self.getOfficeId()
			startTimeStr = startTimeStr.replace(":", "").replace(",", "")
			endTimeStr = endTimeStr.replace(":", "").replace(",", "")
			
			
			locationIdTz = timeZone.toZoneId()
			if IS_CPYTHON:
				ts_id = TimeSeriesIdentifierFactory.from_(OfficeId(officeId), tsId, locationIdTz)
			else:
				ts_id = getattr(TimeSeriesIdentifierFactory, "from")(OfficeId(officeId), tsId, locationIdTz)
			startTime = Date(RMAIO.parseDate(startTimeStr, self._utcTimeZone)).getTime()
			endTime = Date(RMAIO.parseDate(endTimeStr, self._utcTimeZone)).getTime()
			
			ts_template = TimeSeriesTemplate(ts_id, startTime, endTime, Units(units))
			db_conn = self._factory.getDbConnection()
			ts_dao = CwmsDaoServiceLookup.getDao(CwmsTimeSeriesDao, db_conn)
			
			dak = self._factory.getDataAccessKey("getTimeSeriesContainer")
			try:
				ts_templates = Collections.singletonList(ts_template)
				ht = HecTime(HecTime.SECOND_INCREMENT)
				ht.set(versionTimeStr)
				if IS_CPYTHON:
					versionTimestamp = Timestamp.from_(ht.getInstant(ZoneId.of("UTC")))
				else:
					versionTimestamp = getattr(Timestamp, "from")(ht.getInstant(ZoneId.of("UTC")))
				
				time_series = None
				# time_series_map is like: Map<TimeSeriesTemplate, TimeSeries>
				logger.debug("Calling CwmsTimeSeriesDao.retrieveTimeSeries with: dak: {}, "
				             "ts_templates: {}, startTimeInclusive: {}, endTimeInclusive: {}, "
				             "getPrevious: {}, getNext: {}, versionTimestamp: {}, "
				             "maxVersion: {}, trim: {}"
				             .format(dak, ts_templates, startTimeInclusive, endTimeInclusive,
				                     getPrevious, getNext, versionTimestamp, maxVersion, trim))
				time_series_map = ts_dao.retrieveTimeSeries(dak, ts_templates, startTimeInclusive,
				        endTimeInclusive, getPrevious, getNext, versionTimestamp, maxVersion, trim)
				if time_series_map is None:
					logger.warning("No time series found for ts_templates: {}, startTimeInclusive: {}, "
					            "endTimeInclusive: {}, getPrevious: {}, getNext: {}, "
					            "versionTimestamp: {}, maxVersion: {}, trim: {}"
					            .format(ts_templates, startTimeInclusive, endTimeInclusive,
					                    getPrevious, getNext, versionTimestamp, maxVersion, trim))
				else:
					time_series = time_series_map.get(ts_template)
					if time_series is None:
						logger.warning("TimeSeries for ts_template: {} not found in time_series_map: {}"
						            .format(ts_template, time_series_map))
					
				if time_series is not None:
					dstx = DataSetTx(time_series)
					tsc = dstx.getTimeSeriesContainer()
					fromTimezone = TimeZone.getTimeZone(tsc.timeZoneID)
					
					if not timeZone.equals(fromTimezone):
						logger.warning("Converting time zone from %s to %s" % (fromTimezone.getID(), timeZone.getID()))
						tsc.convertTimeZone(timeZone)
				
			except DbIoException as e:
				logger.exception("Error getting TimeSeriesContainer")
			except Exception as e:
				logger.exception ('Undefined error')
# 				traceback.print_exc(e)
			finally:
				dak.close()
		except DataSetIllegalArgumentException as e:
			raise Exception("Illegal Argument", e)
		finally:
			self.unlock()
			
		return tsc
	
	@_timed
	def putTimeSeriesContainer(
			self,
			object,
			units = None,
			timeZone = None,
			storeRule = None,
			overrideProtection = None,
			versionDate = None,
			officeId = None) :
		'''
		Write a TimeSeriesContainer object to the database.
		'''
		#---------------------------------------#
		# sanity checks and data initialization #
		#---------------------------------------#
		if not self.canWrite() :
			raise Exception("Cannot write to database %s" % conn.getMetaData().getURL())
			
		if not isinstance(object, TimeSeriesContainer) :
			raise ValueError("Expected TimeSeriesContainer, got %s." % object.__class__.__name__)
		tsc = object
		if units :
			if tsc.units and units.upper() != tsc.units.upper() :
				print("Warning: Data specifies units as %s, storing as %s" % (tsc.units, units))
		else :
			if not tsc.units :
				raise ValueError("Units not specified in data or parameters")
		
		if timeZone :
			tzid = timeZone
			timeZone = TimeZone.getTimeZone(tzid)
			if tsc.timeZoneID :
				tz2 = TimeZone.getTimeZone(tsc.timeZoneID)
				if not timeZone.hasSameRules(tz2) :
					# The DBAPI version would use the tzid parameter and convert the data...
					raise ValueError("Warning: Data specifies time zone as %s, does not match argument %s" % (tsc.timeZoneID, tzid))
			if tsc.timeZoneRawOffset and timeZone.getRawOffset() != tsc.timeZoneRawOffset :
				# The DBAPI version would use tzid.
				raise ValueError("Warning: Data specifies raw offset %d, which is incompatible with time zone %s" % (
					tsc.timeZoneRawOffset,
					tzid))
		
		if versionDate :
			raise ValueError("Warning: versionDate parameter of putTimeSeriesContainer is not supported in RADARAPI")
			
		if not storeRule : storeRule = self._storeRule
		if not overrideProtection : overrideProtection = self._overrideProtection
		if not officeId : officeId = self._officeId
		
		description_tx = DescriptionTx(officeId, tsc.getFullName())
		dataset_tx = DataSetTx(tsc, description_tx)
		
		self.lock()
		try :
			db_conn = self._factory.getDbConnection()
			ts_dao = CwmsDaoServiceLookup.getDao(CwmsTimeSeriesDao, db_conn)
			dak = self._factory.getDataAccessKey("putTimeSeriesContainer")
			
			# Need a TimeSeries - build one
			time_series = dataset_tx.getTimeSeries()
			
			# ts_dao method like:
			# Timestamp storeTimeSeries(DataAccessKey dataAccessKey, TimeSeries dataset, int storeRule, boolean overrideProtection)
			storeRuleInt = Const.getRuleNumber(storeRule)
			ts_dao.storeTimeSeries(dak, time_series, storeRuleInt, bool(overrideProtection))
		
		finally:
			self.unlock()
		
	@_timed
	def _getParameterUnits_(self) :
		'''
		returns the _parameterUnits field, populating if necessary
		'''
		self.lock()
		try :
			if self._parameterUnits is None :
				
				try:
					param_dict = self._fetch_and_load_csv()
					self._setParameterUnits_(param_dict)
				
				except urllib2.HTTPError as e:
					logger.exception("HTTPError code:%s"% str( e.code))
				except urllib2.URLError as e:
					logger.exception("URLError reason:%s"% str( e.reason))
				except Exception as e:
					logger.exception ('Undefined error") # %s %s'% e, traceback.format_exc())
				
			return self._parameterUnits
		finally :
			self.unlock()
	
	@_timed
	def _setParameterUnits_(self, param_dict):
		self._parameterUnits = param_dict
	
	@_timed
	def _fetch_and_load_json(self):
		url = self._url + "parameters?format=json"
		logger.debug("Making request to %s" % (url))
		request = urllib2.Request(url)
		# parameters controller doesn't use the accept header (yet)
		# request.add_header("Accept", "application/json")
		f = urllib2.urlopen(request)
		# data = f.read()
		pj = json.load(f)  # load expects a file-like obj
		return self._load_parameters(pj)
		
	@_timed
	def _fetch_and_load_csv(self):
		try:
			response = HttpRequestBuilderImpl(self._connectionInfo, "parameters")\
				.addQueryParameter("format", "csv")\
				.get()\
				.withMediaType("*/*")\
				.execute()
			body = response.getBody()
			with StringIO(body) as f:
				reader = csv.reader(f)  # load expects a file-like obj
				
				retval = {'EN': {}, 'SI': {}}
				
				for row in reader:
					if len(row) < 5 or row[0].startswith("#"):
						continue
					param = row[1]
					unitEN = row[3]
					unitSI = row[4]
					if unitEN is not None:
						retval['EN'][param] = unitEN
						
					if unitSI is not None:
						retval['SI'][param] = unitSI
		finally:
			response.close()

		return retval

	@_timed
	def _build_param_dict_from_ParameterLookup(self):
		retval = None
		
		parameters = ParameterLookup.getAvailableParameters()
		if parameters is not None and len(parameters) > 0:
			retval = {"EN": {}, "SI": {}}
			for param in parameters:
				unitEN = ParameterLookup.getUnitsStringForSystem(param, 1)
				unitSI = ParameterLookup.getUnitsStringForSystem(param, 2)
				if unitEN is not None:
					retval['EN'][param] = unitEN
				
				if unitSI is not None:
					retval['SI'][param] = unitSI
		
		return retval
	
	@_timed
	def unitsForParameter(self, parameter) :
		units = None
		unit_system_id = self._unitSystem[:2].upper()  # EN or SI
		param_dict = self._getParameterUnits_()
		try :
			units =  param_dict[unit_system_id][parameter]
		except :
			if '-' in parameter :
				try :
					units = param_dict[unit_system_id][parameter.split('-')[0]]
				except :
					pass
		if not units : raise Exception("Could not determine database units for parameter: %s" % parameter)
		return units
		
	@_timed
	def _load_parameters(self, pj):
		retval = {'EN': {}, 'SI': {}}
		
		p_arr = pj['parameters']['parameters']
		
		for parameter in p_arr:
			param = parameter['name']
			if param is not None:
				unitEN = parameter['default-english-unit']
				if unitEN is not None:
					retval['EN'][param] = unitEN
				
				unitSI = parameter['default-si-unit']
				if unitSI is not None:
					retval['SI'][param] = unitSI
		return retval
	
	@_timed
	def getCatalogedPathnames_1(self, pattern) :
		'''
		Returns the current catalog filtered by the specified pattern.

		NOTE: pattern must use glob chars (*, ?) and not SQL chars (% ,_)
		'''
		pathnameList = []
		if not isinstance(pattern, basestring):
			raise TypeError('Pattern for getCatalogedPathnames must be a string')
		self.lock()
		try:
			dak = self._factory.getDataAccessKey("getCatalogedPathnames_1")
			try:
				ts_ids = self.getTimeSeriesCatalogIds(dak, pattern, self.getOfficeId())
				if ts_ids is not None:
					pathnameList.extend(ts_ids)
				
				# Need to also try rating catalog.
				rating_ids = self.getRatingCatalogIds(dak, pattern, self.getOfficeId())
				if rating_ids is not None:
					pathnameList.extend(rating_ids)
			
			except DbIoException as e:
				logger.exception('DbIoException getting Catalog paths matching %s'% pattern)
			except Exception as e:
				logger.exception ('Undefined error')
				# traceback.print_exc(e)
			finally:
				dak.close()
		finally:
			self.unlock()
		
		return pathnameList
	
	@_timed
	def getTimeSeriesCatalogIds(self, dak, pattern, officeId=None):
		retval = []
		
		db_conn = self._factory.getDbConnection()
		catalog_dao = CwmsDaoServiceLookup.getDao(CwmsCatalogDao, db_conn)
		
		locationCategory = None
		locationGroup = None
		tsCategory = None
		tsGroup = None
		timeseries_id_list = catalog_dao.getTimeSeriesIdentifierCatalog(dak,
			pattern, locationCategory, locationGroup, tsCategory, tsGroup, officeId)
		
		if timeseries_id_list is not None:
			for timeseries_identifier in timeseries_id_list:
				retval.append(timeseries_identifier.getTimeSeriesId())
		
		return retval
		
	@_timed
	def getRatingCatalogIds(self, dak, pattern, officeId = None):
		
		db_conn = self._factory.getDbConnection()
		rating_dao = CwmsDaoServiceLookup.getDao(CwmsRatingDao, db_conn)
		if rating_dao is None:
			raise Exception("Could not get rating dao")
		
		retval = self.getRatingSpecIdsFromSpecCatalog(rating_dao, dak, officeId, pattern)
		return retval
	
	@_timed
	def getRatingSpecIdsFromSpecCatalog(self, rating_dao, dak, officeId, pattern):
		retval = []
		rating_spec_catalog = rating_dao.retrieveRatingSpecCatalog(dak, pattern, officeId)
		# NavigableMap<LocationTemplate, Map<RatingTemplate, Set<RatingSpec>>>
		locTemplateSpecMap = rating_spec_catalog.getSpecifications()
		for ltMapEntry in locTemplateSpecMap.entrySet():
			#LocationTemplate lt = ltMapEntry.getKey()
			specMap = ltMapEntry.getValue()
			for specMapEntry in specMap.entrySet():
				# RatingTemplate rt = specMapEntry.getKey()
				specSet = specMapEntry.getValue()
				for spec in specSet:
					specStr = spec.getRatingSpecId()
					retval.append(specStr)
		return retval
	
	@_timed
	def getCatalogedPathnames_2(self, forceNew) :
		'''
		Returns the current catalog

		NOTE: forceNew is now obsolete
		'''
		return self.getCatalogedPathnames_1('*')
	
	@_timed
	def getCatalogedPathnames_3(self, pattern, forceNew) :
		'''
		Returns the current catalog filtered by the specified pattern

		NOTE: pattern must use glob chars (*, ?) and not SQL chars (% ,_)

		NOTE: forceNew is now obsolete
		'''
		return self.getCatalogedPathnames_1(pattern)

	@_timed
	def getCatalogedPathnames(self, *args) :
		'''
		Overloader for getCatalogedPathnames_? methods

		NOTE: pattern must use glob chars (*, ?) and not SQL chars (% ,_)
		'''
		count = len(args)
		meth  = None
		_args = None
		if count == 0 :
			meth = self.getCatalogedPathnames_1
			_args = ('*')
		elif count == 1 :
			_args = args
			if isinstance(args[0], basestring) :
				meth = self.getCatalogedPathnames_1
			else :
				meth = self.getCatalogedPathnames_2
		elif count == 2 :
			meth = self.getCatalogedPathnames_3
			_args = args
		
		if not meth :
			raise TypeError('getCatalogedPathnames() too many arguments; expected 2 to 3, got %d' % count + 1)
		
		return meth(*_args)
	
	@_timed
	def getTimeSeriesExtents(self, tsId, startTime, endTime) :
		'''
		Returns the time series extents in HecTime objects startTime and endTime
		'''
		start, end = self._getTimeSeriesExtents_(tsId)
		#-------------------------------------------#
		# report the times in the default time zone #
		#-------------------------------------------#
		cal = Calendar.getInstance()
		cal.setTimeZone(self._timeZone)
		cal.setTimeInMillis(start)
		startTime.set(cal)
		cal.setTimeInMillis(end)
		endTime.set(cal)
		
	@_timed
	def _getTimeSeriesExtents_(self, tsId) :
		'''
		Returns the time series extents for a time series id
		'''
		self.lock()
		
		try :
			db_conn = self._factory.getDbConnection()
			ts_dao = CwmsDaoServiceLookup.getDao(CwmsTimeSeriesDao, db_conn)
			
			dak = self._factory.getDataAccessKey("_getTimeSeriesExtents_")
			try:
				versionDate = None
				ignoreNulls = True
				officeId = None
				time_window = ts_dao.getTimeSeriesExtents(dak, officeId, tsId,
				                                          self._utcTimeZone,
				                                          versionDate, ignoreNulls)
				
				startDate = time_window.getStartDate()
				endDate = time_window.getEndDate()
			except DbIoException as e:
				raise Exception("Failed to retrieve timeseries extents", e)
			return \
				startDate.getTime(), endDate.getTime()
		finally :
			self.unlock()
	
	@_timed
	def getPathnameList(self) :
		'''
		Returns the current catalog
		'''
		return self.getCatalogedPathnames()
	
	@_timed
	def get_1(self, id, startTimeStr=None, endTimeStr=None, units=None) :
		'''
		Retrieve a time series or rating container from the database.
		'''
		if isTsId(id) :
			if not units : units = self.unitsForParameter(id.split('.')[1])
			return self.getTimeSeriesContainer(
				id,
				startTimeStr,
				endTimeStr,
				units)
		elif isRatingId(id) :
			if (units is not None) :
				raise ValueError("Cannot specify units with for a rating")
			return self.getRating(id, startTimeStr, endTimeStr)
		else :
			raise ValueError('"%s" is not a time-series id or rating id.' % id)
	
	@_timed
	def get_2(self, id, getEntireTimeWindow=False, units=None) :
		'''
		Read a time-series or a rating object from the database and return it as an DataContiainer object.
		'''
		startTimeStr = endTimeStr = None
		if getEntireTimeWindow :
			startTime = HecTime()
			endTime   = HecTime()
			self.getTimeSeriesExtents(id, startTime, endTime)
			startTimeStr = startTime.dateAndTime(4)
			endTimeStr   = endTime.dateAndTime(4)
		return self.get_1(id, startTimeStr, endTimeStr, units)
	
	@_timed
	def get(self, *args, **kwargs) :
		'''
		Overloader for get_? methods
		'''
		count = len(args) + len(kwargs)
		meth = None
		if   count == 1 : meth = self.get_1
		elif count == 2 : meth = self.get_2
		elif count == 4 : meth = self.get_1
		elif count == 3 :
			if len(args) > 1 :
				meth = (self.get_2, self.get_1)[isinstance(args[1], basestring)]
			else :
				meth = (self.get_1, self.get_2)[kwargs.has_key('getEntireTimeWindow')]
		if not meth :
			raise TypeError('get() too many arguments; expected 2 to 5, got %d' % count + 1)
		
		return meth(*args, **kwargs)
	
	@_timed
	def put(self, object, *args) :
		if not self.canWrite() :
			raise Exception("Cannot write to database %s" % conn.getMetaData().getURL())
		argsLen = len(args)
		if isinstance(object, TimeSeriesContainer) :
			if not isTsId(object.fullName) :
				raise ValueError('"%s" is not a time-series id.' % object.fullName)
			units, timeZone, storeRule, overrideProtection, versionDate, officeId = 6 * [None]
			if argsLen > 0 : units              = args[0]
			if argsLen > 1 : timeZone           = args[1]
			if argsLen > 2 : storeRule          = args[2]
			if argsLen > 3 : overrideProtection = args[3]
			if argsLen > 4 : versionDate        = args[4]
			if argsLen > 5 : officeId           = args[5]
			if argsLen > 6 : raise ValueError("Expected 1-7 arguments, got %d" % argsLen+1)
			self.putTimeSeriesContainer(
				object,
				units,
				timeZone,
				storeRule,
				overrideProtection,
				versionDate,
				officeId)
		elif isinstance(object, RatingSetContainer) :
			failIfExists = None
			if argsLen > 0 : failIfExists = args[0]
			if argsLen > 1 : raise ValueError("Expected 1-2 arguments, got %d" % argsLen+1)
			self.storeRating(object, failIfExists)
		else :
			raise TypeError("Expected TimeSeriesContainer or RatingSetContainer, got %s." % object.__class__.__name__)
	
	@_timed
	def read(self, id, startTimeStr=None, endTimeStr=None, units=None) :
		'''
		Read a time-series or a rating object from the database and return it as an HecMath object.
		'''
		if isTsId(id) :
			if not units :
				units = self.unitsForParameter(id.split('.')[1])
			time_series_container = self.getTimeSeriesContainer(id, startTimeStr, endTimeStr, units)
			
			if(time_series_container is None) :
				raise Exception("Time series container is null")
			return TimeSeriesMath(time_series_container)
		elif isRatingId(id) :
			if (units is not None) :
				raise ValueError("Cannot specify units for a rating")
			return self.readRating(id, startTimeStr, endTimeStr)
		else :
			raise ValueError('"%s" is not a time-series id or rating id.' % id)
	
	@_timed
	def write(self, object, *args) :
		if not self.canWrite() :
			raise Exception("Cannot write to database" )
		argsLen = len(args)
		if isinstance(object, TimeSeriesMath) :
			units = None
			timeZone = None
			storeRule = None
			overrideProtection = None
			versionDate = None
			officeId = None
			
			tsc = object.getData()
			if not isTsId(tsc.fullName) :
				raise ValueError('"%s" is not a time-series id.' % object.fullName)
			if argsLen > 0 : units              = args[0]
			if argsLen > 1 : timeZone           = args[1]
			if argsLen > 2 : storeRule          = args[2]
			if argsLen > 3 : overrideProtection = args[3]
			if argsLen > 4 : versionDate        = args[4]
			if argsLen > 5 : officeId           = args[5]
			if argsLen > 6 : raise ValueError("Expected 1-7 arguments, got %d" % argsLen+1)
			self.putTimeSeriesContainer(
				tsc,
				units,
				timeZone,
				storeRule,
				overrideProtection,
				versionDate,
				officeId)
		elif isinstance(object, RatingSet) :
			failIfExists = None
			if argsLen > 0 : failIfExists = args[0]
			if argsLen > 1 : raise ValueError("Expected 1-2 arguments, got %d" % argsLen+1)
			self.storeRating(object, failIfExists)
		else :
			raise TypeError("Expected TimeSeriesMath or RatingSet, got %s." % object.__class__.__name__)
		
	@_timed
	def _readRating_(self, loadMethodStr, ratingId, startTimeStr=None, endTimeStr=None) :
		'''
		Retrieves a rating from the database as a RatingSet object
		'''
		self.lock()
		try :
			if (startTimeStr == None) != (endTimeStr == None) :
				raise ValueException("Start time and end time must both be specified, or neither.")
			if startTimeStr :
				self._utcCal.setTimeInMillis(RMAIO.parseDate(startTimeStr, self._utcTimeZone))
				startZDT = ZonedDateTime.ofInstant(self._utcCal.toInstant(), self._utcTimeZone.toZoneId())
				
				self._utcCal.setTimeInMillis(RMAIO.parseDate(endTimeStr, self._utcTimeZone))
				endZDT = ZonedDateTime.ofInstant(self._utcCal.toInstant(), self._utcTimeZone.toZoneId())
				
			else :
				startZDT = None
				endZDT = None
			
			if loadMethodStr is not None :
				logger.warning("RADARAPI _read_rating_: Ignoring loadMethodStr.")
			
			use_get_one = True
			
			if use_get_one :
				logger.warning("RADARAPI _read_rating_: Ignoring date range.")
				url = self._build_rating_getOne_url_(ratingId)
				request = urllib2.Request(url)
				request.add_header("Accept", "application/xml;version=2")
			else:
				# getAll supported the date range but the return values
				# didn't parse correctly via RatingSet.fromXml
				url = self._build_rating_getAll_url_(ratingId, startZDT, endZDT)
				request = urllib2.Request(url)
			
			try:
				f = urllib2.urlopen(request)
				rating_xml = f.read()
				rating_set = RatingXmlFactory.ratingSet(rating_xml)
				return rating_set
			
			except RatingException as e:
				logger.exception("Error reading rating from Radar: %s" % e)
			except urllib2.HTTPError as e:
				logger.exception( "HTTPError code:%s"% str(e.code))
			except urllib2.URLError as e:
				logger.exception( "URLError reason:%s"% str( e.reason))
			except Exception as e:
				logger.exception ('Undefined error getting rating %s'% ratingId)
				
		finally :
			self.unlock()
		
	@_timed
	def _build_rating_getOne_url_(self, ratingId, officeId=None):
		url = None
		try:
			args = {}
			
			if officeId is not None:
				args["office"] = officeId
			
			extra_args = ""
			if len(args) > 0:
				extra_args = "?" + urllib.urlencode(args)
			 
			url = self._url + "ratings/" + urllib.quote(ratingId) + extra_args
		except DateTimeException as e:
			logger.exception ('DateTimeException error %s %s'% (e, traceback.format_exc()))
		except Exception as e:
			logger.exception ('Undefined error %s %s'% (e, traceback.format_exc()))
		return url
		
	@_timed
	def _build_rating_getAll_url_(self, ratingId, startZDT, endZDT):
		url = None
		try:
			args = {}
			args["format"] = "xml"  # format will be handled by an accept header in caller
			args['name'] = ratingId
			if startZDT is not None:
				args['at'] = startZDT.format(DateTimeFormatter.ISO_INSTANT)
			if endZDT is not None:
				args['end'] = endZDT.format(DateTimeFormatter.ISO_INSTANT)
			url = self._url + "ratings/?" + urllib.urlencode(args)
		except DateTimeException as e:
			logger.exception ('DateTimeException error %s %s'% (e, traceback.format_exc()))
		except Exception as e:
			logger.exception ('Undefined error %s %s'% (e, traceback.format_exc()))
		return url
	
	
	@_timed
	def readRating(self, *args) :
		'''
		Retrieves a rating from the database as a RatingSet object
		'''
		loadMethodStr = startTimeStr = endTimeStr = None
		argCount = len(args)
		if not 0 < argCount < 5 : raise Exception("readRating() requires 1..4 args, %d specified." % argCount)
		if args[0].upper() in ("EAGER", "LAZY", "REFERENCE") :
			loadMethodStr = args[0]
			ratingId = args[1]
			used = 2
		else :
			ratingId = args[0]
			used = 1
		if argCount not in (used, used+2) :
			raise ValueException("Start time and end time must both be specified, or neither.")
		if argCount == used + 2 :
			startTimeStr = args[used]
			endTimeStr = args[used+1]
		return self._readRating_(loadMethodStr, ratingId, startTimeStr, endTimeStr)
	
	@_timed
	def getRating(self,*args) :
		'''
		Retrieves a rating container from the database as a RatingSetContainer object
		'''
		
		ratingSet = None
		if args[0].upper() in ("EAGER", "LAZY", "REFERENCE") or self._ratingLoadMethod == "EAGER" :
			#-------------------------------------------------------#
			# load method specified or default load method is EAGER #
			#-------------------------------------------------------#
			ratingSet = self.readRating(*args)
		else :
			#---------------------------------------------------------------#
			# no load method specified and default load method is not EAGER #
			#---------------------------------------------------------------#
			ratingSet = self.readRating(*tuple(["EAGER"] + list(args)))
		
		if ratingSet is not None :
			return ratingSet.getData()
		return None
		
		
	@_timed
	def storeRating(self, object, fail_if_exists=None) :
		'''
		Store a rating to the database
		'''
		if not self.canWrite() :
			raise Exception("Cannot write to database " )
			
		if isinstance(object, RatingSet) :
			rating = object
		elif isinstance(object, RatingSetContainer) :
			rating = RatingSetFactory.ratingSet(object)
		else :
			raise TypeError("Object is not a RatingSet or RatingSetContainer")
		if fail_if_exists is None : fail_if_exists = True
		if type(fail_if_exists) != type(True) :
			raise TypeError("Parameter fail_if_exists must be True or False")
		
		db_conn = self._factory.getDbConnection()
		rating_dao = CwmsDaoServiceLookup.getDao(CwmsRatingDao, db_conn)
		dak = self._factory.getDataAccessKey("storeRating")
		
		# fyi CwmsRatingRadarDao wants failIfExists to be False and replace_base to be True
		replace_base = True
		rating_dao.storeRatingSet(dak, rating, fail_if_exists, replace_base)
		
	
##############################################################################

@_timed
def open(*args, **kwargs) :
	'''
	Return a new RadarAccess object.
	'''
	return RadarAccess(*args, **kwargs)

@_timed
def getVersion() :
	'''
	Returns the version of this module in the form "X.XX ddMMMyyyy"
	'''
	return "%s %s" % (VERSION_NUMBER, VERSION_DATE)

@_timed
def main():
	logger.warning("main not implemented.")

if __name__ == '__main__' : main()
