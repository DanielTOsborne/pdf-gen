'''
This module allows Jython scripts to access the CWMS database through the
database application programming interface.

It is intended to provide essentially the same API as is used to access data
stored in DSS files and the DBI module

The API is as follows :

   ==[Functions]================================================================

   getVersion()
      Returns the version of this module in the form "YY.n ddMMMyyyy"

   open([connectString[, startTime, endTime]])
      Returns a DbAccess object. The object has the default store rule set
      to "Replace All" and the units system set to "English".

      If the connection string is specified, it must be of the form
      "user/pass@host:port/sid".

      If the connection string is not specified, the behavior is as follows:
        * If running in a CAVI session, the CAVI database connection is used
        * If running on a CWMS sever as the production user, a connection to
          the CWMS production database for that user is initiated
        * Otherwise a database login dialog will be presented to initiate a
          connection

      If startTime and endTime are specified the object will have the
      specified default time window, which can be changed by subsequent
      calls to set setTimeWindow method. Individual reads can override the
      default time window by specifying a time window in the method call
      to get or read. If no time window is specified, the default time
      window is undefined and must either be set via the setTimeWindow
      method or overridden in get or read method calls.

      In addition to the time window, the following defaults are set:
         officeId           = default office for connected user
         unitSystem         = "English"
         storeRule          = "Replace All"
         timeZone           = "UTC"
         trimMissing        = True
         startTimeInclusive = True
         endTimeInclusive   = True
         getPrevious        = False
         getNext            = False
         overrideProtection = False
         maxVersion         = True

   ==[DbAcess object methods]===================================================

   isOpen()
      Returns whether the DbAccess object is still valid. Returns True until
      the close or done method is called, after which it returns False.

   getFileName()
      Returns a string containing the connection information for the current
      connection.

   getConnection()
      Gets the database connection for external use. As of version 19.1 this
      connection is retrieved from the connection pool and is not a single
      connection held by the DbAccess object. It is the responsibility of the
      code receiving the connection to close it when it is no longer needed.

   close()
      Closes a DbAccess object and releases all its resources.

   done()
      Closes a DbAccess object and releases all its resources.

   get(objectID)
      Retrieves a container object from the database. If objectID is a time
      series identifier, a TimeSeriesContainer will be returned containing
      data for the current default time window. If objectID is a rating
      identifier, a RatingSetContainer will be returned containing the
      specified rating.

   get(timeSeriesID, startTime, endTime[, unit])
      Retrieves a TimeSeriesContainer object for the specified ID and time
      window.

      The startTime and endTime parameters override the default time window.
      StartTime and endTime must be of the format ddMonyyyy[,] hh[:]mm
      (e.g. 06Oct2010 1014, or 06OCT2010, 10:14). Specify startTime and endTime
      as strings.

      If unit is specified, the TimeSeriesContainer will be in the specified
      unit. Otherwise the data will be in the default unit of the current unit
      system. The unit parameter may be a simple unit (e.g. 'ft', 'm'), or a
      complex string that also specifies the vertical datum for elevations
      (e.g. 'u=ft|v=NAVD88'). If the vertical datum is specified in the unit
      parameter, it will override any default vertical datum that has been spec-
      ified.

   get(timeSeriesID, getAllData[, unit])
      Retrieves a TimeSeriesContainer object for the specified ID.

      If getAllData evaluates to true, the effective time window will encompass
      all data for the time series. If getAllData evaluates to False, the
      default time window will be used. The getAllData parameter may be specified
      as any type evaluable as a boolean to Python. Use the Python constants
      True and False for greatest clarity.

      If unit is specified, the TimeSeriesContainer will be in the specified
      unit. Otherwise the data will be in the default unit of the current unit
      system. The unit parameter may be a simple unit (e.g. 'ft', 'm'), or a
      complex string that also specifies the vertical datum for elevations
      (e.g. 'u=ft|v=NAVD88'). If the vertical datum is specified in the unit
      parameter, it will override any default vertical datum that has been spec-
      ified.

   getRating(ratingLoadMethod, ratingID)
      Retrieves a RatingSetContainer object for the specified ID, using the
      specified rating load method.

   read(objectID)
      Retrieves a non-container object from the database. If objectID is a
      time series identifier, a TimeSeriesMath object will be returned with
      data for the current default time window. If objectID is a rating
      identifier, a RatingSet object will be returned containing the specified
      rating.

   read(timeSeriesID, startTime, endTime[, unit])
      Retrieves a TimeSeriesMath object for the specified ID and time
      window.

      The startTime and endTime parameters override the default time window.
      StartTime and endTime must be of the format ddMonyyyy[,] hh[:]mm
      (e.g. 06Oct2010 1014, or 06OCT2010, 10:14). Specify startTime and endTime
      as strings.

      If unit is specified, the TimeSeriesMath will be in the specified
      unit. Otherwise the data will be in the default unit of the current unit
      system. The unit parameter may be a simple unit (e.g. 'ft', 'm'), or a
      complex string that also specifies the vertical datum for elevations
      (e.g. 'u=ft|v=NAVD88'). If the vertical datum is specified in the unit
      parameter, it will override any default vertical datum that has been spec-
      ified.

   readRating(ratingLoadMethod, ratingID)
      Retrieves a RatingSet object for the specified ID, using the specified
      rating load method.

   put(timeSeriesContainer[,unit[,timeZone[,storeRule[,overrideProtection[,versionDate[,officeId]]]]]])
      Stores a time series the database, creating it if necessary.

      If unit is specified, it overrides any unit specified in the object. If
      the unit is specified and is different from the object unit, a warning
      will be issued. If unit is not specified and the object also does not
      specify a unit, an exception will be raised. Specify unit as a string.

      If timeZone is specified, it overrides the default time zone and any time
      zone specified in the object. If timeZone is specified and it is different
      from the the object time zone, a warning will be issued. If timeZone is
      not specified and the object also does not specify a time zone, the de-
      fault time zone will be used. The time zone may be specified as a string
      or a TimeZone object.

      If storeRule is specified, it overrides the default store rule. Valid
      parameters are:
         "Delete Insert"
         "Replace All"
         "Do Not Replace"
         "Replace Missing Values Only"
         "Replace With Non Missing"

      if overrideProtection is specified, it overrides the default override
      protection state. The override protection state may be specified as any
      type evaluable as a boolean to Python. Use the Python constants True and
      False for greatest clarity.

      If versionDate is specified, it overrides the default version date. If
      versionDate is not specified, the default version date will be used.
      Specify versionDate as an empty string ("" or '') to override the default
      version date with the "Non-verioned" date. If specified and not empty,
      versionDate must be a string of the format ddMonyyyy[,] hh[:]mm
      (e.g. 06Oct2010 1014, or 06OCT2010, 10:14).

      If officeId is specified, it overrides the default office id. Specify
      officeId as a string.

   put(ratingSetContainer[, failIfExists])
      Stores a rating specified as a RatingSetContainer to the database. If
      failIfExists is specified, it must be True or False. If not specified,
      it defaults to False. If specified as True, the data in the
      RatingSetContainer object will overwrite any existing data in the
      database for the same rating template, rating specification, or rating.
      If failIfExists is not specified or is specified as False, the method will
      raise an exception of any of the rating template, rating specification,
      or ratings already exist in the database.

   write(timeSeriesMath[,units[,timeZone[,storeRule[,overideProtection[,versionDate,officeId]]]]])
      Stores a time series the database, creating it if necessary.

      If unit is specified, it overrides any unit specified in the object. If
      the unit is specified and is different from the object unit, a warning
      will be issued. If unit is not specified and the object also does not
      specify a unit, an exception will be raised. Specify unit as a string.

      If timeZone is specified, it overrides the default time zone and any time
      zone specified in the object. If timeZone is specified and it is different
      from the the object time zone, a warning will be issued. If timeZone is
      not specified and the object also does not specify a time zone, the de-
      fault time zone will be used. The time zone may be specified as a string
      or a TimeZone object.

      If storeRule is specified, it overrides the default store rule. Valid
      parameters are:
         "Delete Insert"
         "Replace All"
         "Do Not Replace"
         "Replace Missing Values Only"
         "Replace With Non Missing"

      if overrideProtection is specified, it overrides the default override
      protection state. The override protection state may be specified as any
      type evaluable as a boolean to Python. Use the Python constants True and
      False for greatest clarity.

      If versionDate is specified, it overrides the default version date. If
      versionDate is not specified, the default version date will be used.
      Specify versionDate as an empty string ("" or '') to override the default
      version date with the "Non-versioned" date. If specified and not empty,
      versionDate must be a string of the format ddMonyyyy[,] hh[:]mm
      (e.g. 06Oct2010 1014, or 06OCT2010, 10:14).

      If officeId is specified, it overrides the default office id. Specify
      officeId as a string.

   write(ratingSet[, failIfExists])
      Stores a rating specified as a RatingSet to the database. If
      failIfExists is specified, it must be True or False. If not specified,
      it defaults to False. If specified as True, the data in the
      RatingSetContainer object will overwrite any existing data in the
      database for the same rating template, rating specification, or rating.
      If failIfExists is not specified or is specified as False, the method will
      raise an exception of any of the rating template, rating specification,
      or ratings already exist in the database.

   delete(objectID)
      Deletes a time series or rating from the database. Calling this method
      with a valid time series identifier deletes all the data for the time
      series as well as the time series identifier from the database. Calling
      this method with a valid rating specification identifier deletes the
      rating specification and all associated ratings from the database.

   getCatalogedPathnames()
      Returns a list of all object ids in the database. Currently only time
      series ids and rating specification ids are returned. Equivalent to
      getPathnameList()

   getCatalogedPathnames(pattern)
      Returns a list of all object ids in the database that match the specified
      pattern. Currently only time series ids and rating specification ids are
      returned.

      The pattern must be a glob (file name mask) style pattern and not an SQL
      LIKE pattern.

   getCatalogedPathnames(forceNew)
      Simply calls getCatalogedPathnames() as the CWMS database catalog never
      needs refreshing.

      Although Python supports Boolean tests on strings, passing a string to
      this method will result in it being interpreted as a pattern (see above)
      instead of a boolean. Using the Python values True and False provides
      the greatest clarity.

   getCatalogedPathnames(pattern, forceNew)
      Simply calls getCatalogedPathnames(pattern) as the CWMS database catalog
      never needs refreshing.

      The pattern must be a glob (file name mask) style pattern and not an SQL
      LIKE pattern.

   getPathnameList()
      Returns a list of all object ids in the database. Currently only time
      series ids and rating specification ids are returned.

   setTimeZone(timezone)
     Sets the default time zone for the DBAccess object. The time zone may be
     specified as a string or a TimeZone object.

   getTimeZone()
     Retruns the current default time zone as a TimeZone object.

   getTimeZoneName()
     Retruns the current default time zone as a string.

   setTimeWindow(startTime, endTime)
      Sets the default time window for the DbAccess object. The default
      time window will be used for all subsequent time-series reads that
      do not in themselves specify a time window. This setting does not
      affect the reading of rating objects. If a rating is read without
      specifying the time window, the entire time-series of ratings is
      returned. If a time window is specified for a rating, only the
      ratings necessary to rate the specified time window are retrieved.

   getTimeWindow()
      Gets the default time window as the tuple (startTime, endTime).

   setStartTimeInclusive(inclusive)
      Sets whether the (specified or default) start time is included in the time
      window when retrieving time series data.

   getStartTimeInclusive()
      Gets whether the (specified or default) start time is included in the time
      window when retrieving time series data.

   setEndTimeInclusive(inclusive)
      Sets whether the (specified or default) end time is included in the time
      window when retrieving time series data.

   getEndTimeInclusive()
      Gets whether the (specified or default) end time is included in the time
      window when retrieving data.

   setRetrievePrevious(retrievePrevious)
      Sets whether data for the latest time prior to the time window is included
      when retrieving time series data.

   getRetrievePrevious()
      Sets whether data for the latest time prior to the time window is included
      when retrieving time series data.

   setRetrieveNext(retrieveNext)
      Sets whether data for the earliest time after the time window is included
      when retrieving time series data.

   getRetrieveNext()
      Gets whether data for the earliest time after the time window is included
      when retrieving time series data.

   setTrimMissing(trimMissing)
     Sets whether to trim missing data from the first and last of time series
     data retrieved from the database.

   getTrimMissing()
     Gets whether to trim missing data from the first and last of time series
     data retrieved from the database.

   setRetrieveMaxVersionDate(retrieveMaxVersionDate)
      Sets whether to retrieve the time series data with the latest version
      date when no version date is specified. If false, the data with earliest
      version date will be retrieved.

   getRetrieveMaxVersionDate()
      Gets whether to retrieve the time series data with the latest version
      date when no version date is specified. If false, the data with earliest
      version date will be retrieved.

   setVersionDate(versionDate)
      Sets the default version date for time series data to store or retrieve.
      The version date is agnostic of time zones and is not modified by any
      time zone.

   resetVersionDate()
      Sets the default version date for time series data to store or retrieve
      to the "Non-Versioned" date of 11Nov1111 0000.

   getVersionDate()
      Gets the default version date for time series data to store or retrieve.

   setStoreRule(storeRule)
      Sets the default store rule for the DbAccess object. The default store
      rule will be used for all subsequent time series put() and write()
      time series methods that do not in themselves specify a store rule. Valid
      store rules are any of the following strings:

         "Delete Insert"
         "Replace All"
         "Do Not Replace"
         "Replace Missing Values Only"
         "Replace With Non Missing"

   getStoreRule()
      Gets the default store rule.

   setRatingLoadMethod(ratingLoadMethod)
      Sets the default method for loading ratings from the database. Valid
      methods are:

         "EAGER"     - loads all table data initially
         "LAZY"      - loads table data for rating operations as needed
         "REFERENCE" - performs all rating operations in database

   getRatingLoadMethod()
      Gets the default method for loading ratings from the database.

   setUnitSystem(system)
      Sets the units system for the DbAccess object. The unit system is used
      for all subsequent calls to the get() and read() time series  methods.
      Valid unit systems are "English" and "SI".

   setOfficeId()
      Sets the default office identifier.

   getOfficeId()
      Gets the default office identifier.

   setOverrideProtection(overrideProtection)
      Sets the default state of whether to override data protection flags when
      storing time series data. When True, protected data can be updated; when
      False, protected data will not be updated. The override protection state
      may be specified as any type evaluable as a boolean to Python. Use the
      Python constants True and False for greatest clarity.

   getOverrideProtection()
      Gets the default state of whether to override data protection flags when
      storing time series data. When True, protected data can be updated; when
      False,  protected data will not be updated.

    setDefaultVerticalDatum(verticalDatum)
      Sets the default vertical datum for elevations. Initially the default
      vertical datum is unset, in which queried elevations are returned, and
      specified elevations are interpreted in the location's indicated vertical
      datum.

    getDefaultVerticalDatum
      Retrieves the current default vertical datum for elevations.

    getLocationVerticalDatum(locationId [,officeId])
      Retrieves the location's indicated vertical datum. If officeId is not
      specified, the current default office identifier is used.

    setVerticalDatumOffset(locationId, verticalDatumId1, verticalDatumId2, value, unit)
      Sets a known vertical datum offset for a location and pair of vertical
      datums. The value is the offset that must be added to an elevation WRT
      verticalDatumId1 to generate an elevation WRT verticalDatumId2.

    getVerticalDatumOffset(locationId, verticalDatumId1, verticalDatumId2, unit [,officeId])
      Retrieves a (known or estimated) vertical datum offset for a location and
      pair of vertical datums. The value is the offset that must be added to an
      elevation WRT verticalDatumId1 to generate an elevation WRT
      verticalDatumId2.

      If officeId is not specified, the current default office id will be used.

    getVerticalDaumOffset(locationId [,unit [,officeId]])
      Retrieves a (known or estimated) vertical datum offset for a location from
      its indicated vertical datum to the default vertical datum. The offset is
      the value that must be added to an elevation WRT the location's indicated
      vertical datum to generate an elevation WRT the default vertical datum.

      If unit is not specified, the offset will be in "ft" or "m", depending on
      the current setting of the unit system, which defaults to "English". The
      unit parameter may be a simple unit (e.g. 'ft', 'm'), or a complex string
      that also specifies the vertical datum for elevations
      (e.g. 'u=ft|v=NAVD88'). If the vertical datum is specified in the unit
      parameter, it will override any default vertical datum that has been spec-
      ified.

      If officeId is not specified, the current default office id will be used.

Mike Perryman
USACE HEC

Version History

   1.00  06Oct2010   Original version derived from DBI module
   1.10  29Nov2011   Modifications for CWMS 2.1
   1.11  04Jan2012   Fix for units retrieval
   1.12  06Feb2012   Bugfix for previous modification
   1.13  28Feb2012   Fixed typo
   1.20  29May2012   Added support for CWMS-style ratings
   1.21  23Jul2012   Bugfix in DbAccess.put()
   1.22  27Jul2012   Bugfix in DbAccess.putTimeSeries()
   1.23  06Dec2012   Allow use of [ESMP][DS]T in setTimeZone
   1.24  06Jun2013   Bugfix in retrieving time series
   1.25  29Jul2013   Added DbAccess methods for vertical datums
   1.26  30Sep2013   Modified DbAccess.setOfficeId() to be backward compatible with 2.1
   1.27  24Jan2014   Added call to GET_DEFAULT_UNITS if no data in CWMS_V_DISPLAY_UNITS
   1.28  02Apr2014   Added capability to specify user/pass@url for database
   1.30  14Nov2014   Added office requirement for connecting to database for CWMS 3.0 schema
   1.31  24Apr2015   Removed office requirement for connecting to database for CWMS 3.0 schema via CWMS Login dialog
   1.32  05May2015   Bugfix for using open() function with database, user, password, and office
   1.33  11May2016   Removed explicit vertical datum retrieval for ratings as it is now performed by underlying java code.
   1.34  06Dec2016   Bugfix to allow storing of null values (Constants.UNDEFINED) in time series.
   1.40  12Dec2017   Removed dependencies on Oracle jars in the class path and sys.path for JPub/JOOQ combination.
                     Still uses some Oracle features and constants.
   1.41  13Dec2017   Fixed time offset bug in storing time series created by 1.40.
   1.50  04Apr2018   Modified to handle time series version dates with seconds
   1.52  13Aug2018   Improved releasing of database resources (result sets and temporary statements)
   19.1  23Jan2019   Modified to retrieve and close a pooled connection for each operation instead of holding a connection.
                     Added removal of connection factory instance on DbAccess.close() to allow multiple DBAPI.open() calls to different databases in the same script.
                     Removed dependency on wcds.dbi.client.JdbcConnection.
                     Added database connection method field and output on connection banner. Field values are :
                        CREDENTIALS_SPECIFIED_1  # All credentials specified (url, user, password, office)
                        CREDENTIALS_SPECIFIED_2  # All credentials except url specified
                        NETWORKED_CLIENT         # No credentials specified, existing connection used
                        CWMS_PRODUCTION_ACCOUNT  # No credentials specified, taken from environment, production dbi.properties, and production dbi.conf
                        CWMS_SERVICE_ACCOUNT     # No credentials specified, taken from environment, production dbi.properties, and local dbi.conf
                        SERVER_SUITE_WO_DIALOG   # No credentials specified, existing connection used
                        SERVER_SUITE_W_DIALOG    # No credentials specified, login dialog used
                     Improved DbAccess.setTimeWindow() to allow date/date+time/date_time_expression for start and/or end times.
                     Added DbAccess.setRatingLoadMethod() and DbAccess.getRatingLoadMethod() to manage default rating load method
                     Added ability to override default rating load method in new DbAccess.getRating() and DbAccess.readRating() methods.
                     Added ability to use CWMS 3.2+ RatingSet() constructors instead of CWMS 3.1- RatingSet.fromDatabase() generators.
                     Changed version nameing scheme to YY.n.
   19.2  28Jun2019   Modified imports to work around strange issue in CWMS 3.2 that prevents importing of Java packages (but not Python modules or Java classes).
                     Fixed bug in DbAccess.setTimeWindow() - probably introduced in 19.1
   19.3  29Aug2019   Removed TSCatalog class and related functions.
                     Revmoed obsolete calls to CWMS_TS.REFRESH_TS_CATALOG
                     Replaced OracleTypes_XXX constants with OracleTypes class
   19.4  05Sep2019   Modified to work with CWMS 3.1 or CWMS 3.2
                     Added openNational() function and related dialog
                     Added canWrite() methd and calls from put/write/store/delete routines to prevent writing to national database
   19.5  27Sep2019   More modifications to work in multiple environments
                     Fixed unbalanced acquisition/release of database connections
   19.6  08Oct2019   Fixed bug in dbAccess._putTimeSeries_() where tsc.timeZoneID="CST" or "PST" wasn't replaced with proper standard time zone before storing to database.
   21.1  04May2021   Fixed bugs in dbAccess._putTimeSeries_() and dbAccess._getTimeSeries_() for versioned data
   21.2  09Aug2021   Added DBAPI.nonVersionedDate() and fixed bugs setting version date to non-versioned
   22.1  22Sep2022   Bugfix for canWrite() when DB URL is a TNS spec
'''

#  Copyright (c) 2023
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import platform
IS_CPYTHON = True if platform.python_implementation() == "CPython" else False

from hec.data               import Interval
from hec.data               import Parameter
from hec.data               import ParameterType
from hec.data.cwmsRating    import RatingSet
from hec.data.cwmsRating.io import RatingSetContainer
from hec.data.tx            import DSSTimeSeries

from hec.db                 import ConnectionFactory
from hec.db                 import DbConnection
from hec.heclib.util        import HecTime
from hec.hecmath            import TimeSeriesMath
from hec.io                 import PasswordFile
from hec.io                 import TimeSeriesContainer
from hec.io                 import TimeSeriesContainerVertDatum
from hec.io                 import VerticalDatumContainer
from hec.lang               import Const
from java.lang              import System
from java.sql               import Types
from java.text              import SimpleDateFormat
from java.util              import Calendar
from java.util              import Date
from java.util              import SimpleTimeZone
from java.util              import TimeZone
from javax.swing            import UIManager
from rma.util               import RMAIO
from wcds.dbi.oracle        import OracleDirectDataAccessFactory
try :
	#--------------------#
	# CWMS 3.2 and above #
	#--------------------#
	from usace.cwms.db.dao.util.connection import ConnectionLoginInfoImpl
	from hec.serversuite                   import ServerSuiteUtil
	from hec.serversuite.data              import ServerLoginState
	from mil.army.usace.hec.cwms.rating.io.jdbc import RatingJdbcFactory
	from hec.data.cwmsRating.io 		  import RatingJdbcCompatUtil
except ImportError :
	#----------#
	# CWMS 3.1 #
	#----------#
	from cwmsdb               import ConnectionLoginInfoImpl
	from hec.login            import ServerSuiteUtil
	from hec.login.data       import ServerLoginState

import os, string, threading, re, socket, collections, traceback

if IS_CPYTHON:
	basestring = str
	string.strip = str.strip

##############################################################################
'''
Capture open(...) as fopen(...) for calling from DbAccess object since it defines open(...)
'''
fopen = open

'''
Values that specify the database connection method
'''
CREDENTIALS_SPECIFIED_1  = "CREDENTIALS_SPECIFIED_1" # All credentials specified (url, user, password, office)
CREDENTIALS_SPECIFIED_2  = "CREDENTIALS_SPECIFIED_2" # All credentials except url specified
NETWORKED_CLIENT         = "NETWORKED_CLIENT"        # No credentials specified, existing connection used
CWMS_PRODUCTION_ACCOUNT  = "CWMS_PRODUCTION_ACCOUNT" # No credentials specified, taken from environmewnt, production dbi.properties, and production dbi.conf
CWMS_SERVICE_ACCOUNT     = "CWMS_SERVICE_ACCOUNT"    # No credentials specified, taken from environmewnt, production dbi.properties, and local dbi.conf
SERVER_SUITE_WO_DIALOG   = "SERVER_SUITE_WO_DIALOG"  # No credentials specified, existing connection used
SERVER_SUITE_W_DIALOG    = "SERVER_SUITE_W_DIALOG"   # No credentials specified, login dialog used
APP_NAME                 = "DBAPI"                   # Application name

'''
Version Information
'''
VERSION_NUMBER = "22.2"
VERSION_DATE   = "12Oct2022"

'''
National database information
This will probably be converted to a resource file, or some other
'''
NATIONAL_DATA =		collections.namedtuple('NATIONAL_DATA', ['ip', 'users'])
NATIONAL_DB_DATA =	[NATIONAL_DATA(ip="140.194.20.214", users=["S0CWMSP2"]),
					 NATIONAL_DATA(ip="140.194.45.154", users=["S0CWMSZ2"]),
					 NATIONAL_DATA(ip="140.194.45.148", users=["S0CWMSZ2"])]

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

##############################################################################

def isUndefined(value) :
	'''
	Determine if a value is undefined
	'''
	return value < Const.UNDEFINED_DOUBLE / 2

def nonVersionedDate() :
	'''
	Return a date string for non-versioned dates
	'''
	return '11Nov1111 0000'
    
def isNonVersioned(dateStr) :
	'''
	Determine if date string is the non-versioned date string
	'''
	return not dateStr or dateStr.upper().replace(';', '').replace(',', '').replace(':', '') in (
		'1111-11-11',
		'1111-11-11 0000',
		'1111-11-11 000000',
		'1111/11/11',
		'1111/11/11 0000',
		'1111/11/11 000000',
		'11NOV1111',
		'11NOV1111 0000',
		'11NOV1111 000000')

def isTsId(id) :
	'''
	Determine if an identifier looks like a CWMS Times Sries Identifier
	loc-subLoc.param-subParam.paramType.intvl.dur.ver
	'''
	return len(str(id).split('.')) == 6

def isRatingId(id) :
	'''
	Determine if an identifier looks like a CWMS Rating Identifier
	loc.indParams;depParam.templateVer.specVer
	'''
	parts = str(id).split('.')
	return len(parts) == 4 and len(parts[1].split(';')) == 2

def getCwmsHome() :
	'''
	Returns the CWMS_HOME value from the System properites or environment
	'''
	cwmsHome = System.getProperty("CWMS_HOME")
	if cwmsHome is None : cwmsHome = os.getenv("CWMS_HOME")
	if cwmsHome is None : raise Exception("CWMS_HOME is undefined")
	return cwmsHome

def getCwmsDbiProperty(key) :
	'''
	Returns a property in the $CWMS_HOME/config/properties/dbi.properties file
	'''
	home = getCwmsHome()
	dbiPropFileName = os.path.join(home, "config", "properties", "dbi.properties")
	if not os.path.exists(dbiPropFileName) or not os.path.isfile(dbiPropFileName) :
		abs = os.path.abspath(dbiPropFileName)
		err = "Unable to find dbi.properties at:" + os.linesep + \
			  "Original " + str(dbiPropFileName) + os.linesep + \
			  "Absolute " + str(abs) + os.linesep + \
			  "CWMS Home " + home
		raise Exception(err)
	with fopen(dbiPropFileName) as f : lines = f.read().strip().split("\n")
	for line in lines :
		line = line.strip()
		if not line or line[0] == "#"  or line.find("=") == -1 : continue
		k, v = line.split("=", 1)
		if k == key :
			value = v
			break
	else :
		raise Exception("%s property not found in %s" % (key, dbiPropFileName))
	return value

def getPassAndUrl(passwordFilename, userName, dbUrl) :
	'''
	Returns the password and case-corrected database URL from a specified password file for a specified user and database URL.
	The password is passed back in the first position of a 2-string array; the URL is passed back in
	the second position.  If the specified file name is not a valid password file, or if no match
	is found in the password file, the password and URL are both returned as null.
	'''
	results = None
	jdbcStr = "jdbc:oracle:thin:"
	connStr = connUpper = None
	if dbUrl and dbUrl.find("@") != -1 :
		jdbcStr, connStr = dbUrl.split("@", 1)
		connUpper = connStr.upper()
	userUpper = userName.upper()
	if os.path.exists(passwordFilename) and os.path.isfile(passwordFilename) :
		pwf = PasswordFile(passwordFilename, False)
		try :
			pwes = pwf.getEntries()
			for i in range(len(pwes)) :
				if i == 0 : continue # header
				pwe = pwes[i]
				if pwe.getUserName().upper() == userUpper :
					dbid = pwe.getId()
					if connUpper in (None, dbid.upper()) :
						results = (pwe.getPassword(), "%s@%s" % (jdbcStr, dbid))
		finally :
			pwf.close()
	return results

def getDbiDbUserAndPass() :
	'''
	Returns the DBI user and password for the CWMS database URL
	'''
	results = None
	home = getCwmsHome()
	dbiConfFileName = os.path.join(home, "config", "properties", "dbi.conf")
	if not os.path.exists(dbiConfFileName) or not os.path.isfile(dbiConfFileName) :
		abs = os.path.abspath(dbiConfFileName)
		err = "Unable to find dbi.conf at:" + os.linesep + \
			  "Original " + str(dbiConfFileName) + os.linesep + \
			  "Absolute " + str(abs) + os.linesep + \
			  "CWMS Home " + home
		raise Exception(err)
	pwf = PasswordFile(dbiConfFileName, False)
	try :
		dbUrl = getCwmsDbiProperty("cwms.dbi.ConnectUsingUrl")
		connStr = dbUrl.split("@", 1)[-1]
		pwe = pwf.getEntry(connStr)
		results = (pwe.getUserName(), pwe.getPassword())
	finally :
		pwf.close()
	return results

def getConnectionFactory(dbUrl, dbUsr, dbPwd, dbOfc) :
	'''
	Returns a connection factory for the specified database, user, password, and office.
	'''
	factory = conn = None

	try    : dbPwd, dbUrl = getPassAndUrl(dbPwd, dbUsr, dbUrl) # in case dbPwd is actually a password file name
	except : pass

	dbConnection = DbConnection(DbConnection.DB_ORACLE)
	dbConnection.setDatabaseUrl(dbUrl.lower())
	dbConnection.setUserName(dbUsr)
	dbConnection.setSessionOfficeId(dbOfc)
	dbConnection.setShowSecurityLogin(False)
	cli = ConnectionLoginInfoImpl(dbUrl, dbUsr, dbPwd, dbOfc)
	factory = OracleDirectDataAccessFactory(dbConnection, cli)
	return factory

def getTimeWindow(*args) :
	'''
	Returns the time window specified in the arguments.
	'''
	errorMsg = 'Not a valid time window string: "%s"'
	twStr = ",".join(map(str, args))
	fields = list(map(string.strip, twStr.replace(",", " ").split()))
	t1 = HecTime()
	t2 = HecTime()
	s = ""
	try :
		#------------------------#
		# get the start time str #
		#------------------------#
		i = 0
		s += fields[i]
		if not fields[i].upper().startswith("T") :
			assert t1.set(fields[i]) == 0
			if fields[i+1].upper().startswith("T") or t1.set(fields[i+1]) == 0 :
				s += " 0000"
			else :
				i += 1
				s += " " + fields[i]
		#----------------------#
		# get the end time str #
		#----------------------#
		i += 1
		s += " " + fields[i]
		if not fields[i].upper().startswith("T") :
			assert t1.set(fields[i]) == 0
			if i == len(fields)-1 or t1.set(fields[i+1]) == 0 :
				s += " 2400"
			else :
				i += 1
				s += " " + fields[i]
	except Exception as e:
		raise ValueError(errorMsg % twStr)

	if HecTime.getTimeWindow(s, t1, t2) == 0:
		return t1, t2
	else:
		raise ValueError(errorMsg % twStr)

def removeConnectionFactoryInstance() :
	'''
	Removes the current connection factory instance so it won't automatically
	be used by the next zero-argument call to DBAPI.open()
	'''
	try:
		db_connection = ConnectionFactory.getInstances().iterator().next().getDbConnection()
		ConnectionFactory.removeInstance(db_connection)
	except:
		pass

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

class DbAccess :

	'''
	Class that parallels the DssFile object for access to the
	database via the database API, plus extra functionality.
	'''

	def __init__(self, *args, **kwargs) :
		'''
		Construct a DbAccess object.
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
				m = re.match(r"(\w+)/(.+)@(.+/.+)", self._url)
				if m is not None :
					self._user, self._pass, self._url = m.groups()
					if argsLen > 1 : self._officeId     = args[1]
					if argsLen > 2 : self._startTimeStr = args[2]
					if argsLen > 3 : self._endTimeStr   = args[3]
					if argsLen > 4 : raise ValueError("Invalid parameters to DBAccess constructor")
				else :
					if argsLen > 1 : self._user         = args[1]
					if argsLen > 2 : self._pass         = args[2]
					if argsLen > 3 : self._officeId     = args[3]
					if argsLen > 4 : self._startTimeStr = args[4]
					if argsLen > 5 : self._endTimeStr   = args[5]
					if argsLen > 6 : raise ValueError("Invalid parameters to DBAccess constructor")
			#-------------------------------#
			# process the keyword arguments #
			#-------------------------------#
			for key in list(kwargs.keys()) :
				if key == "url" :
					if self._url : raise ValueError("URL already specified in positional parameters")
					self._url = kwargs[key]
				elif key == "username" :
					if self._user : raise ValueError("User name already specified in positional parameters")
					self._user = kwargs[key]
				elif key == "password" :
					if self._pass : raise ValueError("Password already specified in positional parameters")
					self._pass = kwargs[key]
				elif key == "office" :
					if self._officeId : raise ValueError("Office already specified in positional parameters")
					self._officeId = kwargs[key]
				elif key == "startTime" :
					if self._startTimeStr : raise ValueError("Start time already specified in positional parameters")
					self._startTimeStr = kwargs[key]
				elif key == "endTime" :
					if self._endTimeStr : raise ValueError("End time already specified in positional parameters")
					self._endTimeStr = kwargs[key]
				else :
					raise ValueError('Invalid keyword parameter "%s"' % key)
			#---------------------------------#
			# validate parameter combinations #
			#---------------------------------#
			if (self._user is None) != (self._pass is None) :
				raise ValueError("User name and password must be specified together.")
			if self._url and not self._user :
				raise ValueError("User name and password must be specified if database url is specified.")
			if self._url and not self._officeId :
				raise ValueError("User office must be specified if database url is specified.")
			if (self._startTimeStr is None) != (self._endTimeStr is None) :
				raise ValueError("Start time and end time must be specified together.")
			#----------------------------------#
			# set the time window if specified #
			#----------------------------------#
			if self._startTimeStr : self.setTimeWindow(self._startTimeStr, self._endTimeStr)
			#----------------------------------#
			# finally, connect to the database #
			#----------------------------------#
			if self._user :
				if self._url :
					self._factory = getConnectionFactory('jdbc:oracle:thin:@%s' % self._url, self._user, self._pass, self._officeId)
					self._connectionMethod = CREDENTIALS_SPECIFIED_1
				else :
					self._factory = getConnectionFactory(getCwmsDbiProperty("cwms.dbi.ConnectUsingUrl"), self._user, self._pass, self._officeId)
					self._connectionMethod = CREDENTIALS_SPECIFIED_2
			else :
				try :
					#----------------------#
					# try networked client #
					#----------------------#
					loginState = ServerSuiteUtil.getState()
					if loginState == ServerLoginState.LOGGED_IN :
						self._factory = ServerSuiteUtil.getDataAccessFactory()
						if self._factory is not None :
							if self._factory.getDbConnection().getPluginKey() == DbConnection.DB_RADAR:
								self._factory = None
							else:
								self._connectionMethod = SERVER_SUITE_WO_DIALOG
					if self._factory is None:
						try :
							factories = ConnectionFactory.getInstances().iterator()
							for factory in factories:
								if factory.getDbConnection().getPluginKey() != DbConnection.DB_RADAR:
									self._factory = factory
									self._connectionMethod = NETWORKED_CLIENT
						except :
							pass
				except :
					pass
				if self._factory is None :
					dbUsr = System.getProperty("user.name")
					dbPwd = None
					try :
						dbOfc = getCwmsDbiProperty("cwms.dbi.OfficeId")
						isCwmsUser = len(dbUsr) == 8 \
							and (dbUsr[2:6] == "cwms" or dbUsr[2:5] in ("cwp", "cwt")) \
							and dbUsr[0].isalpha() \
							and dbUsr[1].isdigit() \
							and dbUsr == getCwmsHome().split(os.sep)[-1]
						if isCwmsUser :
							#-----------------------------#
							# try CWMS production account #
							#-----------------------------#
							dbUsr, dbPwd = getDbiDbUserAndPass()
							dbUrl = getCwmsDbiProperty("cwms.dbi.ConnectUsingUrl")
							self._factory = getConnectionFactory(dbUrl, dbUsr, dbPwd, dbOfc)
							if self._factory is not None : self._connectionMethod = CWMS_PRODUCTION_ACCOUNT
						else :
							#--------------------------#
							# try CWMS service account #
							#--------------------------#
							try :
								dbUrl = getCwmsDbiProperty("cwms.dbi.ConnectUsingUrl")
								pwfName = "%s/dbi.conf" % System.getProperty("user.home")
								try    : dbPwd, dbUrl = getPassAndUrl(pwfName, dbUsr, dbUrl)
								except : pass
								self._factory = getConnectionFactory(dbUrl, dbUsr, dbPwd, dbOfc)
								if self._factory is not None : self._connectionMethod = CWMS_SERVICE_ACCOUNT
							except :
								pass
					except :
						pass
					if self._factory is None :
						#---------------------------#
						# finally, use login dialog #
						#---------------------------#
						try    : UIManager.setLookAndFeel("com.sun.java.swing.plaf.windows.WindowsLookAndFeel")
						except : pass
						ServerSuiteUtil.login(APP_NAME, False)
						loginState = ServerSuiteUtil.getState()
						if loginState == ServerLoginState.LOGGED_IN :
							self._factory = ServerSuiteUtil.getDataAccessFactory()
							if self._factory is not None : self._connectionMethod = SERVER_SUITE_W_DIALOG
			self._isOpen = self._factory is not None
			if self._isOpen :
				conn = self.getConnection()
				md = conn.getMetaData()
				self._connectionInfo = "%s %s" % (md.getDatabaseProductVersion().split("\n")[0], md.getURL().split("@", 1)[-1])
				stmt = conn.prepareStatement("select cwms_util.get_user_id, cwms_util.get_db_office_id from dual")
				rs = stmt.executeQuery()
				rs.next()
				self._user = rs.getString(1)
				self._officeId = rs.getString(2)
				rs.close()
				stmt.close()
				conn.close()
				print("Connected to %s as user %s, office %s, via %s" % (self._connectionInfo, self._user, self._officeId, self._connectionMethod))
			else :
				raise Exception("Could not connect to database")
		finally :
			self._clientLock.release()

	def _initValues_(self) :
		'''
		Initialize the member variable values.
		'''
		self._isOpen                = False
		self._url                   = None
		self._user                  = None
		self._pass                  = None
		self._factory               = None
		self._connectionMethod      = None
		self._connectionInfo        = None
		self._startTimeStr          = None
		self._startTime             = None
		self._endTimeStr            = None
		self._endTime               = None
		self._officeId              = None
		self._unitSystem            = "English"
		self._storeRule             = Const.Replace_All
		self._utcTimeZone           = TimeZone.getTimeZone("UTC")
		self._timeZone              = self._utcTimeZone
		self._trim                  = True
		self._startTimeInclusive    = True
		self._endTimeInclusive      = True
		self._getPrevious           = False
		self._getNext               = False
		self._overrideProtection    = False
		self._maxVersion            = True
		self._parameterUnits        = None
		self._ratingLoadMethod      = "LAZY"
		self._utcCal                = Calendar.getInstance()
		self._sdf                   = SimpleDateFormat('yyyy/MM/dd HHmm')
		self._sdfDATE               = SimpleDateFormat('yyyy-MM-dd HH:mm:ss')
		self._hts                   = HecTime(HecTime.SECOND_INCREMENT)
		self._canWrite              = {}
		self._utcCal.setTimeZone(self._utcTimeZone)
		self._sdf.setTimeZone(self._utcTimeZone)
		self._catalogTsSql = ' '.join('''
			select cwms_ts_id
			  from cwms_v_ts_id where db_office_id = :1
			   and upper(cwms_ts_id) like cwms_util.normalize_wildcards(upper(:2)) escape '\\'
			'''.strip().split())
		self._catalogRatingSql = ' '.join('''
			select distinct rating_id
			  from cwms_v_rating where office_id = :1
			   and upper(rating_id) like cwms_util.normalize_wildcards(upper(:2)) escape '\\'
			   and parent_rating_code is null
			'''.strip().split())
		self.resetVersionDate()

	def lock(self) :
		'''
		Lock the client lock
		'''
		self._clientLock.acquire()

	def unlock(self) :
		'''
		Unlock the client lock
		'''
		self._clientLock.release()

	def isOpen(self) :
		'''
		Return whether we can use the dbapi object
		'''
		self.lock()
		try     : return self._isOpen
		finally : self.unlock()

	def getConnection(self) :
		'''
		Get a database connection for external use.

		It is the responsibility of the  external user to close the
		connection to return it to the connection pool
		'''
		self.lock()
		try     : return self._factory.getPooledConnection()
		finally : self.unlock()

	def canWrite(self) :
		'''
		Determine if we are connected to a national database and using a user that can't write to
		the database.
		'''
		self.lock()
		try:
			writePermission = False
			
			if self.isOpen():
				writePermission = True
				try :
					if self._url in self._canWrite :
						# short circuit if we've already memoized this URL
						return self._canWrite[self._url]
					conn = self.getConnection()
					try:
						# get the IP address from the database since the URL might be a compound TNS spec
						rs = conn.prepareStatement("select cwms_util.get_db_host from dual").executeQuery()
						rs.next()
						host_ip = rs.getString(1)
						rs.close()
						# handle IP v6 addresses
						if host_ip.find(":") != -1 :
							# IP v6 comes back with extra info
							host = ":".join(host_ip.split(":")[:-1])
							# expand to full IP v6 address
							host_ip = socket.gethostbyaddr(host)[0]
						# see if we have write permission
						for data in NATIONAL_DB_DATA:
							if host_ip == data.ip:
								writePermission = self._user in data.users
								break
					except :
						traceback.print_exc()
					finally :
						conn.close()
				finally :
					# memoize URL for future
					self._canWrite[self._url] = writePermission
		finally:
			self.unlock()
		return writePermission

	def getStartTime(self) :
		'''
		Get the default start time string
		'''
		self.lock()
		try     : return self._startTimeStr
		finally : self.unlock()

	def getEndTime(self) :
		'''
		Get the default start time string
		'''
		self.lock()
		try     : return self._endTimeStr
		finally : self.unlock()

	def getFileName(self) :
		'''
		Get the connection information
		'''
		self.lock()
		try     : return self._connectionInfo
		finally : self.unlock()

	def setTrimMissing(self, state) :
		'''
		Set the default time series trim state
		'''
		self.lock()
		try     : self._trim = bool(state)
		finally : self.unlock()

	def getTrimMissing(self) :
		'''
		Get the default time series trim state
		'''
		self.lock()
		try     : return self._trim
		finally : self.unlock()

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

	def getTimeZone(self) :
		'''
		Get the default time zone
		'''
		self.lock()
		try     : return self._timeZone
		finally : self.unlock()

	def getTimeZoneName(self) :
		'''
		Get the default time zone
		'''
		self.lock()
		try     : return self._timeZone.getID()
		finally : self.unlock()

	def setStartTimeInclusive(self, state) :
		'''
		Set the default start time inclusive state
		'''
		self.lock()
		try     : self._startTimeInclusive = bool(state)
		finally : self.unlock()

	def getStartTimeInclusive(self) :
		'''
		Get the default start time inclusive state
		'''
		self.lock()
		try     : return self._startTimeInclusive
		finally : self.unlock()

	def setEndTimeInclusive(self, state) :
		'''
		Set the default end time inclusive state
		'''
		self.lock()
		try     : self._endTimeInclusive = bool(state)
		finally : self.unlock()

	def getEndTimeInclusive(self) :
		'''
		Get the default end time inclusive state
		'''
		self.lock()
		try     : return self._endTimeInclusive
		finally : self.unlock()

	def setRetrievePrevious(self, state) :
		'''
		Set the default time series get previous state
		'''
		self.lock()
		try     : self._getPrevious = bool(state)
		finally : self.unlock()

	def getRetrievePrevious(self, state) :
		'''
		Get the default time series get previous state
		'''
		self.lock()
		try     : return self._getPrevious
		finally : self.unlock()

	def setRetrieveNext(self, state) :
		'''
		Set the default time series get next state
		'''
		self.lock()
		try     : self._getNext = bool(state)
		finally : self.unlock()

	def getRetrieveNext(self) :
		'''
		Get the default time series get next state
		'''
		self.lock()
		try     : return self._getNext
		finally : self.unlock()

	def setRetrieveMaxVersionDate(self, state) :
		'''
		Set the default time series get max version state
		'''
		self.lock()
		try     : self._maxVersion = bool(state)
		finally : self.unlock()

	def getRetrieveMaxVersionDate(self, state) :
		'''
		Get the default time series get max version state
		'''
		self.lock()
		try     : return self._maxVersion
		finally : self.unlock()

	def setVersionDate(self, dateStr=None) :
		'''
		Set the default time series version date
		'''
		self.lock()
		try :
			if dateStr and not isNonVersioned(dateStr) :
				self._hts.set(dateStr)
				self._sdfDATE.setTimeZone(self._utcTimeZone)
				self._versionDate = self._sdfDATE.format(self._hts.getTimeInMillis())
			else :
				self._versionDate = dateStr
		finally :
			self.unlock()

	def resetVersionDate(self) :
		'''
		Resets the default time series version date to "non-versioned"
		'''
		self.setVersionDate('')

	def unsetVersionDate(self) :
		'''
		Resets the default time series version date to "non-versioned"
		'''
		self.resetVersionDate()

	def getVersionDate(self) :
		'''
		Get the default time series version date
		'''
		self.lock()
		try     : return self._versionDate
		finally : self.unlock()

	def setOverrideProtection(self, state) :
		'''
		Set the default override protection version state
		'''
		self.lock()
		try     : self._overrideProtection = bool(state)
		finally : self.unlock()

	def getOverrideProtection(self) :
		'''
		Get the default override protection version state
		'''
		self.lock()
		try     : return self._overrideProtection
		finally : self.unlock()

	def setTimeWindow(self, *args) :
		'''
		Set the default time window for future database accesses.
		'''
		try :
			self._startTime, self._endTime = getTimeWindow(*args)
		except :
			self._startTimeStr = None
			self._endTimeStr = None
		else :
			self._startTime.showTimeAsBeginningOfDay(True)
			self._startTimeStr = self._startTime.dateAndTime(4)
			self._endTimeStr = self._endTime.dateAndTime(4)

	def getTimeWindow(self) :
		'''
		Get the default time window for future database accesses.
		'''
		self.lock()
		try     : return self._startTimeStr, self._endTimeStr
		finally : self.unlock()

	def setUnitSystem(self, system) :
		'''
		Set the working units system to SI or English, used for reads only.
		'''
		self.lock()
		try :
			if system.lower() == "english" :
				self._unitSystem = "English"
			elif system.lower() == "si" :
				self._unitSystem = "SI"
			else :
				raise ValueError("System must be Enlish or SI.")
		finally :
			self.unlock()

	def getUnitSystem(self) :
		'''
		Get the working units system to SI or English, used for reads only.
		'''
		self.lock()
		try     : return self._unitSystem
		finally : self.unlock()

	def setStoreRule(self, storeRule) :
		'''
		Sets the default store rule, used for writes only.
		'''
		self.lock()
		try     : self._storeRule = storeRule
		finally : self.unlock()

	def getStoreRule(self) :
		'''
		Gets the default store rule, used for writes only.
		'''
		self.lock()
		try     : return self._storeRule
		finally : self.unlock()

	def setRatingLoadMethod(self, loadMethodStr) :
		'''
		Sets the default rating load method.
		'''
		if loadMethodStr.upper() in ("EAGER", "LAZY", "REFERENCE") :
			self._ratingLoadMethod = loadMethodStr.upper()
		else :
			raise ValueError('Invalid rating load method: "%s"' % loadMethodStr)

	def getRatingLoadMethod(self) :
		'''
		Gets the default rating load method.
		'''
		return self._ratingLoadMethod

	def setOfficeId(self, officeId) :
		'''
		Set the default office ID
		'''
		self.lock()
		try     :
			_officeId = officeId.upper()
			conn = self.getConnection()
			try :
				stmt = conn.prepareCall("begin cwms_env.set_session_office_id(:1); end;")
				stmt.setString(1, _officeId)
				stmt.execute()
				stmt.close()
				conn.commit()
				self._officeId = _officeId
			finally :
				conn.close()
		finally :
			self.unlock()

	def getOfficeId(self) :
		'''
		Get the default office ID
		'''
		self.lock()
		try :
			if self._officeId is None :
				conn = self.getConnection()
				try :
					stmt = conn.prepareCall("begin :1 := cwms_util.user_office_id; end;")
					stmt.registerOutParameter(1, Types.VARCHAR)
					stmt.execute()
					self._officeId = stmt.getString(1)
					stmt.close()
				finally :
					conn.close()
			return self._officeId
		finally :
			self.unlock()

	def setDefaultVerticalDatum(self, DefaultVertDatum) :
		'''
		Set the default vertical datum for elevations
		'''
		self.lock()
		try :
			conn = self.getConnection()
			try :
				stmt = conn.prepareCall("begin cwms_loc.set_default_vertical_datum(:1); end;")
				stmt.setString(1, DefaultVertDatum)
				stmt.execute()
				stmt.close()
				conn.commit()
			finally :
				conn.close()
		finally :
			self.unlock()

	def getDefaultVerticalDatum(self) :
		'''
		Retrieve the default vertical datum for elevations
		'''
		self.lock()
		try :
			conn = self.getConnection()
			try :
				stmt = conn.prepareCall("begin :1 := cwms_loc.get_default_vertical_datum; end;")
				stmt.registerOutParameter(1, Types.VARCHAR)
				stmt.execute()
				data = stmt.getString(1)
				stmt.close()
				return data
			finally :
				conn.close()
		finally :
			self.unlock()

	def getLocationVerticalDatum(self, locationId, officeId=None) :
		'''
		Retrieve the identified vertical datum for a location
		'''
		self.lock()
		try :
			conn = self.getConnection()
			try :
				stmt = conn.prepareCall("begin :1 := cwms_loc.get_location_vertical_datum(:2, :3); end;")
				stmt.registerOutParameter(1, Types.VARCHAR)
				stmt.setString(2, locationId)
				stmt.setString(3, (officeId, self._officeId)[officeId is None])
				stmt.execute()
				data =  stmt.getString(1)
				stmt.close()
				return data
			finally :
				conn.close()
		finally :
			self.unlock()

	def setVerticalDatumOffset(self, locationId, verticalDatumId1, verticalDatumId2, value, unit) :
		'''
		Set the offset from vertical datum id 1 to vertical datum id 2 at a location
		'''
		self.lock()
		try :
			conn = self.getConnection()
			try :
				stmt = conn.prepareCall('''
					begin
					   cwms_loc.store_vertical_datum_offset(
					      p_location_id          => :1,
					      p_vertical_datum_id_1  => :2,
					      p_vertical_datum_id_2  => :3,
					      p_offset               => :4,
					      p_unit                 => :5,
					      p_office_id            => :6);
					end;''')
				stmt.setString(1, locationId)
				stmt.setString(2, verticalDatumId1)
				stmt.setString(3, verticalDatumId2)
				stmt.setDouble(4, value)
				stmt.setString(5, unit)
				stmt.setString(6, self.getOfficeId())
				stmt.execute()
				stmt.close()
				conn.commit()
			finally :
				conn.close()
		finally :
			self.unlock()

	def getVerticalDatumOffset1(self, locationId, verticalDatumId1, verticalDatumId2, unit, officeId=None) :
		'''
		Retrieve the offset from vertical datum id 1 to vertical datum id 2 at a location
		'''
		self.lock()
		try :
			conn = self.getConnection()
			try :
				stmt = conn.prepareCall('''
					begin
					   :1 := cwms_loc.get_vertical_datum_offset(
					      p_location_id          => :2,
					      p_vertical_datum_id_1  => :3,
					      p_vertical_datum_id_2  => :4,
					      p_unit                 => :5,
					      p_office_id            => :6);
					end;''')
				stmt.registerOutParameter(1, Types.DOUBLE)
				stmt.setString(2, locationId)
				stmt.setString(3, verticalDatumId1)
				stmt.setString(4, verticalDatumId2)
				stmt.setString(5, unit)
				stmt.setString(6, (officeId,self.getOfficeId())[officeId is None])
				stmt.execute()
				data = stmt.getDouble(1)
				stmt.close()
				return data
			finally :
				conn.close()
		finally :
			self.unlock()

	def getVerticalDatumOffset2(self, locationId, unit=None, officeId=None) :
		'''
		Retrieve the offset from the location's identified vertical datum to the default
		vertical datum or datum specified in the unit spec (if any)
		'''
		self.lock()
		try :
			conn = self.getConnection()
			try :
				stmt = conn.prepareCall('''
					begin
					   :1 := cwms_loc.get_vertical_datum_offset(
					      p_location_id          => :2,
					      p_unit                 => :3,
					      p_office_id            => :4);
					end;''')
				stmt.registerOutParameter(1, Types.DOUBLE)
				if not unit : unit = ('m','ft')[self._unitSystem == "English"]
				stmt.setString(2, locationId)
				stmt.setString(3, unit)
				stmt.setString(4, (officeId,self.getOfficeId())[officeId is None])
				stmt.execute()
				data = stmt.getDouble(1)
				stmt.close()
				return data
			finally :
				conn.close()
		finally :
			self.unlock()

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
			for argName in list(kwargs.keys()) :
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

	def _getParameterUnits_(self) :
		'''
		returns the _parameterUnits field, populating if necessary
		'''
		self.lock()
		try :
			if self._parameterUnits is None :
				conn = self.getConnection()
				try :
					stmt = conn.prepareStatement('''
						select parameter_id,
								 unit_system,
								 unit_id
						  from cwms_v_display_units
						 where office_id = :1'''.strip())
					stmt.setString(1, self.getOfficeId())
					rs = stmt.executeQuery()
					self._parameterUnits = {'EN' : {}, 'SI' : {}}
					while rs.next() :
						param      = rs.getString(1)
						unitSystem = rs.getString(2)
						unit       = rs.getString(3)
						self._parameterUnits[unitSystem[:2].upper()][param] = unit
					rs.close()
					stmt.close()
				finally :
					conn.close()
			return self._parameterUnits
		finally :
			self.unlock()

	def unitsForParameter(self, parameter) :
		units = None
		try :
			units =  self._getParameterUnits_()[self._unitSystem[:2].upper()][parameter]
		except :
			if '-' in parameter :
				try :
					units = self._getParameterUnits_()[self._unitSystem[:2].upper()][parameter.split('-')[0]]
				except :
					pass
		if not units :
			conn = self.getConnection()
			try :
				stmt = conn.prepareStatement(
					"select cwms_util.get_default_units('%s','%s') from dual" % \
					(parameter, self._unitSystem[:2].upper()))
				rs = stmt.executeQuery()
				if rs :
					rs.next()
					units = rs.getString(1)
					self._parameterUnits[self._unitSystem[:2].upper()][parameter] = units
					rs.close()
				stmt.close()
			finally :
				conn.close()
		if not units : raise Exception("Could not determine database units for parameter: %s" % parameter)
		return units

	def _getTimeSeriesExtents_(self, tsId) :
		'''
		Returns the time series extents for a time series id
		'''
		self.lock()
		#-------------------------------------------------------------------#
		# getting min and max in same statement is fast enough, but using a #
		# sub-select for the ts code forces a full table scan!              #
		#-------------------------------------------------------------------#
		tsCode = None
		try :
			conn = self.getConnection()
			try :
				stmt = conn.prepareStatement(
					'''
					select ts_code
					  from cwms_v_ts_id
					 where db_office_id = :1
					   and upper(cwms_ts_id) = upper(:2)
					'''.strip())
				stmt.setString(1, self.getOfficeId())
				stmt.setString(2, tsId)
				rs = stmt.executeQuery()
				try :
					rs.next()
					if rs.isAfterLast() :
						raise ValueError("No such time series: %s" % tsId)
					tsCode = rs.getLong(1)
				finally :
					rs.close()
				stmt.close()
				stmt = conn.prepareStatement(
					'''
					select min(date_time), max(date_time)
					  from cwms_v_tsv
					 where ts_code = :1
					'''.strip())
				stmt.setLong(1, tsCode)
				rs = stmt.executeQuery()
				try :
					rs.next()
					if rs.isAfterLast() :
						raise ValueError("No data for time series: %s" % tsId)
					startDATE = rs.getDATE(1) # UTC
					endDATE = rs.getDATE(2)   # UTC
				finally :
					rs.close()
				stmt.close()

			finally :
				conn.close()
			#------------------------#
			# parse the dates as UTC #
			#------------------------#
			self._sdfDATE.setTimeZone(self._utcTimeZone)
			return \
				self._sdfDATE.parse("%s %s" % (startDATE.dateValue(), startDATE.timeValue())).getTime(), \
				self._sdfDATE.parse("%s %s" % (endDATE.dateValue(), endDATE.timeValue())).getTime()
		finally :
			self.unlock()

	def _getTimeSeries_(
		self,
		tsId,
		startTimeStr = None,
		endTimeStr = None,
		units = None,
		timeZone = None,
		trim = None,
		startTimeInclusive = None,
		endTimeInclusive = None,
		getPrevious = None,
		getNext = None,
		versionTimeStr = None,
		maxVersion = None,
		officeId = None) :
		'''
		Read a time-series from the database
		'''
		self.lock()
		try :
			#----------------------#
			# handle the arguments #
			#----------------------#
			if (not startTimeStr) != (not endTimeStr) :
				raise ValueError("Start time and end time must be specified together")
			if not startTimeStr : startTimeStr = self._startTimeStr
			if not endTimeStr   : endTimeStr   = self._endTimeStr
			if not startTimeStr or not endTimeStr :
				raise ValueError("No default or explicit time window")
			if not units :
				unitSystem = self._unitSystem[:2].upper()
				parameter = tsId.split(".")[1]
				units = self._getParameterUnits_()[unitSystem][parameter]
			if timeZone is not None :
				timeZone = TimeZone.getTimeZone(timeZone)
			else :
				timeZone = self._timeZone
			if trim is not None :
				trim = bool(trim)
			else :
				trim = self._trim
			if startTimeInclusive is not None :
				startTimeInclusive = bool(startTimeInclusive)
			else :
				startTimeInclusive = self._startTimeInclusive
			if endTimeInclusive is not None :
				endTimeInclusive = bool(endTimeInclusive)
			else :
				endTimeInclusive = self._endTimeInclusive
			if getPrevious is not None :
				getPrevious = bool(getPrevious)
			else :
				getPrevious = self._getPrevious
			if getNext is not None :
				getNext = bool(getNext)
			else :
				getNext = self._getNext
			if versionTimeStr is None :
				versionTimeStr = self._versionDate
			if maxVersion is not None :
				maxVersion = bool(maxVersion)
			else :
				maxVersion = self._maxVersion
			if officeId is not None :
				officeId = officeId.upper()
			else :
				officeId = self.getOfficeId()
			startTimeStr = startTimeStr.replace(":", "").replace(",", "")
			endTimeStr = endTimeStr.replace(":", "").replace(",", "")

			conn = self.getConnection()
			try :
				#------------------#
				# prepare the call #
				#------------------#
				stmt = conn.prepareCall('''
					begin
						cwms_ts.retrieve_ts(
							:1,                                 -- P_AT_TSV_RC       OUT SYS_REFCURSOR
							:2,                                 -- P_CWMS_TS_ID      IN  VARCHAR2
							:3,                                 -- P_UNITS           IN  VARCHAR2
							to_date(:4, 'yyyy/mm/dd hh24mi'),   -- P_START_TIME      IN  DATE
							to_date(:5, 'yyyy/mm/dd hh24mi'),   -- P_END_TIME        IN  DATE
							:6,                                 -- P_TIME_ZONE       IN  VARCHAR2   'UTC'
							:7,                                 -- P_TRIM            IN  VARCHAR2   'F'
							:8,                                 -- P_START_INCLUSIVE IN  VARCHAR2   'T'
							:9,                                 -- P_END_INCLUSIVE   IN  VARCHAR2   'T'
							:10,                                -- P_PREVIOUS        IN  VARCHAR2   'F'
							:11,                                -- P_NEXT            IN  VARCHAR2   'F'
							to_date(:12, 'yyyy-mm-dd hh24miss'),-- P_VERSION_DATE    IN  DATE       NULL
							:13,                                -- P_MAX_VERSION     IN  VARCHAR2   'T'
							:14);                               -- P_OFFICE_ID       IN  VARCHAR2   NULL
					end;'''.strip())
				#-------------------------#
				# set the call parameters #
				#-------------------------#
				startTime = self._sdf.format(Date(RMAIO.parseDate(startTimeStr, self._utcTimeZone)))
				endTime = self._sdf.format(Date(RMAIO.parseDate(endTimeStr, self._utcTimeZone)))
				if not versionTimeStr :
					versionTime = ''
				else :
					if isNonVersioned(versionTimeStr) :
						versionTime = '1111-11-11 000000'
					else :
						self._hts.set(versionTimeStr)
						self._sdfDATE.setTimeZone(self._utcTimeZone)
						versionTime = self._sdfDATE.format(self._hts.getTimeInMillis()).replace(":", "").replace(",", "")
				state = ('F','T')
				stmt.registerOutParameter(1, OracleTypes.CURSOR)
				stmt.setString(2, tsId)
				stmt.setString(3, units)
				stmt.setString(4, startTime)
				stmt.setString(5, endTime)
				stmt.setString(6, timeZone.getID())
				stmt.setString(7, state[trim])
				stmt.setString(8, state[startTimeInclusive])
				stmt.setString(9, state[endTimeInclusive])
				stmt.setString(10, state[getPrevious])
				stmt.setString(11, state[getNext])
				stmt.setString(12, versionTime)
				stmt.setString(13, state[maxVersion])
				stmt.setString(14, officeId)
				#----------------------------------------#
				# execute the call and retrieve the data #
				#----------------------------------------#
				times, values, qualities = [], [], []
				self._sdfDATE.setTimeZone(timeZone)
				stmt.execute()
				rs = stmt.getCursor(1)
				try :
					while rs.next() :
						d = rs.getDATE(1)
						if d is None: break
						value = rs.getDouble(2)
						if rs.wasNull() : value = Const.UNDEFINED_DOUBLE
						quality = rs.getInt(3)
						times.append(self._sdfDATE.parse("%s %s" % (d.dateValue(), d.timeValue())).getTime())
						values.append(value)
						qualities.append(quality)
				finally:
					rs.close()
				stmt.close()
				self._sdf.setTimeZone(timeZone)
				startTime = self._sdf.parse(startTime).getTime()
				endTime = self._sdf.parse(endTime).getTime()
				self._sdf.setTimeZone(self._utcTimeZone)
				#--------------------------------------------#
				# get the vertical datum info for elevations #
				#--------------------------------------------#
				loc, param, paramType, intvl, dur, ver = tsId.split('.')
				if param.upper().startswith('ELEV') :
					stmt = conn.prepareCall('''
						begin
						:1 := cwms_loc.get_vertical_datum_info_f(:2, :3, :4);
						end;''')
					stmt.registerOutParameter(1, Types.VARCHAR)
					stmt.setString(2, loc)
					stmt.setString(3, units)
					stmt.setString(4, officeId)
					stmt.execute()
					vertDatumInfo = stmt.getString(1)
					stmt.close()
				else :
					vertDatumInfo = None
				return times, values, qualities, units, startTime, endTime, timeZone, vertDatumInfo
			finally :
				conn.close()
		finally :
			self.unlock()

	def _putTimeSeries_(
		self,
		tsId,
		times,
		values,
		qualities,
		units,
		storeRule,
		overrideProtection,
		versionTimeStr,
		officeId) :
		'''
		Write a time-series to the database
		'''
		self.lock()
		try :
			conn = self.getConnection()
			try :
				#------------------#
				# prepare the call #
				#------------------#
				stmt = conn.prepareCall('''
					declare
						l_cwms_ts_id     varchar2(256) := :1;
						l_units          varchar2(32)  := :2;
						l_times_clob     clob          := :3;
						l_values_clob    clob          := :4;
						l_qualities_clob clob          := :5;
						l_store_rule     varchar2(32)  := :6;
						l_override_prot  varchar2(1)   := :7;
						l_version_date   varchar2(32)  := :8;
						l_office_id      varchar2(16)  := :9;
						l_time_zone      varchar2(28)  := :10;
						l_times          cwms_t_str_tab;
						l_values         cwms_t_str_tab;
						l_qualities      cwms_t_str_tab;
						l_tsv_array      cwms_t_tsv_array;
					begin
						if l_times_clob is null and l_values_clob is null then
							return;
						end if;
						if l_times_clob is null then
							cwms_err.raise('ERROR', 'No times specified');
						end if;
						if l_values_clob is null then
							cwms_err.raise('ERROR', 'No values specified');
						end if;
						l_times     := cwms_util.split_text(l_times_clob,     '|');
						l_values    := cwms_util.split_text(l_values_clob,    '|');
						l_qualities := cwms_util.split_text(l_qualities_clob, '|');
						if l_values.count != l_times.count then
							cwms_err.raise('ERROR', 'Different number of times and values');
						end if;
						if l_qualities.count != l_times.count then
							cwms_err.raise('ERROR', 'Different number of times and qualities');
						end if;

						l_tsv_array := cwms_t_tsv_array();
						l_tsv_array.extend(l_times.count);
						for i in 1..l_tsv_array.count loop
							l_tsv_array(i) := cwms_t_tsv(
								from_tz(cwms_util.to_timestamp(to_number(l_times(i))), 'UTC'),
								to_binary_double(l_values(i)),
								case
								when l_qualities is null then 0
								else nvl(to_number(l_qualities(i)), 0)
								end);
						end loop;

						cwms_ts.store_ts(
							l_cwms_ts_id,
							l_units,
							l_tsv_array,
							l_store_rule,
							l_override_prot,
							cwms_util.change_timezone(to_date(l_version_date, 'yyyy-mm-dd hh24miss'), l_time_zone, 'UTC'),
							l_office_id);
					end;'''.strip())
				#-------------------------#
				# set the call parameters #
				#-------------------------#
				if versionTimeStr is None : versionTimeStr = self._versionDate
				if isNonVersioned(versionTimeStr) :
					versionTime = ''
				else :
					self._hts.set(versionTimeStr)
					self._sdfDATE.setTimeZone(self._utcTimeZone)
					versionTime = self._sdfDATE.format(self._hts.getTimeInMillis()).replace(":", "").replace(",", "")
				_times = conn.createClob()
				_times.setString(1, "|".join(map(str, times)))
				_values = conn.createClob()
				_values.setString(1, "|".join(map(str, values)).replace(str(Const.UNDEFINED_DOUBLE), ""))
				_qualities = conn.createClob()
				_qualities.setString(1, "|".join(map(str, qualities)))
				stmt.setString(1, tsId)
				stmt.setString(2, units)
				stmt.setClob(3, _times)
				stmt.setClob(4, _values)
				stmt.setClob(5, _qualities)
				stmt.setString(6, storeRule)
				stmt.setString(7, ('F','T')[overrideProtection])
				stmt.setString(8, versionTime)
				stmt.setString(9, officeId)
				stmt.setString(10, self._timeZone.getID())
				#----------------#
				# store the data #
				#----------------#
				stmt.execute()
				conn.commit()
				for clob in _times, _values, _qualities :
					try    : clob.free()
					except : pass
				stmt.close()
			finally :
				conn.close()
		finally :
			self.unlock()

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
		times, values, qualities, units, startTime, endTime, timeZone, vertDatumInfo = self._getTimeSeries_(
			tsId,
			startTimeStr,
			endTimeStr,
			units,
			timeZone,
			trim,
			startInclusive,
			endInclusive,
			getPrevious,
			getNext,
			versionDate,
			maxVersion,
			officeId)
		#----------------#
		# parse the unit #
		#----------------#
		for part1 in map(string.strip, units.split('|')) :
			parts2 = list(map(string.strip, part1.split('=', 1)))
			if len(parts2) == 2 and parts2[0].upper() == 'U' :
				units = parts2[1]
				break
		#-----------------------------------------------------------------#
		# convert java millis into HecTime minutes in specified time zone #
		#-----------------------------------------------------------------#
		cal = Calendar.getInstance()
		cal.setTimeZone(timeZone)
		cal.clear()
		time = HecTime()
		i = 0
		while i < len(times) :
			cal.setTimeInMillis(times[i])
			time.set(cal)
			times[i] = time.value()
			i += 1
		cal.setTimeInMillis(startTime)
		time.set(cal)
		startTime = time.value()
		cal.setTimeInMillis(endTime)
		time.set(cal)
		endTime = time.value()
		#-----------------------------------------------#
		# build and return a TimeSeriesContainer object #
		#-----------------------------------------------#
		tsc = TimeSeriesContainer()
		loc, param, paramType, intvl, dur, ver = tsId.split('.')
		tsc.watershed = ''
		tsc.fullName  = tsId
		try    : tsc.location,  tsc.subLocation  = loc.split('-', 1)
		except : tsc.location,  tsc.subLocation  = loc, ''
		try    : tsc.parameter, tsc.subParameter = param.split('-', 1)
		except : tsc.parameter, tsc.subParameter = param, ''
		try    : tsc.version,   tsc.subVersion   = ver.split('-', 1)
		except : tsc.version,   tsc.subVersion   = ver, ''
		tsc.startTime         = startTime
		tsc.endTime           = endTime
		tsc.times             = times
		tsc.values            = values
		tsc.quality           = qualities
		tsc.numberValues      = len(times)
		tsc.interval          = Interval(intvl).getMinutes()
		tsc.type              = DSSTimeSeries.getDSSType(
		                        	Parameter(param),
		                        	ParameterType(paramType))
		tsc.units             = units
		tsc.timeZoneID        = timeZone.getID()
		tsc.timeZoneRawOffset = timeZone.getRawOffset()
		if vertDatumInfo :
			tsc = TimeSeriesContainerVertDatum(
				tsc,
				VerticalDatumContainer(vertDatumInfo))
		return tsc

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
			conn = self.getConnection()
			try :
				raise Exception("Cannot write to database %s" % conn.getMetaData().getURL())
			finally :
				conn.close()
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
					print("Warning: Data specifies time zone as %s, storing as %s" % (tsc.timeZoneID, tzid))
			if tsc.timeZoneRawOffset and timeZone.getRawOffset() != tsc.timeZoneRawOffset :
				print("Warning: Data specifies raw offset %d, which is incompatible with time zone %s, using time zone" % (
					tsc.timeZoneRawOffset,
					tzid))
		else :
			if tsc.timeZoneID :
				tz = tsc.timeZoneID
				if   tz in ("EST", "CDT") : tz = "Etc/GMT+5"
				elif tz in ("CST", "MDT") : tz = "Etc/GMT+6"
				elif tz in ("MST", "PDT") : tz = "Etc/GMT+7"
				elif tz == "PST"          : tz = "Etc/GMT+8"
				timeZone = TimeZone.getTimeZone(tz)
				if tsc.timeZoneRawOffset and timeZone.getRawOffset() != tsc.timeZoneRawOffset :
					print("Warning: Data specifies time zone as %s (raw offset %d) and incompatible raw offset %d, using time zone" % (
						tsc.timeZoneID,
						timeZone.getRawOffset(),
						tsc.timeZoneRawOffset))
			elif tsc.timeZoneRawOffset :
				timeZone = SimpleTimeZone(tsc.timeZoneRawOffset, "Offset_%d" % tsc.timeZoneRawOffset)
			else :
				timeZone = self._timeZone
		if not storeRule : storeRule = self._storeRule
		if not overrideProtection : overrideProtection = self._overrideProtection
		if versionDate is None :
			versionDate = self._versionDate
		elif versionDate:
			versionDate = self._sdfDATE.format(Date(RMAIO.parseDate(versionDate, self._timeZone)))
		if not officeId : officeId = self._officeId
		times  = [0] * tsc.numberValues
		values = tsc.values
		if tsc.quality : qualities = tsc.quality
		else : qualities = [0] * tsc.numberValues
		#-----------------------------------------------------------------#
		# convert HecTime minutes into java millis in specified time zone #
		#-----------------------------------------------------------------#
		cal = Calendar.getInstance()
		cal.setTimeZone(timeZone)
		cal.clear()
		t = HecTime()
		i = 0
		while i < tsc.numberValues :
			t.set(tsc.times[i])
			cal.set(t.year(), t.month()-1, t.day(), t.hour(), t.minute(), t.second())
			times[i] = cal.getTimeInMillis()
			i += 1
		#--------------------------------#
		# store the data to the database #
		#--------------------------------#
		self._putTimeSeries_(
			tsc.fullName,
			times,
			values,
			qualities,
			tsc.units,
			storeRule,
			bool(overrideProtection),
			versionDate,
			officeId)

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

	def deleteTimeSeries(self, tsId) :
		'''
		delete time series (id and data) from the database
		'''
		if not self.canWrite() :
			conn = self.getConnection()
			try :
				raise Exception("Cannot write to database %s" % conn.getMetaData().getURL())
			finally :
				conn.close()
		self.lock()
		try :
			conn = self.getConnection()
			try :
				stmt = conn.prepareCall("begin cwms_ts.delete_ts(:1, cwms_util.delete_ts_cascade, :2); end;")
				stmt.setString(1, tsId)
				stmt.setString(2, self.getOfficeId())
				stmt.execute()
				conn.commit()
				stmt.close()
			finally :
				conn.close()
		finally :
			self.unlock()

	def refreshTsCatalog(self) :
		'''
		No-op retained for backward compatibility. Not needed in current schema
		'''
		pass

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
				startTime = self._utcCal.getTimeInMillis()
				self._utcCal.setTimeInMillis(RMAIO.parseDate(endTimeStr, self._utcTimeZone))
				endTime = self._utcCal.getTimeInMillis()
			else :
				startTime = None
				endTime = None

			if loadMethodStr is None : loadMethodStr = self._ratingLoadMethod
			if   loadMethodStr.upper() == "EAGER"     : loadMethod = RatingSet.DatabaseLoadMethod.EAGER
			elif loadMethodStr.upper() == "LAZY"      : loadMethod = RatingSet.DatabaseLoadMethod.LAZY
			elif loadMethodStr.upper() == "REFERENCE" : loadMethod = RatingSet.DatabaseLoadMethod.REFERENCE
			else  : raise ValueError('Invalid rating load method: "%s"' % loadMethodStr)

			conn = self.getConnection()
			try :
				try :
					#------------------------------------------------#
					# first try the static generator (CWMS <= 3.1.1) #
					#------------------------------------------------#
					ratingSet = RatingSet.fromDatabase(
						loadMethod,
						conn,
						self.getOfficeId(),
						ratingId,
						startTime,
						endTime)
				except AttributeError :
					#-----------------------------------------------------------#
					# if no static generator, use the constructor (CWMS >= 3.2) #
					#-----------------------------------------------------------#
					ratingSet = RatingSet(
						loadMethod,
						conn,
						self.getOfficeId(),
						ratingId,
						startTime,
						endTime)
				return ratingSet
			finally :
				conn.close()
		finally :
			self.unlock()

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

	def getRating(self,*args) :
		'''
		Retrieves a rating container from the database as a RatingSetContainer object
		'''
		if args[0].upper() in ("EAGER", "LAZY", "REFERENCE") or self._ratingLoadMethod == "EAGER" :
			#-------------------------------------------------------#
			# load method specified or default load method is EAGER #
			#-------------------------------------------------------#
			return self.readRating(*args).getData()
		else :
			#---------------------------------------------------------------#
			# no load method specified and default load method is not EAGER #
			#---------------------------------------------------------------#
			return self.readRating(*tuple(["EAGER"] + list(args))).getData()


	def storeRating(self, object, failIfExists=None) :
		'''
		Store a rating to the database
		'''
		if not self.canWrite() :
			conn = self.getConnection()
			try:
				raise Exception("Cannot write to database %s" % conn.getMetaData().getURL())
			finally :
				conn.close()
		if isinstance(object, RatingSet) :
			rating = object
		elif isinstance(object, RatingSetContainer) :
			rating = RatingSet(object)
		else :
			raise TypeError("Object is not a RatingSet or RatingSetContainer")
		if failIfExists is None : failIfExists = True
		if type(failIfExists) != type(True) :
			raise TypeError("Parameter failIfExists must be True or False")
		conn = self.getConnection()
		try :
			# rating.storeToDatabase(conn, failIfExists)
			overwrite_existing = True
			include_template = True
			RatingJdbcCompatUtil.getInstance().storeToDatabase(rating, conn, overwrite_existing, include_template)
		finally :
			conn.close()

	

	def deleteRatings(self, ratingIds) :
		'''
		delete ratings (ratingId and data) from the database
		'''
		if not self.canWrite() :
			conn = self.getConnection()
			try :
				raise Exception("Cannot write to database %s" % conn.getMetaData().getURL())
			finally :
				conn.close()
		if isinstance(ratingIds, basestring) :
			self.deleteRatings([ratingIds])
		else :
			self.lock()
			try :
				conn = self.getConnection()
				try :
					stmt = conn.prepareCall('begin cwms_rating.delete_specs(:1, cwms_util.delete_all, :2); end;')
					stmt.setString(2, self.getOfficeId())
					for ratingId in ratingIds :
						print("Deleting %s" % ratingId)
						stmt.setString(1, ratingId)
						stmt.execute()
					conn.commit()
					stmt.close()
				finally :
					conn.close()
			finally :
				self.unlock()

	def getCatalogedPathnames_1(self, pattern) :
		'''
		Returns the current catalog filtered by the specified pattern.

		NOTE: pattern must use glob chars (*, ?) and not SQL chars (% ,_)
		'''
		pathnameList = []
		if not isinstance(pattern, basestring) :
			raise TypeError('Pattern for getCatalogedPathnames must be a string')
		self.lock()
		try :
			conn = self.getConnection()
			try :
				stmt = conn.prepareStatement(self._catalogTsSql)
				stmt.setString(1, self.getOfficeId())
				stmt.setString(2, pattern)
				rs = stmt.executeQuery()
				try :
					while rs.next() : pathnameList.append(rs.getString(1))
				finally :
					rs.close()
				stmt.close()
				stmt = conn.prepareStatement(self._catalogRatingSql)
				stmt.setString(1, self.getOfficeId())
				stmt.setString(2, pattern)
				rs = stmt.executeQuery()
				try :
					while rs.next() : pathnameList.append(rs.getString(1))
				finally :
					rs.close()
				stmt.close()
				return pathnameList
			finally :
				conn.close()
		finally :
			self.unlock()

	def getCatalogedPathnames_2(self, forceNew) :
		'''
		Returns the current catalog

		NOTE: forceNew is now obsolete
		'''
		return self.getCatalogedPathnames_1('*')

	def getCatalogedPathnames_3(self, pattern, forceNew) :
		'''
		Returns the current catalog filtered by the specified pattern

		NOTE: pattern must use glob chars (*, ?) and not SQL chars (% ,_)

		NOTE: forceNew is now obsolete
		'''
		return self.getCatalogedPathnames_1(pattern)

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

	def getPathnameList(self) :
		'''
		Returns the current catalog
		'''
		return self.getCatalogedPathnames()

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
				meth = (self.get_1, self.get_2)['getEntireTimeWindow' in kwargs]
		if not meth :
			raise TypeError('get() too many arguments; expected 2 to 5, got %d' % count + 1)

		return meth(*args, **kwargs)

	def put(self, object, *args) :
		if not self.canWrite() :
			conn = self.getConnection()
			try :
				raise Exception("Cannot write to database %s" % conn.getMetaData().getURL())
			finally :
				conn.close()
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


	def read(self, id, startTimeStr=None, endTimeStr=None, units=None) :
		'''
		Read a time-series or a rating object from the database and return it as an HecMath object.
		'''
		if isTsId(id) :
			if not units : units = self.unitsForParameter(id.split('.')[1])
			return TimeSeriesMath(self.getTimeSeriesContainer(
				id,
				startTimeStr,
				endTimeStr,
				units))
		elif isRatingId(id) :
			if (units is not None) :
				raise ValueError("Cannot specify units for a rating")
			return self.readRating(id, startTimeStr, endTimeStr)
		else :
			raise ValueError('"%s" is not a time-series id or rating id.' % id)

	def write(self, object, *args) :
		if not self.canWrite() :
			conn = self.getConnection()
			try :
				raise Exception("Cannot write to database %s" % conn.getMetaData().getURL())
			finally :
				conn.close()
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

	def delete(self, ids) :
		'''
		Delete an object from the database
		'''
		if not self.canWrite() :
			conn = self.getConnection()
			try:
				raise Exception("Cannot write to database %s" % conn.getMetaData().getURL())
			finally :
				conn.close()
		if type(ids) not in (type([]), type(())) :
			self.delete([ids])
		else :
			for id in ids :
				if isTsId(id) : self.deleteTimeSeries(id)
				elif isRatingId(id) : self.deleteRatings(id)
				else : raise ValueError('"%s" is not a time-series or rating identifier.' % id)

	def commit(self) :
		'''
		This is now provided only to not break old code. Any method than stores or deletes
		database data peforms its own commit() before returning
		'''
		pass

	def close(self):
		'''
		Release any resources and make this object unusable.
		'''
		self.lock()
		try:
			if self._connectionMethod == CREDENTIALS_SPECIFIED_1 \
					or self._connectionMethod == CREDENTIALS_SPECIFIED_2 \
					or self._connectionMethod == CWMS_PRODUCTION_ACCOUNT \
					or self._connectionMethod == CWMS_SERVICE_ACCOUNT:
				removeConnectionFactoryInstance()
			elif self._connectionMethod == SERVER_SUITE_W_DIALOG:
				login_state = ServerSuiteUtil.getState()
				if login_state == ServerLoginState.LOGGED_IN:
					ServerSuiteUtil.logoff()

			self._initValues_()
		finally:
			self.unlock()

	def done(self) :
		'''
		Release any resources and make this object unusable.
		'''
		self.close()

	def __del__(self) :
		'''
		Destuctor (only called when last reference disappears)
		'''
		self.close()


##############################################################################

def open(*args, **kwargs) :
	'''
	Return a new DbAccess object.
	'''
	return DbAccess(*args, **kwargs)

def getVersion() :
	'''
	Returns the version of this module in the form "X.XX ddMMMyyyy"
	'''
	return "%s %s" % (VERSION_NUMBER, VERSION_DATE)

if __name__ == '__main__' : main()

