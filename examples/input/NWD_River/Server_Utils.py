'''
Author: Ryan Larsen
Last Updated: 04-28-2020
Description: Contains functions used in scripts on the server
'''

import platform
IS_CPYTHON = True if platform.python_implementation() == "CPython" else False

from decimal                import Decimal
from hec.heclib.util        import HecTime
from hec.io                 import TimeSeriesContainer
from hec.script             import Constants, AxisMarker
from hec.script.Constants   import TRUE, FALSE
from wcds.dbi.oracle        import CwmsDaoServiceLookup
from hec.db.cwms            import CwmsLocationDao, CwmsLevelDao
from mil.army.usace.hec.metadata.location import LocationTemplate
from hec.data.level         import JDomLocationLevelImpl
import inspect, math
from java.text              import SimpleDateFormat
import RADARAPI
import java.lang
import sys

#
# createBlankTimeSeries Function : Create a blank time series for plotting purposes
# Author/Editor                  : Ryan Larsen
# Last updated                   : 12-19-2018
#
def createBlankTimeSeries(  debug, 
                            Pathname,       # Generic pathname
                            Units,          # Units of values
                            DatetimeObj,    # Datetime object
                            EndTwStr,       # String of the end of time window
                            ) : 
    PathnameParts = Pathname.split('.')
    Location = PathnameParts[0]
    Parameter = PathnameParts[1]
    TypeStr = PathnameParts[2]
    if TypeStr == 'Total' : Type = 'PER-CUM'
    elif TypeStr == 'Ave' : Type = 'PER-AVER'
    elif TypeStr == 'Inst' : Type = 'INST-VAL'
    IntervalStr = PathnameParts[3]
    if IntervalStr == '1Day' : Interval = 60 * 24
    elif IntervalStr == '~1Day' : Interval = 0
    elif IntervalStr == '1Hour' : Interval = 60
    elif IntervalStr == '15Minutes' : Interval = 15
    Version = PathnameParts[-1]
    
    # Create times array
    HecTimeStartStr  = DatetimeObj.strftime('%d%b%Y ') + '2400'
    hecTime = HecTime(); hecTime.set(HecTimeStartStr)
    EndTime = HecTime(); EndTime.set(EndTwStr)
    times = [hecTime.value()]
    i = 0
    while times[-1] < EndTime.value() : 
        i += 1
        if i > 1000 : break
        hecTime.add(Interval)
        times.append(hecTime.value())

    Tsc = TimeSeriesContainer()                    
    Tsc.fullName       = Pathname
    Tsc.location       = Location
    Tsc.parameter      = Parameter
    Tsc.interval       = Interval
    Tsc.version        = Version
    Tsc.type           = Type
    Tsc.units          = Units
    values             = [Constants.UNDEFINED] * len(times)
    Tsc.times          = times
    Tsc.values         = values
    Tsc.quality        = [0] * len(values)
    Tsc.startTime      = times[0]
    Tsc.endTime        = times[-1]
    Tsc.numberValues   = len(values)
    
    return Tsc

#
# createPlot Function : Plot data with or without markers
# Author/Editor       : Ryan Larsen
# Last updated        : 12-10-2018
#
def createPlot( debug,               # Set to True or False to print debug statements
                CwmsDb,              # CWMS database connection
                Location,            # DCP ID of location
                plot,                # Plot
                layout,              # Plot layout
                ViewportLayoutsInfo, # Lists of ViewportLayouts, time series, and axis for each curve
                LocationProperties,  # Properties of locations
                CurveProperties,     # Properties of curves in the plot
                MarkerProperties,    # Properties of markers in the plot
                PlotProperties,      # Properties of plot
                ) :
    # Define the plot layout
    for x in range(len(ViewportLayoutsInfo)) :
        ViewportLayoutsInfo[x][0].addCurve(ViewportLayoutsInfo[x][2], ViewportLayoutsInfo[x][1])

    plot.configurePlotLayout(layout)
    plot.showPlot()
    
    # Set curve and symbol properties
    for x in range(len(ViewportLayoutsInfo)) :
        TscParts = ViewportLayoutsInfo[x][1].fullName.split('.')
        CurvePropertyKey = ViewportLayoutsInfo[x][3]

        if CurvePropertyKey is not None :
            curve = plot.getCurve(ViewportLayoutsInfo[x][1])
            curve.setLineColor(CurveProperties[CurvePropertyKey][0])
            curve.setLineStyle(CurveProperties[CurvePropertyKey][1])
            curve.setLineWidth(CurveProperties[CurvePropertyKey][2])
            curve.setFillType(CurveProperties[CurvePropertyKey][3])
            curve.setFillColor(CurveProperties[CurvePropertyKey][4])
            curve.setFillPattern(CurveProperties[CurvePropertyKey][5])
            curve.setSymbolsVisible(CurveProperties[CurvePropertyKey][6])
            curve.setSymbolType(CurveProperties[CurvePropertyKey][7])
            curve.setSymbolSize(CurveProperties[CurvePropertyKey][8])
            curve.setSymbolLineColor(CurveProperties[CurvePropertyKey][9])
            curve.setSymbolFillColor(CurveProperties[CurvePropertyKey][10])
            curve.setSymbolInterval(CurveProperties[CurvePropertyKey][11])
            curve.setSymbolSkipCount(CurveProperties[CurvePropertyKey][12])
            curve.setFirstSymbolOffset(CurveProperties[CurvePropertyKey][13])
    
    # Calculate min and max of each curve for each viewport. Viewport scales will automatically be set to the same scale if the same data type is 
    #    displayed on separate viewports. Need to include min and max values for the same data type in each viewport
    MinMaxDict = {}
    for x in range(len(ViewportLayoutsInfo)) :
        TimeSeriesParameter = ViewportLayoutsInfo[x][1].parameter
        TimeSeriesSubParameter = ViewportLayoutsInfo[x][1].subParameter
        outputDebug(debug, lineNo(), 'Time series: %s' % ViewportLayoutsInfo[x][1].fullName)
        InitialTscValues = ViewportLayoutsInfo[x][1].values
        outputDebug(debug, lineNo(), 'IntitialTscValues = ', InitialTscValues)
        # Substitute missing values with maximum value. This is only for scaling purposes. Plots will still contain all of the data
        if InitialTscValues == None or len(InitialTscValues) == 0 or all(x == Constants.UNDEFINED for x in InitialTscValues) == True :
            TscValues = [0.0, 0.5]
        else :
            TscValues = [max(InitialTscValues) if y == Constants.UNDEFINED else y for y in InitialTscValues]

        # Calculate min and max for curve
        MinValue = min(TscValues)
        MaxValue = max(TscValues)
        
        # Save min and max to dictionary
        Viewport = plot.getViewport(ViewportLayoutsInfo[x][1])
        ViewportName = Viewport.getName()
        
        try : 
            # Append min and max values to dictionary
            MinMaxDict[ViewportName][ViewportLayoutsInfo[x][2]]['MinValues'].append(MinValue)
            MinMaxDict[ViewportName][ViewportLayoutsInfo[x][2]]['MaxValues'].append(MaxValue)
        except : 
            # Add min and max values to dictionary
            outputDebug(debug, lineNo(), 'AxisName = ', ViewportLayoutsInfo[x][2])
            MinMaxDict.setdefault(ViewportName, {}).setdefault(ViewportLayoutsInfo[x][2], {}).setdefault('MinValues', [MinValue])
            MinMaxDict.setdefault(ViewportName, {}).setdefault(ViewportLayoutsInfo[x][2], {}).setdefault('MaxValues', [MaxValue])

            # Add viewports to dictionary
            MinMaxDict[ViewportName].setdefault('Viewport', Viewport)
        outputDebug(debug, lineNo(), 'ViewportName = ', ViewportName, '  MinValues = ', MinMaxDict[ViewportName][ViewportLayoutsInfo[x][2]]['MinValues'], 
                    '  MaxValues = ', MinMaxDict[ViewportName][ViewportLayoutsInfo[x][2]]['MaxValues'])
        
        for y in range(len(ViewportLayoutsInfo)) :
            OtherViewport = plot.getViewport(ViewportLayoutsInfo[y][1])
            OtherViewportName = OtherViewport.getName()
            if OtherViewportName != ViewportName :
                OtherTimeSeriesParameter = ViewportLayoutsInfo[y][1].parameter
                OtherTimeSeriesSubParameter = ViewportLayoutsInfo[y][1].subParameter
                
                if OtherTimeSeriesParameter == TimeSeriesParameter and OtherTimeSeriesSubParameter == TimeSeriesSubParameter :
                    outputDebug(debug, lineNo(), 'Parameter is in another viewport. Add values to %s' % OtherViewportName)
                    try : 
                        # Append min and max values to dictionary
                        MinMaxDict[OtherViewportName][ViewportLayoutsInfo[y][2]]['MinValues'].append(MinValue)
                        MinMaxDict[OtherViewportName][ViewportLayoutsInfo[y][2]]['MaxValues'].append(MaxValue)
                    except : 
                        # Add min and max values to dictionary
                        outputDebug(debug, lineNo(), 'AxisName = ', ViewportLayoutsInfo[y][2])
                        MinMaxDict.setdefault(OtherViewportName, {}).setdefault(ViewportLayoutsInfo[y][2], {}).setdefault('MinValues', [MinValue])
                        MinMaxDict.setdefault(OtherViewportName, {}).setdefault(ViewportLayoutsInfo[y][2], {}).setdefault('MaxValues', [MaxValue])
            
                        # Add viewports to dictionary
                        MinMaxDict[OtherViewportName].setdefault('Viewport', OtherViewport)
                    outputDebug(debug, lineNo(), 'OtherViewportName = ', OtherViewportName, '  MinValues = ', MinMaxDict[OtherViewportName][ViewportLayoutsInfo[y][2]]['MinValues'], 
                                '  MaxValues = ', MinMaxDict[OtherViewportName][ViewportLayoutsInfo[y][2]]['MaxValues'])
    
    # Set axis label. When running on the server, the units are sometimes left off the axis label.
    for x in range(len(ViewportLayoutsInfo)) :
        Viewport = plot.getViewport(ViewportLayoutsInfo[x][1])
        Y1Axis = Viewport.getAxis('Y1')
        if Y1Axis != None and ViewportLayoutsInfo[x][2] == 'Y1' : 
            if ViewportLayoutsInfo[x][1].parameter == '%' : Y1Axis.setLabel('%s (%s)' % (ViewportLayoutsInfo[x][1].subParameter, ViewportLayoutsInfo[x][1].units))
            else :Y1Axis.setLabel('%s (%s)' % (ViewportLayoutsInfo[x][1].parameter, ViewportLayoutsInfo[x][1].units))
        Y2Axis = Viewport.getAxis('Y2')
        if Y2Axis != None and ViewportLayoutsInfo[x][2] == 'Y2' : 
            if ViewportLayoutsInfo[x][1].parameter == '%' : Y2Axis.setLabel('%s (%s)' % (ViewportLayoutsInfo[x][1].subParameter, ViewportLayoutsInfo[x][1].units))
            else :Y2Axis.setLabel('%s (%s)' % (ViewportLayoutsInfo[x][1].parameter, ViewportLayoutsInfo[x][1].units))

    # Define the axis markers
    MarkerKeys = list(MarkerProperties.keys())
    for key in MarkerKeys :
        if MarkerProperties[key][0] and MarkerProperties[key][2] != None :
            marker = AxisMarker()
            outputDebug(debug, lineNo(), 'Marker Value = ', MarkerProperties[key][2])
            marker.value = '%s' % MarkerProperties[key][2]
            Viewport = plot.getViewport(MarkerProperties[key][3])
            ViewportName = Viewport.getName()
            marker.labelText = MarkerProperties[key][4]
            marker.labelFont = MarkerProperties[key][5]
            marker.axis = MarkerProperties[key][6]
            marker.labelAlignment = MarkerProperties[key][7]
            marker.labelPosition = MarkerProperties[key][8]
            marker.labelColor = MarkerProperties[key][9]
            marker.lineStyle = MarkerProperties[key][10]
            marker.lineColor = MarkerProperties[key][11]
            marker.lineWidth = MarkerProperties[key][12]
            Viewport.addAxisMarker(marker)
            
            # Determine which axis the marker is applied to
            for x in range(len(ViewportLayoutsInfo)) :
                TscFullName = ViewportLayoutsInfo[x][1].fullName
                if MarkerProperties[key][3].fullName == TscFullName : 
                    AxisName = ViewportLayoutsInfo[x][2]
                    outputDebug(debug, lineNo(), 'AxisName = ', AxisName)
                
            # Add markers to min and max values so the auto scaling with include the values only if marker is on Y axis
            if MarkerProperties[key][6] == 'Y1' or MarkerProperties[key][6] == 'Y2' : 
                MinMaxDict[ViewportName][AxisName]['MinValues'].append(float(MarkerProperties[key][2]))
                MinMaxDict[ViewportName][AxisName]['MaxValues'].append(float(MarkerProperties[key][2]))
    
    # Adjust scales to fit data. Normally this is done automatically when a plot is generated with CWMS. However, the auto-scaling does not always give a scale that fits
    #    the data correctly. Sometimes the scaling is too tight to the data so some curves are difficult to read or the axis markers are not shown on 
    #    the plot. The logic below, will scale the Y axes so the data is visible.
    ViewportKeys = list(MinMaxDict.keys())
    for key in ViewportKeys :
        Viewport = MinMaxDict[key]['Viewport']
        
        # Set yaxis scale
        AxesList = list(MinMaxDict[key].keys())[1 :] # The first key is 'Viewport'. Only want the axes
        for axis in AxesList :
            outputDebug(debug, lineNo(), 'ViewportName = ', key, '  Axis = ', axis)
            MaxValue = max(MinMaxDict[key][axis]['MaxValues'])
            MinValue = min(MinMaxDict[key][axis]['MinValues'])
            NumberOfTics = 5.
            YAxisMajorTic = (MaxValue - MinValue) / NumberOfTics
            if YAxisMajorTic == 0. : YAxisMajorTic = 0.05
            elif YAxisMajorTic < 0.1 : YAxisMajorTic = round(YAxisMajorTic, 2)
            elif YAxisMajorTic < 1. : YAxisMajorTic = round(YAxisMajorTic, 1)
            elif YAxisMajorTic < 10. : YAxisMajorTic = round(YAxisMajorTic, 0)
            elif YAxisMajorTic < 100. : YAxisMajorTic = round(YAxisMajorTic, -1)
            elif YAxisMajorTic < 1000. : YAxisMajorTic = round(YAxisMajorTic, -2)
            elif YAxisMajorTic < 10000. : YAxisMajorTic = round(YAxisMajorTic, -3)
            elif YAxisMajorTic < 100000. : YAxisMajorTic = round(YAxisMajorTic, -4)

            # Axis display digits
            if (YAxisMajorTic - int(YAxisMajorTic)) == 0.0 : YAxisDigits = 0
            elif (YAxisMajorTic - int(YAxisMajorTic)) < 0.1 : YAxisDigits = 2
            elif (YAxisMajorTic - int(YAxisMajorTic)) < 1. : YAxisDigits = 1
            #else : YAxisDigits = 0
                        
            if YAxisMajorTic == 0. : YAxisMajorTic = 0.05 # Double check rounded values are not 0 
            maxscale = int(MaxValue / YAxisMajorTic) * YAxisMajorTic + YAxisMajorTic
            minscale = int(MinValue / YAxisMajorTic) * YAxisMajorTic - YAxisMajorTic
            
            if abs(maxscale - MaxValue) < (YAxisMajorTic / 2.) : maxscale += YAxisMajorTic
            if abs(minscale - MinValue) < (YAxisMajorTic / 2.) : minscale -= YAxisMajorTic
            
            YAxis = Viewport.getAxis(axis)
            YAxis.setScaleLimits(minscale, maxscale)
            YAxis.setViewLimits(minscale, maxscale)
            YAxis.setMajorTicInterval(YAxisMajorTic)
            YAxis.setMaximumFactionDigits(YAxisDigits)
            outputDebug(debug, lineNo(), 'MaxValue = ', MaxValue, '  MinValue = ', MinValue, '  YAxisMajorTic = ', YAxisMajorTic, 
                        '  maxscale = ', maxscale, '  minscale = ', minscale)
            YAxisScaleMax = YAxis.getScaleMax()
            YAxisScaleMin = YAxis.getScaleMin()
            YAxisMajorTic = YAxis.getMajorTic()
            outputDebug(debug, lineNo(), Location, '\tFinal minscale = ', minscale, '\tYAxis scale max = ', YAxisScaleMax, '\tYAxis scale min = ', YAxisScaleMin,
                '\tFinal maxscale = ', maxscale, '\tFinal YAxis Major Tic = ', YAxisMajorTic)
         
    # Plot Title
    title = plot.getPlotTitle()
    title.setForeground(PlotProperties['FontColor'])
    title.setFontFamily(PlotProperties['Font'])
    title.setFontStyle(PlotProperties['FontStyle'])
    title.setFontSize(PlotProperties['FontSize'])
    title.setText(LocationProperties[Location]['PlotTitle'])
    title.setDrawTitleOn()
    plot.setSize(PlotProperties['PlotWidth'], PlotProperties['PlotHeight'])
    
    return plot

#
# lineNo Function   : Retrieves the line number of the script.  Used when debugging
# Author/Editor     : Ryan Larsen
# Last updated      : 01-26-2016
#
def lineNo() :
    return inspect.currentframe().f_back.f_lineno

#
# outputDebug Function  : Debugging function that prints specified arguments
# Author/Editor         : Ryan Larsen
# Last updated          : 04-10-2017
#
def outputDebug(    *args
                    ) :
    ArgCount = len(args)
    if ArgCount < 2 :
        raise ValueError('Expected at least 2 arguments, got %d' % argCount)
    if type(args[0]) != type(True) :
        raise ValueError('Expected first argument to be either True or False')
    if type(args[1]) != type(1) :
        raise ValueError('Expected second argument to be line number')

    if args[0] == True: 
        DebugStatement = 'Debug Line %d   |\t' % args[1]
        for x in range(2, ArgCount, 1) :
            DebugStatement += str(args[x])
        print(DebugStatement)

#
# retrieveElevatonDatum Function : Retrieves Elevation datum
# Author/Editor                  : Scott Hoffman
# Last updated                   : 06-25-2018
#
def retrieveElevatonDatum(  debug,          # Set to True to print all debug statements
                            conn,           #
                            CwmsDb,         # DBAPI connection
                            BaseLocation,   # Full name of time series container
                            ) :
    ElevationDatum = 'None'
    if isinstance(conn, RADARAPI.RadarAccess):
        try :
            dak = conn._factory.getDataAccessKey("retrieveLocation")
            location_dao = CwmsDaoServiceLookup.getDao(CwmsLocationDao, conn._factory.getDbConnection())

            loc = LocationTemplate(conn.getOfficeId(), BaseLocation)
            location = location_dao.retrieveLocation(dak, loc)
            ElevationDatum = str(location.getElevation())
        except :
            pass
    else:
        rs = None
        try :
            stmt = conn.prepareStatement('''
                                        select distinct
                                        bl.elevation as base_location_elevation
                                        from cwms_v_loc loc
                                        inner join cwms_v_loc bl on bl.base_location_code = loc.base_location_code
                                            and bl.db_office_id = loc.db_office_id
                                            and bl.unit_system = loc.unit_system
                                        where loc.UNIT_SYSTEM = 'EN'
                                            and loc.db_office_id = :1
                                            and bl.location_id = :2
                                        ''')
            stmt.setString(1, CwmsDb.getOfficeId())
            stmt.setString(2, BaseLocation)
            rs = stmt.executeQuery()

            while next(rs) :
                ElevationDatum = str(rs.getString(1))
                break
        except java.lang.Exception as e:
            # Java exceptions must be explicitly named
            # raise a regular python exception to pass it through
            raise Exception(e)
        finally :
            try :
                stmt.close()
                if rs: rs.close()
            except :
                pass
    return ElevationDatum

#
# retrieveLocationLevel Function    : Retrieves reservoir zone data
# Author/Editor                     : Mike Perryman
# Last updated                      : 05-01-2017
#
def retrieveLocationLevel(  debug,          # Set to True to print all debug statements
                            conn,           # SQL connection
                            CwmsDb,         # DBAPI connection
                            TscFullName,    # Full name of time series container
                            ) :
    import datetime
    
    CurDate         = datetime.datetime.now() # Current date
    StartTimeStr    = CurDate.strftime('%d%b%Y ') + '0000' # Start date formatted as ddmmmyyy 0000
    EndTimeStr      = CurDate.strftime('%d%b%Y ') + '0000' # End date formatted as ddmmmyyy 0000

    level_1a = TimeSeriesContainer()
    level_1a_parts = TscFullName.split('.')
    level_1aId_parts = level_1a_parts[:]
    level_1aId = '.'.join(level_1aId_parts)
    outputDebug(debug, lineNo(), 'level_1a_parts = ', level_1a_parts, '\tlevel_1aId_parts = ', level_1aId_parts, 
        '\n\t\tlevel_1aId = ', level_1aId)
    
    level_1a.fullName  = TscFullName
    level_1a.location  = level_1a_parts[0]
    level_1a.parameter = level_1a_parts[1]
    level_1a.interval  = 0
    level_1a.version   = level_1a_parts[-1]
    if level_1a_parts[1] == 'Stor' : level_1a.units = 'ac-ft'
    elif level_1a_parts[1] == 'Elev' or level_1a_parts[1] == 'Stage' : level_1a.units = 'ft'
    elif level_1a_parts[1] == 'Flow' : level_1a.units = 'cfs'
    level_1a.type      = 'INST-VAL'
   
    LocationLevel = None
    if isinstance(conn, RADARAPI.RadarAccess):
        try:
            dak = conn._factory.getDataAccessKey("retrieveLocationLevel")
            level_dao = CwmsDaoServiceLookup.getDao(CwmsLevelDao, conn._factory.getDbConnection())

            if level_dao is not None:
                ll = JDomLocationLevelImpl(conn.getOfficeId(), level_1aId, SimpleDateFormat("ddMMMyyyy HHmm").parse(StartTimeStr), level_1a.units, None, None, None)
                LocationLevel = level_dao.retrieveLocationLevel(dak, ll)
        except :
            pass
    else:
        rs = None
        try :
            stmt = conn.prepareStatement('''
                                  select * from table(cwms_level.retrieve_location_level_values(
                                  p_location_level_id => :1,
                                  p_level_units       => :2,
                                  p_start_time        => to_date(:3, 'ddmonyyyy hh24mi'),
                                  p_end_time          => to_date(:4, 'ddmonyyyy hh24mi'),
                                  p_timezone_id       => :5))
                            ''')   
            stmt.setString(1, level_1aId)
            stmt.setString(2, level_1a.units)
            stmt.setString(3, StartTimeStr)
            stmt.setString(4, EndTimeStr)
            stmt.setString(5, CwmsDb.getTimeZoneName())
            rs = stmt.executeQuery()
            
            while next(rs) : 
                LocationLevel = rs.getDouble(2)
                break
        except java.lang.Exception as e:
            # Java exceptions must be explicitly named
            # raise a regular python exception to pass it through
            raise Exception(e)
        finally :
            stmt.close()
            if rs: rs.close()
        
    return LocationLevel
#
# retrieveLongName Function    : Retrieves reservoir zone data
# Author/Editor                : Ryan Larsen
# Last updated                 : 02-11-2019
#
def retrieveLongName(   debug,          # Set to True to print all debug statements
                        conn,         # 
                        CwmsDb,         # DBAPI connection
                        BaseLocation,   # Full name of time series container
                        ) :
    LongName = BaseLocation
    if isinstance(conn, RADARAPI.RadarAccess):
        try :
            dak = conn._factory.getDataAccessKey("retrieveLocation")
            location_dao = CwmsDaoServiceLookup.getDao(CwmsLocationDao, conn._factory.getDbConnection())

            loc = LocationTemplate(conn.getOfficeId(), BaseLocation)
            location = location_dao.retrieveLocation(dak, loc)
            LongName = location.getLongName()
        except :
            pass
    else:
        rs = None
        try :
            stmt = conn.prepareStatement('''
                                        select distinct
                                        bl.long_name as base_location_long_name
                                        from cwms_v_loc loc 
                                        inner join cwms_v_loc bl on bl.base_location_code = loc.base_location_code
                                            and bl.db_office_id = loc.db_office_id 
                                            and bl.unit_system = loc.unit_system
                                        where loc.UNIT_SYSTEM = 'EN' 
                                            and loc.db_office_id = :1 
                                            and bl.location_id = :2
                                        ''')   
            stmt.setString(1, CwmsDb.getOfficeId())
            stmt.setString(2, BaseLocation)
            rs = stmt.executeQuery()
            
            while next(rs) : 
                LongName = str(rs.getString(1))
                break 
        except java.lang.Exception as e:
            # Java exceptions must be explicitly named
            # raise a regular python exception to pass it through
            raise Exception(e)
        finally :
            stmt.close()
            if rs:
                rs.close()
        
    return LongName

#
# retrievePublicName Function    : Retrieves reservoir zone data
# Author/Editor                  : Ryan Larsen
# Last updated                   : 01-30-2018
#
def retrievePublicName( debug,          # Set to True to print all debug statements
                        conn,         # 
                        CwmsDb,         # DBAPI connection
                        BaseLocation,   # Full name of time series container
                        ) :
    PublicName = BaseLocation

    if isinstance(conn, RADARAPI.RadarAccess):
        try :
            dak = conn._factory.getDataAccessKey("retrieveLocation")
            location_dao = CwmsDaoServiceLookup.getDao(CwmsLocationDao, conn._factory.getDbConnection())

            loc = LocationTemplate(conn.getOfficeId(), BaseLocation)
            location = location_dao.retrieveLocation(dak, loc)
            PublicName = location.getPublicName()
        except :
            pass
    else:
        rs = None
        try :
            stmt = conn.prepareStatement('''
                                        select distinct
                                        bl.public_name as base_location_public_name
                                        from cwms_v_loc loc 
                                        inner join cwms_v_loc bl on bl.base_location_code = loc.base_location_code
                                            and bl.db_office_id = loc.db_office_id 
                                            and bl.unit_system = loc.unit_system
                                        where loc.UNIT_SYSTEM = 'EN' 
                                            and loc.db_office_id = :1 
                                            and bl.location_id = :2
                                        ''')   
            stmt.setString(1, CwmsDb.getOfficeId())
            stmt.setString(2, BaseLocation)
            rs = stmt.executeQuery()
            
            while next(rs) : 
                PublicName = str(rs.getString(1))
                break 
        except java.lang.Exception as e:
            # Java exceptions must be explicitly named
            # raise a regular python exception to pass it through
            raise Exception(e)
        finally :
            stmt.close()
            if rs: rs.close()
        
    return PublicName

