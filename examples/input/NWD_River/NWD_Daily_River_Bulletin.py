'''
Author: Scott Hoffman
Last Updated: 11-04-2019
Description: Create the Division River Bulletin
'''
# -------------------------------------------------------------------
# Required imports
# -------------------------------------------------------------------
import platform
IS_CPYTHON = True if platform.python_implementation() == "CPython" else False
        
try:
    # Migrate to newer OpenPDF library if available
    from com.lowagie.text       import Document, DocumentException, Rectangle, Paragraph, Phrase, Chunk, Font, FontFactory, PageSize, Element, Image
    from com.lowagie.text       import Font as FontFamily
    from com.lowagie.text.pdf   import PdfWriter, PdfPCell, PdfPTable, PdfPage, PdfName, PdfPageEventHelper, BaseFont
    from java.awt               import Color as BaseColor
except:
    # Fall back to iText5, if not
    from com.itextpdf.text      import Document, DocumentException, Rectangle, Paragraph, Phrase, Chunk, Font, FontFactory, BaseColor, PageSize, Element, Image
    from com.itextpdf.text.Font import FontFamily
    from com.itextpdf.text.pdf  import PdfWriter, PdfPCell, PdfPTable, PdfPage, PdfName, PdfPageEventHelper, BaseFont

from hec.data.cwmsRating    import RatingSet
from hec.heclib.util        import HecTime
from hec.io                 import TimeSeriesContainer
from hec.script             import Constants, MessageBox
from java.awt.image         import BufferedImage
from java.io                import FileOutputStream, IOException
from java.lang              import System
from java.text              import NumberFormat, SimpleDateFormat
from java.util              import Locale, Calendar, TimeZone
from time                   import mktime, localtime
from subprocess             import Popen
import java.lang
import os, sys, inspect, datetime, time, RADARAPI, DBAPI

# -------------------------------------------------------------------
# Import database pathnames and plotting functions
# -------------------------------------------------------------------
# Determine if OS is Windows or Unix. Use PC pathnames if OS is Windows
OsName = str(java.lang.System.getProperty("os.name")).lower()
print('OsName = ', OsName)
if OsName[ : 7] == 'windows' : 
    # PC pathnames
    ScriptDirectory = os.path.dirname(os.path.realpath(__file__))
    PathList = ScriptDirectory.split('\\')
    BulletinsDirectory = '\\'.join(PathList[: -1]) + '\\'
    CronjobsDirectory = '\\'.join(PathList[: -1]) + '\\'
    OutputDirectory = '\\'.join(PathList[: -1]) + '\\'
else :
    # Server pathnames
    ScriptDirectory = os.path.dirname(os.path.realpath(__file__))
    PathList = ScriptDirectory.split('/')
    BulletinsDirectory = '/'.join(PathList[: -1]) + '/'
    CronjobsDirectory = '/'.join(PathList[: -1]) + '/'
    OutputDirectory = '/'.join(PathList[: -1]) + '/'
    OutputDirectory = '/output'

BulletinFilename = os.path.join(OutputDirectory, 'NWD_Daily_River_Bulletin.pdf')
ArchiveBulletinFilename = os.path.join(OutputDirectory, 'MRBWM_River_Daily_%s.pdf')
CsvFilename = os.path.join(OutputDirectory, 'NWD_Daily_River_Bulletin.csv')
BulletinPropertiesPathname = os.path.join(ScriptDirectory, 'NWD_Daily_River_Bulletin_Properties.txt')

print('BulletinsDirectory = ', BulletinsDirectory, '\tScript Directory = ', ScriptDirectory)
print('BulletinFilename = ', BulletinFilename, '\tBulletinPropertiesPathname = ', BulletinPropertiesPathname)

if CronjobsDirectory not in sys.path : sys.path.append(CronjobsDirectory)
if BulletinsDirectory not in sys.path : sys.path.append(BulletinsDirectory)
if ScriptDirectory not in sys.path : sys.path.append(ScriptDirectory)

#
# Load DatabasePathnames.txt and BulletinProperties
#
while True :
    errorMessage = None
    DatabasePathnamesFile = os.path.join(CronjobsDirectory, "DatabasePathnames.txt")
    if not os.path.exists(DatabasePathnamesFile) :
        errorMessage = "DatabasePathnames.txt does not exist: %s" % DatabasePathnamesFile
    with open(DatabasePathnamesFile, "r") as f : exec(f.read())
    break
if errorMessage :
    print("ERROR : " + errorMessage)
BulletinProperties = open(BulletinPropertiesPathname, "r"); exec(BulletinProperties.read())

# Import server utilities
import Server_Utils
if IS_CPYTHON:
    import importlib
    importlib.reload(Server_Utils)
else:
    reload(Server_Utils)
from Server_Utils import lineNo, outputDebug, retrieveLocationLevel, retrievePublicName, retrieveElevatonDatum

#
# Input
#
# Set debug = True to print all debug statements and = False to turn them off
debug = False

USE_CDA = True  # Set to true to use CDA instead of DBAPI
#CDA_URL = 'https://wm.spk.ds.usace.army.mil:8243/spk-data'
CDA_URL = 'https://cwms-data.usace.army.mil:443/cwms-data'
OFFICE = 'SPK'

##################################################################################################################################
#
# Functions
#

#
# bulletinFooter Function   : Creates a footer for the bulletin
# Author/Editor             : Ryan Larsen
# Last updated              : 12-12-2017
#
def bulletinFooter( debug,  # Set to True to print all debug statements
                    Footer  # PdfPTable object
                    ) :
    # Add the footer image to the footer
    Img = Image.getInstance(FooterImage)
    Img.scalePercent(20)
    # createCell(debug, CellData, RowSpan, ColSpan, HorizontalAlignment, VerticalAlignment, CellPadding, BorderColors, BorderWidths, VariableBorders, BackgroundColor)
    Cell = createCell(debug, Img, TableLayoutDict['Table1']['RowSpan'], TableLayoutDict['Table1']['ColSpan'], Element.ALIGN_LEFT, 
        TableLayoutDict['Table1']['VerticalAlignment'], TableLayoutDict['Table1']['CellPadding'], TableLayoutDict['Table1']['BorderColors'], 
        TableLayoutDict['Table1']['BorderWidths'], TableLayoutDict['Table1']['VariableBorders'], TableLayoutDict['Table1']['BackgroundColor'])
    Cell.setBorder(Rectangle.NO_BORDER)
    Footer.addCell(Cell)

    # Add the page numbers to the footer
    # createCell(debug, CellData, RowSpan, ColSpan, HorizontalAlignment, VerticalAlignment, CellPadding, BorderColors, BorderWidths, VariableBorders, BackgroundColor)
    Cell = createCell(debug, Phrase(Chunk('Page %d of 2' % Writer.getPageNumber(), TableLayoutDict['Table1']['TextFont'])), TableLayoutDict['Table1']['RowSpan'], 
        TableLayoutDict['Table1']['ColSpan'], TableLayoutDict['Table1']['HorizontalAlignment'], TableLayoutDict['Table1']['VerticalAlignment'], 
        TableLayoutDict['Table1']['CellPadding'], TableLayoutDict['Table1']['BorderColors'], TableLayoutDict['Table1']['BorderWidths'], 
        TableLayoutDict['Table1']['VariableBorders'], TableLayoutDict['Table1']['BackgroundColor'])
    Cell.setBorder(Rectangle.NO_BORDER)
    Footer.addCell(Cell)
    
    return Footer

#
# checkTs Function    : Check if the TS is in the database
# Author/Editor       : Scott Hoffman
#
def checkTs(timeseries,
            conn,) :
    if isinstance(conn, RADARAPI.RadarAccess):
        result = conn.getCatalogedPathnames(timeseries)
        if len(result) > 0:
            return 'true'
        else:
            return 'false'
    else:
        try :
            #check if timeseries exist in DB
            string = "select * from CWMS_20.av_cwms_ts_id where cwms_ts_id='" + timeseries + "'"
            stmt = conn.prepareStatement(string)
            rset = stmt.executeQuery()
            if next(rset) :
                #Found timeseries
                flag = 'true'
            else :
                flag = 'false'
        finally :
            try : stmt.close()
            except : pass
            try : rset.close()
            except : pass
        return flag

#
# Check DST Function
#
def is_dst(dayTime):
    
    # example:  2018-06-15 15:09:46
    sdf = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    date = sdf.parseObject(dayTime)
    cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    cal.setTime(date)
    # checking day light
    timezoneone = TimeZone.getDefault()
    day = cal.getTime()

    return timezoneone.inDaylightTime(day) 

#
# createCell Function   : Creates a PdfPCell for tables
# Author/Editor         : Ryan Larsen
# Last updated          : 12-20-2017
#
def createCell( debug,                  # Set to True to print all debug statments
                CellData,               # Data that will appear within the cell
                RowSpan,                # Specifies number of rows information within cell will span
                ColSpan,                # Specifies number of columns information within cell will span
                HorizontalAlignment,    # Specifies horizontal alignment: ALIGN_CENTER, ALIGN_LEFT, ALIGN_RIGHT
                VerticalAlignment,      # Specifies vertical alignment: ALIGN_CENTER, ALIGN_TOP, ALIGN_BOTTOM
                CellPadding,            # List of cell padding around text: [Top, Right, Bottom, Left]
                BorderColors,           # List of border colors: [Top, Right, Bottom, Left]
                BorderWidths,           # List of border widths: [Top, Right, Bottom, Left]
                VariableBorders,        # Allows or denies variable borders: True, False
                BackgroundColor         # Color of cell background
                ) :
    Cell = PdfPCell(CellData)
    Cell.setRowspan(RowSpan); Cell.setColspan(ColSpan)
    Cell.setHorizontalAlignment(HorizontalAlignment); Cell.setVerticalAlignment(VerticalAlignment)
    Cell.setPaddingTop(CellPadding[0]); Cell.setPaddingRight(CellPadding[1]); Cell.setPaddingBottom(CellPadding[2]); Cell.setPaddingLeft(CellPadding[3])
    Cell.setBorderColorTop(BorderColors[0]); Cell.setBorderColorRight(BorderColors[1]); Cell.setBorderColorBottom(BorderColors[2]); Cell.setBorderColorLeft(BorderColors[3])
    Cell.setBorderWidthTop(BorderWidths[0]); Cell.setBorderWidthRight(BorderWidths[1]); Cell.setBorderWidthBottom(BorderWidths[2]); Cell.setBorderWidthLeft(BorderWidths[3])
    Cell.setUseVariableBorders(VariableBorders)
    Cell.setBackgroundColor(BackgroundColor)

    return Cell
##############################################################################################
#
# table1Data Function   : Creates the Data1 block for Table1 in the bulletin
# Author/Editor         : Ryan Larsen
# Last updated          : 12-12-2017
#
def table1Data( debug,      # Set to True to print all debug statements
                Table,      # PdfPTable object
                TableName,  # String name for the table
                DataName,    # String name for data section of table
                startTime,
                endTime,
                startSysTime,
                endSysTime,
                CsvData,
                DbPathnameList
                    ) :
    # Create name for TableData
    TableDataName = '%s%s' % (TableName, DataName)

    # Data Block Heading
    # createCell(debug, CellData, RowSpan, ColSpan, HorizontalAlignment, VerticalAlignment, CellPadding, BorderColors, BorderWidths, VariableBorders, BackgroundColor)
    if DataName == 'Data1' :
        Cell = createCell(debug, Phrase(Chunk(DataBlockDict['DataBlocks'][TableDataName]['Heading'], Font9)), TableLayoutDict[TableName]['RowSpan'], 
            Table1Columns, Element.ALIGN_CENTER, TableLayoutDict[TableName]['VerticalAlignment'], [0, 2, 2, 2], TableLayoutDict[TableName]['BorderColors'], 
            [0.25, 1, 0.25, 1], TableLayoutDict[TableName]['VariableBorders'], Color7)
    elif DataName == 'Data2' :
        textString = "Selected River Gaging Stations as of 6:00 a.m."
        Cell = createCell(debug, Phrase(Chunk(textString, Font9)), TableLayoutDict[TableName]['RowSpan'],
                Table1Columns, Element.ALIGN_CENTER, TableLayoutDict[TableName]['VerticalAlignment'], [0, 2, 2, 2], TableLayoutDict[TableName]['BorderColors'],
                [0.25, 1, 0.25, 1], TableLayoutDict[TableName]['VariableBorders'], Color7) 
        Table.addCell(Cell)
        Cell = createCell(debug, Phrase(Chunk(DataBlockDict['DataBlocks'][TableDataName]['Heading'], Font9)), TableLayoutDict[TableName]['RowSpan'],
                Table1Columns, Element.ALIGN_LEFT, TableLayoutDict[TableName]['VerticalAlignment'], [0, 2, 2, 2], TableLayoutDict[TableName]['BorderColors'],
                [0.5, 1, 0.25, 1], TableLayoutDict[TableName]['VariableBorders'], Color7)
    else :
        Cell = createCell(debug, Phrase(Chunk(DataBlockDict['DataBlocks'][TableDataName]['Heading'], Font9)), TableLayoutDict[TableName]['RowSpan'],
            Table1Columns, Element.ALIGN_LEFT, TableLayoutDict[TableName]['VerticalAlignment'], [0, 2, 2, 2], TableLayoutDict[TableName]['BorderColors'],
            [0.25, 1, 0.25, 1], TableLayoutDict[TableName]['VariableBorders'], Color7)

    Table.addCell(Cell)

    # Add text to CsvData
    CellData = Phrase(Chunk(DataBlockDict['DataBlocks'][TableDataName]['Heading'], Font5))
    CsvData += str(CellData[0])
    CsvData += '\n'
    
    # Data
    for project in DataBlockDict['DataBlocks'][TableDataName]['ProjectList'] :
        # Retrieve Public Name and store it to the DataBlockDict
        PublicName = retrievePublicName(debug, conn, CwmsDb, project)
        PublicName = PublicName.replace(' & Reservoir', '')
        outputDebug(debug, lineNo(), 'Creating %s row' % PublicName)
        
        # If adding the last project in the last data block, create a trigger to use a thick bottom border
        if DataName == 'Data%d' % NumberOfDataBlocks and project == DataBlockDict['DataBlocks'][TableDataName]['ProjectList'][-1] :
            LastProject = True
        else : LastProject = False

        # Reset TotalColSpan to 0
        TotalColSpan = 0

        for data in DataOrder :
            outputDebug(debug, lineNo(), 'Adding %s to the row' % data)
            # Create a variable within the DataDict. This will allow the user to store all data to a dictionary and access the variables throughout
            #   the script
            DataBlockDict['DataBlocks'][TableDataName].setdefault(project, {}).setdefault(data, None)

            # Get column number
            ColumnKey = 'Column%d' % DataOrder.index(data)

            # Default cell properties. If there is a special case, the properties will be changed.
            TextFont = TableLayoutDict[TableName]['TextFont']
            if project in ['SYS'] :
                RowSpan = 3
            else :
                RowSpan = TableLayoutDict[TableName]['RowSpan']
             
            ColSpan = TableLayoutDict[TableName]['ColSpan']   
            HorizontalAlignment = TableLayoutDict[TableName]['HorizontalAlignment']; VerticalAlignment = TableLayoutDict[TableName]['VerticalAlignment']
            CellPadding = TableLayoutDict[TableName]['CellPadding'] #[Top, Right, Bottom, Left]
            BorderColors = TableLayoutDict[TableName]['BorderColors']
            BorderWidths = TableLayoutDict[TableName]['BorderWidths']
            VariableBorders = TableLayoutDict[TableName]['VariableBorders']
            BackgroundColor = TableLayoutDict[TableName]['BackgroundColor']
            
            # 0 - Project Bulletin Name
            if data == 'PublicName' :
                # Create a formatted string that will be added to the table
                CellData = Phrase(Chunk(PublicName, TextFont))
                # Change default cell properties
                HorizontalAlignment = Element.ALIGN_LEFT
                BorderColors = [Color2, Color3, Color2, Color2]
                CellPadding = [0, 2, 2, 7] #[Top, Right, Bottom, Left]#Indent the project names               
                if LastProject or project == 'CEIA' : BorderWidths = [0.25, 0.5, 1, 1] #[Top, Right, Bottom, Left]
                else : BorderWidths = [0.25, 0.5, 0.25, 1] #[Top, Right, Bottom, Left]
            # 1 - River Mile
            elif data == 'RiverMile' :
                try :
                    if type(LocationDict[project]['RiverMile']) == type(0.) : 
                        RivMile = LocationDict[project]['RiverMile']
                        # Create a formatted string that will be added to the table
                        CellData = Phrase(Chunk(TableLayoutDict[TableName][ColumnKey]['Format'] % RivMile, TextFont))
                    else :
                        CellData = Phrase(Chunk(Null, TextFont))
                except :
                    outputDebug(debug, lineNo(), 'Missing River Mile for %s in LocationDict' % project)
                    # Create a formatted string that will be added to the table
                    CellData = Phrase(Chunk(Null, TextFont))

                # Change default cell properties
                BorderColors = [Color2, Color2, Color2, Color3]
                if LastProject or project == 'CEIA' : BorderWidths = [0.25, 0.25, 1, 0.5] #[Top, Right, Bottom, Left]
                else : BorderWidths = [0.25, 0.25, 0.25, 0.5] #[Top, Right, Bottom, Left]
            # 2 - elevation datum
            elif data == 'ElevDatum' :
                try :
                    CellData = Phrase(Chunk(Null, TextFont))
                    if project in ['FTPK', 'GARR', 'OAHE', 'BEND', 'FTRA', 'GAPT'] : 
                        ElevationDatum = Null
                        # Create a formatted string that will be added to the table
                        CellData = Phrase(Chunk(Null, TextFont))
                    elif project in ['SYS'] :
                        CellData = Phrase(Chunk(Null, TextFont))
                    else :
                        #Get the Elvation Datum
                        ElevationDatum = retrieveElevatonDatum(debug, conn, CwmsDb, project)
                        
                    # Create a formatted string that will be added to the table
                    if ElevationDatum == Null or ElevationDatum == 'None' :
                       CellData = Phrase(Chunk(Null, TextFont))
                    else :
                       CellData = Phrase(Chunk(TableLayoutDict[TableName][ColumnKey]['Format'] % float(ElevationDatum), TextFont))
                except Exception as e :
                    print("Elevation Datum Exception = " + str(e))    
                    # Create a formatted string that will be added to the table
                    CellData = Phrase(Chunk(Missing, TextFont))

            # 3 - Flood Stage
            elif data == 'FloodStage' :
                try :
                    NwsFloodStage = Null
                    CellData = Phrase(Chunk(Missing, TextFont))
                    if project in ['SYS', 'FTPK', 'GARR', 'OAHE', 'BEND', 'FTRA', 'GAPT', 'CAFE', 'HAST', 'BAGL'] :
                        CellData = Phrase(Chunk(Null, TextFont)) 
                    else :
                        TscPathname = DataBlockDict['DataBlocks'][TableDataName][data] % project
                        NwsFloodStage = retrieveLocationLevel(debug, conn, CwmsDb, TscPathname)
                        outputDebug(debug, lineNo(), 'Flood Stage Pathname = ', TscPathname, '\tNwsFloodStage = ', NwsFloodStage)
                            
                        if NwsFloodStage != Null and NwsFloodStage != 'None' :
                            # Create a formatted string that will be added to the table
                            CellData = Phrase(Chunk(TableLayoutDict[TableName][ColumnKey]['Format'] % NwsFloodStage, TextFont))
                        else:
                            CellData = Phrase(Chunk(Missing, TextFont))
                except Exception as e :
                    print("FloodStage Exception = " + str(e))

            # 4 - Elevation
            elif data == 'Elevation' :
                if str(DataBlockDict['DataBlocks'][TableDataName][data]) != 'None' :
                    if project in ['FTPK', 'GARR', 'OAHE', 'BEND', 'FTRA', 'GAPT'] :
                        TscPathname = DataBlockDict['DataBlocks'][TableDataName][data] % project
                    elif project in ['CAFE'] : 
                        TscPathname = DataBlockDict['DataBlocks'][TableDataName][data][0] % project
                    elif project in ['HAST', 'BAGL'] : 
                        TscPathname = DataBlockDict['DataBlocks'][TableDataName][data][1] % project
                    else :
                        TscPathname = DataBlockDict['DataBlocks'][TableDataName][data] % project
                    if checkTs(TscPathname, conn) == 'true' or project in ['CAFE'] :
                        if project not in ['SYS'] :
                            try :
                                Tsc = CwmsDb.get(TscPathname, startTime, endTime)

                                PrevElev = Tsc.values[-1] # Previous day's midnight value
                                Prev2xElev = Tsc.values[0] # 2 days previous midnight value
                            
                                # If previous day's value is missing raise an exception and using the missing value
                                outputDebug(debug, lineNo(), 'PrevElev = ', PrevElev, '\tPrev2xElev = ', Prev2xElev)
                                if PrevElev == Constants.UNDEFINED : raise ValueError('Missing elevation data for %s' % project)
                                elif Prev2xElev == Constants.UNDEFINED : Prev2xElev = Missing 
                                
                                # Create a formatted string that will be added to the table
                                CellData = Phrase(Chunk(TableLayoutDict[TableName][ColumnKey]['Format'] % PrevElev, TextFont))
                            except :
                                PrevElev, Prev2xElev = Missing, Missing
                                # Create a formatted string that will be added to the table
                                CellData = Phrase(Chunk(Missing, TextFont))
                
                            # Store value to DataDict
                            outputDebug(debug, lineNo(), 'Set %s %s = ' % (project, data), PrevElev)
                            DataBlockDict['DataBlocks'][TableDataName][project][data] = PrevElev
                            DataBlockDict['DataBlocks'][TableDataName][project][data + '2x'] = Prev2xElev
                        else :
                            #None, set value to Null
                            CellData = Phrase(Chunk(Missing, TextFont))
                    else :
                        #None, set value to Null
                        CellData = Phrase(Chunk(Null, TextFont))
            # 5 Daily elevation change
            elif data == 'ElevChange' :
                try :
                    if project in ['SYS'] :
                        CellData = Phrase(Chunk('System Storage \n Storage Change \n Daily Generation', TextFont))
                        HorizontalAlignment = Element.ALIGN_RIGHT
                        ColSpan = 2
                    else :
                        if DataBlockDict['DataBlocks'][TableDataName][project]['Elevation'] == Missing or \
                            DataBlockDict['DataBlocks'][TableDataName][project]['Elevation2x'] == Missing :
                            raise ValueError('Cannot compute daily elevation change data for %s' % project)
                
                        DlyElevChange = DataBlockDict['DataBlocks'][TableDataName][project]['Elevation'] - DataBlockDict['DataBlocks'][TableDataName][project]['Elevation2x']
                        
                        # Create a formatted string that will be added to the table
                        CellData = Phrase(Chunk(TableLayoutDict[TableName][ColumnKey]['Format'] % DlyElevChange, TextFont))
                except :
                    DlyElevChange = Missing
                    # Create a formatted string that will be added to the table
                    CellData = Phrase(Chunk(Missing, TextFont))
                
            # 6 Storage, Inflow, Energy, and Flow Total
            elif data == 'FlowIn' or data == 'Storage' :
                if str(DataBlockDict['DataBlocks'][TableDataName][data]) != 'None' :
                    if project in ['FTPK', 'GARR', 'OAHE', 'BEND', 'FTRA', 'GAPT', 'SYS'] :
                        TscPathname = DataBlockDict['DataBlocks'][TableDataName][data] % project
                    elif project == 'CAFE' :
                        TscPathname = DataBlockDict['DataBlocks'][TableDataName][data][0] % project
                    elif project in ['HAST', 'BAGL'] :
                        TscPathname = DataBlockDict['DataBlocks'][TableDataName][data][1] % project
                    else :
                        TscPathname = DataBlockDict['DataBlocks'][TableDataName][data] % project
                    
                    if checkTs(TscPathname, conn) == 'true' :
                    #if TscPathname in DbPathnameList :
                        try :
                            if project in ['SYS'] :
                                if data == 'Storage' :
                                    tscPathname = DataBlockDict['DataBlocks'][TableDataName][data] % project
                                    Tsc = CwmsDb.read(tscPathname, startTime, endTime).getData()
                                    Value = Tsc.values[-1]
                                    if Value == Constants.UNDEFINED :
                                        Value = Missing
                                        PrevStor = Value
                                        CellData = Phrase(Chunk(Value, TextFont))
                                    else :
                                        Value = round(Value, 0)
                                        PrevStor = Value
                                        Value = Value/1000
                                        CellData =  Chunk(TableLayoutDict[TableName][ColumnKey][1]['Format'].format(int(Value)), TextFont)
                                    
                                    tscPathname = DataBlockDict['DataBlocks'][TableDataName][data] % project
                                    Tsc2 = CwmsDb.read(tscPathname, startSysTime, endSysTime).getData()
                                    Value2 = Tsc2.values[0]
                                    if Value2 == Missing or PrevStor == Missing :
                                        Value2 = Missing
                                        CellData2 = Phrase(Chunk(Value2, TextFont))
                                    else :
                                        Value2 = round(Value2, 0)
                                        Value2 = (PrevStor - Value2)/1000
                                        CellData2 =  Chunk(TableLayoutDict[TableName][ColumnKey][1]['Format'].format(int(Value2)), TextFont)
                                    
                                    tscPathname = tscPathname.replace("Stor.Inst.~1Day.0.Best-MRBWM", "Energy.Total.~1Day.1Day.Best-MRBWM")
                                    Tsc3 = CwmsDb.read(tscPathname, startTime, endTime).getData()
                                    Value3 = Tsc3.values[-1]
                                    if Value3 == Constants.UNDEFINED :
                                        Value3 = Missing
                                        CellData = Phrase(Chunk(Value3, TextFont))
                                    else :
                                        Value3 = round(Value3, 0)
                                        CellData3 =  Chunk(TableLayoutDict[TableName][ColumnKey][1]['Format'].format(int(Value3)), TextFont)
    
                                    CellData = Phrase(str(CellData) + '\n' + str(CellData2) + '\n' + str(CellData3), TextFont)
                                else :
                                    Value = 0.
                                    CellData = Phrase(Chunk(Missing, TextFont))
                            else : #All Other projects
                                Tsc = CwmsDb.get(TscPathname, startTime, endTime)
    
                                if data == 'FlowIn' and project in ['FTPK', 'GARR', 'OAHE', 'BEND', 'FTRA', 'GAPT','CAFE', 'HAST', 'BAGL'] :
                                    TscPathname2 = TscPathname.replace("Flow-In", "Flow-Out")
                                    
                                    Tsc2 = CwmsDb.get(TscPathname2, startTime, endTime)
    
                                    count = 0
                                    for x in Tsc2.values :
                                        count = count + 1 
                                    if(count >= 1) :
                                        Value2 = Tsc2.values[-1]
                                        Value2 = round(Value2, 0)
                                    else:
                                        Tsc2 = Null
                                        Value2 = Null
                                elif data == 'Storage' and project in ['FTPK', 'GARR', 'OAHE', 'BEND', 'FTRA', 'GAPT', 'HAST'] :
                                    if project in ['FTPK', 'GARR', 'OAHE', 'BEND', 'FTRA', 'GAPT'] : 
                                        TscPathname2 = EnergyTotalDayBestMrbwm % project
                                    else : 
                                        TscPathname2 = EnergyTotalDayBestNwk % project

                                    Tsc2 = CwmsDb.get(TscPathname2, startTime, endTime)
                                    count = 0
                                    for x in Tsc2.values :
                                        count = count + 1
                                    if(count >= 1):
                                        Value2 = Tsc2.values[-1]
                                        Value2 = round(Value2, 0)
                                    else :
                                        Tsc2 = Null
                                        Value2 = Null   
                                elif data == 'Storage' and project in ['CAFE', 'BAGL'] :#Only a storage value no energy for 2 projects
                                    Tsc2 = CwmsDb.get(TscPathname, startTime, endTime)
                                    Value2 = Tsc2.values[-1]/1000
                                    Value2 = round(Value2, 0)
    
                                if Tsc != Null and not project in ['WSN', 'PONE', 'BLNE', 'JEFM' ] :
                                    Value = Tsc.values[-1]
                                    # If value is missing raise an exception and using the missing value
                                    if Value == Constants.UNDEFINED : raise ValueError
    
                                    if data == 'Storage' :
                                        Value = Tsc.values[-1]/1000
                                    Value = round(Value, 0)
                                else :
                                    Value = Null
                                    
                                # Create a formatted string that will be added to the table
                                if Value == Null :
                                    CellData = Phrase(Chunk(Null, TextFont))
                                else :
                                    if (data == 'FlowIn' or data == 'Storage') and project in ['FTPK', 'GARR', 'OAHE', 'BEND', 'FTRA', 'GAPT','CAFE', 'HAST', 'BAGL'] :
    
                                        #Check for valid values FlowIn or Storage
                                        if int(Value) >= 0 :          
                                            CellData = Chunk(TableLayoutDict[TableName][ColumnKey][1]['Format'].format(int(Value)), TextFont)
                                        else :
                                            CellData = Chunk(Missing, TextFont)
    
                                        #Check for valid values Storage or Energy
                                        outputDebug(debug, lineNo(), '%s %s' % (project, data))
                                        if int(Value2) >= 0 : 
                                            if (data == 'Storage') and project in ['BAGL', 'CAFE']:
                                                cellx = '%s' % ('--')
                                                CellData2 = Chunk(cellx)
                                            else:
                                                CellData2 = Chunk(TableLayoutDict[TableName][ColumnKey][2]['Format'].format(int(Value2)), TextFont)
                                        else :
                                            CellData2 = Chunk(Missing, TextFont)
    
                                        #Combine the 2 cells of data
                                        cellInfo = '%s / %s' % (CellData, CellData2)
                                        CellData = Phrase(cellInfo, TextFont)
                                    else :
                                        if data == 'FlowIn' :
                                            CellData = Phrase(Chunk(TableLayoutDict[TableName][ColumnKey][1]['Format'].format(int(Value)), TextFont)) #formatting, comma separator
                                        else :
                                            CellData = Phrase(Chunk(TableLayoutDict[TableName][ColumnKey]['Format'].format(int(Value)), TextFont))
                        except :
                            Value = Missing
                            # Create a formatted string that will be added to the table
                            if data == 'Storage' and project == 'SYS' :
                                CellData = Phrase(Missing + '\n' + Missing + '\n' + Missing, TextFont)
                            else :
                                CellData = Phrase(Chunk(Missing, TextFont))
                    else :
                        #Timeseries not found in the database, set value to Null
                        CellData = Phrase(Chunk(Null, TextFont))
                else :
                    if project in ['SYS'] and data == 'FlowIn' :
                        ColSpan = 0
                    else :
                        #None, set value to Null
                        CellData = Phrase(Chunk(Null, TextFont))    
            # 7  AirTempMax, AirTempMin, and Precip
            elif data == 'AirTempMax' or data == 'AirTempMin' or data == 'Precip' :
                if str(DataBlockDict['DataBlocks'][TableDataName][data]) != 'None' :
                    if project in ['FTPK', 'GARR', 'OAHE', 'BEND', 'FTRA', 'GAPT'] :
                        TscPathname = DataBlockDict['DataBlocks'][TableDataName][data] % project
                    elif project == 'CAFE' and DataBlockDict['DataBlocks'][TableDataName][data][0] != None :
                        TscPathname = DataBlockDict['DataBlocks'][TableDataName][data][0] % project
                    elif project in ['HAST', 'BAGL'] and DataBlockDict['DataBlocks'][TableDataName][data][0] != None  :
                        TscPathname = DataBlockDict['DataBlocks'][TableDataName][data][1] % project
                    else :
                        TscPathname = DataBlockDict['DataBlocks'][TableDataName][data] % project

                    if checkTs(TscPathname, conn) == 'true' :
                    #if TscPathname in DbPathnameList :
                        # Set database time zone to GMT-6 so met data can be retrieved
                        try :
                            Value = 0.
                            CellData = Phrase(Chunk(Missing, TextFont))
                            Tsc = CwmsDb.get(TscPathname, startTime, endTime)
            
                            if Tsc != Null :
                                Value = Tsc.values[-1]
                            
                            if project in ['CAFE'] and data == 'Precip' :
                                CellData = Phrase(Chunk(Null, TextFont))
                            elif project in ['CAFE', 'BAGL', 'HAST' ] and (data == 'AirTempMax' or data == 'AirTempMin') :
                                CellData = Phrase(Chunk(Null, TextFont))
                            elif data == 'Precip' :
                                if float(Value) > -1 :
                                    CellData = Phrase(Chunk(TableLayoutDict[TableName][ColumnKey]['Format'] % float(Value), TextFont))
                                else :
                                    CellData = Phrase(Chunk(Missing, TextFont))    
                            else:
                                Value = round(Value, 0)
                                if Value > -60 :
                                    CellData = Phrase(Chunk(TableLayoutDict[TableName][ColumnKey]['Format'].format(int(Value)), TextFont))
                                else :
                                    CellData = Phrase(Chunk(Missing, TextFont))
            
                        except Exception as e :
                            print("AirTemp/Precip Exception = " + str(e) + "; Tsc = " + TscPathname)
                            CellData = Phrase(Chunk(Missing, TextFont))
                        
                        # Reset database time zone to US/Central
                        #CwmsDb.setTimeZone('US/Central')
                    else :
                        #Timeseries not found in the database, set value to Null
                        CellData = Phrase(Chunk(Null, TextFont))
                else :
                    #Precip set to None
                    if project in ['SYS'] and data == 'Precip':
                        CellData = Phrase(Chunk('KAF\nKAF\nMwh', TextFont))
                        HorizontalAlignment = Element.ALIGN_LEFT 
                    else :    
                        CellData = Phrase(Chunk(Null, TextFont))    

            if project in ['SYS'] and data == 'FlowIn' :
                print("SYS project has no Flow-In values.")
            else : 
                if data == 'AirTempMin' and not LastProject and project != 'CEIA' :
                    BorderWidths = [0.25, 1, 0.25, 0.25] #[Top, Right, Bottom, Left] 
                elif data == 'AirTempMin' and (LastProject or project == 'CEIA') :
                    BorderWidths = [0.25, 1, 1, 0.25] #[Top, Right, Bottom, Left]
    
                if (LastProject or project == 'CEIA') and data != 'AirTempMin' and data != 'PublicName'  and data != 'RiverMile' : 
                    BorderWidths = [0.25, 0.25, 1, 0.25] #[Top, Right, Bottom, Left]
                Cell = createCell(debug, CellData, RowSpan, ColSpan, HorizontalAlignment, VerticalAlignment, CellPadding, BorderColors, BorderWidths, VariableBorders, BackgroundColor)
                Table.addCell(Cell)
    
                # Add data to CsvData. Break data loop if column span reaches the total number columns before each data piece has been added to that table
                outputDebug(debug, lineNo(), 'ColSpan = ', ColSpan)
                TotalColSpan += ColSpan
                UnformattedData = str(CellData[0]).replace(',', '')
                CsvData += UnformattedData
                CsvData += ','
                outputDebug(debug, lineNo(), 'TotalColSpan = ', TotalColSpan)
                if TotalColSpan == Table.getNumberOfColumns() :
                    CsvData += '\n'
                    break
                
        #
        # Add Continued Heading for second page
        #
        if project == 'CEIA' :
            HeadingCont = DataBlockDict['DataBlocks'][TableDataName]['Heading'] + ' Cont.'
            # createCell(debug, CellData, RowSpan, ColSpan, HorizontalAlignment, VerticalAlignment, CellPadding, BorderColors, BorderWidths, VariableBorders, BackgroundColor)
            Cell = createCell(debug, Phrase(Chunk(HeadingCont, Font9)), TableLayoutDict[TableName]['RowSpan'],
                    Table1Columns, Element.ALIGN_LEFT, TableLayoutDict[TableName]['VerticalAlignment'], [0, 2, 2, 2], TableLayoutDict[TableName]['BorderColors'],
                    [0.5, 1, 0.25, 1], TableLayoutDict[TableName]['VariableBorders'], Color7)
            Table.addCell(Cell)

    return Table, CsvData    
#
# table1Heading Function    : Creates the heading for Table1 in the bulletin
# Author/Editor             : Ryan Larsen
# Last updated              : 12-12-2017
#
def table1Heading(  debug,  # Set to True to print all debug statements
                    Table,  # PdfPTable object
                    CsvData # Csv data
                    ) :
    #
    # Create Table1 Heading 
    #
    # Column 0 Heading
    #createCell(debug, CellData, RowSpan, ColSpan, HorizontalAlignment, VerticalAlignment, CellPadding, BorderColors, BorderWidths, VariableBorders, BackgroundColor)
    #0
    CellData = Phrase(Chunk('Station', TableLayoutDict['Table1']['TextFont']))
    BorderWidths = [1, 0.5, 0.25, 1] #[Top, Right, Bottom, Left]
    Cell = createCell(debug,CellData, 3, TableLayoutDict['Table1']['ColSpan'], Element.ALIGN_CENTER, 
        TableLayoutDict['Table1']['VerticalAlignment'], TableLayoutDict['Table1']['CellPadding'], [Color2, Color3, Color2, Color2], 
        BorderWidths, TableLayoutDict['Table1']['VariableBorders'], Color6)
    Cell.setFixedHeight(25)
    Table.addCell(Cell)
    # Add data to CsvData.
    UnformattedData = str(CellData[0]).replace(',', '') + ','
    CsvData += UnformattedData

    #1
    CellData = Phrase(Chunk('Miles above\nMissouri R\nMouth (1960)', TableLayoutDict['Table1']['TextFont']))
    BorderWidths = [1, 0.25, 0.25, 0.5] #[Top, Right, Bottom, Left]
    Cell = createCell(debug, CellData, 3, 1, Element.ALIGN_CENTER, 
        TableLayoutDict['Table1']['VerticalAlignment'], TableLayoutDict['Table1']['CellPadding'], [Color2, Color2, Color2, Color3], 
        BorderWidths, TableLayoutDict['Table1']['VariableBorders'], Color6)
    Cell.setFixedHeight(25)
    Table.addCell(Cell)
    # Add data to CsvData.
    UnformattedData = str(CellData[0]).replace(',', '') + ','
    CsvData += UnformattedData

    #2
    CellData = Phrase(Chunk('Elev\nDatum\n(ft msl)', TableLayoutDict['Table1']['TextFont']))
    BorderWidths = [1, 0.25, 0.25, 0.25] #[Top, Right, Bottom, Left]
    Cell = createCell(debug, CellData, 3, 1, Element.ALIGN_CENTER, 
        TableLayoutDict['Table1']['VerticalAlignment'], TableLayoutDict['Table1']['CellPadding'], [Color2, Color2, Color2, Color2], 
        BorderWidths, TableLayoutDict['Table1']['VariableBorders'], Color6)
    Cell.setFixedHeight(25)
    Table.addCell(Cell)
    # Add data to CsvData.
    UnformattedData = str(CellData[0]).replace(',', '') + ','
    CsvData += UnformattedData

    #3
    CellData = Phrase(Chunk('Flood\nStage\n(feet)', TableLayoutDict['Table1']['TextFont']))
    BorderWidths = [1, 0.25, 0.25, 0.25] #[Top, Right, Bottom, Left]
    Cell = createCell(debug, CellData, 3, 1, Element.ALIGN_CENTER, 
        TableLayoutDict['Table1']['VerticalAlignment'], TableLayoutDict['Table1']['CellPadding'], [Color2, Color2, Color2, Color2], 
        BorderWidths, TableLayoutDict['Table1']['VariableBorders'], Color6)
    Cell.setFixedHeight(25)
    Table.addCell(Cell)
    # Add data to CsvData.
    UnformattedData = str(CellData[0]).replace(',', '') + ','
    CsvData += UnformattedData

    #4
    CellData = Phrase(Chunk('Gage\nReading\n(feet)', TableLayoutDict['Table1']['TextFont']))
    BorderWidths = [1, 0.25, 0.25, 0.25] #[Top, Right, Bottom, Left]
    Cell = createCell(debug, CellData, 3, 1, Element.ALIGN_CENTER, 
        TableLayoutDict['Table1']['VerticalAlignment'], TableLayoutDict['Table1']['CellPadding'], [Color2, Color2, Color2, Color2], 
        BorderWidths, TableLayoutDict['Table1']['VariableBorders'], Color6)
    Cell.setFixedHeight(25)
    Table.addCell(Cell)
    # Add data to CsvData.
    UnformattedData = str(CellData[0]).replace(',', '') + ','
    CsvData += UnformattedData

    #5
    CellData = Phrase(Chunk('24-Hr\nChange\n(feet)', TableLayoutDict['Table1']['TextFont']))
    BorderWidths = [1, 0.25, 0.25, 0.25] #[Top, Right, Bottom, Left]
    Cell = createCell(debug, CellData, 3, 1, Element.ALIGN_CENTER, 
        TableLayoutDict['Table1']['VerticalAlignment'], TableLayoutDict['Table1']['CellPadding'], [Color2, Color2, Color2, Color2], 
        BorderWidths, TableLayoutDict['Table1']['VariableBorders'], Color6)
    Cell.setFixedHeight(25)
    Table.addCell(Cell)
    # Add data to CsvData.
    UnformattedData = str(CellData[0]).replace(',', '') + ','
    CsvData += UnformattedData

    #6
    CellData = Phrase(Chunk('Estimated\nDischarge\nIn / Out (cfs)', TableLayoutDict['Table1']['TextFont']))
    BorderWidths = [1, 0.25, 0.25, 0.25] #[Top, Right, Bottom, Left]
    Cell = createCell(debug, CellData, 3, 1, Element.ALIGN_CENTER, 
        TableLayoutDict['Table1']['VerticalAlignment'], TableLayoutDict['Table1']['CellPadding'], [Color2, Color2, Color2, Color2], 
        BorderWidths, TableLayoutDict['Table1']['VariableBorders'], Color6)
    Cell.setFixedHeight(25)
    Table.addCell(Cell)
    # Add data to CsvData.
    UnformattedData = str(CellData[0]).replace(',', '') + ','
    CsvData += UnformattedData

    #7
    CellData = Phrase(Chunk('Actual\nStor / Gen\n(KAF/Mwh)', TableLayoutDict['Table1']['TextFont']))
    BorderWidths = [1, 0.25, 0.25, 0.25] #[Top, Right, Bottom, Left]
    Cell = createCell(debug, CellData, 3, 1, Element.ALIGN_CENTER, 
        TableLayoutDict['Table1']['VerticalAlignment'], TableLayoutDict['Table1']['CellPadding'], [Color2, Color2, Color2, Color2], 
        BorderWidths, TableLayoutDict['Table1']['VariableBorders'], Color6)
    Cell.setFixedHeight(25)
    Table.addCell(Cell)
    # Add data to CsvData.
    UnformattedData = str(CellData[0]).replace(',', '') + ','
    CsvData += UnformattedData

    #8
    CellData = Phrase(Chunk('24-Hr\nPrecip\n(in)', TableLayoutDict['Table1']['TextFont']))
    BorderWidths = [1, 0.25, 0.25, 0.25] #[Top, Right, Bottom, Left]
    Cell = createCell(debug, CellData, 3, 1, Element.ALIGN_CENTER, 
        TableLayoutDict['Table1']['VerticalAlignment'], TableLayoutDict['Table1']['CellPadding'], [Color2, Color2, Color2, Color2], 
        BorderWidths, TableLayoutDict['Table1']['VariableBorders'], Color6)
    Cell.setFixedHeight(25)
    Table.addCell(Cell)
    # Add data to CsvData.
    UnformattedData = str(CellData[0]).replace(',', '') + ','
    CsvData += UnformattedData

    #9
    CellData = Phrase(Chunk('Air Temp\n(deg F)\nHi  |  Lo', TableLayoutDict['Table1']['TextFont']))
    BorderWidths = [1, 1, 0.25, 0.25] #[Top, Right, Bottom, Left]
    Cell = createCell(debug, CellData, 3, 2, Element.ALIGN_CENTER, 
        TableLayoutDict['Table1']['VerticalAlignment'], TableLayoutDict['Table1']['CellPadding'], [Color2, Color2, Color2, Color2], 
        BorderWidths, TableLayoutDict['Table1']['VariableBorders'], Color6)
    Cell.setFixedHeight(25)
    Table.addCell(Cell)
    # Add data to CsvData.
    UnformattedData = str(CellData[0]).replace(',', '') + ','
    CsvData += UnformattedData

    return Table, CsvData

#
# titleBlock Function   : Creates the title block for the bulletin
# Author/Editor         : Ryan Larsen
# Last updated          : 12-12-2017
#
def titleBlock( debug,      # Set to True to print all debug statements
                TitleBlock, # PdfPTable object
                CsvData     # Csv data
                ) :
    #
    # Add USACE Logo, title block lines, and seal to TitleBlock
    #
    TitleLines = [TitleLine1, TitleLine2, TitleLine3, TitleLine4]

    # Add the USACE Logo to the TitleBlock
    Img = Image.getInstance(UsaceLogo)
    Cell = PdfPCell(Img, 1)
    Cell.setRowspan(len(TitleLines))
    Cell.setHorizontalAlignment(Element.ALIGN_LEFT); Cell.setVerticalAlignment(TableLayoutDict['Table1']['VerticalAlignment'])
    Cell.setPaddingTop(2); Cell.setPaddingRight(2); Cell.setPaddingBottom(2); Cell.setPaddingLeft(2)
    Cell.setBorder(Rectangle.NO_BORDER)
    Cell.setFixedHeight(60.)
    TitleBlock.addCell(Cell)
    
    # Add TitleLine1 to the TitleBlock
    # createCell(debug, CellData, RowSpan, ColSpan, HorizontalAlignment, VerticalAlignment, CellPadding, BorderColors, BorderWidths, VariableBorders, BackgroundColor)
    CellData = Phrase(Chunk(TitleLine1, Font1))
    Cell = createCell(debug, CellData, TableLayoutDict['Table1']['RowSpan'], Table1Columns - 2, TableLayoutDict['Table1']['HorizontalAlignment'], 
        TableLayoutDict['Table1']['VerticalAlignment'], [2, 5, 2, 2], TableLayoutDict['Table1']['BorderColors'], 
        TableLayoutDict['Table1']['BorderWidths'], TableLayoutDict['Table1']['VariableBorders'], TableLayoutDict['Table1']['BackgroundColor'])
    Cell.setBorder(Rectangle.NO_BORDER)
    TitleBlock.addCell(Cell)
    # Add data to CsvData.
    UnformattedData = str(CellData[0]).replace(',', '') + ',\n'
    CsvData += UnformattedData
    
    # Add the seal to the TitleBlock
    Img = Image.getInstance(Seal)
    Cell = PdfPCell(Img, 1)
    Cell.setRowspan(len(TitleLines))
    Cell.setHorizontalAlignment(TableLayoutDict['Table1']['HorizontalAlignment']); Cell.setVerticalAlignment(TableLayoutDict['Table1']['VerticalAlignment'])
    Cell.setPaddingTop(2); Cell.setPaddingRight(2); Cell.setPaddingBottom(2); Cell.setPaddingLeft(2)
    Cell.setBorder(Rectangle.LEFT); Cell.setBorderColorLeft(Color1); Cell.setBorderWidthLeft(0.5)
    Cell.setFixedHeight(60.)
    TitleBlock.addCell(Cell)
    
    # Add TitleLine2 to the TitleBlock
    # createCell(debug, CellData, RowSpan, ColSpan, HorizontalAlignment, VerticalAlignment, CellPadding, BorderColors, BorderWidths, VariableBorders, BackgroundColor)
    CellData = Phrase(Chunk(TitleLine2, Font2))
    Cell = createCell(debug, Phrase(Chunk(TitleLine2, Font2)), TableLayoutDict['Table1']['RowSpan'], Table1Columns - 2, TableLayoutDict['Table1']['HorizontalAlignment'], 
        TableLayoutDict['Table1']['VerticalAlignment'], [0, 5, 2, 2], TableLayoutDict['Table1']['BorderColors'], 
        TableLayoutDict['Table1']['BorderWidths'], TableLayoutDict['Table1']['VariableBorders'], TableLayoutDict['Table1']['BackgroundColor'])
    Cell.setBorder(Rectangle.NO_BORDER)
    TitleBlock.addCell(Cell)
    # Add data to CsvData.
    UnformattedData = str(CellData[0]).replace(',', '') + ',\n'
    CsvData += UnformattedData
    
    # Add TitleLine3 to the TitleBlock
    # createCell(debug, CellData, RowSpan, ColSpan, HorizontalAlignment, VerticalAlignment, CellPadding, BorderColors, BorderWidths, VariableBorders, BackgroundColor)
    CellData = Phrase(Chunk(TitleLine3 % ProjectDateTimeStr, Font1))
    Cell = createCell(debug, Phrase(Chunk(TitleLine3 % ProjectDateTimeStr, Font1)), TableLayoutDict['Table1']['RowSpan'], Table1Columns - 2, 
        TableLayoutDict['Table1']['HorizontalAlignment'], TableLayoutDict['Table1']['VerticalAlignment'], [0, 5, 2, 2], TableLayoutDict['Table1']['BorderColors'], 
        TableLayoutDict['Table1']['BorderWidths'], TableLayoutDict['Table1']['VariableBorders'], TableLayoutDict['Table1']['BackgroundColor'])
    Cell.setBorder(Rectangle.NO_BORDER)
    TitleBlock.addCell(Cell)
    # Add data to CsvData.
    UnformattedData = str(CellData[0]).replace(',', '') + ',\n'
    CsvData += UnformattedData

    # Add TitleLine4 to the TitleBlock
    # createCell(debug, CellData, RowSpan, ColSpan, HorizontalAlignment, VerticalAlignment, CellPadding, BorderColors, BorderWidths, VariableBorders, BackgroundColor)
    CellData = Phrase(Chunk(TitleLine4 % CurDateTimeStr, Font1))
    Cell = createCell(debug, Phrase(Chunk(TitleLine4 % CurDateTimeStr, Font1)), TableLayoutDict['Table1']['RowSpan'], Table1Columns - 2, 
        TableLayoutDict['Table1']['HorizontalAlignment'], TableLayoutDict['Table1']['VerticalAlignment'], [0, 5, 2, 2], TableLayoutDict['Table1']['BorderColors'], 
        TableLayoutDict['Table1']['BorderWidths'], TableLayoutDict['Table1']['VariableBorders'], TableLayoutDict['Table1']['BackgroundColor'])
    Cell.setBorder(Rectangle.NO_BORDER)
    TitleBlock.addCell(Cell)
    # Add data to CsvData.
    UnformattedData = str(CellData[0]).replace(',', '') + ',\n'
    CsvData += UnformattedData

    # Add a blank line to the TitleBlock to separate the TitleBlock from the table
    # createCell(debug, CellData, RowSpan, ColSpan, HorizontalAlignment, VerticalAlignment, CellPadding, BorderColors, BorderWidths, VariableBorders, BackgroundColor)
    Cell = createCell(debug, Phrase(Chunk('', Font1)), TableLayoutDict['Table1']['RowSpan'], Table1Columns, TableLayoutDict['Table1']['HorizontalAlignment'], 
        TableLayoutDict['Table1']['VerticalAlignment'], TableLayoutDict['Table1']['CellPadding'], TableLayoutDict['Table1']['BorderColors'], 
        TableLayoutDict['Table1']['BorderWidths'], TableLayoutDict['Table1']['VariableBorders'], TableLayoutDict['Table1']['BackgroundColor'])
    Cell.setBorder(Rectangle.NO_BORDER)
    Cell.setFixedHeight(4)
    TitleBlock.addCell(Cell)

    return TitleBlock, CsvData

##################################################################
# Main Script
##################################################################
try :    
    #
    # Date and Time Window Info
    #
    CurDateTime = datetime.datetime.now()
    CurDateTimeStr  = CurDateTime.strftime('%m-%d-%Y %H:%M') # Last updated time for bulletin formatted as mm-dd-yyyy hhmm
    ArchiveDateTimeStr  = CurDateTime.strftime('%d%m%Y') # Last updated time for bulletin formatted as dd-mm-yyyy
    if UseCurDate :
        Date = datetime.datetime.now() # Current date
    else :
        TimeObj = time.strptime(HistoricBulletinDate, '%d%b%Y %H%M')
        TimeObj = localtime(mktime(TimeObj)) # Convert TimeObj to local time so it includes the DST component
        Date    = datetime.datetime.fromtimestamp(mktime(TimeObj))


    #DB Time
    StartTw             = Date - datetime.timedelta(2)
    StartTwStr          = StartTw.strftime('%d%b%Y 2400') 
    EndTw               = Date - datetime.timedelta(1)
    EndTribTwStr        = Date
    EndTwStr            = EndTribTwStr.strftime('%d%b%Y 2400')

    #Tributaries
    if is_dst(str(Date)):
       #DST had started (March) 
       StartTribTwStr      = EndTw.strftime('%d%b%Y 1100')
       EndTribStr          = EndTribTwStr.strftime('%d%b%Y 1100') 
    else:
       #DST has ended (in November)
       StartTribTwStr      = EndTw.strftime('%d%b%Y 1200')
       EndTribStr          = EndTribTwStr.strftime('%d%b%Y 1200')

    #Select Tribs
    TrimTwStr2          = EndTw.strftime('%d%b%Y 0600') 
    EndTwStr2           = Date.strftime('%d%b%Y 0600')
    EndTribTw           = Date - datetime.timedelta(2)
    TribTrimTwStr       = EndTribTw.strftime('%d%b%Y 0100')
    TribEndTwStr        = EndTribTw.strftime('%d%b%Y 2400') 
    
    # Mainstem 
    if is_dst(str(Date)):
       #DST had started (March)     
       EndMainStem      = EndTribTwStr.strftime('%d%b%Y 0500')
       StartMainStem    = EndTw.strftime('%d%b%Y 0600')
    else:
       #DST has ended (in November)
       EndMainStem         = EndTribTwStr.strftime('%d%b%Y 0600')
       StartMainStem       = EndTw.strftime('%d%b%Y 0700')
       
    StartMainStemStor   = EndTw.strftime('%d%b%Y 0500')

    ProjectDateTimeStr  = CurDateTime.strftime('%m-%d-%Y 00:00') 
    outputDebug(debug, lineNo(), 'Start of Time Window = ', StartTwStr, '\tEnd of Time Window = ', EndTwStr, 
        '\tProject Date and Time = ', ProjectDateTimeStr)
    
    #
    # Open database connection
    #
    CwmsDb = RADARAPI.open(office=OFFICE, url=CDA_URL) if USE_CDA else DBAPI.open()
    CwmsDb.setTimeZone('UTC')
    CwmsDb.setTimeWindow(StartTwStr, EndTwStr)
    CwmsDb.setOfficeId(OFFICE)
    CwmsDb.setTrimMissing(False)

    if USE_CDA:
        conn = CwmsDb
    else:
        conn = CwmsDb.getConnection()# Create a java.sql.Connection

    # Get list of pathnames in database
    DbPathnameList = CwmsDb.getPathnameList()

    # 
    # Retrieve public names for all projects shown in bulletin. Remove 'Reservoir' from public name for spacing purposes
    #
    locations = list(LocationDict.keys())
    for location in locations :
        PublicName = retrievePublicName(debug, conn, CwmsDb, location)
        BulletinName = PublicName.replace(' & Reservoir', '')
        LocationDict[location].setdefault('PublicName', PublicName)
        LocationDict[location].setdefault('BulletinName', BulletinName)
    
    #
    # Create tables with a finite number of columns that will be written to the pdf file
    #
    # TitleBlock: Contains the title block for the bulletin
    TitleBlock = PdfPTable(Table1Columns)
    
    # Table1: Contains all data and data headings
    Table1 = PdfPTable(Table1Columns)

    # Table1Footnote: Contains the footnotes for Table1
    Table1Footnote = PdfPTable(Table1Columns)

    # BulletinFooter: Footer for the bulletin
    BulletinFooter = PdfPTable(FooterColumns)
    BulletinFooter2 = PdfPTable(FooterColumns)
    
    #
    # Specify column widths
    #
    # Title Block Columns
    TitleBlockColumnWidths = [10] * Table1Columns
    TitleBlockColumnWidths[0] = 25
    TitleBlockColumnWidths[-1] = 17
    if IS_CPYTHON: TitleBlockColumnWidths = JInt[:](TitleBlockColumnWidths)
    TitleBlock.setWidths(TitleBlockColumnWidths)
    
    # Table Columns and Order of Variables for Table1
    DataOrder, ColumnWidths = [], []
    for column in range(Table1Columns) :
        # Column Key
        ColumnKey = 'Column%d' % column

        if column == 6 or column == 7 :
           for x in range(1, 2):
              DataOrder.append(TableLayoutDict['Table1'][ColumnKey][1]['Key'])
              ColumnWidths.append(TableLayoutDict['Table1'][ColumnKey][1]['ColumnWidth'])
        else :
            DataOrder.append(TableLayoutDict['Table1'][ColumnKey]['Key'])
            ColumnWidths.append(TableLayoutDict['Table1'][ColumnKey]['ColumnWidth'])
    if IS_CPYTHON: ColumnWidths = JInt[:](ColumnWidths)
    Table1.setWidths(ColumnWidths)
    
    #
    # Add data to Title Block that will be at the top of the bulletin
    #
    CsvData = ''
    TitleBlock, CsvData = titleBlock(debug, TitleBlock, CsvData)

    #
    # Add data to the heading for Table1
    #
    Table1, CsvData = table1Heading(debug, Table1, CsvData)
    
    #
    # Add data to the data blocks for Table1
    #
    NumberOfDataBlocks = len(list(DataBlockDict['DataBlocks'].keys()))
    for x in range(1, NumberOfDataBlocks + 1, 1) :
        DataBlock = 'Data%d' % x
        if DataBlock == 'Data1' :#or DataBlock == 'Data2' :
            startTime = StartMainStem
            outputDebug(debug, lineNo(), 'startTime = ', startTime)
            endTime = EndMainStem
            outputDebug(debug, lineNo(), 'endTime = ', endTime)
            Table1, CsvData = table1Data(debug, Table1, 'Table1', DataBlock, startTime, endTime, StartMainStemStor, endTime, CsvData, DbPathnameList)
        else :
            startTime = StartTribTwStr
            endTime = EndTribStr
            Table1, CsvData = table1Data(debug, Table1, 'Table1', DataBlock, startTime, endTime, '', '', CsvData, DbPathnameList) 
        '''
        elif DataBlock == 'Data3' :
            startTime = TrimTwStr2
            endTime = EndTwStr2
            Table1, CsvData = table1Data(debug, Table1, 'Table1', DataBlock, startTime, endTime, TribTrimTwStr, TribEndTwStr, CsvData)
        else :
            startTime = StartTribTwStr
            endTime = EndTribStr
            Table1, CsvData = table1Data(debug, Table1, 'Table1', DataBlock, startTime, endTime, '', '', CsvData) 
        '''

    #
    # Create bulletin header that is repeated on each page
    #
    Table1.setHeaderRows(3)

    #
    # Create Pdf file and write tables to create bulletin
    #
    filenames = [BulletinFilename, ArchiveBulletinFilename % ArchiveDateTimeStr]
    for filename in filenames :
        BulletinPdf = Document()
        Writer = PdfWriter.getInstance(BulletinPdf, FileOutputStream(filename))
        BulletinPdf.setPageSize(PageSize.LETTER) # Set the page size
        PageWidth = BulletinPdf.getPageSize().getWidth()
        BulletinPdf.setMargins(LeftMargin, RightMargin, TopMargin, BottomMargin) # Left, Right, Top, Bottom
        BulletinPdf.setMarginMirroring(True) 
        BulletinPdf.open()
        # Build a footer with page numbers and add to PDF. Only need to build it one time.
        if filename == filenames[0] :
            BulletinFooter = bulletinFooter(debug, BulletinFooter)
        BulletinFooter.setTotalWidth(PageWidth - 48) # Total width is 612 pixels (8.5 inches) minus the left and right margins (24 pixels each)
        BulletinFooter.writeSelectedRows(0, -1, 24, 36, Writer.getDirectContent())
        BulletinPdf.add(TitleBlock) # Add TitleBlock to the PDF
        BulletinPdf.add(Table1) # Add Table1 to the PDF
        BulletinPdf.add(Table1Footnote) # Add Table1's footnotes
        # Build a footer with page numbers and add to PDF. Only need to build it one time.
        if filename == filenames[0] :
            BulletinFooter2 = bulletinFooter(debug, BulletinFooter2)
        BulletinFooter2.setTotalWidth(PageWidth - 48)
        BulletinFooter2.writeSelectedRows(0, -1, 24, 36, Writer.getDirectContent()) #Add the footer
        BulletinPdf.close()
        Writer.close()

    #
    # Create csv file
    #
    CsvFile = open(CsvFilename, 'w')
    CsvFile.write(CsvData)

finally :
    try : CwmsDb.done()
    except : pass
    try : conn.close()
    except : pass
    try : BulletinPdf.close()
    except : pass
    try : Writer.close()
    except : pass
    try : CsvFile.close()
    except : pass
    try : BulletinTsFile.close()
    except : pass
    try : BulletinProperties.close()
    except : pass
