# Java libraries
from com.itextpdf.layout import Document, Canvas
from com.itextpdf.layout.borders import Border
from com.itextpdf.kernel.geom import Rectangle, PageSize
from com.itextpdf.layout.properties import TextAlignment, UnitValue, VerticalAlignment
from com.itextpdf.layout.element import Paragraph, Table, Cell, Image
from com.itextpdf.kernel.pdf import PdfDocument, PdfWriter, PdfUAConformance, PdfName
from com.itextpdf.kernel.pdf.canvas import PdfCanvas
from com.itextpdf.kernel.pdf.tagging import StandardRoles
#from com.itextpdf.kernel.pdf.tagutils import TagTreePointer
from com.itextpdf.pdfua import PdfUADocument, PdfUAConfig
from com.itextpdf.io.font import PdfEncodings
from com.itextpdf.kernel.font import PdfFont, PdfFontFactory;
from com.itextpdf.io.image import ImageDataFactory

# Python Libraries
import os
import cwms
import pandas as pd
import datetime
from zoneinfo import ZoneInfo

PDF_NAME = 'example_508'
API_ROOT = os.environ.get('CDA_URL')
OFFICE_ID = os.environ.get('OFFICE_ID')
TZ = ZoneInfo(os.environ.get('TZ'))

REGULAR_FONT_NAME = "dejavu sans"
HEADER_FONT_NAME = "dejavu sans bold"
MONOSPACE_FONT_NAME = "dejavu sans mono"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M %Z"
MARGINS = {
	'TOP': 0.85,
	'BOTTOM': 0.75,
}

class UnitConverter:
	@classmethod
	def uu2inch(cls, uu: float) -> float: return uu / 72.0
	@classmethod
	def inch2uu(cls, inch: float) -> float: return inch * 72.0
	@classmethod
	def inch2mm(cls, uu: float) -> float: return uu / 25.4
	@classmethod
	def mm2inch(cls, mm: float) -> float: return mm * 25.4
	@classmethod
	def uu2mm(cls, uu: float) -> float: return cls.inch2mm(cls.uu2inch(uu))
	@classmethod
	def mm2uu(cls, mm: float) -> float: return cls.inch2uu(cls.mm2inch(mm))

def createFont(name: str) -> PdfFont:
	"""Loads a registered font. Ensures all fonts are loaded with correct configuration for PDF/UA."""
	# If unicode is required, such as for accent characters, replace WINANSI with UTF8
	return PdfFontFactory.createRegisteredFont(name, PdfEncodings.WINANSI, PdfFontFactory.EmbeddingStrategy.FORCE_EMBEDDED)

def initializeDocument(filename: str, title: str) -> tuple[Document, PdfDocument]:
	"""
	Creates a new PDF/UA-1 document. PDF/UA-1 is required for User accessibility and Archiving.
	"""
	pdf = PdfUADocument(PdfWriter(filename), PdfUAConfig(PdfUAConformance.PDF_UA_1, title, "en-US"))

	info = pdf.getDocumentInfo()
	info.setAuthor("United States Army Corps of Engineers")
	info.setSubject("Hourly Report")
	doc = Document(pdf, PageSize.LETTER, False)

	# Set the font for the document; per PDF/UA specification, it must be embedded
	PdfFontFactory.registerSystemDirectories()
	font = createFont(REGULAR_FONT_NAME)
	doc.setFont(font)
	doc.setTopMargin(UnitConverter.inch2uu(MARGINS['TOP']))
	doc.setBottomMargin(UnitConverter.inch2uu(MARGINS['BOTTOM']))

	return doc, pdf

def addHeader(document: Document, pdf: PdfDocument, diffFirstPage: bool = True):
	"""
	Adds the USACE header to the document. This is marked as an artifact so it's ignored by screen readers.
	"""
	# The header is placed at the top of the document, 1/4" from the top

	# This content is not read by screen readers. Any vital information (e.g classification level)
	# must also be placed at the beginning of the document or section with the content.
	numPages = pdf.getNumberOfPages()
	TOP_OFFSET = UnitConverter.inch2uu(1/4)
	IMG_SIZE_IN = 0.7125

	for pageId in range(1, numPages + 1):
		page = pdf.getPage(pageId)
		top = page.getPageSize().getHeight() - TOP_OFFSET

		if diffFirstPage == False or diffFirstPage and pageId == 1:
			pdfcanvas = PdfCanvas(page)
			rect = Rectangle(doc.getLeftMargin(), top - doc.getTopMargin(), page.getPageSize().getWidth() - doc.getLeftMargin() - doc.getRightMargin(), doc.getTopMargin())
			canvas = Canvas(pdfcanvas, rect)
			canvas.setFont(createFont(REGULAR_FONT_NAME))
			canvas.enableAutoTagging(page)

			table = Table(UnitValue.createPercentArray([1,5]))
			table.getAccessibilityProperties().setRole(StandardRoles.ARTIFACT)
			table.setWidth(UnitValue.createPercentValue(50))
			table.setTextAlignment(TextAlignment.LEFT)

			img = Image(ImageDataFactory.create('./Seals_and_Symbols/USACE_Logo.png'))
			img.getAccessibilityProperties().setRole(StandardRoles.ARTIFACT)
			img.scaleToFit(UnitConverter.inch2uu(IMG_SIZE_IN), UnitConverter.inch2uu(IMG_SIZE_IN))
			# Images require an alternative description (in this case it's an artifact, so not required; left as an example)
			img.getAccessibilityProperties().setAlternateDescription("USACE Logo")

			table.addCell(Cell().add(img).setBorder(Border.NO_BORDER))
			table.addCell(Cell().add(Paragraph('US Army Corps of Engineers\nSacramento District')).setBorder(Border.NO_BORDER))
			canvas.add(table)
		else:
			p = Paragraph("US Army Corps of Engineers")
			doc.showTextAligned(p, doc.getLeftMargin(), top, pageId, TextAlignment.LEFT, VerticalAlignment.TOP, 0)

			p = Paragraph("Sacramento District")
			doc.showTextAligned(p, page.getPageSize().getWidth() - doc.getRightMargin(), top, pageId, TextAlignment.RIGHT, VerticalAlignment.TOP, 0)


def addFooter(document: Document, pdf: PdfDocument, title: str, alternate: bool = False):
	"""
	Adds the page number and total number of pages to the footer of each page.
	"""
	# Footer is placed 1/3" from the bottom of the page, as some printers can't physically print lower than that.

	# This content is not read by screen readers, so keep it to repetitive, unimportant information.
	# Any vital information (e.g classification level) must also be placed at the beginning of the document or section with the content.
	numPages = pdf.getNumberOfPages()
	BOTTOM_OFFSET = UnitConverter.inch2uu(1/3)

	for pageId in range(1, numPages + 1):
		page = pdf.getPage(pageId)
		center = page.getPageSize().getWidth() / 2
		leftText = title
		rightText = genTime.strftime(TIMESTAMP_FORMAT)

		# Alternate swaps the title and timestamp on alternate pages, i.e. for binding.
		if alternate and pageId % 2 == 0:
			tmp = leftText
			leftText = rightText
			rightText = tmp
		
		p = Paragraph(f"{leftText}")
		document.showTextAligned(p, document.getLeftMargin(), BOTTOM_OFFSET, pageId, TextAlignment.LEFT, VerticalAlignment.BOTTOM, 0)

		p = Paragraph(f"{pageId} of {numPages}")
		document.showTextAligned(p, center, BOTTOM_OFFSET, pageId, TextAlignment.CENTER, VerticalAlignment.BOTTOM, 0)

		p = Paragraph(f"{rightText}")
		document.showTextAligned(p, page.getPageSize().getWidth() - document.getRightMargin(), BOTTOM_OFFSET, pageId, TextAlignment.RIGHT, VerticalAlignment.BOTTOM, 0)

def prependReport(document: Document):
	"""
	Adds report header info (title, etc). Not to be confused with page header.
	"""
	p = Paragraph("Black Butte Hourly Report").setFontSize(24).setTextAlignment(TextAlignment.CENTER)        # Some functions can be chained
	p.getAccessibilityProperties().setRole(StandardRoles.H1)
	document.add(p)

	p = Paragraph(f"Report generated " + genTime.strftime(TIMESTAMP_FORMAT) + "\n")
	p.setTextAlignment(TextAlignment.CENTER)
	document.add(p)

def appendReport(document: Document):
	"""
	Adds notes to the end of the document.
	"""
	p = Paragraph("* All data provisional and subject to revision.")
	p.setTextAlignment(TextAlignment.LEFT)
	document.add(p)

def fetchData():
	"""
	Fetch report data from CDA.
	"""
	end = datetime.datetime.now(TZ)
	begin = end - datetime.timedelta(days=2)

	cwms.api.init_session(api_root=API_ROOT)
	data = {
		'elev': cwms.get_timeseries('Black Butte-Pool.Elev.Inst.1Hour.0.Calc-val', OFFICE_ID, 'EN', begin=begin, end=end).df,
		'stor': cwms.get_timeseries('Black Butte-Pool.Stor.Inst.1Hour.0.Calc-val', OFFICE_ID, 'EN', begin=begin, end=end).df,
		'out': cwms.get_timeseries('Black Butte.Flow-Res Out.Ave.1Hour.1Hour.Calc-val', OFFICE_ID, 'EN', begin=begin, end=end).df,
		'in': cwms.get_timeseries('Black Butte.Flow-Res In.Ave.1Hour.1Hour.Calc-val', OFFICE_ID, 'EN', begin=begin, end=end).df,
		'stage': cwms.get_timeseries('Black Butte-Outflow.Stage.Inst.1Hour.0.Calc-val', OFFICE_ID, 'EN', begin=begin, end=end).df,
		'flow': cwms.get_timeseries('Black Butte-Outflow.Flow.Ave.1Hour.1Hour.Calc-val', OFFICE_ID, 'EN', begin=begin, end=end).df,
	}

	## This builds a DataFrame composed of all the timeseries together, joined by the timestamp.
	# Rename value column, so they're all unique when merged
	data['elev'].rename(columns={'value': 'elevation_value'}, inplace=True)
	data['stor'].rename(columns={'value': 'storage_value'}, inplace=True)
	data['out'].rename(columns={'value': 'outflow_value'}, inplace=True)
	data['in'].rename(columns={'value': 'inflow_value'}, inplace=True)
	data['stage'].rename(columns={'value': 'stage_value'}, inplace=True)
	data['flow'].rename(columns={'value': 'flow_value'}, inplace=True)

	# Merge the tables, quality column is currently dropped, but can be kept by removing the [['date-time', '*_value']] lists below
	# You'll also want to add the quality-code column to the rename lists above
	combined = pd.merge(data['elev'][['date-time', 'elevation_value']], data['stor'][['date-time', 'storage_value']], how='inner', on='date-time')
	combined = pd.merge(combined, data['out'][['date-time', 'outflow_value']], how='inner', on='date-time')
	combined = pd.merge(combined, data['in'][['date-time', 'inflow_value']], how='inner', on='date-time')
	combined = pd.merge(combined, data['stage'][['date-time', 'stage_value']], how='inner', on='date-time')
	combined = pd.merge(combined, data['flow'][['date-time', 'flow_value']], how='inner', on='date-time')

	return combined

def buildDataTable(document: Document):
	"""
	Builds the main report table content.
	"""
	def buildCell(text: str, font: PdfFont, align: TextAlignment = TextAlignment.RIGHT) -> Cell:
		cell = Cell().add(Paragraph(text))
		cell.setTextAlignment(align)
		cell.setFont(font)
		return cell

	data = fetchData()

	# Build a table with 7 columns, where the first column is 2.4x wider than the rest
	table = Table(UnitValue.createPercentArray([2.4,1.1,1.1,1,1,1,1]))
	table.setWidth(UnitValue.createPercentValue(100))
	table.setTextAlignment(TextAlignment.CENTER)

	# Get font for headers, which is just the bold version of the regular font
	font = createFont(HEADER_FONT_NAME)

	# Build table headers
	for text in ['Date/Time', 'Elevation', 'Storage', 'Outflow', 'Inflow', 'Stage', 'Flow']:
		text = Paragraph(text)
		text.setFont(font)
		header = Cell().add(text)
		# Table headers need to be marked as such (TH) for 508 compliance.
		header.getAccessibilityProperties().setRole(StandardRoles.TH)
		table.addHeaderCell(header)

	# Create monospace font for cell values
	font = createFont(MONOSPACE_FONT_NAME)

	# Fill in cell data
	# By default, auto-creates rows based on the defined number of columns
	# Can also add each row explicitly (not done here)
	for row in data.itertuples():
		table.addCell(buildCell(row._1.astimezone(TZ).strftime("%Y-%m-%d %H:%M"), font, TextAlignment.CENTER))
		table.addCell(buildCell(f"{row.elevation_value:5.2f}", font))
		table.addCell(buildCell(f"{row.storage_value:5.0f}", font))
		table.addCell(buildCell(f"{row.outflow_value:5.0f}", font))
		table.addCell(buildCell(f"{row.inflow_value:5.0f}", font))
		table.addCell(buildCell(f"{row.stage_value:5.2f}", font))
		table.addCell(buildCell(f"{row.flow_value:5.0f}", font))

	document.add(table)

if __name__ == '__main__':
	# Print a list of font names
	# list = PdfFontFactory.getRegisteredFonts().toArray()
	# for font in list:
	# 	print(repr(font))
	genTime = datetime.datetime.now(TZ)

	title = "Black Butte Hourly Report"
	doc,pdf = initializeDocument(f'/output/{PDF_NAME}.pdf', title)

	prependReport(doc)
	buildDataTable(doc)
	appendReport(doc)
	addHeader(doc, pdf, True)
	addFooter(doc, pdf, title, True)

	doc.close()
