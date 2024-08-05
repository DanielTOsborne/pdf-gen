from java.sql import Types
from xml.dom  import minidom
from xml.dom  import Node
from getopt   import getopt, GetoptError
import DBAPI, os, re, sys, traceback

infile_name   = None
overwrite     = False
exist_error   = False
put_templates = False
put_specs     = False
put_ratings   = False

program_name = os.path.splitext(os.path.split(sys.argv[0])[1])[0]
if program_name.upper() == "STORERATINGINFO" : program_name = "StoreRatingInfo"

usage_blurb = '''
	Program %s

	Stores CWMS rating information (templates and/or specs and/or ratings) from
	and XML file.

	Usage: Jython %s <options>

	Where <options> are:

		-i or --input       <infile_name>  Optional, default is read from stdin
		-o or --overwrite                  Optional, default is no overwrite
		-e or --exist_error                Optional, default is no exist_error
		-t or --templates                  Optional, default is no templates
		-s or --specs                      Optional, default id no specs
		-r or --ratings                    Optional, default is no ratings
''' % (program_name, sys.argv[0])

def usage(message) :
	'''
	Output an error message followed by the usage blurb
	'''
	if message : print("\n\tERROR : %s\n" % message)
	print("\n%s" % usage_blurb)
	exit()
#--------------------------#
# process the command line #
#--------------------------#
short_opts = "i:oetsr"
long_opts  = ["input=", "overwrite", "exist_error", "templates", "specs", "ratings"]
try :
	opts, extra = getopt(sys.argv[1:], short_opts, long_opts)
except Exception as e :
	usage(str(e))
except GetoptError as e :
	usage(str(e))
for flag, arg in opts :
	if   flag in ("-i", "--input"      ) : infile_name   = arg
	elif flag in ("-o", "--overwrite"  ) : overwrite     = True
	elif flag in ("-e", "--exist_error") : exist_error   = True
	elif flag in ("-t", "--templates"  ) : put_templates = True
	elif flag in ("-s", "--specs"      ) : put_specs     = True
	elif flag in ("-r", "--ratings"    ) : put_ratings   = True
if extra : usage("Invalid input: '%s'" % extra)
if not any([put_templates, put_specs, put_ratings]) :
	usage("Nothing to do.")
if infile_name and (not os.path.exists(infile_name) or not os.path.isfile(infile_name)) :
	usage("File %s does not exist or is not a file" % infile_name)
#----------------#
# read the input #
#----------------#
if infile_name :
	sys.stdout.write("\nReading from %s..." % infile_name)
	sys.stdout.flush()
	with open(infile_name, "r") as f : xml = f.read()
else :
	sys.stdout.write("\nReading from stdin...")
	sys.stdout.flush()
	xml = sys.stdin.read()
print("%s bytes read" % "{:,}".format(len(xml)))
try :
	sys.stdout.write("\nParsing XML...")
	sys.stdout.flush()
	doc = minidom.parseString(xml)
	print("done")
except :
	usage("\nCannot parse input data as XML")
#---------------------------#
# prepare the data to store #
#---------------------------#
sys.stdout.write("\nBuilding storage documents...")
sys.stdout.flush()
template_docs = {}
spec_docs     = {}
rating_docs   = {}
for node in doc.documentElement.childNodes :
	if node.nodeType == Node.ELEMENT_NODE :
		if (put_templates and node.nodeName == "rating-template") :
			item_name = "%s.%s" % (
				node.getElementsByTagName("parameters-id")[0].firstChild.nodeValue,
				node.getElementsByTagName("version")[0].firstChild.nodeValue)
			template_docs[item_name] = "<ratings>\n  %s\n</ratings>" % node.toxml()
		elif (put_specs     and node.nodeName == "rating-spec") :
			item_name = node.getElementsByTagName("rating-spec-id")[0].firstChild.nodeValue
			spec_docs[item_name] = "<ratings>\n  %s\n</ratings>" % node.toxml()
		elif (put_ratings   and node.nodeName in ("simple-rating", "usgs-stream-rating", "virtual-rating", "transitional-rating")) :
			item_name = "%s %s" % (
				node.getElementsByTagName("rating-spec-id")[0].firstChild.nodeValue,
				node.getElementsByTagName("effective-date")[0].firstChild.nodeValue)
			rating_docs[item_name] = "<ratings>\n  %s\n</ratings>" % node.toxml()
print("done")
doc.unlink()
if not any([template_docs, spec_docs, rating_docs]) :
	sys.stdout.write("\nNothing to store.\n")
	exit()
print("\tTemplates:%5d" % len(template_docs))
print("\tSpecs    :%5d" % len(spec_docs))
print("\tRatings  :%5d" % len(rating_docs))

db = None
try :
	#---------------------#
	# connect to database #
	#---------------------#
	sys.stdout.write("\nConnecting to database\n\n")
	cdb = os.getenv("cdb")
	usr = os.getenv("usr")
	pwd = os.getenv("pwd")
	ofc = os.getenv("ofc")
	if cdb and usr and pwd and ofc :
		db = DBAPI.open(cdb, usr, pwd, ofc)
		cdb = usr = pwd = ofc = None
	else :
		db = DBAPI.open()
	#--------------------#
	# get schema version #
	#--------------------#
	sql = '''
	      select version
	        from cwms_v_db_change_log
	       where application = 'CWMS'
	         and apply_date = (select max(apply_date)
	                             from cwms_v_db_change_log
	                             where application = 'CWMS'
	                          )
	      '''
	stmt = db.getConnection().prepareStatement(sql)
	rs = stmt.executeQuery()
	rs.next()
	schema_version = rs.getString(1)
	rs.close()
	stmt.close()
	print("\nStoring rating info\n")
	#----------------------------------#
	# store the data and report errors #
	#----------------------------------#
	all_errors = False
	if schema_version > "18.1.6" :
		#----------------------------------------------------#
		# use correctly-working API call that returns errors #
		#----------------------------------------------------#
		sql = '''
		      begin
		         cwms_rating.store_ratings_xml(
		            p_errors         => :1,
		            p_xml            => :2,
		            p_fail_if_exists => :3,
		            p_replace_base   => 'T');
		         commit;
		      end;
		      '''
		stmt = db.getConnection().prepareCall(sql)
		stmt.registerOutParameter(1, Types.CLOB)
		stmt.setString(3, ("T","F")[overwrite] + ("","T")[exist_error and not overwrite])
		for docs in template_docs, spec_docs, rating_docs :
			if   docs is template_docs : item_type = "Template"
			elif docs is spec_docs     : item_type = "Spec    "
			else                       : item_type = "Rating  "
			for item_name in sorted(docs.keys()) :
				print("\t%s %s" % (item_type, item_name))
				stmt.setStringForClob(2, docs[item_name])
				stmt.execute()
				clob = stmt.getClob(1)
				if clob :
					text = clob.getSubString(1, clob.length())
					clob.free()
				else :
					text = None
				if text :
					all_errors = True
					print("\n".join(["\t%s" % s for s in text.split("\n")[1:]]))
	else :
		#-----------------------------------------------------------------#
		# API call doesn't return errors properly, have to do it manually #
		#-----------------------------------------------------------------#
		stmt = db.getConnection().prepareStatement("select userenv('sessionid') from dual")
		rs = stmt.executeQuery()
		rs.next()
		session_id = rs.getString(1)
		rs.close()
		stmt.close()
		sql = '''
		      declare
		         l_msgid_1 varchar2(32);
		         l_msgid_2 varchar2(32);
		         l_clob    clob;
		         l_session number;
		      begin
		         select userenv('sessionid') into l_session from dual;
		         l_msgid_1 := cwms_msg.get_msg_id;
		         cwms_rating.store_ratings_xml(
		            p_xml            => :1,
		            p_fail_if_exists => :2,
		            p_replace_base   => 'T');
		         commit;
		         l_msgid_2 := cwms_msg.get_msg_id;
		         dbms_lob.createtemporary(l_clob, true);
		         for rec in (select msg_text,
		                            properties
		                       from cwms_v_log_message
		                      where msg_id between l_msgid_1 and l_msgid_2
		                        and instr(properties, 'session_id = '||l_session||';') > 0
		                    )
		         loop
		            cwms_util.append(l_clob, rec.msg_text||chr(29)||rec.properties||chr(30));
		         end loop;
		         :3 := l_clob;
		      end;
		      '''
		stmt = db.getConnection().prepareCall(sql)
		stmt.setString(2, ("T","F")[overwrite])
		stmt.registerOutParameter(3, Types.CLOB)
		for docs in template_docs, spec_docs, rating_docs :
			if   docs is template_docs : item_type = "Template"
			elif docs is spec_docs     : item_type = "Spec    "
			else                       : item_type = "Rating  "
			for item_name in sorted(docs.keys()) :
				print("\t%s %s" % (item_type, item_name))
				stmt.setStringForClob(1, docs[item_name])
				stmt.execute()
				clob = stmt.getClob(3)
				if clob :
					text = clob.getSubString(1, clob.length())
					clob.free()
				else :
					text = None
				if text :
					records  = text.split(chr(30))
					errors   = False
					for record in records :
						if record.find(chr(29)) != -1 :
							msg_text, properties = record.split(chr(29))
						else :
							msg_text, properites = record, ""
						if msg_text.find("ERROR") != -1 or msg_text.find("ORA-") != -1 :
							if msg_text.find("ITEM_ALREADY_EXISTS") != -1 and not exist_error :
								continue
							errors = all_errors = True
							print("\t%s" % msg_text)
							for line in properties.split("\n") :
								if line.startswith("call stack[") :
									print("\t   %s" % line.split(" = ")[1])
							print("")
	if not all_errors :
		print("\nNo errors")
except :
	traceback.print_exc()
finally :
	sys.stdout.write("\nDisconnecting from database\n")
	if db : db.close()
	exit()

