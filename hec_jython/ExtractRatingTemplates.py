from java.sql import Types
from getopt import getopt, GetoptError
import DBAPI, os, sys

template_mask = "*"
office_id     = None
xmlfile_name  = None
xmlfile       = None

program_name = os.path.splitext(os.path.split(sys.argv[0])[1])[0]
if program_name.upper() == "EXTRACTRATINGTEMPLATES" : program_name = "ExtractRatingTemplates"

usage_blurb = '''
	Program %s :

	Extracts CWMS rating templates in XML format

	Usage: Jython %s <options>

	Where <options> are:

		-m or --mask     <template_mask> Optional, defaults to "*"
		-f or --office   <office_id>     Optional, defaults to connection default office
		-o or --output   <file_name>     Optional, defaults to stdout
''' % (program_name, sys.argv[0])

plsql = '''
declare
   l_clob        clob;
   l_xml         xmltype;
   l_nodes       cwms_t_xml_tab;
   l_template_id varchar2(291);
   l_mask        varchar2(291) := :1;
   l_office_id   varchar2(16)  := :2;
begin
   l_clob  := cwms_rating.retrieve_templates_xml_f('*', l_office_id);
   l_xml   := xmltype(l_clob);
   l_nodes := cwms_util.get_xml_nodes(
      p_xml       => l_xml,
      p_path      => '/ratings/rating-template',
      p_condition => '/ratings/rating-template/parameters-id!=''Stage;Stage-Shift''\'
                     ||'and /ratings/rating-template/parameters-id!=''Stage;Stage-Offset''\');
   l_mask := upper(cwms_util.normalize_wildcards(l_mask));
   dbms_lob.createtemporary(l_clob, true);
   cwms_util.append(l_clob, '<ratings>');
   for i in 1..l_nodes.count loop
      l_template_id := cwms_util.get_xml_text(l_nodes(i), '/rating-template//parameters-id')
                       ||'.'
                       ||cwms_util.get_xml_text(l_nodes(i), '/rating-template//version');
      if upper(l_template_id) like l_mask then
      	cwms_util.append(l_clob, l_nodes(i).getstringval);
      end if;
   end loop;
   cwms_util.append(l_clob, '</ratings>');
   l_xml := xmltype(l_clob);
   dbms_lob.freetemporary(l_clob);
   select xmlserialize(document l_xml as clob indent)
     into :3
     from dual;
end;
'''

def usage(message) :
	'''
	Output an error message followed by the usage blurb
	'''
	if message : sys.stdout.write("\n\tERROR : %s\n" % message)
	sys.stdout.write("\n%s\n" % usage_blurb)
	exit()

#--------------------------#
# process the command line #
#--------------------------#
short_opts = "m:f:o:"
long_opts  = ["mask=", "office=", "output="]
try :
	opts, extra = getopt(sys.argv[1:], short_opts, long_opts)
except Exception as e :
	usage(str(e))
except GetoptError as e :
	usage(str(e))
for flag, arg in opts :
	if   flag in ("-m", "--mask"   ) : template_mask = arg.lstrip("^") # for windows escape weirdness
	elif flag in ("-f", "--office" ) : office_id     = arg
	elif flag in ("-o", "--output" ) : xmlfile_name  = arg
if extra : usage("Invalid input: '%s'" % extra)
if xmlfile_name : xmlfile = open(xmlfile_name, "w")
db = None
try :
	#---------------------#
	# connect to database #
	#---------------------#
	sys.stderr.write("\nConnecting to database\n\n")
	cdb = os.getenv("cdb")
	usr = os.getenv("usr")
	pwd = os.getenv("pwd")
	ofc = os.getenv("ofc")
	if cdb and usr and pwd and ofc :
		db = DBAPI.open(cdb, usr, pwd, ofc)
		cdb = usr = pwd = ofc = None
	else :
		db = DBAPI.open()
	if office_id is None : office_id = db.getOfficeId()
	#-----------------------------------------#
	# execute the PL/SQL code and get results #
	#-----------------------------------------#
	sys.stderr.write("\nExecuting PL/SQL\n")
	stmt = db.getConnection().prepareCall(plsql)
	stmt.setString(1, template_mask)
	stmt.setString(2, office_id)
	stmt.registerOutParameter(3, Types.CLOB)
	stmt.execute()
	sys.stderr.write("Retrieving results...")
	clob = stmt.getClob(3)
	text = clob.getSubString(1, clob.length())
	sys.stderr.write("%s bytes read\n" % "{:,}".format(len(text)))
	clob.free()
	#------------------#
	# write the output #
	#------------------#
	sys.stderr.write("Writing output.......")
	if xmlfile :
		xmlfile.write(text)
		xmlfile.close()
	else :
		sys.stdout.write(text)
	sys.stderr.write("%s bytes written\n" % "{:,}".format(len(text)))
except :
	traceback.print_exc()
finally :
	sys.stderr.write("\nDisconnecting from database\n")
	if db : db.close()
	exit()

