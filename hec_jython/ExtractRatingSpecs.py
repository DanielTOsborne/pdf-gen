from java.sql import Types
from io       import StringIO
from xml.dom  import minidom
from getopt   import getopt, GetoptError
import DBAPI, os, re, sys, traceback

spec_mask     = "*"
office_id     = None
xmlfile_name  = None
xmlfile       = None
get_templates = False
get_ratings   = False

program_name = os.path.splitext(os.path.split(sys.argv[0])[1])[0]
if program_name.upper() == "EXTRACTRATINGSPECS" : program_name = "ExtractRatingSpecs"

usage_blurb = '''
	Program %s :

	Extracts CWMS rating specificaitons in XML format. Optionally also retrieves
	rating templates and/or ratings for the extracted specifications.

	Usage: Jython %s <options>

	Where <options> are:

		-m or --mask      <spec_mask> Optional, defaults to "*"
		-f or --office    <office_id> Optional, defaults to connection default office
		-o or --output    <file_name> Optional, defaults to stdout
		-t or --templates             Optional, default is no templates
		-r or --ratings               Optional, default is no ratings
''' % (program_name, sys.argv[0])

plsql = '''
declare
   type template_ids_t is table of boolean index by varchar2(289);
   l_clob              clob;
   l_xml               xmltype;
   l_nodes             cwms_t_xml_tab;
   l_mask              varchar2(32);
   l_template_id       varchar2(289);
   l_spec_id           varchar2(691);
   l_template_ids      template_ids_t;
   l_template          xmltype;
   l_spec_mask         varchar2(615) := :1;
   l_office_id         varchar2(16)  := :2;
   l_include_templates varchar2(1)   := :3;
   l_include_ratings   varchar2(1)   := :4;
   l_text              clob;
begin
   ------------------------
   -- retrieve the specs --
   ------------------------
   l_clob  := cwms_rating.retrieve_specs_xml_f(l_spec_mask, l_office_id);
   l_xml   := xmltype(l_clob);
   l_nodes := cwms_util.get_xml_nodes(
      p_xml       => l_xml,
      p_path      => '/ratings/rating-spec',
      p_condition => 'not(contains(/ratings/rating-spec, ''Stage;Stage-Shift''))'
                     ||' and not(contains(/ratings/rating-spec, ''Stage;Stage-Offset''))');
   dbms_lob.createtemporary(l_clob, true);
   cwms_util.append(l_clob, '<ratings>');
   for i in 1..l_nodes.count loop
      -----------------------------------------
      -- retrieve the template for this spec --
      -----------------------------------------
      if l_include_templates = 'T' then
         l_template_id := cwms_util.get_xml_text(l_nodes(i), '/rating-spec/template-id');
         if not l_template_ids.exists(l_template_id) or l_template_ids(l_template_id) = false then
            l_template_ids(l_template_id) := false;
            begin
               l_template := xmltype(cwms_rating.retrieve_templates_xml_f(l_template_id, l_office_id));
               ----------------------------------------------
               -- append the template to the working clob --
               ----------------------------------------------
               select xmlserialize(content cwms_util.get_xml_node(l_template, '/ratings/rating-template') as clob indent)
                 into l_text
                 from dual;
               cwms_util.append(l_clob, l_text);
               l_template_ids(l_template_id) := true;
            exception
               when others then null;
            end;
         end if;
      end if;
      ----------------------------------------
      -- retrieve the ratings for this spec --
      ----------------------------------------
      if l_include_ratings = 'T' then
         l_spec_id := cwms_util.get_xml_text(l_nodes(i), '/rating-spec/rating-spec-id');
         l_xml := cwms_util.get_xml_node(xmltype(cwms_rating.retrieve_ratings_xml_f(l_spec_id, null, null, null, l_office_id)), '/ratings/simple-rating|/ratings/usgs-stream-rating|/ratings/virtual-rating|/ratings/transitional-rating');
         --------------------------------------------
         -- append the ratings to the working clob --
         --------------------------------------------
         select xmlserialize(content l_xml as clob indent)
           into l_text
           from dual;
         cwms_util.append(l_clob, l_text);
      end if;
      -----------------------------------------
      -- append the spec to the wroking clob --
      -----------------------------------------
      select xmlserialize(content l_nodes(i) as clob indent)
        into l_text
        from dual;
      cwms_util.append(l_clob, l_text);
   end loop;
   cwms_util.append(l_clob, '</ratings>');
   l_xml := xmltype(l_clob);
   ------------------------------------------------------------------------------------
   -- now re-arrange the elements as templates followed by specs followed by ratings --
   ------------------------------------------------------------------------------------
   if l_include_templates = 'T' or l_include_ratings = 'T' then
      dbms_lob.trim(l_clob, 0);
      cwms_util.append(l_clob, '<ratings>');
      ---------------------------
      -- all template elements --
      ---------------------------
      if l_include_templates = 'T' then
         l_nodes := cwms_util.get_xml_nodes(
            p_xml       => l_xml,
            p_path      => '/ratings/rating-template');
         for i in 1..l_nodes.count loop
            cwms_util.append(l_clob, l_nodes(i).getclobval);
         end loop;
      end if;
      --------------------
      -- all spec nodes --
      --------------------
      l_nodes := cwms_util.get_xml_nodes(
         p_xml       => l_xml,
         p_path      => '/ratings/rating-spec');
      for i in 1..l_nodes.count loop
         cwms_util.append(l_clob, l_nodes(i).getclobval);
      end loop;
      ----------------------
      -- all rating nodes --
      ----------------------
      if l_include_ratings = 'T' then
         l_nodes := cwms_util.get_xml_nodes(
            p_xml       => l_xml,
            p_path      => '/ratings/simple-rating|/ratings/usgs-stream-rating|/ratings/virtual-rating|/ratings/transitional-rating');
         for i in 1..l_nodes.count loop
            cwms_util.append(l_clob, l_nodes(i).getclobval);
         end loop;
      end if;
      cwms_util.append(l_clob, '</ratings>');
      l_xml := xmltype(l_clob);
   end if;
   select xmlserialize(document l_xml as clob indent)
     into l_text
     from dual;
   dbms_lob.freetemporary(l_clob);
   ------------------------
   -- return the results --
   ------------------------
   select xmlserialize(document l_xml as clob indent)
     into :5
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
short_opts = "m:f:o:tr"
long_opts  = ["mask=", "office=", "output=", "templates", "ratings"]
try :
	opts, extra = getopt(sys.argv[1:], short_opts, long_opts)
except Exception as e :
	usage(str(e))
except GetoptError as e :
	usage(str(e))
for flag, arg in opts :
	if   flag in ("-m", "--mask"     ) : spec_mask     = arg
	elif flag in ("-f", "--office"   ) : office_id     = arg
	elif flag in ("-o", "--output"   ) : xmlfile_name  = arg
	elif flag in ("-t", "--templates") : get_templates = True
	elif flag in ("-r", "--ratings"  ) : get_ratings   = True
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
	stmt.setString(1, spec_mask)
	stmt.setString(2, office_id)
	stmt.setString(3, "FT"[get_templates])
	stmt.setString(4, "FT"[get_ratings])
	stmt.registerOutParameter(5, Types.CLOB)
	stmt.execute()
	sys.stderr.write("Retrieving results...")
	clob = stmt.getClob(5)
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

