from xml.dom import minidom
from getopt import getopt, GetoptError
import os, platform, re, sys
if platform.python_version().startswith('3') :
	from io import StringIO
else :
	from StringIO import StringIO

program_name = os.path.splitext(os.path.split(sys.argv[0])[1])[0]
if program_name.upper() == "UPDATERATINGBEHAVIORS" : program_name = "UpdateRatingBehaviors"

infile_name          = None
outfile_name         = None
behaviors_file_name  = None
output_updated_only  = False

usage_blurb = '''
	Program %s

	Reads CWMS rating templates and/or specs in XML format and modifies behaviors
	as specified in a behaviors file. The modified XML is written to the output.
	The input is not modified.

	Usage: Python %s <options>

	Where <options> are:

		-b or --behaviors <file_name>  Required, name of behaviors file
		-i or --input     <file_name>  Optional, name of input file, defaults to stdin
		-o or --output    <file_name>  Optional, name of output file, defaults to stdout
		-u or --updated                Optional, output updated items only, default is all items

	The behaviors file contains lines of behavior names and values, separated by whitespace.
	Neither behavior names or values are case sensitive. Blank lines are ignored. Any portion
	of a line following following the # character (including the character itself) is ignored.

	The valid behavior names follow. The file should contain one or more of:
		template-in-range
		template-out-range-low
		template-out-range-high
		spec-in-range
		spec-out-range-low
		spec-out-range-high
		auto-update
		auto-activate
		auto-migrate-extension

	Valid values for template-in-range, -out-range-low, and -out-range-low are:
		null
		error
		linear
		logarithmic
		lin-log
		log-lin
		previous
		next
		nearest
		lower
		higher
		closest

	Valid values for spec-in-range, -out-range-low, and -out-range-low are:
		null
		error
		linear
		previous
		next
		nearest

	Valid values for auto-update, auto-activate, and auto-migrate-extension are:
		true
		false

	In addition, behavior names of the format <parameter>-rounding, where <parameter>
	is a base or full parameter name are allowed. If <parameter> is a base parameter
	name, it will match parameters whose base parameter is <parameter>. These lines
	are used to modify the rounding specification for independent and/or dependent
	parameters. Valid values for these behaviors are 10-digit rounding specifications.
''' % (program_name, sys.argv[0])

rating_behaviors = {
	"template-in-range"       : ("null", "error", "linear", "logarithmic", "lin-log", "log-lin", "previous", "next", "nearest", "lower", "higher", "closest"),
	"template-out-range-low"  : ("null", "error", "linear", "logarithmic", "lin-log", "log-lin", "previous", "next", "nearest", "lower", "higher", "closest"),
	"template-out-range-high" : ("null", "error", "linear", "logarithmic", "lin-log", "log-lin", "previous", "next", "nearest", "lower", "higher", "closest"),
	"spec-in-range"           : ("null", "error", "linear", "previous", "next", "nearest"),
	"spec-out-range-low"      : ("null", "error", "linear", "previous", "next", "nearest"),
	"spec-out-range-high"     : ("null", "error", "linear", "previous", "next", "nearest"),
	"auto-update"             : ("true", "false"),
	"auto-activate"           : ("true", "false"),
	"auto-migrate-extension"  : ("true", "false")
}

element_names = {
	"template-in-range"       : "in-range-method",
	"template-out-range-low"  : "out-range-low-method",
	"template-out-range-high" : "out-range-high-method",
	"spec-in-range"           : "in-range-method",
	"spec-out-range-low"      : "out-range-low-method",
	"spec-out-range-high"     : "out-range-high-method",
	"auto-update"             : "auto-update",
	"auto-activate"           : "auto-activate",
	"auto-migrate-extension"  : "auto-migrate-extension"
}

all_template_behaviors = [k for k in list(rating_behaviors.keys()) if k.startswith("template")]
all_spec_behaviors     = [k for k in list(rating_behaviors.keys()) if k not in all_template_behaviors] + ["rounding"]


def usage(message) :
	'''
	Output an error message followed by the usage blurb
	'''
	if message : sys.stdout.write("\n\tERROR: %s\n" % message)
	sys.stdout.write("\n%s\n" % usage_blurb)
	exit()

def parse_behaviors_file(filename) :
	with open(filename, "r") as f : lines = f.read().strip().split("\n")
	for i in range(len(lines)) :
		pos = lines[i].find("#")
		if pos >= 0 : lines[i] = lines[i][:pos]
	words = " ".join(lines).split()
	if len(words) % 2 :
		raise Exception("Number of names and behaviors is not the same")
	keys   = words[0::2]
	values = words[1::2]
	behaviors = {}
	for i in range(len(keys)) :
		key, value = keys[i].lower(), values[i].lower()
		if key in rating_behaviors :
			if value not in rating_behaviors[key] :
				raise Exception("Invalid value for %s: %s" % (key, value))
			if key in behaviors and behaviors[key] != value :
				sys.stderr.write('WARNING: Behavior %s redefined from "%s" to "%s"\n' % (key, behaviors[key], value))
			behaviors[key] = value
		else :
			m = re.match(r"(\w+(?:-\w+)*)-rounding", key)
			if m :
				if not re.match(r"\d{10}", value) :
					raise Exception("Invalid value for %s: %s" % (key, value))
				parameter = m.group(1)
				behaviors.setdefault("rounding", {})[parameter] = value
			else :
				raise Exception("Invalid behavior: %s" % key)
	return behaviors


#--------------------------#
# process the command line #
#--------------------------#
short_opts = "i:o:b:u"
long_opts  = ["input=", "output=", "behaviors=", "updated"]
try :
	opts, extra = getopt(sys.argv[1:], short_opts, long_opts)
except Exception as e :
	usage(str(e))
except GetoptError as e :
	usage(str(e))
for flag, arg in opts :
	if   flag in ("-i", "--input"    ) : infile_name          = arg
	elif flag in ("-o", "--output"   ) : outfile_name         = arg
	elif flag in ("-b", "--behaviors") : behaviors_file_name  = arg
	elif flag in ("-u", "--updated"  ) : output_updated_only  = True
if extra :
	usage("Invalid input: '%s'" % extra)
if not behaviors_file_name :
	usage("No behaviors file specified")
#-------------------------#
# read the behaviors file #
#-------------------------#
try :
	sys.stderr.write("\nParsing behaviors file %s\n" % behaviors_file_name)
	behaviors = parse_behaviors_file(behaviors_file_name)
except Exception(e) :
	usage(str(e))
sys.stderr.write("%d behaviors read\n" % len(behaviors))
template_behaviors = [k for k in list(behaviors.keys()) if k in all_template_behaviors]
spec_behaviors     = [k for k in list(behaviors.keys()) if k in all_spec_behaviors]
sys.stderr.write("...%d template behaviors\n" % len(template_behaviors))
sys.stderr.write("...%d spec behaviors\n"  % len(spec_behaviors))
#----------------#
# read the input #
#----------------#
if infile_name :
	sys.stderr.write("\nReading from %s..." % infile_name)
	with open(infile_name, "r") as f : xml_text = f.read()
else :
	sys.stderr.write("\nReading from stdin...")
	infile_name = sys.stdin.name
	xml_text = sys.stdin.read()
sys.stderr.write("%s bytes read\n" % "{:,}".format(len(xml_text)))
sys.stderr.write("Parsing XML\n")
try :
	doc = minidom.parseString(xml_text)
except :
	usage("Cannot parse file %s as XML" % infile_name)

sys.stderr.write("Processing XML\n")
templates_modified = []
specs_modified     = []
behaviors_modified = 0
#------------------#
# handle templates #
#------------------#
if template_behaviors :
	for template_element in doc.getElementsByTagName("rating-template") :
		first = True
		template_id = "%s.%s" % (
			template_element.getElementsByTagName("parameters-id")[0].firstChild.nodeValue,
			template_element.getElementsByTagName("version")[0].firstChild.nodeValue)
		param_specs_elem = template_element.getElementsByTagName("ind-parameter-specs")[0]
		for param_spec_elem in param_specs_elem.getElementsByTagName("ind-parameter-spec") :
			pos = param_spec_elem.getAttribute("position")
			for key in template_behaviors :
				name = element_names[key]
				elem = param_spec_elem.getElementsByTagName(name)[0]
				old_value = elem.firstChild.nodeValue.upper()
				new_value = behaviors[key].upper()
				if new_value != old_value :
					if first :
						first = False
						sys.stderr.write("\nRating Template %s\n" % template_id)
						templates_modified.append(template_element)
					sys.stderr.write("\tind parameter %s: updating %-22s from %-11s to %-11s\n" % (
						pos,
						name,
						old_value,
						new_value))
					elem.firstChild.nodeValue = new_value
					behaviors_modified += 1
#--------------#
# handle specs #
#--------------#
case_correct = lambda x : x.lower() if x in ("TRUE", "FALSE") else x
if spec_behaviors :
	for spec_element in doc.getElementsByTagName("rating-spec") :
		first = True
		spec_id = spec_element.getElementsByTagName("rating-spec-id")[0].firstChild.nodeValue
		parts = spec_id.split(".")
		ind_params = [x.lower() for x in  parts[1].split(";")[0].split(",")]
		dep_param  = parts[1].split(";")[1].lower()
		for key in spec_behaviors :
			if key == "rounding" : continue
			name = element_names[key]
			elem = spec_element.getElementsByTagName(name)[0]
			old_value = elem.firstChild.nodeValue.upper()
			new_value = behaviors[key].upper()
			if new_value != old_value :
				if first :
					first = False
					sys.stderr.write("\nRating Specification %s\n" % spec_id)
					specs_modified.append(spec_element)
				sys.stderr.write("\tupdating %-22s from %-11s to %-11s\n" % (
					name,
					case_correct(old_value),
					case_correct(new_value)))
				elem.firstChild.nodeValue = case_correct(new_value)
				behaviors_modified += 1
		if "rounding" in behaviors :
			roundings = behaviors["rounding"]
			rounding_specs_elem = spec_element.getElementsByTagName("ind-rounding-specs")[0]
			for i in range(len(ind_params)) :
				for param in (ind_params[i], ind_params[i].split("-")[0]) :
					if param in roundings :
						elem = rounding_specs_elem.getElementsByTagName("ind-rounding-spec")[i]
						pos = elem.getAttribute("position")
						if int(pos) != i+1 :
							raise Exception("Unexpected position on ind-rounding-spec element number %s: %s" % (i+1, pos))
						old_value = elem.firstChild.nodeValue.upper()
						new_value = roundings[param]
						if new_value != old_value :
							if first :
								first = False
								sys.stderr.write("\nRating Specification %s\n" % spec_id)
								specs_modified.append(spec_element)
							sys.stderr.write("\tupdating %-22s from %-11s to %-11s\n" % (
								"ind rounding spec %s" % pos,
								old_value,
								new_value))
							elem.firstChild.nodeValue = new_value
							behaviors_modified += 1
						break
			for param in (dep_param, dep_param.split("-")[0]) :
				if param in roundings :
					elem = spec_element.getElementsByTagName("dep-rounding-spec")[0]
					old_value = elem.firstChild.nodeValue.upper()
					new_value = roundings[param]
					if new_value != old_value :
						if first :
							first = False
							sys.stderr.write("\nRating Specification %s\n" % spec_id)
							specs_modified.append(spec_element)
						sys.stderr.write("\tupdating %-22s from %-11s to %-11s\n" % (
							"dep rounding spec",
							old_value,
							new_value))
						elem.firstChild.nodeValue = new_value
						behaviors_modified += 1
				break

sys.stderr.write("\n%d items (%d behaviors) updated\n" % (len(templates_modified+specs_modified), behaviors_modified))
#----------------#
# output results #
#----------------#
buf = StringIO()
if output_updated_only :
	buf.write("<ratings>\n")
	for elem in templates_modified + specs_modified : buf.write("  %s\n" % elem.toxml())
	buf.write("</ratings>")
	xml_text = buf.getvalue()
else :
	doc.writexml(buf, addindent="")
	xml_text = buf.getvalue()
	xml_text = xml_text[xml_text.find(">")+1:]
if outfile_name :
	sys.stderr.write("\nWriting %s items to %s..." % (("all", "updated")[output_updated_only], outfile_name))
	with open(outfile_name, "w") as f : f.write("%s\n" % xml_text)
else :
	sys.stderr.write("\nWriting %s items to stdout..." % ("all", "updated")[output_updated_only])
	sys.stdout.write("%s\n" % xml_text)
sys.stderr.write("%s bytes written\n" % "{:,}".format(len(xml_text)))

