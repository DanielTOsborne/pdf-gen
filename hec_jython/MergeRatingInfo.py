from xml.dom import Node
from xml.dom import minidom
from getopt import getopt, GetoptError
import os, sys

program_name = os.path.splitext(os.path.split(sys.argv[0])[1])[0]
if program_name.upper() == "MERGERATINGINFO" : program_name = "MergeRatingInfo"

outfile_name = None
filter_dups  = True

usage_blurb = '''
	Program %s Merges ratings info from multiple input files.

	Usage: Python %s <options> file1 [file2 ...]

	Where <options> are:

		-o or --output <file_name>  Optional, name of output file, defaults to stdout

	If only one input file is specified, it will be copied to the output.

	Duplicate items are filtered, the first occurrence will be used.

	Duplicate items are identified by :

		rating templates : the office and template identifier
		rating specs     : the office and rating spec identifier
		ratings          : the office, rating spec identifier, and effective date

''' % (program_name, sys.argv[0])

def usage(message) :
	'''
	Output an error message followed by the usage blurb
	'''
	if message : sys.stdout.write("\n\tERROR: %s\n" % message)
	sys.stdout.write("\n%s\n" % usage_blurb)
	exit()
#--------------------------#
# process the command line #
#--------------------------#
short_opts = "o:"
long_opts  = ["output="]
try :
	opts, input_filenames = getopt(sys.argv[1:], short_opts, long_opts)
except Exception as e :
	usage(str(e))
except GetoptError as e :
	usage(str(e))
for flag, arg in opts :
	if flag in ("-o", "--output"  ) : outfile_name  = arg
if not input_filenames :
	usage("No input files specified")
#-------------------------#
# process the input files #
#-------------------------#
template_elems = {}
spec_elems     = {}
rating_elems   = {}
docs = []
sys.stderr.write("\n")
for input_filename in input_filenames :
	template_count = spec_count = rating_count = 0
	sys.stderr.write("\nProcessing file %s\n" % input_filename)
	if not os.path.exists(input_filename) or not os.path.isfile(input_filename) :
		sys.stderr.write("...file does not exist or is not a file, skipping\n")
		continue
	with open(input_filename) as f : xml = f.read()
	try :
		doc = minidom.parseString(xml)
	except :
		sys.stderr.write("...cannot parse file as XML, skipping\n")
		continue
	docs.append(doc)
	for node in doc.documentElement.childNodes :
		if node.nodeType == Node.ELEMENT_NODE :
			if node.nodeName == "rating-template" :
				_id = "%s/%s.%s" % (
					node.getAttribute("office-id"),
					node.getElementsByTagName("parameters-id")[0].firstChild.nodeValue,
					node.getElementsByTagName("version")[0].firstChild.nodeValue)
				if _id in template_elems :
					sys.stderr.write("...skipping duplicate template %s\n" % _id)
				else :
					template_elems[_id] = node
					template_count += 1
			elif node.nodeName == "rating-spec" :
				_id = "%s/%s" % (
					node.getAttribute("office-id"),
					node.getElementsByTagName("rating-spec-id")[0].firstChild.nodeValue)
				if _id in spec_elems :
					sys.stderr.write("...skipping duplicate spec %s\n" % _id)
				else :
					spec_elems[_id] = node
					spec_count += 1
			elif node.nodeName in ("simple-rating", "usgs-stream-rating", "virtual-rating", "transitional-rating") :
				_id = "%s/%s(%s)" % (
					node.getAttribute("office-id"),
					node.getElementsByTagName("rating-spec-id")[0].firstChild.nodeValue,
					node.getElementsByTagName("effective-date")[0].firstChild.nodeValue)
				if _id in rating_elems :
					sys.stderr.write("...skipping duplicate rating %s\n" % _id)
				else :
					rating_elems[_id] = node
					rating_count += 1
			else :
				sys.stderr.write("...ignoring unexpected element: %s\n" % node.nodeName)

	sys.stderr.write("...included %4d templates\n" % template_count)
	sys.stderr.write("...included %4d specs\n" % spec_count)
	sys.stderr.write("...included %4d ratings\n" % rating_count)
#------------------#
# write the output #
#------------------#
if outfile_name :
	sys.stderr.write("\nWriting output to %s\n" % outfile_name)
	f = open(outfile_name, "w")
else :
	sys.stderr.write("\nWriting output to stdout\n")
	f = sys.stdout
f.write("<ratings>\n")
for elems in (template_elems, spec_elems, rating_elems) :
	for _id in sorted(elems.keys()) :
		f.write("  %s\n" % elems[_id].toxml())
f.write("</ratings>")
sys.stderr.write("...output %4d templates\n" % len(template_elems))
sys.stderr.write("...output %4d specs\n" % len(spec_elems))
sys.stderr.write("...output %4d ratings\n" % len(rating_elems))
#---------#
# cleanup #
#---------#
for doc in docs :
	try    : doc.unlink()
	except : pass

if outfile_name : f.close()


