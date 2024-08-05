from hec.heclib.util import HecTime

from java.sql        import Types
from java.text       import SimpleDateFormat
from java.util       import TimeZone
from datetime        import datetime
from getopt          import getopt, GetoptError
from io              import StringIO
import DBAPI, os, stat, sys, traceback
#
# TODO: Allow deletion of ratings (only) that match a template or specification
#       mask and a specified time window, leaving matched templates or specifications
#       in place
#

mask             = None
delete_templates = False
delete_specs     = False
delete_ratings   = False
delete_empty     = False
backup           = False
execute          = False
verbose          = False
nochild          = False
noparent         = False
office           = None
outdir           = "."
start_time       = None
end_time         = None
time_zone        = None
sdf              = None
template_header = "\nTemplates to be deleted      : %5d"
spec_header     = "Specifications to be deleted : %5d"
rating_header_1 = "Ratings to be backed up      : %5d (plus %d matching child ratings included with parent)"
rating_header_2 = "Ratings to be deleted        : %5d (plus %d matching child ratings included with parent)"
delete_prompt   = '\nAre you sure you want to continue?\nEnter "yes" to delete the items. > '
DELETE_ALL      = "DELETE ALL"
DELETE_KEY      = "DELETE KEY"


query_empty_templates_sql = '''
	select template_id
	  from cwms_v_rating_template
	 where office_id = :1
	minus
	select template_id
	  from cwms_v_rating_template
	 where template_code in (select distinct
	                                template_code
	                           from cwms_v_rating_spec
	                          where office_id = :2
	                        )
	 order by 1
	'''
query_templates_by_mask_sql = '''
	select *
	  from cwms_v_rating_template
	 where upper(template_id) like upper(cwms_util.normalize_wildcards(:1)) escape '\\'
	   and office_id = :2
	 order by 1
	'''
delete_single_template_sql = '''
	begin
	   cwms_rating.delete_templates(
	      p_template_id_mask => :1,
	      p_delete_action    => :2,
	      p_office_id_mask   => :3);
	   commit;
	end;
	'''
query_empty_specs_sql = '''
	select rating_id
	  from cwms_v_rating_spec
	 where office_id = :1
	   and aliased_item is null
	minus
	select rating_id
	  from cwms_v_rating_spec
	 where rating_spec_code in (select distinct
	                                   rating_spec_code
	                              from cwms_v_rating
	                             where office_id = :2
	                           )
	minus
	select rating_id
	  from cwms_v_rating_spec
	 where rating_spec_code in (select distinct
	                                   rating_spec_code
	                              from cwms_v_transitional_rating
	                             where office_id = :3
	                           )
	                           minus
	select rating_id
	  from cwms_v_rating_spec
	 where rating_spec_code in (select distinct
	                                   rating_spec_code
	                              from cwms_v_virtual_rating
	                             where office_id = :4
	                           )
	 order by 1
	'''
query_specs_by_mask_sql = '''
	select *
	  from cwms_v_rating_spec
	 where upper(rating_id) like upper(cwms_util.normalize_wildcards(:1)) escape '\\'
	   and office_id = :2
	   and aliased_item is null
	 order by 1
	'''
query_specs_by_template_mask_sql = '''
	select distinct
	       rating_id
	  from cwms_v_rating_spec
	 where upper(template_id) like upper(cwms_util.normalize_wildcards(:1)) escape '\\'
	   and office_id = :2
	   and aliased_item is null
	 order by 1
	'''
delete_single_spec_sql = '''
	begin
	   cwms_rating.delete_specs(
	      p_spec_id_mask   => :1,
	      p_delete_action  => :2,
	      p_office_id_mask => :3);
	   commit;
	end;
	'''
query_ratings_by_mask_sql = '''
	select rating_id,
	       rating_date,
	       count(matching_child_rating_id) as matching_child_rating_count,
	       parent_rating_id,
	       parent_rating_date,
	       virtual_rating_parent,
	       virtual_rating_date,
	       transitional_rating_parent,
	       transitional_rating_date,
	       rating_type
	  from (select q1.rating_id,
	               to_char(from_tz(cast(q1.effective_date as timestamp), 'UTC') at time zone cwms_util.get_timezone('%s'),
	                       'yyyy/mm/dd hh24:mi:sstzhtzm') as rating_date,
	               q3.matching_child_rating_id,
	               q1.parent_rating_code,
	               q1.rating_type,
	               q4.rating_id as parent_rating_id,
	               q4.rating_date as parent_rating_date,
	               q5.rating_spec as virtual_rating_parent,
	               q5.rating_date as virtual_rating_date,
	               q6.rating_spec as transitional_rating_parent,
	               q6.rating_date as transitional_rating_date
	          from (select rating_code,
	                       rating_id,
	                       effective_date,
	                       parent_rating_code,
	                       'Normal' as rating_type
	                  from cwms_v_rating
	                 where office_id = :1
	                   and effective_date >= nvl(cwms_util.change_timezone(to_timestamp(:2, 'yyyy/mm/dd hh24:mi:ss'), :3, 'UTC'), effective_date)
	                   and effective_date <= nvl(cwms_util.change_timezone(to_timestamp(:4, 'yyyy/mm/dd hh24:mi:ss'), :5, 'UTC'), effective_date)
	                   and aliased_item is null
	                union all
	                select distinct
	                       virtual_rating_code as rating_code,
	                       rating_spec as rating_id,
	                       effective_date,
	                       null as parent_rating_code,
	                       'Virtual' as rating_type
	                  from cwms_v_virtual_rating
	                 where office_id = :6
	                   and effective_date >= nvl(cwms_util.change_timezone(to_timestamp(:7, 'yyyy/mm/dd hh24:mi:ss'),  :8, 'UTC'), effective_date)
	                   and effective_date <= nvl(cwms_util.change_timezone(to_timestamp(:9, 'yyyy/mm/dd hh24:mi:ss'), :10, 'UTC'), effective_date)
	                union all
	                select distinct
	                       transitional_rating_code as rating_code,
	                       rating_spec as rating_id,
	                       effective_date,
	                       null as parent_rating_code,
	                       'Transitional' as rating_type
	                  from cwms_v_transitional_rating
	                 where office_id = :11
	                   and effective_date >= nvl(cwms_util.change_timezone(to_timestamp(:12, 'yyyy/mm/dd hh24:mi:ss'), :13, 'UTC'), effective_date)
	                   and effective_date <= nvl(cwms_util.change_timezone(to_timestamp(:14, 'yyyy/mm/dd hh24:mi:ss'), :15, 'UTC'), effective_date)
	               ) q1
	               left outer join
	               (select parent_rating_code,
	                       case
	                       when upper(template_id) like upper(cwms_util.normalize_wildcards(:16)) escape '\\' then rating_id
	                       else null
	                       end as matching_child_rating_id
	                   from cwms_v_rating
	                 where aliased_item is null
	               ) q3 on q3.parent_rating_code = q1.rating_code
	               left outer join
	               (select distinct
	                       rating_code,
	                       rating_id,
	                       to_char(from_tz(cast(effective_date as timestamp), 'UTC') at time zone cwms_util.get_timezone('%s'),
	                               'yyyy/mm/dd hh24:mi:sstzhtzm') as rating_date
	                  from cwms_v_rating
	                 where aliased_item is null
	               ) q4 on q4.rating_code = q1.parent_rating_code
	               left outer join
	               (select rating_spec,
	                       to_char(from_tz(cast(effective_date as timestamp), 'UTC') at time zone cwms_util.get_timezone('%s'),
	                               'yyyy/mm/dd hh24:mi:sstzhtzm') as rating_date,
	                       source_rating
	                  from cwms_v_virtual_rating
	               ) q5 on q5.source_rating = q1.rating_id
	               left outer join
	               (select rating_spec,
	                       to_char(from_tz(cast(effective_date as timestamp), 'UTC') at time zone cwms_util.get_timezone('%s'),
	                               'yyyy/mm/dd hh24:mi:sstzhtzm') as rating_date,
	                       source_rating_spec
	                  from cwms_v_transitional_rating
	               ) q6 on q6.source_rating_spec = q1.rating_id
	               join
	               (select rating_id
	                  from cwms_v_rating_spec
	                 where upper(template_id) like upper(cwms_util.normalize_wildcards(:17)) escape '\\'
	                   and office_id = :18
	                   and aliased_item is null
	               ) q7 on q7.rating_id = q1.rating_id
	       )
	 group by rating_id,
	          rating_date,
	          parent_rating_id,
	          parent_rating_date,
	          virtual_rating_parent,
	          virtual_rating_date,
	          transitional_rating_parent,
	          transitional_rating_date,
	          rating_type
	 order by 1, 2
	'''
query_ratings_by_template_mask_sql = '''
	select rating_id,
	       rating_date,
	       count(matching_child_rating_id) as matching_child_rating_count,
	       parent_rating_id,
	       parent_rating_date,
	       virtual_rating_parent,
	       virtual_rating_date,
	       transitional_rating_parent,
	       transitional_rating_date,
	       rating_type
	  from (select q1.rating_id,
	               to_char(from_tz(cast(q1.effective_date as timestamp), 'UTC') at time zone cwms_util.get_timezone('%s'),
	                       'yyyy/mm/dd hh24:mi:sstzhtzm') as rating_date,
	               q3.matching_child_rating_id,
	               q1.parent_rating_code,
	               q1.rating_type,
	               q4.rating_id as parent_rating_id,
	               q4.rating_date as parent_rating_date,
	               q5.rating_spec as virtual_rating_parent,
	               q5.rating_date as virtual_rating_date,
	               q6.rating_spec as transitional_rating_parent,
	               q6.rating_date as transitional_rating_date
	          from (select rating_code,
	                       rating_id,
	                       effective_date,
	                       parent_rating_code,
	                       'Normal' as rating_type
	                  from cwms_v_rating
	                 where office_id = :1
	                   and aliased_item is null
	                union all
	                select distinct
	                       virtual_rating_code as rating_code,
	                       rating_spec as rating_id,
	                       effective_date,
	                       null as parent_rating_code,
	                       'Virtual' as rating_type
	                  from cwms_v_virtual_rating
	                 where office_id = :2
	                union all
	                select distinct
	                       transitional_rating_code as rating_code,
	                       rating_spec as rating_id,
	                       effective_date,
	                       null as parent_rating_code,
	                       'Transitional' as rating_type
	                  from cwms_v_transitional_rating
	                 where office_id = :3
	               ) q1
	               left outer join
	               (select parent_rating_code,
	                       case
	                       when upper(template_id) like upper(cwms_util.normalize_wildcards(:4)) escape '\\' then rating_id
	                       else null
	                       end as matching_child_rating_id
	                   from cwms_v_rating
	                 where aliased_item is null
	               ) q3 on q3.parent_rating_code = q1.rating_code
	               left outer join
	               (select distinct
	                       rating_code,
	                       rating_id,
	                       to_char(from_tz(cast(effective_date as timestamp), 'UTC') at time zone cwms_util.get_timezone('%s'),
	                               'yyyy/mm/dd hh24:mi:sstzhtzm') as rating_date
	                  from cwms_v_rating
	                 where aliased_item is null
	               ) q4 on q4.rating_code = q1.parent_rating_code
	               left outer join
	               (select rating_spec,
	                       to_char(from_tz(cast(effective_date as timestamp), 'UTC') at time zone cwms_util.get_timezone('%s'),
	                               'yyyy/mm/dd hh24:mi:sstzhtzm') as rating_date,
	                       source_rating
	                  from cwms_v_virtual_rating
	               ) q5 on q5.source_rating = q1.rating_id
	               left outer join
	               (select rating_spec,
	                       to_char(from_tz(cast(effective_date as timestamp), 'UTC') at time zone cwms_util.get_timezone('%s'),
	                               'yyyy/mm/dd hh24:mi:sstzhtzm') as rating_date,
	                       source_rating_spec
	                  from cwms_v_transitional_rating
	               ) q6 on q6.source_rating_spec = q1.rating_id
	               join
	               (select rating_id
	                  from cwms_v_rating_spec
	                 where upper(template_id) like upper(cwms_util.normalize_wildcards(:5)) escape '\\'
	                   and office_id = :6
	                   and aliased_item is null
	               ) q7 on q7.rating_id = q1.rating_id
	       )
	 group by rating_id,
	          rating_date,
	          parent_rating_id,
	          parent_rating_date,
	          virtual_rating_parent,
	          virtual_rating_date,
	          transitional_rating_parent,
	          transitional_rating_date,
	          rating_type
	 order by 1, 2
	'''
query_ratings_by_spec_mask_sql  = query_ratings_by_template_mask_sql.replace("template_id", "rating_id")
retrieve_individual_rating_sql = '''
	select cwms_rating.retrieve_ratings_xml3_f(
	          :1,
	          from_tz(to_timestamp(:2, 'yyyy/mm/dd hh24:mi:ss'), :3) at time zone 'UTC',
	          from_tz(to_timestamp(:4, 'yyyy/mm/dd hh24:mi:ss'), :5) at time zone 'UTC',
	          'UTC',
	          :6
	       ) as rating_xml
	from dual
	'''
delete_single_rating_sql = '''
	declare
	   l_rating_id      varchar2(612) := :1;
	   l_effective_date date          := to_date(:2, 'yyyy/mm/dd hh24:mi:ss');
	   l_time_zone      varchar2(28)  := :3;
	   l_office_id      varchar2(16)  := :4;
	begin
	   cwms_rating.delete_ratings(
	      p_spec_id_mask         => l_rating_id,
	      p_effective_date_start => l_effective_date,
	      p_effective_date_end   => l_effective_date,
	      p_time_zone            => l_time_zone,
	      p_office_id_mask       => l_office_id);
	   commit;
	end;
	'''
program_name = os.path.splitext(os.path.split(sys.argv[0])[1])[0]
if program_name.upper() == "DELETERATINGINFO" : program_name = "DeleteRatingInfo"

usage_blurb = '''
	Program %s

	Deletes CWMS rating information (templates and/or specs and/or ratings).

	Usage: Jython %s <options>

	Where <options> are:

		-m or --mask       <delete_mask>   Optional, wildcard mask of items to delete
		-f or --office     <office_id>     Optional, defaults to connection default office
		-b or --begin      <date/time>     Optional, start of time window
		-e or --end        <date/time>     Optional, end of time window
		-z or --timezone   <time_zone>     Optional, default is local time zone
		-d or --dir        <directory>     Optional, backup file directory, defaults to .
		-t or --templates                  Optional, specifies delete templates
		-s or --specs                      Optional, specifies delete specs
		-p or --empty                      Optional, delete empty templates or specs
		-r or --ratings                    Optional, specifies delete ratings
		-v or --verbose                    Optional, list items to delete
		-c or --nochild                    Optional, don't list child ratings
		-a or --noparent                   Optional, don't list parent ratings
		-k or --backup                     Optional, default is no backup unless execute
		-x or --execute                    Optional, default is to preview

	Only one of -t/--templates, -s/--specs, or -r/--ratings may be specified.

	When -t or --templates is specified, the templates matching the mask, along
	with all associated specifications and all associated ratings will be deleted.

	When -s or --specs is specified, the specifications matching the mask, along
	with all associated ratings will be deleted. No templates will be deleted.

	When -r or --ratings is specified, the ratings matching the mask and the
	time window, if specified, will be deleted. No specifications or templates
	will be deleted

	When -p or --empty is specified (not effective with -r/--ratings), empty
	items (i.e., templates with no specifications or specifications with no
	ratings) are added to the items matched by the mask (if specified). At least
	one of -m/--mask of -p/--empty must be specified.

	The time window only applies when -r or --ratings is specified. If -b/--begin
	and/or -e/--end are not specified, the time window will not be constrained
	by a beginning and/or end, respectively. E.g., specifying only -e/--end will
	delete all ratings on or before the specified time, while specifying neither
	will cause all ratings matching the mask to be deleted. The date/time values
	must be in a format that is recognized by HecTime. If the values contain
	spaces, they must be quoted.

	The time window is interpreted in the specified or default (local) time zone.
	The effective dates of listed ratings are also output in this time zone.

	When -v or --verbose is specified, all items (templates, specifications,
	and ratings) that will be deleted are listed. Otherwise, only the number of
	each item type that will be deleted is listed.

	When -k or --backup is specified, the program will back up all items that
	will be deleted plus all items that have child or source ratings that will
	be deleted. Backup is always performed with -x/--execute, so this option is
	for backing up without actually deleting.

	When -x or --execute is specified, The programs will ask for verification
	and before deleting the items (after outputting the count or list of them).
	Otherwise, the program only outputs the count or list of items that would
	be deleted.

	When -c -or --nochild is specified, no child ratings are listed in the
	verbose output (and thus no parent ratings are output for the child ratings).
	Parent ratings will still be listed for source ratings of virtual or trans-
	itional raiings unless -a/--noparent is also specified. Has no effect without
	-v/--verbose.

	When -a or --noparent is specified, no parent ratings are listed in the
	verbose output for child ratings or source ratings of virtual or transitional
	ratings. Has no effect without -v/--verbose

''' % (program_name, sys.argv[0])

def usage(message) :
	'''
	Output an error message followed by the usage blurb
	'''
	if message : sys.stdout.write("\n\tERROR : %s\n" % message)
	sys.stdout.write("\n%s\n" % usage_blurb)
	exit()

def pos_after(source, substr) :
	'''
	Get the position in a string after a specified portion
	'''
	pos = source.find(substr)
	if pos != -1 : pos += len(substr)
	return pos

def output_ratings(rs) :
	'''
	Output ratings returned from the rating query
	'''
	ratings = []
	rating_ids = set()
	rating_ids_dates = set()
	normal_parents_deleted = set()
	normal_parents_not_deleted = set()
	transitional_parents_deleted = set()
	transitional_parents_not_deleted = set()
	virtual_parents_deleted = set()
	virtual_parents_not_deleted = set()
	children_deleted_with_parent = {}
	while rs.next() :
		rating_id                = rs.getString("RATING_ID")
		rating_date              = rs.getString("RATING_DATE")
		parent_id                = rs.getString("PARENT_RATING_ID")
		parent_date              = rs.getString("PARENT_RATING_DATE")
		virtual_parent_id        = rs.getString("VIRTUAL_RATING_PARENT")
		virtual_parent_date      = rs.getString("VIRTUAL_RATING_DATE")
		transitional_parent_id   = rs.getString("TRANSITIONAL_RATING_PARENT")
		transitional_parent_date = rs.getString("TRANSITIONAL_RATING_DATE")
		rating_type              = rs.getString("RATING_TYPE")
		str = "%s %s %s" % (rating_type[0], rating_date, rating_id)
		if parent_id :
			str += "\n\t          ...child of rating %s %s" % (parent_id, parent_date)
			rating_type = "Child"
			children_deleted_with_parent.setdefault("%s %s" % (parent_id, parent_date), []).append( "%s %s" % (rating_id, rating_date))
		elif virtual_parent_id :
			str += "\n\t          ...source of virtual rating %s %s" % (virtual_parent_id, virtual_parent_date)
		elif transitional_parent_id :
			str += "\n\t          ...source of transitional rating %s %s" % (transitional_parent_id, transitional_parent_date)
		ratings.append(str)
		rating_ids.add(rating_id)
		rating_ids_dates.add("%s %s" % (rating_id, rating_date))
	for i in range(len(ratings)) :
		if ratings[i].find("\n") != -1 :
			if ratings[i].find("child of") != -1 :
				parent = ratings[i][pos_after(ratings[i], "...child of rating "):]
				if parent in rating_ids_dates :
					normal_parents_deleted.add(parent)
					ratings[i] += " which is also being deleted"
				else :
					del children_deleted_with_parent[parent]
					normal_parents_not_deleted.add(parent)
					ratings[i] += " which is NOT being deleted"
			elif ratings[i].find("source of virtual") != -1 :
				parent = ratings[i][pos_after(ratings[i], "virtual rating "):]
				if parent in rating_ids_dates :
					virtual_parents_deleted.add(parent)
					ratings[i] += " which is also being deleted"
				else :
					virtual_parents_not_deleted.add(parent)
					ratings[i] += " which is NOT being deleted"
			elif ratings[i].find("source of transitional") != -1 :
				parent = ratings[i][pos_after(ratings[i], "transitional rating "):]
				if parent in rating_ids_dates :
					transitional_parents_deleted.add(parent)
					ratings[i] += " which is also being deleted"
				else :
					transitional_parents_not_deleted.add(parent)
					ratings[i] += " which is NOT being deleted"

	children = list(children_deleted_with_parent.values())
	children_deleted_with_parent = set() # convert from dictionary keyed by parent to set
	for child_list in children :
		for child in child_list :
			children_deleted_with_parent.add(child)
	print(rating_header_1 % (
		len(ratings) \
		+ len(normal_parents_not_deleted) \
		+ len(virtual_parents_not_deleted) \
		+ len(transitional_parents_not_deleted) \
		- len(children_deleted_with_parent),
		len(children_deleted_with_parent)))
	print("                               %5d are [ normal       ] ratings not being deleted with [ child  ] ratings being deleted" % len(normal_parents_not_deleted))
	print("                               %5d are [ virtual      ] ratings not being deleted with [ source ] ratings being deleted" % len(virtual_parents_not_deleted))
	print("                               %5d are [ transitional ] ratings not being deleted with [ source ] ratings being deleted" % len(transitional_parents_not_deleted))
	print(rating_header_2 % (len(ratings) - len(children_deleted_with_parent), len(children_deleted_with_parent)))
	if verbose :
		skipped = 0
		for i in range(len(ratings)) :
			if nochild :
				if ratings[i].find("child of") != -1 :
					skipped += 1
					continue
			rating = ratings[i]
			if noparent :
				pos = ratings[i].find("\n\t")
				if pos != -1 : rating = rating[:pos]
			print("\t%5d : %s" % (i+1-skipped, rating))
	else :
		if normal_parents_not_deleted :
			print("\nThe following NORMAL ratings are not being deleted but have child ratings being deleted:")
			for rating in sorted(normal_parents_not_deleted) : print("\t%s" % rating)
		if virtual_parents_not_deleted :
			print("\nThe following VIRTUAL ratings are not being deleted but have source ratings being deleted:")
			for rating in sorted(virtual_parents_not_deleted) : print("\t%s" % rating)
		if transitional_parents_not_deleted :
			print("\nThe following TRANSITIONAL ratings are not being deleted but have source ratings being deleted:")
			for rating in sorted(transitional_parents_not_deleted) : print("\t%s" % rating)

	return rating_ids_dates,                 \
	       children_deleted_with_parent,     \
	       normal_parents_deleted,           \
	       normal_parents_not_deleted,       \
	       transitional_parents_deleted,     \
	       transitional_parents_not_deleted, \
	       virtual_parents_deleted,          \
	       virtual_parents_not_deleted

def output_template_xml(buf, rs) :
	'''
	Writes XML to a buffer for a record from CWMS_V_RATING_TEMPLATE
	'''
	buf.write('  <rating-template office-id="%s">\n' % rs.getString("OFFICE_ID"))
	buf.write('    <parameters-id>%s</parameters-id>\n' % rs.getString("PARAMETERS_ID"))
	buf.write('    <version>%s</version>\n' % rs.getString("VERSION"))
	buf.write('    <ind-parameter-specs>\n')
	i = 0
	all_rating_methods = rs.getString("RATING_METHODS").split("/")
	for ind_param in rs.getString("INDEPENDENT_PARAMETERS").split(",") :
		this_rating_methods = all_rating_methods[i].split(',')
		i += 1
		buf.write('      <ind-parameter-spec position="%d">\n' % i)
		buf.write('        <parameter>%s</parameter>\n' % ind_param)
		buf.write('        <in-range-method>%s</in-range-method>\n' % this_rating_methods[1])
		buf.write('        <out-range-low-method>%s</out-range-low-method>\n' % this_rating_methods[0])
		buf.write('        <out-range-high-method>%s</out-range-high-method>\n' % this_rating_methods[2])
		buf.write('      </ind-parameter-spec>\n')
	buf.write('    </ind-parameter-specs>\n')
	buf.write('    <dep-parameter>%s</dep-parameter>\n' % rs.getString("DEPENDENT_PARAMETER"))
	description = rs.getString("DESCRIPTION")
	if description :
		buf.write('    <description>%s</description>\n' % description)
	else :
		buf.write('    <description/>\n')
	buf.write('  </rating-template>\n')

def output_spec_xml(buf, rs) :
	'''
	Writes XML to a buffer for a record in CWMS_V_RATING_SPEC
	'''
	buf.write('  <rating-spec office-id="%s">\n' % rs.getString("OFFICE_ID"))
	buf.write('    <rating-spec-id>%s</rating-spec-id>\n' % rs.getString("RATING_ID"))
	buf.write('    <template-id>%s</template-id>\n' % rs.getString("TEMPLATE_ID"))
	buf.write('    <location-id>%s</location-id>\n' % rs.getString("LOCATION_ID"))
	buf.write('    <version>%s</version>\n' % rs.getString("VERSION"))
	source_agency = rs.getString("SOURCE_AGENCY")
	if source_agency :
		buf.write('    <source-agency>%s</source-agency>\n' % source_agency)
	else :
		buf.write('    <source-agency/>\n')
	date_methods = rs.getString("DATE_METHODS").split(",")
	buf.write('    <in-range-method>%s</in-range-method>\n' % date_methods[1])
	buf.write('    <out-range-low-method>%s</out-range-low-method>\n' % date_methods[0])
	buf.write('    <out-range-high-method>%s</out-range-high-method>\n' % date_methods[2])
	buf.write('    <active>%s</active>\n' % ("false", "true")[rs.getString("ACTIVE_FLAG") == "T"])
	buf.write('    <auto-update>%s</auto-update>\n' % ("false", "true")[rs.getString("AUTO_UPDATE_FLAG") == "T"])
	buf.write('    <auto-activate>%s</auto-activate>\n' % ("false", "true")[rs.getString("AUTO_ACTIVATE_FLAG") == "T"])
	buf.write('    <auto-migrate-extension>%s</auto-migrate-extension>\n' % ("false", "true")[rs.getString("AUTO_MIGRATE_EXT_FLAG") == "T"])
	buf.write('    <ind-rounding-specs>\n')
	rounding_specs = rs.getString("IND_ROUNDING_SPECS").split("/")
	for i in range(len(rounding_specs)) :
		buf.write('      <ind-rounding-spec position="%d">%s</ind-rounding-spec>\n' % (i+1, rounding_specs[i]))
	buf.write('    </ind-rounding-specs>\n')
	buf.write('    <dep-rounding-spec>%s</dep-rounding-spec>\n' % rs.getString("DEP_ROUNDING_SPEC"))
	description = rs.getString("DESCRIPTION")
	if description :
		buf.write('    <description>%s</description>\n' % description)
	else :
		buf.write('    <description/>\n')
	buf.write('  </rating-spec>\n')

#--------------------------#
# process the command line #
#--------------------------#
short_opts = "m:f:b:e:z:d:tsrpvcakx"
long_opts  = [
	"mask=",
	"office=",
	"begin=",
	"end=",
	"timezone=",
	"dir=",
	"templates",
	"specs",
	"ratings",
	"empty",
	"verbose",
	"nochild",
	"noparent",
	"backup",
	"execute"
]
time_zone_str = None
try :
	opts, extra = getopt(sys.argv[1:], short_opts, long_opts)
except Exception as e :
	usage(str(e))
except GetoptError as e :
	usage(str(e))
	
for flag, arg in opts :
	if   flag in ("-m", "--mask"       ) : mask             = arg
	elif flag in ("-f", "--office"     ) : office           = arg
	elif flag in ("-b", "--begin"      ) : start_time       = arg
	elif flag in ("-e", "--end"        ) : end_time         = arg
	elif flag in ("-z", "--timezone"   ) : time_zone_str    = arg
	elif flag in ("-d", "--dir"        ) : outdir           = arg
	elif flag in ("-t", "--templates"  ) : delete_templates = True
	elif flag in ("-s", "--specs"      ) : delete_specs     = True
	elif flag in ("-r", "--ratings"    ) : delete_ratings   = True
	elif flag in ("-p", "--empty"      ) : delete_empty     = True
	elif flag in ("-v", "--verbose"    ) : verbose          = True
	elif flag in ("-c", "--nochild"    ) : nochild          = True
	elif flag in ("-a", "--noparent"   ) : noparent         = True
	elif flag in ("-k", "--backup"     ) : backup           = True
	elif flag in ("-x", "--execute"    ) : execute = backup = True
if extra : usage("Invalid input: '%s'" % extra)
if sum(map(int, [delete_templates, delete_specs, delete_ratings])) != 1 :
	usage("Exactly one of -t/--templates, -s/--specs, -r/--ratings must be specified")
if not mask and not delete_empty :
	if delete_templates :
		usage("At least of -m/--mask or -p/--empty must be specified with -t/--templates")
	if delete_specs :
		usage("At least of -m/--mask or -p/--empty must be specified with -t/--specs")
if delete_ratings and not mask :
	usage("A non-empty mask must be specified with -r/--ratings")
try :
	if time_zone_str :
		if time_zone_str == "CST" : time_zone_str = "Etc/GMT+6"
		if time_zone_str == "PST" : time_zone_str = "Etc/GMT+8"
		time_zone = TimeZone.getTimeZone(time_zone_str)
	else :
		time_zone = TimeZone.getDefault()
except Exception as e :
	usage(str(e))
if not os.path.exists(outdir) or not os.path.isdir(outdir) :
	usage("Specified directory %s does not exist or is not a directory" % (outdir))
time_zone_str = time_zone.getID()
sdf = SimpleDateFormat("yyyy/MM/dd HH:mm:ssZ")
sdf.setTimeZone(time_zone)
if delete_ratings and (start_time or end_time) :
	sdf_hectime = SimpleDateFormat("ddMMMyyyy, HH:mm:ss")
	t = HecTime(HecTime.SECOND_GRANULARITY)
	if start_time :
		try :
			t.set(start_time)
			if not t.time() : t.set("%s;0000" % t.date(4))
			start_time = sdf.format(sdf_hectime.parse(t.dateAndTime(4)))
		except Exception as e :
			usage("start_time : %s" % str(e))
	if end_time :
		try :
			t.set(end_time)
			if not t.time() : t.set("%s;2400" % t.date(4))
			end_time = sdf.format(sdf_hectime.parse(t.dateAndTime(4)))
		except Exception as e :
			usage("end_time : %s" % str(e))
#---------------------------------#
# output command line information #
#---------------------------------#
item_type = "templates" if delete_templates else "specifications" if delete_specs else "ratings"
sys.stdout.write("\nWill delete ")
if mask :
	sys.stdout.write('%s matching "%s"%s' % (item_type, mask, ("\n", " and ")[delete_empty]))
if delete_empty :
	print("empty %s" % item_type)
if delete_ratings :
	if start_time :
		if end_time :
			print("\twith effective dates between %s and %s" % (start_time, end_time))
		else :
			print("\twith effective dates on or after %s" % start_time)
	elif end_time :
		print("\twith effective dates on or before %s" % end_time)
elif start_time or end_time :
	print("Ignoring specified time window (effective only when deleting ratings)")
print("Time zone      : %s" % time_zone_str)
print("Verbose output : %s" % verbose)
if verbose :
	print("                 %s child ratings" % ("with", "without")[nochild])
	print("                 %s parent ratings" % ("with", "without")[noparent])
print("Will backup    : %s" % backup)
print("Will delete    : %s" % execute)

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
	if not office :
		office = db.getOfficeId()
	#--------------------------------------#
	# query and output items to be deleted #
	#--------------------------------------#
	if delete_templates :
		#---------------------------------------------#
		# templates and associated specs, and ratings #
		#---------------------------------------------#
		#-----------------#
		# query templates #
		#-----------------#
		templates = set() # use set since some templates might match mask and be empty
		empty_templates = []
		if delete_empty :
			stmt = db.getConnection().prepareStatement(query_empty_templates_sql)
			stmt.setString(1, office)
			stmt.setString(2, office)
			rs = stmt.executeQuery()
			while rs.next() :
				template_id = rs.getString("TEMPLATE_ID")
				templates.add(template_id)
				empty_templates.append(template_id)
			rs.close()
			stmt.close()
		if mask :
			stmt = db.getConnection().prepareStatement(query_templates_by_mask_sql)
			stmt.setString(1, mask)
			stmt.setString(2, office)
			rs = stmt.executeQuery()
			while rs.next() : templates.add(rs.getString("TEMPLATE_ID"))
			rs.close()
			stmt.close()
		print(template_header % len(templates))
		templates = sorted(templates) # convert from set to sorted list
		if verbose :
			for i in range(len(templates)) : print("\t%5d : %s" % (i+1, templates[i]))
		if mask :
			#-------------#
			# query specs #
			#-------------#
			stmt = db.getConnection().prepareStatement(query_specs_by_template_mask_sql)
			stmt.setString(1, mask)
			stmt.setString(2, office)
			rs = stmt.executeQuery()
			specs = []
			while rs.next() : specs.append(rs.getString("RATING_ID"))
			rs.close()
			stmt.close()
			print(spec_header % len(specs))
			if verbose :
				for i in range(len(specs)) : print("\t%5d : %s" % (i+1, specs[i]))
			#---------------#
			# query ratings #
			#---------------#
			stmt = db.getConnection().prepareStatement(query_ratings_by_template_mask_sql % (time_zone_str, time_zone_str, time_zone_str, time_zone_str)) # won't work as bind variable
			for i in 1, 2, 3, 6 : stmt.setString(i, office)
			for i in 4, 5 : stmt.setString(i, mask)
			rs = stmt.executeQuery()
			rating_ids_dates,                 \
			children_deleted_with_parent,     \
			normal_parents_deleted,           \
			normal_parents_not_deleted,       \
			transitional_parents_deleted,     \
			transitional_parents_not_deleted, \
			virtual_parents_deleted,          \
			virtual_parents_not_deleted = output_ratings(rs)
			rs.close()
			stmt.close()
		else :
			#----------------------------------------------------------------------------#
			# deleting only empty templates, there will be no specifications or ratings  #
			#----------------------------------------------------------------------------#
			print(spec_header % 0)
			print(rating_header_1 % (0, 0))
			print(rating_header_2 % (0, 0))
			normal_parents_not_deleted = set()
			virtual_parents_not_deleted = set()
			transitional_parents_not_deleted = set()
		if backup :
			if execute and input(delete_prompt).upper() != "YES" : exit()
			#--------------------------------#
			# backup the items to be deleted #
			#--------------------------------#
			print("\nBacking up items to be deleted.")
			buf = StringIO()
			buf.write('<?xml version="1.0" encoding="UTF-8"?>\n')
			buf.write('<ratings xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.hec.usace.army.mil/xmlSchema/cwms/Ratings.xsd">\n')
			stmt = None
			#-------------------#
			# get the templates #
			#-------------------#
			sys.stdout.write("\tTemplates:")
			count = 0
			stmt = db.getConnection().prepareStatement(query_templates_by_mask_sql)
			stmt.setString(1, mask)
			stmt.setString(2, office)
			if mask :
				rs = stmt.executeQuery()
				first = True
				while rs.next() :
					count += 1
					sys.stdout.write("%5d%s" % (count, 5*"\b"))
					sys.stdout.flush()
					if first :
						first = False
						buf.write('<!-- TEMPLATES MATCHING MASK "%s" -->\n' % mask)
					output_template_xml(buf, rs)
				rs.close()
			first = True
			for template_id in sorted(empty_templates) :
				count += 1
				sys.stdout.write("%5d%s" % (count, 5*"\b"))
				sys.stdout.flush()
				stmt.setString(1, template_id)
				rs = stmt.executeQuery()
				rs.next()
				if first :
					first = False
					buf.write('<!-- EMPTY TEMPLATES (NO ASSOCIATED SPECIFICATIONS) -->\n')
				output_template_xml(buf, rs)
				rs.close()
			stmt.close()
			print("%5d" % count)
			if mask :
				#---------------#
				# get the specs #
				#---------------#
				sys.stdout.write("\tSpecs    :")
				count = 0
				stmt = db.getConnection().prepareStatement(query_specs_by_mask_sql)
				stmt.setString(1, mask)
				stmt.setString(2, office)
				rs = stmt.executeQuery()
				first = True
				while rs.next() :
					count += 1
					sys.stdout.write("%5d%s" % (count, 5*"\b"))
					sys.stdout.flush()
					if first :
						first = False
						buf.write('<!-- SPECS WITH TEMPLATES MATCHING MASK "%s" -->\n' % mask)
					output_spec_xml(buf, rs)
				rs.close()
				stmt.close()
				print("%5d" % count)
				stmt = None
				#-----------------#
				# get the ratings #
				#-----------------#
				sys.stdout.write("\tRatings  :")
				count = 0
				deleted_with_parent_count = 0
				stmt = db.getConnection().prepareStatement(retrieve_individual_rating_sql)
				stmt.setString(6, office)
				for ratings_set in rating_ids_dates, normal_parents_not_deleted, virtual_parents_not_deleted, transitional_parents_not_deleted :
					first = True
					for rating_id_date in ratings_set :
						if rating_id_date in children_deleted_with_parent :
							deleted_with_parent_count += 1
							continue
						count += 1
						sys.stdout.write("%5d%s" % (count, 5*"\b"))
						sys.stdout.flush()
						rating_id, date = [s.strip() for s in [rating_id_date[:-24], rating_id_date[-24:]]]
						stmt.setString(1, rating_id)
						for i in 2, 4 : stmt.setString(i, date[:19])
						for i in 3, 5 : stmt.setString(i, "%s:%s" % (date[19:22], date[22:]))
						rs = stmt.executeQuery()
						rs.next()
						clob = rs.getClob("RATING_XML")
						xml = clob.getSubString(1, clob.length())
						clob.free()
						lines = xml.strip().split("\n")
						if first :
							first = False
							if ratings_set is rating_ids_dates :
								buf.write('<!-- RATINGS WITH TEMPLATES MATCHING MASK "%s" -->\n' % mask)
							if ratings_set is normal_parents_not_deleted :
								buf.write("<!-- PARENT RATINGS WITH CHILD RATINGS DELETED -->\n")
							elif ratings_set is virtual_parents_not_deleted :
								buf.write("<!-- VIRTUAL RATINGS WITH SOURCE RATINGS DELETED -->\n")
							elif ratings_set is transitional_parents_not_deleted :
								buf.write("<!-- TRANSITIONAL RATINGS WITH SOURCE RATINGS DELETED -->\n")
						buf.write("%s\n" % "\n".join(lines[2:-1]))
				print("%5d" % count)
			else :
				print("\tSpecs    :%5d\n\tRatings  :%5d" % (0, 0))
			if stmt : stmt.close()
			buf.write('</ratings>')
			filename = os.path.join(
				os.path.abspath(outdir),
				"delete_templates.%s.xml" % str(datetime.now())[:19].replace(" ", "_").replace(":", "."))
			sys.stdout.write("\nWriting to backup file %s ... " % filename)
			with open(filename, "w") as f : f.write(buf.getvalue())
			buf.close()
			print("%s bytes written" % "{:,}".format(os.stat(filename)[stat.ST_SIZE]))
		if execute :
			#------------------#
			# delete the items #
			#------------------#
			sys.stdout.write("\nDeleting items\n\tTemplates:")
			sys.stdout.flush()
			count = 0
			deleted_with_parent_count = 0
			rating_count = 0
			stmt = db.getConnection().prepareCall(delete_single_template_sql);
			stmt.setString(2, DELETE_ALL)
			stmt.setString(3, office)
			if mask :
				count_stmt = db.getConnection().prepareStatement(query_ratings_by_template_mask_sql % (time_zone_str, time_zone_str, time_zone_str, time_zone_str))
				for i in 1, 2, 3, 6 : count_stmt.setString(i, office)
				count_stmt.setString(4, mask)
				#----------------------------------------------------------------#
				# get the counts so they're not affected as the deletes progress #
				#----------------------------------------------------------------#
				banner = "...getting counts for each template matching mask..."
				sys.stdout.write(banner)
				sys.stdout.flush()
				for template_id in templates :
					count_stmt.setString(5, template_id)
					rs = count_stmt.executeQuery()
					while rs.next() :
						rating_count += 1
						deleted_with_parent_count += rs.getInt("MATCHING_CHILD_RATING_COUNT")
					rs.close()
				count_stmt.close()
				rating_count -= deleted_with_parent_count
				sys.stdout.write("\b" * len(banner))
				sys.stdout.write(" " * len(banner))
				sys.stdout.write("\b" * len(banner))
				#---------------------#
				# perform the deletes #
				#---------------------#
				for template_id in templates :
					count += 1
					sys.stdout.write("%5d%s" % (count, 5*"\b"))
					sys.stdout.flush()
					stmt.setString(1, template_id)
					stmt.execute()
			stmt.setString(2, DELETE_KEY)
			for template_id in empty_templates :
				stmt.setString(1, template_id)
				stmt.execute()
				count += 1
				sys.stdout.write("%5d%s" % (count, 5*"\b"))
			stmt.close()
			print("%5d" % count)
			print("\tSpecs    :%5d\n\tRatings  :%5d" % (len(specs), rating_count))
			sys.stdout.flush()
	elif delete_specs :
		#------------------------------#
		# specs and associated ratings #
		#------------------------------#
		print(template_header % 0)
		#-------------#
		# query specs #
		#-------------#
		specs = set() # use set since some specs might match mask and be empty
		empty_specs = []
		if delete_empty :
			stmt = db.getConnection().prepareStatement(query_empty_specs_sql)
			for i in 1,2,3,4 : stmt.setString(i, office)
			rs = stmt.executeQuery()
			while rs.next() :
				rating_id = rs.getString("RATING_ID")
				specs.add(rating_id)
				empty_specs.append(rating_id)
			rs.close()
			stmt.close()
		if mask :
			stmt = db.getConnection().prepareStatement(query_specs_by_mask_sql)
			stmt.setString(1, mask)
			stmt.setString(2, office)
			rs = stmt.executeQuery()
			while rs.next() : specs.add(rs.getString("RATING_ID"))
			rs.close()
			stmt.close()
		print(spec_header % len(specs))
		specs = sorted(specs) # convert from set to sorted list
		if verbose :
			for i in range(len(specs)) : print("\t%5d : %s" % (i+1, specs[i]))
		if mask:
			#---------------#
			# query ratings #
			#---------------#
			stmt = db.getConnection().prepareStatement(query_ratings_by_spec_mask_sql % (time_zone_str, time_zone_str, time_zone_str, time_zone_str)) # won't work as bind variable
			for i in 1, 2, 3, 6 : stmt.setString(i, office)
			for i in 4, 5 : stmt.setString(i, mask)
			rs = stmt.executeQuery()
			rating_ids_dates,                 \
			children_deleted_with_parent,     \
			normal_parents_deleted,           \
			normal_parents_not_deleted,       \
			transitional_parents_deleted,     \
			transitional_parents_not_deleted, \
			virtual_parents_deleted,          \
			virtual_parents_not_deleted = output_ratings(rs)
			rs.close()
			stmt.close()
		else :
			#-----------------------------------------------------#
			# deleting only empty specs, there will be no ratings #
			#-----------------------------------------------------#
			print(rating_header_1 % (0, 0))
			print(rating_header_2 % (0, 0))
		if backup :
			if execute and input(delete_prompt).upper() != "YES" : exit()
			#--------------------------------#
			# backup the items to be deleted #
			#--------------------------------#
			print("\nBacking up items to be deleted.")
			buf = StringIO()
			buf.write('<?xml version="1.0" encoding="UTF-8"?>\n')
			buf.write('<ratings xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.hec.usace.army.mil/xmlSchema/cwms/Ratings.xsd">\n')
			stmt = None
			sys.stdout.write("\tTemplates:%5d\n\tSpecs    :" % 0)
			count = 0
			deleted_with_parent_count = 0
			if mask :
				#---------------#
				# get the specs #
				#---------------#
				stmt = db.getConnection().prepareStatement(query_specs_by_mask_sql)
				stmt.setString(1, mask)
				stmt.setString(2, office)
				rs = stmt.executeQuery()
				first = True
				while rs.next() :
					count += 1
					sys.stdout.write("%5d%s" % (count, 5*"\b"))
					sys.stdout.flush()
					if first :
						first = False
						buf.write('<!-- SPECS MATCHING MASK "%s" -->\n' % mask)
					output_spec_xml(buf, rs)
				rs.close()
				stmt.close()
				stmt = None
			first = True
			for spec_id in sorted(empty_specs) :
				count += 1
				sys.stdout.write("%5d%s" % (count, 5*"\b"))
				sys.stdout.flush()
				if not stmt :
					stmt = db.getConnection().prepareStatement(query_specs_by_mask_sql)
					stmt.setString(2, office)
				stmt.setString(1, spec_id)
				rs = stmt.executeQuery()
				rs.next()
				if first :
					first = False
					buf.write('<!-- EMPTY SPECS (NO ASSOCIATED RATINGS) -->\n')
				output_spec_xml(buf, rs)
				rs.close()
			print("%5d" % count)
			sys.stdout.write("\tRatings  :")
			count = 0
			if mask :
				#-----------------#
				# get the ratings #
				#-----------------#
				stmt = db.getConnection().prepareStatement(retrieve_individual_rating_sql)
				stmt.setString(6, office)
				for ratings_set in rating_ids_dates, normal_parents_not_deleted, virtual_parents_not_deleted, transitional_parents_not_deleted :
					first = True
					for rating_id_date in ratings_set :
						if rating_id_date in children_deleted_with_parent :
							deleted_with_parent_count += 1
							continue
						count += 1
						sys.stdout.write("%5d%s" % (count, 5*"\b"))
						sys.stdout.flush()
						rating_id, date = [s.strip() for s in [rating_id_date[:-24], rating_id_date[-24:]]]
						stmt.setString(1, rating_id)
						for i in 2, 4 : stmt.setString(i, date[:19])
						for i in 3, 5 : stmt.setString(i, "%s:%s" % (date[19:22], date[22:]))
						rs = stmt.executeQuery()
						rs.next()
						clob = rs.getClob("RATING_XML")
						xml = clob.getSubString(1, clob.length())
						clob.free()
						lines = xml.strip().split("\n")
						if first :
							first = False
							if ratings_set is rating_ids_dates :
								buf.write('<!-- RATINGS WITH SPECIFICATIONS MATCHING MASK "%s" -->\n' % mask)
							if ratings_set is normal_parents_not_deleted :
								buf.write("<!-- PARENT RATINGS WITH CHILD RATINGS DELETED -->\n")
							elif ratings_set is virtual_parents_not_deleted :
								buf.write("<!-- VIRTUAL RATINGS WITH SOURCE RATINGS DELETED -->\n")
							elif ratings_set is transitional_parents_not_deleted :
								buf.write("<!-- TRANSITIONAL RATINGS WITH SOURCE RATINGS DELETED -->\n")
						buf.write("%s\n" % "\n".join(lines[2:-1]))
			print("%5d" % count)
			if stmt : stmt.close()
			buf.write('</ratings>')
			filename = os.path.join(
				os.path.abspath(outdir),
				"delete_specs.%s.xml" % str(datetime.now())[:19].replace(" ", "_").replace(":", "."))
			sys.stdout.write("\nWriting to backup file %s ... " % filename)
			with open(filename, "w") as f : f.write(buf.getvalue())
			buf.close()
			print("%s bytes written" % "{:,}".format(os.stat(filename)[stat.ST_SIZE]))
		if execute :
			#------------------#
			# delete the items #
			#------------------#
			sys.stdout.write("\nDeleting items\n\tTemplates:%5d\n\tSpecs    :" % 0)
			sys.stdout.flush()
			count = 0
			deleted_with_parent_count = 0
			rating_count = 0
			stmt = db.getConnection().prepareCall(delete_single_spec_sql);
			stmt.setString(2, DELETE_ALL)
			stmt.setString(3, office)
			if mask :
				count_stmt = db.getConnection().prepareStatement(query_ratings_by_spec_mask_sql % (time_zone_str, time_zone_str, time_zone_str, time_zone_str))
				for i in 1, 2, 3, 6 : count_stmt.setString(i, office)
				count_stmt.setString(4, mask)
				#----------------------------------------------------------------#
				# get the counts so they're not affected as the deletes progress #
				#----------------------------------------------------------------#
				banner = "...getting counts for each spec matching mask..."
				sys.stdout.write(banner)
				sys.stdout.flush()
				for spec_id in specs :
					count_stmt.setString(5, spec_id)
					rs = count_stmt.executeQuery()
					while rs.next() :
						rating_count += 1
						deleted_with_parent_count += rs.getInt("MATCHING_CHILD_RATING_COUNT")
					rs.close()
				count_stmt.close()
				rating_count -= deleted_with_parent_count
				sys.stdout.write("\b" * len(banner))
				sys.stdout.write(" " * len(banner))
				sys.stdout.write("\b" * len(banner))
				#---------------------#
				# perform the deletes #
				#---------------------#
				for spec_id in specs :
					count += 1
					sys.stdout.write("%5d%s" % (count, 5*"\b"))
					sys.stdout.flush()
					stmt.setString(1, spec_id)
					stmt.execute()
			stmt.setString(2, DELETE_KEY)
			for spec_id in empty_specs :
				stmt.setString(1, spec_id)
				stmt.execute()
				count += 1
				sys.stdout.write("%5d%s" % (count, 5*"\b"))
			stmt.close()
			print("%5d" % count)
			print("\tRatings  :%5d" % rating_count)
			sys.stdout.flush()
	else :
		#--------------#
		# ratings only #
		#--------------#
		print(template_header % 0)
		print(spec_header % 0)
		#---------------#
		# query ratings #
		#---------------#
		stmt = db.getConnection().prepareStatement(query_ratings_by_mask_sql % (time_zone_str, time_zone_str, time_zone_str, time_zone_str)) # won't work as bind variable
		for i in  1,  6, 11, 18         : stmt.setString(i, office)
		for i in  2,  7, 12             : stmt.setString(i, start_time[:19] if start_time else "")
		for i in  3,  5,  8, 10, 13, 15 : stmt.setString(i, time_zone_str)
		for i in  4,  9, 14             : stmt.setString(i, end_time[:19] if end_time else "")
		for i in 16, 17                 : stmt.setString(i, mask)
		rs = stmt.executeQuery()
		rating_ids_dates,                 \
		children_deleted_with_parent,     \
		normal_parents_deleted,           \
		normal_parents_not_deleted,       \
		transitional_parents_deleted,     \
		transitional_parents_not_deleted, \
		virtual_parents_deleted,          \
		virtual_parents_not_deleted = output_ratings(rs)
		rs.close()
		stmt.close()
		if backup :
			if execute and input(delete_prompt).upper() != "YES" : exit()
			#--------------------------------#
			# backup the items to be deleted #
			#--------------------------------#
			print("\nBacking up items to be deleted.")
			buf = StringIO()
			buf.write('<?xml version="1.0" encoding="UTF-8"?>\n')
			buf.write('<ratings xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.hec.usace.army.mil/xmlSchema/cwms/Ratings.xsd">\n')
			stmt = None
			sys.stdout.write("\tTemplates:%5d\n\tSpecs    :%5d\n\tRatings  :" % (0, 0))
			count = 0
			deleted_with_parent_count = 0
			stmt = db.getConnection().prepareStatement(retrieve_individual_rating_sql)
			stmt.setString(6, office)
			for ratings_set in rating_ids_dates, normal_parents_not_deleted, virtual_parents_not_deleted, transitional_parents_not_deleted :
				first = True
				for rating_id_date in ratings_set :
					if rating_id_date in children_deleted_with_parent :
						deleted_with_parent_count += 1
						continue
					count += 1
					sys.stdout.write("%5d%s" % (count, 5*"\b"))
					sys.stdout.flush()
					rating_id, date = [s.strip() for s in [rating_id_date[:-24], rating_id_date[-24:]]]
					stmt.setString(1, rating_id)
					for i in 2, 4 : stmt.setString(i, date[:19])
					for i in 3, 5 : stmt.setString(i, "%s:%s" % (date[19:22], date[22:]))
					rs = stmt.executeQuery()
					rs.next()
					clob = rs.getClob("RATING_XML")
					xml = clob.getSubString(1, clob.length())
					clob.free()
					lines = xml.strip().split("\n")
					if first :
						first = False
						if ratings_set is rating_ids_dates :
							buf.write('<!-- RATINGS WITH SPECIFICATIONS MATCHING MASK "%s" -->\n' % mask)
						if ratings_set is normal_parents_not_deleted :
							buf.write("<!-- PARENT RATINGS WITH CHILD RATINGS DELETED -->\n")
						elif ratings_set is virtual_parents_not_deleted :
							buf.write("<!-- VIRTUAL RATINGS WITH SOURCE RATINGS DELETED -->\n")
						elif ratings_set is transitional_parents_not_deleted :
							buf.write("<!-- TRANSITIONAL RATINGS WITH SOURCE RATINGS DELETED -->\n")
					buf.write("%s\n" % "\n".join(lines[2:-1]))
			print("%5d" % count)
			buf.write('</ratings>')
			filename = os.path.join(
				os.path.abspath(outdir),
				"delete_ratings.%s.xml" % str(datetime.now())[:19].replace(" ", "_").replace(":", "."))
			sys.stdout.write("\nWriting to backup file %s ... " % filename)
			with open(filename, "w") as f : f.write(buf.getvalue())
			buf.close()
			print("%s bytes written" % "{:,}".format(os.stat(filename)[stat.ST_SIZE]))
		if execute :
			#------------------#
			# delete the items #
			#------------------#
			sys.stdout.write("\nDeleting items\n\tTemplates:%5d\n\tSpecs    :%5d\n\tRatings  :" % (0, 0))
			sys.stdout.flush()
			count = 0
			deleted_with_parent_count = 0
			rating_count = 0
			stmt = db.getConnection().prepareCall(delete_single_rating_sql);
			stmt.setString(4, office)
			for rating_id_date in rating_ids_dates :
				if rating_id_date in children_deleted_with_parent :
					continue
				rating_id, rating_datetime = rating_id_date.split(" ", 1)
				count += 1
				sys.stdout.write("%5d%s" % (count, 5*"\b"))
				sys.stdout.flush()
				stmt.setString(1, rating_id)
				stmt.setString(2, rating_datetime[:19])
				stmt.setString(3, "%s:%s" % (rating_datetime[19:22], date[22:]))
				stmt.execute()
			stmt.close()
			print("%5d" % count)
			sys.stdout.flush()
except SystemExit :
	pass
except :
	traceback.print_exc()
finally :
	sys.stderr.write("\nDisconnecting from database\n")
	if db : db.close()
	exit()

