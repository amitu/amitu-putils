"""
amitu.mysql, utilities for doing mysql stuff with ipython notebook

TODO:
	- 
"""
import MySQLdb
from IPython.display import HTML
from jinja2 import Template
from datetime import date, timedelta
import logging

dbase = None
cursor = None
logger = logging.getLogger('amitu.mysql')
table_name = None
ccache = False
CACHE = {}

def init(
	host="127.0.0.1", user="root", passwd="", db="", table_name=None,
	cache=False
):
	global dbase, cursor, tname, ccache
	dbase = MySQLdb.connect(host=host,user=user, passwd=passwd, db=db)
	cursor = dbase.cursor()
	if table_name:
		tname = table_name
	if cache:
		ccache = cache
	logger.info("database initialized")

def qry(query, *args, **kw):
	"""
	Usage:

	>>> mysql.q("select x from some_table limit %s", 2) # safe quoting
	"""
	print "executing", query

	if kw.get("dry_run"):
		return

	result = None

	if ccache or kw.get("cache"):
		if (query, args) in CACHE:
			result = CACHE[(query, args)]

	if not result:

		if args:
			cursor.execute(query, args)
		else:
			cursor.execute(query)

		result = cursor.fetchall()

		CACHE[(query, args)] = result

	print "found rows =", len(result)

	if kw.get("headers"):
		return [d[0] for d in cursor.description], result

	if kw.get("count"):
		result = result[0][0]

	if kw.get("singles") or kw.get("single"):
		result = [x[0] for x in result]

	return result

def qry2(pre, *posts, **kw):
	if not isinstance(pre, basestring): # assume list or tuple
		pre = "select %s" % " ".join(pre)
	if "select" not in pre:
		pre = "select %s" % pre
	return qry("%s from %s %s" % (pre, tname, " ".join(posts)), **kw)

def q(query, *args):
	"q(query), query and return HTML"
	return table(*qry(query, *args, headers=True))

def q2(pre, *posts, **kw):
	"q2(pre, *posts, count=False, singles=False), qry2 and show HTML table."
	return table(qry2(pre, *posts, **kw))

def table(headers, rows):
	return HTML(TEMPLATE.render(headers=headers, rows=rows))

def desc(table=None):
	if not table:
		table = tname
	return q("describe %s" % table)

TEMPLATE = Template(
	"""
		<table>
			{% if headers %}
				<tr>
					{% for header in headers %}
						<th>{{ header }}</th>
					{% endfor %}
				</tr>
			{% endif %}
			{% for row in rows %}
				<tr style="background: {% if loop.index is not divisibleby 2 %}#f5f5f5{% endif %}">
					{% for col in row %}
						<td>{{ col }}</td>
					{% endfor %}
				</tr>
			{% endfor %}
		</table>
	"""
)

def mqry(query, data, count=False, tablize=False):
	results = []
	for item in data:
		results.append((item, qry(query, item, count=count)))
	if tablize:
		return table(tablize, results)
	return results

def close():
	dbase.close()

def ndays(n, fmt="%Y-%m-%d"):
	today = date.today()
	days = [today - timedelta(days=i) for i in range(n)]
	if fmt:
		days = [day.strftime(fmt) for day in days]
	return days

def ago(days=-1, hrs=-1, seconds=-1):
	pass

def describe_cache():
	return table(["key", "number of rows"])

