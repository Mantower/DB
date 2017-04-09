# simpleSQL.py
#
# simple  using the parsing library to do simple-minded SQL parsing
# could be extended to include where clauses etc.
#
#

from miniDB import *
import shlex
import sys
import re
import unicodedata
from ppUpdate import *
def input_file(DB,file):
	with open(file, 'r') as content_file:
		content = content_file.read()
	#print("file:"+content)
	return DB,content
def input_text(DB,sqlText):
	#Eliminate all newline
	#Text = unicodedata.normalize('NFKD', title).encode('ascii','ignore')
	Uans = re.sub(r"\r\n"," ",sqlText)
	#Generate the SQL command respectively
	pattern = re.compile("insert", re.IGNORECASE)
	st = pattern.sub("\ninsert", Uans)
	pattern1 = re.compile("create", re.IGNORECASE)
	st = pattern1.sub("\ncreate", st)
	pattern2 = re.compile("select", re.IGNORECASE)
	st = pattern2.sub("\nselect", st)
	#Make them into list

	sqlList = [s.strip() for s in st.splitlines()]
	print("sqlList:"+str(sqlList))
	#Call the specific function
	success = []
	errMsg = []
	tables = []
	for obj in sqlList:		
		if str(obj) == "":
			continue
		act = obj.split(' ', 1)[0]
		print(obj)
		sucTemp = "" 
		errTemp = ""
		table = None
		if act.lower()=="create":			
			sucTemp ,errTemp = def_create(DB,obj)
		elif act.lower()=="insert":
			sucTemp ,errTemp = def_insert(DB,obj)
		elif act.lower()=="select":
			sucTemp , table, errTemp = def_select(DB,obj)
		success.append(sucTemp)
		errMsg.append(errTemp)
		tables.append(table)
	return success, table, errMsg


def def_create(DB,text):
	createStmt = Forward()
	CREATE = Keyword("create", caseless = True)
	TABLE = Keyword("table",caseless = True)
	PRIMARY = Keyword("PRIMARY KEY", caseless = True)
	INT = Keyword("int", caseless = True)
	VARCHAR = Keyword("varchar", caseless = True)
	#here ident is for table name
	ident	= Word( alphas, alphanums + "_$").setName("identifier")

	#for brackets
	createStmt = Forward()
	

	
	#createExpression << Combine(CREATE + TABLE + ident) + ZeroOrMore()
	varW = Word(alphas,alphanums+"_$") +  Word(alphas,alphanums+"_$") +Combine("("+Word(nums)+")") + Optional(PRIMARY)
	varI =  Word(alphas,alphanums+"_$") + Word(alphas,alphanums+"_$")  +  Optional(PRIMARY)
	tableRval = Group(varW | varI)
	
	#tableCondition = 
	'''
	varW = Combine(VARCHAR + "("+Word(nums)+")")
	tableValueCondition = Group(
		( Word(alphas,alphanums+"_$") + varW + Optional(PRIMARY)) |
		( Word(alphas,alphanums+"_$") + INT + Optional(PRIMARY) )
		)
	'''
	#tableValueExpression = Forward()
	#tableValueExpression << tableValueCondition + ZeroOrMore(tableValueExpression) 
	
	#define the grammar
	createStmt  << ( Group(CREATE + TABLE ) + 
					ident.setResultsName("tables") + 
					 "(" + delimitedList(tableRval).setResultsName("values") + ")" )
	'''
	createStmt  << ( Group(CREATE + TABLE ) + 
					ident.setResultsName("tables") + 
					 "(" + delimitedList(tableValueCondition).setResultsName("values") + ")" )
	'''
	# define Oracle comment format, and ignore them
	simpleSQL = createStmt
	oracleSqlComment = "--" + restOfLine
	simpleSQL.ignore( oracleSqlComment )
	success ,tokens = simpleSQL.runTests(text)
	if(success):
		doubleCheck, flag = process_input_create(DB,tokens)
		return doubleCheck, flag
	else:
		return success, tokens

def def_insert(DB,text):
	print("insert!")
	insertStmt = Forward()
	INSERT = Keyword("insert", caseless = True)
	INTO = Keyword("into",caseless = True)
	VALUES = Keyword("values", caseless = True)
	
	string_literal = quotedString("'")
	columnRval = Word(alphas,alphanums+"_$") | quotedString | Word(nums)
	#columnRval =  Word(nums) | quotedString
	#here ident is for table name
	ident	= Word(alphas, alphanums + "_$").setName("identifier")
	'''valueCondition = Group(
		 "(" + delimitedList( columnRval ) + ")" 
		)'''
	valueCondition = delimitedList( columnRval )
		
	#for brackets
	insertStmt = Forward()
	

	#define the grammar
	"""
	insertStmt  << ( Group(INSERT + INTO)  + 
					ident.setResultsName("tables")+
					Optional(valueCondition.setResultsName("col")) +VALUES +
					 valueCondition.setResultsName("val")
					)

	"""

	insertStmt  << ( Group(INSERT + INTO)  + 
					ident.setResultsName("tables")+
					Optional( "(" + (delimitedList(valueCondition).setResultsName("col")| (CharsNotIn(")")- ~Word(printables).setName("<unknown>") )) + ")") +
					VALUES +
					"(" + (delimitedList(valueCondition).setResultsName("val") | (CharsNotIn(")")- ~Word(printables).setName("<unknown>") )) + ")"
					)

	# define Oracle comment format, and ignore them
	simpleSQL = insertStmt
	oracleSqlComment = "--" + restOfLine
	simpleSQL.ignore( oracleSqlComment )
	success, tokens = simpleSQL.runTests(text)

	if(success):
		return process_input_insert(DB,tokens)
	else:
		return success, tokens
def def_select(DB, text):
	print("select function")
	LPAR,RPAR,COMMA = map(Suppress,"(),")
	select_stmt = Forward().setName("select statement")

	# keywords
	(UNION, ALL, AND, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, 
	CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
	HAVING, ORDER, BY, LIMIT, OFFSET) =  map(CaselessKeyword, """UNION, ALL, AND, INTERSECT, 
	EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, 
	DISTINCT, FROM, WHERE, GROUP, BY, HAVING, ORDER, BY, LIMIT, OFFSET""".replace(",","").split())
	(CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
	COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE, 
	CURRENT_TIMESTAMP) = map(CaselessKeyword, """CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, 
	END, CASE, WHEN, THEN, EXISTS, COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, 
	CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP""".replace(",","").split())
	keyword = MatchFirst((UNION, ALL, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, 
	CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
	HAVING, ORDER, BY, LIMIT, OFFSET, CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
	COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE, 
	CURRENT_TIMESTAMP))
	
	identifier = ~keyword + Word(alphas, alphanums+"_")
	collation_name = identifier.copy()
	column_name = identifier.copy()
	column_alias = identifier.copy()
	table_name = identifier.copy()
	table_alias = identifier.copy()
	index_name = identifier.copy()
	function_name = identifier.copy()
	parameter_name = identifier.copy()
	database_name = identifier.copy()

	# expression
	expr = Forward().setName("expression")

	integer = Regex(r"[+-]?\d+")
	numeric_literal = Regex(r"\d+(\.\d*)?([eE][+-]?\d+)?")
	string_literal = QuotedString("'")
	blob_literal = Combine(oneOf("x X") + "'" + Word(hexnums) + "'")
	literal_value = ( numeric_literal | string_literal | blob_literal |
		NULL | CURRENT_TIME | CURRENT_DATE | CURRENT_TIMESTAMP )
	bind_parameter = (
		Word("?",nums) |
		Combine(oneOf(": @ $") + parameter_name)
		)
	type_name = oneOf("TEXT REAL INTEGER BLOB NULL")

	expr_term = (
		CAST + LPAR + expr + AS + type_name + RPAR |
		EXISTS + LPAR + select_stmt + RPAR |
		function_name + LPAR + Optional(delimitedList(expr)) + RPAR |
		literal_value |
		bind_parameter |
		identifier
		)

	UNARY,BINARY,TERNARY=1,2,3
	expr << operatorPrecedence(expr_term,
		[
		(oneOf('- + ~') | NOT, UNARY, opAssoc.LEFT),
		('||', BINARY, opAssoc.LEFT),
		(oneOf('* / %'), BINARY, opAssoc.LEFT),
		(oneOf('+ -'), BINARY, opAssoc.LEFT),
		(oneOf('<< >> & |'), BINARY, opAssoc.LEFT),
		(oneOf('< <= > >='), BINARY, opAssoc.LEFT),
		(oneOf('= == != <>') | IS | IN | LIKE | GLOB | MATCH | REGEXP, BINARY, opAssoc.LEFT),
		('||', BINARY, opAssoc.LEFT),
		((BETWEEN,AND), TERNARY, opAssoc.LEFT),
		])

	compound_operator = (UNION + Optional(ALL) | INTERSECT | EXCEPT)

	ordering_term = expr + Optional(COLLATE + collation_name) + Optional(ASC | DESC)

	join_constraint = Optional(ON + expr | USING + LPAR + Group(delimitedList(column_name)) + RPAR)

	join_op = COMMA | (Optional(NATURAL) + Optional(INNER | CROSS | LEFT + OUTER | LEFT | OUTER) + JOIN)

	join_source = Forward()
	select_table =  Group(Group(database_name("database") + "." + table_name("table"))+ Optional(Optional(AS) + table_alias("table_alias")))  | Group(table_name("table")  + Optional(Optional(AS) + table_alias("table_alias")))   
	#single_source = ( Group(delimitedList( Group(Group(database_name("database") + "." + table_name("table")) | table_name("table")).setResultsName("tables")     +   Optional(Group(AS + table_alias("table_alias"))).setResultsName("various"))))
	'''+ Optional(INDEXED + BY + index_name("name") | NOT + INDEXED)("index") | 
	(LPAR + select_stmt + RPAR + Optional(Optional(AS) + table_alias)) | 
	(LPAR + join_source + RPAR) )
	'''
	#here ident is for table name
	ident   = Word( alphas, alphanums + "_$")

	result_column =  Group(table_name + "."+ ident) | "*" | Group(table_name + "." + "*") | (expr + Optional(Optional(AS) + column_alias)) 

	select_core = (SELECT + Optional(DISTINCT | ALL) + Group(delimitedList(result_column))("columns") +
					Optional(FROM + Group(delimitedList(select_table))("tables")) +
					Optional(WHERE + expr("where_expr")) +
					Optional(GROUP + BY + Group(delimitedList(ordering_term)("group_by_terms")) + 
							Optional(HAVING + expr("having_expr"))))

	select_stmt << (select_core + ZeroOrMore(compound_operator + select_core) +
					Optional(ORDER + BY + Group(delimitedList(ordering_term))("order_by_terms")) +
					Optional(LIMIT + (integer + OFFSET + integer | integer + COMMA + integer)))
	# define Oracle comment format, and ignore them
	simpleSQL = select_stmt
	oracleSqlComment = "--" + restOfLine
	simpleSQL.ignore( oracleSqlComment )
	
	success, tokens = simpleSQL.runTests(text)
	
	if(success):
		return process_input_select(DB,tokens)
	else:
		return success, tokens, None
def process_input_select(DB, tokens):
	col_names = []
	tables = []
	table_alias = []
	table_names=[]
	where_expr = []
	predicates = []
	columns = []
	print(tokens)
	for i in range(len(tokens)):
		tables = tokens[i]["tables"]

		col_names = tokens[i]["columns"]
		#Not deal with table name, and "." and SUM and COUNT
		for k in range(len(col_names)):
			if(len(col_names[k])==3):
				columns.append([col_names[k][0], col_names[k][2], None])
			else:
				columns.append([None, col_names[k], None])
		
		for k in range(len(tables)):
			table = tables["table"]
			try:
				table_alias = tables["table_alias"]
				table_names.append([table_alias, table])
			except:
				table_names.append([None, table])

		"""try:
			table_alias = tokens[i]["various"]
			for k in range(len(table_alias[1])):
				#print("alias")
				#print(table_alias[1][k])
				table_names.append([table_alias[1][k], tables[k]])
		except:
			#print("No Alias")
			for k in range(len(tables)):
				table_names.append([None, tables[k]])"""
		try:
			where_expr = tokens[i]["where_expr"]
			#not consider the . condition
			predicates.append([None, where_expr[0],None], where_expr[1], [None, where_expr[2], None ])
			
		except:
			print("No where exception")
		print("tables:"+str(tables))
		print("col_names:"+str(columns))
		print("table_names:"+str(table_names))
		print("predicates:"+str(predicates))
		return DB.select(columns, table_names, predicates, operator=None)
		


		
def process_input_create(DB,tokens):
	keys = []
	col_names = []
	col_datatypes = []
	col_constraints = []
	
	for i in range(len(tokens)):
		try:
			tables = tokens[i]["tables"]
			values = tokens[i]["values"]
		except:
			print("TABLE INCORRECT SQL")
			return False, "FAT: Illegal value type or table name"
		print("table:"+tables)
		print("values:"+str(len(values))+" "+str(values))
		for k in values:
			length = len(k)
			col = k[0]
			typeOri = k[1]
			key = False
			con = None
			if typeOri.lower() == "varchar":
				try:
					con = k[2][k[2].find("(")+1:typeOri.find(")")]	
					con = int(con)
				except:
					return False, "FATL: the correct type of varchar :'varchar(int)'"
				if length == 4:
					key = True
			
				#with primary key, the primary key string should have been checked during parsing
			if typeOri.lower() =="int" and length == 3:
				key = True
			elif length > 4 or length < 2 :
				print("values error")
			
			col_names.append(col)
			col_datatypes.append(typeOri.lower())
			col_constraints.append(con)
			keys.append(key)
		"""
		print("tables:"+tables)
		print("col_names:"+str(col_names))
		print("col_datatypes:"+str(col_datatypes))
		print("col_constraints:"+str(col_constraints))
		print("keys:"+str(keys))
		"""
		return DB.create_table(tables, col_names, col_datatypes, col_constraints, keys)
		
def process_input_insert(DB,tokens):
	for i in range(len(tokens)):
		print(str(tokens[i]))
		tables = tokens[i]["tables"]
		#cols = tokens[i]["col"]
		values = tokens[i]["val"]
		print("lenght:"+str(len(values)))
		for k in range(len(values)):
			try:
				print("turn the value")
				values[k] = int(values[k])
				print(int(k))
			except:				
				values[k] = values[k].replace("'","").replace('"', '')
				print("type:string:"+values[k])
		try:
			cols = tokens[i]["col"]		
			print("cols:"+str(cols))	
		except:
			cols = None
			print("no col asssigned")

		print("values:"+str(len(values))+"\t "+str(values))
		print("table:"+tables)
		print("value:"+str(values))
		print("cols:"+str(cols))
		tableObj = DB.get_table(tables)
		if tableObj:
			return tableObj.insert(values, cols)
		else:
			return False, "Table not exists."	
		
def stage1Test():
	txt = input_file("string.txt")
	print("txt:"+txt)
	input_text(txt)
def testVochar():
	#m = re.search(r"\((0-9+)\)", s)
	while(1):
		st = input()
		num = st[st.find("(")+1:st.find(")")]
		ans = st.split("(",1)[0]
		print (ans)

def test3():
	ans = input_file("string.txt")
	Uans = ans.replace("\n"," ")

	pattern = re.compile("insert", re.IGNORECASE)
	st = pattern.sub("\ninsert", Uans)
	
	tokens = def_insert(st)
	#print(tokens)

def test1():
	ans = input_file("string.txt")
	Uans = ans.replace("\n"," ")
	#s = """\CREATE TABLE Person (person_id int PRIMARY KEY,name varchar(20),gender varchar(1));"""
	
	tokens = def_create(Uans)
	tables = tokens["tables"]
	values = tokens["values"]
	print("table:"+tables)
	print("values:"+str(len(values))+" "+str(values))
	"""
	for ind in tokens:
		print(str(ind))
		print("\n\n\n\n")
	"""
	print(type(tokens[0]))

def test2():
	ans = input_file("string.txt")
	Uans = ans.replace("\n"," ")

	pattern = re.compile("create", re.IGNORECASE)
	st = pattern.sub("\ncreate", Uans)
	
	tokens = def_create(st)
	
	
