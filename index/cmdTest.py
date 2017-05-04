import miniDB
from parseSQL import *
import operator as Libop
import copy


def input_file(file):
	with open(file, 'r') as content_file:
		content = content_file.read()
	#print("file:"+content)
	return content	



# read DB from pkl file
DB = None
st = input_file("user.sql")
def_insert(DB,st)
'''
sqlList = [s.strip() for s in st.splitlines()]
 #print(sqlList)

success, tables, err_msgs = [], [], []
for small_sql in sqlList:
    def_insert(DB,small_sql)
    #s, t, err = parseSQL.exec_sql(small_sql)
    #success.extend(s)
    #tables.extend(t)
    #err_msgs.extend(err)'''



