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




