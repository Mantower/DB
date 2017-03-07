"""
Data visualization -- retrieve data only
"""
def get_all_table_names(database):
    table_names = ['Round Table','Square table','Triangle Table']
    return table_names

def get_table(table_name, database):
    title = ['title1','title2']
    content = [['row1 content1','row1 content2'],['row2 content1','row2 content2']]
    return title, content

""" 
Data modification
"""

def create_table(table_name, database):
    return False
