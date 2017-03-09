# db that stores everything
class Database:
  def __init__(self):
    self.tables = []
  
  def exec_sql(self, sql):
    pass
  
  # Create up to 10 individual tables
  def create_table(self, table_name,columns,constraints, keys=None):
    pass
  
  def get_all_table_names(self):
    pass
  
  def get_table(self, name):
    pass
  
    
# each table
class Table:
  def __init__(self, name, columns, pks=[0]):
    self.name = name
    # store column if column is_valid 
    self.columns = columns
    # data structure for entity may need to change 
    self.entities = []
    # the index of primary key
    # we may have several pks
    self.pks = pks
    
  
  def insert(self,entity):
    # Check for column constraint   
    # Check for duplicates (either KeyConstraint or DuplicateConstraint)
    pass
  
  def delete(self,entity):
    pass
  
  # Check if column values are valid
  def is_vaild(self, entity):
    return False
  
  # Getting data for specific key
  def fetch(self, column_name):
    pass

class Column:
  def __init__(self, name, constraint):
    self.name = name
    self.constraint = constraint
    
# may not be needed?
'''class Constraint:
  def fulfil(value):
    return False
'''

class TableConstraint:
  # Check that the DB doesn't contain more then 10 tables already
  def fulfil(database):
    
    return false
  
class IntConstraint:
  # Check that value is an int within -2,147,483,648 to 2,147,483,647
  def fulfil(value):
    if isInstance(value, int) and value >= -2147483648 and value <= 2147483647:
        return True
    return False
  
class VarcharConstraint:
  # Check that value is a string within char limit 40
  def fulfil(value):
    if isInstance(value, basestring) and value.length <= 40:
      return True
    return False

class KeyConstraint:
  # Check that there are no keys with the same value
  # Check for null value
  def fulfil(value):
      return False
    
class DuplicateConstraint:
  # Check that there are no duplicate values (maybe stringify all values in a row, and compare the two strings?)
  def fulfil(value):
    return False
  
# each row
class Entity:
  def __init__(self):
    # in array 
    self.values = []


"""
Temporary functions that insert fake data into views
Data visualization -- retrieve data only
"""
def get_all_table_names(database):
    table_names = ['Round Table','Square table','Triangle Table']
    return table_names

def get_table(table_name, database):
    title = ['title1','title2']
    content = [['row1 content1','row1 content2'],['row2 content1','row2 content2']]
    return title, content