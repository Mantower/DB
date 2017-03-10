# db that stores everything
class Database:
    def __init__(self):
        self.tables = []
  
    def exec_sql(self, sql):
        pass
  
    # Create up to 10 individual tables
    def create_table(self, table_name, columns, constraints, keys=None):
        if can_create():
            table = Table(table_name,columns,keys)
            tables.append(table)
    
    def can_create(self):
        # Check that the DB doesn't contain more then 10 tables already
        return len(self.tables) < 10
    
    def get_all_table_names(self):
        return [t.name for t in self.tables]
  
    def get_table(self, name):
        for t in self.tables:
            if t.name == name:
                return t
        return None
  
    
# each table
class Table:
    def __init__(self, name, columns, contraints, pks=[0]):
        self.name = name
        # store column if column is_valid 
        self.columns = columns
        # data structure for entity may need to change 
        self.entities = []
        # the index of primary key
        # we may have several pks
        self.pks = pks
    
    def insert(self, entity):
        # validate entity
        if not entity_is_valid(entity):
            return False
        
        # insert entity
        pass
  
    def delete(self, entity):
        pass
  
    def entity_is_vaild(self, entity):
        # Check if column values are valid
        # Check that there are no keys with the same value
        # Check for null value
        
        
        # Check if all values are valid
        for v, c in zip(entity.values, self.columns):
            if not c.is_valid(v):
                return False 
            
        # Pass all validation 
        return True
  
    # Getting data for specific key
    def fetch(self, column_name):
        pass

class Column:
    def __init__(self, name, constraint):
        self.name = name
        # defines which type of data the column should accept
        self.constraint = constraint
    
# each row
class Entity:
    def __init__(self):
        # in array 
        self.values = []

class IntConstraint:
    def __init__(self):
        pass
    
    @staticmethod
    def is_valid(value):
        # Check that value is an int within -2,147,483,648 to 2,147,483,647
        return isInstance(value, int) and value >= -2147483648 and value <= 2147483647

class VarcharConstraint:
    def __init__(self):
        pass
    
    @staticmethod
    def is_valid(value):
        # Check that value is a string within char limit 40
        return isInstance(value, basestring) and len(value) <= 40

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