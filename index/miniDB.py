# db that stores everything
from parseSQL import *
class Database:
    def __init__(self):
        self.tables = []
  
    def exec_sql(self, sql):
        """The fucntion to run sql
        Args:
            sql (String): The SQL we want to run.
        Returns:
            bool: The return value. True for valid, False otherwise.
            String: The error message. None if no error.
        """
        #Call string parsing
        return input_text(self, sql)
      
    def can_create(self, name, col_name, col_datatypes, col_constraints):
        """The fucntion to check whether the type or constraint is valid for table creation
        Args:
            name (String): The table name.
            col_name ([String]): The column names.
            col_datatypes ([String]): The column data types.
            col_constraints ([int]): The constraint value for each column.
        Returns:
            bool: The return value. True for valid, False otherwise.
            String: The error message. None if no error.
        """
        # Check that all three list have the same length
        if not len(col_name) == len(col_datatypes) == len(col_constraints):
            return False, "Internal Error: Length of column name and data type and constraints is not equal."
        
        # All column length are the same
        # Check that length of column is less than 10
        if len(col_name) >= 10:
            return False, "Number of column exceed 10."
          
        # Check that the DB doesn't contain more then 10 tables already
        if len(self.tables) >= 10:
            return False, "Table limit exceed."
        
        # Check that the DB doesn't constain same name table
        if self.get_table(name):
            return False, "Table with same name exists."

        # Check that the col_name are distinct
        if len(col_name) != len(list(set(col_name))):
            #return False, col_name
            return False, "Columns contain duplicate name."

        # Check that the column data types are either varchar or int
        for dtype in col_datatypes:
            if dtype not in Datatype.str2dt:
                return False, "Type " + dtype + " is not defined."
            
        # Check that the column constraints are < 40 for varchar
        for dtype, cons in zip(col_datatypes, col_constraints):
            if Datatype.str2dt[dtype] == Datatype.VARCHAR and cons > 40:
                return False, "Maximum length of varchar exceed 40."
        
        return True, None  
    
    # Create up to 10 individual tables
    def create_table(self, table_name, col_names, col_datatypes, col_constraints, keys=None):
        """The fucntion to create a new table. It will check if the given parameter is valid. 
        Args:
            table_name (String): The table name.
            col_names ([String]): The column names.
            col_datatypes ([String]): The column data types.
            col_constraints ([int]): The constraint value for each column.
            keys ([Bool]): To indicate if the column is primary key.
        Returns:
            bool: The return value. True for successful creation, False otherwise.
            String: The error message. None if no error.
        """
        passed, err_msg = self.can_create(table_name, col_names, col_datatypes, col_constraints)
        if passed:
            columns = []
            for cname, dtype, cons, key in zip(col_names, col_datatypes, col_constraints, keys):
                columns.append(Column(cname, Datatype.str2dt[dtype], cons, key))
            table = Table(table_name, columns)
            self.tables.append(table)
            return True, None
        else:
            return False, err_msg
    
    def get_all_table_names(self):
        return [t.name for t in self.tables]
  
    def get_table(self, name):
        for t in self.tables:
            if t.name == name:
                return t
        return None
  
class Datatype():
    INT = 1
    VARCHAR = 2
    # mapping string to datatype
    str2dt = {'int':INT, 'varchar':VARCHAR}
    
# each table
class Table:
    def __init__(self, name, columns):
        """The init fucntion of Table. It assumes that all columns are valid.
        Args:
            name (String): The table name.
            columns ([Column]): The Column objects .
        
        Todo: May need to create hidden column for non-pk table.
        """
        self.name = name
        self.columns = columns
        # data structure for entity may need to change 
        self.entities = []
        # dictionary for fast look up from col_name to its order in the table
        self.col_name2id = {}
        for i, c in enumerate(columns):
            self.col_name2id[c.name] = i
  
    def entity_is_vaild(self, entity):
        """The fucntion checks if the entity is fine to insert into the table.
        Args:
            entity (Entity): The entity we want to test.
        Returns:
            bool: The return value. True for valid, False otherwise.
            String: The error message. None if no error.
        
        Test:
            Check for empty key value
            Check if column values are valid
            Check that there are no keys with the same value
        
        Todo:
            Move Bool:key from Column to Table to save some time gererating key_id list
            
        """
        # Basic setup.
        # Get all order id that the corresponding column is marked as primary key
        key_id = []
        for i, c in enumerate(self.columns):
            if c.key:
                key_id.append((i,c))
        
        # Get all value that the corresponding column is marked as primary key
        entity_key_values = [v for (v, c) in zip(entity.values, self.columns) if c.key]
        
        
        # Check for empty key value
        for i, v in enumerate(entity_key_values):
            if not v:
                return False, "Empty value for primary key Column " + self.columns[i].name + "."
        
        
        # Check if column values are valid
        for v, c in zip(entity.values, self.columns):
            passed, err_msg = c.constraint.is_valid(v)
            if not passed:
                return False, err_msg
        
        
        # Check that there are no keys with the same value
        # Go through all entities, and check there are no same primary key
        # Can speed up by better Data Structure
        for e in self.entities:
            if not key_id:
                if (e.values[:]==entity.values[:]):
                    return False, "Duplicate data insertion"
            # Extract key column values of every entities, and compare with the one we want to test
            # If there's same combination, we say this entity is invalid
            if (entity_key_values == [e.values[i] for (i, c) in key_id]) and (key_id):
                return False, "Primary key pair (" + ','.join(str(v) for v in entity_key_values) + ") duplicate."
            
        # Pass all validation 
        return True, None

    def insert(self, values, col_names=None):
        """The function inserts a row into the table. It will check if the given parameter is valid. 
        Args:
            values ([int || String]): The value to insert.
            col_names ([String] || None): The column names. If this value is None, we will use default sequence.
        Returns:
            bool: The return value. True for successful insertion, False otherwise.
            String: The error message. None if no error.
        """
        # check if the col_name is in column
        # and convert the whole list to their order in the table
        col_ids = []
        if col_names:
            for n in col_names:
                if n not in self.col_name2id:
                    print(n)
                    return False, "Column " + str(n) + " is not in Table " + self.name
                else:
                    # convert col_name to its order in the table and append to list
                    col_ids.append(self.col_name2id[n])
        
        # create Entitiy
        if col_names:
            entity = Entity(values, col_ids)
        else:
            entity = Entity(values)

        print(values)
        # validate entity
        passed, err_msg = self.entity_is_vaild(entity)
        if not passed:
            return False, err_msg
        
        # insert entity
        self.entities.append(entity)
        return True, None  

    # Getting data for specific key
    def get_column(self, name):
        for c in self.columns:
            if c.name == name:
                return c
        return None

class Column:
    def __init__(self, name, datatype, constraint_val, key):
        """The fucntion to create a column. It assumes that all parameter are valid. 
        Args:
            name (String): The column name.
            datatype (int): The column data types id, transformed by str2dt.
            constraint_val (int): The constraint value for each column.
            key (Bool): To indicate if the column is a primary key.
        """
        self.name = name
        # defines which type of data the column should accept
        self.datatype = datatype
        # Defines boundaries for the data
        # Create Constraint object
        if datatype == Datatype.INT:
            self.constraint = IntConstraint()
        elif datatype == Datatype.VARCHAR:
            self.constraint = VarcharConstraint(constraint_val)
        # Sets bool whether a column contains a key
        self.key = key
    
# each row
class Entity:
    def __init__(self, values, col_id=None):
        """The init fucntion to create an Entity. It assumes that all parameter are valid. 
        The order of the values in the Entity is sorted by col_id. 
        Use default sequence if col_id is None.
        Args:
            values ([String|int]): The value for the corresponding column.
            col_id ([int] | None): The order of the corresponding value. 
        """
        self.values = [None] * 10
        if col_id:
            for cid, v in zip(col_id, values):
                self.values[cid] = v
        else:
            self.values = values

class IntConstraint:
    def __init__(self):
        pass
    
    @staticmethod
    def is_valid(value):
        """The fucntion checks that value is an int within -2,147,483,648 to 2,147,483,647 
        Args:
            values (Any): The value to test.
        Returns:
            bool: The return value. True for valid, False otherwise.
            String: The error message. None if no error.
        """
        if not isinstance(value, int):
            return False, "Value " + str(value) + " is not int."
        if value < -2147483648 or value > 2147483647:
            return False, "Value " + str(value) + " out of range."
        return True, None

class VarcharConstraint:
    def __init__(self, max_len):
        """The init fucntion to create a VarcharConstraint. It assumes that max_len <= 40. 
        Args:
            max_len (int): The maximum length of a varchar.
        """
        self.max_len = max_len
        pass
    
    def is_valid(self, value):
        """The fucntion checks that length of varchar is within maximum length of a varchar.
        Args:
            values (Any): The value to test.
        Returns:
            bool: The return value. True for valid, False otherwise.
            String: The error message. None if no error.
        """
        if not isinstance(value, basestring):
            return False, "Value " + str(value) + " is not varchar."
        if len(value) > self.max_len:
            return False, "Value " + str(value) + " exceed maximum length " + str(self.max_len) + "."
        return True, None
        

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