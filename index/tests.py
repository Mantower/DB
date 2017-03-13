from django.test import TestCase
import pickle
import miniDB

#https://docs.djangoproject.com/en/1.10/topics/testing/overview/

TEST_DB_NAME = "TESTING.pkl"

def save_db(database):
    # dump new db into a file
    output = open(TEST_DB_NAME, 'wb')
    pickle.dump(database, output)

def load_db():
    # read DB from pkl file
    with open(TEST_DB_NAME, 'rb') as f:
        return pickle.load(f)

# Create your tests here.
class TableTestCase(TestCase):
    # We can always start from clean Database if we don't save our Database
    def setUp(self):
        database = miniDB.Database()
        save_db(database)

    def test_table_creation(self):
        # test table creation 
        database = load_db()
        sql1 = "CREATE TABLE STUDENT (\
            studentId int PRIMARY KEY,\
            name VARCHAR(15),\
            gender VARCHAR(1),\
            age int\
            )"
        passed, err_msg = database.exec_sql(sql1)
        self.assertEqual(passed, [True])
        self.assertEqual(err_msg, [None])

    def test_table_creation_duplicate(self):
        # test table creation with already exists name
        database = load_db()
        sql1 = "CREATE TABLE STUDENT (\
            studentId int PRIMARY KEY,\
            name VARCHAR(15),\
            gender VARCHAR(1),\
            age int\
            );"
        passed, err_msg = database.exec_sql(sql1+sql1)
        self.assertEqual(passed, [True, False])
        self.assertEqual(err_msg, [None, "Table with same name exists."])

    def test_table_creation2(self):
        database = load_db()
        sql = "CREATE TABLE VEHICLE (\
            licenseNumber VARCHAR(10),\
            brand VARCHAR(15),\
            model VARCHAR(15),\
            type VARCHAR(2),\
            engineSize int\
            );"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed, [True])
        self.assertEqual(err_msg, [None])
    
    def test_table_creation3(self):
        database = load_db()
        sql = "CREATE TABLE BOOK (\
        isbn VARCHAR(20) PRIMARY KEY,\
        title VARCHAR(20),\
        author VARCHAR(20),\
        pages int,\
        editorial VARCHAR(15)\
        )"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed, [True])
        self.assertEqual(err_msg, [None])

    def test_table_creation_with_2pks(self):
        database = load_db()
        sql = "CREATE TABLE COURSE (\
            name VARCHAR(40) PRIMARY KEY,\
            id INT PRIMARY KEY\
            )"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed, [True])
        self.assertEqual(err_msg, [None]) 

    def test_insert_singles(self):
        database = load_db("testDB")
        sql = "INSERT INTO STUDENT \
        VALUES(10, 'John Smith', 'M', 22)"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])

        database = load_db("testDB")
        sql = "INSERT INTO STUDENT \
        VALUES(11, 'Hsu You-Ting', 'F', 23)"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])

    def test_insert_multiple(self):
        database = load_db("testDB")
        sql = "INSERT INTO STUDENT \
        VALUES(12, 'John Cena', 'M', 45),\
        VALUES(14, 'Chuck Norris', 'M', 55)"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])

     def test_insert_duplicate_key(self):
        database = load_db("testDB")
        sql = "INSERT INTO STUDENT \
        VALUES(10 'Huang Hao-Wei', 'M', 26)"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])   

    def test_insert_data_mismatch(self):
        database = load_db("testDB")
        sql = "INSERT INTO STUDENT \
        VALUES(15, 'Mr. Bean', 'M', '45')"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])
    
    def test_insert_string_length(self):
        database = load_db("testDB")
        sql = "INSERT INTO STUDENT \
        VALUES(16, 'Caitlyn Jenner', 'MF', 45)"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])

    def test_insert_int_size(self):
        database = load_db("testDB")
        sql = "INSERT INTO STUDENT \
        VALUES(17, 'Infinity Man', 'M', 2147483650)"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])

    def test_insert_int_size(self):
        database = load_db("testDB")
        sql = "INSERT INTO STUDENT \
        VALUES(18, 'Max Int Man', 'M', 2147483647)"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])

    def test_insert_null_key(self):
        database = load_db("testDB")
        sql = "INSERT INTO STUDENT \
        VALUES(, 'Null Woman', 'W', 100)"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])

    def test_insert_missing_paranthesis(self):
        database = load_db("testDB")
        sql = "INSERT INTO STUDENT \
        VALUES(19, 'Dr. Paranthesis', 'M', 67"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])

    def test_insert_missing_attribute(self):
        database = load_db("testDB")
        sql = "INSERT INTO STUDENT \
        VALUES(20, 'Prof. No Age', 'M')"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])



