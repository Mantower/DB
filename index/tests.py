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


    
        