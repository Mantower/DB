from django.test import TestCase
import pickle
import miniDB

#https://docs.djangoproject.com/en/1.10/topics/testing/overview/

TEST_DB_NAME = "TESTING.pkl"
TEST_DB_WITH_STUDENT = "TESTING_STUDENT.pkl"

def save_db(database, db_name):
    # dump new db into a file
    output = open(db_name, 'wb')
    pickle.dump(database, output)

def load_db(db_name):
    # read DB from pkl file
    with open(db_name, 'rb') as f:
        return pickle.load(f)

# Create your tests here.
class TableTestCase(TestCase):
    # We can always start from clean Database if we don't save our Database
    # valie Table name character, A-Z, 0-9 and _
    def setUp(self):
        database = miniDB.Database()
        save_db(database, TEST_DB_NAME)

        database_with_student_table = miniDB.Database()
        sql = "CREATE TABLE STUDENT (\
            studentId int PRIMARY KEY,\
            name VARCHAR(15),\
            gender VARCHAR(1),\
            age int\
            )"
        passed, err_msg = database_with_student_table.exec_sql(sql)
        save_db(database_with_student_table, TEST_DB_WITH_STUDENT)

# TABLE TESTS

    def test_table_creation(self):
        # test table creation 
        database = load_db(TEST_DB_NAME)
        sql = "CREATE TABLE STUDENT (\
            studentId int PRIMARY KEY,\
            name VARCHAR(15),\
            gender VARCHAR(1),\
            age int\
            )"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed, [True])
        self.assertEqual(err_msg, [None])

    def test_table_creation_duplicate(self):
        # test table creation with already exists name
        database = load_db(TEST_DB_NAME)
        sql = "CREATE TABLE STUDENT (\
            studentId int PRIMARY KEY,\
            name VARCHAR(15),\
            gender VARCHAR(1),\
            age int\
            );"
        passed, err_msg = database.exec_sql(sql+sql)
        self.assertEqual(passed, [True, False])
        self.assertEqual(err_msg, [None, "Table with same name exists."])

    def test_table_creation2(self):
        database = load_db(TEST_DB_NAME)
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
        database = load_db(TEST_DB_NAME)
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
        database = load_db(TEST_DB_NAME)
        sql = "CREATE TABLE COURSE (\
            name VARCHAR(40) PRIMARY KEY,\
            id INT PRIMARY KEY\
            )"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed, [True])
        self.assertEqual(err_msg, [None]) 

    def test_table_creation_with_missing_paranthesis(self):
        database = load_db(TEST_DB_NAME)
        sql = "CREATE TABLE TEACHERS\
        FName VARCHAR,\
        LName VARCHAR(20)\
        )"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed, [False])
        #self.assertEqual(err_msg, [None]) 

    def test_table_creation_with_unknown_type(self):
        database = load_db(TEST_DB_NAME)
        sql = "CREATE TABLE TEACHERS (\
        FName VARCHAR(1),\
        LName DATE\
        )"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed, [False])
        #self.assertEqual(err_msg, [None]) 

    def test_table_creation_with_extra_comma(self):
        database = load_db(TEST_DB_NAME)
        sql = "CREATE TABLE TEACHERS (\
            FName VARCHAR(4),\
            LName VARCHAR(4),\
            )"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed, [False])
        #self.assertEqual(err_msg, [None])

    def test_table_creation_with_duplicate_column_name(self):
        database = load_db(TEST_DB_NAME)
        sql = "CREATE TABLE TEACHERS (\
            FName VARCHAR(4),\
            LName VARCHAR(4),\
            Fname VARCHAR(4)\
            )"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed, [False])
        self.assertEqual(err_msg, ["Columns contain duplicate name."])

    def test_table_creation_with_bounds_varchar_limit(self):
        database = load_db(TEST_DB_NAME)
        sql = "CREATE TABLE TEACHERS (\
            FName VARCHAR(4),\
            LName VARCHAR(50)\
            )"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed, [False])
        #self.assertEqual(err_msg, [None])

# INSERTION TESTS

    def test_insert_singles(self):
        database = load_db(TEST_DB_WITH_STUDENT)
        sql = "INSERT INTO STUDENT \
        VALUES(10, 'John Smith', 'M', 22)"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])

        database = load_db(TEST_DB_WITH_STUDENT)
        sql = "INSERT INTO STUDENT \
        VALUES(11, 'Hsu You-Ting', 'F', 23)"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])

    def test_insert_multiple(self):
        database = load_db(TEST_DB_WITH_STUDENT)
        sql = "INSERT INTO STUDENT \
        VALUES(12, 'John Cena', 'M', 45),\
        VALUES(14, 'Chuck Norris', 'M', 55)"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])

    def test_insert_duplicate_key(self):
        database = load_db(TEST_DB_WITH_STUDENT)
        sql = "INSERT INTO STUDENT \
        VALUES(10 'Huang Hao-Wei', 'M', 26)"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])   

    def test_insert_data_mismatch(self):
        database = load_db(TEST_DB_WITH_STUDENT)
        sql = "INSERT INTO STUDENT \
        VALUES(15, 'Mr. Bean', 'M', '45')"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])
    
    def test_insert_string_length(self):
        database = load_db(TEST_DB_WITH_STUDENT)
        sql = "INSERT INTO STUDENT \
        VALUES(16, 'Caitlyn Jenner', 'MF', 45)"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])

    def test_insert_int_size(self):
        database = load_db(TEST_DB_WITH_STUDENT)
        sql = "INSERT INTO STUDENT \
        VALUES(17, 'Infinity Man', 'M', 2147483650)"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])

    def test_insert_int_size(self):
        database = load_db(TEST_DB_WITH_STUDENT)
        sql = "INSERT INTO STUDENT \
        VALUES(18, 'Max Int Man', 'M', 2147483647)"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])

    def test_insert_null_key(self):
        database = load_db(TEST_DB_WITH_STUDENT)
        sql = "INSERT INTO STUDENT \
        VALUES(, 'Null Woman', 'W', 100)"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])

    def test_insert_missing_paranthesis(self):
        database = load_db(TEST_DB_WITH_STUDENT)
        sql = "INSERT INTO STUDENT \
        VALUES(19, 'Dr. Paranthesis', 'M', 67"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])

    def test_insert_missing_attribute(self):
        database = load_db(TEST_DB_WITH_STUDENT)
        sql = "INSERT INTO STUDENT \
        VALUES(20, 'Prof. No Age', 'M')"
        passed, err_msg = database.exec_sql(sql)
        self.assertEqual(passed,[True])
        self.assertEqual(err_msg, [None])



