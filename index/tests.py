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
    def setUp(self):
        database = miniDB.Database()
        save_db(database)

    def test_table_creation(self):
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

        