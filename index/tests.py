from django.test import TestCase
import miniDB
import views

# Create your tests here.
class TableTestCase(TestCase):
    def setUp(self):
        database = miniDB.Database()
        views.save_db(database)

    def test_table_creation(self):
        database = views.load_db()
        sql1 = "CREATE TABLE STUDENT (\
        studentId int PRIMARY KEY,\
        name VARCHAR(15),\
        gender VARCHAR(1),\
        age int\
        )"
        passed, err_msg = database.exec_sql(sql1)
        self.assertEqual(passed, [True])
        self.assertEqual(err_msg, [None])

        