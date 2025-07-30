import unittest
import tempfile
import os
from PyStoreJSONLib.PyStoreManager import PyStoreManager
from PyStoreJSONLib.PyStoreJSON import PyStoreJSONDB

class TestPyStoreManagerAndDB(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.manager = PyStoreManager(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_create_and_get_database(self):
        db = self.manager.create_database("testdb")
        self.assertIsInstance(db, PyStoreJSONDB)
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir.name, "testdb.json")))

        db2 = self.manager.get_database("testdb")
        self.assertEqual(db, db2)

    def test_list_databases(self):
        self.manager.create_database("db1")
        self.manager.create_database("db2")
        self.manager.create_database("db3")
        listed = self.manager.list_databases()
        self.assertCountEqual(listed, ["db1", "db2", "db3"])

    def test_delete_database(self):
        self.manager.create_database("delete_me")
        result = self.manager.delete_database("delete_me")
        self.assertTrue(result)
        self.assertNotIn("delete_me", self.manager.databases)
        self.assertFalse(os.path.exists(os.path.join(self.temp_dir.name, "delete_me.json")))

        result2 = self.manager.delete_database("does_not_exist")
        self.assertFalse(result2)

    def test_insert_and_get_all(self):
        db = self.manager.create_database("people")
        db.insert({"name": "Alice", "age": 30})
        db.insert({"name": "Bob", "age": 25})
        all_rows = db.get_all()
        self.assertEqual(len(all_rows), 2)
        self.assertEqual(all_rows[0]["name"], "Alice")

    def test_find_by(self):
        db = self.manager.create_database("find_test")
        db.insert({"id": 1, "value": "x"})
        db.insert({"id": 2, "value": "y"})
        db.insert({"id": 3, "value": "x"})
        results = db.find_by("value", "x")
        self.assertEqual(len(results), 2)
        self.assertTrue(all(r["value"] == "x" for r in results))

    def test_update_by(self):
        db = self.manager.create_database("update_test")
        db.insert({"id": 1, "score": 50})
        db.insert({"id": 2, "score": 50})
        updated = db.update_by("score", 50, {"score": 100})
        self.assertEqual(updated, 2)
        all_rows = db.get_all()
        self.assertTrue(all(r["score"] == 100 for r in all_rows))

    def test_delete_by(self):
        db = self.manager.create_database("delete_test")
        db.insert({"id": 1})
        db.insert({"id": 2})
        db.insert({"id": 1})
        deleted = db.delete_by("id", 1)
        self.assertEqual(deleted, 2)
        remaining = db.get_all()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["id"], 2)

def manual_test_database(manager: PyStoreManager):
    db = manager.delete_database("print_test")
    db = manager.create_database("print_test")
    db.insert({"name": "Charlie", "age": 40})
    db.insert({"name": "Diana", "age": 35})

    db.insert({"name": "Eve", "age": 28, "city": "New York"})
    db.insert({"name": "Frank", "age": 22, "city": "Los Angeles"})
    db.insert({"name": "George", "age": 45, "city": "New York"})

    print(manager.get_database("print_test").find_by("city", "New York"))

if __name__ == "__main__":
    print("--------- Manual Test Begin ---------\n")
    manual_test_database(PyStoreManager("./test_databases"))
    print("\n--------- Manual Test End -----------\n")

    unittest.main()

