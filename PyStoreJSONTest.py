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

    def test_sort_database(self):
        db = self.manager.create_database("sort_test")
        db.insert({"name": "Charlie", "age": 28})
        db.insert({"name": "Alice", "age": 22})
        db.insert({"name": "Bob", "age": 25})

        # Sort by 'name' ascending
        sorted_by_name = self.manager.sort_database("sort_test", "name")
        self.assertEqual([row["name"] for row in sorted_by_name], ["Alice", "Bob", "Charlie"])

        # Sort by 'age' descending
        sorted_by_age_desc = self.manager.sort_database("sort_test", "age", reverse=True)
        self.assertEqual([row["age"] for row in sorted_by_age_desc], [28, 25, 22])

    def test_update_by_prunes_empty_columns(self):
        db = self.manager.create_database("update_prune_test")
        db.insert({"id": 1, "value": 100, "obsolete": "x"})
        db.insert({"id": 2, "value": 200, "obsolete": "x"})

        # Set 'obsolete' to None for all rows
        db.update_by("id", 1, {"obsolete": None})

        # obsolete should still exist since not all values are None
        all_rows = db.get_all()
        self.assertIn("obsolete", all_rows[0])
        self.assertIn("obsolete", all_rows[1])

        db.update_by("id", 2, {"obsolete": None})
        # 'obsolete' should be pruned since all values are now None
        all_rows = db.get_all()
        for row in all_rows:
            self.assertNotIn("obsolete", row)
            self.assertIn("value", row)
            self.assertIn("id", row)

    def test_delete_by_prunes_empty_columns(self):
        db = self.manager.create_database("delete_prune_test")
        db.insert({"id": 1, "name": "Alice", "remove_me": "yes"})
        db.insert({"id": 2, "name": "Bob", "remove_me": "yes"})
        db.insert({"id": 3, "name": "Charlie", "remove_me": "keep"})

        # Delete two rows, the only ones that had "remove_me" = "yes"
        db.delete_by("remove_me", "yes")

        # Only one row remains, and it should still contain 'remove_me'
        all_rows = db.get_all()
        self.assertEqual(len(all_rows), 1)
        self.assertIn("remove_me", all_rows[0])

        # Now set the remaining 'remove_me' to None, then check pruning
        db.update_by("id", 3, {"remove_me": None})
        all_rows = db.get_all()
        for row in all_rows:
            self.assertNotIn("remove_me", row)

    def test_rename_key(self):
        db = self.manager.create_database("rename_test")
        db.insert({"name": "Charlie", "age": 40})
        db.insert({"name": "Alice", "age": 25})

        # Rename 'name' to 'nickname'
        renamed = db.rename_key("name", "nickname")
        self.assertTrue(renamed)

        all_rows = db.get_all()
        for row in all_rows:
            self.assertIn("nickname", row)
            self.assertNotIn("name", row)

        # Ensure values were preserved
        self.assertEqual(all_rows[0]["nickname"], "Charlie")
        self.assertEqual(all_rows[1]["nickname"], "Alice")

        # Try renaming a key that doesn't exist
        result = db.rename_key("nonexistent", "something")
        self.assertFalse(result)


def manual_test_database(manager: PyStoreManager):
    db = manager.delete_database("print_test")
    print(f"Test Database 'print_test' deleted: {db}")

    db = manager.create_database("print_test")
    print(f"Test Database 'print_test' created: {db}")

    print("Test Database Insertions:")
    db.insert({"name": "Charlie", "age": 40})
    db.insert({"name": "Diana", "age": 35})
    db.insert({"name": "Eve", "age": 28, "city": "New York"})
    db.insert({"name": "Frank", "age": 22, "city": "Los Angeles"})
    db.insert({"name": "George", "age": 45, "city": "New York"})
    db.insert({"name": "Hannah", "city": None, "age": 30})
    print(f"Total entries in 'print_test': {len(db.get_all())}, expected: 5")

    print(f"Test Database Query:")
    print(f"\tfind by city None: {len(manager.get_database('print_test').find_by('city', None))}, expected: 2")
    print(f"\tfind by city 'New York': {len(manager.get_database('print_test').find_by('city', 'New York'))}, expected: 2")
    print(f"\tfind by city 'Los Angeles': {len(manager.get_database('print_test').find_by('city', 'Los Angeles'))}, expected: 1")

    # test dynamic column insertion
    db.insert({"test": "test", "test2": 1234, "test3": "3"})

    # sort test
    print("Test Database Printing:")
    manager.print_database("print_test")

    print("Test Database Sorting")
    print("\tsorting columns to match row 0:")
    manager.sort_columns("print_test", 0)
    manager.print_database("print_test")

    # test dynamic column deletion
    db.delete_by("test", "test")

    print("\tsorting columns to match provided list:")
    manager.sort_columns_by_list("print_test", ["age", "name", "not_a_column", "city"])
    manager.print_database("print_test")

    print("\treverting to original order:")
    manager.sort_columns_by_list("print_test", ["name", "age", "city"])
    manager.print_database("print_test")

    print("\tsorting by age descending:")
    manager.sort_database("print_test", "age", reverse=True)
    manager.print_database("print_test")

    print("\tsorting by name ascending:")
    manager.sort_database("print_test", "name")
    manager.print_database("print_test")

    print("\tsorting by age ascending:")
    manager.sort_database("print_test", "age")
    manager.print_database("print_test")

    print("\tsorting by city ascending:")
    manager.sort_database("print_test", "city")
    manager.print_database("print_test")

if __name__ == "__main__":
    print("--------- Manual Test Begin ---------\n")
    manual_test_database(PyStoreManager("./test_databases"))
    print("\n--------- Manual Test End -----------\n")

    unittest.main()
