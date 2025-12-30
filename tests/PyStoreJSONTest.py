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

    def test_batch_update_by(self):
        db = self.manager.create_database("batch_update_test")
        db.insert([{"id": 1, "name": "Alice", "score": 70},
                   {"id": 2, "name": "Bob", "score": 65},
                   {"id": 3, "name": "Charlie", "score": 80}])
        
        updates = [
            ("id", 1, {"score": 90, "passed": True}),
            ("name", "Bob", {"score": 75}),
            ("id", 3, {"passed": True})
        ]

        db.insert({"id": 4, "name": "David", "score": 20, "passed": False})

        updated_count = db.batch_update_by(updates)
        self.assertEqual(updated_count, 3)

        all_rows = db.get_all()
        for row in all_rows:
            self.assertIn("passed", row)
            if row["id"] == 1:
                self.assertEqual(row["score"], 90)
                self.assertTrue(row["passed"])

            elif row["name"] == "Bob":
                self.assertEqual(row["score"], 75)
                self.assertEqual(row["passed"], None)

            elif row["id"] == 3:
                self.assertEqual(row["score"], 80)
                self.assertTrue(row["passed"])

            elif row["id"] == 4:
                self.assertEqual(row["score"], 20)
                self.assertFalse(row["passed"])

    def test_sort_columns(self):
        db = self.manager.create_database("sort_columns_test")
        db.insert({"b": 1, "c": 2, "a": 3})
        db.insert({"a": 6, "b": 4, "c": 5})
        db.insert({"a": 9, "b": 7, "c": 8})

        # Sort columns based on the values in row 0, ascending
        sorted_rows = db.sort_columns(0)
        self.assertEqual(list(sorted_rows[0].keys()), ["b", "c", "a"])
        self.assertEqual([row["b"] for row in sorted_rows], [1, 4, 7])
        self.assertEqual([row["c"] for row in sorted_rows], [2, 5, 8])
        self.assertEqual([row["a"] for row in sorted_rows], [3, 6, 9])

        # Sort columns based on row 0, descending
        sorted_rows_desc = db.sort_columns(0, reverse=True)
        self.assertEqual(list(sorted_rows_desc[0].keys()), ["a", "c", "b"])
        self.assertEqual([row["a"] for row in sorted_rows_desc], [3, 6, 9])
        self.assertEqual([row["c"] for row in sorted_rows_desc], [2, 5, 8])
        self.assertEqual([row["b"] for row in sorted_rows_desc], [1, 4, 7])

        # Insert row with a missing key to test defaulting to None
        db.insert({"a": 0, "c": 0})
        sorted_rows_missing = db.sort_columns(0)
        # Should still sort using row 0 as reference: b, c, a
        self.assertEqual(list(sorted_rows_missing[-1].keys()), ["b", "c", "a"])
        self.assertIsNone(sorted_rows_missing[-1]["b"])
        self.assertEqual(sorted_rows_missing[-1]["c"], 0)
        self.assertEqual(sorted_rows_missing[-1]["a"], 0)

        # Test index out of range
        with self.assertRaises(IndexError):
            db.sort_columns(100)


def manual_test_database(manager: PyStoreManager):
    db_len = len(manager.list_databases())
    db_deleted = manager.delete_database("print_test")
    print(f"Test Database 'print_test' deleted: {db_deleted}\n\tDatabases count expected? {len(manager.list_databases()) < db_len}")
    db = manager.create_database("print_test")
    print(f"Test Database 'print_test' created: {len(manager.list_databases()) == db_len}")
    print()

    print("Test Database Insertions:")
    db.insert({"name": "Charlie", "age": 40})
    db.insert({"name": "Diana", "age": 35})
    db.insert({"name": "Eve", "age": 28, "city": "New York"})
    db.insert({"name": "Frank", "age": 22, "city": "Los Angeles"})
    db.insert({"name": "George", "age": 45, "city": "New York"})
    db.insert({"name": "Hannah", "city": None, "age": 30})
    print(f"\tTotal entries in 'print_test': {len(db.get_all())}\n\t\tGot expected? {len(db.get_all()) == 6}")
    db.insert([{"name": "Alice", "age": 30, "city": "New York"},
               {"name": "Bob", "age": 25, "city": "Los Angeles"},
               {"name": "Ivy", "age": 29, "city": None}])
    print(f"\tTotal entries in 'print_test': {len(db.get_all())}\n\t\tGot expected? {len(db.get_all()) == 9}")
    print()

    print(f"Test Database Query:")
    print(f"\tfind by city None: {len(manager.get_database('print_test').find_by('city', None))}\n\t\tGot expected? {len(manager.get_database('print_test').find_by('city', None)) == 4}")
    print(f"\tfind by city 'New York': {len(manager.get_database('print_test').find_by('city', 'New York'))}\n\t\tGot expected? {len(manager.get_database('print_test').find_by('city', 'New York')) == 3}")
    print(f"\tfind by city 'Los Angeles': {len(manager.get_database('print_test').find_by('city', 'Los Angeles'))}\n\t\tGot expected? {len(manager.get_database('print_test').find_by('city', 'Los Angeles')) == 2}")
    print()

    print("Test Database Dynamic Column Insertion:")
    db.insert({"test": "test", "test2": 1234})
    manager.print_database("print_test")
    print()

    print("Test Database Dynamic Update:")
    db.update_by("test", "test", {"test2": None})
    manager.print_database("print_test")
    print()

    print("Test Database Dynamic Column Deletion:")
    db.delete_by("test", "test")
    manager.print_database("print_test")
    print()

    print("Test Database Sorting")
    print(f"sorting columns to match row 5: {db.at_index(5)}")
    manager.sort_columns("print_test", 5)
    manager.print_database("print_test")

    print("sorting columns to match provided list with non-column: ['age', 'city', 'name']")
    manager.sort_columns_by_list("print_test", ['age', 'city', 'name'])
    manager.print_database("print_test")

    print("sorting columns to match provided list with non-column: ['name', 'not_a_column', 'age', 'city']")
    manager.sort_columns_by_list("print_test", ['name', 'not_a_column', 'age', 'city'])
    manager.print_database("print_test")

    print("sorting by age descending:")
    manager.sort_database("print_test", "age", reverse=True)
    manager.print_database("print_test")

    print("sorting by name ascending:")
    manager.sort_database("print_test", "name")
    manager.print_database("print_test")

    print("sorting by age ascending:")
    manager.sort_database("print_test", "age")
    manager.print_database("print_test")

    print("sorting by city ascending:")
    manager.sort_database("print_test", "city")
    manager.print_database("print_test")

if __name__ == "__main__":
    print("--------- Manual Test Begin ---------\n")
    manual_test_database(PyStoreManager("./test_databases"))
    print("\n--------- Manual Test End -----------\n")

    unittest.main()
