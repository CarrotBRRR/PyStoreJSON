"""
PyStoreJSONDBManager
===

A manager for multiple JSON databases.

Authors:
-----
Dominic Choi
    GitHub: [CarrotBRRR](https://github.com/CarrotBRRR)

"""

import os, json, pprint
from typing import List, Dict

from PyStoreJSONLib.PyStoreJSON import PyStoreJSONDB

class PyStoreManager:
    def __init__(self, directory: str):
        """
        Manages multiple JSON databases within a directory.
        """
        self.directory = directory
        os.makedirs(self.directory, exist_ok=True)
        self.databases: Dict[str, PyStoreJSONDB] = {}

    def _get_path(self, name: str) -> str:
        return os.path.join(self.directory, f"{name}.json")

    def get_database(self, name: str) -> PyStoreJSONDB:
        """
        Return a PyStoreJSONDB instance for the given database name.
        Creates the database if it does not exist.
        """
        if name not in self.databases:
            db_path = self._get_path(name)
            self.databases[name] = PyStoreJSONDB(db_path)
        return self.databases[name]

    def create_database(self, name: str) -> PyStoreJSONDB:
        """
        Create a new database file with the given name if it doesn't exist.
        Returns a PyStoreJSONDB instance.
        """
        db_path = self._get_path(name)
        if not os.path.exists(db_path):
            with open(db_path, 'w') as f:
                json.dump([], f)
        db = PyStoreJSONDB(db_path)
        self.databases[name] = db
        return db

    def list_databases(self) -> List[str]:
        """
        List all database names (without the `.json` extension).
        """
        return [
            os.path.splitext(f)[0]
            for f in os.listdir(self.directory)
            if f.endswith(".json")
        ]

    def delete_database(self, name: str) -> bool:
        """
        Delete the specified database file from disk.
        Returns True if deleted, False if it did not exist.
        """
        db_path = self._get_path(name)
        if os.path.exists(db_path):
            os.remove(db_path)
            if name in self.databases:
                del self.databases[name]
            return True
        return False

    def sort_database(self, name: str, key: str, reverse: bool = False) -> List[Dict]:
        """
        Sort the rows in the specified database by the given key.
        Returns a sorted list of rows.
        """
        db = self.get_database(name)
        sorted_data = db._sort(key, reverse)

        db._save(sorted_data)

        return sorted_data

    def sort_columns(self, name: str, key_row_index: int, reverse: bool = False) -> List[Dict]:
        """
        Sort the rows in the specified database by the given key.
        Returns a sorted list of rows.
        """
        db = self.get_database(name)
        sorted_data = db._sort_columns(key_row_index, reverse)

        db._save(sorted_data)

        return sorted_data

    def sort_columns_by_list(self, name: str, column_order: List[str]) -> List[Dict]:
        """
        Sort the rows in the specified database by the given column order.
        Returns a sorted list of rows.
        """
        db = self.get_database(name)
        sorted_data = db._sort_columns_by_order(column_order)

        db._save(sorted_data)

        return sorted_data

    def rename_column(self, name: str, old_key: str, new_key: str) -> bool:
        """
        Rename a column in the specified database.

        :param name: Database name.
        :param old_key: Existing column name.
        :param new_key: New column name.
        :return: True if renamed, False if no such column existed.
        """
        db = self.get_database(name)
        return db.rename_key(old_key, new_key)

    def print_database(self, name: str) -> None:
        """
        Print the contents of the specified database.
        If the database does not exist, print a warning.
        """
        db_path = self._get_path(name)
        if not os.path.exists(db_path):
            print(f"[!] Database '{name}' does not exist.")
            return

        db = self.get_database(name)
        data = db.get_all()

        if not data:
            print(f"[i] Database '{name}' is empty.")
        else:
            print(f"--- Contents of '{name}' ---")
            for i, row in enumerate(data, start=1):
                print(f"[Row {i}]")
                pprint.pprint(row, indent=4, sort_dicts=False)
            print("--- End ---")