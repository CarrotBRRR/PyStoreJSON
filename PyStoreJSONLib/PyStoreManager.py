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
        Initialize the PyStoreManager with a directory to store datatables.
        If the directory does not exist, it will be created.
        
        :param directory: Directory where database files will be stored.
        :type directory: str
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

        :param name: Database name (without `.json` extension).
        :return: PyStoreJSONDB instance for the database.
        """
        if name not in self.databases:
            db_path = self._get_path(name)
            self.databases[name] = PyStoreJSONDB(db_path)
        return self.databases[name]

    def create_database(self, name: str) -> PyStoreJSONDB:
        """
        Create a new database file with the given name if it doesn't exist.
        Returns a PyStoreJSONDB instance.

        :param name: Database name (without `.json` extension).
        :return: PyStoreJSONDB instance for the new database.
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

        :param name: Database name.
        :return: True if the database was deleted, False if it did not exist.
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

        :param name: Database name.
        :param key: Key to sort by.
        :param reverse: If True, sort in descending order.
        :return: Sorted list of rows.
        """
        db = self.get_database(name)
        sorted_data = db.sort(key, reverse)

        db._save(sorted_data)

        return sorted_data

    def sort_columns(self, name: str, key_row_index: int, reverse: bool = False) -> List[Dict]:
        """
        Sort the rows in the specified database by the given key.
        Returns a sorted list of rows.

        :param name: Database name.
        :param key_row_index: Index of the reference row to sort by.
        """
        db = self.get_database(name)
        sorted_data = db.sort_columns(key_row_index, reverse)

        db._save(sorted_data)

        return sorted_data

    def sort_columns_by_list(self, name: str, column_order: List[str]) -> List[Dict]:
        """
        Sort the rows in the specified database by the given column order in a list.
        Returns a sorted list of rows.

        :param name: Database name.
        :param column_order: List of column names in desired order.
        :return: Sorted list of rows with columns reordered.
        """
        db = self.get_database(name)
        sorted_data = db.sort_columns_by_order(column_order)

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
        Print the contents of the specified database as a table.
        If the database does not exist, print a warning.

        :param name: Database name.
        """
        db_path = self._get_path(name)
        if not os.path.exists(db_path):
            print(f"[!] Database '{name}' does not exist.")
            return

        db = self.get_database(name)
        data = db.get_all()

        if not data:
            print(f"[i] Database '{name}' is empty.")
            return

        # Preserve the key order from the first row
        columns = list(data[0].keys())

        # Include any additional keys from other rows in order of first appearance
        for row in data[1:]:
            for key in row:
                if key not in columns:
                    columns.append(key)

        # Convert all rows to strings and fill in missing values
        rows = []
        for row in data:
            rows.append([str(row.get(col, "")) for col in columns])

        # Calculate column widths
        col_widths = [max(len(col), max(len(r[i]) for r in rows)) for i, col in enumerate(columns)]

        def format_row(row_vals):
            return "| " + " | ".join(f"{val:<{w}}" for val, w in zip(row_vals, col_widths)) + " |"

        # Print the table
        print(f"--- Contents of '{name}' ---")
        print("-" * (sum(col_widths) + 3 * len(col_widths) + 4))
        print(format_row(columns))  # Header row
        print("-" * (sum(col_widths) + 3 * len(col_widths) + 4))
        for row in rows:
            print(format_row(row))
        print("-" * (sum(col_widths) + 3 * len(col_widths) + 4))
        print("--- End ---")
