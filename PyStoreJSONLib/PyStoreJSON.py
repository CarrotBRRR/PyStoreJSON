"""
PyStoreJSON.py
===
Database controller that manages JSON-based databases.
To be embedded in other Python scripts.

Each database is a JSON file with columns and rows
Each row is a dictionary with column names as keys

Authors:
-----
Dominic Choi
    GitHub: [CarrotBRRR](https://github.com/CarrotBRRR)
"""

import json
import os
from typing import List, Dict, Optional


class PyStoreJSONDB:
    def __init__(self, filepath: str):
        """
        Initialize the PyStoreJSONDB with the path to the JSON file.
        Creates the file if it does not exist.
        """
        self.filepath = filepath
        if not os.path.exists(self.filepath):
            with open(self.filepath, 'w') as f:
                json.dump([], f)

    def _load(self) -> List[Dict]:
        """Load and return all rows from the JSON file."""
        with open(self.filepath, 'r') as f:
            return json.load(f)

    def _save(self, data: List[Dict]) -> None:
        """Save all rows to the JSON file."""
        with open(self.filepath, 'w') as f:
            json.dump(data, f, indent=4)

    def insert(self, row: Dict):
        """
        Insert a new row into the database, adding missing columns to existing rows
        and missing values to the new row to ensure column consistency.

        :param row: Dictionary representing a row.
        """
        data = self._load()

        # Collect all keys from existing data
        all_keys = set()
        for entry in data:
            all_keys.update(entry.keys())

        new_keys = set(row.keys()) - all_keys
        missing_keys = all_keys - set(row.keys())

        # Add new keys to existing rows
        if new_keys:
            for entry in data:
                for key in new_keys:
                    entry[key] = None

        # Add missing keys to the new row
        for key in missing_keys:
            row[key] = None

        data.append(row)
        self._save(data)

    def get_all(self) -> List[Dict]:
        """Return all rows in the database."""
        return self._load()

    def find_by(self, key: str, value) -> List[Dict]:
        """
        Find all rows where the given key matches the specified value.

        :param key: Column name to match.
        :param value: Value to match.
        :return: List of matching rows.
        """
        return [row for row in self._load() if row.get(key) == value]

    def update_by(self, key: str, value, updates: Dict) -> int:
        """
        Update rows that match the given key-value condition.
        Adds any new keys in `updates` to all rows with a default value of None.

        :param key: Column name to match.
        :param value: Value to match.
        :param updates: Dictionary of fields to update.
        :return: Number of rows updated.
        """
        data = self._load()
        count = 0

        # Determine if any update keys are new
        existing_keys = set().union(*(row.keys() for row in data))
        new_keys = set(updates.keys()) - existing_keys

        # Add new keys to all rows
        if new_keys:
            for row in data:
                for new_key in new_keys:
                    row[new_key] = None

        # Apply updates to matching rows
        for row in data:
            if row.get(key) == value:
                row.update(updates)
                count += 1

        self._save(data)
        return count

    def delete_by(self, key: str, value) -> int:
        """
        Delete rows that match the given key-value condition.

        :param key: Column name to match.
        :param value: Value to match.
        :return: Number of rows deleted.
        """
        data = self._load()
        new_data = [row for row in data if row.get(key) != value]
        count = len(data) - len(new_data)
        self._save(new_data)

        return count

    def _sort(self, key: str, reverse: bool = False) -> List[Dict]:
        """
        Sort the database rows by the specified key.

        :param key: The key (column) to sort by.
        :param reverse: Whether to sort in descending order.
        :return: Sorted list of rows.
        """
        data = self._load()
        try:
            sorted_data = sorted(data, key=lambda row: row.get(key, None), reverse=reverse)
        except TypeError:
            raise ValueError(f"Cannot sort by key '{key}' due to incomparable types.")
        return sorted_data
