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
from typing import List, Dict, Union


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

    def _prune_empty_columns(self, data: List[Dict]) -> None:
        """
        Remove columns that are None or missing in *every* row.
        This modifies the `data` list in place.
        """
        if not data:
            return

        # Get all keys across all rows
        all_keys = set().union(*(row.keys() for row in data))

        # Identify keys where every row has either None or does not include the key
        empty_keys = set()
        for key in all_keys:
            if all(row.get(key, None) is None for row in data):
                empty_keys.add(key)

        # Remove these keys from all rows
        for row in data:
            for key in empty_keys:
                row.pop(key, None)

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
        Update rows that match the given key-value condition with new values.

        :param key: Column name to match.
        :param value: Value to match.
        :param updates: Dictionary of updates to apply.
        :return: Number of rows updated.
        """
        data = self._load()
        count = 0

        existing_keys = set().union(*(row.keys() for row in data))
        new_keys = set(updates.keys()) - existing_keys

        # Ensure schema consistency
        for row in data:
            for k in new_keys:
                row[k] = None

        # Apply the updates
        for row in data:
            if row.get(key) == value:
                row.update(updates)
                count += 1

        self._prune_empty_columns(data)
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

        self._prune_empty_columns(new_data)
        self._save(new_data)

        return count

    def rename_key(self, old_key: str, new_key: str) -> bool:
        """
        Rename a key in all rows of the database.

        :param old_key: Existing column name to rename.
        :param new_key: New column name to assign.
        :return: True if renamed, False if old_key did not exist in any row.
        """
        data = self._load()
        renamed = False

        for row in data:
            if old_key in row:
                row[new_key] = row.pop(old_key)
                renamed = True

        if renamed:
            self._save(data)

        return renamed

    def _sort(self, key: str, reverse: bool = False) -> List[Dict]:
        """
        Sort the database rows by the specified key, handling None values.

        :param key: The key (column) to sort by.
        :param reverse: Whether to sort in descending order.
        :return: Sorted list of rows.
        """
        data = self._load()

        def safe_sort_key(row):
            val = row.get(key, None)
            # Use a tuple: (is_none, actual_value)
            return (val is None, val)

        try:
            sorted_data = sorted(data, key=safe_sort_key, reverse=reverse)
        except TypeError:
            raise ValueError(f"Cannot sort by key '{key}' due to incomparable types.")

        return sorted_data

    def _sort_columns(self, row_index: int, reverse: bool = False) -> List[Dict]:
        """
        Sort the columns of all rows based on the values in the specified row index.

        :param row_index: Index of the row to use as a reference for column sort order.
        :param reverse: Whether to sort columns in descending order.
        :return: List of rows with reordered keys.
        """
        data = self._load()

        if not (0 <= row_index < len(data)):
            raise IndexError("Row index out of range.")

        reference_row = data[row_index]

        # Sort keys by value, with None last, and coercion to string for safe comparison
        def sort_key(k):
            val = reference_row.get(k)
            return (val is None, str(val))

        sorted_keys = sorted(reference_row.keys(), key=sort_key, reverse=reverse)

        # Reconstruct each row with keys in sorted order
        sorted_rows = []
        for row in data:
            sorted_row = {key: row.get(key, None) for key in sorted_keys}
            sorted_rows.append(sorted_row)

        return sorted_rows

    def _sort_columns_by_order(self, column_order: Union[List[str], Dict[str, int]]) -> List[Dict]:
        """
        Reorders columns in all rows according to the provided column order.
        Columns not listed will appear at the end in original order.

        :param column_order: Desired column order as a list or dict with priorities.
        :return: List of rows with keys reordered.
        """
        from collections import defaultdict

        data = self._load()

        # Convert list to index-priority dict
        if isinstance(column_order, list):
            priority_map = {key: idx for idx, key in enumerate(column_order)}
        elif isinstance(column_order, dict):
            priority_map = column_order
        else:
            raise TypeError("column_order must be a list or dict")

        def sort_key(k, original_index):
            return (
                0 if k in priority_map else 1,                    # Priority bucket: known vs unknown
                priority_map.get(k, float('inf')),                # Sort known keys by specified priority
                original_index                                     # Preserve original order for unknowns
            )

        # Reconstruct each row with sorted keys
        sorted_rows = []
        for row in data:
            keys_with_index = list(enumerate(row.keys()))
            sorted_keys = [k for _, k in sorted(
                keys_with_index,
                key=lambda pair: sort_key(pair[1], pair[0])
            )]
            sorted_row = {k: row.get(k, None) for k in sorted_keys}
            sorted_rows.append(sorted_row)

        return sorted_rows
