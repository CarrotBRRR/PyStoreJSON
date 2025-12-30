#!/usr/bin/env python3

import json, sys
import shlex
from PyStoreJSONLib import PyStoreManager

def print_help():
    print(
        "Commands:\n"
        "  create <name>\n"
        "  list\n"
        "  delete <name>\n"
        "  print <name>\n"
        "  insert <db> <json>\n"
        "  find <db> <key> <value>\n"
        "  update <db> <key> <value> <json>\n"
        "  rename-key <db> <old_keyname> <new_keyname>\n"
        "  delete-by <db> <key> <value>\n"
        "  sort <db> <key> [--reverse | -r]\n"
        "  exit\n"
    )

def parse_value_token(token: str):
    """
    Parse a CLI token into an appropriate Python value.
    Examples:
      "null"   -> None
      "true"   -> True
      "3.14"   -> 3.14
      "42"     -> 42
      "hello"  -> "hello"
      "'hi'"   -> "hi"
      '"hi"'   -> "hi"
    """
    # Try raw JSON parse first (handles null, true, false, numbers, quoted strings)
    try:
        return json.loads(token)
    except Exception:
        pass

    # If token looks like an unquoted word (e.g. null was already tried),
    # try adding quotes to force a string.
    try:
        return json.loads(f'"{token}"')
    except Exception:
        # Fallback: return the original string
        return token

def interactive_cli(directory):
    manager = PyStoreManager(directory)
    print("PyStore JSON Database Shell")
    print("Type help for commands")

    while True:
        try:
            raw = input(">>> ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not raw:
            continue

        args = shlex.split(raw)
        cmd = args[0]

        if cmd == "exit":
            break

        if cmd == "help":
            print_help()
            continue

        if cmd == "create" and len(args) == 2:
            manager.create_database(args[1])
            print("[i] Database created")
            continue

        if cmd == "list":
            print("\n".join(manager.list_databases()))
            continue

        if cmd == "delete" and len(args) == 2:
            print("[i] This is a irreversible and destructive operation!")
            confirm = input(f"[?] Are you sure you want to delete the database `{args[1]}`? (Y/N): ").strip().lower()
            if confirm != 'y':
                print("[i] Delete cancelled")
                continue
            else:
                print("[i] Deleted" if manager.delete_database(args[1]) else f"[!] Database {args[1]} not found!")
                continue

        if cmd == "print" and len(args) == 2:
            manager.print_database(args[1])
            continue

        if cmd == "insert" and len(args) >= 3:
            db = manager.get_database(args[1])
            json_str = raw.split(" ", 2)[2]                 # take everything after the db name
            json_str = json_str.replace("'", '"')           # optional convenience
            
            try:
                data = json.loads(json_str)
            except json.JSONDecodeError as e:
                print(f"[!] Invalid JSON: {e}")
                continue

            db.insert(data)
            print("[i] Inserted")
            continue

        # FIND using parsed token
        if cmd == "find" and len(args) == 4:
            db = manager.get_database(args[1])
            value = parse_value_token(args[3])
            result = db.find_by(args[2], value)

            print(f"[i] Found {len(result)} matching entries:")
            for idx, row in enumerate(result):
                print(f"  {row}")

            continue

        # UPDATE with interactive selection if multiple matches found
        if cmd == "update" and len(args) >= 5:
            db = manager.get_database(args[1])
            key, value = args[2], parse_value_token(args[3])

            json_str = raw.split(" ", 4)[4].replace("'", '"')
            try:
                updates = json.loads(json_str)
            except json.JSONDecodeError as e:
                print(f"[!] Invalid JSON for updates: {e}")
                continue

            matches = db.find_by(key, value)

            if not matches:
                print("[i] No matching entries found")
                continue

            # Direct update when only one match exists
            if len(matches) == 1:
                db.update_by(key, value, updates)
                print("[i] Updated 1 entry")
                continue

            # Multiple matches detected
            print(f"[i] {len(matches)} matching entries found:")
            for idx, row in enumerate(matches):
                print(f"  [{idx}] {row}")

            print("\nOptions:")
            print("  index number     Update record at specified index")
            print("  all              Update all matching records")
            print("  cancel           Abort update\n")

            while True:
                choice = input("Select option: ").strip().lower()

                if choice == "cancel":
                    print("[i] Update cancelled")
                    break

                if choice == "all":
                    count = db.update_by(key, value, updates)
                    print(f"[i] Updated {count} entries")
                    break

                # Single index update
                if choice.isdigit() and int(choice) < len(matches):
                    selected = matches[int(choice)]
                    data = db.get_all()
                    row_index = data.index(selected)
                    data[row_index].update(updates)
                    db._save(data)
                    print(f"[i] Updated entry at index {choice}")
                    break

                print("[!] Invalid selection, please try again")
            continue
        
        if cmd == "rename-key" and len(args) == 4:
            ok = manager.get_database(args[1]).rename_key(args[2], args[3])
            print(f"Key {args[2]} renamed to {args[3]}" if ok else f"Key {args[2]} not found")
            continue

        if cmd == "delete-by" and len(args) == 4:
            value = parse_value_token(args[3])
            n_affected_rows = manager.get_database(args[1]).find_by(args[2], value).__len__()
            print("[i] This is a irreversible and destructive operation!")
            confirm = input(f"[?] Are you sure you want to delete {n_affected_rows} matching entries in database `{args[1]}`? (Y/N): ").strip().lower()
            if confirm != 'y':
                print("[i] Delete cancelled")
                continue
            else:
                count = manager.get_database(args[1]).delete_by(args[2], value)
                print(f"[i] Deleted {count} entries")
                continue

        if cmd == "sort" and len(args) >= 3:
            reverse = ("--reverse" or "-r") in args
            manager.sort_database(args[1], args[2], reverse)
            print("Sorted!")
            continue

        print("Invalid command or arguments")

if __name__ == "__main__":
    if len(sys.argv) >= 2: # directory passed as argument
        directory = sys.argv[1]
    else:
        directory = input("Database directory path: ").strip()

    interactive_cli(directory)
