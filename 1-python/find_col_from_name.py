#!/usr/bin/env python3
"""Search columns by name fragment across all indexed databases."""

import os
import sys
import csv
from typing import List, Tuple


def _detect_delimiter(first_line: str) -> str:
    """Heuristically determine whether the file is comma or tab delimited."""
    comma_count = first_line.count(',')
    tab_count = first_line.count('\t')
    if tab_count and not comma_count:
        return '\t'
    if comma_count and not tab_count:
        return ','
    if tab_count >= comma_count:
        return '\t'
    return ','


def read_index(index_path: str) -> List[Tuple[str, str, str, str]]:
    """Load (source, schema, table, column) tuples from the aggregated index."""
    if not os.path.exists(index_path):
        return []

    with open(index_path, encoding='utf-8', errors='replace', newline='') as handle:
        first_line = handle.readline()
        if not first_line:
            return []
        delimiter = _detect_delimiter(first_line)
        handle.seek(0)

        reader = csv.reader(handle, delimiter=delimiter, skipinitialspace=True)
        try:
            header_row = next(reader)
        except StopIteration:
            return []

        header_clean = [cell.strip().upper() for cell in header_row]
        required_headers = {'SOURCE_SCHEMA', 'TABLE_SCHEMA', 'TABLE_NAME', 'COLUMN_NAME'}
        has_header = required_headers.issubset(set(header_clean))

        rows: List[List[str]] = []
        if not has_header:
            rows.append(header_row)

        rows.extend(reader)

    results: List[Tuple[str, str, str, str]] = []
    for raw_row in rows:
        row = [cell.strip() for cell in raw_row]
        if len(row) < 4:
            continue
        source, schema, table, column = row[0], row[1], row[2], row[3]
        if not source or not schema or not table or not column:
            continue
        results.append((source, schema, table, column))

    results.sort(key=lambda entry: (
        entry[0].lower(),
        entry[1].lower(),
        entry[2].lower(),
        entry[3].lower(),
    ))
    return results


def prompt_search_string() -> str:
    """Ask the user for the search fragment they want to look up."""
    while True:
        search = input('\nEnter search string: ').strip()
        if search:
            return search
        print('Search string must not be empty.')


def find_columns(columns: List[Tuple[str, str, str, str]], search: str) -> List[Tuple[str, str, str, str]]:
    """Return columns whose name contains the search fragment (case insensitive)."""
    search_lower = search.lower()
    return [entry for entry in columns if search_lower in entry[3].lower()]


def main() -> None:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    index_path = os.path.join(base_dir, '0-data', 'index.csv')

    columns = read_index(index_path)
    if not columns:
        print(f'No column metadata found in {index_path}.')
        sys.exit(1)

    search = prompt_search_string()
    matches = find_columns(columns, search)

    if not matches:
        print(f'No columns found containing "{search}".')
        sys.exit(0)

    print(f'\nColumns containing "{search}":')
    for idx, (source, schema, table, column) in enumerate(matches, 1):
        print(f'{idx}. {source}.{schema}.{table}.{column}')


if __name__ == '__main__':
    main()
