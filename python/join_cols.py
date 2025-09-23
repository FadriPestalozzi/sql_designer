#!/usr/bin/env python3
"""Find shortest join path between two columns using column-names file and schema XML.

Usage: run the script and follow prompts.

Behavior implemented per user request:
- loads `0-db-schema\2-col-names\vs-sql9-1-refactored_schemas.txt`
- prompts user to choose a schema XML from `0-db-schema\1-ID-schemas` (DWH_Dyconex.xml or DycoPlanEx.xml)
- prompts for two target column names
- parses chosen schema XML for <table name> and <row> and <relation table=.. row=..> elements to build a graph of table relationships
- finds shortest path of table joins (BFS) from a table containing first column to any table containing second column
- prints an SQL inner-join statement joining tables along the found path

Assumptions:
- table names in the XML are fully qualified (e.g. `DWH_Dyconex.production.ProductionOrders`) and the col-names file uses dash/sep names (we will match column names case-insensitively across tables by exact column name)
- relation elements in XML are the source of join edges; relation table attribute points to the referenced table and row attribute names the referenced column
"""

import os
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from typing import Dict, List, Tuple, Set


ROOT = os.path.dirname(os.path.dirname(__file__))  # py/ -> project root


def read_colnames_file(path: str):
    """Parse the tab-indented col-names file.

    The file uses leading tabs to indicate level:
      0: database
      1: schema
      2: table
      3: column lines (numbered and in form "N. ColumnName/typ/...")

    Returns:
      table_to_columns: Dict[str, List[str]] using the table label as found (e.g. 'DWH_Dyconex-engineering-Products')
      column_to_tables: Dict[str, Set[str]] mapping column name lowercase -> set(table)
    """
    table_to_columns: Dict[str, List[str]] = defaultdict(list)
    column_to_tables: Dict[str, Set[str]] = defaultdict(set)

    with open(path, encoding='utf-8', errors='replace') as f:
        cur_db = None
        cur_schema = None
        cur_table = None
        for raw in f:
            line = raw.rstrip('\n')
            if not line.strip():
                continue
            # count leading tabs
            stripped = line.lstrip('\t')
            level = len(line) - len(stripped)
            txt = stripped.strip()
            # lines starting with '-' are headings for database/schema/table; column lines start with a number + '.'
            if level == 0:
                cur_db = txt.lstrip('- ').strip()
                cur_schema = None
                cur_table = None
            elif level == 1:
                cur_schema = txt.lstrip('- ').strip()
                cur_table = None
            elif level == 2:
                # table heading
                cur_table = txt.lstrip('- ').strip()
                # normalize spaces
                table_to_columns[cur_table] = []
            elif level >= 3:
                # column line like: "1. ColumnName/type/..."
                # extract part after number and dot
                # find first '.' then take remainder before first '/'
                part = txt
                # remove leading numbering if present
                if '.' in part and part.split('.')[0].strip().isdigit():
                    part = '.'.join(part.split('.')[1:]).strip()
                # column name is before first '/'
                colname = part.split('/')[0].strip()
                if cur_table:
                    table_to_columns[cur_table].append(colname)
                    column_to_tables[colname.lower()].add(cur_table)
    return table_to_columns, column_to_tables


def list_schema_files(schema_dir: str) -> List[str]:
    files = [f for f in os.listdir(schema_dir) if f.lower().endswith('.xml')]
    return sorted(files)


def parse_schema_xml(path: str) -> Tuple[Dict[str, List[Tuple[str,str]]], Set[str]]:
    """Parse the WWW SQL Designer XML and extract relations.

    Returns:
      relations: Dict[src_table, List[(dst_table, (src_col, dst_col))]]
      tables: set of table names present in XML

    We parse <table name="..."> with child <row name="..."> and inner <relation table="..." row="..." /> elements.
    For each relation element inside a row of a table, we create an undirected edge between the current table and the referenced table with join columns (current row name, relation row).
    """
    tree = ET.parse(path)
    root = tree.getroot()
    relations: Dict[str, List[Tuple[str, Tuple[str,str]]]] = defaultdict(list)
    tables: Set[str] = set()

    for table in root.findall('.//table'):
        tname = table.get('name')
        if not tname:
            continue
        tables.add(tname)
        for row in table.findall('row'):
            src_col = row.get('name')
            if not src_col:
                continue
            for rel in row.findall('relation'):
                dst_table = rel.get('table')
                dst_col = rel.get('row')
                if dst_table and dst_col:
                    # add bidirectional edge
                    relations[tname].append((dst_table, (src_col, dst_col)))
                    relations[dst_table].append((tname, (dst_col, src_col)))
                    tables.add(dst_table)
    return relations, tables


def find_tables_containing(col_lower: str, column_to_tables: Dict[str, Set[str]], tables_in_schema: Set[str]) -> List[str]:
    # from mapping, return tables that exist in the chosen schema
    candidates = column_to_tables.get(col_lower, set())
    # filter by tables present in schema (names in col-names file might be formatted differently than xml names)
    # If candidate table names exist verbatim in schema XML tables (or contained within), keep them.
    result = []
    for t in candidates:
        # try direct match with xml table names: many XML table names are fully qualified like 'DWH_Dyconex.engineering.Products'
        # The col-names file table keys often use dash names like 'DWH_Dyconex-engineering-Products'. We'll match by replacing '-' with '.' and vice versa.
        alt1 = t.replace('-', '.')
        alt2 = t.replace('-', '_')
        # prefer exact matches, then alt1, then substring match
        matches = [x for x in tables_in_schema if x == t or x == alt1 or x.endswith('.' + t) or t in x or alt1 in x or alt2 in x]
        if matches:
            # pick first matching xml table name
            result.append(matches[0])
    return result


def bfs_shortest_path(relations: Dict[str, List[Tuple[str, Tuple[str,str]]]], start: str, targets: Set[str]) -> List[Tuple[str, Tuple[str,str]]]:
    """BFS over tables to find shortest path from start table to any target table.

    Returns a list of (table_name, join_info) entries representing the path. join_info is the join columns used between previous table and this table.
    The first element is (start, None).
    """
    q = deque()
    q.append(start)
    parent = {start: None}
    parent_join = {start: None}

    found = None
    while q:
        cur = q.popleft()
        if cur in targets:
            found = cur
            break
        for (nbr, (cur_col, nbr_col)) in relations.get(cur, []):
            if nbr not in parent:
                parent[nbr] = cur
                parent_join[nbr] = (cur_col, nbr_col)
                q.append(nbr)

    if not found:
        return []

    # reconstruct path
    path = []
    cur = found
    while cur is not None:
        path.append((cur, parent_join[cur]))
        cur = parent[cur]
    path.reverse()
    return path


def build_sql_from_path(path: List[Tuple[str, Tuple[str,str]]]) -> str:
    """Construct an INNER JOIN SQL joining all tables along path.

    Path is list of (table_name, join_info) where join_info for first item is None, for subsequent items is (prev_col, this_col)
    We'll alias tables t0, t1, ... and select * from first table.
    """
    if not path:
        return '-- no path found'
    aliases = {}
    parts = []
    for i, (table, join_info) in enumerate(path):
        aliases[table] = f't{i}'
    first_table = path[0][0]
    sql = [f'SELECT *\nFROM {first_table} AS {aliases[first_table]}']
    for i in range(1, len(path)):
        table, join_info = path[i]
        prev_table = path[i-1][0]
        prev_alias = aliases[prev_table]
        this_alias = aliases[table]
        prev_col, this_col = join_info
        # quote identifiers simply
        sql.append(f'INNER JOIN {table} AS {this_alias} ON {prev_alias}."{prev_col}" = {this_alias}."{this_col}"')
    return '\n'.join(sql) + ';'


def main():
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    colnames_path = os.path.join(base, '0-db-schema', '2-col-names', 'vs-sql9-1-refactored_schemas.txt')
    schema_dir = os.path.join(base, '0-db-schema', '1-ID-schemas')

    if not os.path.exists(colnames_path):
        print('Column names file not found at', colnames_path)
        sys.exit(1)

    print('Parsing column-names file...')
    table_to_columns, column_to_tables = read_colnames_file(colnames_path)
    print(f'Found {len(table_to_columns)} tables in col-names file.')

    files = list_schema_files(schema_dir)
    if not files:
        print('No schema XML files found in', schema_dir)
        sys.exit(1)

    print('\nAvailable schema files:')
    for idx, f in enumerate(files, 1):
        print(f'{idx}. {f}')

    # prompt user for schema choice
    while True:
        choice = input('\nChoose schema file by number (default 1): ').strip() or '1'
        if not choice.isdigit() or not (1 <= int(choice) <= len(files)):
            print('Invalid choice')
            continue
        schema_file = files[int(choice) - 1]
        break

    schema_path = os.path.join(schema_dir, schema_file)
    print('Parsing schema XML', schema_path)
    relations, tables_in_schema = parse_schema_xml(schema_path)
    print(f'Parsed {len(tables_in_schema)} tables and {sum(len(v) for v in relations.values())} relation entries.')

    # prompt for two target column names
    col1 = input('\nEnter first target column name (e.g. ProductID): ').strip()
    col2 = input('Enter second target column name (e.g. CustomerID): ').strip()
    if not col1 or not col2:
        print('Both column names required')
        sys.exit(2)

    # find candidate tables for each column using the col-names mapping and filter by tables present in schema
    cand1 = find_tables_containing(col1.lower(), column_to_tables, tables_in_schema)
    cand2 = find_tables_containing(col2.lower(), column_to_tables, tables_in_schema)

    if not cand1:
        print(f'No tables in chosen schema contain column "{col1}" (by matching col-names mapping)')
        # also try a fallback: search xml tables for rows with that column name directly
        cand1 = [t for t in tables_in_schema if any(col1 == r.get('name') for r in ET.parse(schema_path).getroot().findall(f".//table[@name='{t}']/row"))]

    if not cand2:
        print(f'No tables in chosen schema contain column "{col2}" (by matching col-names mapping)')
        cand2 = [t for t in tables_in_schema if any(col2 == r.get('name') for r in ET.parse(schema_path).getroot().findall(f".//table[@name='{t}']/row"))]

    if not cand1 or not cand2:
        print('Could not find candidate tables for one or both columns. Candidates1:', cand1, 'Candidates2:', cand2)
        sys.exit(3)

    print('\nCandidate tables for first column:', cand1)
    print('Candidate tables for second column:', cand2)

    # pick the first table containing the first column as search start (per request)
    start = cand1[0]
    targets = set(cand2)
    print(f'Using start table {start} (first table containing {col1})')

    path = bfs_shortest_path(relations, start, targets)
    if not path:
        print('No join path found between tables containing the two columns')
        sys.exit(0)

    print('\nFound path:')
    for t, join in path:
        print('-', t, 'via', join)

    sql = build_sql_from_path(path)
    print('\nGenerated SQL:')
    print(sql)


if __name__ == '__main__':
    main()
