#!/usr/bin/env python3
"""Find shortest join path between two columns using columns.csv file and schema XML.

Usage: run the script and follow prompts.

Behavior implemented per user request:
- prompts user to choose among available database data folders (e.g. DWH_Dyconex) inside folder csv
- uses columns.csv inside chosen folder as list of all available schemas, tables, columns
- prompts user to choose iteratively between 1st and 2nd target-path
- a target-path is a unique concatenation of schema-table-column
- finds shortest path of table joins (BFS) between tables containing the two target paths
- prints an SQL inner-join statement joining tables along the found path

Assumptions:
- columns.csv has format: TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
- schema XML files contain relation elements for building the join graph
- table names in XML match those in columns.csv
"""

import os
import sys
import csv
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from typing import Dict, List, Tuple, Set  


def find_csv_databases(base_dir: str) -> List[str]:
    """Find all database folders in the csv directory"""
    csv_dir = os.path.join(base_dir, '0-data', 'csv')
    databases = []
    
    if not os.path.exists(csv_dir):
        return databases
    
    for item in os.listdir(csv_dir):
        item_path = os.path.join(csv_dir, item)
        if os.path.isdir(item_path):
            columns_file = os.path.join(item_path, 'columns.csv')
            if os.path.exists(columns_file):
                databases.append(item)
    
    return sorted(databases)

def read_columns_csv(path: str) -> Tuple[Dict[str, List[str]], Dict[str, Set[str]], List[str]]:
    """Parse the columns.csv file.

    Returns:
      target_paths: List[str] - all unique schema-table-column paths
      column_to_tables: Dict[str, Set[str]] - mapping column name -> set of table names
      table_to_columns: Dict[str, List[str]] - mapping table name -> list of column names
    """
    target_paths = []
    column_to_tables: Dict[str, Set[str]] = defaultdict(set)
    table_to_columns: Dict[str, List[str]] = defaultdict(list)
    
    with open(path, encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            schema = row['TABLE_SCHEMA']
            table = row['TABLE_NAME']
            column = row['COLUMN_NAME']
            
            # Create target path as schema-table-column
            target_path = f"{schema}-{table}-{column}"
            target_paths.append(target_path)
            
            # Create mappings for table lookup
            full_table_name = f"{schema}.{table}"
            table_to_columns[full_table_name].append(column)
            column_to_tables[column.lower()].add(full_table_name)
    
    return sorted(target_paths), column_to_tables, table_to_columns


def create_table_to_schema_mapping(target_paths: List[str]) -> Dict[str, str]:
    """Create mapping from table name to schema.table format."""
    table_to_schema = {}
    for target_path in target_paths:
        parts = target_path.split('-')
        if len(parts) >= 3:
            schema = parts[0]
            table = parts[1]
            full_table = f"{schema}.{table}"
            table_to_schema[table] = full_table
    return table_to_schema


def select_target_path(target_paths: List[str], prompt: str, database_name: str, tables_in_schema: Set[str]) -> str:
    """Let user choose a target path interactively from the selected database only"""
    print(f"\n{prompt}")
    
    # Filter target_paths to only include tables that exist in the schema XML
    # (tables with defined primary/foreign key relationships)
    schema_paths = []
    for path in target_paths:
        parts = path.split('-')
        if len(parts) >= 3:
            table = parts[1]  # Extract table name
            if table in tables_in_schema:
                schema_paths.append(path)
    
    db_paths = schema_paths
    
    if not db_paths:
        print(f"No columns found for database '{database_name}' with defined relationships.")
        print(f"Available tables with relationships: {', '.join(sorted(tables_in_schema))}")
        return ""
    
    print(f"Available columns in database '{database_name}' ({len(db_paths)} total):")
    
    def show_paths(paths_to_show, show_limit=None):
        """Display paths with optional limit"""
        if show_limit is None:
            show_limit = len(paths_to_show)
        
        # Group by table for better display
        by_table = defaultdict(list)
        for path in paths_to_show[:show_limit]:
            parts = path.split('-')
            if len(parts) >= 3:
                table = parts[1]  # Just table name without schema
                column = '-'.join(parts[2:])
                by_table[table].append((path, column))
        
        # Display grouped
        path_index = {}
        idx = 1
        for table, columns in sorted(by_table.items()):
            print(f"\n{table}:")
            for path, column in sorted(columns):
                print(f"  {idx:3d}. {column}")
                path_index[idx] = path
                idx += 1
        
        if show_limit < len(paths_to_show):
            print(f"\n... and {len(paths_to_show) - show_limit} more results (use search to filter)")
        
        return path_index
    
    # Allow iterative filtering
    current_paths = db_paths
    while True:
        # Always show all results first
        print(f"\nShowing all {len(current_paths)} results:")
        path_index = show_paths(current_paths, show_limit=len(current_paths))  # Show all
        
        if len(current_paths) > 30:
            search = input(f"\nEnter search term to filter, or number to choose (1-{len(current_paths)}): ").strip()
        else:
            search = input(f"\nEnter search term to filter, or number to choose (1-{len(current_paths)}): ").strip()
        
        # Check if input is a number (choice) or search term
        if search.isdigit() and 1 <= int(search) <= len(current_paths):
            selected_path = path_index[int(search)]
            print(f"Selected: {selected_path}")
            return selected_path
        elif search:  # It's a search term
            filtered_paths = [p for p in current_paths if search.lower() in p.lower()]
            if not filtered_paths:
                print(f"No matching paths found for '{search}'. Try a different search term.")
                continue
            
            print(f"\nFiltered results for '{search}' ({len(filtered_paths)} matches):")
            current_paths = filtered_paths
        else:
            print("Please enter either a number to select or a search term to filter.")
            continue


def path_to_table_column(target_path: str) -> Tuple[str, str]:
    """Convert schema-table-column path to (table_name, column_name)"""
    parts = target_path.split('-')
    if len(parts) >= 3:
        schema = parts[0]
        table = parts[1]
        column = '-'.join(parts[2:])
        # Return just the table name since XML schema doesn't include schema prefixes
        return table, column
    return "", ""

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


def build_sql_from_path(path: List[Tuple[str, Tuple[str,str]]], database_name: str, table_to_schema: Dict[str, str]) -> str:
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
    first_table_full = table_to_schema.get(first_table, first_table)
    sql = [f'USE {database_name};', f'', f'SELECT *\nFROM {first_table_full} AS {aliases[first_table]}']
    for i in range(1, len(path)):
        table, join_info = path[i]
        prev_table = path[i-1][0]
        prev_alias = aliases[prev_table]
        this_alias = aliases[table]
        this_table_full = table_to_schema.get(table, table)
        prev_col, this_col = join_info
        # quote identifiers simply
        sql.append(f'INNER JOIN {this_table_full} AS {this_alias} ON {prev_alias}."{prev_col}" = {this_alias}."{this_col}"')
    return '\n'.join(sql) + ';'


def main():
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    
    # Find available database folders
    databases = find_csv_databases(base)
    if not databases:
        print('No database folders found in 0-data/csv/')
        sys.exit(1)

    print('\nAvailable databases:')
    for idx, db in enumerate(databases, 1):
        print(f'{idx}. {db}')

    # Prompt user for database choice
    while True:
        choice = input(f'\nChoose database (1-{len(databases)}): ').strip()
        if choice.isdigit() and 1 <= int(choice) <= len(databases):
            selected_db = databases[int(choice) - 1]
            break
        print('Invalid choice')

    print(f'Selected database: {selected_db}')
    
    # Load columns from selected database
    columns_path = os.path.join(base, '0-data', 'csv', selected_db, 'columns.csv')
    schema_path = os.path.join(base, '0-data', f'{selected_db}-schema.xml')
    
    if not os.path.exists(columns_path):
        print(f'Columns file not found: {columns_path}')
        sys.exit(1)
        
    if not os.path.exists(schema_path):
        print(f'Schema XML file not found: {schema_path}')
        sys.exit(1)

    print('Loading columns data...')
    target_paths, column_to_tables, table_to_columns = read_columns_csv(columns_path)
    print(f'Found {len(target_paths)} target paths in {len(set(t.split("-")[1] for t in target_paths))} tables.')

    # Create table to schema mapping
    table_to_schema = create_table_to_schema_mapping(target_paths)

    print('Parsing schema XML...')
    relations, tables_in_schema = parse_schema_xml(schema_path)
    print(f'Parsed {len(tables_in_schema)} tables and {sum(len(v) for v in relations.values())} relation entries.')

    # Prompt for two target paths
    target_path1 = select_target_path(target_paths, "Choose first target path:", selected_db, tables_in_schema)
    target_path2 = select_target_path(target_paths, "Choose second target path:", selected_db, tables_in_schema)
    
    # Convert target paths to table and column names
    table1, col1 = path_to_table_column(target_path1)
    table2, col2 = path_to_table_column(target_path2)
    
    print(f'\nFirst target: {table1}.{col1}')
    print(f'Second target: {table2}.{col2}')
    
    # Check if tables exist in schema
    if table1 not in tables_in_schema:
        print(f'Table "{table1}" not found in schema XML.')
        print(f'This means the table exists in the database but has no primary/foreign key relationships defined.')
        print(f'Available tables in schema: {", ".join(sorted(tables_in_schema))}')
        print(f'Please choose columns from tables that have relationships defined.')
        sys.exit(3)
    if table2 not in tables_in_schema:
        print(f'Table "{table2}" not found in schema XML.')
        print(f'This means the table exists in the database but has no primary/foreign key relationships defined.')
        print(f'Available tables in schema: {", ".join(sorted(tables_in_schema))}')
        print(f'Please choose columns from tables that have relationships defined.')
        sys.exit(3)

    # Find shortest path between the two tables
    targets = {table2}
    print(f'Finding path from {table1} to {table2}...')

    path = bfs_shortest_path(relations, table1, targets)
    if not path:
        print('No join path found between the two tables')
        sys.exit(0)

    print('\nFound path:')
    for t, join in path:
        if join:
            print(f'  {t} (via {join[0]} = {join[1]})')
        else:
            print(f'  {t} (start)')

    sql = build_sql_from_path(path, selected_db, table_to_schema)
    print('\nGenerated SQL:')
    print(sql)


if __name__ == '__main__':
    main()
