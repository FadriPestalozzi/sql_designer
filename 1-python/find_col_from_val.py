#!/usr/bin/env python3
"""Generate an SQL script that searches for a value across character columns of a table.

The script reads the aggregated column index (0-data/index.csv) to help the user
select a database, schema, and table. It then adapts the seed template
2-sql/find-ColumnName-containing-target-value.sql to the chosen table and
search value, producing a ready-to-run T-SQL script.
"""

from __future__ import annotations

import os
import sys
import csv
import textwrap
from typing import Dict, Iterable, List, Optional, Tuple

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
INDEX_PATH = os.path.join(BASE_DIR, '0-data', 'index.csv')
SEED_SQL_PATH = os.path.join(BASE_DIR, '2-sql', 'find-ColumnName-containing-target-value.sql')
SQL_OUTPUT_DIR = os.path.join(BASE_DIR, '2-sql')


def _detect_delimiter(first_line: str) -> str:
    """Heuristically determine whether the CSV uses comma or tab delimiters."""
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
    """Load (source, schema, table, column) tuples from index.csv."""
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


def build_table_index(entries: Iterable[Tuple[str, str, str, str]]) -> Dict[Tuple[str, str, str], List[str]]:
    """Group columns by (source, schema, table)."""
    table_map: Dict[Tuple[str, str, str], List[str]] = {}
    for source, schema, table, column in entries:
        key = (source, schema, table)
        table_map.setdefault(key, []).append(column)
    return table_map


def format_table(key: Tuple[str, str, str]) -> str:
    source, schema, table = key
    return f"{source}.{schema}.{table}"


def prompt_manual_table() -> Tuple[str, str, str]:
    """Prompt the user to type database, schema, and table names manually."""
    print('\nEnter target table manually.')
    while True:
        database = input('Database name: ').strip()
        if database:
            break
        print('Database name must not be empty.')
    while True:
        schema = input('Schema name: ').strip()
        if schema:
            break
        print('Schema name must not be empty.')
    while True:
        table = input('Table name: ').strip()
        if table:
            break
        print('Table name must not be empty.')
    return database, schema, table


def prompt_table_choice(tables: List[Tuple[str, str, str]]) -> Tuple[str, str, str]:
    """Interactively pick a table from the index or fall back to manual input."""
    if not tables:
        print('No indexed tables found; switching to manual input.')
        return prompt_manual_table()

    filtered = tables
    display_limit = 20

    instructions = (
        '\nSelect a target table. Enter the number shown, type a search fragment to filter, '
        "type 'manual' to enter table names directly, or 'all' to reset filters."
    )
    print(instructions)

    while True:
        to_show = filtered[:display_limit]
        print(f"\nShowing {len(to_show)} of {len(filtered)} matches:")
        for idx, table_key in enumerate(to_show, 1):
            print(f"  {idx:2d}. {format_table(table_key)}")
        if len(filtered) > display_limit:
            print('  ... use search to narrow results.')

        choice = input('Choice: ').strip()
        if not choice:
            print('Please enter a number, search fragment, or command.')
            continue

        lowered = choice.lower()
        if lowered == 'manual':
            return prompt_manual_table()
        if lowered == 'all':
            filtered = tables
            continue

        if choice.isdigit():
            idx = int(choice)
            if 1 <= idx <= len(to_show):
                return to_show[idx - 1]
            print(f'Enter a number between 1 and {len(to_show)}.')
            continue

        filtered = [key for key in tables if choice.lower() in format_table(key).lower()]
        if not filtered:
            print(f"No tables match '{choice}'. Try another search term.")
            filtered = tables


def prompt_non_empty(prompt: str) -> str:
    """Prompt until the user provides a non-empty string."""
    while True:
        value = input(prompt).strip()
        if value:
            return value
        print('Value must not be empty.')


def prompt_optional(prompt: str) -> str:
    """Prompt for optional input (returns empty string if skipped)."""
    return input(prompt).strip()


def parse_exclusions(raw: str) -> List[str]:
    """Parse a comma-delimited exclusion list."""
    if not raw:
        return []
    parts = [item.strip() for item in raw.split(',')]
    return [item for item in parts if item]


def escape_sql_literal(value: str) -> str:
    """Escape single quotes for embedding inside NVARCHAR literals."""
    return value.replace("'", "''")


def quote_identifier(identifier: str) -> str:
    """Wrap an identifier in brackets with escaping."""
    return f"[{identifier.replace(']', ']]')}]"


def sanitize_filename_component(component: str) -> str:
    """Create a filesystem-friendly component (letters, numbers, underscores)."""
    safe_chars = []
    for char in component:
        if char.isalnum():
            safe_chars.append(char)
        else:
            safe_chars.append('_')
    sanitized = ''.join(safe_chars).strip('_')
    return sanitized or 'value'


def render_sql(
    database: str,
    schema: str,
    table: str,
    search_value: str,
    row_filter: str,
    excluded_columns: List[str],
) -> str:
    """Build the final SQL script text."""
    db_ident = quote_identifier(database)
    schema_ident = quote_identifier(schema)
    table_ident = quote_identifier(table)
    full_table_ident = f"{db_ident}.{schema_ident}.{table_ident}"

    escaped_search = escape_sql_literal(search_value)
    row_filter_expression = row_filter.strip() or '1 = 1'
    escaped_row_filter = escape_sql_literal(row_filter_expression)

    exclusion_comment = ', '.join(excluded_columns) if excluded_columns else 'none'
    if excluded_columns:
        exclusion_literals = ', '.join(f"N'{escape_sql_literal(col)}'" for col in excluded_columns)
        exclusion_clause = f"\n      AND  c.name NOT IN ({exclusion_literals})"
    else:
        exclusion_clause = ''

    column_scan = textwrap.dedent(f"""
        INSERT INTO #Cols (ColumnName)
        SELECT c.name
        FROM   {db_ident}.sys.tables  AS t
        JOIN   {db_ident}.sys.schemas AS s  ON s.schema_id = t.schema_id
        JOIN   {db_ident}.sys.columns AS c  ON c.object_id = t.object_id
        JOIN   {db_ident}.sys.types   AS ty ON ty.user_type_id = c.user_type_id
        WHERE  s.name = N'{escape_sql_literal(schema)}'
          AND  t.name = N'{escape_sql_literal(table)}'
          AND  ty.name IN (N'char', N'nchar', N'varchar', N'nvarchar'){exclusion_clause};
    """)

    sql_body = textwrap.dedent(f"""
        /*
        Auto-generated by 1-python/find_col_from_val.py
        Seed template: 2-sql/find-ColumnName-containing-target-value.sql
        Target table: {full_table_ident}
        Search value: {search_value}
        Row filter: {row_filter_expression}
        Excluded columns: {exclusion_comment}
        */
        SET NOCOUNT ON;

        DECLARE @SearchValue NVARCHAR(200) = N'{escaped_search}';

        IF OBJECT_ID('tempdb..#Cols') IS NOT NULL DROP TABLE #Cols;
        CREATE TABLE #Cols (ColumnName SYSNAME NOT NULL);

        IF OBJECT_ID('tempdb..#Hits') IS NOT NULL DROP TABLE #Hits;
        CREATE TABLE #Hits (
            ColumnName SYSNAME NOT NULL,
            MatchCount INT     NOT NULL
        );

        IF OBJECT_ID('tempdb..#Samples') IS NOT NULL DROP TABLE #Samples;

        DECLARE @RowFilter NVARCHAR(MAX) = N'{escaped_row_filter}';
        DECLARE @FullTable NVARCHAR(512) = N'{full_table_ident}';

        SELECT TOP (0)
            CAST(NULL AS SYSNAME) AS ColumnName,
            t.*
        INTO #Samples
        FROM {full_table_ident} AS t;

    """)

    cursor_block = textwrap.dedent("""
        DECLARE @Col SYSNAME;
        DECLARE @sql NVARCHAR(MAX);

        DECLARE col_cur CURSOR LOCAL FAST_FORWARD FOR
            SELECT ColumnName FROM #Cols;

        OPEN col_cur;
        FETCH NEXT FROM col_cur INTO @Col;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @sql = N'
    IF EXISTS (
        SELECT 1
        FROM ' + @FullTable + N'
                WHERE ' + QUOTENAME(@Col) + N' = @p
          AND ' + @RowFilter + N'
    )
    BEGIN
     INSERT INTO #Hits (ColumnName, MatchCount)
     SELECT N''' + REPLACE(@Col, '''', '''''') + N''',
         COUNT(*)
     FROM ' + @FullTable + N'
     WHERE ' + QUOTENAME(@Col) + N' = @p
       AND ' + @RowFilter + N';

     INSERT INTO #Samples
     SELECT TOP (5)
         N''' + REPLACE(@Col, '''', '''''') + N''',
         t.*
     FROM ' + @FullTable + N' AS t
     WHERE ' + QUOTENAME(@Col) + N' = @p
    AND ' + @RowFilter + N';
    END';

            EXEC sp_executesql @sql, N'@p NVARCHAR(200)', @p = @SearchValue;

            FETCH NEXT FROM col_cur INTO @Col;
        END

        CLOSE col_cur;
        DEALLOCATE col_cur;
    """)

    result_block = textwrap.dedent("""
        SELECT ColumnName, MatchCount
        FROM   #Hits
        ORDER BY ColumnName;

    SELECT *
    FROM   #Samples
    ORDER BY ColumnName;

        SELECT
            TargetTable = @FullTable,
            RowFilter   = @RowFilter,
            SearchValue = @SearchValue,
            ColumnsScanned = (SELECT COUNT(*) FROM #Cols);
    """)

    parts = [sql_body, column_scan, cursor_block, result_block]
    return '\n'.join(part.strip('\n') for part in parts) + '\n'


def choose_output_path(database: str, schema: str, table: str) -> Optional[str]:
    """Ask the user whether to save the SQL script and determine the destination path."""
    default_name = (
        f"find-value-in-"
        f"{sanitize_filename_component(database)}-"
        f"{sanitize_filename_component(schema)}-"
        f"{sanitize_filename_component(table)}.sql"
    )
    print('\nHow should the SQL script be saved?')
    print(f"Press Enter to use the default name ({default_name}).")
    print("Type 'skip' to avoid writing a file, or provide a custom filename.")

    while True:
        choice = input('Output filename: ').strip()
        if not choice:
            filename = default_name
            break
        if choice.lower() == 'skip':
            return None
        if os.path.basename(choice) != choice:
            print('Please provide a filename without directory separators.')
            continue
        filename = choice
        break

    return os.path.join(SQL_OUTPUT_DIR, filename)


def write_sql_file(path: str, sql_text: str) -> None:
    """Persist the generated SQL script to disk."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8', newline='\n') as handle:
        handle.write(sql_text)


def summarize_inputs(
    database: str,
    schema: str,
    table: str,
    search_value: str,
    row_filter: str,
    excluded_columns: List[str],
) -> None:
    """Display a concise summary before generating SQL."""
    print('\nConfiguration summary:')
    print(f'  Target table : {database}.{schema}.{table}')
    print(f'  Search value : {search_value}')
    print(f"  Row filter   : {row_filter.strip() or '1 = 1'}")
    if excluded_columns:
        print(f"  Exclusions   : {', '.join(excluded_columns)}")
    else:
        print('  Exclusions   : none')


def load_seed_preview() -> None:
    """Print the seed template path for user awareness."""
    exists = os.path.exists(SEED_SQL_PATH)
    print(f"Seed template: {SEED_SQL_PATH} ({'found' if exists else 'missing'})")


def main() -> None:
    load_seed_preview()

    entries = read_index(INDEX_PATH)
    table_index = build_table_index(entries)
    table_choices = sorted(table_index.keys(), key=format_table)

    database, schema, table = prompt_table_choice(table_choices)
    search_value = prompt_non_empty('\nSearch value (literal fragment to look for): ')
    row_filter = prompt_optional("Row filter (T-SQL predicate, default 1 = 1): ")
    exclusions_raw = prompt_optional('Columns to exclude (comma-separated, optional): ')
    excluded_columns = parse_exclusions(exclusions_raw)

    summarize_inputs(database, schema, table, search_value, row_filter, excluded_columns)

    sql_text = render_sql(database, schema, table, search_value, row_filter, excluded_columns)

    print('\nGenerated SQL script:\n')
    print(sql_text)

    output_path = choose_output_path(database, schema, table)
    if output_path:
        write_sql_file(output_path, sql_text)
        print(f'\nSQL script written to {output_path}')
    else:
        print('\nSkipped writing SQL file.')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nAborted by user.')
        sys.exit(1)
