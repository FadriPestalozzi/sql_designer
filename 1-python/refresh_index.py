import os
import csv
from pathlib import Path

def collect_column_names():
    """Collect the column catalogue rows and emit a comma-delimited index.csv."""
    data_folder = Path('0-data')
    all_rows = set()
    header_row = None
    
    # Walk through all subdirectories in 0-data/
    for root, dirs, files in os.walk(data_folder):
        if 'columns.csv' in files:
            csv_path = Path(root) / 'columns.csv'

            try:
                relative_root = Path(root).relative_to(data_folder)
                schema_name = relative_root.parts[0] if relative_root.parts else ''
            except ValueError:
                schema_name = ''

            if not schema_name:
                continue
            
            # Read column names from each columns.csv
            try:
                with open(csv_path, 'r', newline='', encoding='utf-8') as f:
                    first_line = f.readline()
                    if not first_line:
                        continue

                    comma_count = first_line.count(',')
                    tab_count = first_line.count('\t')
                    delimiter = '\t' if tab_count >= comma_count else ','

                    f.seek(0)
                    reader = csv.reader(f, delimiter=delimiter)
                    for row in reader:
                        cleaned_row = [col.strip() for col in row]
                        if not any(cleaned_row):
                            continue

                        if cleaned_row[0].lower() == 'table_schema':
                            if header_row is None:
                                header_row = ['SOURCE_SCHEMA'] + cleaned_row
                            continue

                        all_rows.add(tuple([schema_name] + cleaned_row))
            except Exception as e:
                print(f"Error reading {csv_path}: {e}")
    
    # Write collected column names to index.csv
    output_path = data_folder / 'index.csv'
    try:
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=',')
            sorted_rows = sorted(all_rows)

            if header_row is None:
                header_row = [
                    'SOURCE_SCHEMA',
                    'TABLE_SCHEMA',
                    'TABLE_NAME',
                    'COLUMN_NAME',
                    'DATA_TYPE',
                    'CHARACTER_MAXIMUM_LENGTH',
                    'IS_NULLABLE',
                ]

            writer.writerow(header_row)

            for row in sorted_rows:
                writer.writerow(row)
        
        print(f"Successfully wrote {len(all_rows)} unique columns to {output_path}")
    except Exception as e:
        print(f"Error writing to {output_path}: {e}")

if __name__ == '__main__':
    collect_column_names()