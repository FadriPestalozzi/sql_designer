import os
import csv
from pathlib import Path

def collect_column_names():
    """Collect the column catalogue rows and emit a comma-delimited index.csv."""
    data_folder = Path('0-data')
    all_rows = set()
    
    # Walk through all subdirectories in 0-data/
    for root, dirs, files in os.walk(data_folder):
        if 'columns.csv' in files:
            csv_path = Path(root) / 'columns.csv'
            
            # Read column names from each columns.csv
            try:
                with open(csv_path, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.reader(f, delimiter='\t')
                    for row in reader:
                        cleaned_row = [col.strip() for col in row]
                        if any(cleaned_row):
                            all_rows.add(tuple(cleaned_row))
            except Exception as e:
                print(f"Error reading {csv_path}: {e}")
    
    # Write collected column names to index.csv
    output_path = data_folder / 'index.csv'
    try:
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=',')
            sorted_rows = sorted(all_rows)

            # Ensure the header stays at the top if present
            header = next((row for row in sorted_rows if row and row[0].lower() == 'table_schema'), None)
            if header:
                sorted_rows.remove(header)
                sorted_rows.insert(0, header)

            for row in sorted_rows:
                writer.writerow(row)
        
        print(f"Successfully wrote {len(all_rows)} unique columns to {output_path}")
    except Exception as e:
        print(f"Error writing to {output_path}: {e}")

if __name__ == '__main__':
    collect_column_names()