#!/usr/bin/env python3
"""
keys_2_schema.py

This script reads primary and foreign key information from CSV files 
and generates an XML schema file compatible with WWW SQL Designer.

Input files:
- 1-sql/keys-primary.csv: Contains primary key definitions
- 1-sql/keys-foreign.csv: Contains foreign key relationships

Output:
- output-schema.xml: XML schema file for SQL Designer
"""

import csv
import xml.etree.ElementTree as ET
from collections import defaultdict
import os
import math

class Table:
    """Represents a database table with its columns and keys"""
    def __init__(self, name):
        self.name = name
        self.columns = {}  # column_name -> Column object
        self.primary_keys = []  # list of (column_name, order) tuples
        self.x = 100  # Will be calculated by positioning algorithm
        self.y = 100
        self.width = 0  # Will be calculated based on content
        self.height = 0
        self.connections = 0  # Number of foreign key relationships
        self.is_positioned = False  # Track if table has been positioned
    
    def add_column(self, column_name, is_primary=False, is_foreign=False):
        """Add a column to the table"""
        if column_name not in self.columns:
            self.columns[column_name] = Column(column_name, is_primary, is_foreign)
        return self.columns[column_name]
    
    def add_primary_key(self, column_name, order):
        """Add a primary key column"""
        self.primary_keys.append((column_name, order))
        if column_name in self.columns:
            self.columns[column_name].is_primary = True
    
    def add_foreign_key(self, column_name, referenced_table, referenced_column):
        """Add a foreign key relationship"""
        if column_name not in self.columns:
            self.add_column(column_name, is_foreign=True)
        self.columns[column_name].add_foreign_key(referenced_table, referenced_column)
        self.connections += 1
    
    def calculate_dimensions(self):
        """Calculate table dimensions based on content"""
        # More accurate dimensions based on SQL Designer rendering
        char_width = 9  # Slightly wider for better accuracy
        row_height = 18  # Row height in SQL Designer
        header_height = 30  # Header height
        min_width = 160  # Minimum table width
        padding_width = 20  # Left/right padding
        padding_height = 15  # Top/bottom padding
        
        # Calculate width based on longest text (table name or column name)
        max_text_length = len(self.name)
        for column_name in self.columns:
            max_text_length = max(max_text_length, len(column_name))
        
        # Add extra width for foreign key indicators and datatypes
        self.width = max(min_width, max_text_length * char_width + padding_width * 2)
        
        # Calculate height: header + (number of columns * row height) + padding
        self.height = header_height + len(self.columns) * row_height + padding_height
        
        # Ensure minimum dimensions for very small tables
        self.width = max(self.width, 140)
        self.height = max(self.height, 60)
    
    def get_connected_tables(self):
        """Get list of tables this table connects to via foreign keys"""
        connected = set()
        for column in self.columns.values():
            for ref_table, ref_column in column.foreign_keys:
                connected.add(ref_table)
        return list(connected)

class Column:
    """Represents a table column"""
    def __init__(self, name, is_primary=False, is_foreign=False):
        self.name = name
        self.is_primary = is_primary
        self.is_foreign = is_foreign
        self.foreign_keys = []  # list of (table, column) tuples
        self.datatype = "INTEGER"  # Default datatype
        self.null = "1"  # Allow null by default
        self.autoincrement = "1" if is_primary else "0"
        self.default = "NULL"
    
    def add_foreign_key(self, referenced_table, referenced_column):
        """Add a foreign key reference"""
        self.foreign_keys.append((referenced_table, referenced_column))
        self.is_foreign = True

class SchemaGenerator:
    """Generates XML schema from CSV key files"""
    
    def __init__(self, primary_keys_file, foreign_keys_file):
        self.primary_keys_file = primary_keys_file
        self.foreign_keys_file = foreign_keys_file
        self.tables = {}  # table_name -> Table object
    
    def read_primary_keys(self):
        """Read primary key definitions from CSV"""
        print(f"Reading primary keys from {self.primary_keys_file}")
        
        with open(self.primary_keys_file, 'r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                table_name = row['TableName']
                column_name = row['ColumnName']
                key_order = int(row['KeyOrder'])
                
                # Create table if it doesn't exist
                if table_name not in self.tables:
                    self.tables[table_name] = Table(table_name)
                
                # Add column and primary key
                self.tables[table_name].add_column(column_name, is_primary=True)
                self.tables[table_name].add_primary_key(column_name, key_order)
        
        print(f"Loaded {len(self.tables)} tables from primary keys")
    
    def read_foreign_keys(self):
        """Read foreign key relationships from CSV"""
        print(f"Reading foreign keys from {self.foreign_keys_file}")
        
        foreign_key_count = 0
        with open(self.foreign_keys_file, 'r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                parent_table = row['ParentTable']
                referenced_table = row['ReferencedTable'] 
                parent_column = row['ParentColumn']
                referenced_column = row['ReferencedColumn']
                
                # Create parent table if it doesn't exist
                if parent_table not in self.tables:
                    self.tables[parent_table] = Table(parent_table)
                
                # Create referenced table if it doesn't exist
                if referenced_table not in self.tables:
                    self.tables[referenced_table] = Table(referenced_table)
                
                # Add foreign key relationship
                self.tables[parent_table].add_foreign_key(parent_column, referenced_table, referenced_column)
                
                # Ensure referenced column exists in referenced table
                self.tables[referenced_table].add_column(referenced_column)
                
                foreign_key_count += 1
        
        print(f"Loaded {foreign_key_count} foreign key relationships")
        print(f"Total tables: {len(self.tables)}")
    
    def calculate_table_connections(self):
        """Calculate connection counts for all tables (including incoming connections)"""
        # Count outgoing connections (already counted in add_foreign_key)
        # Now count incoming connections
        for table_name, table in self.tables.items():
            incoming_connections = 0
            for other_table_name, other_table in self.tables.items():
                if other_table_name != table_name:
                    connected_tables = other_table.get_connected_tables()
                    if table_name in connected_tables:
                        incoming_connections += connected_tables.count(table_name)
            table.connections += incoming_connections
        
        print(f"Calculated connections for all tables")
    
    def position_tables_intelligently(self):
        """Position tables using an intelligent layout algorithm"""
        print("Arranging tables intelligently...")
        
        # Calculate table dimensions first
        for table in self.tables.values():
            table.calculate_dimensions()
        
        # Calculate total connections for ranking
        self.calculate_table_connections()
        
        # Canvas dimensions
        canvas_width = 2400
        canvas_height = 1800
        center_x = canvas_width // 2
        center_y = canvas_height // 2
        
        # Sort tables by connection count (most connected first)
        sorted_tables = sorted(self.tables.values(), key=lambda t: t.connections, reverse=True)
        
        print(f"Most connected table: {sorted_tables[0].name} with {sorted_tables[0].connections} connections")
        
        # Show top 10 most connected tables with dimensions
        print("Top 10 most connected tables:")
        for i, table in enumerate(sorted_tables[:10]):
            print(f"  {i+1:2d}. {table.name:25s} - {table.connections:2d} connections - {table.width:3.0f}x{table.height:3.0f}px")
        
        positioned_tables = []
        
        # Position the most connected table at center
        center_table = sorted_tables[0]
        center_table.x = center_x - center_table.width // 2
        center_table.y = center_y - center_table.height // 2
        center_table.is_positioned = True
        positioned_tables.append(center_table)
        
        # Position tables level by level
        for table in sorted_tables[1:]:
            if not table.is_positioned:
                self.position_table_near_connections(table, positioned_tables, canvas_width, canvas_height)
                positioned_tables.append(table)
        
        print("Table arrangement completed")
        
        # Show positioning statistics
        center_area_count = 0
        for table in self.tables.values():
            if (center_x - 300 <= table.x + table.width//2 <= center_x + 300 and 
                center_y - 300 <= table.y + table.height//2 <= center_y + 300):
                center_area_count += 1
        
        print(f"Tables positioned in center area (600x600): {center_area_count}")
        print(f"Most connected tables are clustered near center for optimal visibility")
    
    def position_table_near_connections(self, table, positioned_tables, canvas_width, canvas_height):
        """Position a table near its connected tables"""
        connected_table_names = table.get_connected_tables()
        
        # Find positioned tables that this table connects to
        anchor_tables = []
        for positioned_table in positioned_tables:
            if positioned_table.name in connected_table_names:
                anchor_tables.append(positioned_table)
        
        if not anchor_tables:
            # No connections to positioned tables, find nearest positioned table
            anchor_tables = positioned_tables[-3:]  # Use last few positioned tables
        
        # Calculate average position of anchor tables
        avg_x = sum(t.x + t.width // 2 for t in anchor_tables) // len(anchor_tables)
        avg_y = sum(t.y + t.height // 2 for t in anchor_tables) // len(anchor_tables)
        
        # Try positions around the anchor point with expanding radius
        best_position = None
        min_overlap = float('inf')
        
        # Try multiple radii for better placement
        for radius in [250, 350, 450, 550]:
            for angle in range(0, 360, 15):  # Check positions every 15 degrees
                rad = math.radians(angle)
                test_x = avg_x + radius * math.cos(rad) - table.width // 2
                test_y = avg_y + radius * math.sin(rad) - table.height // 2
            
                # Keep within canvas bounds
                test_x = max(10, min(test_x, canvas_width - table.width - 10))
                test_y = max(10, min(test_y, canvas_height - table.height - 10))
                
                # Check for overlaps
                overlap_score = self.calculate_overlap_score(test_x, test_y, table.width, table.height, positioned_tables)
                
                if overlap_score < min_overlap:
                    min_overlap = overlap_score
                    best_position = (test_x, test_y)
                
                # If we found a position with no overlaps, use it immediately
                if overlap_score == 0:
                    break
            
            # If we found a non-overlapping position, no need to try larger radii
            if min_overlap == 0:
                break
        
        if best_position:
            table.x, table.y = best_position
        else:
            # Fallback: find any available space
            table.x, table.y = self.find_available_space(table, positioned_tables, canvas_width, canvas_height)
        
        table.is_positioned = True
    
    def calculate_overlap_score(self, x, y, width, height, positioned_tables):
        """Calculate overlap score for a potential table position"""
        overlap_score = 0
        margin = 50  # Increased minimum distance between tables
        
        for other_table in positioned_tables:
            # Check if rectangles overlap (with margin)
            if (x < other_table.x + other_table.width + margin and
                x + width + margin > other_table.x and
                y < other_table.y + other_table.height + margin and
                y + height + margin > other_table.y):
                
                # Calculate overlap area
                overlap_width = min(x + width + margin, other_table.x + other_table.width + margin) - max(x, other_table.x)
                overlap_height = min(y + height + margin, other_table.y + other_table.height + margin) - max(y, other_table.y)
                overlap_score += max(0, overlap_width) * max(0, overlap_height)
        
        return overlap_score
    
    def find_available_space(self, table, positioned_tables, canvas_width, canvas_height):
        """Find any available space for a table"""
        grid_size = 80  # Larger grid size for better spacing
        
        for y in range(10, canvas_height - table.height, grid_size):
            for x in range(10, canvas_width - table.width, grid_size):
                if self.calculate_overlap_score(x, y, table.width, table.height, positioned_tables) == 0:
                    return x, y
        
        # If no space found, place at bottom right area
        return canvas_width - table.width - 10, canvas_height - table.height - 10
    
    def generate_datatypes_xml(self):
        """Generate the datatypes XML section"""
        datatypes_xml = '''<datatypes db="mysql">
    <group label="Numeric" color="rgb(238,238,170)">
        <type label="Integer" length="0" sql="INTEGER" quote=""/>
        <type label="TINYINT" length="0" sql="TINYINT" quote=""/>
        <type label="SMALLINT" length="0" sql="SMALLINT" quote=""/>
        <type label="MEDIUMINT" length="0" sql="MEDIUMINT" quote=""/>
        <type label="INT" length="0" sql="INT" quote=""/>
        <type label="BIGINT" length="0" sql="BIGINT" quote=""/>
        <type label="Decimal" length="1" sql="DECIMAL" re="DEC" quote=""/>
        <type label="Single precision" length="0" sql="FLOAT" quote=""/>
        <type label="Double precision" length="0" sql="DOUBLE" re="DOUBLE" quote=""/>
    </group>
    <group label="Character" color="rgb(255,200,200)">
        <type label="Char" length="1" sql="CHAR" quote="'"/>
        <type label="Varchar" length="1" sql="VARCHAR" quote="'"/>
        <type label="Text" length="0" sql="MEDIUMTEXT" re="TEXT" quote="'"/>
        <type label="Binary" length="1" sql="BINARY" quote="'"/>
        <type label="Varbinary" length="1" sql="VARBINARY" quote="'"/>
        <type label="BLOB" length="0" sql="BLOB" re="BLOB" quote="'"/>
    </group>
    <group label="Date &amp; Time" color="rgb(200,255,200)">
        <type label="Date" length="0" sql="DATE" quote="'"/>
        <type label="Time" length="0" sql="TIME" quote="'"/>
        <type label="Datetime" length="0" sql="DATETIME" quote="'"/>
        <type label="Year" length="0" sql="YEAR" quote=""/>
        <type label="Timestamp" length="0" sql="TIMESTAMP" quote="'"/>
    </group>
    <group label="Miscellaneous" color="rgb(200,200,255)">
        <type label="ENUM" length="1" sql="ENUM" quote=""/>
        <type label="SET" length="1" sql="SET" quote=""/>
        <type label="Bit" length="0" sql="bit" quote=""/>
    </group>
</datatypes>'''
        return datatypes_xml
    
    def generate_xml(self, output_file):
        """Generate the complete XML schema file"""
        print(f"Generating XML schema to {output_file}")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            # XML header
            f.write('<?xml version="1.0" encoding="utf-8" ?>\n')
            f.write('<!-- SQL XML created by keys_2_schema.py -->\n')
            f.write('<!-- Generated from primary and foreign key CSV files -->\n')
            f.write('<sql>\n')
            
            # Write datatypes
            f.write(self.generate_datatypes_xml())
            
            # Write tables (sorted by connection count for better readability)
            sorted_tables = sorted(self.tables.items(), key=lambda x: x[1].connections, reverse=True)
            for table_name, table in sorted_tables:
                f.write(f'<table x="{int(table.x)}" y="{int(table.y)}" name="{table_name}">\n')
                
                # Write columns (rows)
                for column_name, column in table.columns.items():
                    f.write(f'<row name="{column_name}" null="{column.null}" autoincrement="{column.autoincrement}">\n')
                    f.write(f'<datatype>{column.datatype}</datatype>\n')
                    f.write(f'<default>{column.default}</default>')
                    
                    # Write foreign key relations
                    for ref_table, ref_column in column.foreign_keys:
                        f.write(f'<relation table="{ref_table}" row="{ref_column}" />\n')
                    
                    f.write('</row>\n')
                
                # Write primary key definition
                if table.primary_keys:
                    f.write('<key type="PRIMARY" name="">\n')
                    # Sort primary keys by order
                    sorted_pk = sorted(table.primary_keys, key=lambda x: x[1])
                    for column_name, order in sorted_pk:
                        f.write(f'<part>{column_name}</part>\n')
                    f.write('</key>\n')
                
                f.write('</table>\n')
            
            f.write('</sql>\n')
        
        print(f"Successfully generated XML schema with {len(self.tables)} tables")

def main():
    """Main function to generate schema from CSV files"""
    # File paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(script_dir)  # Go up one level from python/
    
    primary_keys_file = os.path.join(base_dir, '1-sql', 'keys-primary.csv')
    foreign_keys_file = os.path.join(base_dir, '1-sql', 'keys-foreign.csv')
    output_file = os.path.join(base_dir, 'output-schema.xml')
    
    # Check if input files exist
    if not os.path.exists(primary_keys_file):
        print(f"Error: Primary keys file not found: {primary_keys_file}")
        return
    
    if not os.path.exists(foreign_keys_file):
        print(f"Error: Foreign keys file not found: {foreign_keys_file}")
        return
    
    print("=== SQL Designer Schema Generator ===")
    print(f"Primary keys file: {primary_keys_file}")
    print(f"Foreign keys file: {foreign_keys_file}")
    print(f"Output file: {output_file}")
    
    # Generate schema
    generator = SchemaGenerator(primary_keys_file, foreign_keys_file)
    generator.read_primary_keys()
    generator.read_foreign_keys()
    generator.position_tables_intelligently()
    generator.generate_xml(output_file)
    
    print("=== Schema generation completed ===")

if __name__ == "__main__":
    main()