#!/usr/bin/env python3
"""
keys_2_schema.py

This script reads primary and foreign key information from CSV files 
and generates an XML schema file compatible with WWW SQL Designer.

Input file        max_tables_to_place = 3  # Set to a number like 10, 20, etc. for debugging, or None for all tables:
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
        """Calculate exact table dimensions based on actual content"""
        # Very precise measurements based on SQL Designer rendering characteristics
        char_width = 9.0   # Character width in pixels (conservative estimate)
        row_height = 18    # Each row height in pixels including line spacing
        header_height = 28 # Table header/title height with proper spacing
        min_width = 140    # Increased minimum width for better visibility
        padding_width = 24 # Increased left/right padding
        padding_height = 12 # Increased top/bottom padding
        
        # Calculate exact width based on longest text (table name or any column name)
        max_text_length = len(self.name)
        for column_name in self.columns:
            max_text_length = max(max_text_length, len(column_name))
        
        # Calculate exact dimensions
        self.width = max(min_width, max_text_length * char_width + padding_width)
        self.height = header_height + len(self.columns) * row_height + padding_height
        
        # Round up to avoid sub-pixel issues
        self.width = int(self.width + 0.5)
        self.height = int(self.height + 0.5)
    
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
    
    # Global margin for table spacing
    MARGIN = 10
    
    def __init__(self, primary_keys_file, foreign_keys_file):
        self.primary_keys_file = primary_keys_file
        self.foreign_keys_file = foreign_keys_file
        self.tables = {}  # table_name -> Table object
        self.occupied_areas = []  # Track all occupied rectangular areas
    
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
        """Position tables using clustering algorithm to minimize connection lengths"""
        print("Arranging tables using clustering algorithm...")
        
        # Calculate table dimensions first
        for table in self.tables.values():
            table.calculate_dimensions()
        
        # Calculate total connections for ranking
        self.calculate_table_connections()
        
        # Initial canvas dimensions (will expand as needed)
        self.canvas_width = 2400
        self.canvas_height = 1800
        self.center_x = self.canvas_width // 2
        self.center_y = self.canvas_height // 2
        
        # Use new clustering algorithm
        # For debugging: set max_tables_to_place to limit placement
        max_tables_to_place = 3  # Set to a number like 10, 20, etc. for debugging, or None for all tables
        self.position_tables_with_clustering(max_tables_to_place)
        
        print("Table arrangement completed")
        print(f"Final canvas size: {self.canvas_width}x{self.canvas_height}")
        print(f"Total occupied areas tracked: {len(self.occupied_areas)}")
        
        # Show positioning statistics
        center_area_count = 0
        for table in self.tables.values():
            if (self.center_x - 300 <= table.x + table.width//2 <= self.center_x + 300 and 
                self.center_y - 300 <= table.y + table.height//2 <= self.center_y + 300):
                center_area_count += 1
        
        print(f"Tables positioned in center area (600x600): {center_area_count}")
        print(f"Area tracking with {self.MARGIN}px margin prevents overlaps")
        
        # Verify no overlaps exist in final positions
        overlap_count = self.verify_no_overlaps()
        print(f"Final verification: {overlap_count} overlaps detected (should be 0)")
        
        # Calculate total connection length
        total_length = self.calculate_total_connection_length()
        print(f"Total connection length: {total_length:.2f} pixels")
    
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
        
        # Try multiple radii for better placement with larger initial radius
        for radius in [300, 400, 500, 600, 700, 800, 900]:
            for angle in range(0, 360, 10):  # Check positions every 10 degrees for better coverage
                rad = math.radians(angle)
                test_x = avg_x + radius * math.cos(rad) - table.width // 2
                test_y = avg_y + radius * math.sin(rad) - table.height // 2
            
                # Keep within canvas bounds
                test_x = max(10, min(test_x, canvas_width - table.width - 10))
                test_y = max(10, min(test_y, canvas_height - table.height - 10))
                
                # Check if this area is completely free
                if self.is_area_free(test_x, test_y, table.width, table.height):
                    best_position = (test_x, test_y)
                    break
            
            # If we found a free position, no need to try larger radii
            if best_position:
                break
        
        if best_position:
            table.x, table.y = best_position
        else:
            # Fallback: find any available space
            table.x, table.y = self.find_available_space(table, positioned_tables, canvas_width, canvas_height)
        
        table.is_positioned = True
        
        # Track the occupied area for this table
        self.add_occupied_area(table.x, table.y, table.width, table.height)
        
        # Track the occupied area
        self.add_occupied_area(table.x, table.y, table.width, table.height)
    
    def add_occupied_area(self, x, y, width, height):
        """Add an occupied rectangular area to prevent future overlaps"""
        # Store the area with margin included
        occupied_rect = {
            'x1': x - self.MARGIN,
            'y1': y - self.MARGIN, 
            'x2': x + width + self.MARGIN,
            'y2': y + height + self.MARGIN
        }
        self.occupied_areas.append(occupied_rect)
    
    def is_area_free(self, x, y, width, height):
        """Check if a rectangular area is completely free of overlaps"""
        test_rect = {
            'x1': x - self.MARGIN,
            'y1': y - self.MARGIN,
            'x2': x + width + self.MARGIN, 
            'y2': y + height + self.MARGIN
        }
        
        # Check against all occupied areas
        for occupied in self.occupied_areas:
            # Check if rectangles overlap
            if not (test_rect['x2'] <= occupied['x1'] or  # test is completely to the left
                   test_rect['x1'] >= occupied['x2'] or   # test is completely to the right
                   test_rect['y2'] <= occupied['y1'] or   # test is completely above
                   test_rect['y1'] >= occupied['y2']):    # test is completely below
                return False  # Overlap detected
        
        return True  # No overlaps found
    
    def verify_no_overlaps(self):
        """Verify that no tables actually overlap in their final positions"""
        overlap_count = 0
        tables_list = list(self.tables.values())
        
        for i, table1 in enumerate(tables_list):
            for table2 in tables_list[i+1:]:
                # Check if tables overlap (without margin for actual overlap check)
                if not (table1.x + table1.width <= table2.x or  # table1 is completely to the left
                       table1.x >= table2.x + table2.width or   # table1 is completely to the right
                       table1.y + table1.height <= table2.y or  # table1 is completely above
                       table1.y >= table2.y + table2.height):   # table1 is completely below
                    overlap_count += 1
                    print(f"  OVERLAP: {table1.name} ({table1.x},{table1.y} {table1.width}x{table1.height}) overlaps {table2.name} ({table2.x},{table2.y} {table2.width}x{table2.height})")
        
        return overlap_count
    
    def position_tables_with_clustering(self, max_tables_to_place=None):
        """Position tables using the clustering algorithm to minimize connection lengths"""
        # Global cutoff variable
        cutoff = 2
        
        # Debug parameter to limit number of tables placed
        if max_tables_to_place is None:
            max_tables_to_place = len(self.tables)  # Place all tables by default
        
        # Create table_of_tables with required columns
        table_of_tables = []
        table_id_map = {}  # table_name -> ID mapping
        id_table_map = {}  # ID -> table mapping
        
        # Build table_of_tables
        for table_id, (table_name, table) in enumerate(self.tables.items(), 1):
            table_id_map[table_name] = table_id
            id_table_map[table_id] = table
            
            # Get connected table names
            connected_names = table.get_connected_tables()
            
            table_entry = {
                'ID': table_id,
                'table_name': table_name,
                'table_obj': table,
                'connections_num': table.connections,
                'connected_names': connected_names,
                'connections_str': '',  # Will be filled after all IDs are assigned
                'is_placed': False
            }
            table_of_tables.append(table_entry)
        
        # Fill connections_str with IDs
        for entry in table_of_tables:
            connected_ids = []
            for connected_name in entry['connected_names']:
                if connected_name in table_id_map:
                    connected_ids.append(str(table_id_map[connected_name]))
            entry['connections_str'] = '_'.join(connected_ids) if connected_ids else ''
        
        # Sort by connections_num descending, then by table name for consistency
        table_of_tables.sort(key=lambda x: (x['connections_num'], x['table_name']), reverse=True)
        
        print(f"Created table_of_tables with {len(table_of_tables)} entries")
        print(f"Using cutoff value: {cutoff}")
        
        # Run clustering iterations
        iteration = 0
        tables_placed = 0
        while any(not entry['is_placed'] for entry in table_of_tables) and tables_placed < max_tables_to_place:
            iteration += 1
            print(f"Clustering iteration {iteration}")
            
            # Find first unplaced table (highest connections_num among unplaced)
            temp_top_el = None
            for entry in table_of_tables:
                if not entry['is_placed']:
                    temp_top_el = entry
                    break
            
            if not temp_top_el:
                break
                
            # Place the top element at next free position
            table_obj = temp_top_el['table_obj']
            x, y = self.get_next_free_position_near_center(table_obj.width, table_obj.height)
            table_obj.x = x
            table_obj.y = y
            table_obj.is_positioned = True
            temp_top_el['is_placed'] = True
            self.add_occupied_area(x, y, table_obj.width, table_obj.height)
            tables_placed += 1
            
            print(f"  Placed {temp_top_el['table_name']} at ({x}, {y}) with {temp_top_el['connections_num']} connections")
            
            # Check if we've reached the limit
            if tables_placed >= max_tables_to_place:
                print(f"  Reached limit of {max_tables_to_place} tables placed. Stopping.")
                break
            
            # Build temp_block from connections with connections_num <= cutoff
            temp_block = []
            if temp_top_el['connections_str']:
                connected_ids = temp_top_el['connections_str'].split('_')
                for connected_id_str in connected_ids:
                    if connected_id_str.isdigit():
                        connected_id = int(connected_id_str)
                        if connected_id in id_table_map:
                            # Find the entry in table_of_tables
                            for entry in table_of_tables:
                                if entry['ID'] == connected_id and not entry['is_placed']:
                                    if entry['connections_num'] <= cutoff:
                                        temp_block.append(entry)
                                    break
            
            # Sort temp_block by connections_num descending, then by table name for consistency
            temp_block.sort(key=lambda x: (x['connections_num'], x['table_name']), reverse=True)
            
            # Place elements from temp_block (pop last = minimum connections_num)
            while temp_block and tables_placed < max_tables_to_place:
                temp_single_el = temp_block.pop()  # Remove last (minimum connections)
                
                # Place as close as possible to temp_top_el
                single_table_obj = temp_single_el['table_obj']
                x, y = self.get_next_free_position_near_table(temp_top_el['table_obj'], single_table_obj.width, single_table_obj.height)
                single_table_obj.x = x
                single_table_obj.y = y
                single_table_obj.is_positioned = True
                temp_single_el['is_placed'] = True
                self.add_occupied_area(x, y, single_table_obj.width, single_table_obj.height)
                tables_placed += 1
                
                print(f"    Placed connected {temp_single_el['table_name']} at ({x}, {y}) near {temp_top_el['table_name']}")
                
                # Check if we've reached the limit
                if tables_placed >= max_tables_to_place:
                    print(f"    Reached limit of {max_tables_to_place} tables placed. Stopping.")
                    break
    
    def get_next_free_position_near_center(self, table_width=200, table_height=150):
        """Find next free position as close as possible to canvas center"""
        center_x, center_y = self.center_x, self.center_y
        
        # Try positions in expanding rings around center
        for radius in range(0, max(self.canvas_width, self.canvas_height), 40):
            for angle in range(0, 360, 12):
                rad = math.radians(angle)
                test_x = int(center_x + radius * math.cos(rad))
                test_y = int(center_y + radius * math.sin(rad))
                
                # Check if position is within canvas and free
                if (10 <= test_x <= self.canvas_width - table_width - 10 and 
                    10 <= test_y <= self.canvas_height - table_height - 10):
                    if self.is_area_free(test_x, test_y, table_width, table_height):
                        return test_x, test_y
        
        # If no space found, expand canvas and return edge position
        self.canvas_width += 400
        return self.canvas_width - table_width - 20, center_y
    
    def get_next_free_position_near_table(self, anchor_table, table_width=200, table_height=150):
        """Find next free position as close as possible to anchor table"""
        anchor_x = anchor_table.x + anchor_table.width // 2
        anchor_y = anchor_table.y + anchor_table.height // 2
        
        # Try positions in expanding rings around anchor table
        for radius in range(60, max(self.canvas_width, self.canvas_height), 25):
            for angle in range(0, 360, 8):
                rad = math.radians(angle)
                test_x = int(anchor_x + radius * math.cos(rad))
                test_y = int(anchor_y + radius * math.sin(rad))
                
                # Check if position is within canvas and free
                if (10 <= test_x <= self.canvas_width - table_width - 10 and 
                    10 <= test_y <= self.canvas_height - table_height - 10):
                    if self.is_area_free(test_x, test_y, table_width, table_height):
                        return test_x, test_y
        
        # If no space found, expand canvas and place at edge
        if anchor_x > self.canvas_width // 2:
            self.canvas_width += 400
            return self.canvas_width - table_width - 20, anchor_y
        else:
            self.canvas_height += 300
            return anchor_x, self.canvas_height - table_height - 20
    
    def calculate_total_connection_length(self):
        """Calculate total pythagorean distance of all connections"""
        total_length = 0
        processed_pairs = set()
        
        for table in self.tables.values():
            table_center_x = table.x + table.width // 2
            table_center_y = table.y + table.height // 2
            
            connected_names = table.get_connected_tables()
            for connected_name in connected_names:
                if connected_name in self.tables:
                    # Avoid double counting connections
                    pair = tuple(sorted([table.name, connected_name]))
                    if pair not in processed_pairs:
                        processed_pairs.add(pair)
                        
                        other_table = self.tables[connected_name]
                        other_center_x = other_table.x + other_table.width // 2
                        other_center_y = other_table.y + other_table.height // 2
                        
                        # Calculate pythagorean distance
                        x_diff = table_center_x - other_center_x
                        y_diff = table_center_y - other_center_y
                        distance = math.sqrt(x_diff**2 + y_diff**2)
                        total_length += distance
        
        return total_length
    
    def calculate_overlap_score(self, x, y, width, height, positioned_tables):
        """Calculate overlap score for a potential table position"""
        overlap_score = 0
        margin = 120  # Very large minimum distance between tables to ensure no overlaps
        
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
        """Find any available space for a table, expanding canvas if needed"""
        grid_size = 80  # Grid size for search
        
        # First try a systematic search across the current canvas
        for y in range(10, canvas_height - table.height, grid_size):
            for x in range(10, canvas_width - table.width, grid_size):
                if self.is_area_free(x, y, table.width, table.height):
                    return x, y
        
        # If no space found, expand the canvas area and try extended regions
        # Try extending to the right
        extended_width = canvas_width + 800
        for y in range(10, canvas_height - table.height, grid_size):
            for x in range(canvas_width, extended_width - table.width, grid_size):
                if self.is_area_free(x, y, table.width, table.height):
                    self.canvas_width = extended_width  # Update canvas size
                    return x, y
        
        # Try extending downward
        extended_height = canvas_height + 600
        for y in range(canvas_height, extended_height - table.height, grid_size):
            for x in range(10, canvas_width - table.width, grid_size):
                if self.is_area_free(x, y, table.width, table.height):
                    self.canvas_height = extended_height  # Update canvas size
                    return x, y
        
        # Try extending both directions
        for y in range(canvas_height, extended_height - table.height, grid_size):
            for x in range(canvas_width, extended_width - table.width, grid_size):
                if self.is_area_free(x, y, table.width, table.height):
                    self.canvas_width = extended_width
                    self.canvas_height = extended_height
                    return x, y
        
        # Final fallback: place at expanded edge with unique position
        fallback_x = extended_width - table.width - 20
        fallback_y = extended_height - table.height - 20 - len(positioned_tables) * 30  # Offset each table
        self.canvas_width = extended_width
        self.canvas_height = extended_height
        return fallback_x, fallback_y
    
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
    output_dir = os.path.join(base_dir, '0-db-schema', '3-output')
    output_file = os.path.join(output_dir, 'output-schema.xml')
    
    # Check if input files exist
    if not os.path.exists(primary_keys_file):
        print(f"Error: Primary keys file not found: {primary_keys_file}")
        return
    
    if not os.path.exists(foreign_keys_file):
        print(f"Error: Foreign keys file not found: {foreign_keys_file}")
        return
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    
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