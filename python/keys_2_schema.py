#!/usr/bin/env python3
"""
keys_2_schema.py

This script reads primary and foreign key information from CSV files 
and generates an XML schema file compatible with WWW SQL Designer.

Input file        max_table        max_tables_to_place = 20  # Set to a number like 10, 20, etc. for debugging, or None for all tables_to_place = 3  # Set to a number like 10, 20, etc. for debugging, or None for all tables:
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
        """Calculate bi-directional connection counts for all tables"""
        # Reset all connection counts
        for table in self.tables.values():
            table.connections = 0
            table.incoming_connections = 0
            table.outgoing_connections = 0
        
        # Count outgoing connections (foreign keys from this table)
        for table_name, table in self.tables.items():
            connected_tables = table.get_connected_tables()
            table.outgoing_connections = len(connected_tables)
        
        # Count incoming connections (foreign keys pointing to this table)
        for table_name, table in self.tables.items():
            incoming_count = 0
            for other_table_name, other_table in self.tables.items():
                if other_table_name != table_name:
                    connected_tables = other_table.get_connected_tables()
                    incoming_count += connected_tables.count(table_name)
            table.incoming_connections = incoming_count
        
        # Calculate total bi-directional connections
        for table in self.tables.values():
            table.connections = table.incoming_connections + table.outgoing_connections
        
        print(f"Calculated bi-directional connections for all tables")
        # Debug: show connection counts for top connected tables
        sorted_by_connections = sorted(self.tables.items(), key=lambda x: x[1].connections, reverse=True)
        for table_name, table in sorted_by_connections[:5]:
            print(f"  {table_name}: {table.connections} total ({table.incoming_connections} in, {table.outgoing_connections} out)")
    
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
        max_tables_to_place = None  # Set to a number like 10, 20, etc. for debugging, or None for all tables
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
        
        # Step 0: Place all orphan tables (no connections) at bottom right corner first
        tables_placed = self.place_orphan_tables(table_of_tables, max_tables_to_place)
        
        # Enhanced bi-directional clustering algorithm
        flag_table = None  # The FLAG table (most connected table)
        
        # Step 1: Place the FLAG table (table with most connections)
        if table_of_tables and tables_placed < max_tables_to_place:
            flag_table = table_of_tables[0]  # Already sorted by connections descending
            table_obj = flag_table['table_obj']
            x, y = self.get_next_free_position_near_center(table_obj.width, table_obj.height)
            table_obj.x = x
            table_obj.y = y
            table_obj.is_positioned = True
            flag_table['is_placed'] = True
            self.add_occupied_area(x, y, table_obj.width, table_obj.height)
            tables_placed += 1
            
            print(f"FLAG: Placed {flag_table['table_name']} at ({x}, {y}) with {flag_table['connections_num']} total connections")
            
            # Place all direct children of FLAG table
            tables_placed = self.place_direct_children(flag_table, table_of_tables, tables_placed, max_tables_to_place)
        
        # Step 2: Continue placement based on connections to FLAG table
        while tables_placed < max_tables_to_place and flag_table:
            # Find unplaced table with most connections to the FLAG table
            best_candidate = None
            best_connection_count = 0
            
            for entry in table_of_tables:
                if not entry['is_placed']:
                    # Count connections to FLAG table (bi-directional)
                    connections_to_flag = 0
                    
                    # Check if this table connects to FLAG table (outgoing)
                    if flag_table['table_name'] in entry['connected_names']:
                        connections_to_flag += entry['connected_names'].count(flag_table['table_name'])
                    
                    # Check if FLAG table connects to this table (incoming)
                    if entry['table_name'] in flag_table['connected_names']:
                        connections_to_flag += flag_table['connected_names'].count(entry['table_name'])
                    
                    # Use total connections as tiebreaker
                    if (connections_to_flag > best_connection_count or 
                        (connections_to_flag == best_connection_count and 
                         (best_candidate is None or entry['connections_num'] > best_candidate['connections_num']))):
                        best_candidate = entry
                        best_connection_count = connections_to_flag
            
            # If no table has connections to FLAG, find table with most total connections
            if best_candidate is None or best_connection_count == 0:
                for entry in table_of_tables:
                    if not entry['is_placed']:
                        if best_candidate is None or entry['connections_num'] > best_candidate['connections_num']:
                            best_candidate = entry
            
            # If we found a candidate, place it
            if best_candidate:
                table_obj = best_candidate['table_obj']
                
                # If connected to FLAG, place near FLAG; otherwise place at next center position
                if best_connection_count > 0:
                    x, y = self.get_next_free_position_near_table(flag_table['table_obj'], table_obj.width, table_obj.height)
                    print(f"  Placed {best_candidate['table_name']} at ({x}, {y}) near FLAG ({best_connection_count} connections to FLAG)")
                else:
                    x, y = self.get_next_free_position_near_center(table_obj.width, table_obj.height)
                    print(f"  Placed {best_candidate['table_name']} at ({x}, {y}) with {best_candidate['connections_num']} total connections (new cluster)")
                
                table_obj.x = x
                table_obj.y = y
                table_obj.is_positioned = True
                best_candidate['is_placed'] = True
                self.add_occupied_area(x, y, table_obj.width, table_obj.height)
                tables_placed += 1
                
                # Place all direct children of this newly placed table
                tables_placed = self.place_direct_children(best_candidate, table_of_tables, tables_placed, max_tables_to_place)
            else:
                # No more unplaced tables
                break
        
        print(f"Enhanced bi-directional clustering completed. Placed {tables_placed} tables.")
    
    def place_orphan_tables(self, table_of_tables, max_tables_to_place):
        """Place all orphan tables (no connections) at the bottom right corner of the canvas"""
        orphan_tables = []
        
        # Find all tables with no connections
        for entry in table_of_tables:
            if entry['connections_num'] == 0:
                orphan_tables.append(entry)

        if not orphan_tables:
            print("No orphan tables found")
            return 0

        print(f"Placing {len(orphan_tables)} orphan tables at bottom right corner")
        
        # Sort orphans by name for consistent placement
        orphan_tables.sort(key=lambda x: x['table_name'])
        
        # Calculate actual dimensions for all orphan tables
        for orphan_entry in orphan_tables:
            orphan_entry['table_obj'].calculate_dimensions()
        
        # Calculate required space using actual table dimensions
        max_width = max(table['table_obj'].width for table in orphan_tables)
        max_height = max(table['table_obj'].height for table in orphan_tables)
        
        # Estimate area needed - place in rows of up to 4 tables
        tables_per_row = 4
        num_rows = (len(orphan_tables) + tables_per_row - 1) // tables_per_row
        orphan_area_width = (max_width + self.MARGIN) * tables_per_row + self.MARGIN
        orphan_area_height = (max_height + self.MARGIN) * num_rows + self.MARGIN
        
        # Ensure canvas is large enough for orphans at the edge
        min_canvas_width = self.canvas_width + orphan_area_width
        min_canvas_height = self.canvas_height + orphan_area_height
        
        if self.canvas_width < min_canvas_width:
            self.canvas_width = min_canvas_width
        if self.canvas_height < min_canvas_height:
            self.canvas_height = min_canvas_height
        
        # Organize orphans into rows for proper placement
        orphan_rows = []
        for i in range(0, len(orphan_tables), tables_per_row):
            row_tables = orphan_tables[i:i+tables_per_row]
            orphan_rows.append(row_tables)
        
        # Calculate dimensions for each row
        row_info = []
        for row_tables in orphan_rows:
            row_width = sum(table['table_obj'].width for table in row_tables) + (len(row_tables) - 1) * self.MARGIN
            row_height = max(table['table_obj'].height for table in row_tables)
            row_info.append({'tables': row_tables, 'width': row_width, 'height': row_height})
        
        # Start from bottom-right corner of canvas
        current_y = self.canvas_height - self.MARGIN
        tables_placed = 0
        
        # Place rows from bottom to top
        for row_idx in reversed(range(len(row_info))):
            row = row_info[row_idx]
            
            # Position this row's Y coordinate
            current_y -= row['height']
            
            # Start X position from the right edge of canvas
            current_x = self.canvas_width - self.MARGIN
            
            # Place tables in this row from right to left
            for table_idx in reversed(range(len(row['tables']))):
                if tables_placed >= max_tables_to_place:
                    break
                    
                orphan_entry = row['tables'][table_idx]
                table_obj = orphan_entry['table_obj']
                
                # Position table at right edge minus its width
                current_x -= table_obj.width
                
                # Place the orphan table
                table_obj.x = current_x
                table_obj.y = current_y
                table_obj.is_positioned = True
                orphan_entry['is_placed'] = True
                self.add_occupied_area(current_x, current_y, table_obj.width, table_obj.height)
                tables_placed += 1
                
                print(f"  Placed orphan {orphan_entry['table_name']} at ({current_x}, {current_y}) size ({table_obj.width}x{table_obj.height})")
                
                # Move to next position (leftward)
                current_x -= self.MARGIN
            
            # Move up for next row
            if row_idx > 0:  # Don't add margin after the top row
                current_y -= self.MARGIN
        
        print(f"Successfully placed {tables_placed} orphan tables")
        return tables_placed
    
    def place_direct_children(self, parent_entry, table_of_tables, tables_placed, max_tables_to_place):
        """Place tables that are only connected to the parent table (direct children) in touching stacks"""
        parent_name = parent_entry['table_name']
        parent_table = parent_entry['table_obj']
        
        # Find all unplaced tables that have only 1 connection and are connected to this parent
        direct_children = []
        for entry in table_of_tables:
            if (not entry['is_placed'] and 
                entry['connections_num'] == 1 and 
                parent_name in entry['connected_names']):
                direct_children.append(entry)
        
        if not direct_children:
            return tables_placed
        
        # Limit children to fit within max_tables_to_place
        remaining_slots = max_tables_to_place - tables_placed
        if len(direct_children) > remaining_slots:
            direct_children = direct_children[:remaining_slots]
        
        # Sort direct children by table name for consistent placement order
        direct_children.sort(key=lambda x: x['table_name'])
        
        print(f"      Placing {len(direct_children)} direct children of {parent_name} in touching stack")
        
        # Calculate total stack dimensions
        total_stack_width = max(parent_table.width, max(child['table_obj'].width for child in direct_children))
        total_stack_height = parent_table.height + sum(child['table_obj'].height for child in direct_children)
        
        print(f"        Total stack dimensions: {total_stack_width}x{total_stack_height}")
        
        # Find a free location that can accommodate the entire stack
        stack_x, stack_y = self.find_free_location_for_stack(total_stack_width, total_stack_height)
        
        # Move parent to the stack location (top of stack)
        print(f"        Moving parent {parent_name} to stack position ({stack_x}, {stack_y})")
        old_x, old_y = parent_table.x, parent_table.y
        parent_table.x = stack_x
        parent_table.y = stack_y
        
        # Add the parent's new occupied area (the old area is already in occupied_areas but that's okay for now)
        self.add_occupied_area(stack_x, stack_y, parent_table.width, parent_table.height)
        
        # Place children directly below parent in touching formation
        current_y = stack_y + parent_table.height
        
        for child_entry in direct_children:
            child_table_obj = child_entry['table_obj']
            
            # Position child directly below previous table, left-aligned with stack
            child_x = stack_x
            child_y = current_y
            
            # Place the child
            child_table_obj.x = child_x
            child_table_obj.y = child_y
            child_table_obj.is_positioned = True
            child_entry['is_placed'] = True
            self.add_occupied_area(child_x, child_y, child_table_obj.width, child_table_obj.height)
            tables_placed += 1
            
            print(f"        Stacked {child_entry['table_name']} at ({child_x}, {child_y}) below parent")
            
            # Update position for next child
            current_y += child_table_obj.height
        
        print(f"        Successfully stacked all {len(direct_children)} children in touching formation")        
        return tables_placed
    
    def find_free_location_for_stack(self, stack_width, stack_height):
        """Find a free location that can accommodate the entire stack"""
        print(f"        Searching for free location for {stack_width}x{stack_height} stack")
        
        # Try positions starting from center and expanding outward
        center_x, center_y = self.center_x, self.center_y
        
        # Try positions in expanding rings around center with fine granularity
        for radius in range(0, max(self.canvas_width, self.canvas_height), 25):  # Smaller radius steps
            for angle in range(0, 360, 4):  # Much finer angle steps (90 positions per ring)
                rad = math.radians(angle)
                test_x = int(center_x + radius * math.cos(rad))
                test_y = int(center_y + radius * math.sin(rad))
                
                # Ensure the stack fits within canvas bounds
                if (10 <= test_x <= self.canvas_width - stack_width - 10 and 
                    10 <= test_y <= self.canvas_height - stack_height - 10):
                    
                    # Check if this entire area is free
                    if self.is_area_free(test_x, test_y, stack_width, stack_height):
                        print(f"        Found free location at ({test_x}, {test_y})")
                        return test_x, test_y
        
        # If no space found in current canvas, expand and place at edge
        print(f"        No free location found, expanding canvas")
        self.canvas_width += 600
        new_x = self.canvas_width - stack_width - 20
        new_y = center_y
        
        # Ensure new position fits in expanded canvas
        if new_y + stack_height > self.canvas_height:
            self.canvas_height = new_y + stack_height + 50
            
        return new_x, new_y
    
    def update_occupied_area_for_table(self, table):
        """Update the occupied area for a table that has been moved"""
        # Remove old occupied areas for this table (this is simplified - in practice we'd need to track which areas belong to which table)
        # For now, just add the new area since we're only moving within the same placement cycle
        pass  # The occupied area will be updated when we call add_occupied_area
    
    def find_best_stack_direction(self, parent_table):
        """Determine the best direction (top or bottom) to stack direct children"""
        # Check available space below the parent
        space_below = self.canvas_height - (parent_table.y + parent_table.height)
        
        # Check available space above the parent
        space_above = parent_table.y
        
        # Check for existing occupied areas with a reasonable test size
        test_height = min(150, max(space_below, space_above))  # Use standard table height or available space
        
        below_clear = (space_below > 20 and 
                      self.is_area_free(parent_table.x, parent_table.y + parent_table.height, 
                                       max(200, parent_table.width), test_height))
        
        above_clear = (space_above > 20 and 
                      self.is_area_free(parent_table.x, parent_table.y - test_height, 
                                       max(200, parent_table.width), test_height))
        
        # Debug output
        print(f"        Stack direction analysis: below_clear={below_clear} (space={space_below}), above_clear={above_clear} (space={space_above})")
        
        # Prefer direction with more clear space
        if below_clear and (not above_clear or space_below >= space_above):
            return 'bottom'
        elif above_clear:
            return 'top'
        else:
            return 'bottom'  # Default to bottom and let individual placement handle conflicts
    
    def get_next_free_position_near_center(self, table_width=200, table_height=150):
        """Find next free position as close as possible to canvas center"""
        center_x, center_y = self.center_x, self.center_y
        
        # Try positions in expanding rings around center with fine granularity
        for radius in range(0, max(self.canvas_width, self.canvas_height), 20):  # Smaller radius steps
            for angle in range(0, 360, 3):  # Much finer angle steps (120 positions per ring)
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
        
        # Try positions in expanding rings around anchor table with fine granularity
        for radius in range(60, max(self.canvas_width, self.canvas_height), 15):  # Smaller radius steps
            for angle in range(0, 360, 2):  # Much finer angle steps (180 positions per ring)
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