import sqlite3
import json
import os
import threading
import logging
from datetime import datetime

class Database:
    def __init__(self, db_path):
        """Initialize database connection and create tables if they don't exist."""
        self.db_path = db_path
        self.conn = None
        self.lock = threading.RLock()  # Reentrant lock for thread safety
        self.connect()
        self.create_tables()
    
    def connect(self):
        """Connect to the SQLite database."""
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
        except sqlite3.Error as e:
            logging.error(f"Database connection error: {e}")
            raise
    
    def create_tables(self):
        """Create necessary tables if they don't exist."""
        with self.lock:
            try:
                cursor = self.conn.cursor()
                # Create unmatched_items table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS unmatched_items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        rating_key TEXT,
                        grandparent_rating_key TEXT,
                        media_type TEXT,
                        title TEXT,
                        year TEXT,
                        grandparent_title TEXT,
                        parent_title TEXT,
                        guid TEXT,
                        date INTEGER,
                        watched_date TEXT,
                        ignored INTEGER DEFAULT 0,
                        fixed INTEGER DEFAULT 0,
                        fix_date INTEGER,
                        fix_details TEXT,
                        json_data TEXT,
                        scan_id INTEGER
                    )
                ''')
                
                # Create scans table to track scan history
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS scans (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        scan_date INTEGER,
                        total_items INTEGER,
                        unmatched_items INTEGER,
                        fixed_items INTEGER
                    )
                ''')
                
                # Create settings table to store application settings
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS settings (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        updated_at INTEGER
                    )
                ''')
                
                cursor.close()
                self.conn.commit()
            except sqlite3.Error as e:
                logging.error(f"Error creating tables: {e}")
                self.conn.rollback()
                raise
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
    
    def add_scan(self, total_items, unmatched_items):
        """Add a new scan record and return its ID."""
        with self.lock:
            try:
                cursor = self.conn.cursor()
                now = int(datetime.now().timestamp())
                cursor.execute(
                    "INSERT INTO scans (scan_date, total_items, unmatched_items, fixed_items) VALUES (?, ?, ?, ?)",
                    (now, total_items, unmatched_items, 0)
                )
                lastrowid = cursor.lastrowid
                cursor.close()
                self.conn.commit()
                return lastrowid
            except sqlite3.Error as e:
                logging.error(f"Error adding scan: {e}")
                self.conn.rollback()
                return None
    
    def update_scan_fixed_count(self, scan_id, fixed_count):
        """Update the fixed items count for a scan."""
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute(
                    "UPDATE scans SET fixed_items = ? WHERE id = ?",
                    (fixed_count, scan_id)
                )
                cursor.close()
                self.conn.commit()
            except sqlite3.Error as e:
                logging.error(f"Error updating scan: {e}")
                self.conn.rollback()
    
    def add_unmatched_item(self, item, scan_id):
        """Add an unmatched item to the database."""
        with self.lock:
            try:
                # Convert the item to JSON for storage
                json_data = json.dumps(item)
                
                # Extract relevant fields
                rating_key = item.get('rating_key', '')
                grandparent_rating_key = item.get('grandparent_rating_key', '')
                media_type = item.get('media_type', '')
                title = item.get('title', '')
                year = item.get('year', '')
                grandparent_title = item.get('grandparent_title', '')
                parent_title = item.get('parent_title', '')
                guid = item.get('guid', '')
                date = item.get('date', 0)
                watched_date = datetime.fromtimestamp(int(date)).strftime('%Y-%m-%d %H:%M:%S') if date else ''
                
                cursor = self.conn.cursor()
                cursor.execute('''
                    INSERT INTO unmatched_items 
                    (rating_key, grandparent_rating_key, media_type, title, year, grandparent_title, 
                    parent_title, guid, date, watched_date, json_data, scan_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (rating_key, grandparent_rating_key, media_type, title, year, grandparent_title, 
                      parent_title, guid, date, watched_date, json_data, scan_id))
                
                lastrowid = cursor.lastrowid
                cursor.close()
                self.conn.commit()
                return lastrowid
            except sqlite3.Error as e:
                logging.error(f"Error adding unmatched item: {e}")
                self.conn.rollback()
                return None
    
    def get_unmatched_items(self, page=None, per_page=None, include_ignored=False, include_fixed=False, media_type=None, sort_by=None, sort_order='asc', group_items=False):
        """Get unmatched items from the database.
        
        Args:
            page (int, optional): Page number for pagination (1-indexed)
            per_page (int, optional): Number of items per page
            include_ignored (bool): Whether to include ignored items
            include_fixed (bool): Whether to include fixed items
            media_type (str, optional): Filter by media type ('movie' or 'episode')
            sort_by (str, optional): Column to sort by ('type', 'title', 'details', 'watched_date')
            sort_order (str, optional): Sort order ('asc' or 'desc')
            group_items (bool, optional): Whether to group similar items together
            
        Returns:
            If page and per_page are provided: tuple of (items, total_count)
            Otherwise: list of items
        """
        with self.lock:
            try:
                # Build the base query
                base_query = "SELECT * FROM unmatched_items WHERE 1=1"
                count_query = "SELECT COUNT(*) as count FROM unmatched_items WHERE 1=1"
                
                if not include_ignored:
                    base_query += " AND ignored = 0"
                    count_query += " AND ignored = 0"
                    
                if not include_fixed:
                    base_query += " AND fixed = 0"
                    count_query += " AND fixed = 0"
                
                # Add media_type filter if specified
                if media_type in ['movie', 'episode']:
                    filter_clause = f" AND media_type = '{media_type}'"
                    base_query += filter_clause
                    count_query += filter_clause
                
                # Add sorting
                if sort_by == 'type':
                    base_query += f" ORDER BY media_type {sort_order.upper()}"
                elif sort_by == 'title':
                    # Sort by title or grandparent_title if available
                    base_query += f" ORDER BY CASE WHEN grandparent_title != '' THEN grandparent_title ELSE title END {sort_order.upper()}"
                elif sort_by == 'details':
                    # Sort by parent_title + title for episodes, or title for movies
                    base_query += f" ORDER BY CASE WHEN parent_title != '' THEN parent_title ELSE title END {sort_order.upper()}, title {sort_order.upper()}"
                elif sort_by == 'watched_date':
                    base_query += f" ORDER BY date {sort_order.upper()}"
                else:
                    # Default sort by date descending
                    base_query += " ORDER BY date DESC"
                
                # If pagination is requested
                if page is not None and per_page is not None:
                    cursor = self.conn.cursor()
                    # Get total count
                    cursor.execute(count_query)
                    total = cursor.fetchone()['count']
                    
                    # Calculate offset
                    offset = (page - 1) * per_page
                    
                    # Add pagination
                    base_query += f" LIMIT {per_page} OFFSET {offset}"
                    
                    # Execute query
                    cursor.execute(base_query)
                    items = cursor.fetchall()
                    cursor.close()
                    
                    # Group similar items if requested
                    if group_items:
                        items = self.group_similar_items(items)
                    
                    return items, total
                else:
                    # No pagination, return all items
                    cursor = self.conn.cursor()
                    cursor.execute(base_query)
                    items = cursor.fetchall()
                    cursor.close()
                    
                    # Group similar items if requested
                    if group_items:
                        return self.group_similar_items(items)
                    else:
                        return items
            except sqlite3.Error as e:
                logging.error(f"Error getting unmatched items: {e}")
                if page is not None and per_page is not None:
                    return [], 0
                else:
                    return []
                
    def get_setting(self, key, default=None):
        """Get a setting value from the database.
        
        Args:
            key (str): The setting key
            default: The default value to return if the setting doesn't exist
            
        Returns:
            The setting value or default if not found
        """
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
                result = cursor.fetchone()
                cursor.close()
                if result:
                    return result['value']
                return default
            except sqlite3.Error as e:
                logging.error(f"Error getting setting {key}: {e}")
                return default
    
    def set_setting(self, key, value):
        """Set a setting value in the database.
        
        Args:
            key (str): The setting key
            value: The setting value (will be converted to string)
            
        Returns:
            bool: True if successful, False otherwise
        """
        with self.lock:
            try:
                cursor = self.conn.cursor()
                now = int(datetime.now().timestamp())
                cursor.execute(
                    "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
                    (key, str(value), now)
                )
                cursor.close()
                self.conn.commit()
                return True
            except sqlite3.Error as e:
                logging.error(f"Error setting {key}: {e}")
                self.conn.rollback()
                return False
    
    def get_all_settings(self):
        """Get all settings as a dictionary.
        
        Returns:
            dict: Dictionary of all settings
        """
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute("SELECT key, value FROM settings")
                settings = cursor.fetchall()
                cursor.close()
                return {item['key']: item['value'] for item in settings}
            except sqlite3.Error as e:
                logging.error(f"Error getting all settings: {e}")
                return {}
    
    def delete_setting(self, key):
        """Delete a setting from the database.
        
        Args:
            key (str): The setting key to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute("DELETE FROM settings WHERE key = ?", (key,))
                cursor.close()
                self.conn.commit()
                return True
            except sqlite3.Error as e:
                logging.error(f"Error deleting setting {key}: {e}")
                self.conn.rollback()
                return False
    
    def group_similar_items(self, items):
        """Group similar items together based on title.
        
        For movies: Group by title + year
        For episodes: Group by grandparent_title (show name)
        
        Only groups items if there are 2 or more with the same key.
        
        Returns:
            A list of dictionaries, with groups for duplicates and individual items for singles
        """
        # First convert all SQLite Row objects to dictionaries
        dict_items = [dict(item) for item in items]
        
        # First pass: count occurrences of each group key
        key_counts = {}
        for item_dict in dict_items:
            # Create a group key based on media type
            if item_dict['media_type'] == 'movie':
                # For movies, group by title + year
                group_key = f"{item_dict['title']} ({item_dict['year']})"
            elif item_dict['media_type'] == 'episode':
                # For episodes, group by show name (grandparent_title)
                group_key = item_dict['grandparent_title']
            else:
                # For other types, use the id as key (no grouping)
                group_key = f"other_{item_dict['id']}"
            
            # Count occurrences
            key_counts[group_key] = key_counts.get(group_key, 0) + 1
        
        # Second pass: create groups only for items with 2+ occurrences
        result = []
        grouped_keys = set()
        groups = {}
        
        for item_dict in dict_items:
            # Create the same group key as before
            if item_dict['media_type'] == 'movie':
                group_key = f"{item_dict['title']} ({item_dict['year']})"
            elif item_dict['media_type'] == 'episode':
                group_key = item_dict['grandparent_title']
            else:
                group_key = f"other_{item_dict['id']}"
            
            # Only group if there are 2 or more occurrences
            if key_counts[group_key] >= 2:
                grouped_keys.add(group_key)
                
                # Add to existing group or create a new one
                if group_key in groups:
                    groups[group_key]['group_items'].append(item_dict)
                    groups[group_key]['count'] += 1
                else:
                    groups[group_key] = {
                        'key': group_key,
                        'media_type': item_dict['media_type'],
                        'title': group_key,  # Use the group key as the title
                        'details': 'Varies',  # Details vary within the group
                        'watched_date': 'Varies',  # Dates vary within the group
                        'count': 1,
                        'group_items': [item_dict],
                        'is_group': True
                    }
            else:
                # Add as individual item (not grouped)
                result.append(item_dict)
        
        # Add all the groups to the result
        result.extend(list(groups.values()))
        
        return result
    
    def get_unmatched_item(self, item_id):
        """Get a specific unmatched item by ID."""
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute("SELECT * FROM unmatched_items WHERE id = ?", (item_id,))
                item = cursor.fetchone()
                cursor.close()
                if item:
                    # Convert to dict and add status based on flags
                    item_dict = dict(item)
                    if item_dict['fixed']:
                        item_dict['status'] = 'fixed'
                    elif item_dict['ignored']:
                        item_dict['status'] = 'ignored'
                    else:
                        item_dict['status'] = 'unmatched'
                    return item_dict
                return None
            except sqlite3.Error as e:
                logging.error(f"Error getting unmatched item: {e}")
                return None

    def get_item_by_id(self, item_id):
        """Alias for get_unmatched_item for compatibility."""
        return self.get_unmatched_item(item_id)

    def update_item_status(self, item_id, status):
        """Update the status of an item.
        
        Args:
            item_id (int): The ID of the item to update
            status (str): The new status ('fixed', 'ignored', or 'unmatched')
            
        Returns:
            bool: True if successful, False otherwise
        """
        with self.lock:
            try:
                cursor = self.conn.cursor()
                now = int(datetime.now().timestamp())
                if status == 'fixed':
                    cursor.execute(
                        "UPDATE unmatched_items SET fixed = 1, ignored = 0, fix_date = ? WHERE id = ?",
                        (now, item_id)
                    )
                elif status == 'ignored':
                    cursor.execute(
                        "UPDATE unmatched_items SET ignored = 1, fixed = 0 WHERE id = ?",
                        (item_id,)
                    )
                elif status == 'unmatched':
                    cursor.execute(
                        "UPDATE unmatched_items SET ignored = 0, fixed = 0 WHERE id = ?",
                        (item_id,)
                    )
                else:
                    logging.error(f"Invalid status: {status}")
                    cursor.close()
                    return False
                
                cursor.close()
                self.conn.commit()
                return True
            except sqlite3.Error as e:
                logging.error(f"Error updating item status: {e}")
                self.conn.rollback()
                return False

    def update_item_rating_key(self, item_id, new_rating_key):
        """Update the rating key of an item.
        
        Args:
            item_id (int): The ID of the item to update
            new_rating_key (str): The new rating key
            
        Returns:
            bool: True if successful, False otherwise
        """
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute(
                    "UPDATE unmatched_items SET rating_key = ? WHERE id = ?",
                    (new_rating_key, item_id)
                )
                cursor.close()
                self.conn.commit()
                return True
            except sqlite3.Error as e:
                logging.error(f"Error updating item rating key: {e}")
                self.conn.rollback()
                return False
    
    def mark_item_fixed(self, item_id, fix_details):
        """Mark an item as fixed with details about the fix."""
        with self.lock:
            try:
                cursor = self.conn.cursor()
                now = int(datetime.now().timestamp())
                cursor.execute(
                    "UPDATE unmatched_items SET fixed = 1, fix_date = ?, fix_details = ? WHERE id = ?",
                    (now, fix_details, item_id)
                )
                cursor.close()
                self.conn.commit()
                return True
            except sqlite3.Error as e:
                logging.error(f"Error marking item as fixed: {e}")
                self.conn.rollback()
                return False
    
    def revert_item_to_unmatched(self, item_id):
        """Revert a fixed item back to unmatched status, restoring original rating key."""
        with self.lock:
            try:
                cursor = self.conn.cursor()
                # First, get the item to access its json_data
                cursor.execute("SELECT json_data FROM unmatched_items WHERE id = ?", (item_id,))
                item_row = cursor.fetchone()
                if not item_row:
                    cursor.close()
                    logging.error(f"Item with ID {item_id} not found for revert.")
                    return False
                
                item_data = json.loads(item_row['json_data'])
                original_rating_key = item_data.get('rating_key')
                if not original_rating_key:
                    cursor.close()
                    logging.error(f"Original rating_key not found in json_data for item {item_id}.")
                    # Fallback or decide how to handle - for now, prevent update if no original key
                    return False

                cursor.execute(
                    """UPDATE unmatched_items SET 
                       fixed = 0, 
                       ignored = 0, 
                       fix_date = NULL, 
                       fix_details = NULL, 
                       rating_key = ? 
                       WHERE id = ?""",
                    (original_rating_key, item_id)
                )
                cursor.close()
                self.conn.commit()
                logging.info(f"Item {item_id} reverted to unmatched with rating_key {original_rating_key}.")
                return True
            except sqlite3.Error as e:
                logging.error(f"Error reverting item {item_id} to unmatched: {e}")
                self.conn.rollback()
                return False
            except json.JSONDecodeError as e:
                logging.error(f"Error decoding json_data for item {item_id}: {e}")
                return False
    
    def mark_items_ignored(self, item_ids, ignored=True):
        """Mark items as ignored or un-ignored."""
        if not item_ids:
            return False
        
        with self.lock:
            try:
                cursor = self.conn.cursor()
                # Convert to int value for SQLite
                ignored_val = 1 if ignored else 0
                
                # Create placeholders for SQL query
                placeholders = ', '.join(['?'] * len(item_ids))
                
                cursor.execute(
                    f"UPDATE unmatched_items SET ignored = ? WHERE id IN ({placeholders})",
                    [ignored_val] + item_ids
                )
                cursor.close()
                self.conn.commit()
                return True
            except sqlite3.Error as e:
                logging.error(f"Error marking items as ignored: {e}")
                self.conn.rollback()
                return False
    
    def get_ignored_items(self, page=1, per_page=50, sort_by=None, sort_order='asc'):
        """Get ignored items with pagination and sorting.
        
        Args:
            page (int): Page number (1-indexed)
            per_page (int): Number of items per page
            sort_by (str, optional): Column to sort by ('type', 'title', 'details', 'watched_date')
            sort_order (str): Sort order ('asc' or 'desc')
            
        Returns:
            tuple: (list of items, total count)
        """
        with self.lock:
            try:
                # Calculate offset
                offset = (page - 1) * per_page
                
                # Build base query
                query = "SELECT * FROM unmatched_items WHERE ignored = 1 AND fixed = 0"
                
                # Add sorting
                if sort_by == 'type':
                    query += f" ORDER BY media_type {sort_order.upper()}"
                elif sort_by == 'title':
                    query += f" ORDER BY CASE WHEN grandparent_title != '' THEN grandparent_title ELSE title END {sort_order.upper()}"
                elif sort_by == 'details':
                    query += f" ORDER BY CASE WHEN parent_title != '' THEN parent_title ELSE title END {sort_order.upper()}, title {sort_order.upper()}"
                elif sort_by == 'watched_date':
                    query += f" ORDER BY date {sort_order.upper()}"
                else:
                    # Default sort by date descending
                    query += " ORDER BY date DESC"
                
                # Add pagination
                query += " LIMIT ? OFFSET ?"
                
                cursor = self.conn.cursor()
                # Get total count
                cursor.execute("SELECT COUNT(*) as count FROM unmatched_items WHERE ignored = 1 AND fixed = 0")
                total = cursor.fetchone()['count']
                
                # Get items for current page
                cursor.execute(query, (per_page, offset))
                items = cursor.fetchall()
                cursor.close()
                
                return items, total
            except sqlite3.Error as e:
                logging.error(f"Error getting ignored items: {e}")
                return [], 0
    
    def get_fixed_items(self, page=1, per_page=50, sort_by=None, sort_order='asc'):
        """Get fixed items with pagination and sorting.
        
        Args:
            page (int): Page number (1-indexed)
            per_page (int): Number of items per page
            sort_by (str, optional): Column to sort by ('type', 'title', 'details', 'watched_date', 'fix_date')
            sort_order (str): Sort order ('asc' or 'desc')
            
        Returns:
            tuple: (list of items, total count)
        """
        with self.lock:
            try:
                # Calculate offset
                offset = (page - 1) * per_page
                
                # Build base query
                query = "SELECT * FROM unmatched_items WHERE fixed = 1"
                
                # Add sorting
                if sort_by == 'type':
                    query += f" ORDER BY media_type {sort_order.upper()}"
                elif sort_by == 'title':
                    query += f" ORDER BY CASE WHEN grandparent_title != '' THEN grandparent_title ELSE title END {sort_order.upper()}"
                elif sort_by == 'details':
                    query += f" ORDER BY CASE WHEN parent_title != '' THEN parent_title ELSE title END {sort_order.upper()}, title {sort_order.upper()}"
                elif sort_by == 'watched_date':
                    query += f" ORDER BY date {sort_order.upper()}"
                elif sort_by == 'fix_date':
                    query += f" ORDER BY fix_date {sort_order.upper()}"
                else:
                    # Default sort by fix_date descending
                    query += " ORDER BY fix_date DESC"
                
                # Add pagination
                query += " LIMIT ? OFFSET ?"
                
                cursor = self.conn.cursor()
                # Get total count
                cursor.execute("SELECT COUNT(*) as count FROM unmatched_items WHERE fixed = 1")
                total = cursor.fetchone()['count']
                
                # Get items for current page
                cursor.execute(query, (per_page, offset))
                items = cursor.fetchall()
                cursor.close()
                
                return items, total
            except sqlite3.Error as e:
                logging.error(f"Error getting fixed items: {e}")
                return [], 0
    
    def get_scan_history(self):
        """Get the history of all scans."""
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute("SELECT * FROM scans ORDER BY scan_date DESC")
                scans = cursor.fetchall()
                cursor.close()
                return scans
            except sqlite3.Error as e:
                logging.error(f"Error getting scan history: {e}")
                return []
    
    def get_stats(self):
        """Get statistics about unmatched and fixed items."""
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN fixed = 0 AND ignored = 0 THEN 1 ELSE 0 END) as unmatched,
                        SUM(CASE WHEN fixed = 1 THEN 1 ELSE 0 END) as fixed,
                        SUM(CASE WHEN ignored = 1 THEN 1 ELSE 0 END) as ignored
                    FROM unmatched_items
                """)
                stats = cursor.fetchone()
                cursor.close()
                return stats
            except sqlite3.Error as e:
                logging.error(f"Error getting stats: {e}")
                return None
                
    def item_exists(self, item):
        """Check if an item with the same key information already exists in the database.
        
        Args:
            item (dict): The history item to check
            
        Returns:
            bool: True if the item exists, False otherwise
        """
        with self.lock:
            try:
                # Extract key fields for matching
                rating_key = item.get('rating_key', '')
                media_type = item.get('media_type', '')
                guid = item.get('guid', '')
                
                # For episodes, also check grandparent_rating_key
                if media_type == 'episode':
                    grandparent_rating_key = item.get('grandparent_rating_key', '')
                    query = """SELECT id FROM unmatched_items 
                              WHERE rating_key = ? AND media_type = ? AND guid = ? 
                              AND grandparent_rating_key = ?"""
                    params = (rating_key, media_type, guid, grandparent_rating_key)
                else:
                    query = "SELECT id FROM unmatched_items WHERE rating_key = ? AND media_type = ? AND guid = ?"
                    params = (rating_key, media_type, guid)
                
                cursor = self.conn.cursor()
                cursor.execute(query, params)
                result = cursor.fetchone()
                cursor.close()
                
                return result is not None
            except sqlite3.Error as e:
                logging.error(f"Error checking if item exists: {e}")
                return False
