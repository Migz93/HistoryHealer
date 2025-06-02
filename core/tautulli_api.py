import requests
import logging
from datetime import datetime

class TautulliAPI:
    def __init__(self, base_url, api_key):
        """Initialize the Tautulli API client."""
        self.base_url = base_url
        self.api_key = api_key
        self.api_endpoint = f"{base_url}/api/v2"
        
    def _make_request(self, cmd, params=None):
        """Make a request to the Tautulli API."""
        if params is None:
            params = {}
            
        params['apikey'] = self.api_key
        params['cmd'] = cmd
        
        try:
            response = requests.get(self.api_endpoint, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"API request error: {e}")
            return None
    
    def get_library_media_info(self, section_ids):
        """Get library media info from Tautulli API for specified sections."""
        all_media_items = []
        
        for section_id in section_ids:
            params = {
                'section_id': section_id,
                'length': 100000,
                'refresh': 'true'
            }
            
            data = self._make_request('get_library_media_info', params)
            if data and data.get('response', {}).get('result') == 'success':
                media_items = data.get('response', {}).get('data', {}).get('data', [])
                all_media_items.extend(media_items)
                logging.info(f"Found {len(media_items)} items in section {section_id}")
            else:
                logging.error(f"Failed to get library media info for section {section_id}")
        
        return all_media_items
    
    def get_watch_history(self, start_date, end_date):
        """Get watch history from Tautulli."""
        params = {
            'length': 100000,
            'after': start_date,
            'media_type': 'movie,episode'
        }
        
        # Add before parameter only if end_date is not "current"
        if end_date.lower() != "current":
            params['before'] = end_date
        
        data = self._make_request('get_history', params)
        if data and data.get('response', {}).get('result') == 'success':
            history = data.get('response', {}).get('data', {}).get('data', [])
            logging.info(f"Found {len(history)} history items")
            
            # Filter out items with empty rating keys
            history = [item for item in history if item.get('rating_key')]
            logging.info(f"Found {len(history)} items with rating keys")
            
            return history
        
        return []
    
    def get_metadata(self, rating_key):
        """Get metadata for a specific rating_key from Tautulli API."""
        params = {
            'rating_key': rating_key
        }
        
        return self._make_request('get_metadata', params)
    
    def update_metadata_details(self, old_rating_key, new_rating_key, media_type):
        """Update metadata details for a media item."""
        # Convert episode type to show type since we're working with the show (grandparent)
        update_type = "show" if media_type == "episode" else media_type
        
        params = {
            'old_rating_key': old_rating_key,
            'new_rating_key': new_rating_key,
            'media_type': update_type
        }
        
        data = self._make_request('update_metadata_details', params)
        if data and data.get('response', {}).get('result') == 'success':
            logging.info(f"Successfully updated metadata details for {media_type} (rating key: {old_rating_key})")
            return True
        else:
            logging.error(f"Failed to update metadata details for {media_type} (rating key: {old_rating_key})")
            return False
    
    def search_title(self, title, history_guid, media_type, history_year=None):
        """Search for a title using Tautulli API and match by GUID or year."""
        # Use only the title for search, without year
        params = {
            'query': title,
            'limit': 20  # Increased limit to get more potential matches
        }
        
        logging.info(f"Searching Tautulli for: '{title}'")
        data = self._make_request('search', params)
        if not data or data.get('response', {}).get('result') != 'success':
            logging.warning("Search returned no results or failed")
            return None
        
        results_list = data.get('response', {}).get('data', {}).get('results_list', {})
        
        if media_type == 'movie':
            # Get the movie results list
            movie_results = results_list.get('movie', [])
            if not movie_results:
                logging.warning("No movie results found")
                return None
                
            logging.info(f"Found {len(movie_results)} movie results")
            
            # Filter out results with "HQ" in library_name
            filtered_results = []
            for result in movie_results:
                library_name = result.get('library_name', '')
                if 'HQ' not in library_name:
                    filtered_results.append(result)
                else:
                    logging.info(f"Filtered out result with library_name: {library_name}")
            
            logging.info(f"After filtering HQ libraries: {len(filtered_results)} results")
            
            # First try to match by GUID
            for result in filtered_results:
                if result.get('guid') == history_guid:
                    logging.info(f"[Match] Found matching GUID in search results: {result.get('title')}")
                    return result.get('rating_key')
            
            # If no GUID match and we have a year, try to match by year
            if history_year:
                for result in filtered_results:
                    result_year = result.get('year')
                    if result_year and str(result_year) == str(history_year):
                        logging.info(f"[Match] Found matching year {history_year} for movie: {result.get('title')}")
                        return result.get('rating_key')
            
            # If we have results but no matches, use the first result
            if filtered_results:
                logging.info(f"No exact matches found, using first result: {filtered_results[0].get('title')}")
                return filtered_results[0].get('rating_key')
        
        elif media_type == 'episode':
            # Get the episode results list
            episode_results = results_list.get('episode', [])
            if not episode_results:
                logging.warning("No episode results found")
                return None
                
            logging.info(f"Found {len(episode_results)} episode results")
            
            # Filter out results with "HQ" in library_name
            filtered_results = []
            for result in episode_results:
                library_name = result.get('library_name', '')
                if 'HQ' not in library_name:
                    filtered_results.append(result)
                else:
                    logging.info(f"Filtered out result with library_name: {library_name}")
            
            logging.info(f"After filtering HQ libraries: {len(filtered_results)} results")
            
            # Look for matching GUID
            for result in filtered_results:
                if result.get('guid') == history_guid:
                    logging.info(f"[Match] Found matching GUID in search results: {result.get('title')}")
                    return result.get('grandparent_rating_key')
            
            # If we have results but no matches, use the first result
            if filtered_results:
                logging.info(f"No exact matches found, using first result: {filtered_results[0].get('title')}")
                return filtered_results[0].get('grandparent_rating_key')
        
        logging.warning("No suitable matches found")
        return None


class HistoryProcessor:
    def __init__(self, tautulli_api, database):
        """Initialize the history processor."""
        self.api = tautulli_api
        self.db = database
        
    def format_date(self, timestamp):
        """Format timestamp to be human readable."""
        try:
            dt = datetime.fromtimestamp(int(timestamp))
            return dt.strftime('%B %d, %Y %I:%M %p')
        except:
            return timestamp
    
    def scan_history(self, section_ids, start_date, end_date):
        """Scan history and identify unmatched items."""
        # Get media info for specified sections
        logging.info("=== Fetching library media info for sections ===")
        media_info_list = self.api.get_library_media_info(section_ids)
        if not media_info_list:
            logging.error("No media items found in sections.")
            return {'success': False, 'message': 'No media items found in sections.'}
        
        # Get watch history
        logging.info("=== Fetching watch history ===")
        history = self.api.get_watch_history(start_date, end_date)
        if not history:
            logging.error("No watch history found.")
            return {'success': False, 'message': 'No watch history found.'}
        
        # First pass: Process all items and collect unmatched ones
        logging.info("=== Processing History Items ===")
        unmatched_items = []
        unmatched_keys_set = set()
        total_items = len(history)
        
        for item in history:
            media_type = item.get('media_type')
            rating_key = item.get('rating_key')
            
            # For episodes, also check grandparent_rating_key
            if media_type == 'episode':
                grandparent_rating_key = item.get('grandparent_rating_key')
                if rating_key in unmatched_keys_set or grandparent_rating_key in unmatched_keys_set:
                    continue
            else:
                # Skip if we've already processed this rating key
                if rating_key in unmatched_keys_set:
                    continue
            
            # Process based on media type
            unmatched_item = None
            if media_type == 'movie':
                unmatched_item = self._process_movie(item, media_info_list)
            elif media_type == 'episode':
                unmatched_item = self._process_episode(item, media_info_list)
            
            # Add to unmatched list if not found
            if unmatched_item:
                unmatched_items.append(unmatched_item)
                unmatched_keys_set.add(rating_key)
                if media_type == 'episode':
                    unmatched_keys_set.add(grandparent_rating_key)
        
        # Filter out items that already exist in the database
        new_unmatched_items = []
        skipped_items = 0
        
        for item in unmatched_items:
            if not self.db.item_exists(item):
                new_unmatched_items.append(item)
            else:
                skipped_items += 1
                logging.info(f"Skipping already existing item: {item.get('title', '')} ({item.get('rating_key', '')})")
        
        # Save scan results to database
        scan_id = self.db.add_scan(total_items, len(new_unmatched_items))
        
        # Save new unmatched items to database
        for item in new_unmatched_items:
            self.db.add_unmatched_item(item, scan_id)
        
        logging.info(f"Skipped {skipped_items} items that already exist in the database")
        
        return {
            'success': True,
            'scan_id': scan_id,
            'total': total_items,
            'unmatched': len(new_unmatched_items),
            'skipped': skipped_items
        }
    
    def _process_movie(self, history_item, media_info_list):
        """
        First pass: Check if movie exists in media info by rating key.
        If not found, add to unmatched list.
        """
        rating_key = history_item.get('rating_key')
        
        # Check if movie exists in media info by rating key
        for media in media_info_list:
            if str(media.get('rating_key')) == str(rating_key):
                return None
        
        # If we get here, no match was found
        return history_item
    
    def _process_episode(self, history_item, media_info_list):
        """
        First pass: Check if show exists in media info by grandparent rating key.
        If not found, add to unmatched list.
        """
        grandparent_rating_key = history_item.get('grandparent_rating_key')
        
        # Check if show exists in media info by grandparent rating key
        for media in media_info_list:
            if str(media.get('rating_key')) == str(grandparent_rating_key):
                return None
        
        # If we get here, no match was found
        return history_item
    
    def fix_unmatched_item(self, item_id):
        """Try to fix a specific unmatched item."""
        item = self.db.get_unmatched_item(item_id)
        if not item:
            logging.error(f"Item with ID {item_id} not found in database")
            return False
        
        # Convert SQLite row to dict
        item_dict = dict(item)
        
        # Parse the stored JSON data
        try:
            import json
            history_item = json.loads(item_dict['json_data'])
            
            media_type = history_item.get('media_type')
            fixed = False
            fix_details = ""
            
            logging.info(f"Attempting to fix {media_type} item: {item_id}")
            
            if media_type == 'movie':
                fixed, fix_details = self._fix_unmatched_movie(history_item)
            elif media_type == 'episode':
                fixed, fix_details = self._fix_unmatched_episode(history_item)
            else:
                logging.error(f"Unknown media type: {media_type}")
                return False
            
            if fixed:
                self.db.mark_item_fixed(item_id, fix_details)
                logging.info(f"Successfully fixed item {item_id}: {fix_details}")
            else:
                logging.error(f"Failed to fix item {item_id}: {fix_details}")
            
            return fixed
        except Exception as e:
            logging.error(f"Error fixing item {item_id}: {str(e)}")
            return False
    
    def fix_all_unmatched(self):
        """Try to fix all unmatched items."""
        try:
            items = self.db.get_unmatched_items(include_ignored=False, include_fixed=False)
            fixed_count = 0
            
            logging.info(f"Attempting to fix {len(items)} unmatched items")
            
            for item in items:
                try:
                    if self.fix_unmatched_item(item['id']):
                        fixed_count += 1
                except Exception as e:
                    logging.error(f"Error fixing item {item['id'] if 'id' in item else 'unknown'}: {str(e)}")
                    continue
            
            logging.info(f"Fixed {fixed_count} out of {len(items)} items")
            return fixed_count
        except Exception as e:
            logging.error(f"Error in fix_all_unmatched: {str(e)}")
            return 0
    
    def _fix_unmatched_movie(self, history_item):
        """
        Second pass: Try to fix unmatched movie using title/year search.
        
        Steps:
        1. Try to find a match using the full title
        2. If no match found and title contains a colon, try with the part before the colon
        """
        try:
            rating_key = history_item.get('rating_key')
            title = history_item.get('title', '')
            year = history_item.get('year', '')
            guid = history_item.get('guid', '')
            media_type = history_item.get('media_type')
            
            if not rating_key or not title:
                logging.error(f"Missing required movie data: rating_key={rating_key}, title={title}")
                return False, "Missing required movie data"
            
            logging.info(f"Searching for movie: '{title}' ({year}) [GUID: {guid}]")
            
            # Step 1: Search for the movie by full title
            new_rating_key = self.api.search_title(title, guid, media_type, history_year=year)
            
            # Step 2: If no match found and title contains a colon, try with the part before the colon
            if not new_rating_key and ':' in title:
                # Extract the part before the colon and strip whitespace
                simplified_title = title.split(':', 1)[0].strip()
                logging.info(f"No match found with full title. Trying simplified title: '{simplified_title}'")
                
                # Search again with the simplified title
                new_rating_key = self.api.search_title(simplified_title, guid, media_type, history_year=year)
                
                if new_rating_key:
                    logging.info(f"Found match using simplified title: '{simplified_title}'")
            
            if not new_rating_key:
                logging.warning(f"No matching movie found for '{title}' ({year})")
                return False, "No matching movie found in search results"
            
            logging.info(f"Found matching movie with rating key: {new_rating_key}")
            
            # Try to update metadata
            if self.api.update_metadata_details(rating_key, new_rating_key, media_type):
                fix_details = f"Updated rating key from {rating_key} to {new_rating_key}"
                logging.info(fix_details)
                return True, fix_details
            
            return False, "Failed to update metadata"
        except Exception as e:
            logging.error(f"Error fixing movie: {str(e)}")
            return False, f"Error: {str(e)}"
    
    def _fix_unmatched_episode(self, history_item):
        """
        Second pass: Try to fix unmatched episode using show title/year search.
        """
        try:
            rating_key = history_item.get('rating_key')
            show_title = history_item.get('grandparent_title', '')
            season = history_item.get('parent_title', '')
            episode = history_item.get('title', '')
            guid = history_item.get('guid', '')
            media_type = history_item.get('media_type')
            
            if not rating_key or not show_title:
                logging.error(f"Missing required episode data: rating_key={rating_key}, show_title={show_title}")
                return False, "Missing required episode data"
            
            logging.info(f"Searching for episode: '{episode}' (from show: '{show_title}' - {season}) [GUID: {guid}]")
            
            # Search for the episode by episode title instead of show title
            # This often yields better results for finding the correct episode
            new_rating_key = self.api.search_title(episode, guid, media_type)
            
            if not new_rating_key:
                logging.warning(f"No matching show found for '{show_title}'")
                return False, "No matching show found in search results"
            
            logging.info(f"Found matching show with rating key: {new_rating_key}")
            
            # Try to update metadata
            if self.api.update_metadata_details(rating_key, new_rating_key, media_type):
                fix_details = f"Updated rating key from {rating_key} to {new_rating_key}"
                logging.info(fix_details)
                return True, fix_details
            
            return False, "Failed to update metadata"
        except Exception as e:
            logging.error(f"Error fixing episode: {str(e)}")
            return False, f"Error: {str(e)}"
