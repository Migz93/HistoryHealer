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
        
        logging.info(f"Updating metadata: {old_rating_key} -> {new_rating_key} (type: {update_type})")
        data = self._make_request('update_metadata_details', params)
        
        if not data:
            logging.error("API request failed or returned no data")
            return False
            
        response = data.get('response', {})
        if response.get('result') == 'success':
            logging.info(f"Successfully updated metadata details for {media_type} (rating key: {old_rating_key})")
            return True
        else:
            error = response.get('message', 'Unknown error')
            logging.error(f"Failed to update metadata details: {error}")
            logging.error(f"Update params: old_key={old_rating_key}, new_key={new_rating_key}, type={update_type}")
            return False
    
    def search_title(self, search_term, history_guid=None, media_type=None, history_year=None, manual_search=False, allowed_section_ids=None):
        """Search for a title in Tautulli.
        
        Args:
            search_term: The term to search for
            history_guid: The GUID to match against (optional)
            media_type: Type of media to search for (movie, show, episode)
            history_year: Year to match against for movies (optional)
            manual_search: If True, return all results without filtering for matches.
            allowed_section_ids: Optional list of section IDs (as strings). If provided and not manual_search,
                                 results will be filtered to include only these section IDs.
        """
        try:
            if not search_term:
                return None
            
            if media_type == 'movie':
                search_results = self.search_movie(search_term)
            elif media_type == 'show':
                search_results = self.search_show(search_term)
            elif media_type == 'episode':
                # Try episode search first
                search_results = self.search_episode(search_term)
                # If no episode results and not manual search, try show search
                if not search_results and not manual_search:
                    search_results = self.search_show(search_term)
            else:
                logging.warning(f"Unsupported media type: {media_type}")
                return None
            
            if not search_results:
                logging.warning("No search results found")
                return None
            
            # Filter results:
            # 1. If manual_search, no specific filtering is applied here based on section_id.
            # 2. If automatic search (not manual_search) AND allowed_section_ids are provided,
            #    filter by those section_ids.
            # The old 'HQ' library name filter is removed.
            
            if not manual_search and allowed_section_ids:
                temp_results = []
                for result in search_results:
                    # Ensure section_id from result is treated as string for comparison
                    result_section_id = str(result.get('section_id'))
                    if result_section_id in allowed_section_ids:
                        temp_results.append(result)
                    else:
                        logging.info(f"Filtered out (auto-fix) result from section_id: {result_section_id} (not in {allowed_section_ids}). Item: {result.get('title')}")
                filtered_results = temp_results
            else:
                # For manual search, or if no allowed_section_ids provided for auto search, use all results from initial search
                filtered_results = list(search_results) # Ensure it's a list copy
            
            logging.info(f"After filtering: {len(filtered_results)} results")
            
            # For manual searches, return all results
            if manual_search:
                return filtered_results
            
            # For automatic fixes, look for exact GUID match
            if history_guid:
                for result in filtered_results:
                    if result.get('guid') == history_guid:
                        return result # Exact GUID match found
            
            # For movies, GUID match is now the only criteria for automatic fix.
            # Year-based fallback has been removed.
            
            return None # No exact GUID match found for automatic fix
            
        except Exception as e:
            logging.exception("Error during title search")
            return None
    
    def search_movie(self, title):
        """Search for a movie using Tautulli API."""
        params = {
            'query': title,
            'limit': 20  # Increased limit to get more potential matches
        }
        
        data = self._make_request('search', params)
        if not data or data.get('response', {}).get('result') != 'success':
            logging.warning("Search returned no results or failed")
            return None
        
        results_list = data.get('response', {}).get('data', {}).get('results_list', {})
        movie_results = results_list.get('movie', [])
        
        if not movie_results:
            logging.warning("No movie results found")
            return None
        
        logging.info(f"Found {len(movie_results)} movie results")
        return movie_results
    
    def search_episode(self, title):
        """Search for TV episodes using Tautulli API."""
        params = {
            'query': title,
            'limit': 20
        }
        
        data = self._make_request('search', params)
        if not data or data.get('response', {}).get('result') != 'success':
            logging.warning("Search returned no results or failed")
            return None
        
        results_list = data.get('response', {}).get('data', {}).get('results_list', {})
        episode_results = results_list.get('episode', [])
        
        if not episode_results:
            logging.warning("No episode results found")
            return None
        
        logging.info(f"Found {len(episode_results)} episode results")
        return episode_results

    def search_show(self, title):
        """Search for a TV show using Tautulli API."""
        params = {
            'query': title,
            'limit': 20  # Increased limit to get more potential matches
        }
        
        data = self._make_request('search', params)
        if not data or data.get('response', {}).get('result') != 'success':
            logging.warning("Search returned no results or failed")
            return None
        
        results_list = data.get('response', {}).get('data', {}).get('results_list', {})
        show_results = results_list.get('show', [])
        
        if not show_results:
            logging.warning("No show results found")
            return None
        
        logging.info(f"Found {len(show_results)} show results")
        return show_results


class HistoryProcessor:
    def __init__(self, tautulli_api, database, config):
        """Initialize the history processor."""
        self.api = tautulli_api
        self.db = database
        self.config = config
        
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
            
            allowed_section_ids_str = [str(sid) for sid in self.config.get('TAUTULLI_SECTION_IDS', [])]
            # Step 1: Search for the movie by full title
            result = self.api.search_title(title, guid, media_type, history_year=year, allowed_section_ids=allowed_section_ids_str)
            
            # Step 2: If no match found and title contains a colon, try with the part before the colon
            if not result and ':' in title:
                # Extract the part before the colon and strip whitespace
                simplified_title = title.split(':', 1)[0].strip()
                logging.info(f"No match found with full title. Trying simplified title: '{simplified_title}'")
                
                # Search again with the simplified title
                # allowed_section_ids_str is already defined from the first search attempt in this method
                result = self.api.search_title(simplified_title, guid, media_type, history_year=year, allowed_section_ids=allowed_section_ids_str)
            
            if not result:
                logging.warning(f"No matching movie found for '{title}' ({year})")
                return False, "No matching movie found in search results"
            
            new_rating_key = result.get('rating_key')
            if not new_rating_key:
                logging.warning(f"Found match but no rating key present")
                return False, "Match found but no rating key present"
            
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
        """Attempt to fix an unmatched episode by first matching episode GUID, then show GUID."""
        try:
            episode_title = history_item.get('title', '')
            episode_guid = history_item.get('guid', '') # Actual GUID of the episode
            original_episode_rating_key = history_item.get('rating_key')
            media_type_episode = history_item.get('media_type') # Should be 'episode'
            
            show_title = history_item.get('grandparent_title', '')
            show_guid = history_item.get('grandparent_guid', '') # Actual GUID of the show
            # season = history_item.get('parent_title', '') # For logging if needed

            if not original_episode_rating_key or not media_type_episode == 'episode':
                logging.error(f"Missing required episode data: rating_key={original_episode_rating_key}, media_type={media_type_episode}")
                return False, "Missing or incorrect episode data"

            allowed_section_ids_str = [str(sid) for sid in self.config.get('TAUTULLI_SECTION_IDS', [])]

            # Attempt 1: Match Episode by Episode GUID
            logging.info(f"Attempt 1: Searching for EPISODE '{episode_title}' [GUID: {episode_guid}]")
            if episode_title and episode_guid:
                result_episode_match = self.api.search_title(episode_title, episode_guid, media_type_episode, allowed_section_ids=allowed_section_ids_str)
                # Simplified title logic for episode title (e.g. if it has a colon)
                if not result_episode_match and ':' in episode_title:
                    simplified_ep_title = episode_title.split(':', 1)[0].strip()
                    logging.info(f"No match for '{episode_title}', trying simplified EP title: '{simplified_ep_title}'")
                    result_episode_match = self.api.search_title(simplified_ep_title, episode_guid, media_type_episode, allowed_section_ids=allowed_section_ids_str)

                if result_episode_match:
                    new_episode_rating_key = result_episode_match.get('rating_key')
                    if new_episode_rating_key:
                        logging.info(f"Found matching EPISODE with rating key: {new_episode_rating_key} by episode GUID.")
                        if self.api.update_metadata_details(original_episode_rating_key, new_episode_rating_key, media_type_episode):
                            return True, f"Fixed by matching episode GUID to {new_episode_rating_key}"
                        else:
                            return False, "Failed to update metadata after episode GUID match"
                    else:
                        logging.warning("Episode GUID match found but no rating key present in result.")
            else:
                logging.info("Skipping episode GUID match attempt: missing episode title or episode GUID.")

            # Attempt 2: Match Show by Show GUID (if Attempt 1 failed)
            logging.info(f"Attempt 2: Searching for SHOW '{show_title}' [GUID: {show_guid}]")
            if show_title and show_guid:
                result_show_match = self.api.search_title(show_title, show_guid, 'show', allowed_section_ids=allowed_section_ids_str)
                # Simplified title logic for show title (e.g. if it has brackets)
                if not result_show_match and ('(' in show_title or '[' in show_title):
                    simplified_show_title = show_title.split('(')[0].split('[')[0].strip()
                    if simplified_show_title and simplified_show_title != show_title:
                        logging.info(f"No match for '{show_title}', trying simplified SHOW title: '{simplified_show_title}'")
                        result_show_match = self.api.search_title(simplified_show_title, show_guid, 'show', allowed_section_ids=allowed_section_ids_str)
                
                if result_show_match:
                    new_show_rating_key = result_show_match.get('rating_key') # This is the show's rating key
                    if new_show_rating_key:
                        logging.info(f"Found matching SHOW with rating key: {new_show_rating_key} by show GUID.")
                        # Tautulli updates episode metadata using the episode's old rating key and the show's new rating key
                        if self.api.update_metadata_details(original_episode_rating_key, new_show_rating_key, media_type_episode):
                            return True, f"Fixed by matching show GUID to {new_show_rating_key} (for episode update)"
                        else:
                            return False, "Failed to update metadata after show GUID match"
                    else:
                        logging.warning("Show GUID match found but no rating key present in result.")
            else:
                logging.info("Skipping show GUID match attempt: missing show title or show GUID.")

            return False, "Failed to find match by episode or show GUID"
            
        except Exception as e:
            logging.error(f"Error fixing episode: {str(e)}")
            return False, f"Error: {str(e)}"
