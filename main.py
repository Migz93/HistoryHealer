import os
import logging
from logging.handlers import RotatingFileHandler
from flask import Flask, render_template, request, jsonify, redirect, url_for
from datetime import datetime
import json

from core.version import __version__

from core.database import Database
from core.tautulli_api import TautulliAPI, HistoryProcessor
from core.filters import filters

# Determine config directory
def get_config_dir():
    """Determine the configuration directory based on environment"""
    # Check if /config exists (Docker environment)
    if os.path.exists('/config'):
        return '/config'
    # Otherwise use local config directory
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config')

# Set config directory
CONFIG_DIR = get_config_dir()
os.makedirs(CONFIG_DIR, exist_ok=True)

# Create logs directory if it doesn't exist
LOGS_DIR = os.path.join(CONFIG_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)

# Set up logging with rotation
log_file = os.path.join(LOGS_DIR, 'historyhealer.log')
rotating_handler = RotatingFileHandler(
    filename=log_file,
    maxBytes=1024 * 1024,  # 1 MB
    backupCount=7,  # Keep 7 backup files
    encoding='utf-8'
)
rotating_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        rotating_handler,
        logging.StreamHandler()
    ]
)

# Initialize Flask app
app = Flask(__name__, 
            template_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'core', 'templates'),
            static_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'core', 'static'))

# Add custom Jinja2 filters
app.jinja_env.filters['max'] = max
app.jinja_env.filters['min'] = min
app.register_blueprint(filters)

# Add version to all templates
@app.context_processor
def inject_version():
    return {'version': __version__}

def load_default_settings(db):
    """Load default settings into the database if they don't exist"""
    # Check if we have any settings
    if not db.get_setting('tautulli.base_url'):
        # Set default settings
        db.set_setting('tautulli.base_url', 'http://localhost:8181')
        db.set_setting('tautulli.api_key', 'your_tautulli_api_key_here')
        db.set_setting('history.start_date', '2022-01-01')
        db.set_setting('history.end_date', 'current')
        db.set_setting('libraries.section_ids', '1,2')
        logging.info("Created default settings in database")

# Initialize database
db_path = os.path.join(CONFIG_DIR, 'historyhealer.db')
db = Database(db_path)

# Load default settings if needed
load_default_settings(db)

# Initialize Tautulli API client
tautulli_api = TautulliAPI(
    db.get_setting('tautulli.base_url', 'http://localhost:8181'),
    db.get_setting('tautulli.api_key', '')
)

# Load TAUTULLI_SECTION_IDS into app.config from database settings
raw_section_ids = db.get_setting('libraries.section_ids', '1,2')
app.config['TAUTULLI_SECTION_IDS'] = [s.strip() for s in raw_section_ids.split(',') if s.strip()] if raw_section_ids else []
logging.info(f"Initial app.config TAUTULLI_SECTION_IDS: {app.config['TAUTULLI_SECTION_IDS']}")

# Initialize history processor
# The app.config object contains TAUTULLI_SECTION_IDS which HistoryProcessor now uses
history_processor = HistoryProcessor(tautulli_api, db, app.config)

@app.route('/')
def index():
    """Render the main dashboard page."""
    stats = db.get_stats()
    scan_history = db.get_scan_history()
    return render_template('index.html', stats=stats, scan_history=scan_history)

@app.route('/documentation')
def documentation():
    """Render the documentation page."""
    return render_template('documentation.html')

@app.route('/unmatched')
def unmatched():
    """Show unmatched items."""
    page = request.args.get('page', 1, type=int)
    per_page = 50
    media_type = request.args.get('media_type', '')
    sort_by = request.args.get('sort_by')
    sort_order = request.args.get('sort_order', 'asc')
    
    # Get unmatched items with pagination and sorting
    items, total = db.get_unmatched_items(
        page=page,
        per_page=per_page,
        media_type=media_type if media_type else None,
        sort_by=sort_by,
        sort_order=sort_order
    )
    
    # Calculate pagination values
    total_pages = (total + per_page - 1) // per_page
    has_prev = page > 1
    has_next = page < total_pages
    
    return render_template('unmatched.html', 
                          items=items, 
                          page=page, 
                          total_pages=total_pages,
                          has_prev=has_prev, 
                          has_next=has_next, 
                          total_items=total,
                          media_type=media_type,
                          sort_by=sort_by,
                          sort_order=sort_order)

@app.route('/ignored')
def ignored():
    """Show ignored items."""
    page = request.args.get('page', 1, type=int)
    per_page = 50
    sort_by = request.args.get('sort_by')
    sort_order = request.args.get('sort_order', 'asc')
    
    # Get ignored items with pagination and sorting
    items, total = db.get_ignored_items(
        page=page,
        per_page=per_page,
        sort_by=sort_by,
        sort_order=sort_order
    )
    
    # Calculate pagination values
    total_pages = (total + per_page - 1) // per_page
    has_prev = page > 1
    has_next = page < total_pages
    
    return render_template('ignored.html', 
                          items=items, 
                          page=page, 
                          total_pages=total_pages,
                          has_prev=has_prev, 
                          has_next=has_next, 
                          total_items=total,
                          sort_by=sort_by,
                          sort_order=sort_order)

@app.route('/fixed')
def fixed():
    """Show fixed items."""
    page = request.args.get('page', 1, type=int)
    per_page = 50
    sort_by = request.args.get('sort_by')
    sort_order = request.args.get('sort_order', 'asc')
    
    # Get fixed items with pagination and sorting
    items, total = db.get_fixed_items(
        page=page,
        per_page=per_page,
        sort_by=sort_by,
        sort_order=sort_order
    )
    
    # Calculate pagination values
    total_pages = (total + per_page - 1) // per_page
    has_prev = page > 1
    has_next = page < total_pages
    
    return render_template('fixed.html', 
                          items=items, 
                          page=page, 
                          total_pages=total_pages,
                          has_prev=has_prev, 
                          has_next=has_next, 
                          total_items=total,
                          sort_by=sort_by,
                          sort_order=sort_order)

@app.route('/api/scan', methods=['POST'])
def api_scan():
    """API endpoint to start a new scan."""
    try:
        # Get section IDs from database settings
        section_ids_str = db.get_setting('libraries.section_ids', '')
        if not section_ids_str:
            return jsonify({'success': False, 'message': 'No library section IDs configured'})
        
        # Convert comma-separated string to list of integers
        section_ids = [int(id.strip()) for id in section_ids_str.split(',') if id.strip().isdigit()]
        if not section_ids:
            return jsonify({'success': False, 'message': 'Invalid library section IDs'})
        
        # Get date range from database settings
        start_date = db.get_setting('history.start_date', '2022-01-01')
        end_date = db.get_setting('history.end_date', 'current')
        
        # If end_date is 'current', use current date
        if end_date == 'current':
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # Always get the latest Tautulli settings from the database
        base_url = db.get_setting('tautulli.base_url', 'http://localhost:8181')
        api_key = db.get_setting('tautulli.api_key', '')
        
        # Create a new Tautulli API client with the latest settings
        current_api = TautulliAPI(base_url, api_key)
        
        # Create a temporary history processor with the current API client and app config
        temp_processor = HistoryProcessor(current_api, db, app.config)
        
        # Start scan with the updated client
        result = temp_processor.scan_history(section_ids, start_date, end_date)
        
        if result['success']:
            return jsonify({
                'success': True, 
                'message': f"Scan completed. Found {result['total']} items, {result['unmatched']} unmatched."
            })
        else:
            return jsonify({'success': False, 'message': result['message']})
    except Exception as e:
        logging.exception("Error during scan")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/fix-all', methods=['POST'])
def api_fix_all():
    """API endpoint to attempt to fix all unmatched items."""
    try:
        # Get unmatched items
        items = db.get_unmatched_items(include_ignored=False, include_fixed=False)
        total_items = len(items)
        
        # Attempt to fix all items
        fixed_count = history_processor.fix_all_unmatched()
        
        return jsonify({
            'success': True, 
            'message': f"Fix attempt completed. Fixed {fixed_count} of {total_items} items."
        })
    except Exception as e:
        logging.exception("Error during fix all")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/fix-item/<int:item_id>', methods=['POST'])
def api_fix_item(item_id):
    """API endpoint to attempt to fix a specific unmatched item."""
    try:
        result = history_processor.fix_unmatched_item(item_id)
        
        if result:
            return jsonify({
                'success': True, 
                'message': f"Item fixed successfully."
            })
        else:
            return jsonify({'success': False, 'message': "Failed to fix item"})
    except Exception as e:
        logging.exception(f"Error fixing item {item_id}")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/manual-fix/<int:item_id>')
def manual_fix(item_id):
    """Show the manual fix page for a specific item."""
    try:
        # Get the unmatched item
        item = db.get_item_by_id(item_id)
        
        if not item:
            return render_template('error.html', message="Item not found")
        
        if item['status'] != 'unmatched':
            return redirect(url_for('unmatched'))
        
        # Parse the stored JSON data
        try:
            import json
            item_data = json.loads(item['json_data'])
        except (json.JSONDecodeError, KeyError) as e:
            logging.error(f"Error parsing item JSON data: {e}")
            return render_template('error.html', message="Error parsing item data")
        
        return render_template('manual_fix.html', item=item, item_data=item_data)
    except Exception as e:
        logging.exception(f"Error loading manual fix page for item {item_id}")
        return render_template('error.html', message=str(e))

@app.route('/api/manual-fix/search', methods=['POST'])
def api_manual_fix_search():
    """API endpoint for the search step of manual fix."""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'message': 'No data provided'})
        
        item_id = data.get('item_id')
        search_query = data.get('search_query')
        media_type = data.get('media_type')
        guid = data.get('guid')
        
        if not all([item_id, search_query, media_type]):
            return jsonify({'success': False, 'message': 'Missing required parameters: item_id, search_query, and media_type'})
        
        # Get the unmatched item
        item = db.get_item_by_id(item_id)
        
        if not item:
            return jsonify({'success': False, 'message': 'Item not found'})
        
        if item['status'] != 'unmatched':
            return jsonify({'success': False, 'message': 'Item is not unmatched'})
        
        # Get the JSON data from the item to check year
        try:
            item_json = json.loads(item['json_data'])
            year = item_json.get('year')
        except (json.JSONDecodeError, KeyError):
            year = None

        # Search using the Tautulli API - for manual fixes, we want all results
        results = tautulli_api.search_title(search_query, None, media_type, history_year=None, manual_search=True)
        
        # Process results into a format suitable for the frontend
        matches = []
        if results and isinstance(results, list):
            for result in results:
                # Still mark potential matches to help the user
                is_guid_match = result.get('guid') == guid
                is_year_match = str(result.get('year', '')) == str(year) if year and result.get('year') else False
                
                matches.append({
                    'result': result,
                    'is_match': is_guid_match or is_year_match,
                    'is_guid_match': is_guid_match,
                    'is_year_match': is_year_match
                })
        
        return jsonify({
            'success': True,
            'matches': matches,
            'raw_response': results
        })
    except Exception as e:
        logging.exception("Error during manual fix search")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/manual-fix/update', methods=['POST'])
def api_manual_fix_update():
    """API endpoint for the update step of manual fix."""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'message': 'No data provided'})
        
        item_id = data.get('item_id')
        new_rating_key = data.get('new_rating_key')
        
        if not all([item_id, new_rating_key]):
            return jsonify({'success': False, 'message': 'Missing required parameters'})
        
        # Get the unmatched item
        item = db.get_item_by_id(item_id)
        
        if not item:
            return jsonify({'success': False, 'message': 'Item not found'})
        
        if item['status'] != 'unmatched':
            return jsonify({'success': False, 'message': 'Item is not unmatched'})
        
        # Update the item with the new rating key
        db.update_item_rating_key(item_id, new_rating_key)
        
        # Attempt to update Tautulli
        tautulli_api_key = db.get_setting('tautulli.api_key')
        tautulli_url = db.get_setting('tautulli.base_url')
        fix_details = f'Manually updated rating key to {new_rating_key}'
        tautulli_updated_successfully = False

        if tautulli_api_key and tautulli_url:
            try:
                tautulli = TautulliAPI(tautulli_url, tautulli_api_key)
                # The item's original rating key is needed for the Tautulli update call
                original_rating_key = item.get('rating_key') 
                media_type = item.get('media_type')
                if tautulli.update_metadata_details(original_rating_key, new_rating_key, media_type):
                    logging.info(f"Tautulli metadata updated for item {item_id}: {original_rating_key} -> {new_rating_key}")
                    fix_details += ' (Tautulli updated).' # Append to fix_details
                    tautulli_updated_successfully = True
                else:
                    logging.warning(f"Failed to update Tautulli for item {item_id}. Check Tautulli logs.")
                    fix_details += ' (Tautulli update FAILED).' # Append to fix_details
            except Exception as e:
                logging.error(f"Error during Tautulli update for item {item_id}: {e}")
                fix_details += f' (Tautulli update error: {e}).' # Append to fix_details
        else:
            logging.warning(f"Tautulli not configured. Skipping Tautulli update for item {item_id}.")
            fix_details += ' (Tautulli not configured).' # Append to fix_details

        # Update the item in the local database with the new rating key and fix details
        db.update_item_rating_key(item_id, new_rating_key) # Update to the new key first
        db.mark_item_fixed(item_id, fix_details) # Then mark as fixed with all details
        
        return jsonify({
            'success': True,
            'message': 'Item updated successfully. ' + ('Tautulli update successful.' if tautulli_updated_successfully else 'Tautulli update may have failed or was skipped.'),
            'new_rating_key': new_rating_key
        })
    except Exception as e:
        logging.exception("Error during manual fix update")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/revert-item/<int:item_id>', methods=['POST'])
def api_revert_item(item_id):
    """API endpoint to revert a fixed item back to unmatched."""
    try:
        item = db.get_item_by_id(item_id)
        if not item:
            return jsonify({'success': False, 'message': 'Item not found'})
        
        if item['status'] != 'fixed':
            return jsonify({'success': False, 'message': 'Item is not marked as fixed'})
        
        # The revert_item_to_unmatched method now handles fetching the original rating key
        if db.revert_item_to_unmatched(item_id):
            logging.info(f"Item {item_id} successfully reverted to unmatched.")
            return jsonify({'success': True, 'message': 'Item reverted to unmatched successfully.'})
        else:
            logging.error(f"Failed to revert item {item_id} to unmatched.")
            return jsonify({'success': False, 'message': 'Failed to revert item. Check logs.'})
    except Exception as e:
        logging.exception(f"Error reverting item {item_id}")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/ignore-items', methods=['POST'])
def api_ignore_items():
    """API endpoint to mark items as ignored."""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'message': 'No data provided'})
        
        item_ids = data.get('item_ids', [])
        
        if not item_ids:
            return jsonify({'success': False, 'message': 'No items specified'})
        
        # Convert to list if single ID
        if not isinstance(item_ids, list):
            item_ids = [item_ids]
        
        # Mark items as ignored
        for item_id in item_ids:
            db.update_item_status(item_id, 'ignored')
        
        return jsonify({
            'success': True,
            'message': f'{len(item_ids)} item(s) marked as ignored',
            'ignored_count': len(item_ids)
        })
    except Exception as e:
        logging.exception("Error ignoring items")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/unignore-items', methods=['POST'])
def api_unignore_items():
    """API endpoint to mark items as unignored."""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'message': 'No data provided'})
        
        item_ids = data.get('item_ids', [])
        
        if not item_ids:
            return jsonify({'success': False, 'message': 'No items specified'})
        
        # Convert to list if single ID
        if not isinstance(item_ids, list):
            item_ids = [item_ids]
        
        # Mark items as unmatched (unignored)
        for item_id in item_ids:
            db.update_item_status(item_id, 'unmatched')
        
        return jsonify({
            'success': True,
            'message': f'{len(item_ids)} item(s) unignored',
            'unignored_count': len(item_ids)
        })
    except Exception as e:
        logging.exception("Error unignoring items")
        return jsonify({'success': False, 'message': str(e)})

@app.route('/settings')
def settings():
    """Render the settings page."""
    # Get current settings from database
    settings = {
        'tautulli': {
            'base_url': db.get_setting('tautulli.base_url', 'http://localhost:8181'),
            'api_key': db.get_setting('tautulli.api_key', '')
        },
        'history': {
            'start_date': db.get_setting('history.start_date', '2022-01-01'),
            'end_date': db.get_setting('history.end_date', 'current')
        },
        'libraries': {
            'section_ids': db.get_setting('libraries.section_ids', '')
        }
    }
    return render_template('settings.html', settings=settings)

@app.route('/api/save-settings', methods=['POST'])
def api_save_settings():
    """API endpoint to save settings."""
    try:
        # Get form data
        base_url = request.form.get('base_url', '').strip()
        api_key = request.form.get('api_key', '').strip()
        start_date = request.form.get('start_date', '').strip()
        end_date = request.form.get('end_date', '').strip()
        section_ids = request.form.get('section_ids', '').strip()
        
        # Validate base_url
        if not base_url.startswith(('http://', 'https://')):
            return redirect(url_for('settings', error='Base URL must start with http:// or https://'))
        
        # Save settings to database
        db.set_setting('tautulli.base_url', base_url)
        db.set_setting('tautulli.api_key', api_key)
        db.set_setting('history.start_date', start_date)
        db.set_setting('history.end_date', end_date)
        db.set_setting('libraries.section_ids', section_ids) # Save as comma-separated string to DB
        
        # Update app.config with the list of section IDs for HistoryProcessor
        app.config['TAUTULLI_SECTION_IDS'] = [s.strip() for s in section_ids.split(',') if s.strip()] if section_ids else []
        logging.info(f"Updated app.config TAUTULLI_SECTION_IDS to: {app.config['TAUTULLI_SECTION_IDS']}")

        # Update Tautulli API client with new settings
        global tautulli_api
        tautulli_api = TautulliAPI(base_url, api_key)
        
        # Redirect to settings page with success message
        return redirect(url_for('settings', success=True))
    except Exception as e:
        logging.error(f"Error saving settings: {e}")
        return redirect(url_for('settings', error=str(e)))

@app.route('/api/test-connection', methods=['POST'])
def api_test_connection():
    """API endpoint to test Tautulli connection."""
    try:
        # Get connection parameters
        base_url = request.form.get('base_url', '').strip()
        api_key = request.form.get('api_key', '').strip()
        
        # Validate base_url
        if not base_url.startswith(('http://', 'https://')):
            return jsonify({'success': False, 'message': 'Base URL must start with http:// or https://'})
        
        # Create a temporary API client for testing
        test_api = TautulliAPI(base_url, api_key)
        
        # Test the connection with get_tautulli_info command
        response = test_api._make_request('get_tautulli_info')
        
        if response and response.get('response', {}).get('result') == 'success':
            # Extract version info
            data = response.get('response', {}).get('data', {})
            version = data.get('tautulli_version', 'Unknown')
            return jsonify({
                'success': True, 
                'message': f'Successfully connected to Tautulli version {version}'
            })
        else:
            return jsonify({
                'success': False, 
                'message': 'Failed to connect to Tautulli. Please check your URL and API key.'
            })
    except Exception as e:
        logging.error(f"Error testing connection: {e}")
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})

if __name__ == '__main__':
    # Start the Flask application
    app.run(debug=True, host='0.0.0.0', port=6120)
