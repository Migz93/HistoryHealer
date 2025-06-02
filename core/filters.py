from datetime import datetime
from flask import Blueprint

filters = Blueprint('filters', __name__)

@filters.app_template_filter('timestamp_to_date')
def timestamp_to_date(timestamp):
    """Convert a Unix timestamp to a human-readable date."""
    if not timestamp:
        return "Unknown"
    try:
        dt = datetime.fromtimestamp(int(timestamp))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return str(timestamp)
