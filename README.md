# HistoryHealer

A web-based application for fixing and repairing Tautulli watch history entries that have become unmatched from their Plex media items. This sometimes happens when upgrading media or if you've deleted media and later on re-added it.

HistoryHealer provides a user-friendly web interface that allows you to:

1. Scan your Tautulli history for unmatched items
2. View all unmatched items
3. Attempt to fix all unmatched items at once
4. Fix individual unmatched items
5. Ignore items you don't want to fix or can't fix
6. Track fixed and ignored items

## Features

- **Dashboard**: View statistics and recent scan history
- **Unmatched Items**: View and manage all unmatched items
- **Fixed Items**: View all successfully fixed items
- **Ignored Items**: View and manage all ignored items
- **Settings**: Configure Tautulli connection and scan parameters

## Usage

1. Start the application (see installation instructions below)
2. Open a web browser and navigate to `http://localhost:6120`
3. Configure your Tautulli connection settings in the Settings page
4. Click "Scan" to scan your Tautulli history for unmatched items
5. View unmatched items and use the "Fix" button to attempt to fix them
6. Use the "Ignore" button to hide items you don't want to fix
7. Use the "Attempt Fix All" button to try fixing all unmatched items at once
8. Navigate between pages using the sidebar menu

## Installation

### Docker Installation (Recommended)

Run the Docker container:

```bash
docker run -d \
  --name historyhealer \
  -p 6120:6120 \
  -v /path/to/your/config:/config \
  ghcr.io/migz93/historyhealer:latest
```

Or using Docker Compose:

```yaml
version: '3'
services:
  historyhealer:
    image: ghcr.io/migz93/historyhealer:latest
    container_name: historyhealer
    ports:
      - 6120:6120
    volumes:
      - /path/to/your/config:/config
    restart: unless-stopped
```

The application will create a configuration database in your mounted volume.
Access the web interface at http://localhost:6120 and configure your settings.

### Native Installation

1. Clone the repository
2. Install the requirements:
   ```
   pip install -r requirements.txt
   ```
3. Run the application:
   ```
   python main.py
   ```
4. The application will create a configuration database in the `config` directory
5. Access the web interface and configure your Tautulli connection and scan parameters

## Configuration

HistoryHealer uses a web-based settings interface. After installation, navigate to the Settings page to configure:

### Tautulli Connection Settings
- **Tautulli Base URL**: The URL of your Tautulli server (including http:// or https:// and port)
- **Tautulli API Key**: Your Tautulli API key

### History Scan Settings
- **Start Date**: The start date for history scanning (YYYY-MM-DD)
- **End Date**: The end date for history scanning (YYYY-MM-DD or "current" for today's date)

### Library Settings
- **Section IDs**: Comma-separated list of Plex library section IDs to scan

## License

This project is open source and available under the MIT License.
