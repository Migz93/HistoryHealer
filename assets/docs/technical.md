# HistoryHealer Technical Documentation

This document contains technical information about the HistoryHealer application that will be useful for developers and maintainers.

## Version Management

The application version is stored in `/core/version.py` and is defined as the `__version__` variable. When making changes to the application, update this version number following semantic versioning principles:

- **Major version**: Increment for incompatible API changes
- **Minor version**: Increment for new functionality in a backward-compatible manner
- **Patch version**: Increment for backward-compatible bug fixes

Current version: `v1.0.0`

The version is automatically injected into all templates via a Flask context processor in `main.py` and displayed in the bottom of the sidebar.

## Application Structure

- **main.py**: Entry point of the application, contains Flask routes and API endpoints
- **core/**: Core application modules
  - **database.py**: Database operations for SQLite
  - **tautulli_api.py**: Handles communication with Tautulli API
  - **filters.py**: Custom Jinja2 template filters
  - **version.py**: Contains application version information
  - **templates/**: HTML templates for the web interface
  - **static/**: Static assets (CSS, JS, images)
- **assets/**: Additional assets
  - **images/**: Application images (logo, icon)
  - **screenshots/**: Screenshots for documentation
  - **docs/**: Technical documentation

## Configuration

The application uses a SQLite database to store settings and history data. The database is located at:
- `/config/historyhealer.db` in Docker environments
- `./config/historyhealer.db` in native installations

Settings are managed through the web interface and stored in the database.

## Web Interface

The web interface is built with:
- Flask (Python web framework)
- Bootstrap 5 (CSS framework)
- jQuery (JavaScript library)

The base template (`core/templates/base.html`) provides the common layout for all pages, including:
- Navigation bar
- Sidebar menu
- Loading overlay
- Common JavaScript functions

## API Endpoints

The application provides several API endpoints for:
- Scanning Tautulli history
- Fixing unmatched items
- Managing settings
- Testing connections

All API endpoints are defined in `main.py`.

## Docker Support

The application is containerized and can be run using Docker. The Dockerfile is in the root directory and the container exposes port 6120.

## Logging

Logs are stored in:
- `/config/logs/historyhealer.log` in Docker environments
- `./config/logs/historyhealer.log` in native installations

The application uses Python's built-in logging module with rotation to manage log files.
