"""
Flask web application for Datafeed Scanner.

Provides a web interface for scanning Azure Blob Storage datafeed folders
with user-level OAuth authentication.
"""

import os
import sys
import secrets
from flask import Flask, render_template, redirect, url_for, session, request, jsonify, Response
from flask_session import Session
import pandas as pd
from io import StringIO

from auth_helper import create_msal_helper_from_env, MSALHelper
from web_config import init_app_config, WebConfig
from datafeed_scanner import DatafeedScanner


# Initialize Flask app
app = Flask(__name__)

# Load configuration
init_app_config(app)

# Initialize session management
Session(app)

# Create MSAL helper
try:
    msal_helper = create_msal_helper_from_env()
except ValueError as e:
    print(f"Error initializing MSAL: {e}")
    sys.exit(1)


# ============================================================================
# Authentication Routes
# ============================================================================

@app.route('/')
def index():
    """Home page - shows login if not authenticated, otherwise redirects to dashboard."""
    if session.get('user'):
        return redirect(url_for('dashboard'))

    return render_template('index.html')


@app.route('/login')
def login():
    """Initiate OAuth login flow."""
    # Generate state for CSRF protection
    state = secrets.token_urlsafe(16)
    session['oauth_state'] = state

    # Get redirect URI
    redirect_uri = WebConfig.get_redirect_uri(request)

    # Generate authorization URL
    auth_url = msal_helper.get_authorization_url(
        redirect_uri=redirect_uri,
        state=state
    )

    return redirect(auth_url)


@app.route('/callback')
def callback():
    """OAuth callback - handles redirect from Microsoft login."""
    # Verify state for CSRF protection
    state = request.args.get('state')
    if state != session.get('oauth_state'):
        return render_template('error.html',
                             error_title="Authentication Error",
                             error_message="Invalid state parameter. Possible CSRF attack."), 400

    # Get authorization code
    code = request.args.get('code')
    if not code:
        error_description = request.args.get('error_description', 'Unknown error')
        return render_template('error.html',
                             error_title="Login Failed",
                             error_message=f"Authorization failed: {error_description}"), 400

    # Exchange code for token
    redirect_uri = WebConfig.get_redirect_uri(request)
    token_result = msal_helper.acquire_token_by_auth_code(
        code=code,
        redirect_uri=redirect_uri
    )

    # Check if token acquisition was successful
    if not msal_helper.is_token_valid(token_result):
        error_msg = token_result.get('error_description', 'Failed to acquire token')
        return render_template('error.html',
                             error_title="Authentication Failed",
                             error_message=error_msg), 400

    # Store user info and token in session
    session['access_token'] = token_result['access_token']
    session['expires_in'] = token_result.get('expires_in', 3600)
    session['user'] = MSALHelper.extract_user_info(token_result['id_token_claims'])

    # Clean up OAuth state
    session.pop('oauth_state', None)

    return redirect(url_for('dashboard'))


@app.route('/logout')
def logout():
    """Log out the current user."""
    session.clear()
    return redirect(url_for('index'))


# ============================================================================
# Application Routes
# ============================================================================

@app.route('/dashboard')
def dashboard():
    """Main dashboard - shown after successful login."""
    if not session.get('user'):
        return redirect(url_for('index'))

    user = session['user']

    # Check if there are previous scan results
    has_results = 'scan_results' in session

    return render_template('dashboard.html',
                         user=user,
                         has_results=has_results)


@app.route('/scan', methods=['POST'])
def start_scan():
    """Start a datafeed scan."""
    if not session.get('access_token'):
        return redirect(url_for('index'))

    try:
        # Create credential from user's token
        credential = msal_helper.create_credential_from_token(
            access_token=session['access_token'],
            expires_in=session.get('expires_in')
        )

        # Initialize datafeed scanner with user's credentials
        scanner = DatafeedScanner(credential=credential)

        # Run the scan
        df = scanner.generate_report()

        # Store results in session as JSON
        if not df.empty:
            session['scan_results'] = df.to_json(orient='split')
            session['scan_stats'] = {
                'total_rows': len(df),
                'total_paths': df['Path'].nunique(),
                'excel_tables': df[df['Source_Type'] == 'Excel']['Table_Name'].nunique() if 'Excel' in df['Source_Type'].values else 0,
                'excel_columns': len(df[df['Source_Type'] == 'Excel']) if 'Excel' in df['Source_Type'].values else 0,
                'parquet_files': df[df['Source_Type'] == 'Parquet']['Table_Name'].nunique() if 'Parquet' in df['Source_Type'].values else 0,
                'parquet_columns': len(df[df['Source_Type'] == 'Parquet']) if 'Parquet' in df['Source_Type'].values else 0,
            }
        else:
            session['scan_results'] = None
            session['scan_stats'] = None

        return redirect(url_for('results'))

    except Exception as e:
        return render_template('error.html',
                             error_title="Scan Failed",
                             error_message=f"An error occurred during scanning: {str(e)}"), 500


@app.route('/results')
def results():
    """Display scan results."""
    if not session.get('user'):
        return redirect(url_for('index'))

    if not session.get('scan_results'):
        return redirect(url_for('dashboard'))

    # Load results from session
    df = pd.read_json(StringIO(session['scan_results']), orient='split')
    stats = session.get('scan_stats', {})

    # Convert DataFrame to HTML table with Bootstrap classes
    table_html = df.to_html(
        classes='table table-striped table-bordered table-hover',
        index=False,
        table_id='results-table'
    )

    return render_template('results.html',
                         user=session['user'],
                         table=table_html,
                         stats=stats,
                         row_count=len(df))


@app.route('/download-csv')
def download_csv():
    """Download scan results as CSV."""
    if not session.get('scan_results'):
        return "No results to download", 404

    # Load results from session
    df = pd.read_json(StringIO(session['scan_results']), orient='split')

    # Convert to CSV
    csv_data = df.to_csv(index=False)

    # Return as downloadable file
    return Response(
        csv_data,
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment;filename=datafeed_report.csv'}
    )


@app.route('/clear-results', methods=['POST'])
def clear_results():
    """Clear scan results from session."""
    session.pop('scan_results', None)
    session.pop('scan_stats', None)
    return redirect(url_for('dashboard'))


# ============================================================================
# Error Handlers
# ============================================================================

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors."""
    return render_template('error.html',
                         error_title="Page Not Found",
                         error_message="The page you're looking for doesn't exist."), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors."""
    return render_template('error.html',
                         error_title="Internal Server Error",
                         error_message="An unexpected error occurred. Please try again later."), 500


# ============================================================================
# Health Check (for Azure App Service)
# ============================================================================

@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({'status': 'healthy', 'app': 'datafeed-scanner'}), 200


# ============================================================================
# Run Application
# ============================================================================

if __name__ == '__main__':
    # Run in development mode
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=WebConfig.DEBUG
    )
