"""
Demo Flask Web Application for Datafeed Scanner.

This is a standalone demo version that does NOT require Azure authentication.
Perfect for previewing the UI and user experience without setting up OAuth.

Features:
- Mock user authentication (no Azure AD required)
- Simulated scan with realistic mock data
- All UI pages functional
- Interactive results table
- CSV download

To run:
    pip install -r requirements-web.txt
    python webapp_demo.py

Then open: http://localhost:5000
"""

import os
import secrets
from datetime import datetime
from flask import Flask, render_template, redirect, url_for, session, request, Response
import pandas as pd
from io import StringIO

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = secrets.token_hex(16)
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_PERMANENT'] = True

# Mock user data
DEMO_USER = {
    'name': 'Demo User',
    'email': 'demo.user@example.com',
    'oid': 'demo-user-12345',
    'tenant_id': 'demo-tenant'
}


# ============================================================================
# Mock Data Generator
# ============================================================================

def generate_mock_scan_results():
    """Generate realistic mock datafeed scan results."""

    mock_data = []

    # Mock Datafeed Folder 1: Excel and Parquet
    datafeed_path_1 = "0000_test_parquet/100007-16_Showcase/Report Documentation/Datafeed"

    # Excel tables from DimManager
    excel_tables_1 = {
        'DimBrand': ['BrandID', 'BrandName', 'BrandCategory', 'IsActive', 'CreatedDate'],
        'DimProduct': ['ProductID', 'ProductName', 'ProductSKU', 'BrandID', 'Price', 'Stock'],
        'DimCustomer': ['CustomerID', 'FirstName', 'LastName', 'Email', 'Phone', 'Country'],
        'DimDate': ['DateKey', 'FullDate', 'Year', 'Quarter', 'Month', 'DayOfWeek']
    }

    for table_name, columns in excel_tables_1.items():
        for column in columns:
            mock_data.append({
                'Path': datafeed_path_1,
                'Source_Type': 'Excel',
                'Sheet_Name': 'Tables',
                'Table_Name': table_name,
                'Column_Name': column
            })

    # Parquet files
    parquet_files_1 = {
        'FactSales.parquet': ['SaleID', 'DateKey', 'ProductID', 'CustomerID', 'Quantity', 'Revenue', 'Profit'],
        'FactInventory.parquet': ['InventoryID', 'ProductID', 'DateKey', 'StockLevel', 'WarehouseID']
    }

    for file_name, columns in parquet_files_1.items():
        for column in columns:
            mock_data.append({
                'Path': datafeed_path_1,
                'Source_Type': 'Parquet',
                'Sheet_Name': '',
                'Table_Name': file_name,
                'Column_Name': column
            })

    # Mock Datafeed Folder 2: More Excel tables
    datafeed_path_2 = "999999_WeitereKDdec/128019_18_Ruegenwalder_Welle4/Report Documentation/Datafeed"

    excel_tables_2 = {
        'DimStore': ['StoreID', 'StoreName', 'StoreType', 'City', 'Region', 'Manager'],
        'DimPromotion': ['PromotionID', 'PromotionName', 'StartDate', 'EndDate', 'DiscountPct'],
        'DimSupplier': ['SupplierID', 'SupplierName', 'ContactPerson', 'Phone', 'Country']
    }

    for table_name, columns in excel_tables_2.items():
        for column in columns:
            mock_data.append({
                'Path': datafeed_path_2,
                'Source_Type': 'Excel',
                'Sheet_Name': 'Dimensions',
                'Table_Name': table_name,
                'Column_Name': column
            })

    # Parquet files
    parquet_files_2 = {
        'FactOrders.parquet': ['OrderID', 'OrderDate', 'CustomerID', 'StoreID', 'TotalAmount', 'Status'],
        'FactReturns.parquet': ['ReturnID', 'OrderID', 'ProductID', 'ReturnDate', 'Quantity', 'Reason']
    }

    for file_name, columns in parquet_files_2.items():
        for column in columns:
            mock_data.append({
                'Path': datafeed_path_2,
                'Source_Type': 'Parquet',
                'Sheet_Name': '',
                'Table_Name': file_name,
                'Column_Name': column
            })

    # Mock Datafeed Folder 3: Parquet only
    datafeed_path_3 = "555555_Analytics/DataWarehouse/Report Documentation/Datafeed"

    parquet_files_3 = {
        'FactWebTraffic.parquet': ['SessionID', 'UserID', 'PageURL', 'Timestamp', 'Duration', 'DeviceType'],
        'FactConversions.parquet': ['ConversionID', 'SessionID', 'ProductID', 'ConversionDate', 'Revenue']
    }

    for file_name, columns in parquet_files_3.items():
        for column in columns:
            mock_data.append({
                'Path': datafeed_path_3,
                'Source_Type': 'Parquet',
                'Sheet_Name': '',
                'Table_Name': file_name,
                'Column_Name': column
            })

    return pd.DataFrame(mock_data)


def calculate_mock_stats(df):
    """Calculate statistics from mock data."""
    return {
        'total_rows': len(df),
        'total_paths': df['Path'].nunique(),
        'excel_tables': df[df['Source_Type'] == 'Excel']['Table_Name'].nunique() if 'Excel' in df['Source_Type'].values else 0,
        'excel_columns': len(df[df['Source_Type'] == 'Excel']) if 'Excel' in df['Source_Type'].values else 0,
        'parquet_files': df[df['Source_Type'] == 'Parquet']['Table_Name'].nunique() if 'Parquet' in df['Source_Type'].values else 0,
        'parquet_columns': len(df[df['Source_Type'] == 'Parquet']) if 'Parquet' in df['Source_Type'].values else 0,
    }


# ============================================================================
# Routes
# ============================================================================

@app.route('/')
def index():
    """Home page - shows login if not authenticated, otherwise redirects to dashboard."""
    if session.get('user'):
        return redirect(url_for('dashboard'))

    return render_template('index.html')


@app.route('/login')
def login():
    """Mock login - automatically log in as demo user."""
    session['user'] = DEMO_USER
    session['access_token'] = 'demo-token-12345'
    session['demo_mode'] = True
    return redirect(url_for('dashboard'))


@app.route('/logout')
def logout():
    """Log out the demo user."""
    session.clear()
    return redirect(url_for('index'))


@app.route('/dashboard')
def dashboard():
    """Main dashboard - shown after successful login."""
    if not session.get('user'):
        return redirect(url_for('index'))

    user = session['user']
    has_results = 'scan_results' in session

    return render_template('dashboard.html',
                         user=user,
                         has_results=has_results)


@app.route('/scan', methods=['POST'])
def start_scan():
    """Simulate a datafeed scan with mock data."""
    if not session.get('user'):
        return redirect(url_for('index'))

    try:
        # Generate mock scan results
        df = generate_mock_scan_results()

        # Store results in session as JSON
        if not df.empty:
            session['scan_results'] = df.to_json(orient='split')
            session['scan_stats'] = calculate_mock_stats(df)
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
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"datafeed_report_demo_{timestamp}.csv"

    return Response(
        csv_data,
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment;filename={filename}'}
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
# Health Check
# ============================================================================

@app.route('/health')
def health():
    """Health check endpoint."""
    return {'status': 'healthy', 'app': 'datafeed-scanner-demo', 'mode': 'demo'}, 200


# ============================================================================
# Run Application
# ============================================================================

if __name__ == '__main__':
    print("\n" + "="*70)
    print("DATAFEED SCANNER - DEMO MODE")
    print("="*70)
    print("\nThis is a DEMO version with mock data - no Azure setup required!")
    print("\nFeatures:")
    print("  ✓ Mock authentication (no real login)")
    print("  ✓ Simulated scan results")
    print("  ✓ Full UI preview")
    print("  ✓ Interactive results table")
    print("  ✓ CSV download")
    print("\nStarting server...")
    print("Open your browser and go to: http://localhost:5000")
    print("\nPress Ctrl+C to stop the server")
    print("="*70 + "\n")

    app.run(
        host='0.0.0.0',
        port=5000,
        debug=True
    )
