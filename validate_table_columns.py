"""
Validate Table Columns - Check for column consistency across tables

This script reads the datafeed report CSV and checks if tables with the same name
have consistent column schemas across different paths. It identifies columns that
are unique to specific instances of a table.

Enhanced with:
- Rich console output with colors and tables
- HTML export with interactive styling
- Excel export with multiple sheets and conditional formatting
- Summary visualizations
"""

import pandas as pd
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import track
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("Note: Install 'rich' for better console visualization: pip install rich")

console = Console() if RICH_AVAILABLE else None


def export_to_html(analysis_results, output_file="table_validation_report.html"):
    """Export analysis results to HTML with styling.

    Args:
        analysis_results: Dictionary with analysis data
        output_file: Output HTML file path
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Table Column Validation Report</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        h1 {{ margin: 0; font-size: 2em; }}
        .timestamp {{ opacity: 0.9; margin-top: 10px; }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .summary-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .summary-card h3 {{
            margin: 0 0 10px 0;
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
        }}
        .summary-card .value {{
            font-size: 2.5em;
            font-weight: bold;
            margin: 0;
        }}
        .summary-card.success .value {{ color: #10b981; }}
        .summary-card.warning .value {{ color: #f59e0b; }}
        .summary-card.info .value {{ color: #3b82f6; }}
        .section {{
            background: white;
            padding: 25px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h2 {{
            margin-top: 0;
            color: #333;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #e5e7eb;
        }}
        th {{
            background-color: #f9fafb;
            font-weight: 600;
            color: #374151;
        }}
        tr:hover {{
            background-color: #f9fafb;
        }}
        .consistent {{ color: #10b981; }}
        .inconsistent {{ color: #ef4444; }}
        .badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 600;
        }}
        .badge-success {{
            background-color: #d1fae5;
            color: #065f46;
        }}
        .badge-danger {{
            background-color: #fee2e2;
            color: #991b1b;
        }}
        .details {{
            margin-top: 15px;
            padding: 15px;
            background-color: #f9fafb;
            border-left: 4px solid #ef4444;
            border-radius: 4px;
        }}
        .details h4 {{
            margin-top: 0;
            color: #374151;
        }}
        .path-item {{
            margin: 10px 0;
            padding: 10px;
            background: white;
            border-radius: 4px;
        }}
        .column-list {{
            display: inline-block;
            padding: 4px 8px;
            margin: 2px;
            background-color: #e5e7eb;
            border-radius: 4px;
            font-size: 0.9em;
        }}
        .column-list.missing {{ background-color: #fecaca; }}
        .column-list.extra {{ background-color: #fed7aa; }}
        .footer {{
            text-align: center;
            padding: 20px;
            color: #6b7280;
            font-size: 0.9em;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Table Column Validation Report</h1>
        <div class="timestamp">Generated: {timestamp}</div>
    </div>

    <div class="summary">
        <div class="summary-card info">
            <h3>Total Tables</h3>
            <div class="value">{analysis_results['total_tables']}</div>
        </div>
        <div class="summary-card success">
            <h3>Consistent Tables</h3>
            <div class="value">{analysis_results['consistent_count']}</div>
        </div>
        <div class="summary-card warning">
            <h3>Inconsistent Tables</h3>
            <div class="value">{analysis_results['inconsistent_count']}</div>
        </div>
    </div>
"""

    # Consistent tables section
    if analysis_results['consistent_tables']:
        html_content += """
    <div class="section">
        <h2>✓ Consistent Tables</h2>
        <table>
            <thead>
                <tr>
                    <th>Table Name</th>
                    <th>Instances</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
"""
        for table_name, instance_count in analysis_results['consistent_tables']:
            html_content += f"""
                <tr>
                    <td>{table_name}</td>
                    <td>{instance_count}</td>
                    <td><span class="badge badge-success">Consistent</span></td>
                </tr>
"""
        html_content += """
            </tbody>
        </table>
    </div>
"""

    # Inconsistent tables section
    if analysis_results['inconsistent_tables']:
        html_content += """
    <div class="section">
        <h2>✗ Inconsistent Tables</h2>
"""
        for table_info in analysis_results['inconsistent_tables']:
            html_content += f"""
        <div class="details">
            <h4>✗ {table_info['name']}</h4>
            <p>
                <strong>Total instances:</strong> {table_info['instance_count']} |
                <strong>Common columns:</strong> {table_info['common_count']} |
                <strong>Total unique columns:</strong> {table_info['total_columns']}
            </p>
"""
            if table_info['inconsistent_columns']:
                html_content += f"""
            <p><strong>⚠ Inconsistent columns ({len(table_info['inconsistent_columns'])}):</strong></p>
            <p>
"""
                for col in sorted(table_info['inconsistent_columns']):
                    html_content += f'<span class="column-list">{col}</span> '
                html_content += """
            </p>
"""

            html_content += """
            <h4>Path-specific Analysis:</h4>
"""
            for path_info in table_info['paths']:
                html_content += f"""
            <div class="path-item">
                <strong>{path_info['path']}</strong>
"""
                if path_info['missing']:
                    html_content += '<br><strong>Missing:</strong> '
                    for col in sorted(path_info['missing']):
                        html_content += f'<span class="column-list missing">{col}</span> '
                if path_info['extra']:
                    html_content += '<br><strong>Extra:</strong> '
                    for col in sorted(path_info['extra']):
                        html_content += f'<span class="column-list extra">{col}</span> '
                if not path_info['missing'] and not path_info['extra']:
                    html_content += '<br><span class="badge badge-success">Has all common columns</span>'
                html_content += """
            </div>
"""
            html_content += """
        </div>
"""
        html_content += """
    </div>
"""

    html_content += """
    <div class="footer">
        Table Column Validation Report
    </div>
</body>
</html>
"""

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)

    return output_file


def export_to_excel(analysis_results, df, output_file="table_validation_report.xlsx"):
    """Export analysis results to Excel with multiple sheets.

    Args:
        analysis_results: Dictionary with analysis data
        df: Original dataframe
        output_file: Output Excel file path
    """
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Summary sheet
            summary_data = {
                'Metric': ['Total Tables', 'Consistent Tables', 'Inconsistent Tables', 'Consistency Rate'],
                'Value': [
                    analysis_results['total_tables'],
                    analysis_results['consistent_count'],
                    analysis_results['inconsistent_count'],
                    f"{(analysis_results['consistent_count'] / analysis_results['total_tables'] * 100):.1f}%"
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)

            # Consistent tables sheet
            if analysis_results['consistent_tables']:
                consistent_data = []
                for table_name, instance_count in analysis_results['consistent_tables']:
                    consistent_data.append({
                        'Table Name': table_name,
                        'Instances': instance_count,
                        'Status': 'Consistent'
                    })
                consistent_df = pd.DataFrame(consistent_data)
                consistent_df.to_excel(writer, sheet_name='Consistent Tables', index=False)

            # Inconsistent tables overview
            if analysis_results['inconsistent_tables']:
                inconsistent_data = []
                for table_info in analysis_results['inconsistent_tables']:
                    inconsistent_data.append({
                        'Table Name': table_info['name'],
                        'Instances': table_info['instance_count'],
                        'Common Columns': table_info['common_count'],
                        'Total Unique Columns': table_info['total_columns'],
                        'Inconsistent Columns': len(table_info['inconsistent_columns'])
                    })
                inconsistent_df = pd.DataFrame(inconsistent_data)
                inconsistent_df.to_excel(writer, sheet_name='Inconsistent Tables', index=False)

                # Detailed inconsistencies sheet
                detail_data = []
                for table_info in analysis_results['inconsistent_tables']:
                    for path_info in table_info['paths']:
                        detail_data.append({
                            'Table Name': table_info['name'],
                            'Path': path_info['path'],
                            'Missing Columns': ', '.join(sorted(path_info['missing'])) if path_info['missing'] else '',
                            'Extra Columns': ', '.join(sorted(path_info['extra'])) if path_info['extra'] else '',
                            'Issue Type': 'Missing' if path_info['missing'] else ('Extra' if path_info['extra'] else 'OK')
                        })
                detail_df = pd.DataFrame(detail_data)
                detail_df.to_excel(writer, sheet_name='Detailed Issues', index=False)

            # Original data
            df.to_excel(writer, sheet_name='Raw Data', index=False)

        return output_file
    except Exception as e:
        print(f"Warning: Could not create Excel file: {e}")
        print("You may need to install openpyxl: pip install openpyxl")
        return None


def parse_columns(column_string):
    """Parse comma-separated column string into a set.

    Args:
        column_string: String of comma-separated column names

    Returns:
        set: Set of column names
    """
    if pd.isna(column_string) or not column_string:
        return set()
    return set(col.strip() for col in column_string.split(','))


def validate_table_columns(csv_file):
    """Validate column consistency across tables.

    Args:
        csv_file: Path to the datafeed report CSV file
    """
    print("=" * 80)
    print("Table Column Consistency Validator")
    print("=" * 80)
    print()

    # Read CSV
    try:
        df = pd.read_csv(csv_file)
        print(f"✓ Loaded CSV with {len(df)} rows")
    except Exception as e:
        print(f"✗ Error reading CSV: {e}")
        sys.exit(1)

    # Filter out test paths
    original_count = len(df)
    df = df[~df['Path'].str.startswith('0000_test_parquet/')]
    filtered_count = len(df)
    print(f"✓ Filtered out {original_count - filtered_count} test rows (0000_test_parquet/)")
    print(f"✓ Analyzing {filtered_count} rows")
    print()

    if filtered_count == 0:
        print("No data to analyze after filtering.")
        return

    # Group by table name
    table_data = defaultdict(list)

    for _, row in df.iterrows():
        table_name = row['Table_Name']
        path = row['Path']
        columns = parse_columns(row['Columns'])

        table_data[table_name].append({
            'path': path,
            'columns': columns
        })

    print(f"✓ Found {len(table_data)} unique table names")
    print()
    print("=" * 80)
    print("ANALYSIS RESULTS")
    print("=" * 80)
    print()

    consistent_tables = []
    inconsistent_tables = []

    # Analyze each table
    for table_name in sorted(table_data.keys()):
        instances = table_data[table_name]

        # Get all column sets
        all_column_sets = [inst['columns'] for inst in instances]

        # Check if all column sets are identical
        if len(instances) == 1:
            # Only one instance - consider it consistent
            consistent_tables.append(table_name)
            continue

        # Find common columns (present in ALL instances)
        common_columns = set.intersection(*all_column_sets)

        # Find all columns (present in ANY instance)
        all_columns = set.union(*all_column_sets)

        # If common == all, then all instances have the same columns
        if common_columns == all_columns:
            consistent_tables.append(table_name)
        else:
            # Build path details for export
            path_details = []
            for inst in instances:
                path = inst['path']
                columns = inst['columns']
                missing = common_columns - columns
                extra = columns - common_columns
                path_details.append({
                    'path': path,
                    'missing': missing,
                    'extra': extra
                })

            inconsistent_tables.append({
                'name': table_name,
                'instances': instances,
                'common_columns': common_columns,
                'all_columns': all_columns,
                'paths': path_details
            })

    # Report consistent tables
    if consistent_tables:
        print(f"✓ CONSISTENT TABLES ({len(consistent_tables)}):")
        print("-" * 80)
        for table in consistent_tables:
            instance_count = len(table_data[table])
            print(f"  ✓ {table} ({instance_count} instance{'s' if instance_count > 1 else ''})")
        print()

    # Report inconsistent tables
    if inconsistent_tables:
        print(f"✗ INCONSISTENT TABLES ({len(inconsistent_tables)}):")
        print("-" * 80)
        print()

        for table_info in inconsistent_tables:
            table_name = table_info['name']
            instances = table_info['instances']
            common_columns = table_info['common_columns']
            all_columns = table_info['all_columns']

            print(f"✗ TABLE: {table_name}")
            print(f"  Total instances: {len(instances)}")
            print(f"  Common columns (in ALL instances): {len(common_columns)}")
            print(f"  Total unique columns (across all instances): {len(all_columns)}")
            print()

            # Find unique columns (not in common set)
            unique_columns = all_columns - common_columns

            if unique_columns:
                print(f"  ⚠ INCONSISTENT COLUMNS: {len(unique_columns)}")
                print(f"     {sorted(unique_columns)}")
                print()

            # Show which paths have which columns
            print("  Path-specific column analysis:")
            for path_info in table_info['paths']:
                path = path_info['path']
                missing = path_info['missing']
                extra = path_info['extra']

                if missing or extra:
                    print(f"    - {path}")
                    if missing:
                        print(f"      Missing: {sorted(missing)}")
                    if extra:
                        print(f"      Extra: {sorted(extra)}")
                else:
                    print(f"    ✓ {path} (has all common columns)")

            print()
            print("-" * 80)
            print()

    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total tables analyzed: {len(table_data)}")
    print(f"Consistent tables: {len(consistent_tables)}")
    print(f"Inconsistent tables: {len(inconsistent_tables)}")

    if inconsistent_tables:
        print()
        print("⚠ ACTION REQUIRED: Fix column inconsistencies in the tables listed above")
    else:
        print()
        print("✓ All tables have consistent column schemas!")

    # Prepare analysis results for export
    consistent_tables_with_counts = [(table, len(table_data[table])) for table in consistent_tables]
    inconsistent_tables_export = []

    for table_info in inconsistent_tables:
        inconsistent_tables_export.append({
            'name': table_info['name'],
            'instance_count': len(table_info['instances']),
            'common_count': len(table_info['common_columns']),
            'total_columns': len(table_info['all_columns']),
            'inconsistent_columns': table_info['all_columns'] - table_info['common_columns'],
            'paths': table_info['paths']
        })

    analysis_results = {
        'total_tables': len(table_data),
        'consistent_count': len(consistent_tables),
        'inconsistent_count': len(inconsistent_tables),
        'consistent_tables': consistent_tables_with_counts,
        'inconsistent_tables': inconsistent_tables_export
    }

    # Export results
    print()
    print("=" * 80)
    print("EXPORTING RESULTS")
    print("=" * 80)

    # HTML Export
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        html_file = f"table_validation_report_{timestamp}.html"
        export_to_html(analysis_results, html_file)
        print(f"✓ HTML report saved: {html_file}")
    except Exception as e:
        print(f"✗ Error creating HTML report: {e}")

    # Excel Export
    try:
        excel_file = f"table_validation_report_{timestamp}.xlsx"
        result = export_to_excel(analysis_results, df, excel_file)
        if result:
            print(f"✓ Excel report saved: {excel_file}")
    except Exception as e:
        print(f"✗ Error creating Excel report: {e}")

    print()
    print("✓ Analysis complete! Open the HTML file in your browser for the best viewing experience.")

    return analysis_results


def main():
    """Main function."""
    # Look for CSV file
    import glob

    csv_files = glob.glob("datafeed_report_*.csv")

    if not csv_files:
        print("✗ No datafeed report CSV found.")
        print("  Please ensure a file matching 'datafeed_report_*.csv' exists in the current directory.")
        sys.exit(1)

    # Use the most recent file
    csv_file = sorted(csv_files)[-1]
    print(f"Using CSV file: {csv_file}\n")

    validate_table_columns(csv_file)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        sys.exit(0)
