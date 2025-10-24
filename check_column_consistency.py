import pandas as pd
from datetime import datetime
import os

# Define Master Paths (same as table consistency check)
MASTER_PATH_1 = "0000_test_parquet/100007-16_Showcase/Report Documentation/Datafeed"
MASTER_PATH_2 = "999999_WeitereKDdec/128019_18_Ruegenwalder_Welle4/Report Documentation/Datafeed"

# Read the CSV file (new format: one row per column)
df = pd.read_csv('downloads/datafeed_report_.csv')
#df = pd.read_csv('downloads/parammanager_report_20251021_171403.csv')

# Ask user where to save the output file
print("=" * 80)
print("OUTPUT FILE LOCATION")
print("=" * 80)
output_dir = input("Enter output directory (press Enter for default 'downloads'): ").strip()

# Use downloads as default if no input provided
if not output_dir:
    output_dir = 'downloads'

# Create directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# HTML output file
html_filename = 'column_consistency_report.html'
html_file = os.path.join(output_dir, html_filename)

print(f"Output will be saved to: {html_file}")
print()

# The CSV now has one row per column (Column_Name field)
# We'll group by Path and Table_Name to create column sets later

print("=" * 80)
print("COLUMN CONSISTENCY CHECK")
print("=" * 80)
print()

# Get unique table names
unique_tables = df['Table_Name'].unique()
print(f"Analyzing {len(unique_tables)} unique table(s) across all paths...")
print()

# Store analysis results
table_analysis = {}

for table_name in unique_tables:
    print(f"Analyzing table: {table_name}")

    # Get all rows for this table
    table_df = df[df['Table_Name'] == table_name]

    # Get columns from master paths (new format: one row per column)
    master_1_rows = table_df[table_df['Path'] == MASTER_PATH_1]
    master_2_rows = table_df[table_df['Path'] == MASTER_PATH_2]

    # Get master column sets by collecting Column_Name values
    master_1_cols = set()
    if not master_1_rows.empty:
        master_1_cols = set(master_1_rows['Column_Name'].dropna())

    master_2_cols = set()
    if not master_2_rows.empty:
        master_2_cols = set(master_2_rows['Column_Name'].dropna())

    # Create master column set (union)
    master_column_set = master_1_cols.union(master_2_cols)

    # Analyze each path for this table
    path_results = {}

    for path, group in table_df.groupby('Path'):
        # Get all columns for this path by collecting Column_Name values
        path_cols = set(group['Column_Name'].dropna())

        # Find missing and extra columns
        missing_cols = master_column_set - path_cols
        extra_cols = path_cols - master_column_set

        path_results[path] = {
            'columns': path_cols,
            'missing': missing_cols,
            'extra': extra_cols,
            'is_consistent': len(missing_cols) == 0 and len(extra_cols) == 0
        }

    table_analysis[table_name] = {
        'master_1_cols': master_1_cols,
        'master_2_cols': master_2_cols,
        'master_column_set': master_column_set,
        'path_results': path_results,
        'source_types': table_df['Source_Type'].unique().tolist()
    }

    # Print summary
    consistent_paths = sum(1 for pr in path_results.values() if pr['is_consistent'])
    total_paths = len(path_results)
    print(f"  Master columns: {len(master_column_set)}")
    print(f"  Consistent paths: {consistent_paths}/{total_paths}")

    if consistent_paths < total_paths:
        print(f"  ⚠️ Found inconsistencies in {total_paths - consistent_paths} path(s)")
    else:
        print(f"  ✓ All paths have consistent columns")
    print()

# Summary Statistics
print("=" * 80)
print("SUMMARY STATISTICS")
print("=" * 80)
total_tables = len(table_analysis)
consistent_tables = sum(1 for ta in table_analysis.values()
                        if all(pr['is_consistent'] for pr in ta['path_results'].values()))
inconsistent_tables = total_tables - consistent_tables

print(f"Total tables analyzed: {total_tables}")
print(f"Tables with fully consistent columns: {consistent_tables}")
print(f"Tables with column mismatches: {inconsistent_tables}")
if total_tables > 0:
    consistency_pct = (consistent_tables / total_tables) * 100
    print(f"Overall consistency: {consistency_pct:.1f}%")
print()

# Generate HTML Report
html_content = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Column Consistency Report</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            line-height: 1.6;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }}

        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}

        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
        }}

        .header p {{
            font-size: 1.1em;
            opacity: 0.9;
        }}

        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            padding: 30px;
            background: #f8f9fa;
        }}

        .summary-card {{
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            text-align: center;
            transition: transform 0.3s ease;
        }}

        .summary-card:hover {{
            transform: translateY(-5px);
        }}

        .summary-card .number {{
            font-size: 3em;
            font-weight: bold;
            color: #667eea;
            margin-bottom: 10px;
        }}

        .summary-card .number.success {{
            color: #28a745;
        }}

        .summary-card .number.warning {{
            color: #ffc107;
        }}

        .summary-card .label {{
            color: #666;
            font-size: 1.1em;
        }}

        .section {{
            padding: 30px;
        }}

        .section-title {{
            font-size: 1.8em;
            color: #333;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 3px solid #667eea;
        }}

        .table-card {{
            background: white;
            border: 2px solid #e9ecef;
            border-radius: 10px;
            padding: 25px;
            margin-bottom: 25px;
            transition: all 0.3s ease;
        }}

        .table-card:hover {{
            border-color: #667eea;
            box-shadow: 0 5px 15px rgba(102, 126, 234, 0.2);
        }}

        .table-card.consistent {{
            border-color: #28a745;
            background: #f8fff9;
        }}

        .table-card.inconsistent {{
            border-color: #dc3545;
            background: #fff8f8;
        }}

        .table-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            flex-wrap: wrap;
            gap: 10px;
        }}

        .table-name {{
            font-size: 1.3em;
            color: #333;
            font-weight: 600;
            flex: 1;
            min-width: 200px;
        }}

        .badge {{
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: bold;
        }}

        .badge-success {{
            background: #d4edda;
            color: #155724;
        }}

        .badge-warning {{
            background: #fff3cd;
            color: #856404;
        }}

        .badge-danger {{
            background: #f8d7da;
            color: #721c24;
        }}

        .badge-info {{
            background: #d1ecf1;
            color: #0c5460;
        }}

        .master-info {{
            background: #e8f5e9;
            border-left: 4px solid #4caf50;
            padding: 15px;
            margin: 15px 0;
            border-radius: 5px;
        }}

        .master-info h4 {{
            color: #2e7d32;
            margin-bottom: 10px;
        }}

        .columns-list {{
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-top: 10px;
        }}

        .column-tag {{
            background: #e9ecef;
            padding: 5px 12px;
            border-radius: 15px;
            font-size: 0.85em;
            color: #495057;
            font-family: 'Courier New', monospace;
        }}

        .column-tag.master {{
            background: #d4edda;
            color: #155724;
            font-weight: 600;
        }}

        .path-section {{
            margin-top: 20px;
            padding-top: 20px;
            border-top: 2px solid #e9ecef;
        }}

        .path-card {{
            background: #f8f9fa;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 15px;
        }}

        .path-card.consistent {{
            background: #d4edda;
            border-left: 4px solid #28a745;
        }}

        .path-card.has-issues {{
            background: #fff3cd;
            border-left: 4px solid #ffc107;
        }}

        .path-card.has-missing {{
            background: #f8d7da;
            border-left: 4px solid #dc3545;
        }}

        .path-name-label {{
            font-weight: 600;
            color: #333;
            margin-bottom: 10px;
            word-break: break-all;
        }}

        .issue-section {{
            margin-top: 10px;
        }}

        .issue-section h5 {{
            color: #dc3545;
            margin-bottom: 8px;
            font-size: 0.95em;
        }}

        .issue-section.extra h5 {{
            color: #007bff;
        }}

        .issue-columns {{
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
        }}

        .missing-col {{
            background: #f8d7da;
            color: #721c24;
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 0.8em;
            font-family: 'Courier New', monospace;
        }}

        .extra-col {{
            background: #d1ecf1;
            color: #0c5460;
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 0.8em;
            font-family: 'Courier New', monospace;
        }}

        .footer {{
            background: #f8f9fa;
            padding: 20px;
            text-align: center;
            color: #666;
            border-top: 1px solid #e9ecef;
        }}

        .status-icon {{
            font-size: 1.5em;
            margin-right: 10px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 Column Consistency Report</h1>
            <p>Generated on {datetime.now().strftime("%B %d, %Y at %I:%M %p")}</p>
        </div>

        <div class="summary">
            <div class="summary-card">
                <div class="number">{total_tables}</div>
                <div class="label">Total Tables</div>
            </div>
            <div class="summary-card">
                <div class="number success">{consistent_tables}</div>
                <div class="label">Fully Consistent</div>
            </div>
            <div class="summary-card">
                <div class="number warning">{inconsistent_tables}</div>
                <div class="label">With Mismatches</div>
            </div>
            <div class="summary-card">
                <div class="number">{consistency_pct:.1f}%</div>
                <div class="label">Consistency Rate</div>
            </div>
        </div>

        <div class="section">
            <h2 class="section-title">📊 Table-by-Table Analysis</h2>
'''

# Add each table's analysis
for table_name, analysis in sorted(table_analysis.items()):
    master_cols = analysis['master_column_set']
    path_results = analysis['path_results']
    source_types = ', '.join(analysis['source_types'])

    # Determine if table is fully consistent
    is_fully_consistent = all(pr['is_consistent'] for pr in path_results.values())
    card_class = 'consistent' if is_fully_consistent else 'inconsistent'
    status_icon = '✅' if is_fully_consistent else '⚠️'
    badge_class = 'badge-success' if is_fully_consistent else 'badge-warning'

    consistent_count = sum(1 for pr in path_results.values() if pr['is_consistent'])
    total_count = len(path_results)

    html_content += f'''
            <div class="table-card {card_class}">
                <div class="table-header">
                    <div class="table-name">
                        <span class="status-icon">{status_icon}</span>
                        {table_name}
                    </div>
                    <div class="badge badge-info">{source_types}</div>
                    <div class="badge {badge_class}">{consistent_count}/{total_count} Paths Consistent</div>
                </div>

                <div class="master-info">
                    <h4>📋 Master Column Set ({len(master_cols)} columns)</h4>
'''

    # Show master path info
    if analysis['master_1_cols']:
        html_content += f'                    <p><strong>Master Path 1:</strong> {len(analysis["master_1_cols"])} columns</p>\n'
    if analysis['master_2_cols']:
        html_content += f'                    <p><strong>Master Path 2:</strong> {len(analysis["master_2_cols"])} columns</p>\n'

    html_content += '''
                    <div class="columns-list">
'''

    # Show all master columns
    for col in sorted(master_cols):
        html_content += f'                        <div class="column-tag master">{col}</div>\n'

    html_content += '''
                    </div>
                </div>

                <div class="path-section">
                    <h4 style="color: #666; margin-bottom: 15px;">Path-by-Path Comparison</h4>
'''

    # Show each path's results
    for path, result in sorted(path_results.items()):
        if result['is_consistent']:
            path_class = 'consistent'
            status = '✓ All columns match'
        elif result['missing'] and result['extra']:
            path_class = 'has-missing'
            status = f'✗ Missing {len(result["missing"])}, Extra {len(result["extra"])}'
        elif result['missing']:
            path_class = 'has-missing'
            status = f'✗ Missing {len(result["missing"])} columns'
        else:
            path_class = 'has-issues'
            status = f'+ Has {len(result["extra"])} extra columns'

        html_content += f'''
                    <div class="path-card {path_class}">
                        <div class="path-name-label">{path}</div>
                        <p><strong>{status}</strong> | Total: {len(result["columns"])} columns</p>
'''

        # Show missing columns
        if result['missing']:
            html_content += '''
                        <div class="issue-section">
                            <h5>❌ Missing Columns:</h5>
                            <div class="issue-columns">
'''
            for col in sorted(result['missing']):
                html_content += f'                                <div class="missing-col">{col}</div>\n'
            html_content += '''
                            </div>
                        </div>
'''

        # Show extra columns
        if result['extra']:
            html_content += '''
                        <div class="issue-section extra">
                            <h5>➕ Extra Columns (not in master):</h5>
                            <div class="issue-columns">
'''
            for col in sorted(result['extra']):
                html_content += f'                                <div class="extra-col">{col}</div>\n'
            html_content += '''
                            </div>
                        </div>
'''

        html_content += '''
                    </div>
'''

    html_content += '''
                </div>
            </div>
'''

html_content += f'''
        </div>

        <div class="footer">
            <p>Report generated from: datafeed_report_.csv</p>
            <p>Master Path 1: {MASTER_PATH_1}</p>
            <p>Master Path 2: {MASTER_PATH_2}</p>
            <p>Total paths analyzed: {df['Path'].nunique()}</p>
        </div>
    </div>
</body>
</html>
'''

# Write HTML file
with open(html_file, 'w', encoding='utf-8') as f:
    f.write(html_content)

print()
print("=" * 80)
print(f"✓ HTML Report generated: {html_file}")
print("=" * 80)
print()
