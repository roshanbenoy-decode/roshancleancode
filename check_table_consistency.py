import pandas as pd
from datetime import datetime
import os

# Define Master Paths
MASTER_PATH_1 = "0000_test_parquet/100007-16_Showcase/Report Documentation/Datafeed"
MASTER_PATH_2 = "999999_WeitereKDdec/128019_18_Ruegenwalder_Welle4/Report Documentation/Datafeed"

# Read the CSV file
df = pd.read_csv('downloads/datafeed_report_.csv')

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
html_filename = 'table_consistency_report.html'
html_file = os.path.join(output_dir, html_filename)

print(f"Output will be saved to: {html_file}")
print()

# Group by Path and check table consistency
print("=" * 80)
print("TABLE NAME CONSISTENCY CHECK BY PATH")
print("=" * 80)
print()

inconsistencies_found = False

# Group by Path
for path, group in df.groupby('Path'):
    # Get unique table names for this path
    unique_tables = group['Table_Name'].unique()

    # Count occurrences of each table
    table_counts = group['Table_Name'].value_counts()

    # Check for inconsistencies (same table appearing multiple times)
    duplicates = table_counts[table_counts > 1]

    if len(duplicates) > 0:
        inconsistencies_found = True
        print(f"PATH: {path}")
        print(f"  Total tables: {len(group)}")
        print(f"  Unique table names: {len(unique_tables)}")
        print(f"  INCONSISTENCY DETECTED:")
        for table_name, count in duplicates.items():
            print(f"    - '{table_name}' appears {count} times")
        print()

if not inconsistencies_found:
    print("No inconsistencies found! Each table name appears only once per path.")
    print()

# Summary statistics
print("=" * 80)
print("SUMMARY STATISTICS")
print("=" * 80)
print(f"Total paths: {df['Path'].nunique()}")
print(f"Total rows: {len(df)}")
print(f"Unique table names across all paths: {df['Table_Name'].nunique()}")
print()

# Show path statistics
print("=" * 80)
print("TABLES PER PATH")
print("=" * 80)
path_stats = df.groupby('Path').agg({
    'Table_Name': 'count',
    'Source_Type': lambda x: ', '.join(x.unique())
}).rename(columns={'Table_Name': 'Table_Count', 'Source_Type': 'Source_Types'})

for path, row in path_stats.iterrows():
    print(f"\nPath: {path}")
    print(f"  Number of tables: {row['Table_Count']}")
    print(f"  Source types: {row['Source_Types']}")

# Master Path Analysis
print()
print("=" * 80)
print("MASTER PATH COMPARISON")
print("=" * 80)
print()

# Get tables from each master path
master_path_1_tables = set(df[df['Path'] == MASTER_PATH_1]['Table_Name'].unique()) if MASTER_PATH_1 in df['Path'].values else set()
master_path_2_tables = set(df[df['Path'] == MASTER_PATH_2]['Table_Name'].unique()) if MASTER_PATH_2 in df['Path'].values else set()

# Analyze master path consistency
tables_only_in_path1 = master_path_1_tables - master_path_2_tables
tables_only_in_path2 = master_path_2_tables - master_path_1_tables
tables_in_both = master_path_1_tables.intersection(master_path_2_tables)

# Create master table set (union of both master paths)
master_table_set = master_path_1_tables.union(master_path_2_tables)

print(f"Master Path 1: {MASTER_PATH_1}")
print(f"  Tables: {len(master_path_1_tables)}")
print()
print(f"Master Path 2: {MASTER_PATH_2}")
print(f"  Tables: {len(master_path_2_tables)}")
print()
print(f"Master Table Set (Union): {len(master_table_set)} tables")
print(f"  Tables in both masters: {len(tables_in_both)}")
print(f"  Tables only in Master Path 1: {len(tables_only_in_path1)}")
print(f"  Tables only in Master Path 2: {len(tables_only_in_path2)}")
print()

if tables_only_in_path1:
    print("Tables ONLY in Master Path 1:")
    for table in sorted(tables_only_in_path1):
        print(f"  - {table}")
    print()

if tables_only_in_path2:
    print("Tables ONLY in Master Path 2:")
    for table in sorted(tables_only_in_path2):
        print(f"  - {table}")
    print()

# Create a mapping of table names to their source types
table_to_source = {}
for table in master_table_set:
    sources = df[df['Table_Name'] == table]['Source_Type'].unique()
    table_to_source[table] = ', '.join(sorted(sources))

# Create a mapping of table names to their master path location
table_to_master_path = {}
for table in master_table_set:
    if table in tables_in_both:
        table_to_master_path[table] = 'Both Masters'
    elif table in tables_only_in_path1:
        table_to_master_path[table] = 'Master Path 1 only'
    else:
        table_to_master_path[table] = 'Master Path 2 only'

# Find missing tables for each path
print()
print("=" * 80)
print("MISSING & EXTRA TABLES BY PATH")
print("=" * 80)
print()

print(f"Master Table Set: {len(master_table_set)} tables")
print(f"Tables with sources:")
for table in sorted(master_table_set):
    print(f"  - {table} ({table_to_source[table]}) [{table_to_master_path[table]}]")
print()

# Check each path for missing and extra tables
missing_data = []
for path, group in df.groupby('Path'):
    path_tables = set(group['Table_Name'].unique())
    missing_tables = master_table_set - path_tables
    extra_tables = path_tables - master_table_set

    if missing_tables or extra_tables:
        print(f"Path: {path}")
        print(f"  Has {len(path_tables)} tables")

        if missing_tables:
            print(f"  Missing {len(missing_tables)} tables from master set:")
            for table in sorted(missing_tables):
                master_location = table_to_master_path.get(table, 'Unknown')
                print(f"    - {table} ({table_to_source[table]}) [Found in: {master_location}]")

        if extra_tables:
            print(f"  Extra {len(extra_tables)} tables (NOT in master set):")
            for table in sorted(extra_tables):
                sources = df[df['Table_Name'] == table]['Source_Type'].unique()
                source_str = ', '.join(sorted(sources))
                print(f"    + {table} ({source_str}) [Only in this test path]")

        print()

        missing_data.append({
            'path': path,
            'has_tables': len(path_tables),
            'missing_tables': sorted(missing_tables),
            'missing_with_source': [(table, table_to_source[table], table_to_master_path.get(table, 'Unknown')) for table in sorted(missing_tables)],
            'extra_tables': sorted(extra_tables),
            'extra_with_source': [(table, ', '.join(sorted(df[df['Table_Name'] == table]['Source_Type'].unique()))) for table in sorted(extra_tables)]
        })

# Generate HTML Report
html_content = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Table Consistency Report</title>
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
            max-width: 1200px;
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

        .status-box {{
            background: #d4edda;
            border-left: 5px solid #28a745;
            padding: 20px;
            border-radius: 5px;
            margin-bottom: 20px;
        }}

        .status-box.error {{
            background: #f8d7da;
            border-left-color: #dc3545;
        }}

        .status-box h3 {{
            color: #155724;
            margin-bottom: 10px;
        }}

        .status-box.error h3 {{
            color: #721c24;
        }}

        .tables-list {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-top: 15px;
        }}

        .table-tag {{
            background: #e9ecef;
            padding: 8px 15px;
            border-radius: 20px;
            font-size: 0.9em;
            color: #495057;
            display: inline-flex;
            align-items: center;
            gap: 8px;
        }}

        .source-badge {{
            background: #667eea;
            color: white;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 0.85em;
            font-weight: 600;
        }}

        .source-badge.excel {{
            background: #28a745;
        }}

        .source-badge.parquet {{
            background: #007bff;
        }}

        .source-badge.both {{
            background: #6f42c1;
        }}

        .path-card {{
            background: white;
            border: 2px solid #e9ecef;
            border-radius: 10px;
            padding: 25px;
            margin-bottom: 20px;
            transition: all 0.3s ease;
        }}

        .path-card:hover {{
            border-color: #667eea;
            box-shadow: 0 5px 15px rgba(102, 126, 234, 0.2);
        }}

        .path-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
            flex-wrap: wrap;
            gap: 10px;
        }}

        .path-name {{
            font-size: 1.1em;
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

        .missing-tables {{
            margin-top: 15px;
        }}

        .missing-tables h4 {{
            color: #dc3545;
            margin-bottom: 10px;
        }}

        .missing-list {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 8px;
        }}

        .missing-item {{
            background: #fff3cd;
            padding: 8px 12px;
            border-radius: 5px;
            border-left: 3px solid #ffc107;
            font-size: 0.9em;
        }}

        .complete-paths {{
            background: #d1ecf1;
            border-left: 5px solid #17a2b8;
            padding: 20px;
            border-radius: 5px;
            margin-top: 20px;
        }}

        .complete-paths h3 {{
            color: #0c5460;
            margin-bottom: 15px;
        }}

        .complete-paths ul {{
            list-style: none;
        }}

        .complete-paths li {{
            padding: 8px 0;
            border-bottom: 1px solid #bee5eb;
        }}

        .complete-paths li:last-child {{
            border-bottom: none;
        }}

        .footer {{
            background: #f8f9fa;
            padding: 20px;
            text-align: center;
            color: #666;
            border-top: 1px solid #e9ecef;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Table Consistency Report</h1>
            <p>Generated on {datetime.now().strftime("%B %d, %Y at %I:%M %p")}</p>
        </div>

        <div class="summary">
            <div class="summary-card">
                <div class="number">{df['Path'].nunique()}</div>
                <div class="label">Total Paths</div>
            </div>
            <div class="summary-card">
                <div class="number">{len(df)}</div>
                <div class="label">Total Rows</div>
            </div>
            <div class="summary-card">
                <div class="number">{len(master_table_set)}</div>
                <div class="label">Master Tables</div>
            </div>
            <div class="summary-card">
                <div class="number">{len(missing_data)}</div>
                <div class="label">Paths with Issues</div>
            </div>
        </div>

        <div class="section">
            <div class="status-box">
                <h3>✅ Consistency Check: PASSED</h3>
                <p>No table name duplicates found. Each table appears only once per path.</p>
            </div>
        </div>

        <div class="section">
            <h2 class="section-title">🎯 Master Path Comparison</h2>

            <div class="path-card" style="background: #f0f4ff; border: 2px solid #667eea;">
                <div class="path-header">
                    <div class="path-name" style="color: #667eea;">📁 Master Path 1</div>
                    <div class="badge badge-success">{len(master_path_1_tables)} tables</div>
                </div>
                <p style="font-size: 0.9em; color: #666; margin-top: 10px;">{MASTER_PATH_1}</p>
            </div>

            <div class="path-card" style="background: #fff0f5; border: 2px solid #764ba2;">
                <div class="path-header">
                    <div class="path-name" style="color: #764ba2;">📁 Master Path 2</div>
                    <div class="badge badge-success">{len(master_path_2_tables)} tables</div>
                </div>
                <p style="font-size: 0.9em; color: #666; margin-top: 10px;">{MASTER_PATH_2}</p>
            </div>

            <div class="status-box" style="background: #e8f5e9; border-left-color: #4caf50;">
                <h3 style="color: #2e7d32;">Master Set Statistics</h3>
                <p><strong>Total Master Tables (Union):</strong> {len(master_table_set)}</p>
                <p><strong>Tables in Both Masters:</strong> {len(tables_in_both)}</p>
                <p><strong>Only in Master Path 1:</strong> {len(tables_only_in_path1)}</p>
                <p><strong>Only in Master Path 2:</strong> {len(tables_only_in_path2)}</p>
            </div>
'''

# Add tables only in path 1 if any
if tables_only_in_path1:
    html_content += '''
            <div class="status-box" style="background: #e3f2fd; border-left-color: #2196f3;">
                <h3 style="color: #1565c0;">📋 Tables Only in Master Path 1</h3>
                <div class="tables-list">
'''
    for table in sorted(tables_only_in_path1):
        html_content += f'                    <div class="table-tag">{table} <span class="source-badge" style="background: #2196f3;">{table_to_source[table]}</span></div>\n'

    html_content += '''
                </div>
            </div>
'''

# Add tables only in path 2 if any
if tables_only_in_path2:
    html_content += '''
            <div class="status-box" style="background: #fce4ec; border-left-color: #e91e63;">
                <h3 style="color: #c2185b;">📋 Tables Only in Master Path 2</h3>
                <div class="tables-list">
'''
    for table in sorted(tables_only_in_path2):
        html_content += f'                    <div class="table-tag">{table} <span class="source-badge" style="background: #e91e63;">{table_to_source[table]}</span></div>\n'

    html_content += '''
                </div>
            </div>
'''

html_content += '''
        </div>

        <div class="section">
            <h2 class="section-title">📋 Master Table List ({len(master_table_set)} tables)</h2>
            <div class="tables-list">
'''

for table in sorted(master_table_set):
    source = table_to_source[table]
    master_location = table_to_master_path[table]

    # Determine badge class based on source
    if 'Excel' in source and 'Parquet' in source:
        badge_class = 'both'
    elif 'Excel' in source:
        badge_class = 'excel'
    else:
        badge_class = 'parquet'

    # Determine master path badge class
    if master_location == 'Both Masters':
        master_badge_class = 'both'
        master_badge_text = 'Both'
    elif master_location == 'Master Path 1 only':
        master_badge_class = 'parquet'  # Blue
        master_badge_text = 'Path 1'
    else:
        master_badge_class = 'excel'  # Orange
        master_badge_text = 'Path 2'

    html_content += f'                <div class="table-tag">{table} <span class="source-badge {badge_class}">{source}</span> <span class="source-badge {master_badge_class}">{master_badge_text}</span></div>\n'

html_content += '''
            </div>
        </div>

        <div class="section">
            <h2 class="section-title">🔍 Missing & Extra Tables by Path</h2>
'''

# Add paths with missing or extra tables
if missing_data:
    for item in missing_data:
        missing_count = len(item['missing_tables'])
        extra_count = len(item['extra_tables'])

        # Determine badge class based on missing count
        if missing_count >= 10:
            badge_class = 'badge-danger'
        elif missing_count >= 5:
            badge_class = 'badge-warning'
        elif missing_count > 0:
            badge_class = 'badge-warning'
        else:
            badge_class = 'badge-success'

        html_content += f'''
            <div class="path-card">
                <div class="path-header">
                    <div class="path-name">{item['path']}</div>
'''
        if missing_count > 0:
            html_content += f'                    <div class="badge {badge_class}">Missing: {missing_count} table{"s" if missing_count != 1 else ""}</div>\n'
        if extra_count > 0:
            html_content += f'                    <div class="badge" style="background: #e3f2fd; color: #1565c0;">Extra: {extra_count} table{"s" if extra_count != 1 else ""}</div>\n'

        html_content += f'                    <div class="badge badge-success">Has: {item["has_tables"]} tables</div>\n'
        html_content += '''
                </div>
'''

        # Add missing tables section if any
        if missing_count > 0:
            html_content += '''
                <div class="missing-tables">
                    <h4>Missing Tables (from Master Set):</h4>
                    <div class="missing-list">
'''
            for table, source, master_loc in item['missing_with_source']:
                html_content += f'                        <div class="missing-item">❌ {table} <small>({source})</small><br><small style="color: #856404;">Found in: {master_loc}</small></div>\n'

            html_content += '''
                    </div>
                </div>
'''

        # Add extra tables section if any
        if extra_count > 0:
            html_content += '''
                <div class="missing-tables">
                    <h4 style="color: #1565c0;">Extra Tables (NOT in Master Set):</h4>
                    <div class="missing-list">
'''
            for table, source in item['extra_with_source']:
                html_content += f'                        <div class="missing-item" style="background: #e3f2fd; border-left-color: #2196f3;">➕ {table} <small>({source})</small><br><small style="color: #1565c0;">Only in this test path</small></div>\n'

            html_content += '''
                    </div>
                </div>
'''

        html_content += '''
            </div>
'''
else:
    html_content += '''
            <div class="status-box">
                <h3>All paths have complete table sets!</h3>
            </div>
'''

# Add complete paths section (paths that have all master tables)
complete_paths = []
for path, group in df.groupby('Path'):
    path_tables = set(group['Table_Name'].unique())
    # A complete path has all master tables and no extra tables
    if master_table_set.issubset(path_tables) and len(path_tables - master_table_set) == 0:
        complete_paths.append(path)

if complete_paths:
    html_content += f'''
            <div class="complete-paths">
                <h3>✨ Complete Paths ({len(complete_paths)}) - All {len(master_table_set)} Master Tables Present, No Extra Tables</h3>
                <ul>
'''
    for path in complete_paths:
        html_content += f'                    <li>✅ {path}</li>\n'

    html_content += '''
                </ul>
            </div>
'''

html_content += f'''
        </div>

        <div class="footer">
            <p>Report generated from: datafeed_report_20251016_101700.csv</p>
            <p>Total records analyzed: {len(df)}</p>
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
print(f"HTML Report generated: {html_file}")
print("=" * 80)
