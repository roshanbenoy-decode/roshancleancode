# Datafeed Scanner - Demo Version

This is a **demo version** of the Datafeed Scanner web app that lets you preview the UI without any Azure setup.

## Features

✅ **No Azure Setup Required** - Runs immediately
✅ **Mock Authentication** - Auto-login as demo user
✅ **Realistic Mock Data** - Simulated datafeed scan results
✅ **Full UI Preview** - See all pages and interactions
✅ **Interactive Table** - Sortable, filterable results
✅ **CSV Download** - Export sample data

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements-web.txt
```

### 2. Run the Demo

```bash
python webapp_demo.py
```

### 3. Open Browser

Go to: **http://localhost:5000**

---

## What You'll See

### Landing Page
- Clean, professional design
- "Sign in with Microsoft" button (mock login)
- Feature highlights

### Dashboard
- Welcome message with user info
- "Start Datafeed Scan" button
- Previous results section (after first scan)

### Scan Process
- Loading modal while "scanning"
- Simulates real scan experience

### Results Page
- **Statistics cards** showing:
  - Total rows
  - Datafeed paths
  - Excel tables and columns
  - Parquet files and columns
- **Interactive table** with:
  - Sorting by any column
  - Search/filter functionality
  - Pagination
- **Download CSV** button
- **Back to Dashboard** button

---

## Mock Data Includes

The demo generates realistic data from 3 sample Datafeed folders:

1. **`0000_test_parquet/100007-16_Showcase/Report Documentation/Datafeed`**
   - Excel: DimBrand, DimProduct, DimCustomer, DimDate
   - Parquet: FactSales, FactInventory

2. **`999999_WeitereKDdec/128019_18_Ruegenwalder_Welle4/Report Documentation/Datafeed`**
   - Excel: DimStore, DimPromotion, DimSupplier
   - Parquet: FactOrders, FactReturns

3. **`555555_Analytics/DataWarehouse/Report Documentation/Datafeed`**
   - Parquet: FactWebTraffic, FactConversions

Total mock data:
- **~70 rows** (column entries)
- **3 Datafeed paths**
- **7 Excel tables**
- **6 Parquet files**

---

## Testing Checklist

Try these interactions:

- [ ] View the landing page
- [ ] Click "Sign in with Microsoft" (auto-login)
- [ ] See the dashboard
- [ ] Click "Start Datafeed Scan"
- [ ] Watch the loading spinner
- [ ] View the results page
- [ ] Check the statistics cards
- [ ] Sort the table by different columns
- [ ] Search/filter the table
- [ ] Navigate through pages (if applicable)
- [ ] Download CSV
- [ ] Open the CSV file
- [ ] Click "Back to Dashboard"
- [ ] Test logout
- [ ] Responsive design (resize browser window)

---

## Differences from Production Version

| Feature | Demo Version | Production Version |
|---------|-------------|-------------------|
| **Authentication** | Auto-login (mock) | Real Microsoft OAuth |
| **Data Source** | Mock data (hardcoded) | Real Azure Blob Storage |
| **Scan Process** | Instant (fake) | Real-time scanning |
| **User Permissions** | N/A | Based on Azure RBAC |
| **Session** | In-memory | Secure encrypted cookies |

---

## Next Steps

After previewing the UI:

1. **Like what you see?** → Proceed with Azure setup
   - Follow [AZURE_SETUP.md](AZURE_SETUP.md) for full deployment
   - Run real scans with `python webapp.py`

2. **Want changes?** → Let me know what you'd like to modify
   - UI/design changes
   - Additional features
   - Different layout

3. **Ready for production?** → Deploy to Azure App Service
   - Complete OAuth setup
   - Grant user permissions
   - Deploy the real `webapp.py`

---

## Troubleshooting

### Port Already in Use

If you see "Address already in use" error:

```bash
# Kill process on port 5000 (Windows)
netstat -ano | findstr :5000
taskkill /PID <PID> /F

# Or use a different port
# Edit webapp_demo.py line 331: app.run(port=5001)
```

### Template Not Found

Ensure you're running from the project directory:

```bash
cd "c:\Users\RoshanBenoy\Desktop\Workall\New folder"
python webapp_demo.py
```

### Missing Dependencies

Install all requirements:

```bash
pip install Flask pandas
```

---

## Demo vs Real App

**Demo App**: `webapp_demo.py`
- Mock data
- No Azure required
- Perfect for UI preview

**Real App**: `webapp.py`
- Real authentication
- Real Azure scanning
- Requires Azure setup

---

Enjoy exploring the Datafeed Scanner UI! 🚀
