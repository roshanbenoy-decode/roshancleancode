# Azure Setup Guide for Datafeed Scanner Web App

This guide walks you through setting up Azure resources and deploying the Datafeed Scanner web application with user-level OAuth authentication.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Azure App Registration Setup](#azure-app-registration-setup)
3. [Configure API Permissions](#configure-api-permissions)
4. [Grant Storage Permissions to Users](#grant-storage-permissions-to-users)
5. [Local Development Setup](#local-development-setup)
6. [Azure App Service Deployment](#azure-app-service-deployment)
7. [Testing](#testing)
8. [Troubleshooting](#troubleshooting)

---

## Prerequisites

- Azure subscription with access to:
  - Azure Active Directory (AAD)
  - Azure Storage Account (with the `30-projects` container)
  - Azure App Service (for deployment)
- Python 3.9 or higher
- Git (optional, for deployment)
- Azure CLI (optional, for command-line deployment)

---

## Azure App Registration Setup

### Step 1: Create App Registration

1. Navigate to **Azure Portal** (https://portal.azure.com)

2. Go to **Azure Active Directory** → **App registrations**

3. Click **+ New registration**

4. Fill in the registration form:
   - **Name**: `Datafeed Scanner Web App` (or your preferred name)
   - **Supported account types**: Select **Accounts in this organizational directory only (Single tenant)**
   - **Redirect URI**:
     - Platform: **Web**
     - For local development: `http://localhost:5000/callback`
     - For production: `https://your-app-name.azurewebsites.net/callback` (you'll update this later)

5. Click **Register**

6. **Copy the following values** (you'll need them later):
   - **Application (client) ID** - Copy this value
   - **Directory (tenant) ID** - Copy this value

### Step 2: Create Client Secret

1. In your App Registration, go to **Certificates & secrets**

2. Click **+ New client secret**

3. Fill in the form:
   - **Description**: `Web app secret`
   - **Expires**: Choose `24 months` (or your preferred duration)

4. Click **Add**

5. **IMPORTANT**: Copy the **Value** (client secret) immediately
   - This is shown only once!
   - Store it securely (you'll add it to environment variables)

---

## Configure API Permissions

### Step 3: Add Azure Storage Permissions

1. In your App Registration, go to **API permissions**

2. Click **+ Add a permission**

3. Select **Azure Storage**

4. Select **Delegated permissions**

5. Check the box for:
   - `user_impersonation` - Access Azure Storage on behalf of the user

6. Click **Add permissions**

7. **Grant Admin Consent** (if you have admin rights):
   - Click **Grant admin consent for [Your Organization]**
   - Click **Yes** to confirm
   - Status should change to green checkmark

   **Note**: If you don't have admin rights, ask your Azure AD administrator to grant consent.

---

## Grant Storage Permissions to Users

Each user who will use the web app needs permission to access the Azure Storage account.

### Step 4: Assign Storage Blob Data Contributor Role

1. Navigate to your **Storage Account** in Azure Portal

2. Go to **Access Control (IAM)**

3. Click **+ Add** → **Add role assignment**

4. On the **Role** tab:
   - Search for and select: **Storage Blob Data Contributor**
   - Click **Next**

5. On the **Members** tab:
   - Select **User, group, or service principal**
   - Click **+ Select members**
   - Search for and select the users who should have access
   - Click **Select**
   - Click **Next**

6. On the **Review + assign** tab:
   - Review the settings
   - Click **Review + assign**

7. Repeat for all users who will use the web app

**Alternative**: Assign to a security group containing all authorized users.

---

## Local Development Setup

### Step 5: Configure Environment Variables

1. Copy the example environment file:
   ```bash
   cp .env.example.web .env
   ```

2. Edit `.env` and fill in your values:
   ```bash
   # Azure Storage
   AZURE_STORAGE_ACCOUNT_NAME=your-storage-account-name

   # From App Registration
   AZURE_CLIENT_ID=your-application-client-id
   AZURE_CLIENT_SECRET=your-client-secret-value
   AZURE_TENANT_ID=your-directory-tenant-id

   # Flask
   FLASK_SECRET_KEY=your-random-secret-key
   FLASK_ENV=development
   ```

3. Generate a random secret key for Flask:
   ```bash
   python -c "import secrets; print(secrets.token_hex(32))"
   ```
   Copy the output and paste it as `FLASK_SECRET_KEY` value.

### Step 6: Install Dependencies

```bash
# Create a virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements-web.txt
```

### Step 7: Run Locally

```bash
python webapp.py
```

The application will be available at: `http://localhost:5000`

**Test the OAuth flow:**
1. Open http://localhost:5000
2. Click "Sign in with Microsoft"
3. You'll be redirected to Microsoft login
4. Sign in with your Azure account
5. You should be redirected back to the dashboard

---

## Azure App Service Deployment

### Step 8: Create Azure App Service

#### Option A: Using Azure Portal

1. Go to **Azure Portal** → **App Services**

2. Click **+ Create**

3. Fill in the form:
   - **Subscription**: Select your subscription
   - **Resource Group**: Create new or select existing
   - **Name**: Choose a unique name (e.g., `datafeed-scanner-app`)
   - **Publish**: **Code**
   - **Runtime stack**: **Python 3.11**
   - **Operating System**: **Linux**
   - **Region**: Choose your preferred region
   - **Pricing plan**: Select appropriate plan (B1 or higher recommended)

4. Click **Review + create** → **Create**

5. Wait for deployment to complete

#### Option B: Using Azure CLI

```bash
# Login to Azure
az login

# Create resource group (if needed)
az group create --name datafeed-scanner-rg --location eastus

# Create App Service plan
az appservice plan create \
  --name datafeed-scanner-plan \
  --resource-group datafeed-scanner-rg \
  --sku B1 \
  --is-linux

# Create web app
az webapp create \
  --name datafeed-scanner-app \
  --resource-group datafeed-scanner-rg \
  --plan datafeed-scanner-plan \
  --runtime "PYTHON:3.11"
```

### Step 9: Configure App Service Settings

1. Go to your App Service in Azure Portal

2. Navigate to **Configuration** → **Application settings**

3. Click **+ New application setting** for each of the following:

   | Name | Value |
   |------|-------|
   | `AZURE_STORAGE_ACCOUNT_NAME` | Your storage account name |
   | `AZURE_CLIENT_ID` | Your App Registration client ID |
   | `AZURE_CLIENT_SECRET` | Your client secret |
   | `AZURE_TENANT_ID` | Your tenant ID |
   | `FLASK_SECRET_KEY` | Random secret key (generate new one for production) |
   | `FLASK_ENV` | `production` |
   | `SCM_DO_BUILD_DURING_DEPLOYMENT` | `true` |

4. Click **Save** → **Continue**

### Step 10: Update Redirect URI

1. Go back to your **App Registration** in Azure AD

2. Navigate to **Authentication**

3. Under **Redirect URIs**, add your production URL:
   - `https://datafeed-scanner-app.azurewebsites.net/callback`
   - (Replace `datafeed-scanner-app` with your actual app name)

4. Click **Save**

### Step 11: Deploy Application Code

#### Option A: Using Git Deployment

```bash
# In your project directory
git init
git add .
git commit -m "Initial commit"

# Configure deployment
az webapp deployment source config-local-git \
  --name datafeed-scanner-app \
  --resource-group datafeed-scanner-rg

# Get deployment URL
az webapp deployment list-publishing-credentials \
  --name datafeed-scanner-app \
  --resource-group datafeed-scanner-rg

# Add Azure remote and push
git remote add azure <deployment-url>
git push azure master
```

#### Option B: Using ZIP Deployment

```bash
# Create a ZIP file of your code
zip -r app.zip . -x "venv/*" ".git/*" ".env" "__pycache__/*"

# Deploy
az webapp deployment source config-zip \
  --resource-group datafeed-scanner-rg \
  --name datafeed-scanner-app \
  --src app.zip
```

#### Option C: Using VS Code Extension

1. Install **Azure App Service** extension for VS Code
2. Sign in to Azure
3. Right-click your App Service
4. Select **Deploy to Web App**
5. Select your project folder

### Step 12: Verify Deployment

1. Go to your App Service URL: `https://your-app-name.azurewebsites.net`

2. You should see the Datafeed Scanner landing page

3. Test the full flow:
   - Click "Sign in with Microsoft"
   - Authenticate with your Azure account
   - Click "Start Datafeed Scan"
   - Wait for results
   - Download CSV

---

## Testing

### Test Checklist

- [ ] **Local Development**
  - [ ] App runs without errors
  - [ ] OAuth login works
  - [ ] Scan executes successfully
  - [ ] Results display correctly
  - [ ] CSV download works

- [ ] **Azure Deployment**
  - [ ] App is accessible via HTTPS
  - [ ] OAuth redirect works (no CORS errors)
  - [ ] User authentication succeeds
  - [ ] Scan completes with user's credentials
  - [ ] Results persist across requests
  - [ ] CSV download works

- [ ] **Multiple Users**
  - [ ] Different users can log in
  - [ ] Each user's session is isolated
  - [ ] Users with permissions can scan
  - [ ] Users without permissions see appropriate error

---

## Troubleshooting

### Common Issues

#### 1. "AZURE_CLIENT_ID environment variable is required"

**Cause**: Environment variables not set correctly

**Solution**:
- Local: Check your `.env` file
- Azure: Verify Application Settings in App Service Configuration

---

#### 2. "Invalid state parameter" error during login

**Cause**: Session cookie issues or CSRF protection triggered

**Solution**:
- Ensure cookies are enabled in browser
- For Azure deployment, ensure `SESSION_COOKIE_SECURE` is set correctly
- Clear browser cookies and try again

---

#### 3. "Authentication failed" or redirect errors

**Cause**: Redirect URI mismatch

**Solution**:
- Verify redirect URI in App Registration matches your app URL exactly
- Local: `http://localhost:5000/callback`
- Azure: `https://your-app-name.azurewebsites.net/callback`
- Ensure no trailing slashes

---

#### 4. "Permission denied" when scanning

**Cause**: User doesn't have Storage Blob Data Contributor role

**Solution**:
- Grant the role as described in Step 4
- Wait 5-10 minutes for permissions to propagate
- User may need to log out and log back in

---

#### 5. Scan takes too long or times out

**Cause**: Default timeout too short for large scans

**Solution**:
- In Azure App Service, increase timeout in startup.txt: `--timeout 600` (10 minutes)
- For very large scans, consider implementing background job processing

---

#### 6. "Application Error" on Azure

**Cause**: Application failed to start

**Solution**:
1. Check logs in Azure Portal:
   - App Service → **Log stream**
   - Or **Advanced Tools (Kudu)** → **LogFiles**

2. Common fixes:
   - Ensure `startup.txt` is present
   - Verify `requirements-web.txt` is complete
   - Check environment variables are set

---

### Viewing Logs

#### Azure Portal
1. Go to your App Service
2. Navigate to **Monitoring** → **Log stream**
3. View real-time application logs

#### Azure CLI
```bash
az webapp log tail \
  --name datafeed-scanner-app \
  --resource-group datafeed-scanner-rg
```

#### Kudu Console
1. Go to `https://your-app-name.scm.azurewebsites.net`
2. Navigate to **Debug console** → **CMD** or **PowerShell**
3. Check `/home/LogFiles/` directory

---

## Security Best Practices

1. **Never commit `.env` file** to version control
   - Add `.env` to `.gitignore`

2. **Use strong secret keys** in production
   - Generate unique keys: `python -c "import secrets; print(secrets.token_hex(32))"`

3. **Rotate client secrets** periodically
   - Set expiration dates in Azure AD
   - Update App Service configuration when rotating

4. **Use HTTPS only** in production
   - Azure App Service provides HTTPS by default

5. **Limit user access**
   - Only grant Storage permissions to authorized users
   - Review permissions regularly

6. **Monitor application logs**
   - Enable Application Insights (optional)
   - Review logs for suspicious activity

---

## Next Steps

- [ ] Set up monitoring and alerting (Application Insights)
- [ ] Configure custom domain (optional)
- [ ] Set up staging slot for testing updates
- [ ] Implement automated deployments (GitHub Actions/Azure DevOps)
- [ ] Add more features (scan history, email notifications, etc.)

---

## Support

For issues or questions:
- Check the [Troubleshooting](#troubleshooting) section
- Review Azure documentation: https://docs.microsoft.com/azure
- Check MSAL Python documentation: https://github.com/AzureAD/microsoft-authentication-library-for-python

---

**Congratulations!** Your Datafeed Scanner web app is now set up and deployed with user-level OAuth authentication.
