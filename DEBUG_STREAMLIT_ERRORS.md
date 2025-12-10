# How to Get Detailed Error Logs from Streamlit Cloud

## Your Current Situation

Based on your logs, I can see:
- ✅ Dependencies are being processed: "Resolved 54 packages"
- ✅ Using `uv pip install` (newer package manager)
- ⚠️ Python 3.13.9 is being used (but runtime.txt specified 3.11)
- ❌ ModuleNotFoundError still occurs

## Step-by-Step: Getting Full Error Details

### 1. Access Detailed Build Logs

1. Go to https://share.streamlit.io/
2. Click on your app
3. Click **"Manage app"** (lower right corner or from dashboard)
4. Click **"Logs"** tab
5. Look for the **"Build logs"** section (not just runtime logs)

### 2. What to Look For in Build Logs

Search for these specific lines:

```
Collecting apify-client==1.8.1
  Downloading apify_client-1.8.1-...
Successfully installed apify-client-1.8.1
```

**OR if it fails:**
```
ERROR: Could not find a version that satisfies the requirement apify-client==1.8.1
ERROR: No matching distribution found for apify-client
```

### 3. Check Runtime Logs for Full Traceback

In the same "Logs" tab, scroll to find the **runtime error** (not the redacted one):

Look for lines like:
```
File "/mount/src/vyzz-maps-apify-scrapping/app.py", line 4, in <module>
    from apify_client import ApifyClient
ModuleNotFoundError: No module named 'apify_client'
```

The full traceback will show:
- Exact file path
- Line number
- Full error message
- Python path being used

### 4. Check Python Environment

In the logs, look for:
```
Using Python 3.13.9 environment at /home/adminuser/venv
```

This tells you:
- Which Python version is active
- Where packages are being installed

### 5. Verify Package Installation

Add this temporary debug code to `app.py` (at the very top, before imports):

```python
import sys
import subprocess
import streamlit as st

# Debug: Check Python and packages
st.write("### Debug Info")
st.write(f"Python: {sys.version}")
st.write(f"Python path: {sys.executable}")

# Try to import
try:
    from apify_client import ApifyClient
    st.success("✅ apify_client imported successfully")
except ImportError as e:
    st.error(f"❌ Import failed: {e}")
    st.write("### Installed packages:")
    result = subprocess.run([sys.executable, "-m", "pip", "list"], 
                          capture_output=True, text=True)
    st.code(result.stdout)
```

**Remove this debug code after troubleshooting!**

## Common Issues & Solutions

### Issue 1: Package Installed in Wrong Environment
**Symptom**: Logs show "Successfully installed" but import still fails

**Solution**: 
- Check if Python path in logs matches where packages are installed
- Verify the venv path: `/home/adminuser/venv`

### Issue 2: Python Version Mismatch
**Symptom**: runtime.txt says 3.11 but logs show 3.13.9

**Solution**: 
- I've updated `runtime.txt` to `python-3.12` (explicitly supported)
- Or let Streamlit use 3.13.9 but ensure package compatibility

### Issue 3: Package Name Case Sensitivity
**Symptom**: Package name looks correct but still fails

**Solution**: 
- Verified: `apify-client` (with dash) is correct in requirements.txt
- Import uses: `apify_client` (with underscore) - this is correct

### Issue 4: Cached Environment
**Symptom**: Changes don't take effect

**Solution**:
1. Go to "Manage app" → "Settings"
2. Click "Reboot app" or "Redeploy"
3. Wait for fresh build

## What I've Updated

1. ✅ **runtime.txt**: Changed from `python-3.11` to `python-3.12` (explicitly supported)
2. ✅ **requirements.txt**: Changed `apify-client==1.8.1` to `apify-client>=1.8.1` (allows newer compatible versions)

## Next Steps

1. **Commit and push** these changes:
   ```bash
   git add runtime.txt requirements.txt
   git commit -m "Update Python version and apify-client version"
   git push
   ```

2. **Redeploy** on Streamlit Cloud (or wait for auto-deploy)

3. **Check the logs** again using the steps above

4. **If still failing**, share the **exact error message** from the logs (not the redacted one)

## Quick Test: Verify Package Name

The package name in PyPI is `apify-client` (with dash).
The import statement is `from apify_client import ApifyClient` (with underscore).

This is correct! The package installs as `apify-client` but imports as `apify_client`.

## Still Having Issues?

If the error persists after these changes, please share:
1. The **full build logs** (especially the `pip install` section)
2. The **full runtime error** (not redacted)
3. The **Python version** shown in logs
4. Whether you see `apify-client` in the installed packages list
