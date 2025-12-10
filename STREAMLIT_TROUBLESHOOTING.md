# Streamlit Cloud Error Troubleshooting Guide

## Getting Detailed Error Logs

### Method 1: Streamlit Cloud Dashboard (Recommended)
1. **Go to your Streamlit Cloud dashboard**: https://share.streamlit.io/
2. **Click on your app** to open it
3. **Click "Manage app"** (button in the lower right corner of your app, or go to the dashboard)
4. **Click "Logs" tab** - This shows real-time deployment and runtime logs
5. **Look for**:
   - Build logs (showing `pip install` output)
   - Runtime logs (showing Python errors)
   - Any red error messages

### Method 2: Check Build Logs
1. In the app management page, look for **"Build logs"** or **"Deployment logs"**
2. Search for:
   - `pip install` commands
   - `ERROR` or `WARNING` messages
   - `ModuleNotFoundError` details
   - Package installation failures

### Method 3: Enable Detailed Logging in App
Add this to the top of `app.py` (temporarily) to see more details:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Verification Checklist

✅ **requirements.txt is correct:**
- File exists at repo root
- Contains: `apify-client==1.8.1`
- No typos (dash `-` not underscore `_`)
- Proper line endings

✅ **File is committed and pushed:**
- Check: `git status` shows requirements.txt is committed
- Check: Pushed to the branch Streamlit Cloud is watching (usually `main`)

✅ **Streamlit Cloud is using the correct branch:**
- In Streamlit Cloud settings, verify the branch name
- If you're on `cursor/add-apify-client-to-requirements-24ef`, make sure Streamlit is watching that branch OR merge to `main`

## Common Issues & Solutions

### Issue 1: Branch Mismatch
**Problem**: Streamlit Cloud watches `main` branch, but your changes are on a different branch.

**Solution**: 
- Merge your branch to `main`: `git checkout main && git merge cursor/add-apify-client-to-requirements-24ef`
- OR: Update Streamlit Cloud settings to watch your branch

### Issue 2: Cached Deployment
**Problem**: Streamlit Cloud might be using a cached version.

**Solution**:
1. Go to Streamlit Cloud dashboard
2. Click "Manage app"
3. Click "Settings" → "Reboot app" or "Redeploy"

### Issue 3: Python Version Mismatch
**Problem**: Package might not be compatible with Python version.

**Check**: Your `runtime.txt` specifies `python-3.11`
- Verify `apify-client==1.8.1` works with Python 3.11
- Consider updating to latest: `apify-client>=1.8.1` or `apify-client==2.3.0`

### Issue 4: Installation Order/Dependencies
**Problem**: Some packages might have conflicting dependencies.

**Solution**: Try pinning versions more specifically or updating all packages.

## Quick Test Commands

Run these locally to verify:
```bash
# Test if package can be installed
pip install apify-client==1.8.1

# Test if import works
python3 -c "from apify_client import ApifyClient; print('OK')"

# Verify requirements.txt format
pip install -r requirements.txt
```

## What to Look For in Logs

When checking Streamlit Cloud logs, look for:

1. **During build**:
   ```
   Collecting apify-client==1.8.1
   Successfully installed apify-client-1.8.1
   ```

2. **If it fails**:
   ```
   ERROR: Could not find a version that satisfies the requirement apify-client==1.8.1
   ```
   OR
   ```
   ERROR: No matching distribution found for apify-client
   ```

3. **At runtime**:
   ```
   ModuleNotFoundError: No module named 'apify_client'
   ```

## Next Steps

1. **Check the logs** using Method 1 above
2. **Share the exact error** from the logs (not the redacted one)
3. **Verify the branch** Streamlit Cloud is watching matches where your changes are
4. **Try redeploying** after confirming requirements.txt is correct
