# Checklist: What to Check in Streamlit Cloud Logs

## ✅ Your Setup (Verified)

- ✅ `requirements.txt` exists at repo root (`/workspace/requirements.txt`)
- ✅ File name is correct: `requirements.txt` (lowercase, plural)
- ✅ Contains: `apify-client>=1.8.1` (line 3)
- ✅ No other dependency files found (no uv.lock, Pipfile, pyproject.toml, environment.yml)
- ✅ `app.py` is at repo root
- ✅ Import statement: `from apify_client import ApifyClient` (correct)

## 📋 What to Check in "Manage app" → Logs

### Step 1: Open the Logs
1. Go to your app's `.streamlit.app` URL
2. Click **"Manage app"** (lower-right corner)
3. Click the **"Logs"** tab

### Step 2: Check Build Logs (Dependency Installation)

Look for the section that says:
```
📦 Processing dependencies...
```

**What to look for:**

✅ **GOOD - Package is being installed:**
```
+ apify-client==1.8.1
```
OR
```
+ apify-client==2.3.0
```
(Any version >= 1.8.1 is fine)

❌ **BAD - Package is NOT listed:**
- You see other packages like `+ streamlit==1.40.0`, `+ pandas==2.2.0`, etc.
- But `apify-client` is missing from the list

**Also check for:**
- Any ERROR messages during installation
- Warnings about package resolution
- Lines showing `uv pip install` or `pip install` output

### Step 3: Check Runtime Logs (Error Details)

Scroll to find the **runtime error** section. Look for:

```
File "/mount/src/vyzz-maps-apify-scrapping/app.py", line 4, in <module>
    from apify_client import ApifyClient
ModuleNotFoundError: No module named 'apify_client'
```

**What to check:**
1. Is the full traceback visible? (It should be in logs, not redacted)
2. What's the Python path shown?
3. Are there any other error messages before this one?

### Step 4: Verify Python Version

Look for a line like:
```
Using Python 3.13.9 environment at /home/adminuser/venv
```

**Note:** This is expected if you selected Python 3.13 in Advanced settings, even if `runtime.txt` says 3.12.

## 🔍 What to Share for Further Debugging

If `apify-client` is **NOT** in the build logs, share:

1. **The entire "Processing dependencies..." section** from build logs
2. **Any error messages** during dependency installation
3. **The file structure** showing where `requirements.txt` is located

If `apify-client` **IS** in the build logs but still fails, share:

1. **The full runtime error traceback** (from logs, not the redacted UI)
2. **The Python version** being used
3. **Any warnings** about package compatibility

## 🎯 Quick Test

After checking logs, you can also add this temporary debug code to `app.py`:

```python
import sys
import streamlit as st

# Add at the very top, before other imports
st.write("### Debug Info")
st.write(f"Python: {sys.version}")
st.write(f"Executable: {sys.executable}")

try:
    import apify_client
    st.success(f"✅ apify_client found at: {apify_client.__file__}")
except ImportError as e:
    st.error(f"❌ Import failed: {e}")
    st.write("### sys.path:")
    for p in sys.path:
        st.write(f"- {p}")
```

**Remove this after debugging!**
