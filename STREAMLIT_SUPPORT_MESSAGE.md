# Message Template for Streamlit Support

## Copy and paste this message to Streamlit Support:

---

**Subject:** ModuleNotFoundError: Need Full Error Logs and Build Details

**Message:**

Hello Streamlit Support Team,

I'm experiencing a `ModuleNotFoundError` for the `apify_client` package in my Streamlit Cloud app, but the error message shown in the app is redacted. I need access to the full, unredacted error logs to debug this issue.

**App Details:**
- Repository: [Your repo URL, e.g., https://github.com/AmineArfa/vyzz-maps-apify-scrapping]
- Branch: [Your branch name, e.g., main or cursor/add-apify-client-to-requirements-24ef]
- App URL: [Your Streamlit Cloud app URL]

**Issue:**
- Error: `ModuleNotFoundError: No module named 'apify_client'`
- Location: `app.py`, line 4: `from apify_client import ApifyClient`
- The error message shown in the app is redacted, preventing me from seeing the full traceback

**What I've Verified:**
- ✅ `requirements.txt` includes `apify-client>=1.8.1` (package name with dash is correct)
- ✅ File is at the repository root
- ✅ File is committed and pushed to the branch
- ✅ Import statement uses `apify_client` (underscore) which matches the package

**Build Logs Show:**
- Using `uv pip install`
- Python 3.13.9 environment at `/home/adminuser/venv`
- "Resolved 54 packages" message appears
- Dependencies were processed successfully

**What I Need:**
1. **Full build logs** showing:
   - Whether `apify-client` was actually installed
   - Any errors during package installation
   - Complete `pip install` or `uv pip install` output

2. **Full runtime error logs** showing:
   - Complete Python traceback (not redacted)
   - Python path being used at runtime
   - sys.path at the time of the error
   - Whether the package appears in the installed packages list

3. **Environment details:**
   - Exact Python version used at runtime (logs show 3.13.9, but runtime.txt specifies 3.12)
   - Virtual environment path
   - Whether packages are installed in the correct environment

**Questions:**
1. How can I access the full, unredacted error logs?
2. Is there a way to see the complete build logs, especially the package installation step?
3. Could there be an environment mismatch (Python 3.13.9 vs runtime.txt specifying 3.12)?
4. Is `apify-client` being installed correctly, and if so, why can't it be imported?

**Additional Context:**
- The app works locally when I install dependencies manually
- The `requirements.txt` file format has been verified (no hidden characters, correct encoding)
- I've tried redeploying/rebooting the app multiple times

Thank you for your assistance!

---

## Alternative Shorter Version (if character limit):

**Subject:** Need Full Error Logs - ModuleNotFoundError for apify_client

**Message:**

Hello,

I'm getting a redacted `ModuleNotFoundError` for `apify_client` in my Streamlit Cloud app. The build logs show "Resolved 54 packages" but the import fails at runtime.

**App:** [Your app URL]
**Repo:** [Your repo URL]
**Branch:** [Your branch]

**Request:**
1. Full build logs showing if `apify-client` was installed
2. Complete runtime error traceback (not redacted)
3. Python environment details (version, path, installed packages)

**What I've checked:**
- ✅ `requirements.txt` has `apify-client>=1.8.1` at repo root
- ✅ File is committed and pushed
- ✅ Import uses correct syntax: `from apify_client import ApifyClient`

The error message is redacted, so I can't see the full details. How can I access the complete logs?

Thank you!

---

## Key Information to Include (Fill in the blanks):

Before sending, replace these placeholders:
- `[Your repo URL]` - Your GitHub repository URL
- `[Your branch name]` - The branch Streamlit Cloud is watching
- `[Your Streamlit Cloud app URL]` - Your app's share.streamlit.io URL

## Additional Tips:

1. **Be specific**: Mention you need "unredacted" or "full" logs
2. **Include evidence**: Reference the build logs you can see (the "Resolved 54 packages" message)
3. **Show you've tried**: Mention you've verified requirements.txt and tried redeploying
4. **Ask direct questions**: What specific information do you need to debug this?

## What Support Can Provide:

Streamlit Support should be able to:
- Share full build logs from their backend
- Provide complete error tracebacks
- Check if packages are actually being installed
- Verify environment configuration
- Help identify any platform-specific issues
