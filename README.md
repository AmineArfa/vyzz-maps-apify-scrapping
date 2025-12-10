# Lead Generation Engine

## 1. Local Setup

1.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configure Secrets:**
    - Rename `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml`.
    - Fill in your API keys.

3.  **Run Locally:**
    ```bash
    streamlit run app.py
    ```

## 2. Deployment (Streamlit Community Cloud)

1.  **Push to GitHub:**
    - Create a repository on GitHub.
    - Push `app.py`, `requirements.txt`, and this `README.md`.
    - **IMPORTANT:** Do NOT push `.streamlit/secrets.toml` (add it to `.gitignore` if needed).

2.  **Deploy:**
    - Go to [share.streamlit.io](https://share.streamlit.io/).
    - Log in with GitHub.
    - Click **"New app"**.
    - Select your repository, branch (usually `main`), and main file path (`app.py`).

3.  **Configure Secrets on Cloud:**
    - Before clicking "Deploy" (or in App Settings after):
    - Click **"Advanced settings"** (or "Settings" -> "Secrets").
    - Copy the contents of your local `secrets.toml` and paste them into the secrets text area.
    - Click **"Save"**.

4.  **Launch:**
    - Click **"Deploy"**.
    - The app should be live in a few minutes!
