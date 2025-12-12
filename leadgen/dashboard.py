import streamlit as st


class StatusDashboard:
    """Helper to manage live UI updates during execution."""

    def __init__(self):
        self.status_container = st.empty()
        self.metrics_container = st.empty()
        self.log_container = st.container()  # For scrolling logs if debug
        self.progress_bar = st.progress(0)

        self.stats = {
            "Total Scraped": 0,
            "Enriched": 0,
            "Success": 0,
            "Skipped": 0,
            "Errors": 0,
        }

    def update_status(self, message, progress=0):
        self.status_container.info(f"🔄 {message}")
        self.progress_bar.progress(progress)

    def update_metric(self, key, increment=1):
        self.stats[key] += increment
        self.refresh_metrics()

    def refresh_metrics(self):
        cols = self.metrics_container.columns(len(self.stats))
        for col, (key, val) in zip(cols, self.stats.items()):
            col.metric(key, val)

    def log(self, msg, level="info"):
        if st.session_state.get("debug_mode", False):
            if level == "error":
                self.log_container.error(msg)
            elif level == "warning":
                self.log_container.warning(msg)
            else:
                self.log_container.text(msg)


