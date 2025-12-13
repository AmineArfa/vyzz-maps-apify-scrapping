import streamlit as st


class StatusDashboard:
    """Helper to manage live UI updates during execution."""

    def __init__(self):
        self.status_container = st.empty()
        self.metrics_container = st.empty()
        self.split_container = st.container()
        self.log_container = st.container()  # For scrolling logs if debug
        self.progress_bar = st.progress(0)

        self.stats = {
            "Total Scraped": 0,
            "Enriched": 0,
            "Success": 0,
            "Skipped": 0,
            "Errors": 0,
        }

        # Split/scrape progress (always visible, not tied to debug mode)
        self._split_rows = []
        self._split_table_placeholder = self.split_container.empty()
        self._split_stop_placeholder = self.split_container.empty()

    def init_split_view(self, *, zones, per_zone_cap, max_leads, enabled: bool):
        """Initialize (or reset) the split/scrape table."""
        self._split_rows = []
        title = "🧩 Split & Scrape"
        subtitle = ""
        if enabled and zones:
            subtitle = f"Using **Gemini split**: {len(zones)} zones, ~{per_zone_cap} places/zone, stop at **{max_leads}** unique leads."
        elif enabled and not zones:
            subtitle = f"Gemini split enabled but **not available** (invalid output or API error). Falling back to single query."
        else:
            subtitle = "Split disabled. Using single query."

        with self.split_container:
            st.markdown(f"### {title}")
            st.caption(subtitle)

        self._split_table_placeholder = self.split_container.empty()
        self._split_stop_placeholder = self.split_container.empty()
        self._render_split_table()

    def update_split_row(
        self,
        *,
        zone_index: int,
        zone: str,
        query: str,
        scraped_count: int,
        cumulative_unique: int,
        status: str,
    ):
        """Upsert a row for the current zone and re-render."""
        row = {
            "#": zone_index + 1,
            "zone": zone,
            "query": query,
            "scraped": int(scraped_count or 0),
            "cumulative_unique": int(cumulative_unique or 0),
            "status": status,
        }

        replaced = False
        for i, existing in enumerate(self._split_rows):
            if existing.get("#") == row["#"]:
                self._split_rows[i] = row
                replaced = True
                break
        if not replaced:
            self._split_rows.append(row)

        self._render_split_table()

    def set_split_stop_reason(self, reason: str):
        if reason:
            self._split_stop_placeholder.success(reason)

    def _render_split_table(self):
        # Render a stable order by zone index
        rows = sorted(self._split_rows, key=lambda r: r.get("#", 0))
        if rows:
            self._split_table_placeholder.dataframe(rows, use_container_width=True, hide_index=True)
        else:
            self._split_table_placeholder.info("Waiting for split/scrape to start...")

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


