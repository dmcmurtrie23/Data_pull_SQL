import streamlit as st
import pandas as pd
import numpy as np
import requests
import re
import os
from datetime import date, timedelta

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Date Select Data Pull App", page_icon="🏃", layout="centered")

st.title("🏃 Date Select Data Pull App")
st.caption("Pull session stats from Catapult OpenField, filter by date, export to CSV or push to SQL Server.")

# ── Constants ─────────────────────────────────────────────────────────────────
PARAMS = [
    "activity_id", "activity_name", "team_name", "date", "start_time",
    "day_code", "month_name", "week_number", "day_name",
    "athlete_id", "athlete_name", "first_name", "last_name", "position_name",
    "total_duration", "total_distance", "meterage_per_minute", "total_player_load",
    "velocity_band1_total_distance", "velocity_band2_total_distance",
    "velocity_band3_total_distance", "velocity_band4_total_distance",
    "velocity_band5_total_distance", "velocity_band6_total_distance",
    "velocity_band7_total_distance", "velocity_band8_total_distance",
    "max_vel", "total_load", "max_load", "percentage_max_velocity",
    "total_acceleration_load", "accel_load_density_index",
    "acceleration_density", "max_effort_acceleration",
    "max_effort_deceleration", "player_load_per_minute",
]

MD_MAPPING = {
    "MD-7": "MD-7", "MD-6": "MD-6", "MD-5": "MD-5",
    "MD-4": "MD-4", "MD-3": "MD-3", "MD-2": "MD-2", "MD-1": "MD-1",
    "MD+1": "MD+1", "MD+2": "MD+2", "MD+3": "MD+3", "MD+4": "MD+4",
    "MD": "Game Day", "GPS": "GPS",
}

# ── Catapult helpers ──────────────────────────────────────────────────────────
def process_activities(df, date_from, date_to):
    df = df.copy()
    df["tags_combined"] = df["tags"].apply(
        lambda x: "|".join([str(t) for t in x]) if isinstance(x, list) else str(x)
    )
    df["day_code"] = "Other"
    for pattern, day_code in MD_MAPPING.items():
        df.loc[df["tags_combined"].str.contains(re.escape(pattern), regex=True), "day_code"] = day_code

    df["start_dt"] = pd.to_datetime(df["start_time"], unit="s")
    df["end_dt"]   = pd.to_datetime(df["end_time"],   unit="s")
    df["date"]       = df["start_dt"].dt.date
    df["start_time"] = df["start_dt"].dt.time
    df["end_time"]   = df["end_dt"].dt.time
    df = df[df["date"] >= date_from]
    df = df[df["date"] <= date_to]
    df = df.drop(columns=["start_dt", "end_dt", "owner_id", "owner", "tags", "tag_list"], errors="ignore")
    return df


def get_stats_batch(token, params, group_by, activity_ids, batch_size=25, progress_bar=None):
    attrs = vars(token)
    base = attrs["url_base"]
    if not base.endswith("/"):
        base += "/"
    url = f"{base}stats"
    all_results = []
    batches = [activity_ids[i:i+batch_size] for i in range(0, len(activity_ids), batch_size)]
    total = len(batches)
    for i, batch in enumerate(batches):
        payload = {
            "params": params,
            "group_by": group_by,
            "filters": [{"name": "activity_id", "comparison": "=", "values": batch}],
        }
        response = requests.post(url, json=payload, headers=attrs["headers"])
        if not response.ok:
            raise requests.HTTPError(
                f"Batch {i+1}/{total} failed ({response.status_code}): {response.text[:300]}"
            )
        data = response.json()
        if isinstance(data, list):
            all_results.extend(data)
        elif isinstance(data, dict):
            for key in ("data", "rows", "results", "records"):
                if key in data and isinstance(data[key], list):
                    all_results.extend(data[key])
                    break
            else:
                all_results.append(data)
        if progress_bar is not None:
            progress_bar.progress((i + 1) / total, text=f"Fetching batch {i+1} of {total}…")
    return pd.DataFrame(all_results)


def process_stats(df, params):
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], dayfirst=True)
    if "start_time" in df.columns and pd.api.types.is_numeric_dtype(df["start_time"]):
        df["start_time"] = pd.to_datetime(df["start_time"], unit="s").dt.time
    df["week_number"] = df["date"].dt.isocalendar().week.astype(int)
    df["month_name"]  = df["date"].dt.strftime("%B")
    df["day_name"]    = df["date"].dt.strftime("%A")
    if "athlete_name" in df.columns:
        name_split = df["athlete_name"].str.split(" ", n=1, expand=True)
        df["first_name"] = name_split[0]
        df["last_name"]  = name_split[1] if 1 in name_split.columns else ""
    if "total_duration" in df.columns:
        df["total_duration_min"] = (df["total_duration"] / 60).round(2)
    numeric_cols = df.select_dtypes(include=["number"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)
    df["date"] = df["date"].dt.strftime("%m/%d/%Y")
    sort_cols = [c for c in ["athlete_name", "date"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(by=sort_cols).reset_index(drop=True)
    keep_cols = [c for c in params if c in df.columns]
    return df[keep_cols] if keep_cols else df


# ── SQL Server helpers ────────────────────────────────────────────────────────
def test_sql_connection(server, database, username, password, port=1433):
    try:
        import pyodbc
    except ImportError:
        raise ImportError("pyodbc not installed. Run: pip install pyodbc")
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server},{port};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
    )
    conn = pyodbc.connect(conn_str, timeout=5)
    conn.close()


def push_to_sql(df, server, database, username, password, table_name, if_exists, port=1433):
    try:
        from sqlalchemy import create_engine
        import urllib
    except ImportError:
        raise ImportError("sqlalchemy not installed. Run: pip install sqlalchemy pyodbc")

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server},{port};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
    )
    params = urllib.parse.quote_plus(conn_str)
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
    )
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Catapult Config")
    api_token = st.text_input(
        "API Token",
        value=os.environ.get("CATAPULT_API_TOKEN", ""),
        type="password",
        help="Your Catapult OpenField JWT token",
    )
    region = st.selectbox("Region", ["us", "eu", "au"], index=0)

    st.divider()
    st.subheader("📅 Date Range")
    col1, col2 = st.columns(2)
    with col1:
        date_from = st.date_input("From", value=date.today() - timedelta(days=90))
    with col2:
        date_to = st.date_input("To", value=date.today())

    st.divider()
    batch_size = st.slider(
        "Batch size", min_value=5, max_value=50, value=25,
        help="Reduce if you're getting 500 errors",
    )

    st.divider()
    st.subheader("🗄️ SQL Server")
    sql_server    = st.text_input("Server",   placeholder="myserver.database.windows.net")
    sql_port      = st.number_input("Port",   value=1433, step=1)
    sql_database  = st.text_input("Database", placeholder="my_database")
    sql_username  = st.text_input("Username", placeholder="sa")
    sql_password  = st.text_input("Password", type="password")
    sql_table     = st.text_input("Table name", value="catapult_stats")
    sql_if_exists = st.selectbox(
        "If table exists",
        ["append", "replace", "fail"],
        help="append = add rows | replace = drop & recreate | fail = error if exists",
    )
    test_conn_btn = st.button("🔌 Test Connection", use_container_width=True)

# Handle test connection click (outside sidebar so error shows inline too)
if test_conn_btn:
    if not all([sql_server, sql_database, sql_username, sql_password]):
        st.sidebar.error("Fill in all SQL Server fields first.")
    else:
        with st.sidebar:
            with st.spinner("Testing…"):
                try:
                    test_sql_connection(
                        sql_server, sql_database, sql_username, sql_password, int(sql_port)
                    )
                    st.success("✅ Connected!")
                except Exception as e:
                    st.error(f"❌ Failed:\n{e}")

# ── Main ──────────────────────────────────────────────────────────────────────
if not api_token:
    st.info("👈 Enter your Catapult API token in the sidebar to get started.")
    st.stop()

if date_from > date_to:
    st.error("'From' date must be before 'To' date.")
    st.stop()

fetch_btn = st.button("🔄 Fetch Data", type="primary", use_container_width=True)

if fetch_btn or "stats_df" in st.session_state:

    if fetch_btn:
        st.session_state.pop("stats_df", None)

        try:
            import CatapultPy
        except ImportError:
            st.error("CatapultPy is not installed. Run: `pip install CatapultPy`")
            st.stop()

        with st.status("Connecting to Catapult…", expanded=True) as status:

            st.write("🔑 Authenticating…")
            try:
                token = CatapultPy.ofCreateToken(api_token, region=region)
            except Exception as e:
                status.update(label="Authentication failed", state="error")
                st.error(f"Could not create token: {e}")
                st.stop()

            st.write("📋 Fetching activities…")
            try:
                activities_raw = CatapultPy.ofGetActivities(token)
                activities = process_activities(activities_raw, date_from, date_to)
            except Exception as e:
                status.update(label="Failed to fetch activities", state="error")
                st.error(f"Activities error: {e}")
                st.stop()

            if activities.empty:
                status.update(label="No activities found", state="error")
                st.warning(f"No activities found between {date_from} and {date_to}.")
                st.stop()

            st.write(f"✅ Found {len(activities)} activities")

            st.write("⏱️ Processing periods…")
            periods_list = []
            for _, row in activities.iterrows():
                if isinstance(row.get("periods"), list):
                    for period in row["periods"]:
                        periods_list.append({
                            "activity_id":     row["id"],
                            "activity_name":   row["name"],
                            "period_id":       period.get("id"),
                            "period_name":     period.get("name"),
                            "period_start_dt": pd.to_datetime(period.get("start_time"), unit="s"),
                            "period_end_dt":   pd.to_datetime(period.get("end_time"),   unit="s"),
                        })
            periods = pd.DataFrame(periods_list)
            if not periods.empty:
                periods["date"]       = periods["period_start_dt"].dt.date
                periods["start_time"] = periods["period_start_dt"].dt.time
                periods["end_date"]   = periods["period_end_dt"].dt.date
                periods["end_time"]   = periods["period_end_dt"].dt.time
                periods = periods.drop(columns=["period_start_dt", "period_end_dt"])

            activities   = activities.drop(columns=["periods"], errors="ignore")
            activity_ids = activities["id"].tolist()

            st.write(f"📊 Fetching stats in batches of {batch_size}…")
            pb = st.progress(0, text="Starting…")
            try:
                stats_raw = get_stats_batch(
                    token, params=PARAMS, group_by=["athlete", "activity"],
                    activity_ids=activity_ids, batch_size=batch_size, progress_bar=pb,
                )
            except Exception as e:
                status.update(label="Stats fetch failed", state="error")
                st.error(f"Stats error: {e}")
                st.stop()

            pb.empty()

            if stats_raw.empty:
                status.update(label="No stats returned", state="error")
                st.warning("Stats came back empty. Try a different date range.")
                st.stop()

            stats_df = process_stats(stats_raw, PARAMS)
            st.session_state["stats_df"] = stats_df
            st.session_state["date_from"] = date_from
            st.session_state["date_to"]   = date_to
            status.update(label=f"✅ Done — {len(stats_df)} rows loaded", state="complete")

    # ── Results ───────────────────────────────────────────────────────────────
    if "stats_df" in st.session_state:
        stats_df = st.session_state["stats_df"]
        d_from   = st.session_state["date_from"]
        d_to     = st.session_state["date_to"]

        st.success(f"**{len(stats_df)} rows** fetched for {d_from} → {d_to}")
        st.dataframe(stats_df, use_container_width=True, height=400)

        st.divider()
        st.subheader("Export")

        col_csv, col_sql = st.columns(2)

        with col_csv:
            csv = stats_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Download CSV",
                data=csv,
                file_name=f"catapult_stats_{d_from}_{d_to}.csv",
                mime="text/csv",
                use_container_width=True,
            )

        with col_sql:
            sql_ready = all([sql_server, sql_database, sql_username, sql_password, sql_table])
            push_btn = st.button(
                "🗄️ Push to SQL Server",
                use_container_width=True,
                disabled=not sql_ready,
                help="Fill in SQL Server details in the sidebar first" if not sql_ready else
                     f"Write {len(stats_df)} rows to {sql_database}.dbo.{sql_table}",
            )

        if not sql_ready:
            st.caption("💡 Fill in the SQL Server connection details in the sidebar to enable the push button.")

        if sql_ready and push_btn:
            with st.spinner(f"Writing {len(stats_df)} rows to `{sql_table}`…"):
                try:
                    push_to_sql(
                        df=stats_df,
                        server=sql_server,
                        database=sql_database,
                        username=sql_username,
                        password=sql_password,
                        table_name=sql_table,
                        if_exists=sql_if_exists,
                        port=int(sql_port),
                    )
                    st.success(
                        f"✅ **{len(stats_df)} rows** written to "
                        f"`{sql_database}.dbo.{sql_table}` on `{sql_server}`!"
                    )
                except ImportError as e:
                    st.error(f"Missing dependency — {e}")
                except Exception as e:
                    st.error(f"❌ SQL push failed:\n\n{e}")
