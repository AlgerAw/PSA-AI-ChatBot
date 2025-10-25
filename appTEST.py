import os
import re
import json
import requests
import msal
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# -------------------- Load Environment --------------------
load_dotenv()

# Power BI Credentials
TENANT     = os.getenv("PBI_TENANT_ID")
CLIENT_ID  = os.getenv("PBI_CLIENT_ID")
CLIENT_SEC = os.getenv("PBI_CLIENT_SECRET")
GROUP_ID   = os.getenv("PBI_WORKSPACE_ID")
REPORT_ID  = os.getenv("PBI_REPORT_ID")
DATASET_ID = os.getenv("PBI_DATASET_ID")

# Azure OpenAI Credentials
KEY = os.getenv("API_KEY")
DEPLOYMENT = os.getenv("API_DEPLOYMENT")
API_VERSION = os.getenv("API_VERSION")
BASE = f"https://psacodesprint2025.azure-api.net/openai/deployments/{DEPLOYMENT}/chat/completions"

AUTHORITY = f"https://login.microsoftonline.com/{TENANT}"
SCOPE     = ["https://analysis.windows.net/powerbi/api/.default"]
PBI_API   = "https://api.powerbi.com/v1.0/myorg"

# -------------------- Auth Helpers --------------------
def aad_token():
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SEC
    )
    tok = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in tok:
        raise RuntimeError(f"AAD auth failed: {tok}")
    return tok["access_token"]

def _hdrs():
    return {
        "Authorization": f"Bearer {aad_token()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

# -------------------- Azure OpenAI Helper --------------------
def ask(prompt, system="You are a helpful PSA port analytics assistant.", **kwargs):
    payload = {
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        "temperature": 1,
        "max_completion_tokens": 1000,
    }
    payload.update(kwargs)

    r = requests.post(
        BASE,
        params={"api-version": API_VERSION},
        headers={"Content-Type": "application/json", "api-key": KEY},
        json=payload,
        timeout=(10, 120),
    )
    r.raise_for_status()
    data = r.json()
    choice = data.get("choices", [{}])[0]
    return choice.get("message", {}).get("content", "").strip()

# -------------------- Power BI Query Engine --------------------
SCHEMA = {
    "table": "data",
    "categorical": {
        "Operator", "Service", "Dir", "BU", "Vessel", "From", "To",
        "Berth Status", "Arrival Accuracy (Final BTR)"
    },
    "numeric": {
        "IMO", "Rotation No.",
        "Arrival Variance (within 4h target)",
        "Wait Time (Hours): ATB-BTR",
        "Wait Time (Hours): ABT-BTR",
        "Wait Time (hours): ATB-ABT",
        "Berth Time (hours): ATU - ATB",
        "Assured Port Time Achieved (%)",
        "Bunker Saved (USD)",
        "Carbon Abatement (Tonnes)",
        "Year", "Month"
    },
    "datetime": {
        "BTR as at 96h to ATB",
        "Final BTR (Local Time)",
        "ABT (Local Time)",
        "ATB (Local Time)",
        "ATU (Local Time)"
    }
}

def get_dataset_schema():
    return {
        "table": SCHEMA["table"],
        "columns": (
            [{"name": c, "dataType": "String"}   for c in SCHEMA["categorical"]] +
            [{"name": c, "dataType": "Double"}   for c in SCHEMA["numeric"]] +
            [{"name": c, "dataType": "DateTime"} for c in SCHEMA["datetime"]]
        ),
        "by_type": {
            "categorical": set(SCHEMA["categorical"]),
            "numeric":     set(SCHEMA["numeric"]),
            "datetime":    set(SCHEMA["datetime"]),
        }
    }

def _best_column_guess(token: str):
    schema = get_dataset_schema()
    all_cols = {c["name"] for c in schema["columns"]}
    for c in all_cols:
        if token.lower() in c.lower():
            return c
    return None

def dax_query(dax: str):
    if not DATASET_ID:
        return {"error": "No DATASET_ID set."}
    url = f"{PBI_API}/groups/{GROUP_ID}/datasets/{DATASET_ID}/executeQueries"
    body = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
    r = requests.post(url, headers=_hdrs(), json=body, timeout=90)
    if not r.ok:
        return {"error": f"DAX error ({r.status_code}): {r.text}", "dax": dax}
    return dict(r.json(), **{"dax": dax})

def build_dax(q: str) -> str:
    """Simple natural language → DAX translator"""
    if "top" in q.lower() and "by" in q.lower():
        m = re.search(r"top\s+(\d+)\s+(.+?)\s+by\s+(.+)", q, re.I)
        n = int(m.group(1)) if m else 10
        group_col = _best_column_guess(m.group(2)) if m else "BU"
        metric_col = _best_column_guess(m.group(3)) if m else "Bunker Saved (USD)"
        return f"""
        EVALUATE
        TOPN({n},
            ADDCOLUMNS(
                SUMMARIZECOLUMNS('{SCHEMA["table"]}'[{group_col}]),
                "Metric", CALCULATE(SUM('{SCHEMA["table"]}'[{metric_col}]))
            ),
            [Metric], DESC
        )
        """
    # Fallback
    col = next(iter(SCHEMA["categorical"]))
    return f"""
    EVALUATE
    TOPN(20,
        ADDCOLUMNS(
            SUMMARIZECOLUMNS('{SCHEMA["table"]}'[{col}]),
            "Count", COUNTROWS('{SCHEMA["table"]}')
        ),
        [Count], DESC
    )
    """

def ask_with_powerbi(user_q: str):
    dax = build_dax(user_q)
    res = dax_query(dax)
    if "error" in res:
        return res["error"]
    try:
        rows = res["results"][0]["tables"][0]["rows"]
        return pd.DataFrame(rows)
    except Exception as e:
        return f"Unable to parse data: {e}"

# -------------------- Streamlit UI --------------------
st.set_page_config(page_title="Power BI Chatbot", page_icon="💬", layout="wide")
st.title("💬 Power BI Conversational Dashboard")

with st.sidebar:
    st.image("psa-logo.png", width=120)
    st.markdown("### PSA Code Sprint 2025")
    st.divider()
    st.markdown("Team: **Smelly Monkeys**")
    st.divider()
    st.caption("Filters (visual only)")
    kpi = st.selectbox("KPI", ["Arrival Accuracy", "Berth Time Savings", "Carbon Savings", "Calls Made"], index=0)

tab_dash, tab_chat, tab_insights = st.tabs(["🖥️ Dashboard", "💬 Chatbot", "✨ Quick Insights"])

# --- Dashboard Tab ---
with tab_dash:
    st.subheader("Embedded Power BI Dashboard")
    st.components.v1.iframe(
        f"https://powerbiembeddedexample-gsffd0h3fxe2hmgm.southeastasia-01.azurewebsites.net/",
        height=820
    )

# --- Chatbot Tab ---
with tab_chat:
    st.subheader("Chat with Power BI Data")
    user_q = st.text_input("Ask about the data (e.g., 'Top 10 BU by Bunker Saved (USD)')")
    if user_q:
        st.write("🔍 Generating DAX and querying Power BI…")
        df_or_msg = ask_with_powerbi(user_q)
        if isinstance(df_or_msg, pd.DataFrame):
            st.dataframe(df_or_msg)
            insight_prompt = f"Summarize the following Power BI data and give actionable insights:\n{df_or_msg.head(15).to_string(index=False)}"
            summary = ask(insight_prompt)
            st.markdown("### 💡 AI Insights")
            st.write(summary)
        else:
            st.error(df_or_msg)

# --- Quick Insights Tab ---
with tab_insights:
    st.subheader("Automatic Insights")
    st.caption("AI-generated overview based on Power BI data schema.")
    base_prompt = """
    You are an AI-powered conversational interface that interprets the dashboard and delivers actionable insights.
    Summarize the key insights, observations, or changes by using clear business language.
    Then suggest a few actionable next steps aligned with PSA's global strategy.
    PSA’s global strategy connects individual terminals into a digitally integrated global network. This enables real-time visibility,
    operational synergy and sustainability across the supply chain. The Global Insights dashboard provides key metrics
    such as berth time savings, arrival accuracy, carbon savings, with drill-down views to vessel and business unit performance.
    """
    summary = ask(base_prompt)
    st.markdown(summary)
