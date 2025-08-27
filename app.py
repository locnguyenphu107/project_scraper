import streamlit as st
import pandas as pd
import requests
import json
import time
import datetime
import math
import io
import re

# =================================================================================
# Streamlit App UI and Logic
# =================================================================================

# --- App Title and Description ---
st.set_page_config(
    page_title="Smartlead Campaign Automation",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🦁" # Use an emoji as the icon
)
st.title("Smartlead Campaign Automation")
st.markdown("Automate campaign setup on Smartlead with a simple, user-friendly interface.")

# --- API Key from Secrets ---
try:
    API_KEY = st.secrets["SMARTLEAD_API_KEY"]
except KeyError:
    st.error("Smartlead API key not found. Please add it to your Streamlit secrets.")
    st.stop()

# =================================================================================
# Functions to Process Data
# =================================================================================

# Leads to JSON Processing
def safe_value(val):
    if pd.isna(val) or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
        return ""
    return str(val).strip()

def load_and_format(_df_list, sp_list, email_type, campaign_base_name):
    campaign_with_leads = {}
    for df, sp_name in zip(_df_list, sp_list):
        funnel_value = f"{email_type} - {campaign_base_name} ({sp_name})"
        df.loc[:, "funnel"] = funnel_value
        leads = []
        for _, row in df.iterrows():
            lead = {
                "first_name": safe_value(row.get("Name", "")),
                "email": safe_value(row.get("Email", "")),
                "website": safe_value(row.get("Domain", "")),
                "custom_fields": {
                    "merchant_name": safe_value(row.get("merchant_name", "")),
                    "SP_Selection": safe_value(row.get("SP Selection", "")),
                    "Title": safe_value(row.get("Title", "")),
                    "country": safe_value(row.get("country", "")),
                    "app": safe_value(row.get("RC", "")),
                    "country_name": safe_value(row.get("country_name", "")),
                    "first_template": safe_value(row.get("first_template", ""))
                },
            }
            leads.append(lead)
        campaign_with_leads[sp_name] = {"leads": leads, "funnel": funnel_value}
    return campaign_with_leads

# Sequences Processing
def process_bold_text(email_body, bold_texts):
    detected_bold = re.findall(r'\*\*(.*?)\*\*', email_body)
    all_bold_texts = set(detected_bold) | set(map(str.strip, bold_texts.split(","))) if pd.notna(bold_texts) else set(detected_bold)
    for text in all_bold_texts:
        email_body = email_body.replace(f"**{text}**", f"<strong>{text}</strong>")
    if pd.notna(bold_texts):
        for text in map(str.strip, bold_texts.split(",")):
             email_body = email_body.replace(text, f"<strong>{text}</strong>")
    email_body = re.sub(r'\*\*', '', email_body)
    return email_body

def process_italic_text(email_body, italic_texts):
    detected_italic = re.findall(r'\*(.*?)\*', email_body)
    all_italic_texts = set(detected_italic) | set(map(str.strip, italic_texts.split(","))) if pd.notna(italic_texts) else set(detected_italic)
    for text in all_italic_texts:
        email_body = email_body.replace(f"*{text}*", f"<em>{text}</em>")
    if pd.notna(italic_texts):
        for text in map(str.strip, italic_texts.split(",")):
             email_body = email_body.replace(text, f"<em>{text}</em>")
    email_body = re.sub(r'\*', '', email_body)
    return email_body

def process_links(email_body, link_texts):
    email_body = re.sub(r'\[(.*?)\]\((.*?)\)', r'<a href="\2">\1</a>', email_body)
    if pd.notna(link_texts):
        links = [link.strip() for link in link_texts.split(",")]
        for link in links:
            parts = link.split("|")
            if len(parts) == 2:
                text, url = parts
                email_body = email_body.replace(text, f'<a href="{url}">{text}</a>')
    return email_body

def convert_variables(email_body, subject):
    email_body = str(email_body) if not pd.isna(email_body) else ""
    subject = str(subject) if not pd.isna(subject) else ""
    replacements = {
        "store/name": "{{first_name}}", "name": "{{first_name}}", "SP": "{{SP_Selection}}",
        "Brand": "{{merchant_name}}", "brand": "{{merchant_name}}", "brand’s": "{{merchant_name}}'s",
        "country": "{{country}}", "first name": "{{first_name}}", "App": "{{app}}", "country_name": "{{country_name}}",
    }
    for key, value in replacements.items():
        email_body = email_body.replace(f'`{key}`', value)
        subject = subject.replace(f'`{key}`', value)
    return email_body, subject

def process_email_sequences(uploaded_sequences):
    """Processes sequences from an uploaded Excel file, grouping variants and formatting for the API."""
    try:
        df = pd.read_excel(uploaded_sequences)
        sequences_dict = {}

        for _, row in df.iterrows():
            seq_number = int(row["seq_number"])
            seq_delay = int(row.get("seq_delay_details", 0))
            variant_label = row.get("variant_label", "")
            subject = row.get("subject", "")
            email_body = row.get("email_body", "")
            bold_texts = row.get("bold_texts", "")
            italic_texts = row.get("italic_texts", "")
            link_texts = row.get("link_texts", "")
            
            email_body, subject = convert_variables(email_body, subject)
            email_body = process_bold_text(email_body, bold_texts)
            email_body = process_italic_text(email_body, italic_texts)
            email_body = process_links(email_body, link_texts)
            email_body = re.sub(r'\n\s*\n', '<br><br>', email_body)
            email_body = email_body.replace("\n", "<br>")
            
            if seq_number not in sequences_dict:
                sequences_dict[seq_number] = {
                    "seq_number": seq_number,
                    "seq_delay_details": {"delay_in_days": seq_delay},
                    "seq_variants": []
                }

            sequences_dict[seq_number]["seq_variants"].append({
                "subject": subject,
                "email_body": email_body,
                "variant_label": variant_label,
                "variant_distribution_percentage": 100
            })
        
        return list(sequences_dict.values())
    
    except Exception as e:
        st.error(f"Error processing sequences file: {e}")
        return []

# =================================================================================
# Core Automation Functions (Communicates with Smartlead API)
# =================================================================================

@st.cache_data(ttl=3600)  # Cache for 1 hour
def fetch_all_accounts():
    fetch_data = []
    offset = 0
    limit = 100
    while True:
        url = f"https://server.smartlead.ai/api/v1/email-accounts/?api_key={API_KEY}&offset={offset}&limit={limit}"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if not data:
                break
            fetch_data.extend(data)
            offset += limit
        else:
            raise requests.exceptions.RequestException(f"Error fetching accounts: {response.status_code} - {response.text}")
    return pd.DataFrame(fetch_data)

def campaign_creation(campaign_name):
    url = f"https://server.smartlead.ai/api/v1/campaigns/create?api_key={API_KEY}"
    headers = {"accept": "application/json", "content-type": "application/json"}
    payload = {"name": campaign_name}
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        return response.json().get("id")
    else:
        raise requests.exceptions.RequestException(f"Error creating campaign '{campaign_name}': {response.status_code} - {response.text}")

def sanitize_leads_data(leads):
    sanitized_leads = []
    for lead in leads:
        sanitized_lead = lead.copy()
        for key, value in lead["custom_fields"].items():
            if isinstance(value, (float, int)) and (math.isnan(value) or math.isinf(value)):
                sanitized_lead["custom_fields"][key] = ""
        sanitized_leads.append(sanitized_lead)
    return sanitized_leads

def add_leads_to_campaign(campaign_id, leads):
    sanitized_leads = sanitize_leads_data(leads)
    url = f"https://server.smartlead.ai/api/v1/campaigns/{campaign_id}/leads?api_key={API_KEY}"
    headers = {"accept": "application/json", "content-type": "application/json"}
    payload = {
        "lead_list": sanitized_leads,
        "settings": {
            "ignore_global_block_list": True,
            "ignore_unsubscribe_list": True,
            "ignore_duplicate_leads_in_other_campaign": False
        }
    }
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code != 200:
        raise requests.exceptions.RequestException(f"Error adding leads to campaign {campaign_id}: {response.status_code} - {response.text}")

def add_email_sequence(campaign_id, sequence_payload):
    url = f"https://server.smartlead.ai/api/v1/campaigns/{campaign_id}/sequences?api_key={API_KEY}"
    headers = {"accept": "application/json", "content-type": "application/json"}
    response = requests.post(url, json=sequence_payload, headers=headers)
    if response.status_code != 200:
        raise requests.exceptions.RequestException(f"Error adding sequence to campaign {campaign_id}: {response.status_code} - {response.text}")

def map_account_id(merged_df, sp_to_process):
    try:
        df_selected_account = merged_df[merged_df["sp"] == sp_to_process]
        if df_selected_account.empty:
            return []
        
        if "id" not in df_selected_account.columns:
            st.warning(f"Warning: 'id' column not found for SP '{sp_to_process}'. Skipping account selection.")
            return []
            
        account_list = df_selected_account["id"].dropna().tolist()
        return account_list
    except KeyError:
        return []

def account_selection(account_to_select, campaign_id):
    url = f"https://server.smartlead.ai/api/v1/campaigns/{campaign_id}/email-accounts?api_key={API_KEY}"
    headers = {"accept": "application/json", "content-type": "application/json"}
    if account_to_select:
        payload_select = {"email_account_ids": account_to_select}
        response = requests.post(url, json=payload_select, headers=headers)
        if response.status_code != 200:
            raise requests.exceptions.RequestException(f"Error adding accounts to campaign {campaign_id}: {response.status_code} - {response.text}")

def add_unsub(campaign_id):
    url = f"https://server.smartlead.ai/api/v1/campaigns/{campaign_id}/settings?api_key={API_KEY}"
    headers = {"accept": "application/json", "content-type": "application/json"}
    payload_unsub = {
        "unsubscribe_text": "Click here to opt out of this email, or reply 'Not interested' to be removed from our list",
        "stop_lead_settings": "REPLY_TO_AN_EMAIL",
        "auto_pause_domain_leads_on_reply": True
    }
    response = requests.post(url, headers=headers, json=payload_unsub)
    if response.status_code != 200:
        raise requests.exceptions.RequestException(f"Error setting up unsubscribe for campaign {campaign_id}: {response.status_code} - {response.text}")

def campaign_scheduling(campaign_id, payload):
    url = f"https://server.smartlead.ai/api/v1/campaigns/{campaign_id}/schedule?api_key={API_KEY}"
    headers = {"accept": "application/json", "content-type": "application/json"}
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code != 200:
        raise requests.exceptions.RequestException(f"Failed to schedule campaign {campaign_id}. Status: {response.status_code}, Response: {response.text}")


# =================================================================================
# Streamlit UI Form and File Uploaders
# =================================================================================

with st.sidebar:
    st.header("Campaign Setup")
    file_name = st.text_input("Campaign Base Name", "US - Footwear - Set 1")
    # email_type = st.selectbox("Email Type", ["PIC", "GEN", "ALL"])
    # time_zone = st.selectbox("Time Zone", [
    #     "America/Toronto", "America/Los_Angeles", "Europe/London", "Australia/Melbourne",
    #     "Asia/Kuala_Lumpur", "Asia/Tokyo"
    # ])

    # Create the initial list of options, adding a special "Other" choice
    email_options = ["PIC", "GEN", "ALL", "Other..."]
    time_zone_options = [
        "America/Toronto", "America/Los_Angeles", "Europe/London", "Australia/Melbourne",
        "Asia/Kuala_Lumpur", "Asia/Tokyo", "Other..."
    ]

    # --- Email Type Logic ---
    email_type_selection = st.selectbox("Email Type", email_options)

    # Check if the user selected "Other..."
    if email_type_selection == "Other...":
        # If so, display a text input field for the custom value
        custom_email_type = st.text_input("Enter a new Email Type")
        # Use the custom value if the user entered something
        email_type = custom_email_type if custom_email_type else None
    else:
        # Otherwise, use the value from the selectbox
        email_type = email_type_selection

    # --- Time Zone Logic ---
    time_zone_selection = st.selectbox("Time Zone", time_zone_options)

    # Check if the user selected "Other..."
    if time_zone_selection == "Other...":
        # If so, display a text input field for the custom value
        custom_time_zone = st.text_input("Enter a new Time Zone")
        # Use the custom value if the user entered something
        time_zone = custom_time_zone if custom_time_zone else None
    else:
        # Otherwise, use the value from the selectbox
        time_zone = time_zone_selection

    start_date = st.date_input("Schedule Start Date")
    start_time = f"{start_date}"

    sp_num = st.number_input("Number of SPs", min_value=1, value=1)
    sp_names = []
    for i in range(1, sp_num + 1):
        sp_name = st.text_input(f"SP #{i} Name", f"SP {i}")
        sp_names.append(sp_name)
    
    st.header("File Uploads")
    uploaded_leads = st.file_uploader("Upload Leads Excel File", type=["xlsx", "xls"], help="The main lead list file.")
    uploaded_sequences = st.file_uploader("Upload Sequences Excel File", type=["xlsx", "xls"], help="The email templates file.")
    uploaded_accounts = st.file_uploader("Upload Accounts Excel File", type=["xlsx", "xls"], help="The file with a list of email accounts to use.")

# =================================================================================
# Data Preview Section
# =================================================================================
if uploaded_leads:
    st.subheader("Leads File Preview")
    try:
        leads_df_preview = pd.read_excel(uploaded_leads)
        st.dataframe(leads_df_preview.head(5))
    except Exception as e:
        st.error(f"Error reading leads file for preview: {e}")

if uploaded_sequences:
    st.subheader("Sequences File Preview")
    try:
        sequences_df_preview = pd.read_excel(uploaded_sequences)
        st.dataframe(sequences_df_preview.head(5))
    except Exception as e:
        st.error(f"Error reading sequences file for preview: {e}")

if uploaded_accounts:
    st.subheader("Accounts File Preview")
    try:
        accounts_df_preview = pd.read_excel(uploaded_accounts)
        st.dataframe(accounts_df_preview.head(5))
    except Exception as e:
        st.error(f"Error reading accounts file for preview: {e}")

# =================================================================================
# Main Execution Logic
# =================================================================================
if st.button("🚀 Launch Campaign"):
    # Input validation
    if not all([file_name, email_type, time_zone, start_date, uploaded_leads, uploaded_sequences, uploaded_accounts]):
        st.error("Please fill in all the required fields and upload all three files.")
        st.stop()

    # Check if SP names are unique
    if len(set(sp_names)) != len(sp_names):
        st.error("All SP names must be unique.")
        st.stop()

    try:
        # Step 1: Process Leads Data
        with st.spinner("Step 1/5: Processing leads file..."):
            main_df = pd.read_excel(uploaded_leads)
            
            df_list = []
            sp_list_to_process = []
            for sp in sp_names:
                df_to_export = main_df[main_df["SP Selection"] == sp].copy()
                if not df_to_export.empty:
                    df_list.append(df_to_export)
                    sp_list_to_process.append(sp)
                else:
                    st.warning(f"No leads found for SP: {sp}. Skipping this SP.")

            if not df_list:
                st.error("No leads found for any of the selected SPs. Aborting.")
                st.stop()
            
            campaign_with_leads = load_and_format(df_list, sp_list_to_process, email_type, file_name)
        st.success("✅ Step 1/5: Leads processed successfully!")

        # Step 2: Fetch Accounts and Create Campaigns
        campaign_ids = {}
        with st.spinner("Step 2/5: Creating campaigns and fetching accounts..."):
            # Fetch all accounts from Smartlead API
            df_account_api = fetch_all_accounts()
            if df_account_api.empty:
                st.error("No accounts found in your Smartlead account. Aborting.")
                st.stop()
            df_account_api["sp"] = df_account_api['from_name'].apply(lambda x: x.split(' ')[0])

            # Read accounts from the uploaded file and clean the column name
            df_account_uploaded = pd.read_excel(uploaded_accounts)
            df_account_uploaded.columns = df_account_uploaded.columns.str.strip().str.lower()
            
            if 'account' not in df_account_uploaded.columns:
                st.error("The uploaded Accounts file must have a column named 'account'. Aborting.")
                st.stop()

            # Merge the two dataframes to get the specific accounts with their IDs and SPs
            merged_accounts_df = pd.merge(df_account_uploaded, df_account_api, left_on='account', right_on='username', how='left')
            merged_accounts_df = merged_accounts_df.dropna(subset=['id'])
            
            if merged_accounts_df.empty:
                st.error("No matches found between your uploaded accounts list and your Smartlead email accounts. Aborting.")
                st.stop()
            
            for sp in sp_list_to_process:
                campaign_name = f"{email_type} - {file_name} ({sp})"
                campaign_id = campaign_creation(campaign_name)
                campaign_ids[sp] = campaign_id
                st.markdown(f"**Campaign '{campaign_name}'** created with ID: `{campaign_id}`")
                time.sleep(1)
        st.success("✅ Step 2/5: Campaigns created and accounts fetched!")

        # Step 3: Add Leads to Campaigns
        with st.spinner("Step 3/5: Adding leads to campaigns..."):
            for sp, campaign_id in campaign_ids.items():
                leads = campaign_with_leads[sp]["leads"]
                if not leads:
                    st.warning(f"No leads to add for SP: {sp}. Skipping.")
                    continue
                add_leads_to_campaign(campaign_id, leads)
                st.markdown(f"Leads added to campaign: `{campaign_id}`")
                time.sleep(1)
        st.success("✅ Step 3/5: Leads added to all campaigns!")

        # Step 4: Add Email Sequences
        with st.spinner("Step 4/5: Processing and adding email sequences..."):
            sequences_payload = process_email_sequences(uploaded_sequences)
            if not sequences_payload:
                st.error("No valid sequences found in the Excel file. Aborting.")
                st.stop()
            for campaign_id in campaign_ids.values():
                add_email_sequence(campaign_id, {"sequences": sequences_payload})
                st.markdown(f"Sequences added to campaign: `{campaign_id}`")
                time.sleep(1)
        st.success("✅ Step 4/5: Email sequences added to all campaigns!")

        # Step 5: Configure and Schedule Campaigns
        with st.spinner("Step 5/5: Configuring accounts and scheduling campaigns..."):
            for sp, campaign_id in campaign_ids.items():
                # Add accounts to campaign
                account_list = map_account_id(merged_accounts_df, sp)
                if account_list:
                    account_selection(account_list, campaign_id)
                    st.markdown(f"Accounts added for SP **{sp}** to campaign `{campaign_id}`")
                else:
                    st.warning(f"No accounts found for SP: {sp}. Skipping account selection.")
                time.sleep(1)

                # Add unsubscribe text
                add_unsub(campaign_id)
                st.markdown(f"Unsubscribe text set for campaign: `{campaign_id}`")
                time.sleep(1)

                # Schedule the campaign
                schedule_payload = {
                    "timezone": time_zone,
                    "days_of_the_week": [1, 2, 3, 4, 5],
                    "start_hour": "09:00",
                    "end_hour": "16:00",
                    "min_time_btw_emails": 9,
                    "max_new_leads_per_day": 1000,
                    "schedule_start_time": start_time,
                }
                campaign_scheduling(campaign_id, schedule_payload)
                st.markdown(f"Campaign **{campaign_id}** has been scheduled successfully!")
                time.sleep(1)

        st.success("🎉 **Automation Complete!** All campaigns have been created, leads added, and campaigns scheduled.")

    except requests.exceptions.RequestException as e:
        st.error(f"API Request Failed: {e}")
        st.warning("Please check your API key, network connection, or Smartlead account status.")
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        st.warning("Please check your file formats and inputs and try again.")