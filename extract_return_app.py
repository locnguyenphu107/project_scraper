import streamlit as st
import pandas as pd
import numpy as np
import io

def process_return_apps(df_main: pd.DataFrame, df_app: pd.DataFrame):
    """
    Processes two dataframes to identify, count, and list return-related applications.

    Args:
        df_main (pd.DataFrame): The main dataframe with installed apps.
        df_app (pd.DataFrame): The dataframe with competitor names and their short codes.

    Returns:
        tuple: A tuple containing two pandas DataFrames: df_with_returns and df_multiple_returns.
    """
    # Step 1: Preprocess the main dataframe (df_main)
    st.info("Preprocessing main data...")
    try:
        df = df_main.copy()
        # Sort and remove duplicates by domain
        df = df.sort_values(by=['platform_rank', 'estimated_yearly_sales'], ascending=[False, False])
        df = df.drop_duplicates(subset=['domain'], keep='first')
        
        # Fill NaN values to avoid errors during string concatenation
        df['installed_apps_names'] = df['installed_apps_names'].fillna('')
        df['technologies'] = df['technologies'].fillna('')

        # Join the two app columns into a single column
        df['all_apps'] = df['installed_apps_names'] + ':' + df['technologies']
    except KeyError as e:
        st.error(f"Missing required column in the main data: {e}. Please check your file.")
        return None, None
        
    # Step 2: Preprocess the competitor dataframe (df_app)
    st.info("Preprocessing return app reference data...")
    try:
        # Create a mapping from the full competitor name to the short RC name
        df_app['Competitor'] = df_app['Competitor'].str.lower().str.strip()
        df_app = df_app.set_index('Competitor')
        competitor_mapping = df_app['RC'].to_dict()
    except KeyError as e:
        st.error(f"Missing required column in the return app reference file: {e}. Please check your file.")
        return None, None
    
    # Step 3: Find and list return apps for each domain
    st.info("Identifying return apps in your data...")
    
    def find_and_list_matches(app_string):
        if not isinstance(app_string, str):
            return 0, ''
        
        # Split the string by ':' and normalize each app name
        apps_list = [app.strip().lower() for app in app_string.split(':') if app.strip()]
        
        # Find matches and get their shortened names
        found_apps = [competitor_mapping.get(app) for app in apps_list if competitor_mapping.get(app)]
        
        # Return count and comma-separated string of names
        return len(found_apps), ', '.join(found_apps)

    # Apply the function and assign results to two new columns
    df[['return_app_count', 'return_app_names']] = df['all_apps'].apply(
        lambda x: pd.Series(find_and_list_matches(x))
    )

    # Step 4: Create the final dataframes
    st.info("Creating output files...")
    df_with_returns = df[df['return_app_count'] > 0].copy()
    df_multiple_returns = df[df['return_app_count'] > 1].copy()

    return df_with_returns, df_multiple_returns

def create_excel_download(df_with_returns, df_multiple_returns):
    """Creates a downloadable Excel file in memory."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_with_returns.to_excel(writer, sheet_name='Brands With Return App', index=False)
        df_multiple_returns.to_excel(writer, sheet_name='Brands With >1 Return App', index=False)
    output.seek(0)
    return output

# --- Streamlit App UI ---
st.set_page_config(page_title="Return App Identifier", layout="wide")
st.title("🛍️ Return App Identifier")
st.markdown("Upload your data and we'll identify all the return applications for you!")

# Default competitor data
default_app_data = {
    'Competitor': [
        'AfterShip Returns & Exchanges', 'Return Prime:Return & Exchange', 'ReturnGO Returns & Exchanges',
        'Happy Returns, a UPS Company', 'Sorted Returns Center', 'Swap Shipping & Returns',
        'ZigZag Returns & Exchanges', 'Narvar Return and Exchange', 'Refundid: Returns Portal',
        'Return Rabbit', 'Parcel Panel Returns &Exchange', 'Order Returns | easyReturns',
        'Baback - Exchanges & Returns', 'EcoReturns- AI Powered Returns', 'Yanet: Returns and Exchanges',
        'Rich Returns & Exchanges', 'ReturnZap Returns & Exchanges', 'Bold Returns',
        'Easy Returns Management System', 'Atomic Returns', 'Frate Returns & Recommerce',
        'Zon Customer Accounts & Return', 'Quick Returns and Exchanges', 'Keepoala: Returns & rewards',
        'COS Order Returns Manager', 'Optoro Returns & Exchanges', 'Navidium Returns & Exchanges',
        'BOXO Return', 'WeSupply Returns & Exchanges', 'IF Returns (not on storeleads)',
        'ReturnLogic', 'Ready Returns'
    ],
    'RC': [
        'AfterShip', 'Return Prime', 'ReturnGO', 'Happy Returns', 'Sorted', 'Swap',
        'ZigZag', 'Narvar', 'Refundid', 'Return Rabbit', 'Parcel Panel', 'easyReturns',
        'Baback', 'EcoReturns', 'Yanet', 'Rich', 'ReturnZap', 'Bold Returns',
        'Easy Returns', 'Atomic Returns', 'Frate', 'Zon', 'QuickReturns', 'Keepoala',
        'COS Order Returns Manager', 'Optoro', 'Navidium', 'BOXO Return',
        'WeSupply', 'IF Returns', 'ReturnLogic', 'Ready Returns'
    ]
}

# --- New Section for Displaying Default Data ---
st.subheader("Default Return App Reference List")
st.markdown("This is the list of apps used for identification if you do not upload your own file.")
st.dataframe(pd.DataFrame(default_app_data))
st.divider()

# File Uploaders
st.subheader("1. Upload Your Data File")
main_file = st.file_uploader(
    "Upload your main Excel or CSV file (must have 'domain', 'installed_apps_names', 'technologies', 'platform_rank', 'estimated_yearly_sales' columns)",
    type=["csv", "xlsx"]
)

st.subheader("2. Upload Your Return App Reference File (Optional)")
st.info("If you don't upload a file, a default list will be used.")
app_file = st.file_uploader(
    "Upload your custom return app Excel or CSV file (must have 'Competitor' and 'RC' columns)",
    type=["csv", "xlsx"]
)

if main_file:
    # Read the main data file
    if main_file.name.endswith('.csv'):
        df_main = pd.read_csv(main_file)
    else:
        df_main = pd.read_excel(main_file)

    # Determine which app reference data to use
    if app_file:
        if app_file.name.endswith('.csv'):
            df_app = pd.read_csv(app_file)
        else:
            df_app = pd.read_excel(app_file)
    else:
        df_app = pd.DataFrame(default_app_data)
        st.info("Using default return app reference data.")
    
    # Run the processing function
    if st.button("🚀 Run Analysis"):
        with st.spinner("Processing your files... This may take a moment."):
            df_with_returns, df_multiple_returns = process_return_apps(df_main, df_app)
        
        if df_with_returns is not None:
            st.success("✅ Analysis Complete!")

            # Display key metrics
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Brands with at least one return app", df_with_returns.shape[0])
            with col2:
                st.metric("Brands with more than one return app", df_multiple_returns.shape[0])

            # Download buttons
            excel_output = create_excel_download(df_with_returns, df_multiple_returns)
            st.download_button(
                label="📥 Download Processed Excel File",
                data=excel_output,
                file_name="return_app_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # Display dataframes in the app
            st.subheader("Brands with At Least One Return App")
            st.dataframe(df_with_returns)

            st.subheader("Brands with Multiple Return Apps")
            st.dataframe(df_multiple_returns)

else:
    st.warning("Please upload a main data file to begin.")