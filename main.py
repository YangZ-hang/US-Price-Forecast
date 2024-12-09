import streamlit as st
from get_data import DataFetcher
from forecast_performance import visualization_export
from forecast_performance import error_table_hour_bucket
from forecast_performance import error_table_price_bucket
import sys
import os
sys.path.append('Streamlit exploration')
from get_data import DataFetcher
import boto3


# Set Streamlit to wide layout
st.set_page_config(layout="wide", page_title="US RTLMP Forecast Monitoring - Weekly Report")

st.write("# US RTLMP Forecast Monitoring - Weekly Report")

# Simulate data-fetching logic
def fetch_data():
    config_path = config_path = r"C:\Users\UI154044\OneDrive - RWE\Desktop\3rd Rotation\US Power Price & Battery\Yang test Weekly\Code Updated\Streamlit exploration\config.json"
    Raw_Data = DataFetcher(config_path)
    with st.spinner("Fetching data..."):
        import time
        time.sleep(2)  # Simulating a delay
        df_dic = Raw_Data.get_data()
    return "Raw data fetched successfully!", df_dic


# Streamlit app
st.subheader("Data Fetching, Filtering, and Visualization")

def refresh_session_state():
    # Initialize session state
    if "data_fetched" not in st.session_state:
        st.session_state["data_fetched"] = False
        st.session_state["df_dic"] = None

    if not st.session_state["data_fetched"]:
        st.write("Data is being fetched automatically...")
        try:
            message, df_dic = fetch_data()
            st.session_state["data_fetched"] = True
            st.session_state["df_dic"] = df_dic
            st.success(message)
        except Exception as e:
            st.error(f"An error occurred while fetching data: {e}")
            st.session_state["df_dic"] = None
    else:
        st.write("Data has already been fetched.")
        st.write("To fetch the data again, refresh the page.")
        # Button to re-fetch data
        if st.button("Re-fetch Data"):
            try:
                message, df_dic = fetch_data()
                st.session_state["df_dic"] = df_dic  # Update session state with new data
                st.session_state["data_fetched"] = True
                st.success(message)
            except Exception as e:
                st.error(f"An error occurred while re-fetching data: {e}")            

    return st.session_state["df_dic"]

df_dic = refresh_session_state()
#print(df_dic)

if df_dic is not None:
    #df_source = pd.read_csv(df_dic)
    columns = []
    for battery, df in df_dic.items():
        #st.subheader(f'Data preview for {battery}')
        #st.write(df.head(10))
        columns.append(battery)  
        # Collect column names from each dataframe

    st.subheader(f'Filter data in Battery')
    selected_columns = st.selectbox('Select columns to filter by', columns)
    selected_df = df_dic[selected_columns]
    st.write(f'Preview the first 20 rows of {selected_columns}')
    st.write(selected_df.head(20))
    
    # Add a grid line to separate sections
    st.markdown("<hr style='border: 1px solid #ccc; margin: 20px 0;'>", unsafe_allow_html=True)

    try:
        date_range, lineplot_dic, boxplot_dic, fig_enertel, fig_dalmp, fig_RT, fig_DA = visualization_export(df_dic)
        error_tables_hour = error_table_hour_bucket(df_dic)
        error_tables_price = error_table_price_bucket(df_dic)
        st.subheader(f'Forecast Comparison of {selected_columns}')
        st.write(f'{date_range}')
        # Create a two-column layout
        col1, col2 = st.columns(2)
        # Left column: fig_enertel on top of fig_dalmp
        with col1:
            st.plotly_chart(fig_enertel)  # Display fig_enertel
            st.plotly_chart(fig_dalmp)  # Display fig_dalmp
        # Right column: fig_RT on top of fig_DA
        with col2:
            st.plotly_chart(fig_RT)  # Display fig_RT
            st.plotly_chart(fig_DA)  # Display fig_DA

        # Add a grid line to separate sections
        st.markdown("<hr style='border: 1px solid #ccc; margin: 20px 0;'>", unsafe_allow_html=True)
        
        st.subheader(f'ME, RMSE, and MAE for {selected_columns} on different time buckets')
        if error_tables_hour:
            for battery_name, error_data_hour in error_tables_hour.items():

                if battery_name == selected_columns:
                    st.write(f"**Battery: {battery_name}**")
                    # Create the table header
                    header_col1, header_col2, header_col3, header_col4= st.columns([1, 2.5, 2.5, 2.5])
                    header_col1.write(" ")
                    header_col2.write("**ME Chart**")
                    header_col3.write("**RMSE Chart**")
                    header_col4.write("**MAE Chart**")


                    for error in error_data_hour:
                        # Create the table body
                        col1, col2, col3, col4= st.columns([1, 2.5, 2.5, 2.5])
                        # Combine "Hour Bucket" and "Count" in the first column
                        hour_bucket = error.get("Hour Bucket", "N/A")
                        count = error.get("Count", "N/A")
                        col1.markdown(f"""
                        <div style='font-size:12px; line-height:1.4;margin-bottom:5px;'>
                        <b>Hour Bucket:</b> {hour_bucket}<br>
                        <b>Count:</b> {count}
                        </div>
                        """, unsafe_allow_html=True)
                        #print(error["fig"])
                        col2.plotly_chart(error["fig"][0], use_container_width=True)
                        col3.plotly_chart(error["fig"][1], use_container_width=True)
                        col4.plotly_chart(error["fig"][2], use_container_width=True)
                else: 
                    continue
        else:
            st.write("No error table data available.")
        
        
        # Add a grid line to separate sections
        st.markdown("<hr style='border: 1px solid #ccc; margin: 20px 0;'>", unsafe_allow_html=True)
        

        st.subheader(f'ME, RMSE, and MAE for {selected_columns} on different price buckets')
        if error_tables_price:
            for battery_name, error_data_price in error_tables_price.items():
                if battery_name == selected_columns:
                    st.write(f"**Battery: {battery_name}**")
                    # Create the table header
                    header_col1, header_col2, header_col3, header_col4= st.columns([1, 2.5, 2.5, 2.5])
                    header_col1.write(" ")
                    header_col2.write("**ME Chart**")
                    header_col3.write("**RMSE Chart**")
                    header_col4.write("**MAE Chart**")
                    for error in error_data_price:
                        # Create the table body
                        col1, col2, col3, col4= st.columns([1, 2.5, 2.5, 2.5])
                        # Combine "Price Bucket" and "Count" in the first column
                        price_bucket = error.get("Price Bucket", "N/A")
                        count = error.get("Count", "N/A")
                        col1.markdown(f"""
                        <div style='font-size:12px; line-height:1.4;margin-bottom:5px;'>
                        <b>Price Bucket:</b> {price_bucket}<br>
                        <b>Count:</b> {count}
                        </div>
                        """, unsafe_allow_html=True)
                        col2.plotly_chart(error["fig"][0], use_container_width=True)
                        col3.plotly_chart(error["fig"][1], use_container_width=True)
                        col4.plotly_chart(error["fig"][2], use_container_width=True)
                else: 
                    continue
    except Exception as e:
        st.error(f"An error occurred during visualization: {e}")
        
else:
    st.error("Data has not been fetched yet. Please refresh the page.")
    st.stop()
    


