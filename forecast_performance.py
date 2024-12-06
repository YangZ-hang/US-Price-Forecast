import numpy as np
import pandas as pd
import json
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def line_plot(df, forecast_proxy):
    Forecast_error = df.iloc[:,1] - forecast_proxy
    forecast_proxy_name = forecast_proxy.name

    # Filter the dataframe to include only the last week's data
    last_week_data = df[df.index >= (df.index.max() - pd.Timedelta(weeks=1))]
    #last_week_data = last_week_data[last_week_data.iloc[:, 1] <= 1000]
    label = "Date Range: " + str(last_week_data.index.min()) + " to " + str(last_week_data.index.max()) #+ "  Outlier >1000 removed"
    date_range = label
    
    #print(last_week_data)

    # Create subplots with shared x-axis
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.2, 
                        subplot_titles=("Forecast Proxy", "Forecast Error"))

    # Add Forecast Proxy plot
    fig.add_trace(go.Scatter(x=last_week_data.index, y=last_week_data.iloc[:, 1], mode='lines', name='RTLMP', line=dict(color='darkblue')), row=1, col=1)
    fig.add_trace(go.Scatter(x=last_week_data.index, y=forecast_proxy[last_week_data.index], mode='lines', name= forecast_proxy_name, line=dict(color='red')), row=1, col=1)

    # Add Forecast Error plot
    fig.add_trace(go.Scatter(x=last_week_data.index, y=Forecast_error[last_week_data.index], mode='lines', name='Forecast Error', line=dict(color='black')), row=2, col=1)
    # Add a horizontal dotted/dashed line at y=0 in the error chart
    fig.add_shape(type="line", x0=last_week_data.index.min(), y0=0, x1=last_week_data.index.max(), y1=0, line=dict(
            color="red",
            width=2,
            dash="dashdot",
        ),
        row=2,
        col=1
    )
    fig.update_layout(title=label, xaxis2_title='Timestamp', yaxis_title='Value', template='plotly_white')
    #fig.show()

    return fig, date_range


def box_plot(df, proxy, ylabel):
    # Ensure the index is a datetime index
    df.index = pd.to_datetime(df.index)

    # Create a new column for the hour of the day
    df['Hour'] = df.index.hour

    # Print the DataFrame for debugging
    #print("DataFrame head:\n", df.head())
    #print("DataFrame index length:", len(df.index))
    #print("DataFrame 'Hour' column length:", len(df['Hour']))

    # Group the data by hour and create box plots
    fig = go.Figure()

    for hour in range(24):
        hour_data = df[df['Hour'] == hour]
        # Remove outliers larger than 1000
        #hour_data = hour_data[hour_data[ylabel] <= 1000]
        #print(f"Hour {hour} data length:", len(hour_data))
        fig.add_trace(go.Box(y=hour_data[ylabel].values, name=str(hour), marker_color='blue', showlegend=False))

    fig.update_layout(title="RTLMP Actual Vs " + proxy, xaxis_title="Hour", yaxis_title=ylabel, template='plotly_white')

    #fig.show()

    return fig


def visualization_export(df_dic):
    date_range = []
    lineplot_dic = {}
    boxplot_dic = {}
    battery_name = df_dic.keys()
    for battery_name, df in df_dic.items():
        Forecast_enertel = df.iloc[:,0]
        rtlmp = df.iloc[:,1]
        Proxy_dalmp = df.iloc[:,2]
        lineplot_results_enertel = line_plot(df, Forecast_enertel)
        lineplot_results_dalmp = line_plot(df, Proxy_dalmp)
        fig_enertel = lineplot_results_enertel[0]
        fig_dalmp = lineplot_results_dalmp[0]
        date_range = lineplot_results_enertel[1]

        # Create DataFrames for the errors
        error_RT_Enertel_df = pd.DataFrame({
            'datetime': df.index,
            'error_Enertel_RTLMP': rtlmp - Forecast_enertel
        })
        error_RT_Enertel_df.set_index('datetime', inplace=True)

        error_DA_Enertel_df = pd.DataFrame({
            'datetime': df.index,
            'error_DALMP_RTLMP': rtlmp - Proxy_dalmp
        })
        error_DA_Enertel_df.set_index('datetime', inplace=True)

        fig_RT = box_plot(error_RT_Enertel_df, "Enertel Forecast Error", "error_Enertel_RTLMP")
        fig_DA = box_plot(error_DA_Enertel_df, "DALMP Actual as Forecast", "error_DALMP_RTLMP")

        lineplot_dic[battery_name] = fig_enertel, error_RT_Enertel_df, fig_dalmp, error_DA_Enertel_df
        boxplot_dic[battery_name] = fig_RT, fig_DA

    return date_range, lineplot_dic, boxplot_dic, fig_enertel, fig_dalmp, fig_RT, fig_DA


def error_table_hour_bucket(df_dic):
    #Load in the config file
    with open("./config.json", "r") as file:
        config = json.load(file)
    error_table_batteries_dic = {}
    hour_bucket_range = config["report"]["buckets"]["hours"]
    for battery_name, df in df_dic.items():
        # Create a new column for the hour of the day
        df['Hour'] = df.index.hour
        error_df_list = []  # Initialize an empty list to store error data
        for hour_range in hour_bucket_range:
            hour_data = df[df['Hour'].isin(range(hour_range[0], hour_range[1] + 1))]
            #print(hour_data.head(20))
            if not hour_data.empty:
                # Calculate weekly averages
                weekly_ME = []
                weekly_RMSE = []
                weekly_MAE = []
                for week in range(1, 5):
                    weekly_data = hour_data[(hour_data.index >= (hour_data.index.min() + pd.Timedelta(weeks=week-1))) & 
                                            (hour_data.index < (hour_data.index.min() + pd.Timedelta(weeks=week)))]
                    if not weekly_data.empty:
                        weekly_ME.append((weekly_data.iloc[:, 1] - weekly_data.iloc[:, 0]).mean())
                        weekly_RMSE.append(np.sqrt(((weekly_data.iloc[:, 1] - weekly_data.iloc[:, 0]) ** 2).mean()))
                        weekly_MAE.append(np.abs(weekly_data.iloc[:, 1] - weekly_data.iloc[:, 0]).mean())
                    
                    #print(weekly_data.head())

                 # Get the week numbers for the x-axis labels
                week_numbers = df.index.isocalendar().week.unique()

                # Create a plot for each error type
                fig_ME = go.Figure()
                fig_ME.add_trace(go.Scatter(x=week_numbers[:4], y=weekly_ME, mode='lines+markers', name='ME'))
                fig_ME.update_layout(
                    title=dict(text = f"ME for {battery_name} - Hour Bucket {hour_range} - ME {np.mean(weekly_ME).round(2)}", font = dict(size = 12)),
                    xaxis=dict(title = dict(text = "Week", font = dict(size = 10))),
                    yaxis=dict(title = dict(text = "ME", font = dict(size = 10))), 
                    template='plotly_white',height=250, plot_bgcolor = "#E0F7FA")
                fig_ME.update_xaxes(ticklabelstep=2, ticktext=['Wk' + str(week) for week in week_numbers[:4]], tickvals=week_numbers[:4],constrain="domain")
                fig_ME.add_shape(type="line", x0=week_numbers[0], y0=0, x1=week_numbers[3], y1=0, line=dict(
                    color="red",
                    width=2,
                    dash="dashdot"))
                #fig_ME.show()

                fig_RMSE = go.Figure()
                fig_RMSE.add_trace(go.Scatter(x=week_numbers[:4], y=weekly_RMSE, mode='lines+markers', name='RMSE'))
                fig_RMSE.update_layout(
                    title=dict(text = f"RMSE for {battery_name} - Hour Bucket {hour_range} - RMSE {np.mean(weekly_RMSE).round(2)}", font = dict(size = 12)),
                    xaxis=dict(title = dict(text = "Week", font = dict(size = 12))),
                    yaxis=dict(title = dict(text = "RMSE", font = dict(size = 12))), 
                    template='plotly_white',height=250, plot_bgcolor = "#E0F7FA")
                fig_RMSE.update_xaxes(ticklabelstep=2, ticktext=['Wk' + str(week) for week in week_numbers[:4]], tickvals=week_numbers[:4],constrain="domain")
                #fig_RMSE.show()

                fig_MAE = go.Figure()
                fig_MAE.add_trace(go.Scatter(x=week_numbers[:4], y=weekly_MAE, mode='lines+markers', name='MAE'))
                fig_MAE.update_layout(
                    title=dict(text = f"MAE for {battery_name} - Hour Bucket {hour_range} - MAE {np.mean(weekly_MAE).round(2)}", font = dict(size = 12)),
                    xaxis=dict(title = dict(text = "Week", font = dict(size = 12))),
                    yaxis=dict(title = dict(text = "MAE", font = dict(size = 12))), 
                    template='plotly_white',height=250, plot_bgcolor = "#E0F7FA")
                fig_MAE.update_xaxes(ticklabelstep=2, ticktext=['Wk' + str(week) for week in week_numbers[:4]], tickvals=week_numbers[:4],constrain="domain")
                #fig_MAE.show()

                error_df_list.append({
                    'Hour Bucket': hour_range,
                    'ME': np.mean(weekly_ME),
                    'RMSE': np.mean(weekly_RMSE),
                    'MAE': np.mean(weekly_MAE),
                    'Count': len(hour_data),
                    'fig': [fig_ME, fig_RMSE, fig_MAE]
                })

        error_table_batteries_dic[battery_name] = error_df_list

    return error_table_batteries_dic

def error_table_price_bucket(df_dic):
    #Load in the config file
    with open("./config.json", "r") as file:
        config = json.load(file)
    error_table_batteries_dic = {}
    price_bucket_range = config["report"]["buckets"]["prices"]
    price_bucket_labels = [f"[{low}, {high})" for low, high in price_bucket_range]
    for battery_name, df in df_dic.items():
        error_df_list = []  # Initialize an empty list to store error data
        # Create a new column for the hour of the day
        df['Price Bin'] = pd.cut(df['rtlmp'], bins=[-500, 0, 25, 55, 120, 250, 5000], labels= price_bucket_labels ,right=False)
        print(df['Price Bin'])
        print(df.head())  # Check if 'Price Bin' exists
        print(df.columns)  # Check available columns
        for price_range in price_bucket_range:
            price_data = df[df['Price Bin'] == f"[{price_range[0]}, {price_range[1]})"]
            #print(hour_data.head(20))
            if not price_data.empty:
                # Calculate weekly averages
                weekly_ME = []
                weekly_RMSE = []
                weekly_MAE = []
                for week in range(1, 5):
                    weekly_data = price_data[(price_data.index >= (price_data.index.min() + pd.Timedelta(weeks=week-1))) & 
                                             (price_data.index < (price_data.index.min() + pd.Timedelta(weeks=week)))]
                    if not weekly_data.empty:
                        weekly_ME.append((weekly_data['rtlmp'] - weekly_data['forecast']).mean())
                        weekly_RMSE.append(np.sqrt(((weekly_data['rtlmp'] - weekly_data['forecast']) ** 2).mean()))
                        weekly_MAE.append(np.abs(weekly_data['rtlmp'] - weekly_data['forecast']).mean())
                    
                    #print(weekly_data.head())

                 # Get the week numbers for the x-axis labels
                if not pd.api.types.is_datetime64_any_dtype(df.index):
                    df.index = pd.to_datetime(df.index)
                week_numbers = df.index.isocalendar().week.unique()

                # Create a plot for each error type
                fig_ME = go.Figure()
                fig_ME.add_trace(go.Scatter(x=week_numbers[:4], y=weekly_ME, mode='lines+markers', name='ME'))
                fig_ME.update_layout(
                    title=dict(text = f"ME for {battery_name} - Price Bucket {price_range} - ME {np.mean(weekly_ME).round(2)}", font = dict(size = 12)),
                    xaxis=dict(title = dict(text = "Week", font = dict(size = 10))),
                    yaxis=dict(title = dict(text = "ME", font = dict(size = 10))), 
                    template='plotly_white',height=250, plot_bgcolor = "#E0F7FA")
                fig_ME.update_xaxes(ticklabelstep=2, ticktext=['Wk' + str(week) for week in week_numbers[:4]], tickvals=week_numbers[:4],constrain="domain")
                fig_ME.add_shape(type="line", x0=week_numbers[0], y0=0, x1=week_numbers[3], y1=0, line=dict(
                    color="red",
                    width=2,
                    dash="dashdot"))
                #fig_ME.show()

                fig_RMSE = go.Figure()
                fig_RMSE.add_trace(go.Scatter(x=week_numbers[:4], y=weekly_RMSE, mode='lines+markers', name='RMSE'))
                fig_RMSE.update_layout(
                    title=dict(text = f"RMSE for {battery_name} - Price Bucket {price_range} - RMSE {np.mean(weekly_RMSE).round(2)}", font = dict(size = 12)),
                    xaxis=dict(title = dict(text = "Week", font = dict(size = 12))),
                    yaxis=dict(title = dict(text = "RMSE", font = dict(size = 12))), 
                    template='plotly_white',height=250, plot_bgcolor = "#E0F7FA")
                fig_RMSE.update_xaxes(ticklabelstep=2, ticktext=['Wk' + str(week) for week in week_numbers[:4]], tickvals=week_numbers[:4],constrain="domain")
                #fig_RMSE.show()

                fig_MAE = go.Figure()
                fig_MAE.add_trace(go.Scatter(x=week_numbers[:4], y=weekly_MAE, mode='lines+markers', name='MAE'))
                fig_MAE.update_layout(
                    title=dict(text = f"MAE for {battery_name} - Price Bucket {price_range} - MAE {np.mean(weekly_MAE).round(2)}", font = dict(size = 12)),
                    xaxis=dict(title = dict(text = "Week", font = dict(size = 12))),
                    yaxis=dict(title = dict(text = "MAE", font = dict(size = 12))), 
                    template='plotly_white',height=250, plot_bgcolor = "#E0F7FA")
                fig_MAE.update_xaxes(ticklabelstep=2, ticktext=['Wk' + str(week) for week in week_numbers[:4]], tickvals=week_numbers[:4],constrain="domain")
                #fig_MAE.show()

                error_df_list.append({
                    'Price Bucket': price_range,
                    'ME': np.mean(weekly_ME),
                    'RMSE': np.mean(weekly_RMSE),
                    'MAE': np.mean(weekly_MAE),
                    'Count': len(price_data),
                    'fig': [fig_ME, fig_RMSE, fig_MAE]
                })

        error_table_batteries_dic[battery_name] = error_df_list

    return error_table_batteries_dic
