import Dashboard_templates as dt
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import ChakraView

class AnalyticsDashboard:
    def __init__(self, strategy_list):
        self.strategy = strategy_list
        self.home = dt.Homepage(strategy_list)
        self.radio = self.home.radio("Select A Strategy")
        self.st = st
        self.tradesheets = ChakraView.Tradesheets()

    def orb_long(self):
        if self.radio == "ORB LONG":
            metrics_list = []
            folder_path = f"C:/Users/admin/Desktop/ORB/"
            initial_margin = self.st.number_input("Initial Margin")
            if initial_margin > 0:
                for file in os.listdir(folder_path):
                    file_path = os.path.join(folder_path+file)
                    df = pd.read_csv(file_path)
                    uid = file.split('.')[0]
                    metrics_dict = self.tradesheets.calculate_metrics(df, initial_margin, uid)
                    metrics_list.append(metrics_dict)
                metrics_df = pd.DataFrame(metrics_list)
                self.st.dataframe(metrics_df)
            else:
                self.st.write("Please Enter Initial Margin.")







# Initialize and run the dashboard
dash = AnalyticsDashboard(['ORB LONG', '108STRAT1'])
dash.orb_long()
