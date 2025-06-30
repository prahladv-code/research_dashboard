import streamlit as st

class Homepage:
    def __init__(self, options):
        self.st = st
        self.st.set_page_config(
            page_title=f"108 Capital Dashboard",  # This sets the title on the browser tab
            page_icon=r"C:\Users\admin\Desktop\108 TEST LOGO.png",
            # You can use an emoji or a local file like "logo.png"
            layout="wide",  # Optional: use "wide" for full-width layout
            initial_sidebar_state="expanded",  # Optional,
            menu_items={
                'About': 'This is a reseach Dashboard For the 108 Capital Research Team. All analysis of various quantitative strategies is done on this dashboard.'})
        self.options = options
        self.st.title("108 Capital Research Dashboard")
        self.st.write("Welcome to the 108 Capital Research Dashboard. Please select a strategy from the Strat Navigator menu to analyze. Wishing you a happy and productive analysis!")
        self.st.sidebar.title("Strategy Navigator")
        self.st.logo(r'C:\Users\admin\Desktop\108 TEST LOGO.png')


    def radio(self, title):
        return self.st.sidebar.radio(f'{title}', self.options)


    def create_page(self, page_title):
        self.st.title(f'{page_title}')
        self.st.divider()

    def create_dataframe(self, df):
        return self.st.dataframe(df)

    def create_eq_curve(self, eq_curve, x_axis, y_axis):
        return self.st.line_chart(data=eq_curve, x=x_axis, y=y_axis, x_label='Date/Time', y_label='Equity', color='#0000ff')

    def create_bar_chart(self, bar):
        return self.st.bar_chart(bar)