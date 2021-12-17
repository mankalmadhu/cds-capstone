import streamlit as st

class MultiPage: 

    def __init__(self) -> None:
        self.pages = {}
    
    def add_page(self, title, func) -> None: 
        self.pages[title] =  func
            
    def run(self):
        # Drodown to select the page to run  
        page = st.sidebar.selectbox(
            'Go to Page', 
            self.pages.keys(), 
        )

        # run the app function 
        self.pages[page]()