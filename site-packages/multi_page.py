import streamlit as st

class MultiPage: 

    def __init__(self) -> None:
        self.pages = {}
    
    def add_page(self, title, func) -> None: 
        self.pages[title] =  func
            
    def run(self):
        st.sidebar.title('Navigation')
        selection = st.sidebar.radio("Go to", self.pages.keys())

        # run the app function 
        self.pages[selection]()