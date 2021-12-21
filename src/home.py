import os
import streamlit as st
import numpy as np
from PIL import Image
from dotenv import load_dotenv

# Custom imports
from multi_page import MultiPage
from pages import demo, about, scrape_data

load_dotenv()

app = MultiPage()

# Title of the main page
display = Image.open('resources/logo2.png')
display = np.array(display)
col1, col2 = st.columns(2)
col1.image(display, width=800)
col2.title(" ")

app.add_page("Course Era Demo", demo.render_page)
app.add_page("About Project", about.render_page)
app.add_page("Scrape Data", scrape_data.render_page)

app.run()
