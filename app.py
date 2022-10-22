import streamlit as st
from filepicker import st_file_selector

tif_file = st_file_selector(st, path = 'datasets', key = 'tif', label = 'Choose tif file')

from streamlit_option_menu import option_menu

with st.sidebar:
      selected = option_menu("Main Menu", ["Home", 'Settings'], default_index=1)
      selected