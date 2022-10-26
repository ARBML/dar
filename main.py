import os
import datasets
from datasets.utils.download_manager import DownloadManager
from datasets import load_dataset
from utils import * 
from config_code import get_config_code
from class_code import get_class_code
from constants import * 
from features_code import get_features_code
from generate_code import get_generate_code
from imports_code import get_imports_code
from split_code import get_split_code
from glob import glob
dl_manager = DownloadManager()
datasets_path = 'datasets'
while True:
    DATASET_NAME = input("Dataset Name: ") 
    
    main_class_code = get_class_code(DATASET_NAME)
    URL = input("Enter Direct URL: ")
    file_urls = convert_link(URL)
    print(file_urls)
    zipped = input("Enter zipped y or n") == 'y'
    zip_base_dir = ''
    if zipped:
      try:
        download_data_path = dl_manager.download_and_extract(file_urls)[0]
      except:
        file_urls = [URL]
        download_data_path = dl_manager.download_and_extract(file_urls)[0]
      zip_base_dir = download_data_path
      print(zip_base_dir)
      download_data_path = []
      for ext in valid_file_ext:
        download_data_path += glob(f"{zip_base_dir}/**/**.{ext}", recursive = True)
      print(download_data_path)
      alt_glob = input('Enter different glob structure: ')
      if len(alt_glob) > 0:
        print(alt_glob)
        download_data_path = eval(alt_glob.replace("glob('", f"glob('{zip_base_dir}/"))
    else:
      download_data_path = dl_manager.download(file_urls)
    print(download_data_path)
    
    split_code = get_split_code(file_urls, download_data_path , zip_base_dir)
    print(split_code)

    type = input("Enter the type: ")
    
    df, best_sep = get_df(type, download_data_path[0])
    columns = [] 
    if type == 'xml':
      columns = input('enter the columns: ').split(",")
      df = read_xml(download_data_path[0], columns)
    print(df.head())
    if type in ['csv', 'txt', 'tsv']:
      print("Found best sep , ", best_sep)
      best_sep = input(f"Set a different Separator for {type}")
      if len(best_sep) > 0:
        df, _ = get_df(type, download_data_path[0], 0, sep = best_sep)
        print(df.head())

    skip_rows = 0
    skip_rows = int(input("Enter rows to skip: "))
    if skip_rows != 0:
      df, _  = get_df(type, download_data_path[0], skip_rows, sep = best_sep)
      print(df.head())
    columns = list(df.columns)
    print(columns)
    header = None if input("Data has a column? (y/n)") == 'n' else 0
    new_columns = input("Enter new columns separated by comma").split(",")
    if len(new_columns[0]) != 0:
      columns = new_columns
    df.columns= columns
    set_label = True
    # columns = input("Enter the column names: ").split(",")
    label_column_name = input("Enter label column name: ")
    file_label_names = input("Enter labels for files(s): ")

    label_names = None

    if file_label_names:
      label_names = file_label_names.split(',')

    if label_column_name != '':
      label_names = list(set(df[label_column_name]))
      print(label_names)

    generate_code = ""
    if type == 'xml':
      generate_code = xml_code
    
    print(columns)
    generate_code += get_generate_code(type, columns, label_names, label_column_name, file_label_names, skip_rows, sep = best_sep, header = header)
    print(generate_code)
      
    features_code = get_features_code(columns, label_names)
    print(features_code)
    extra_imports = []
    if type == 'xml':
      extra_imports = ['import re', 'from bs4 import BeautifulSoup']
    import_code = get_imports_code(extra_imports)
    code = import_code+main_class_code+features_code+split_code+generate_code
    os.makedirs(f"{datasets_path}/{DATASET_NAME}", exist_ok = True)
    open(f"{datasets_path}/{DATASET_NAME}/{DATASET_NAME}.py", "w").write(code)
    dataset = load_dataset(f"{datasets_path}/{DATASET_NAME}")
    print(dataset)
    if input("push to hub: ") == 'y':
      print('pushing to the hub') 
      dataset.push_to_hub(f"arbml/{DATASET_NAME}")
    break