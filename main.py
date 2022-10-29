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
import argparse

import os
import sys

# Create the parser
my_parser = argparse.ArgumentParser()
my_parser.add_argument('--pal','-paths_as_labels', action='store_true')
args = my_parser.parse_args()
print(args.pal)

dl_manager = DownloadManager()

datasets_path = 'datasets'
while True:
    DATASET_NAME = input("Dataset Name: ") 
    
    main_class_code = get_class_code(DATASET_NAME)
    URL = input("Enter Direct URL: ")
    file_urls = convert_link(URL)
    print(file_urls)
    zipped = any([ext in file_urls[0] for ext in ['zip', 'rar', 'tar.gz', '7z']])
    label_names = None

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
        download_data_path = eval(alt_glob.replace("glob('", f"glob('{zip_base_dir}/"))
        print(download_data_path)
      if args.pal:
        i = int(input('level for the labels: '))
        label_names = list(set([path.split('/')[i:i+1][0] for path in download_data_path]))
        print(label_names)
    else:
      download_data_path = dl_manager.download(file_urls)
    # print(download_data_path)
    
    split_code = get_split_code(file_urls, download_data_path , zip_base_dir, alt_glob = alt_glob)
    print(split_code)

    type = input("Enter the type: ")
    
    lines = False 
    
    if type == 'json':
      lines = True if input('set lines (y/n) ? ') == 'y' else False

    df, best_sep = get_df(type, download_data_path[0], lines = lines)
    columns = [] 
    if type == 'xml':
      columns = input('enter the columns: ').split(",")
      df = read_xml(download_data_path[0], columns)
    lines = False
    
    print(df.head())
    if type in ['csv', 'tsv']:
      print("Found best sep , ", best_sep)
      best_sep = input(f"Set a different Separator for {type}")
      if len(best_sep) > 0:
        df, _ = get_df(type, download_data_path[0], 0, sep = best_sep)
        print(df.head())

    skiprows = 0
    skiprows = int(input("Enter rows to skip: "))
    if skiprows != 0:
      df, _  = get_df(type, download_data_path[0], skiprows, sep = best_sep, lines = lines)
      print(df.head())
    columns = list(df.columns)
    print(columns)
    header = None if input("Data has a column? (y/n)") == 'n' else 0
    new_columns = input("Enter new columns separated by comma").split(",")
    if len(new_columns[0]) != 0:
      columns = new_columns
    df.columns= columns
    set_label = True
    label_column_name = ''
    if not args.pal:
      label_column_name = input("Enter label column name: ")

    if label_column_name != '':
      label_names = list(set(df[label_column_name]))
      print(label_names)

    generate_code = ""
    if args.pal:
      generate_code += get_labels_from_path
    if type == 'xml':
      generate_code += xml_code
    if type == 'txt':
      generate_code += txt_code
    
    print(columns)
    generate_code += get_generate_code(type, columns, label_names, label_column_name, skiprows = skiprows, use_labels_from_path = args.pal, sep = best_sep, header = header, lines = lines)
    print(generate_code)

    if label_column_name != '':
      if label_column_name in columns:
        columns.remove(label_column_name)

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