import os

from datasets.utils.download_manager import DownloadManager
from datasets import load_dataset
from utils import * 
from class_code import get_class_code
from features_code import get_features_code
from generate_code import get_generate_code
from imports_code import get_imports_code
from split_code import get_split_code
from squad_code import get_squad_code
import argparse
import os

# Create the parser
my_parser = argparse.ArgumentParser()
my_parser.add_argument('--pal','-paths_as_labels', action='store_true')
my_parser.add_argument('--squad','-squad_link_dataset', action='store_true')
my_parser.add_argument('--p','-save_pah', type = str, action='store', default = 'datasets')
my_parser.add_argument('--hf','-hf_path', type = str, action='store', default = 'arbml')
args = my_parser.parse_args()

dl_manager = DownloadManager()

datasets_path = args.p
DATASET_NAME = input("Dataset Name: ") 

main_class_code = get_class_code(DATASET_NAME)
URL = input("Enter Direct URL: ")
file_urls = convert_link(URL)
print(file_urls)
zipped = any([ext in file_urls[0] for ext in ['zip', 'rar', 'tar.gz', '7z', 'drive']])
label_names = None

zip_base_dir = ''
alt_globs = []

level = None
download_data_path = {}

if zipped:
  try:
    download_data_path = dl_manager.download_and_extract(file_urls)[0]
  except:
    file_urls = [URL]
    download_data_path = dl_manager.download_and_extract(file_urls)[0]
  zip_base_dir = download_data_path
  extract_all(zip_base_dir)
  print(zip_base_dir)
  download_data_path = {}
  download_data_path['inputs'] = get_valid_files(zip_base_dir)
  print(download_data_path)

  alt_glob = ''
  i = 1
  while True:
    alt_glob = input('Enter a glob structure: ')
    if not alt_glob:
      break
    if len(alt_globs) == 0:
      download_data_path['inputs'] = eval(alt_glob.replace("glob('", f"glob('{zip_base_dir}/"))
      download_data_path['inputs'].sort()
      alt_globs.append({'inputs':alt_glob})
    else:
      download_data_path[f'targets{i}'] = eval(alt_glob.replace("glob('", f"glob('{zip_base_dir}/"))
      download_data_path[f'targets{i}'].sort()
      alt_globs.append({f'targets{i}':alt_glob})
      i += 1
  
  if args.pal:
    level = int(input('level for the labels: '))
    if level == -1:
      label_names = list(set([path.split('/')[-1] for path in download_data_path['inputs']]))
      label_names = list(set([lbl.split('.')[-2] for lbl in label_names]))
    else:
      label_names = list(set([path.split('/')[level:level+1][0] for path in download_data_path['inputs']]))
    print(label_names)
else:
  download_data_path['inputs'] = dl_manager.download(file_urls)

print(download_data_path)

split_code = get_split_code(file_urls, download_data_path , zip_base_dir, alt_globs)

if args.squad:
  code = get_squad_code(DATASET_NAME, split_code.replace('\t', '    '))
else:
  type = input("Enter the type: ")

  # file types paramters
  lines = False 
  json_key = ''
  columns = []
  best_sep = ','

  if type == 'json':
    lines = True if input('set lines (y/n)? ') == 'y' else False
    json_key = input('set json key: ')

  if type == 'txt':
    lines = True if input('set lines (y/n)? ') == 'y' else False

  if type == 'xml':
    columns = input('enter the columns: ').split(",")

  df = get_df(type, download_data_path, lines = lines, json_key = json_key, columns = columns)
  print(df.head())
  
  if type in ['csv', 'tsv']:
    alt_sep = input(f"Set a separator for {type}: ")
    if alt_sep:
      df = get_df(type, download_data_path, 0, sep = alt_sep, json_key=json_key)
      print(df.head())
    else:
      print('using default separator ', {best_sep})

  skiprows = input("Enter rows to skip: ")
  skiprows = int(skiprows) if skiprows else 0

  if skiprows != 0:
    df, _  = get_df(type, download_data_path, skiprows = skiprows, sep = best_sep, lines = lines, json_key=json_key)
    print(df.head())

  columns = list(df.columns)

  header = None if input("Data has a column (y/n)? ") == 'n' else 0
  new_columns = input("Enter new columns separated by comma: ").split(",")

  if len(new_columns[0]) != 0:
    columns = new_columns
  df.columns= columns
  print(columns)

  label_column_name = ''

  if not args.pal:
    label_column_name = input("Enter label column name: ")

  if label_column_name != '':
    label_names = list(set(df[label_column_name]))
    print(label_names)

  generate_code = get_generate_code(type, columns, label_names, label_column_name, skiprows = skiprows, use_labels_from_path = args.pal 
                                    , sep = best_sep, header = header, lines = lines, json_key = json_key, level = level, alt_globs = alt_globs)

  import_code = get_imports_code(type)
  features_code = get_features_code(columns, label_names)

  # generate code and load the dataset
  code = import_code+main_class_code+features_code+split_code+generate_code

os.makedirs(f"{datasets_path}/{DATASET_NAME}", exist_ok = True)
open(f"{datasets_path}/{DATASET_NAME}/{DATASET_NAME}.py", "w").write(code)
dataset = load_dataset(f"{datasets_path}/{DATASET_NAME}")
print(dataset)

if input("push to hub: ") == 'y':
  print('pushing to the hub') 
  dataset.push_to_hub(f"{args.hf}/{DATASET_NAME}")
