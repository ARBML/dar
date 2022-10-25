import fsspec
import re
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import pandas as pd

def convert_link(links):
  output = []
  for link in links.split(","):
    if 'github.com' in link.lower():
      user_name = link.split("/")[3]
      repo_name = link.split("/")[4]
      if link.count('/') > 4:    
        branch_name = 'master' if 'master' in link else 'main'

        
        base_path = f"https://raw.githubusercontent.com/{user_name}/{repo_name}/{branch_name}/"
        file_name = link.split(branch_name)[-1][1:]
        fs = fsspec.filesystem("github", org=user_name, repo=repo_name)
        
        if fs.isdir(file_name):
          output = output +  [base_path+f for f in fs.ls(f"{file_name}/")]
        else:
          output.append(base_path+file_name)
      else:
        output.append(f'https://github.com/{user_name}/{repo_name}/archive/master.zip')
    elif 'drive.google' in link.lower():
      base = "https://drive.google.com/file/d/"
      trail = "/view"
      id = link.replace(trail,"").replace(base,"")
      output.append(f"https://drive.google.com/uc?export=download&id={id}")
    elif 'docs.google' in link.lower():
      sheet_name = "Sheet1"
      sheet_id = link.split("/d/")[-1].split("/")[0]
      url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
      output.append(url)
    else:
      output.append(link)
  return output

def get_data(bs, column):
    elements =  [attr[column] for attr in bs.find_all(attrs={column : re.compile(".")})]
    if len(elements) == 0:
      elements = [el.text for el in bs.find_all(column)]
    return elements

def read_xml(path, columns):
  with open(path, 'rb') as f:
      data = f.read()
  
  bs = BeautifulSoup(data, "xml")
  data = {}
  for column in columns:
    elements = get_data(bs, column)
    data[column] = elements
  return pd.DataFrame(data)

def get_df(type, path, skiprows = 0, sep = ""):
  print(path)
  best_sep = ""
  best_columns = 0
  df = None
  if type == "xlsx":
    df = pd.read_excel(path, skiprows = skiprows)
  if type == 'jsonl' or type == 'json':
    df = pd.read_json(path, lines=True)
  if type in ['csv', 'txt', 'tsv']:
    if len(sep) > 0:
      df = pd.read_csv(path, sep = f'{sep}', skiprows = skiprows, error_bad_lines = False)
    else:
      for sep in ["\\\t", ";", ","]: #TODO I need to consider the case when we have single sepace separator
        try:
          df = pd.read_csv(path, sep = f'{sep}', skiprows = skiprows, error_bad_lines = False)
          num_columns = len(list(df.columns))
          if best_columns < num_columns:
            best_sep = sep
            best_columns = num_columns
        except:
          continue
  if type =='xml':
    tree = ET.parse(path)
    root = tree.getroot()
    df = ET.tostring(root, encoding='unicode', method='xml')
    print(df[:500])
  return df, best_sep

def get_split_user(split_files):
    dif_splits = input('Enter different splits (y/n): ')
    if dif_splits == 'y':
        split_files = {}
        train_files = input('Enter train split files as a list: ')
        test_files = input('Enter test split files as a list: ')
        dev_files = input('Enter dev split files as a list: ')

        if train_files != '':
            split_files['TRAIN'] = train_files
        if test_files != '':
            split_files['TEST'] = test_files
        if dev_files != '':
            split_files['VALIDATION'] = dev_files
    return split_files
