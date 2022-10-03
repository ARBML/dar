TABS_1 = "\t"
TABS_2 = "\t\t"
TABS_3 = "\t\t\t"

def get_imports_code():
  return "import os\nimport pandas as pd \nimport datasets\n"

def get_class_code(DATASET_NAME):
  return "class AjgtTwitterAr(datasets.GeneratorBasedBuilder):\n"

def get_features_code(columns, labels):
  
  features = ','.join([f"'{feature}':datasets.Value('string')" for feature in columns])
  label = f", 'label': datasets.features.ClassLabel(names={str(labels)})"
  func_name = 'def _info(self):\n'
  func_body = "return datasets.DatasetInfo(features=datasets.Features({"+ features + label+"}))"
  return f"\t{func_name}\t\t{func_body}\n"

def get_config_code(DATASET_NAME):
  return f"class {DATASET_NAME}Config(datasets.BuilderConfig):\n"\
          +"\tdef __init__(self, **kwargs):\n"\
          +f"\t\tsuper({DATASET_NAME}Config, self).__init__(**kwargs)"

import os 

def get_split_code(url, download_data_path, zipped = True):
  MAIN_SPLITS = {'train':'TRAIN', 'test':'TEST', 'valid':'VALIDATION', 'dev':'VALIDATION'}
  func_name ="def _split_generators(self, dl_manager):\n"
  body  = TABS_2 + f"url = '{url}'\n"
  if zipped:
    body += TABS_2+ f"downloaded_files = dl_manager.download_and_extract(url)\n"
  else:
    body += TABS_2+ f"downloaded_files = dl_manager.download(url)\n"
  try:
    files = os.listdir(download_data_path)
  except:
    files = []
  print(files)
  result = []
  if len(files) == 0:
    result.append("datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepath': downloaded_files})")
  else:
    for f in files:
      for split in MAIN_SPLITS:
          if split in f.lower():
            result.append(f"datasets.SplitGenerator(name=datasets.Split.{MAIN_SPLITS[split]}"+", gen_kwargs={"+f"'filepath': os.path.join(downloaded_files,'{f}')"+"})")
  result = TABS_2+'return ['+','.join(result)+']'
  return f"\t{func_name}{body}{result}\n"

def get_generate_code(filepath, label, type = 'excel'):

  func_name ="def _generate_examples(self, filepath):\n"
  func_name += TABS_2 +"print(filepath)\n"
  if type == 'excel':
    pandas_df = TABS_2+f"df = pd.read_excel(open(filepath, 'rb'))\n"
    df = pd.read_excel(filepath)
  elif type == 'tsv':
    pandas_df = TABS_2+f"df = pd.read_csv(open(filepath, 'rb'), sep = '\\t')\n"
    df = pd.read_csv(filepath, sep='\t')
  columns = list(df.columns)
  print(columns)
  loop_entry = TABS_2+"for _id, record in df.iterrows():\n"
  loop_body = TABS_3+",".join([column for column in columns])
  loop_body += " = "+ ",".join([f"record['{column}']" for column in columns]) + "\n"
  columns.remove(label)
  loop_body += TABS_3+"yield str(_id), {" + ",".join([f"'{column}': {column}" for column in columns if column != label])
  loop_body += f",'label':{label}"+"}" 
  loop = loop_entry+loop_body
  label_names = list(set(df[label]))
  return f"\t{func_name}{pandas_df}{loop}\n", columns, label_names

def convert_github_link(link):
  file_name = link.split("/")[-1]
  branch_name = link.split("/")[-2]
  user_name = link.split("/")[3]
  repo_name = link.split("/")[4]
  return f"https://raw.githubusercontent.com/{user_name}/{repo_name}/{branch_name}/{file_name}"
