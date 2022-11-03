import os
from datasets.utils.download_manager import DownloadManager
dl_manager = DownloadManager()

def create_script_adcv():
  DATASET_NAME = "ADCV"
  label = "label"
  url = "https://github.com/msmadi/Arabic-Dataset-for-Commonsense-Validationion/blob/master/Data.zip"
  features = ["sent1", "sent2"]
  file_name = url.split("/")[-1]

  import_code = get_imports_code()
  main_class_code = get_class_code(DATASET_NAME)
  converted_url = convert_github_link(url)
  zipped = False
  try:
    download_data_path = dl_manager.download_and_extract(converted_url)
    zipped = True
  except:
    download_data_path = dl_manager.download(converted_url)

  split_code = get_split_code(converted_url, download_data_path , zipped = zipped)
  generate_code, features, label_names = get_generate_code(download_data_path+ "/"+os.listdir(download_data_path)[0], label, type = 'tsv')
  features_code = get_features_code(features, label_names)
  code = import_code+main_class_code+features_code+split_code+generate_code
  os.makedirs(DATASET_NAME, exist_ok = True)
  open(f"{DATASET_NAME}/{DATASET_NAME}.py", "w").write(code)
  
def create_script_ajgt():
  DATASET_NAME = "AJGT"
  label = "Sentiment"
  url = "https://github.com/komari6/Arabic-twitter-corpus-AJGT/blob/master/AJGT.xlsx"
  label = "Sentiment"
  features = ["ID", "Feed"]
  file_name = url.split("/")[-1]

  import_code = get_imports_code()
  main_class_code = get_class_code(DATASET_NAME)
  converted_url = convert_github_link(url)
  split_code = get_split_code(converted_url)
  generate_code, features, label_names = get_generate_code(file_name,label, type = 'excel')
  features_code = get_features_code(features, label_names)
  code = import_code+main_class_code+features_code+split_code+generate_code
  os.makedirs(DATASET_NAME, exist_ok = True)
  open(f"{DATASET_NAME}/{DATASET_NAME}.py", "w").write(code)
