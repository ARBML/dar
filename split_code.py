from constants import *

def get_split_code(urls, files, zip_base_dir):
  MAIN_SPLITS = {'train':'TRAIN', 'test':'TEST', 'valid':'VALIDATION', 'dev':'VALIDATION'}
  func_name ="def _split_generators(self, dl_manager):\n"
  body  = TABS_2 + f"url = {urls}\n"
  if len(zip_base_dir) > 0:
    body += TABS_2+ f"downloaded_files = dl_manager.download_and_extract(url)\n"
  else:
    body += TABS_2+ f"downloaded_files = dl_manager.download(url)\n"

  result = []    
  if len(zip_base_dir) > 0:
    files = [f.replace(zip_base_dir, "")[1:] for f in files] # only extract the directory to files, the base dir is random
    for i,f in enumerate(files):
      for split in MAIN_SPLITS:
          if split in f.lower():
            result.append(f"datasets.SplitGenerator(name=datasets.Split.{MAIN_SPLITS[split]}"+", gen_kwargs={"+f"'filepaths': [os.path.join(downloaded_files[0],'{f}')]"+"})")
    if len(result) == 0:
      result.append("datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in os.listdir(downloaded_files[0])]})")
    
  else:
    for i, url in enumerate(urls):
      for split in MAIN_SPLITS:
        if split in url.lower():
          result.append(f"datasets.SplitGenerator(name=datasets.Split.{MAIN_SPLITS[split]}"+", gen_kwargs={"+f"'filepaths': [downloaded_files[{i}]]"+"})")
    if len(result) == 0:
      result.append(f"datasets.SplitGenerator(name=datasets.Split.TRAIN"+", gen_kwargs={'filepaths': downloaded_files})")

  result = TABS_2+'return ['+','.join(result)+']'
  return f"\t{func_name}{body}{result}\n"