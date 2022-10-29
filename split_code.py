from constants import *
from utils import get_split_user 

def get_split_code(urls, files, zip_base_dir, alt_glob = ''):
  MAIN_SPLITS = {'train':'TRAIN', 'test':'TEST', 'valid':'VALIDATION', 'dev':'VALIDATION'}
  func_name ="def _split_generators(self, dl_manager):\n"
  body  = TABS_2 + f"url = {urls}\n"
  if len(zip_base_dir) > 0:
    body += TABS_2+ f"downloaded_files = dl_manager.download_and_extract(url)\n"
  else:
    body += TABS_2+ f"downloaded_files = dl_manager.download(url)\n"

  result = []    
  if len(zip_base_dir) > 0:
    split_files = {}
    files = [f.replace(zip_base_dir, "")[1:] for f in files] # only extract the directory to files, the base dir is random
    for i,f in enumerate(files):
      for split in MAIN_SPLITS:
          if split in f.lower():
            if MAIN_SPLITS[split] in split_files:
              split_files[MAIN_SPLITS[split]]+=','+f
            else:
              split_files[MAIN_SPLITS[split]] =f

    if len(split_files) == 0:
      alt_glob = alt_glob.replace("glob('", "glob(downloaded_files[0]+'/")
      print(alt_glob)
      result.append(f"datasets.SplitGenerator(name=datasets.Split.TRAIN"+", gen_kwargs={"+f"'filepaths': {alt_glob}"+"})")
    else:
      split_files = get_split_user(split_files)
      for split in split_files:
        result.append(f"datasets.SplitGenerator(name=datasets.Split.{split}"+", gen_kwargs={"+f"'filepaths': [os.path.join(downloaded_files[0],f) for f in {split_files[split].split(',')}]"+"})")
  else:
    split_files = {}
    for i, url in enumerate(urls):
      for split in MAIN_SPLITS:
        if split in url.split('/')[-1].lower():
          if MAIN_SPLITS[split] in split_files:
              split_files[MAIN_SPLITS[split]]+=f',downloaded_files[{i}]'
          else:
              split_files[MAIN_SPLITS[split]] =f'downloaded_files[{i}]'

    if len(split_files) == 0:
      result.append("datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})")
    else:
      for split in split_files:
        result.append(f"datasets.SplitGenerator(name=datasets.Split.{split}"+", gen_kwargs={"+f"'filepaths': [{split_files[split]}]"+"})")

  result = TABS_2+'return ['+','.join(result)+']'
  return f"\t{func_name}{body}{result}\n"