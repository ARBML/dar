def get_imports_code(type):
  extra_imports = []
  if type == 'xml':
    extra_imports = ['import re', 'from bs4 import BeautifulSoup']
  if type == 'json':
    extra_imports = ['import json']

  return "import os\nimport pandas as pd \nimport datasets\nfrom glob import glob\nimport zipfile\n"+'\n'.join(extra_imports)+'\n'