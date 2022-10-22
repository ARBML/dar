def get_imports_code(more_imports = []):
  return "import os\nimport pandas as pd \nimport datasets\n"+'\n'.join(more_imports)+'\n'