from constants import *

def get_generate_code(type, features, label, skiprows = 0, sep = "\\t", header = None):
  func_name ="def _generate_examples(self, filepaths):\n"
  loop_files = TABS_2+"_id = 0\n"
  loop_files += TABS_2+"for filepath in filepaths:\n"
  if type == 'xlsx':
    pandas_df = TABS_3+f"df = pd.read_excel(open(filepath, 'rb'), skiprows = {skiprows}, header = {header})\n"
  elif type in ['csv', 'txt', 'tsv']:
    pandas_df = TABS_3+f"df = pd.read_csv(open(filepath, 'rb'), sep = '{sep}', skiprows = {skiprows}, error_bad_lines = False, header = {header})\n"
  elif type == 'jsonl' or type == 'json':
      pandas_df = TABS_3+f"df = pd.read_json(open(filepath, 'rb'), lines=True, header = {header})\n"
  elif type == 'xml':
      pandas_df = TABS_3+f"df = self.read_xml(filepath, {features})\n"
  pandas_df += TABS_3+f"df.columns = {features}\n"
  loop_entry = TABS_3+"for _, record in df.iterrows():\n"
  loop_body  = TABS_4+"yield str(_id), {" + ",".join([f"'{column}':record['{column}']" for column in features if column != label])
  if len(label) > 0:
    loop_body += f",'label':str(record['{label}'])"
  loop_body += "}\n"
  loop_body += TABS_4+"_id += 1 \n" 
  loop = loop_entry+loop_body
  return f"\t{func_name}{loop_files}{pandas_df}{loop}\n"