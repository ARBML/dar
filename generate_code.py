from constants import *

def get_generate_code(type, features, labels, label_column_name, file_label_names,
                       skiprows = 0, sep = "\\t", header = None, lines = False):
  func_name ="def _generate_examples(self, filepaths):\n"
  loop_files = TABS_2+"_id = 0\n"
  if file_label_names is not None:
    loop_files += TABS_2+f"labels = {labels}\n"
  loop_files += TABS_2+"for i,filepath in enumerate(filepaths):\n"
  if type == 'xlsx':
    pandas_df = TABS_3+f"df = pd.read_excel(open(filepath, 'rb'), skiprows = {skiprows}, header = {header})\n"
  elif type in ['csv', 'tsv']:
    pandas_df = TABS_3+f"df = pd.read_csv(open(filepath, 'rb'), sep = '{sep}', skiprows = {skiprows}, error_bad_lines = False, header = {header})\n"
  elif type == 'txt':
      pandas_df = TABS_3+f"df = self.read_txt(filepath, skiprows = {skiprows})\n"
  elif type == 'jsonl' or type == 'json':
      pandas_df = TABS_3+f"df = pd.read_json(open(filepath, 'rb'), lines={lines})\n"
  elif type == 'xml':
      pandas_df = TABS_3+f"df = self.read_xml(filepath, {features})\n"
  pandas_df += TABS_3+f"df.columns = {features}\n"
  loop_entry = TABS_3+"for _, record in df.iterrows():\n"
  loop_body  = TABS_4+"yield str(_id), {" + ",".join([f"'{column}':record['{column}']" for column in features if column != label_column_name])
  if labels is not None:      
    if label_column_name:
      loop_body += f",'label':str(record['{label_column_name}'])"      
    else:
      loop_body += f",'label':str(labels[i])"

  loop_body += "}\n"
  loop_body += TABS_4+"_id += 1 \n" 
  loop = loop_entry+loop_body
  return f"\t{func_name}{loop_files}{pandas_df}{loop}\n"