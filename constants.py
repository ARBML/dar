TABS_1 = "\t"
TABS_2 = "\t\t"
TABS_3 = "\t\t\t"
TABS_4 = "\t\t\t\t"
valid_file_ext = ['txt', 'csv', 'tsv', 'xlsx', 'xls', 'xml', 'json', 'jsonl', 'html', 'arff']
xml_code = f'''

{TABS_1}def get_data(self, bs, column):
{TABS_2}elements =  [attr[column] for attr in bs.find_all(attrs={{column : re.compile(".")}})]
{TABS_2}if len(elements) == 0:
{TABS_3}elements = [el.text for el in bs.find_all(column)]
{TABS_2}return elements

{TABS_1}def read_xml(self, path, columns):
{TABS_2}with open(path, 'rb') as f:
{TABS_3}data = f.read()
    
{TABS_2}bs = BeautifulSoup(data, "xml")
{TABS_2}data = {{}}
{TABS_2}for column in columns:
{TABS_3}elements = self.get_data(bs, column)
{TABS_3}data[column] = elements
{TABS_2}return pd.DataFrame(data)
'''

txt_code = f'''
{TABS_1}def read_txt(self, filepath, skiprows = 0):
{TABS_2}lines = open(filepath, 'r').read().splitlines()[skiprows:]
{TABS_2}return pd.DataFrame(lines)
'''

get_labels_from_path = f'''
{TABS_1}def get_label_from_path(self, labels, path):
{TABS_2}for label in labels:
{TABS_3}if label in path:
{TABS_4}return label
'''

arff_code = f'''
{TABS_1}def read_arff(self, filepath, skiprows):
{TABS_2}raw_data = loadarff(filepath)
{TABS_2}df_data = pd.DataFrame(raw_data[0])
{TABS_2}return df_data
'''

extract_all_code = f'''
{TABS_1}def extract_all(self, dir):
{TABS_2}zip_files = glob(dir+'/**/**.zip', recursive=True)
{TABS_2}for file in zip_files:
{TABS_3}with zipfile.ZipFile(file) as item:
{TABS_4}item.extractall('/'.join(file.split('/')[:-1])) 
'''
get_all_files_code = f'''
{TABS_1}def get_all_files(self, dir):
{TABS_2}files = []
{TABS_2}valid_file_ext = ['txt', 'csv', 'tsv', 'xlsx', 'xls', 'xml', 'json', 'jsonl', 'html', 'arff'] 
{TABS_2}for ext in valid_file_ext:
{TABS_3}files += glob(f"{{dir}}/**/**.{{ext}}", recursive = True)
{TABS_2}return files
'''