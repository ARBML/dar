TABS_1 = "\t"
TABS_2 = "\t\t"
TABS_3 = "\t\t\t"
TABS_4 = "\t\t\t\t"
valid_file_ext = ['txt', 'csv', 'tsv', 'xlsx', 'xls', 'xml', 'json', 'jsonl']
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