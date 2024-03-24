from .constants import *


def get_generate_code(type,
                      columns,
                      prev_columns,
                      labels,
                      label_column_name,
                      use_labels_from_path=False,
                      skiprows=0,
                      sep=",",
                      header=None,
                      lines=False,
                      json_key='',
                      level=None,
                      alt_globs={}):

    type_helper_fns = ""
    if use_labels_from_path:
        type_helper_fns += get_labels_from_path
    if type == 'xml':
        type_helper_fns += xml_code
    if type == 'txt':
        type_helper_fns += txt_code
    if type == 'json':
        type_helper_fns += json_code
    if type == 'wav':
        type_helper_fns += wav_code
    if type == 'jpg':
        type_helper_fns += jpg_code        

    func_name = "def _generate_examples(self, filepaths):\n"
    loop_files = TABS_2 + "_id = 0\n"

    loop_files += TABS_2 + "for i,filepath in enumerate(filepaths['inputs']):\n"
    if type == 'xlsx':
        pandas_df = TABS_3 + f"df = pd.read_excel(open(filepath, 'rb'), skiprows = {skiprows}, header = {header})\n"
    elif type in ['csv', 'tsv']:
        if sep == "tab":
            pandas_df = TABS_3 + f"df = pd.read_csv(filepath, sep = r'\\t', skiprows = {skiprows}, error_bad_lines = False, header = {header}, engine = 'python')\n"
        else:
            pandas_df = TABS_3 + f"df = pd.read_csv(open(filepath, 'rb'), sep = r'{sep}', skiprows = {skiprows}, error_bad_lines = False, header = {header})\n"

    elif type == 'txt':
        pandas_df = TABS_3 + f"df = self.read_txt(filepath, skiprows = {skiprows}, lines = {lines})\n"
        if len(alt_globs) > 1:
            pandas_df += TABS_3 + f"dfs = [df] \n"
            for j in range(1, len(alt_globs)):
                pandas_df += TABS_3 + f"dfs.append(self.read_txt(filepaths['targets{j}'][i], skiprows = {skiprows}, lines = {lines}))\n"
            pandas_df += TABS_3 + f"df = pd.concat(dfs, axis = 1)\n"
    elif type == 'jsonl' or type == 'json':
        pandas_df = TABS_3 + f"df = self.read_json(filepath, lines={lines}, json_key='{json_key}')\n"
    elif type == 'xml':
        pandas_df = TABS_3 + f"df = self.read_xml(filepath, {prev_columns})\n"
    if type in ['wav', 'mp3']:
        pandas_df = TABS_3 + f"df = self.read_wav(filepath)\n"
        if len(alt_globs) > 1:
            pandas_df += TABS_3 + f"dfs = [df] \n"
            for j in range(1, len(alt_globs)):
                pandas_df += TABS_3 + f"dfs.append(self.read_wav(filepaths['targets{j}'][i]))\n"
            pandas_df += TABS_3 + f"df = pd.concat(dfs, axis = 1)\n"
    if type in ['jpg', 'png']:
        pandas_df = TABS_3 + f"df = self.read_image(filepath)\n"
        if len(alt_globs) > 1:
            pandas_df += TABS_3 + f"dfs = [df] \n"
            for j in range(1, len(alt_globs)):
                pandas_df += TABS_3 + f"dfs.append(self.read_image(filepaths['targets{j}'][i]))\n"
            pandas_df += TABS_3 + f"df = pd.concat(dfs, axis = 1)\n"
    pandas_df += TABS_3 + f"if len(df.columns) != {len(columns)}:\n"
    pandas_df += TABS_4 + f"continue\n"
    pandas_df += TABS_3 + f"df.columns = {columns}\n"
    if use_labels_from_path:
        if level is not None:
            if level != -1:
                pandas_df += TABS_3 + f"label = self.get_label_from_path({labels}, filepath.split('/')[{level}])\n"
            else:
                pandas_df += TABS_3 + f"label = self.get_label_from_path({labels}, filepath.split('/')[{level}].split('.')[-2])\n"
        else:
            pandas_df += TABS_3 + f"label = self.get_label_from_path({labels}, filepath)\n"
    loop_entry = TABS_3 + "for _, record in df.iterrows():\n"
    loop_body = TABS_4 + "yield str(_id), {" + ",".join([
        f"'{column}':record['{column}']"
        for column in columns if column != label_column_name
    ])
    if labels is not None:
        if use_labels_from_path:
            loop_body += f",'label':str(label)"
        else:
            loop_body += f",'label':str(record['{label_column_name}'])"
            # columns.remove(label_column_name)

    loop_body += "}\n"
    loop_body += TABS_4 + "_id += 1 \n"
    loop = loop_entry + loop_body
    return f"{type_helper_fns}\n\t{func_name}{loop_files}{pandas_df}{loop}\n"
