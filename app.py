import streamlit as st
import os
import yaml
from huggingface_hub import login
from datasets.utils.download_manager import DownloadManager
from datasets import load_dataset
from src.utils import *
from src.class_code import get_class_code
from src.features_code import get_features_code
from src.generate_code import get_generate_code
from src.imports_code import get_imports_code
from src.split_code import get_split_code
from src.squad_code import get_squad_code
from urllib.parse import urlparse
import opendatasets as od
import shutil

valid_file_types = ("", "csv", "txt", "json", "xml", "xlsx", "wav", "jpg")
valid_csv_sep = (" ", ",", ";", "|", "tab")

# https://stackoverflow.com/questions/7160737/how-to-validate-a-url-in-python-malformed-or-not
def is_url(url):
  try:
    result = urlparse(url)
    return all([result.scheme, result.netloc])
  except ValueError:
    return False
  
def insert_image(img_path, caption):
    col1, col2, col3 = st.columns([1, 3, 1])

    with col1:
        st.write("")

    with col2:
        st.image(img_path, caption=caption)

    with col3:
        st.write("")


def get_input(input_text,
              config_key,
              glob_idx=-1,
              default_value="",
              label_columns=[],
              description = "",
              key=0):

    if type(default_value) == list and len(default_value):
        if type(default_value[0]) != dict:
            default_value = ",".join(default_value)

    if config_key == "file_type":
        default_value = default_value if default_value in valid_file_types else ""
        result = create_select_box(input_text, valid_file_types, key = config_key,
                              index=valid_file_types.index(default_value), description=description)
    elif config_key == "alt_sep":
        default_value = default_value if default_value in valid_csv_sep else ""
        result = create_select_box(input_text,valid_csv_sep, key = config_key,
                              index=valid_csv_sep.index(default_value), description=description)
    elif config_key in ["encoding"]:
        valid_text_types = ["utf-8", "latin-1"]
        result = create_select_box(input_text, valid_text_types, key = config_key,
                              index=0, description=description)    
    elif config_key in ["header", "lines"]:
        result = create_radio(input_text, (True, False), description = description, key = config_key)
    elif config_key in ["pal", "local_dir"]:
        result = create_radio(input_text, (False, True), key = config_key)
    elif config_key in ["include_script"]:
        result = create_radio(input_text, (False, True), description=description, key = config_key)
    elif config_key == "label_column_name":
        columns = [""] + list(label_columns)
        index = columns.index(default_value) if default_value in columns else 0
        result = create_select_box(input_text, columns, index=index,
                                   description = description, key = config_key)
    elif config_key == "usecols":
        columns = list(label_columns)
        # st.write(columns)
        result = create_multi_select_box(input_text, columns, columns,
                                   description = description, key = config_key)
    elif config_key == "skiprows":
        result = create_number_input(input_text, value=0, min_value = 0, description=description)
    elif config_key == "level":
        result = create_number_input(input_text, value=-1,
                                    min_value=-3,
                                    max_value=-1,
                                    description=description)
    elif config_key == "alt_glob" and glob_idx >= 0:
        if glob_idx >= len(default_value):
            result = create_text_input(input_text, key=key, description = description)
        else:
            if glob_idx == 0:
                key = "inputs"
            else:
                key = f"targets{glob_idx}"

            result = create_text_input(input_text,
                                   default_value[glob_idx][key],
                                   key=key, description = description)
    else:
        result = create_text_input(input_text, default_value, description = description, key = config_key)
    return result


if "config" not in st.session_state:
    with open("default.yaml", "r") as f:
        st.session_state.config = yaml.safe_load(f)

if "readme_config" not in st.session_state:
    st.session_state.readme_config = {}


def update_session_config():
    for key in st.session_state.config:
        if key == "alt_glob":   
            for comp in st.session_state.config[key]:
                st.session_state[comp] = st.session_state.config[key][comp]
        else:
            if st.session_state.config[key] is not None and key not in ['usecols']:
                st.session_state[key] = st.session_state.config[key]
            if key == 'usecols':
                st.session_state[key] = st.session_state.config[key].split(',')

def switch_state():
    update_session_config()
    
    for key in st.session_state.readme_config:
        st.session_state[key] = st.session_state.readme_config[key]

def reload_config(uploaded_file):
    st.session_state.config = yaml.load(uploaded_file.read())
    update_session_config()

def main():
    # Register your pages
    pages = {
        "Dataset Creation": first_page,
        "Dataset README": second_page,
    }

    st.sidebar.title("Dar")

    # Widget to select your page, you can choose between radio buttons or a selectbox
    page = st.sidebar.radio("Select a choice", tuple(pages.keys()), on_change = switch_state)

    if page:
        pages[page]()    
        
    # Display the selected page

def first_page():
    st.session_state.page = "first"
    st.title("Dataset Creation")
    dataset_link = ""
    zipped = False
    label_names = None
    zip_base_dir = ""
    alt_globs = {}
    xml_columns = ""
    level = None
    download_data_path = {}
    label_column_name = ""
    file_urls = ""
    header = 0
    pal = False
    alt_glob = ""
    default_file_type = ""
    lines = True
    json_key = ""
    encoding = "utf-8"

    insert_image('logo.png', caption='dar: build datasets by answering questions')

    uploaded_file = st.file_uploader("Load yaml file")
    if uploaded_file:
        reload_config(uploaded_file)

    dl_manager = DownloadManager()
        
    datasets_path = ""
    dataset_name = get_input("Dataset Name", "dataset_name", description = "Dataset Name don\'t add spaces")
    if dataset_name:
        main_class_code = get_class_code(dataset_name)
        st.session_state.config["dataset_name"] = dataset_name
    
    dataset_link = get_input("Dataset Directory", "dataset_link", description="Link or local directory")

    is_dir = False
    local_dir = False

    if dataset_link:
        if 'kaggle.com' in dataset_link:
            local_dir = True
            st.session_state.config["local_dir"] = local_dir
            username = st.text_input("User name", key = "kaggle_username")
            kaggle_key = st.text_input("Kaggle key", key = "kaggle_key")
            kaggle_login = od.authenticate_kaggle(username=username, kaggle_key = kaggle_key)
            
            if kaggle_login:
                st.write('logged in as ', username)
                dataset_link = od.download(dataset_link, data_dir = "./kaggle/")
            else:
                st.error(f"Please provide correct credentials",)
                

        elif not is_url(dataset_link):
            local_dir = True
            st.session_state.config["local_dir"] = local_dir

        if os.path.isdir(dataset_link):
            is_dir = True

        file_urls = convert_link(dataset_link)
        default_file_type = file_urls[0].split('.')[-1]
        zipped = any([
            ext in file_urls[0] for ext in ["zip", "rar", "tar.gz", "7z", "drive", "tgz", ".gz"]
        ])
        st.session_state.config["dataset_link"] = dataset_link
        if zipped or is_dir:
            if zipped:
                try:
                    zip_base_dir = dl_manager.download_and_extract(file_urls)[0]
                except:
                    file_urls = [dataset_link]
                    zip_base_dir = dl_manager.download_and_extract(file_urls)[0]
                extract_all(zip_base_dir)
            else:
                zip_base_dir = file_urls[0]

            
            download_data_path["inputs"] = get_valid_files(zip_base_dir)
            st.write(download_data_path)

            i = 0
            while True:
                if i== 0:
                    config_key = "inputs"
                else:
                    config_key = f"targets{i}" 

                alt_glob = get_input("Enter an input structure", config_key,
                                description="Use glob structure like **.txt", glob_idx=0)
                st.write({'base dir':zip_base_dir})
                if alt_glob:
                    download_data_path[config_key] = eval(f"glob('{zip_base_dir}/{alt_glob}')")
                    download_data_path[config_key].sort()
                    alt_globs[config_key] = alt_glob
                    i += 1
                else:
                    st.session_state.config["alt_glob"] = alt_globs
                    break

            pal = get_input("Path as labels ", "pal")
            st.session_state.config["pal"] = pal
            if pal:
                level = get_input(
                    "Label Level",
                    "level",
                    description="Useful for datasets where the labels are strctured as foulders, for example\
                    path\\sport\\00.txt should have level -2"
                )

                if level:
                    level = int(level)
                    st.session_state.config["level"] = level
                    if level == -1:
                        label_names = list(
                            set([
                                path.split("/")[-1]
                                for path in download_data_path["inputs"]
                            ]))
                        label_names = list(
                            set([lbl.split(".")[-2] for lbl in label_names]))
                    else:
                        label_names = list(
                            set([
                                path.split("/")[level:level + 1][0]
                                for path in download_data_path["inputs"]
                            ]))
                    st.write(label_names)
        else:
            download_data_path["inputs"] = dl_manager.download(file_urls)

    if dataset_link:
        split_code = get_split_code(file_urls,
                                    download_data_path,
                                    zip_base_dir,
                                    alt_globs,
                                    local_dir=local_dir,
                                    is_dir=is_dir)

        file_type = get_input("File Type", "file_type", default_value=default_file_type,
                              description= "Supported files: csv,txt,json,xml,xlsx,wav,jpg")
        # file types paramters
        columns = []
        best_sep = ","

        if file_type:
            st.session_state.config["file_type"] = file_type
            if file_type in ["txt", "csv"]:
                encoding = get_input("Set Encoding", "encoding", description="Encoding of the text")
                if encoding:
                    df = get_df(
                        file_type,
                        download_data_path,
                        lines=lines,
                        json_key=json_key,
                        header=header,
                        encoding= encoding
                    )                
                    st.session_state.config["encoding"] = encoding
                    st.write(df.head())

            if file_type in ["json", "txt"]:
                lines = get_input("Set Lines", "lines", description="Whether to consider new lines or not.")
                st.session_state.config["lines"] = lines

                df = get_df(
                        file_type,
                        download_data_path,
                        lines=lines,
                        json_key=json_key,
                        header=header,
                        encoding = encoding
                    )
                st.write(df.head())
            
            if file_type == "json":
                json_key = get_input("Json Key", "json_key", description="The json key that contains the data. ")
                if json_key:
                    st.session_state.config["json_key"] = json_key
                    df = get_df(
                        file_type,
                        download_data_path,
                        lines=lines,
                        json_key=json_key,
                        xml_columns=xml_columns,
                        header=header
                    )
                    st.write(df.head())

            if file_type == "xml":
                xml_columns = get_input(
                    "XML Columns",
                    "xml_columns",
                    description="Enter xml columns separated by comma."
                )
                if xml_columns:
                    st.session_state.config["xml_columns"] = xml_columns
                    xml_columns = xml_columns.split(",")
                    df = get_df(
                        file_type,
                        download_data_path,
                        lines=lines,
                        json_key=json_key,
                        xml_columns=xml_columns,
                        header=header
                    )
                    st.write(df.head())

            if file_type == "csv":
                alt_sep = get_input(f"CSV Separator",
                                    "alt_sep", default_value=best_sep, description="The separator used to split columns. ")
                if alt_sep:
                    st.session_state.config["alt_sep"] = alt_sep
                    best_sep = alt_sep
                    df = get_df(file_type, download_data_path, skiprows=0, sep=best_sep, header=header, encoding = encoding)
                    st.write(df.head())

            skiprows = get_input("Skipped Rows", "skiprows", description="Number of rows to skipp when reading the file.")
            st.session_state.config["skiprows"] = skiprows

            if skiprows:
                skiprows = int(skiprows)

                df = get_df(
                    file_type,
                    download_data_path,
                    skiprows=skiprows,
                    sep=best_sep,
                    lines=lines,
                    json_key=json_key,
                    xml_columns=xml_columns,
                    header=header,
                    encoding=encoding
                )
                st.write(df.head())

            df = get_df(
                    file_type,
                    download_data_path,
                    skiprows=skiprows,
                    sep=best_sep,
                    lines=lines,
                    json_key=json_key,
                    xml_columns=xml_columns,
                    header=header,
                    encoding = encoding
                )
            st.write(df.head())
            
            columns = list(df.columns)
            columns = [str(c) for c in columns]
            prev_columns = columns

            if file_type in ["jpg", "wav", "xml", "json"]:
                header = 0
            else:
                if get_input(
                    "Headers", "header", description="Does the dataset have a header with column names?"
                ):
                    header = 0
                else:
                    header = None
                
                st.session_state.config["header"] = True if header ==0 else False

                
                df = get_df(
                        file_type,
                        download_data_path,
                        skiprows=skiprows,
                        sep=best_sep,
                        lines=lines,
                        json_key=json_key,
                        header=header,
                        encoding = encoding
                    )
                


            new_columns = get_input("New Column Names","new_columns", 
                                    description="Enter new column names separated by comma: Column1,Column2, etc. ")
            if new_columns:
                new_columns = new_columns.split(",") if type(new_columns) != list else new_columns
                st.session_state.config["new_columns"] = ",".join(new_columns)
                st.write(new_columns)
                df = get_df(
                        file_type,
                        download_data_path,
                        new_columns=new_columns,
                        skiprows=skiprows,
                        sep=best_sep,
                        lines=lines,
                        json_key=json_key,
                        xml_columns=xml_columns,
                        header=header,
                        encoding = encoding
                    )
                st.write(df.head())

            usecols = get_input("Columns to Use","usecols", label_columns= new_columns if len(new_columns) else columns, 
                                    description="Choose which columns to be used")
            if usecols:
                df = get_df(
                        file_type,
                        download_data_path,
                        new_columns = new_columns, 
                        skiprows=skiprows,
                        sep=best_sep,
                        lines=lines,
                        json_key=json_key,
                        header=header,
                        encoding = encoding,
                        xml_columns=xml_columns,
                        usecols = usecols,
                    )
                st.session_state.config["usecols"] = ",".join(usecols)
            # st.write(st.session_state)

            if not pal:
                label_column_name = get_input("Label Column Name", "label_column_name", label_columns=df.columns, 
                                            description="The column name for the labels, useful for classificaiton datasets")
                st.session_state.config["label_column_name"] = label_column_name

            if label_column_name:
                label_names = list(set(df[label_column_name]))
                st.write(label_names)

            generate_code = get_generate_code(
                file_type,
                columns,
                new_columns,
                label_names,
                label_column_name,
                skiprows=skiprows if skiprows else 0,
                use_labels_from_path=pal,
                sep=best_sep if best_sep else ",",
                header=header,
                lines=lines,
                json_key=json_key if json_key else "",
                level=level if level else None,
                alt_globs=alt_globs,
                encoding=encoding,
                usecols = usecols
            )

            import_code = get_imports_code(file_type)
            feature_columns = columns
            if len(new_columns):
                feature_columns = new_columns
            if len(usecols):
                feature_columns = usecols
            features_code = get_features_code([c for c in feature_columns if c != label_column_name], label_names)

            # generate code and load the dataset
            code = import_code + main_class_code + features_code + split_code + generate_code
            datasets_path = get_input("Save Directory",
                                    "datasets_path",
                                    description="Local directory to save the data")
            if datasets_path:
                
                save_path = f"{datasets_path}/{dataset_name}"
                os.makedirs(save_path, exist_ok=True)


                open(f"{save_path}/{dataset_name}.py", "w").write(code)
                if local_dir:
                    st.write(dataset_link)
                    dataset = load_dataset(save_path, data_dir=dataset_link)
                else:
                    dataset = load_dataset(save_path)
                
                st.write(dataset)
                if 'train' in dataset:
                    st.write(dataset['train'][0])
                elif 'validation' in dataset:
                    st.write(dataset['validation'][0])
                else:
                    st.write(dataset['test'][0])
                st.session_state.config["datasets_path"] = datasets_path
                saved_yaml_file = f"{save_path}/config.yaml"
                # st.write(config)
                with open(saved_yaml_file, "w") as outfile:
                    yaml.dump(st.session_state.config, outfile, default_flow_style=False)
                if not os.path.isfile(f"{save_path}/README.md"):
                    shutil.copyfile(f"temp.md", f"{save_path}/README.md")
                

                st.sidebar.write(st.session_state.config)

                include_script = get_input("Include script", "include_script", description="Include the same name for the script in the upload. ")
                hf_path = get_input("HuggingFace path", "hf_path", description="Save data in HuggingFace hub Username/dataset_name")
                token = create_text_input("HuggingFace token", type="password", description="HuggingFace token hf_**")   
                    
                if token and hf_path:
                    login(token)
                    upload = st.button('Upload')
                    if upload:
                        with st.spinner('Uploading ...'):
                            dataset.push_to_hub(f"{hf_path}")
                            upload_file(f"{save_path}/README.md", repo_id=hf_path)
                            if include_script:
                                upload_file(f"{save_path}/{dataset_name}.py", repo_id=hf_path)
                            upload_file(f"{save_path}/config.yaml", repo_id=hf_path)
                            st.write(f"Uploaded to [{hf_path}](https://huggingface.co/datasets/{hf_path})")
                
def second_page():
    st.session_state.page = "second"
    with open('temp.md', 'r') as f:
        lines = f.read().splitlines()
    title = lines[0].replace("[Dataset Name]", st.session_state.config["dataset_name"]) + "\n"
    toc = lines[2:26]
    info = lines[26:35]
    rest = lines[35:]
    st.markdown(title)
    col1, col2 = st.columns([5, 5])
    output_readme = ""
    with col1:
        for i, line in enumerate(info):
            if "[info]" in line:
                st.markdown(line.replace(": [info]", ""))
                input = st.text_input(line, label_visibility="collapsed", key = line)
                if input:
                    output_readme = output_readme + line.replace("[info]", input)+  "\n"
                    st.session_state.readme_config[line] = input.strip()
                else:
                    output_readme = output_readme + line+  "\n"

            else:
                output_readme = output_readme + line+  "\n"

        for i, line in enumerate(rest):            
            if line == "[More Information Needed]":
                st.markdown(rest[i-2])
                input = st.text_area(rest[i-2], label_visibility="collapsed", key = rest[i-2])
                if input:
                    if 'Citation' in rest[i-2]:
                        output_readme = output_readme + f"```\n{input.strip()}\n``` \n"
                        st.session_state.readme_config[rest[i-2]] = input
                    else:
                        output_readme = output_readme + input+  "\n"
                else:
                    output_readme = output_readme + line+  "\n"

            else:
                output_readme = output_readme + line+  "\n"
    with col2:
        st.write(output_readme)
    
    output_readme = title + "\n" + "\n".join(toc) + "\n"+output_readme
    save = st.button('save')
    if save:
        readme_loc = f'datasets/{st.session_state.config["dataset_name"]}/README.md'
        with open(readme_loc, "w") as f:
            f.write(output_readme)
    


if __name__ == "__main__":
    main()

