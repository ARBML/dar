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
import shutil

valid_file_types = ("", "csv", "txt", "json", "xml", "xlsx", "wav", "jpg")
valid_csv_sep = ("", ",", ";", "|", "tab")

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
    
    if st.session_state.load_config:
        default_value = st.session_state.config[config_key]

    if type(default_value) == list:
        default_value = ",".join(default_value)

    if config_key == "file_type":
        default_value = default_value if default_value in valid_file_types else ""
        result = create_select_box(input_text, valid_file_types,
                              index=valid_file_types.index(default_value), description=description)
    elif config_key == "alt_sep":
        default_value = default_value if default_value in valid_csv_sep else ""
        result = create_select_box(input_text,valid_csv_sep,
                              index=valid_csv_sep.index(default_value), description=description)
    elif config_key in ["header", "lines"]:
        result = create_radio(input_text, (True, False), description = description)
    elif config_key in ["pal", "local_dir"]:
        result = create_radio(input_text, (False, True))
    elif config_key == "label_column_name":
        columns = [""] + list(label_columns)
        index = columns.index(default_value) if default_value in columns else 0
        result = create_select_box(input_text, columns, index=index,
                                   description = description)
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
        result = create_text_input(input_text, default_value, description = description)
    return result


if "config" not in st.session_state:
        with open("default.yaml", "r") as f:
            st.session_state.config = yaml.safe_load(f)

if "load_config" not in st.session_state:
    st.session_state.load_config = False 

def main():
    # Register your pages
    pages = {
        "Dataset Creation": first_page,
        "Dataset README": second_page,
    }

    st.sidebar.title("Dar")

    # Widget to select your page, you can choose between radio buttons or a selectbox
    page = st.sidebar.radio("Select a choice", tuple(pages.keys()))

    # Display the selected page
    pages[page]()

def first_page():
    st.title("Dataset Creation")
    dataset_link = ""
    zipped = False
    label_names = None
    zip_base_dir = ""
    alt_globs = []
    xml_columns = ""
    level = None
    download_data_path = {}
    label_column_name = ""
    file_urls = ""
    header = None
    pal = False
    alt_glob = ""
    config = {}
    default_file_type = ""


    insert_image('logo.png', caption='dar: build datasets by answering questions')

    uploaded_file = st.file_uploader("Load yaml file")
    if uploaded_file:
        st.session_state.config = yaml.load(uploaded_file.read())
        st.session_state.load_config = True

    dl_manager = DownloadManager()

    datasets_path = ""

    dataset_name = get_input("Dataset Name", "dataset_name", default_value="NewDataset", 
                             description = "Dataset Name don\'t add spaces")
    if dataset_name:
        main_class_code = get_class_code(dataset_name)
        config["dataset_name"] = dataset_name

    dataset_link = get_input("Dataset Directory", "dataset_link", description="Link or local directory")

    is_dir = False
    local_dir = False

    if dataset_link:
        if not is_url(dataset_link):
            local_dir = True
            config["local_dir"] = local_dir

        if os.path.isdir(dataset_link):
            is_dir = True

        file_urls = convert_link(dataset_link)
        default_file_type = file_urls[0].split('.')[-1]
        zipped = any([
            ext in file_urls[0] for ext in ["zip", "rar", "tar.gz", "7z", "drive"]
        ])
        config["dataset_link"] = dataset_link
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
            alt_glob = get_input("Enter an input structure", "alt_glob",
                                description="Use glob structure like **.txt", glob_idx=0)
            i = 1
            while alt_glob:
                alt_glob = f"glob('{alt_glob}')"
                if len(alt_globs) == 0:
                    download_data_path["inputs"] = eval(
                        alt_glob.replace("glob('", f"glob('{zip_base_dir}/"))
                    download_data_path["inputs"].sort()
                    alt_globs.append({"inputs": alt_glob})
                else:
                    download_data_path[f"targets{i}"] = eval(
                        alt_glob.replace("glob('", f"glob('{zip_base_dir}/"))
                    download_data_path[f"targets{i}"].sort()
                    alt_globs.append({f"targets{i}": alt_glob})
                    i += 1
                alt_glob = get_input("Enter a target structure",
                                    "alt_glob", glob_idx=i, key=i, 
                                    description= "Target files, useful for parallel datasets, like machine translation, speech recognition, etc.")
            else:
                config["alt_glob"] = alt_globs
            pal = get_input("Path as labels ", "pal")
            if pal:
                level = get_input(
                    "Label Level",
                    "level",
                    description="Useful for datasets where the labels are strctured as foulders, for example\
                    path\\sport\\00.txt should have level -2"
                )

                if level:
                    level = int(level)
                    config["level"] = level
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
        config["file_type"] = file_type
        # file types paramters
        lines = False
        json_key = ""
        columns = []
        best_sep = ","

        if file_type:
            df = get_df(
                file_type,
                download_data_path,
                lines=lines,
                json_key=json_key,
                columns=xml_columns,
            )
            st.write(df.head())
            if file_type in ["json", "txt"]:
                lines = get_input("Set Lines", "lines", description="Whether to consider new lines or not.")
                if lines:
                    config["lines"] = lines

            if file_type == "json":
                json_key = get_input("Json Key", "json_key", description="The json key that contains the data. ")
                if json_key:
                    config["json_key"] = json_key
                    df = get_df(
                        file_type,
                        download_data_path,
                        lines=lines,
                        json_key=json_key,
                        columns=xml_columns,
                    )
                    st.write(df.head())

            if file_type == "xml":
                xml_columns = get_input(
                    "XML Columns",
                    "xml_columns",
                    description="Enter xml columns separated by comma."
                )
                if xml_columns:
                    config["xml_columns"] = xml_columns
                    xml_columns = xml_columns.split(",")
                    df = get_df(
                        file_type,
                        download_data_path,
                        lines=lines,
                        json_key=json_key,
                        columns=xml_columns,
                    )
                    st.write(df.head())

            if file_type == "csv":
                alt_sep = get_input(f"CSV Separator",
                                    "alt_sep", default_value=best_sep, description="The separator used to split columns. ")
                if alt_sep:
                    config["alt_sep"] = alt_sep
                    best_sep = alt_sep
                    df = get_df(file_type, download_data_path, 0, sep=best_sep)
                    st.write(df.head())

            skiprows = get_input("Skipped Rows", "skiprows", description="Number of rows to skipp when reading the file.")
            if skiprows:
                skiprows = int(skiprows)
                config["skiprows"] = skiprows

                if skiprows != 0:
                    df = get_df(
                        file_type,
                        download_data_path,
                        skiprows=skiprows,
                        sep=best_sep,
                        lines=lines,
                        json_key=json_key,
                    )
                    st.write(df.head())

            columns = list(df.columns)
            columns = [str(c) for c in columns]
            prev_columns = columns

            if file_type in ["jpg", "wav", "xml", "json"]:
                header = 0
            else:
                header = get_input(
                    "Headers", "header", description="Does the dataset have a header with column names?"
                )
                if header:
                    config["header"] = header
                    header = 0 if header else None

            new_columns = get_input("New Column Names","new_columns", 
                                    description="Enter new column names separated by comma: Column1,Column2, etc. ")
            if new_columns:
                new_columns = new_columns.split(",") if type(new_columns) != list else new_columns
                config["new_columns"] = new_columns
                st.write(new_columns)

                if len(new_columns) > 0:
                    columns = new_columns
                df.columns = columns
                st.write(df.head())

            if not pal:
                label_column_name = get_input("Label Column Name", "label_column_name", label_columns=df.columns, 
                                            description="The column name for the labels, useful for classificaiton datasets")
                config["label_column_name"] = label_column_name

            if label_column_name:
                label_names = list(set(df[label_column_name]))
                st.write(label_names)

            generate_code = get_generate_code(
                file_type,
                columns,
                prev_columns,
                label_names,
                label_column_name,
                skiprows=skiprows if skiprows else 0,
                use_labels_from_path=pal,
                sep=best_sep if best_sep else ",",
                header=header if header else 0,
                lines=True if lines else False,
                json_key=json_key if json_key else "",
                level=level if level else None,
                alt_globs=alt_globs,
            )

            import_code = get_imports_code(file_type)
            features_code = get_features_code([c for c in columns if c != label_column_name], label_names)

            # generate code and load the dataset
            code = import_code + main_class_code + features_code + split_code + generate_code
            datasets_path = get_input("Save Directory",
                                    "datasets_path",
                                    default_value="datasets",
                                    description="Local directory to save the data")
            if datasets_path:
                config["datasets_path"] = datasets_path
            else:
                datasets_path = st.session_state.config["datasets_path"]

            saved = st.button('Save')
            save_path = f"{datasets_path}/{dataset_name}"

            if saved:
                os.makedirs(save_path, exist_ok=True)
                saved_yaml_file = f"{save_path}/config.yaml"
                open(f"{save_path}/{dataset_name}.py",
                "w").write(code)
                for key_config in st.session_state.config:
                    if key_config not in config:
                        config[key_config] = st.session_state.config[key_config]
                # st.write(config)
                with open(saved_yaml_file, "w") as outfile:
                    yaml.dump(config, outfile, default_flow_style=False)
                if not os.path.isfile(f"{save_path}/README.md"):
                    shutil.copyfile(f"temp.md", f"{save_path}/README.md")
                
                if local_dir:
                    dataset = load_dataset(save_path, data_dir=dataset_link)
                else:
                    dataset = load_dataset(save_path)

                st.write(dataset)
                if 'train' in dataset:
                    st.write(dataset['train'][0])
                with st.spinner('Saving ...'):
                    dataset.save_to_disk(save_path)

            hf_path = get_input("HuggingFace path", "hf_path", description="Save data in HuggingFace hub Username/dataset_name")
            token = create_text_input("HuggingFace token", type="password", description="HuggingFace token hf_**")   
                
            if token and hf_path:
                if local_dir:
                    dataset = load_dataset(save_path, data_dir=dataset_link)
                else:
                    dataset = load_dataset(save_path)
                login(token)
                upload = st.button('Upload')
                if upload:
                    # st.write(dataset)
                    # st.write(dataset['train'][0])
                    with st.spinner('Uploading ...'):
                        dataset.push_to_hub(f"{hf_path}")
                        upload_file(f"{save_path}/README.md", repo_id=hf_path)
                        upload_file(f"{save_path}/{dataset_name}.py", repo_id=hf_path)
                        upload_file(f"{save_path}/config.yaml", repo_id=hf_path)
                        st.write(f"Uploaded to [{hf_path}](https://huggingface.co/datasets/{hf_path})")

def second_page():
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
                input = st.text_input(line, label_visibility="collapsed")
                if input:
                    output_readme = output_readme + line.replace("[info]", input)+  "\n"
                else:
                    output_readme = output_readme + line+  "\n"

            else:
                output_readme = output_readme + line+  "\n"

        for i, line in enumerate(rest):            
            if line == "[More Information Needed]":
                st.markdown(rest[i-2])
                input = st.text_area(rest[i-2], label_visibility="collapsed")
                if input:
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

