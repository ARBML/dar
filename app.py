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


def insert_image(img_path, caption):
    col1, col2, col3 = st.columns([1, 3, 1])

    with col1:
        st.write("")

    with col2:
        st.image(img_path, caption=caption)

    with col3:
        st.write("")


def get_input(input_text,
              config,
              value,
              glob_idx=-1,
              default_value="",
              label_columns=[],
              key=0):
    global load_from_config

    config_value = config[value]
    default_value = config_value

    if value == "file_type":
        valid_types = ("", "csv", "txt", "json", "xml", "xlsx", "wav")
        result = st.selectbox(input_text,
                              options=valid_types,
                              index=valid_types.index(default_value))
    elif value == "alt_sep":
        valid_seps = ("", ",", ";", "|", "tab")
        result = st.selectbox(input_text,
                              options=valid_seps,
                              index=valid_seps.index(default_value))
    elif value in ["header", "lines"]:
        result = st.radio(input_text, (True, False))
    elif value in ["pal", "local_dir"]:
        result = st.radio(input_text, (False, True))
    elif value == "label_column_name":
        columns = [""] + list(label_columns)
        result = st.selectbox(input_text,
                              columns,
                              index=columns.index(default_value))
    elif value == "skiprows":
        result = st.number_input(input_text, value=0)
    elif value == "level":
        result = st.number_input(input_text,
                                 value=-1,
                                 min_value=-3,
                                 max_value=-1)
    elif value == "alt_glob" and glob_idx >= 0:
        if glob_idx >= len(default_value):
            result = st.text_input(input_text, key=key)
        else:
            result = st.text_input(input_text,
                                   default_value[glob_idx],
                                   key=key)
    else:
        result = st.text_input(input_text, default_value)
    return result


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

insert_image('logo.png', caption='dar: build datasets by answering questions')

uploaded_file = st.file_uploader("Load yaml file: ")
if uploaded_file:
    config = yaml.load(uploaded_file.read())
else:
    with open("default.yaml", "r") as f:
        config = yaml.safe_load(f)

dl_manager = DownloadManager()

datasets_path = ""

dataset_name = get_input("Dataset Name: ", config, "dataset_name")
saved_yaml_file = f"{config['datasets_path']}/{dataset_name}/config.yaml"
if dataset_name:
    if os.path.isfile(saved_yaml_file):
        st.write(f"loading yaml from {saved_yaml_file}")
        with open(saved_yaml_file, "r") as f:
            config = yaml.safe_load(f)
    main_class_code = get_class_code(dataset_name)
    config["dataset_name"] = dataset_name

local_dir = get_input("Local Dir ", config, "local_dir")
if local_dir:
    config["local_dir"] = local_dir
else:
    local_dir = False
dataset_link = get_input("Dataset Link/Dir ", config, "dataset_link")

if dataset_link:
    file_urls = convert_link(dataset_link)
    zipped = any([
        ext in file_urls[0] for ext in ["zip", "rar", "tar.gz", "7z", "drive"]
    ])
    config["dataset_link"] = dataset_link

alt_glob = ""

if zipped:
    try:
        zip_base_dir = dl_manager.download_and_extract(file_urls)[0]
    except:
        file_urls = [dataset_link]
        zip_base_dir = dl_manager.download_and_extract(file_urls)[0]

    extract_all(zip_base_dir)
    st.write(zip_base_dir)
    download_data_path["inputs"] = get_valid_files(zip_base_dir)
    st.write(download_data_path)
    alt_glob = get_input("Input glob structure: ",
                         config,
                         "alt_glob",
                         glob_idx=0)
    i = 1
    while alt_glob:

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
        alt_glob = get_input("Enter a target structure: ",
                             config,
                             "alt_glob",
                             glob_idx=i,
                             key=i)

    pal = get_input("path as labels ", config, "pal")
    if pal:
        level = get_input(
            "level for the labels: ",
            config,
            "level",
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
                                local_dir=local_dir)

    file_type = get_input("Enter the type: ", config, "file_type")
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
            lines = get_input("set lines: ", config, "lines")
            if lines:
                config["lines"] = lines

        if file_type == "json":
            json_key = get_input("set json key: ", config, "json_key")
            if json_key:
                config["json_key"] = json_key

        if file_type == "xml":
            xml_columns = get_input(
                "Enter xml columns: ",
                config,
                "xml_columns",
            )
            if xml_columns:
                config["xml_columns"] = xml_columns
                xml_columns = xml_columns.split(",")

        if file_type == "csv":
            alt_sep = get_input(f"Set a separator for {file_type}: ", config,
                                "alt_sep")
            if alt_sep:
                config["alt_sep"] = alt_sep
                best_sep = alt_sep
                df = get_df(file_type, download_data_path, 0, sep=best_sep)
                st.write(df.head())
        else:
            df = get_df(
                file_type,
                download_data_path,
                lines=lines,
                json_key=json_key,
                columns=xml_columns,
            )
            st.write(df.head())

        skiprows = get_input("Enter rows to skip: ", config, "skiprows")
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

        if file_type in ["wav", "xml", "json"]:
            header = 0
        else:
            header = get_input(
                "Columns has a header: ",
                config,
                "header",
            )
            if header:
                config["header"] = header
                header = 0 if header else None

        new_columns = get_input("Enter new columns separated by comma: ",
                                config, "new_columns")
        if new_columns:
            new_columns = new_columns.split(
                ",") if type(new_columns) != list else new_columns
            config["new_columns"] = new_columns

            if len(new_columns) > 0:
                columns = new_columns
            df.columns = columns
            st.write(df.head())

        if not pal:
            label_column_name = get_input("Enter label column name: ",
                                          config,
                                          "label_column_name",
                                          label_columns=df.columns)
            config["label_column_name"] = label_column_name

        if label_column_name:
            label_names = list(set(df[label_column_name]))
            print(label_names)

        generate_code = get_generate_code(
            file_type,
            columns,
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
        features_code = get_features_code(columns, label_names)

        # generate code and load the dataset
        code = import_code + main_class_code + features_code + split_code + generate_code
        datasets_path = get_input("Save Directory: ",
                                  config,
                                  "datasets_path",
                                  default_value="datasets")
        if datasets_path:
            config["datasets_path"] = datasets_path
        else:
            datasets_path = config["datasets_path"]

        os.makedirs(f"{datasets_path}/{dataset_name}", exist_ok=True)
        saved_yaml_file = f"{datasets_path}/{dataset_name}/config.yaml"
        open(f"{datasets_path}/{dataset_name}/{dataset_name}.py",
             "w").write(code)
        with open(saved_yaml_file, "w") as outfile:
            yaml.dump(config, outfile, default_flow_style=False)

        dataset = load_dataset(f"{datasets_path}/{dataset_name}")
        st.write(dataset)
        st.write(dataset['train'][0])
        hf_path = get_input("HF path", config, "hf_path")

        token = st.text_input("Hf token", type="password")
        if token and st.button('Upload'):
            login(token)
            if hf_path:
                dataset.push_to_hub(f"{hf_path}/{dataset_name}")
                st.write("uploaded!")
