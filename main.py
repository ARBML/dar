import os
import argparse
import signal
import sys
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


def get_input(input_text, config_value, glob_idx=-1):
    global load_from_config

    if load_from_config:
        if glob_idx >= 0:
            if glob_idx == len(config_value):
                return ""
            else:
                return config_value[glob_idx]
        else:
            return config_value
    else:
        result = input(input_text)
        if result in ["y", "n"]:
            return True if result == "y" else "n"
        elif result:
            return result
        else:
            return config_value


def signal_handler(signal, frame):
    # your code here
    print("\n ...")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

# Create the parser
my_parser = argparse.ArgumentParser()
my_parser.add_argument("--pal", "-paths_as_labels", action="store_true")
my_parser.add_argument("--squad", "-squad_link_dataset", action="store_true")
my_parser.add_argument("--p", "-save_pah", type=str, action="store", default="datasets")
my_parser.add_argument("--hf", "-hf_path", type=str, action="store", default="arbml")
my_parser.add_argument("--t", "-hf_token", type=str, action="store", default="")
my_parser.add_argument(
    "--yaml", "-load_yaml_file", type=str, action="store", default=""
)

args = my_parser.parse_args()
yaml_path = args.yaml if args.yaml else "default.yaml"
load_from_config = args.yaml

with open(yaml_path, "r") as f:
    config = yaml.safe_load(f)

dl_manager = DownloadManager()

datasets_path = args.p

dataset_name = get_input("Dataset Name: ", config["dataset_name"])
os.makedirs(f"{datasets_path}/{dataset_name}", exist_ok=True)
saved_yaml_file = f"{datasets_path}/{dataset_name}/config.yaml"
if os.path.isfile(saved_yaml_file):
    load_from_config = True
    with open(saved_yaml_file, "r") as f:
        config = yaml.safe_load(f)
main_class_code = get_class_code(dataset_name)
dataset_link = get_input("Enter Direct URL: ", config["dataset_link"])
config["dataset_link"] = dataset_link

file_urls = convert_link(dataset_link)
print(file_urls)
zipped = any([ext in file_urls[0] for ext in ["zip", "rar", "tar.gz", "7z", "drive"]])
label_names = None

zip_base_dir = ""
alt_globs = []
xml_columns = []
level = None
download_data_path = {}
label_column_name = ""

if zipped:
    try:
        download_data_path = dl_manager.download_and_extract(file_urls)[0]
    except:
        file_urls = [dataset_link]
        download_data_path = dl_manager.download_and_extract(file_urls)[0]
    zip_base_dir = download_data_path
    extract_all(zip_base_dir)
    print(zip_base_dir)
    download_data_path = {}
    download_data_path["inputs"] = get_valid_files(zip_base_dir)
    print(download_data_path)

    alt_glob = ""
    i = 0

    while True:
        alt_glob = get_input(
            "Enter a glob structure: ",
            config["alt_glob"],
            glob_idx=i,
        )

        if not alt_glob:
            break

        if len(alt_globs) == 0:
            download_data_path["inputs"] = eval(
                alt_glob.replace("glob('", f"glob('{zip_base_dir}/")
            )
            download_data_path["inputs"].sort()
            alt_globs.append({"inputs": alt_glob})
        else:
            download_data_path[f"targets{i}"] = eval(
                alt_glob.replace("glob('", f"glob('{zip_base_dir}/")
            )
            download_data_path[f"targets{i}"].sort()
            alt_globs.append({f"targets{i}": alt_glob})
        i += 1

    if args.pal:
        level = get_input(
            "level for the labels: ",
            config["level"],
        )
        level = None if level is None else int(level)
        config["level"] = level

        if level == -1:
            label_names = list(
                set([path.split("/")[-1] for path in download_data_path["inputs"]])
            )
            label_names = list(set([lbl.split(".")[-2] for lbl in label_names]))
        else:
            label_names = list(
                set(
                    [
                        path.split("/")[level : level + 1][0]
                        for path in download_data_path["inputs"]
                    ]
                )
            )
        print(label_names)
else:
    download_data_path["inputs"] = dl_manager.download(file_urls)

print(download_data_path)

split_code = get_split_code(file_urls, download_data_path, zip_base_dir, alt_globs)

if args.squad:
    code = get_squad_code(dataset_name, split_code.replace("\t", "    "))
else:
    file_type = get_input("Enter the type: ", config["file_type"])
    config["file_type"] = file_type
    # file types paramters
    lines = False
    json_key = ""
    columns = []
    best_sep = ","

    if file_type in ["json", "txt"]:
        lines = get_input("set lines (y/n)? ", config["lines"])
        config["lines"] = lines

    if file_type == "json":
        json_key = get_input("set json key: ", config["json_key"])
        config["json_key"] = json_key

    if file_type == "xml":
        xml_columns = get_input(
            "Enter columns separated by comma: ",
            config["xml_columns"],
        ).split(",")
        config["xml_columns"] = xml_columns

    df = get_df(
        file_type,
        download_data_path,
        lines=lines,
        json_key=json_key,
        columns=xml_columns,
    )
    print(df.head())

    if file_type == "csv":
        alt_sep = get_input(
            f"Set a separator for {file_type}: ",
            config["alt_sep"],
        )
        config["alt_sep"] = alt_sep

        if alt_sep:
            best_sep = alt_sep
            if alt_sep == "tab":
                df = get_df(file_type, download_data_path, 0, sep="\t")
            else:
                df = get_df(file_type, download_data_path, 0, sep=best_sep)
            print(df.head())
        else:
            print("using default separator ", {best_sep})

    skiprows = get_input("Enter rows to skip: ", config["skiprows"])
    skiprows = int(skiprows)
    config["skiprows"] = skiprows

    if skiprows != 0:
        df, _ = get_df(
            file_type,
            download_data_path,
            skiprows=skiprows,
            sep=best_sep,
            lines=lines,
            json_key=json_key,
        )
        print(df.head())

    columns = list(df.columns)

    if file_type in ["wav", "xml", "json"]:
        header = 0
    else:
        header = get_input(
            "Columns has a header (y/n)? ",
            config["header"],
        )
        config["header"] = header
        header = 0 if header else None

    new_columns = get_input(
        "Enter new columns separated by comma: ",
        config["new_columns"],
    )
    new_columns = new_columns.split(",") if type(new_columns) != list else new_columns
    config["new_columns"] = new_columns

    if len(new_columns) > 0:
        columns = new_columns
    df.columns = columns
    print(columns)

    if not args.pal:
        label_column_name = get_input(
            "Enter label column name: ",
            config["label_column_name"],
        )
        config["label_column_name"] = label_column_name

    if label_column_name != "":
        label_names = list(set(df[label_column_name]))
        print(label_names)

    generate_code = get_generate_code(
        file_type,
        columns,
        label_names,
        label_column_name,
        skiprows=skiprows,
        use_labels_from_path=args.pal,
        sep=best_sep,
        header=header,
        lines=lines,
        json_key=json_key,
        level=level,
        alt_globs=alt_globs,
    )

    import_code = get_imports_code(file_type)
    features_code = get_features_code(columns, label_names)

    # generate code and load the dataset
    code = import_code + main_class_code + features_code + split_code + generate_code

open(f"{datasets_path}/{dataset_name}/{dataset_name}.py", "w").write(code)
with open(saved_yaml_file, "w") as outfile:
    yaml.dump(config, outfile, default_flow_style=False)

dataset = load_dataset(f"{datasets_path}/{dataset_name}")
print(dataset)

if input("push to hub: ") == "y":
    print("logging in ...")
    try:
        login(args.t)
    except:
        raise ("Error: need to provide token using --t parameter")
    print("pushing to the hub")
    dataset.push_to_hub(f"{args.hf}/{dataset_name}")
