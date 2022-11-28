import fsspec
import re
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import pandas as pd
import zipfile
from glob import glob
from .constants import *
import json


def convert_link(links):
    output = []
    for link in links.split(","):
        if "github.com" in link.lower():
            user_name = link.split("/")[3]
            repo_name = link.split("/")[4]
            if link.count("/") > 4:
                branch_name = "master" if "master" in link else "main"

                base_path = f"https://raw.githubusercontent.com/{user_name}/{repo_name}/{branch_name}/"
                file_name = link.split(branch_name)[-1][1:]
                fs = fsspec.filesystem("github", org=user_name, repo=repo_name)

                if fs.isdir(file_name):
                    output = output + [base_path + f for f in fs.ls(f"{file_name}/")]
                else:
                    output.append(base_path + file_name)
            else:
                output.append(
                    f"https://github.com/{user_name}/{repo_name}/archive/master.zip"
                )
        elif "drive.google" in link.lower():
            base = "https://drive.google.com/file/d/"
            trail = "/view"
            id = link.replace(trail, "").replace(base, "")
            output.append(f"https://drive.google.com/uc?export=download&id={id}")
        elif "docs.google" in link.lower():
            sheet_name = "Sheet1"
            sheet_id = link.split("/d/")[-1].split("/")[0]
            url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
            output.append(url)
        else:
            output.append(link)
    return output


def get_xml_data(bs, column):
    elements = [attr[column] for attr in bs.find_all(attrs={column: re.compile(".")})]
    if len(elements) == 0:
        elements = [el.text for el in bs.find_all(column)]
    return elements


def read_xml(paths, i, columns=None):
    if columns is None:
        tree = ET.parse(paths["inputs"][i])
        root = tree.getroot()
        xml_string = ET.tostring(root, encoding="unicode", method="xml")
        return pd.DataFrame(xml_string[:500].split("\n"))
    else:
        dfs = []
        for path_name in paths:
            with open(paths[path_name][i], "rb") as f:
                data = f.read()

            bs = BeautifulSoup(data, "xml")
            data = {}
            for column in columns:
                elements = get_xml_data(bs, column)
                data[column] = elements
            dfs.append(pd.DataFrame(data))
        return pd.concat(dfs, axis=1)


def read_json(paths, i, lines=False, json_key=""):
    dfs = []
    for path_name in paths:
        if json_key:
            data = json.load(open(paths[path_name][i]))
            df = pd.DataFrame(data[json_key])
        else:
            df = pd.read_json(open(paths[path_name][i]), lines=lines)
        dfs.append(df)
    return pd.concat(dfs, axis=1)


def read_csv(paths, i, sep=",", skiprows=0):
    dfs = []
    for path_name in paths:
        df = pd.read_csv(
            paths[path_name][i], sep=sep, skiprows=skiprows, error_bad_lines=False
        )
        dfs.append(df)

    return pd.concat(dfs, axis=1)


def read_wav(paths, i):
    dfs = []
    for path_name in paths:
        if paths[path_name][i].endswith(".wav") or paths[path_name][i].endswith(".mp3"):
            raw_data = {"path": [paths[path_name][i]], "audio": [paths[path_name][i]]}
        else:
            raw_data = {"text": [open(paths[path_name][i]).read()]}
        df = pd.DataFrame(raw_data)
        dfs.append(df)
    df = pd.concat(dfs, axis=1)
    return df


def read_excel(paths, i, skiprows=0):
    dfs = []
    for path_name in paths:
        df = pd.read_excel(paths[path_name][i], skiprows=skiprows)
        dfs.append(df)
    return pd.concat(dfs, axis=1)


def read_txt(paths, i, skiprows=0, lines=True):
    dfs = []
    for path_name in paths:
        if lines:
            df = pd.DataFrame(
                open(paths[path_name][i], "r").read().splitlines()[skiprows:]
            )
        else:
            df = pd.DataFrame([open(paths[path_name][i], "r").read()])
        dfs.append(df)
    return pd.concat(dfs, axis=1)


def get_df(type, paths, skiprows=0, sep=",", lines=False, json_key="", columns=None):
    dfs = []

    for i, _ in enumerate(paths["inputs"]):

        if type == "xlsx":
            df = read_excel(paths, i, skiprows=skiprows)

        if type == "jsonl" or type == "json":
            df = read_json(paths, i, lines=lines, json_key=json_key)

        if type == "wav":
            df = read_wav(paths, i)

        if type == "xml":
            df = read_xml(paths, i, columns=columns)

        if type == "txt":
            df = read_txt(paths, i, skiprows=skiprows, lines=lines)

        if type == "csv":
            df = read_csv(paths, i, sep=f"{sep}", skiprows=skiprows)

        if len(dfs) > 0:
            df.columns = dfs[-1].columns

        dfs.append(df)
    return pd.concat(dfs)


def get_split_user(split_files):
    dif_splits = input("Enter different splits (y/n): ")
    if dif_splits == "y":
        split_files = {}
        train_files = input("Enter train split files as a list: ")
        test_files = input("Enter test split files as a list: ")
        dev_files = input("Enter dev split files as a list: ")

        if train_files != "":
            split_files["TRAIN"] = train_files
        if test_files != "":
            split_files["TEST"] = test_files
        if dev_files != "":
            split_files["VALIDATION"] = dev_files
    return split_files


def extract_all(dir):
    zip_files = glob(f"{dir}/**/**.zip", recursive=True)
    for file in zip_files:
        with zipfile.ZipFile(file) as item:
            item.extractall("/".join(file.split("/")[:-1]))


def get_valid_files(zip_base_dir):
    files = []
    for ext in valid_file_ext:
        files += glob(f"{zip_base_dir}/**/**.{ext}", recursive=True)
    return files
