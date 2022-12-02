from .constants import *
import os


def get_split_code(urls, files, zip_base_dir, alt_globs=[], local_dir=False):
    MAIN_SPLITS = {
        "train": "TRAIN",
        "test": "TEST",
        "valid": "VALIDATION",
        "dev": "VALIDATION",
        "val": "VALIDATION",
    }
    func_name = "def _split_generators(self, dl_manager):\n"
    if local_dir:
        body = TABS_2 + f"url = [os.path.abspath('{urls[0]}')]\n"
    else:
        body = TABS_2 + f"url = {urls}\n"
    if len(zip_base_dir) > 0:
        body += TABS_2 + f"downloaded_files = dl_manager.download_and_extract(url)\n"
        body += TABS_2 + "self.extract_all(downloaded_files[0])\n"
    else:
        body += TABS_2 + f"downloaded_files = dl_manager.download(url)\n"

    result = []
    if len(zip_base_dir) > 0:
        split_files = {}
        for i, glob_name in enumerate(files):
            for f in files[glob_name]:
                f = f.replace(zip_base_dir, "")[1:]
                for split in MAIN_SPLITS:
                    if split in f.lower():
                        if MAIN_SPLITS[split] not in split_files:
                            split_files[MAIN_SPLITS[
                                split]] = f"'{glob_name}':[os.path.join(downloaded_files[0],'{f}'),]"
                        elif glob_name in split_files[MAIN_SPLITS[split]]:
                            split_files[MAIN_SPLITS[split]] = (
                                split_files[MAIN_SPLITS[split]][:-1] +
                                f"os.path.join(downloaded_files[0],'{f}'),]")
                        else:
                            split_files[MAIN_SPLITS[
                                split]] += f",'{glob_name}':[os.path.join(downloaded_files[0],'{f}'),]"

        if len(split_files) == 0:
            if len(alt_globs) > 0:
                split_gen = "datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{"

                for alt_glob in alt_globs:
                    glob_name = list(alt_glob.keys())[0]
                    glob_struct = alt_glob[glob_name]
                    glob_struct = glob_struct.replace(
                        "glob('", "glob(downloaded_files[0]+'/")
                    split_gen += f"'{glob_name}':sorted({glob_struct}),"
                split_gen += "} })"
                result.append(split_gen)
            else:
                body += TABS_2 + f"files = self.get_all_files(downloaded_files[0])\n"
                result.append(
                    "datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':files} })"
                )

        else:
            for split in split_files:
                result.append(
                    f"datasets.SplitGenerator(name=datasets.Split.{split}" +
                    ", gen_kwargs={'filepaths':{" + f"{split_files[split]}" +
                    "} })")
    else:
        split_files = {}
        for i, url in enumerate(urls):
            for split in MAIN_SPLITS:
                if split in url.split("/")[-1].lower():
                    if MAIN_SPLITS[split] in split_files:
                        split_files[
                            MAIN_SPLITS[split]] += f",downloaded_files[{i}]"
                    else:
                        split_files[
                            MAIN_SPLITS[split]] = f"downloaded_files[{i}]"

        if len(split_files) == 0:
            result.append(
                "datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })"
            )
        else:
            for split in split_files:
                result.append(
                    f"datasets.SplitGenerator(name=datasets.Split.{split}" +
                    ", gen_kwargs={'filepaths': {'inputs':" +
                    f"[{split_files[split]}]" + "} })")

    result = TABS_2 + "return [" + ",".join(result) + "]"
    return f"{extract_all_code}\n{get_all_files_code}\n\t{func_name}{body}{result}\n\n"
