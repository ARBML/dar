import os
import pandas as pd
import datasets
from glob import glob
import zipfile


class Dvoice(datasets.GeneratorBasedBuilder):

    def _info(self):
        return datasets.DatasetInfo(features=datasets.Features(
            {
                'path': datasets.Value('string'),
                'audio': datasets.Audio(sampling_rate=16_000),
                'text': datasets.Value('string')
            }))

    def extract_all(self, dir):
        zip_files = glob(dir + '/**/**.zip', recursive=True)
        for file in zip_files:
            with zipfile.ZipFile(file) as item:
                item.extractall('/'.join(file.split('/')[:-1]))

    def _split_generators(self, dl_manager):
        url = [
            'https://zenodo.org/record/5824476/files/dvoice-v1.0%28downsampled%29.tar.gz'
        ]
        downloaded_files = dl_manager.download_and_extract(url)
        self.extract_all(downloaded_files[0])
        return [
            datasets.SplitGenerator(name=datasets.Split.TEST,
                                    gen_kwargs={
                                        'filepath':
                                        os.path.join(downloaded_files[0], f)
                                        for f in ['dataset/texts/test.csv']
                                    }),
            datasets.SplitGenerator(name=datasets.Split.TRAIN,
                                    gen_kwargs={
                                        'filepath':
                                        os.path.join(downloaded_files[0], f)
                                        for f in ['dataset/texts/train.csv']
                                    }),
            datasets.SplitGenerator(name=datasets.Split.VALIDATION,
                                    gen_kwargs={
                                        'filepath':
                                        os.path.join(downloaded_files[0], f)
                                        for f in ['dataset/texts/dev.csv']
                                    })
        ]

    def _generate_examples(self, filepath):
        _id = 0
        base_path = '/'.join(filepath.split("/")[:-3])
        df = pd.read_csv(open(filepath, 'rb'),
                         sep=r'\t',
                         skiprows=0,
                         error_bad_lines=False,
                         header=0)
        for _, record in df.iterrows():
            yield str(_id), {
                'path': os.path.join(f'{base_path}/dataset/wavs',
                                     record['wav']),
                'audio': os.path.join(f'{base_path}/dataset/wavs',
                                      record['wav']),
                'text': record['words']
            }
            _id += 1
