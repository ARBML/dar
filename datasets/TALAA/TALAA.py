import os
import pandas as pd 
import datasets

class TALAA(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Article':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['algeria', 'algeria2', 'entertainment2', 'entertainment', 'religion', 'religion2', 'society', 'society2', 'sport', 'sport2', 'world', 'world2'])}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/saidziani/Arabic-News-Article-Classification/master/Articles/algeria.txt', 'https://raw.githubusercontent.com/saidziani/Arabic-News-Article-Classification/master/Articles/algeria2.txt', 'https://raw.githubusercontent.com/saidziani/Arabic-News-Article-Classification/master/Articles/entertaiment2.txt', 'https://raw.githubusercontent.com/saidziani/Arabic-News-Article-Classification/master/Articles/entertainment.txt', 'https://raw.githubusercontent.com/saidziani/Arabic-News-Article-Classification/master/Articles/religion.txt', 'https://raw.githubusercontent.com/saidziani/Arabic-News-Article-Classification/master/Articles/religion2.txt', 'https://raw.githubusercontent.com/saidziani/Arabic-News-Article-Classification/master/Articles/society.txt', 'https://raw.githubusercontent.com/saidziani/Arabic-News-Article-Classification/master/Articles/society2.txt', 'https://raw.githubusercontent.com/saidziani/Arabic-News-Article-Classification/master/Articles/sport.txt', 'https://raw.githubusercontent.com/saidziani/Arabic-News-Article-Classification/master/Articles/sport2.txt', 'https://raw.githubusercontent.com/saidziani/Arabic-News-Article-Classification/master/Articles/world.txt', 'https://raw.githubusercontent.com/saidziani/Arabic-News-Article-Classification/master/Articles/world2.txt']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = ['algeria', 'algeria2', 'entertainment2', 'entertainment', 'religion', 'religion2', 'society', 'society2', 'sport', 'sport2', 'world', 'world2']
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = None)
			df.columns = ['Article']
			for _, record in df.iterrows():
				yield str(_id), {'Article':record['Article'],'label':str(labels[i])}
				_id += 1 

