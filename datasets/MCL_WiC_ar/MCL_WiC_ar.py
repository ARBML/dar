import os
import pandas as pd 
import datasets

class MCL_WiC_ar(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'id':datasets.Value('string'),'lemma':datasets.Value('string'),'pos':datasets.Value('string'),'sentence1':datasets.Value('string'),'sentence2':datasets.Value('string'),'start1':datasets.Value('string'),'end1':datasets.Value('string'),'start2':datasets.Value('string'),'end2':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/SapienzaNLP/mcl-wic/master/SemEval-2021_MCL-WiC_all-datasets.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['MCL-WiC/test/multilingual/test.ar-ar.data']]}),datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['MCL-WiC/dev/multilingual/dev.ar-ar.data']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = None
		for i,filepath in enumerate(filepaths):
			df = pd.read_json(open(filepath, 'rb'), lines=False)
			df.columns = ['id', 'lemma', 'pos', 'sentence1', 'sentence2', 'start1', 'end1', 'start2', 'end2']
			for _, record in df.iterrows():
				yield str(_id), {'id':record['id'],'lemma':record['lemma'],'pos':record['pos'],'sentence1':record['sentence1'],'sentence2':record['sentence2'],'start1':record['start1'],'end1':record['end1'],'start2':record['start2'],'end2':record['end2']}
				_id += 1 

