import os
import pandas as pd 
import datasets

class iSarcasmEval_task_C(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'text_0':datasets.Value('string'),'text_1':datasets.Value('string'),'dialect':datasets.Value('string'),'sarcastic_id':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/iabufarha/iSarcasmEval/main/test/task_C_Ar_test.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [downloaded_files[0]]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			df.columns = ['text_0', 'text_1', 'dialect', 'sarcastic_id']
			for _, record in df.iterrows():
				yield str(_id), {'text_0':record['text_0'],'text_1':record['text_1'],'dialect':record['dialect'],'sarcastic_id':record['sarcastic_id']}
				_id += 1 

