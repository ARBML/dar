import os
import pandas as pd 
import datasets

class DAWQAS(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'QID':datasets.Value('string'),'Site_id':datasets.Value('string'),'Question':datasets.Value('string'),'Answer':datasets.Value('string'),'Answer1':datasets.Value('string'),'Answer2':datasets.Value('string'),'Answer3':datasets.Value('string'),'Answer4':datasets.Value('string'),'Answer5':datasets.Value('string'),'Answer6':datasets.Value('string'),'Answer7':datasets.Value('string'),'Answer8':datasets.Value('string'),'Answer9':datasets.Value('string'),'Answer10':datasets.Value('string'),'Answer11':datasets.Value('string'),'Original_Category':datasets.Value('string'),'Author':datasets.Value('string'),'Date':datasets.Value('string'),'Site':datasets.Value('string'),'Year':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/masun/DAWQAS/master/DAWQAS_Masun_Nabhan_Homsi.xlsx']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_excel(open(filepath, 'rb'), skiprows = 0, header = 0)
			df.columns = ['QID', 'Site_id', 'Question', 'Answer', 'Answer1', 'Answer2', 'Answer3', 'Answer4', 'Answer5', 'Answer6', 'Answer7', 'Answer8', 'Answer9', 'Answer10', 'Answer11', 'Original_Category', 'Author', 'Date', 'Site', 'Year']
			for _, record in df.iterrows():
				yield str(_id), {'QID':record['QID'],'Site_id':record['Site_id'],'Question':record['Question'],'Answer':record['Answer'],'Answer1':record['Answer1'],'Answer2':record['Answer2'],'Answer3':record['Answer3'],'Answer4':record['Answer4'],'Answer5':record['Answer5'],'Answer6':record['Answer6'],'Answer7':record['Answer7'],'Answer8':record['Answer8'],'Answer9':record['Answer9'],'Answer10':record['Answer10'],'Answer11':record['Answer11'],'Original_Category':record['Original_Category'],'Author':record['Author'],'Date':record['Date'],'Site':record['Site'],'Year':record['Year']}
				_id += 1 

