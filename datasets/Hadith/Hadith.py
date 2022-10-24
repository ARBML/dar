import os
import pandas as pd 
import datasets

class Hadith(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Text':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://github.com/abdelrahmaan/Hadith-Data-Sets/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':[os.path.join(downloaded_files[0],f) for f in ['Hadith-Data-Sets-master/All Hadith Books/Sunan al-Nasai Without_Tashkel.csv', 'Hadith-Data-Sets-master/All Hadith Books/Sunan al Tirmidhi.csv', 'Hadith-Data-Sets-master/All Hadith Books/Sunan Abu Dawud.csv', 'Hadith-Data-Sets-master/All Hadith Books/Maliks Muwatta Without_Tashkel.csv', 'Hadith-Data-Sets-master/All Hadith Books/Sunan al Darami.csv', 'Hadith-Data-Sets-master/All Hadith Books/Musnad Ahmad ibn Hanbal Without_Tashkel.csv', 'Hadith-Data-Sets-master/All Hadith Books/Sunan Ibn Maja.csv', 'Hadith-Data-Sets-master/All Hadith Books/Maliks Muwatta.csv', 'Hadith-Data-Sets-master/All Hadith Books/Musnad Ahmad ibn Hanbal.csv', 'Hadith-Data-Sets-master/All Hadith Books/Sunan Ibn Maja Without_Tashkel.csv', 'Hadith-Data-Sets-master/All Hadith Books/Sahih Bukhari.csv', 'Hadith-Data-Sets-master/All Hadith Books/Sunan al Darami Without_Tashkel.csv', 'Hadith-Data-Sets-master/All Hadith Books/Sahih Bukhari Without_Tashkel.csv', 'Hadith-Data-Sets-master/All Hadith Books/Sahih Muslime Without_Tashkel.csv', 'Hadith-Data-Sets-master/All Hadith Books/Sahih Muslim.csv', 'Hadith-Data-Sets-master/All Hadith Books/Sunan Abu Dawud Without_Tashkel.csv', 'Hadith-Data-Sets-master/All Hadith Books/Sunan al Tirmidhi Without_Tashkel.csv', 'Hadith-Data-Sets-master/All Hadith Books/Sunan al-Nasai.csv']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 1, error_bad_lines = False, header = None)
			df.columns = ['Text']
			for _, record in df.iterrows():
				yield str(_id), {'Text':record['Text']}
				_id += 1 

