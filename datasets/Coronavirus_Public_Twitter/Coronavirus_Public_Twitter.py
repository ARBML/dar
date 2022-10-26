import os
import pandas as pd 
import datasets

class jkjk(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Tweet':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://github.com/aseelad/Coronavirus-Public-Arabic-Twitter-Data-Set/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-03.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-24.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-21.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-31.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-15.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-17.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-16.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-14.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-30.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-12.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-09.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-27.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-28.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-23.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-07.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-04.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-26.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-20.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-29.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-08.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-13.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-11.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-06.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-25.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-19.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-01.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-10.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-18.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-05.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-02.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/12-2019/coronavirus-tweet-id-2019-12-22.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-03.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-27.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-10.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-29.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-22.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-17.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-13.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-06.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-07.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-12.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-01.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-08.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-04.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-20.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-30.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-21.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-14.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-28.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-26.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-19.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-15.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-02.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-09.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-05.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-11.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-31.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-18.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-23.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-25.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-16.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/01-2020/coronavirus-tweet-id-2020-01-24.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-06.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-08.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-20.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-29.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-03.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-17.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-02.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-15.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-14.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-21.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-09.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-10.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-04.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-11.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-01.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-19.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-25.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-05.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-28.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-24.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-13.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-18.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-26.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-31.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-07.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-30.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-22.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-16.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-12.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-27.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/03-2020/coronavirus-tweet-id-2020-03-23.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-12.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-29.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-28.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-09.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-07.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-20.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-06.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-23.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-13.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-02.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-04.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-24.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-16.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-05.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-15.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-22.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-01.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-25.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-11.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-03.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-27.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-18.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-19.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-17.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-21.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-26.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-08.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-10.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/02-2020/coronavirus-tweet-id-2020-02-14.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/04-2020/coronavirus-tweet-id-2020-04-03.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/04-2020/coronavirus-tweet-id-2020-04-09.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/04-2020/coronavirus-tweet-id-2020-04-08.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/04-2020/coronavirus-tweet-id-2020-04-04.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/04-2020/coronavirus-tweet-id-2020-04-11.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/04-2020/coronavirus-tweet-id-2020-04-10.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/04-2020/coronavirus-tweet-id-2020-04-06.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/04-2020/coronavirus-tweet-id-2020-04-01.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/04-2020/coronavirus-tweet-id-2020-04-07.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/04-2020/coronavirus-tweet-id-2020-04-05.txt', 'Coronavirus-Public-Arabic-Twitter-Data-Set-master/04-2020/coronavirus-tweet-id-2020-04-02.txt']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = None)
			df.columns = ['Tweet']
			for _, record in df.iterrows():
				yield str(_id), {'Tweet':record['Tweet']}
				_id += 1 
