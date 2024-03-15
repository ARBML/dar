# dar

<p align="center">
    <img src = "logo.png" width = "250"/>
</p>

_dar_ or _دار_ which means house in Arabic is a simple semi-supervised approach for creating huggingface data script loaders. Here is an example of creating a loading script for a simple dataset:

[demo.mp4](https://github.com/zaidalyafeai/dar/assets/15667714/e6980af1-5f2e-4843-b16c-869b103d34c5)

## Scripts

The main interface can be ran using the following commands 

```bash
streamlit run app.py
```

## Supported Links Types 
Mainly after entering the dataset name, the user will be prompted to enter the dataset link. The user can either enter one link or mutliple links separated by comma. The supported links are the following 

* `GitHub` The user can enter a link from GitHub without using `raw` as it will be converted automatically. The user can also either choose to enter a link to the repository in the following format `https://github.com/user/repo` which will download and extract the full directory or `https://github.com/user/repo/foulder` which will download all the files from that foulders as individual links. 
* `Google Drive` The user can also enter a link from google drive in the following form `https://drive.google.com/file/d/id/view` which will directly download and extract the foulder to the local disk. Google sheets could also be used, the can be provided in the same format `https://docs.google.com/spreedshots/d/id/view`. You can test with the following gooel sheet [example](https://docs.google.com/spreadsheets/d/12laGoTSuLmmqSQmnn4PEpTlo6Sl4zn1SgXGasr142no/view).
* `Direct links` The user can enter direct links for files i.e `https://domain/**.ext` with any extension and the file will be downloaded. Multilple links can be concatenated using comma `https://domain/file1.ext,https://domain/file2.ext,...,https://domain/filen.ext`

## Filteration
The user can use glob structures to filter out some files from being used in the dataset. For example, the user is prompted with `Enter an input structure` the user can enter something like `'foulder/**.txt` which will include text files with the extension `.txt` from the `foulder`. The user will be prompted to enter multiple glob structures unless an empty `Enter` key is pressed. Multiple glob strucutres are used for datasets that have inputs and multiple targets like machine translation, summarization, speech transcription, etc.

## Supported file types
The user will enter the file type when asked for `File Type`, the supported file formats can be one of the following 

* `txt` mainly for reading the file as a whole or separated by lines. To differentiate between such options the user enters `Set Lines` which if `y` the file will be separated into multiple lines or if `n` it will be read as a whole. 
* `csv` this is used for files with sepcial separator, for example `.tsv` and `.txt` can be part of such family if a special separator is used. The program will try to guess the best separator but the user can also choose the separator using the command `CSV Separator`. The user can enter the separtors as `tab`,`,`,`;`,`|`, etc.
* `json` can be used for dictionary like files. The use can choose `Set Lines` as well which will decide whether to split the file by new lines or read the file as a whole. Also some datasets can have a parent dictionary for example `{'data':{'col1': [...], 'col2': [...]}}`, to support that the user can `Json Key` whcih is `data` in this example. 
* `xml` can be used for files that contain tags, for example `html` files. The user will be prompted to enter the column names for example `<s>this is good</s><l>positive</l> .....` then upon getting the prompt `XML Columns` the user can choose `s,l` such tags as columns. 
* `xlsx` used for `excel` file formats. 
* `wav` this is used for audio files like `mp3,wav` files. Upon choosing that the program will automatically create the following features as columns `{'audio':np.array(...)}`
* `jpg` this is used for image files like `jpg,png` files. Upon choosing that the program will automatically create the following features as columns `{'image':np.array(...)}`

## File Processing 
All the files will be processed using `pandas`. The user can modify some contents when prompted to 

* `Skipped rows` this is used to skip some lines from the beginning of all the files. Mainly used to remove some metadata that is usually put as the header of files. The user can enter `0` which indicates that no lines will be skipped. 
* `Headers` used to deal with files that have no column names, the user can set that `False` and enter the column names in the next step. 
* `New Column Names` used to creat different names for the columns or add columns if non exist. 
* `Label Column Name` used to choose the column that contains the labels. For example in sentiment anlaysis we will have the contents in a column as `positive` or `negative`. The user can put the name of that column to recognize that as the label. `datasets` will convert that to an integer which can be easily procssed by nlp model pipleines. 
* `push to hub:` used to upload the dataset to hub. The file will uploaded to the following directory `hf/DATASET_NAME` where `hf` can be specified using the argument `--hf`

