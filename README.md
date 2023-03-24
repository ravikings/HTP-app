# HTP app
 This app Process two datasets containing financial data, and performs various operations on them to generate a new output file.

## Installation Steps for Windows Machine.

1. Download or clone HTP app to your workstation from the Git repository. for steps [click](https://blog.hubspot.com/website/download-from-github)

Before proceeding, make sure you have Python installed and that it's available from your command line. You can check this by simply running:
```bash
python --version
```
You should get some output like "3.10.8". If you do not have Python, please install the latest 3.x version from python.org. Additionally, make sure you have pip available. You can check this by running:
```bash
$ pip --version
pip 22.3.1
```

2. Install Pipenv.

If you have a working installation of pip, proceed with the pipenv installation. To learn more about the package manager [pipenv](https://pipenv.pypa.io/en/latest/installation/), run
```bash
$ pip install pipenv
```

3. Activate the virtual environment.

For the ease of use, just activate the environment and you're set to go.
```bash
$ python -m pipenv install
Installing dependencies from Pipfile.lock
$ python -m pipenv shell
Launching subshell in virtual environment...
```

## How to use  HTP app
1. **One-time data processing job**

The files to be processed are required to be placed inside the root directory, where the dataset1.csv file can be found. Additionally, to run the program, make sure that you are in the same directory in the command line.
```bash
######## For help use ################
$ python main.py -h
output similar to below:
positional arguments:
  dataset1_path         Type path to your first dataset in csv format containing financial data.       
  dataset2_path         Type path to your second dataset in csv format containing tier data.

optional arguments:
  -h, --help            show this help message and exit
  --output_path OUTPUT_PATH
                        Type path to output file, or use default result.csv file.

########### process file from terminal #############
 python main.py dataset1.csv dataset2.csv --output_path "[name of file]"

```

2. ** To use Apache-beam framework for one-time data processing job**

The files to be processed are required to be placed inside the root directory, where the dataset1.csv file can be found. Additionally, to run the program, make sure that you are in the same directory in the command line.
```bash
######## For help use ################
$ python pipeline.py -h
output similar to below:
positional arguments:
  dataset1_path         Type path to your first dataset in csv format containing financial data.       
  dataset2_path         Type path to your second dataset in csv format containing tier data.

optional arguments:
  -h, --help            show this help message and exit
  --output_path OUTPUT_PATH
                        Type path to output file, or use default result.csv file.

########### process file from terminal #############
 python pipeline.py dataset1.csv dataset2.csv --output_path "[name of file]"

```
## TODO
- Enhance the app to support email notification, retries, and API endpoints.
- Enhance both batch and data streaming capabilities.
- Improve test coverage.
- Intergation testing.
- Deploy to a server using a CI/CD approach.

## License
[MIT](https://choosealicense.com/licenses/mit/)