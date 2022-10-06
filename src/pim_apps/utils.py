import csv
import os
import tempfile
import zipfile
import requests
import pandas as pd

os.environ['A2C_BASE_URL'] = "https://api.api2cart.com/"

os.environ['PIM_APP_BASE_URL'] = "https://pim-apps.unbxd.io/pim/"
os.environ['PIM_BASE_URL'] = "https://pim.unbxd.io/"
os.environ['PEPPERX_URL'] = "https://pim.unbxd.io/pepperx/"

os.environ['QA_PIM_APP_BASE_URL'] = "http://pimqa-apps.unbxd.io/pim/"
os.environ['QA_PIM_BASE_URL'] = "http://pimqa.unbxd.io/"
os.environ['QA_PEPPERX_URL'] = "https://pimqa.unbxd.io/pepperx/"

os.environ['PIMDEV_APP_BASE_URL'] = "http://pimdev-apps.unbxd.io/pim/"
os.environ['PIMDEV_BASE_URL'] = "http://pimdev.unbxd.io/"
os.environ['PIMDEV_PEPPERX_URL'] = "http://pimdev.unbxd.io/pepperx/"

EXPORT_STATUS = {"STARTED": "STARTED", "CHECK_IN_PROGRESS": "CHECK_IN_PROGRESS",
                 "EXPORT_IN_PROGRESS": "EXPORT_IN_PROGRESS", "PRODUCTS_PROCESSED": "PRODUCTS_PROCESSED",
                 "PRODUCTS_FAILED": "PRODUCTS_FAILED", "EXPORTED": "EXPORTED", "FAILED": "FAILED",
                 "TIMED_OUT": "TIMED_OUT", "WRITING_TO_FILE": "WRITING_TO_FILE", "UPLOADED_FILE": "UPLOADED_FILE",
                 "FAILED_TO_UPLOAD_FILE": "FAILED_TO_UPLOAD_FILE"}


def get_pim_app_domain():
    env = os.environ['PEPPERX_ENV']

    if env == "PROD":
        url = os.environ['PIM_APP_BASE_URL']
    elif env == "QA":
        url = os.environ['QA_PIM_APP_BASE_URL']
    elif env == "PIMDEV":
        url = os.environ['PIMDEV_APP_BASE_URL']

    # TODO: When upgrading to python 3.10.X use the below code
    # match env:
    #     case "PROD":
    #         url = os.environ['PIM_APP_BASE_URL']
    #     case "QA":
    #         url = os.environ['QA_PIM_APP_BASE_URL']
    #     case "PIMDEV":
    #         url = os.environ['PIMDEV_APP_BASE_URL']

    # url = os.environ['PIM_APP_BASE_URL'] if env == "PROD" else os.environ['QA_PIM_APP_BASE_URL']

    print(f" {env} ---- {url} ")
    return url


def get_pim_domain():
    env = os.environ['PEPPERX_ENV']

    if env == "PROD":
        url = os.environ['PIM_BASE_URL']
    elif env == "QA":
        url = os.environ['QA_PIM_BASE_URL']
    elif env == "PIMDEV":
        url = os.environ['PIMDEV_BASE_URL']

    # TODO: When upgrading to python 3.10.X use the below code
    # match env:
    #     case "PROD":
    #         url = os.environ['PIM_APP_BASE_URL']
    #     case "QA":
    #         url = os.environ['QA_PIM_APP_BASE_URL']
    #     case "PIMDEV":
    #         url = os.environ['PIMDEV_BASE_URL']

    # url = os.environ['PIM_BASE_URL'] if env == "PROD" else os.environ['QA_PIM_BASE_URL']
    print(f" {env} ---- {url} ")
    return url


def get_a2c_domain():
    return os.environ['A2C_BASE_URL']


def get_pepperx_domain():
    env = os.environ['PEPPERX_ENV']

    if env == "PROD":
        url = os.environ['PEPPERX_URL']
    elif env == "QA":
        url = os.environ['QA_PEPPERX_URL']
    elif env == "PIMDEV":
        url = os.environ['PIMDEV_PEPPERX_URL']

    # TODO: When upgrading to python 3.10.X use the below code
    # match env:
    #     case "PROD":
    #         url = os.environ['PIM_APP_BASE_URL']
    #     case "QA":
    #         url = os.environ['QA_PIM_APP_BASE_URL']
    #     case "PIMDEV":
    #         url = os.environ['PIMDEV_PEPPERX_URL']

    # url = os.environ['PEPPERX_URL'] if env == "PROD" else os.environ['QA_PEPPERX_URL']
    print(f" {env} ---- {url} ")
    return url


def download_url(url, file_name=""):
    get_response = requests.get(url, stream=True)
    if file_name == "":
        file_name = url.split("/")[-1]
    with open(file_name, 'wb') as f:
        for chunk in get_response.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)

    return file_name


def write_csv_file(data, delimiter="\t", filename="IndexedExport.csv"):
    with open(filename, 'w') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=delimiter)
        csvwriter.writerows(data)
    return filename


def flatten(d, sep="_"):
    import collections

    obj = collections.OrderedDict()

    def recurse(t, parent_key=""):

        if isinstance(t, list):
            for i in range(len(t)):
                recurse(t[i], parent_key + sep + str(i) if parent_key else str(i))
        elif isinstance(t, dict):
            for k, v in t.items():
                recurse(v, parent_key + sep + k if parent_key else k)
        else:
            obj[parent_key] = t

    recurse(d)

    return obj


def unflatten(dictionary, sep="_"):
    result_dict = dict()
    for flatten_key, value in dictionary.items():
        unflatten_key_list = flatten_key.split(sep)
        tmp = result_dict
        for index, unflatten_key in enumerate(unflatten_key_list[:-1]):
            if unflatten_key in tmp:
                pass
            elif unflatten_key.isnumeric() and isinstance(tmp, list):
                unflatten_key = int(unflatten_key)
                if len(tmp) == unflatten_key:
                    tmp.append({})
            else:
                if index + 1 < len(unflatten_key_list) and \
                        unflatten_key_list[index + 1].isnumeric():
                    tmp[unflatten_key] = []
                    if len(unflatten_key_list) < index + 2:
                        if unflatten_key_list[index + 2].isnumeric():
                            tmp[unflatten_key].append(None)
                        else:
                            tmp[unflatten_key].append({})
                    else:
                        tmp[unflatten_key].append({})
                elif unflatten_key.isnumeric():
                    unflatten_key = int(unflatten_key)
                else:
                    tmp[unflatten_key] = {}
            tmp = tmp[unflatten_key]
        last_key = unflatten_key_list[-1]
        if unflatten_key_list[-1].isnumeric():
            last_key = int(unflatten_key_list[-1])
            if len(tmp) == last_key and isinstance(tmp, list):
                tmp.append(None)
        tmp[last_key] = value
    return result_dict


class FileParser(object):
    def load(self, url):
        self.url = url
        self.file_type = url.split(".")[-1]
        print("The URL file type is : ", self.file_type)
        method_name = 'parse_' + self.file_type
        method = getattr(self, method_name, lambda: 'Invalid')
        return method()

    def infer_schema(self):
        self.df.info()
        self.columns = list(self.df.columns.values.tolist())
        print("List of all columns are : ", self.columns)
        print("##### Pandas inferred Schema")
        pandas_schema = self.df.columns.to_series().groupby(self.df.dtypes).groups
        print(pandas_schema)

    def parse_xlsx(self):
        return self.parse_excel()

    def parse_xlsm(self):
        return self.parse_xlsm()

    def parse_xls(self):
        return self.parse_excel()

    def parse_csv(self):
        df = pd.read_csv(self.url, sep=",", header=0)
        return df

    def parse_zip(self):
        zip = zipfile.ZipFile('filename.zip')

        # available files in the container
        print(zip.namelist())
        zip.open(zip.namelist()[0])

    def parse_tsv(self):
        df = pd.read_csv(self.url, sep="\t", header=0)
        return df

    def parse_json(self):
        df = pd.read_json(self.url)
        return df

    #         https://www.dataquest.io/blog/python-json-tutorial/
    # def parse_xml(self):
    #     xml2df = XML2DataFrame(self.url)
    #     self.df = xml2df.process_data()

    def parse_txt(self):
        df = pd.read_csv(self.url, sep=" ")
        return df

    def parse_tsv(self):
        df = pd.read_csv(self.url, sep="\t", header=0)
        return df

    def parse_excel(self):
        xls = pd.ExcelFile(self.url)
        # Now you can list all sheets in the file
        # sheets = xls.sheet_names;
        # print("Sheets present in excel file are : ", sheets)
        # self.df = pd.read_excel(xls, sheets[0])
        return xls

    def parse_xlsm(self):
        print("Pasring Amazon File in xlsm format")
        xls = pd.ExcelFile(self.url)
        # Now you can list all sheets in the file
        # sheets = xls.sheet_names;
        # enum_value_rules = pd.read_excel(xls, sheet_name="Valid Values")
        # # valid_enum_values = pd.read_excel(xls, sheet_name="Valid Values", header=1)
        # properties_list = pd.read_excel(xls, sheet_name="Data Definitions", header=1)
        # properties_template = pd.read_excel(xls, sheet_name="Template", header=0)
        return xls


class Dict2Class(object):

    def __init__(self, my_dict):
        for key in my_dict:
            setattr(self, key, my_dict[key])
