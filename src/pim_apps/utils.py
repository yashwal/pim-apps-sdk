import csv
import os
import json
import tempfile
import zipfile
import requests
import pandas as pd
import boto3
from traceback import print_exc
from botocore.exceptions import ClientError

os.environ['A2C_BASE_URL'] = "https://api.api2cart.com/"

os.environ['PIM_APP_BASE_URL'] = os.environ.get('PIM_APP_BASE_URL')  or "https://pim-apps.unbxd.io/pim/"
os.environ['PIM_BASE_URL'] = os.environ.get('PIM_BASE_URL') or  "https://pim.unbxd.io/"
os.environ['PEPPERX_URL'] = os.environ.get('PEPPERX_URL') or "https://pim.unbxd.io/pepperx/"

os.environ['QA_PIM_APP_BASE_URL'] = "https://pimqa-apps.unbxd.io/pim/"
os.environ['QA_PIM_BASE_URL'] = "https://pimqa.unbxd.io/"
os.environ['QA_PEPPERX_URL'] = "https://pimqa.unbxd.io/pepperx/"

os.environ['PIMDEV_APP_BASE_URL'] = "http://pimdev-apps.unbxd.io/pim/"
os.environ['PIMDEV_BASE_URL'] = "http://pimdev.unbxd.io/"
os.environ['PIMDEV_PEPPERX_URL'] = "http://pimdev.unbxd.io/pepperx/"


os.environ['PXM_APP_BASE_URL'] = "https://pxm-apps.unbxd.io/pim/"
os.environ['PXM_BASE_URL'] = "https://pxm.unbxd.io/"
os.environ['PXM_PEPPERX_URL'] = "https://pxm.unbxd.io/pepperx/"

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
    elif env == "PXM":
      url = os.environ['PXM_APP_BASE_URL']

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
    elif env == "PXM":
        url = os.environ['PXM_BASE_URL']

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
    elif env == "PXM":
        url = os.environ['PXM_PEPPERX_URL']

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
    with open(filename, 'w', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=delimiter)
        csvwriter.writerows(data)
    return filename

  
# @title Enter CSV file name to be generated for the API response and run the cells
def generate_csv(data, file_name="API_data_fetch", zipped=False, index=False):
    named_tuple = time.localtime()  # get struct_time
    time_string = time.strftime("-%m-%d-%y-%H-%M", named_tuple)
    df = pd.DataFrame(data)
    file_name = f'{file_name}{time_string}.csv'

    if zipped:
        compression_opts = dict(method='zip',
                                archive_name=f'{file_name}')
        final_local_url = f'{file_name.split(".")[0]}.zip'
        df.to_csv(final_local_url, index=index,
                  compression=compression_opts)
    else:
        final_local_url = file_name
        df.to_csv(final_local_url, index=index)
    return final_local_url

def upload_to_s3(filename, bucket = "unbxd-pim-ui"):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    #bucket = "unbxd-pim-ui"
    if 'aws_region' in os.environ:
        region = os.environ.get('aws_region')
    else:
        region = os.environ.get(f"{str('aws_region').upper()}")

    if 'aws_access_key_id' in os.environ:
        aws_access_key_id = os.environ.get('aws_access_key_id')
    else:
        aws_access_key_id = os.environ.get(f"{str('aws_access_key_id').upper()}")

    if 'aws_secret_access_key' in os.environ:
        aws_secret_access_key = os.environ.get('aws_secret_access_key')
    else:
        aws_secret_access_key = os.environ.get(f"{str('aws_secret_access_key').upper()}")

    key = "app-uploads/" + filename
    object_name = filename
    s3 = boto3.resource(
        service_name='s3',
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    try:
        s3.Bucket(bucket).upload_file(Filename=filename, Key=key)
        url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
    except ClientError as e:
        # logging.error(e)
        print_exc()
        print(e)
        return False

    print(url)
    return url

def upload_csv(req_data, input_file_name):
    file_name = generate_csv(req_data, input_file_name, True)
    # csv_url = file_name
    csv_url = upload_to_s3(file_name)
    return csv_url

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

def remove_duplicates_from_list(list_of_dicts):
    unique_ids = set(item['pimUniqueId'] for item in list_of_dicts)

    # Use a list comprehension to create a new list with only the unique items
    new_list_of_dicts = [item for item in list_of_dicts if item['pimUniqueId'] in unique_ids]
    return new_list_of_dicts

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


def slack_notifier(channel="#infinity-template-jobs", title="Pepper-X App Alert",
                   header="New Pepper-X App User installed", parameters={}):
    url = "https://hooks.slack.com/services/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"

    payload = {
        "channel": channel,
        "username": title,
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": header
                }
            },
            {
                "type": "section",
                "fields": [
                ]
            }
        ]
    }

    for key in parameters:
        print(f"*{key} -- :*n{parameters.get(key, '-')} ")
        payload["blocks"][1]["fields"].append({
            "type": "mrkdwn",
            "text": f"*{key}:*\n {parameters.get(key, '-')} "
        })

    payload = json.dumps(payload)
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)


def import_hook(api_key, file_url, reference_id=None, template_id=None):
    url = f"{get_pim_app_domain()}v1/imports"

    reference_id = reference_id
    template_id = template_id

    payload = json.dumps({
        "url": file_url,  # import_csv_url #import_json_url
        "referenceId": reference_id if reference_id and reference_id != "-" and reference_id != "" else None,
        "templateId": template_id if template_id and template_id != "-" and template_id != "" else None
    })
    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }
    print(f"Requesting URL ---  {url} ")
    print(f"{json.dumps(payload)} --- {json.dumps(headers)}")

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)

    return json.loads(response.text)

def add_prefix_to_headers(file_path, prefix):
    file_url = ""
    try:
        file_extension = file_path.split('.')[-1]
        modified_file_path = f"prefixed_{file_path.split('/')[-1]}"

        if file_extension in ['csv', 'tsv']:
            delimiter = ',' if file_extension == 'csv' else '\t'
            df = pd.read_csv(file_path, delimiter=delimiter)
            df.columns = [prefix + col for col in df.columns]
            df.to_csv(modified_file_path, index=False, sep=delimiter)

        elif file_extension in ['xlsx', 'xlsm', 'xls']:
            df = pd.read_excel(file_path)
            df.columns = [prefix + col for col in df.columns]
            df.to_excel(modified_file_path, index=False)

        elif file_extension == 'json':
            with open(file_path, 'r') as f:
                data = json.load(f)
            if isinstance(data, dict):
                modified_data = {prefix + key: value for key, value in data.items()}
            elif isinstance(data, list):
                modified_data = [
                    {prefix + key: value for key, value in item.items()} if isinstance(item, dict) else item for item in
                    data]
            else:
                raise ValueError("Unsupported JSON structure")
            with open(modified_file_path, 'w') as f:
                json.dump(modified_data, f, indent=4)

        else:
            raise ValueError("Unsupported file format")

        file_url = upload_to_s3(modified_file_path)
        os.remove(modified_file_path)
    except Exception as e:
        print(e)
        print_exc()
    return file_url
