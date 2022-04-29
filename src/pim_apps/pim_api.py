from traceback import print_exc
import json
import os
from datetime import datetime
import time
from time import time as time_time,sleep
import pandas as pd
import math
import requests
import random
from .utils import get_pepperx_domain, get_pim_domain, get_pim_app_domain, get_a2c_domain, write_csv_file
from .pepperx_db import ProductStatus, App, AppUser, AppUserPIM
from urllib.request import urlretrieve
import boto3
import random
from botocore.exceptions import ClientError

class PIMChannelAPI(object):
    def __init__(
            self, api_key, reference_id=None, properties=[], group_by_parent=None, parent_id=None, q=None,
            cache_count=20, slice_id = None, max_slice = None
    ):
        self.api_key = api_key
        self.properties = properties
        # self.group_by_parent = self.group_by_parent if group_by_parent is None else group_by_parent
        self.group_by_parent = group_by_parent
        self.q = q
        self.cache_count = cache_count
        self.cache = []
        self.reference_id = reference_id
        self.parent_id = parent_id
        self.slice_id = slice_id
        self.max_slice = max_slice
        self.scroll_id = None

    def count(self):
        response = self.get(count=0)
        if "data" not in response or "total" not in response["data"]:
            raise ValueError("Invalid response returned by PIM")
        return response["data"]["total"]

    def __iter__(self):
        self.n = 0
        self.max = -1
        return self

    def __next__(self):
        if self.max == -1 or self.n < self.max:
            index = self.n % self.cache_count
            if index == 0:
                response = self.get(count=self.cache_count, type="SCROLL", scroll_id=self.scroll_id)
                if "data" not in response or "products" not in response["data"] or "scrollId" not in response["data"]:
                    raise ValueError("Invalid response returned by PIM")
                products = response["data"]["products"]
                self.scroll_id = response["data"]["scrollId"]
                self.max = response["data"]["total"]
                self.cache = products
            self.n += 1
            if len(self.cache) > index:
                return self.cache[index]
        else:
            raise StopIteration

    def is_retryable(self, count, page, scroll_id, retry_count, message):
        retry_count = retry_count - 1
        if retry_count == 0:
            raise ValueError(message)
        else:
            sleep(60)
            self.get(count=count, page=page, scroll_id=scroll_id, retry_count=retry_count)

    def get(self, count=20, page=1, type="PAGINATION", scroll_id=None, retry_count=3):
        url = "{}v1/products".format(get_pim_app_domain())
        # url = get_pim_domain() + "/pim/v1/products"
        headers = {
            'Content-Type': "application/json",
            'Authorization': self.api_key,
        }
        req = {
            "count": count,
            "groupByParent": self.group_by_parent,
            # "q": self.q
        }
        if type == "SCROLL":
            req["type"] = "SCROLL"
            if scroll_id:
                req["scrollId"] = scroll_id
        elif type == "PAGINATION":
            req["page"] = page

        if self.properties is not None and len(self.properties) > 0:
            req["properties"] = self.properties
        if self.reference_id is not None:
            req["referenceId"] = self.reference_id
        if self.parent_id is not None:
            req["parentId"] = self.parent_id
        if self.max_slice is not None and self.slice_id is not None:
            req["sliceID"] = self.slice_id
            req["maxSliceCount"] = self.max_slice
            req["type"] = "SCROLL"

        time_before_pull_product = (int(round(time_time() * 1000)))
        try:
            response = requests.post(url, headers=headers, json=req)
        except ConnectionError as e:
            msg = "Pim Product Pull failed because of " + str(e) + "==> Request Object >> " + str(req) \
                  + " for org "
            print(msg)
            self.is_retryable(count, page, scroll_id, retry_count, msg)
        time_taken_to_pull_product = (int(round(time_time() * 1000))) - time_before_pull_product

        if response.status_code == 500:
            msg = "Pim Product Pull failed with 500 status " + response.text + "==> Request Object >> " + str(req) \
                  + " for org "
            print(msg)
            print("!!!!!! pim Products pull faied after " + str(time_taken_to_pull_product) + "  : {}".
                  format(str(json.dumps(req))))
            print("!!!!!! pim Products pull faied with error   : {}".
                  format(str(json.dumps(response))))
            self.is_retryable(count, page, scroll_id, retry_count, msg)
        elif response.status_code != 200:
            # print(msg)
            print("!!!!!! pim Products pull faied after " + str(time_taken_to_pull_product) + "  : {}".
                  format(str(json.dumps(req))))
            print("!!!!!! pim Products pull faied with error   : {}".
                  format(str(json.dumps(response))))
            raise ValueError(
                "Pim Product Pull failed due non " + str(response.status_code)  + " status " + response.text
                + "==> Request Object >> " + str(req))
        print("@@@@ Finished fetching total of pim Products pull it took " + str(time_taken_to_pull_product) + "  : {}".
              format(str(json.dumps(req))))
        if "data" not in response.json() or "products" not in response.json()["data"]:
            raise ValueError("Pim Product Pull failed due to data expectation")

        if response.status_code != 200:
            raise ValueError(
                "Non 200 status thrown by PIM get product " + response.text + "==> Request Object >> " + str(req))
        return response.json()


    def import_to_pim(self, file_url):

        url = f"{get_pim_app_domain()}v1/imports"

        payload = json.dumps({
            "url": file_url, #import_csv_url #import_json_url
            "referenceId": self.reference_id
        })
        headers = {
            'Authorization': self.api_key,
            'Content-Type': 'application/json'
        }
        print(f"Requesting URL ---  {url} " )
        print(f"{json.dumps(payload)} --- {json.dumps(headers)}")

        response = requests.request("POST", url, headers=headers, data=payload)

        print(response.text)



    #@title Enter CSV file name to be generated for the API response and run the cells
    def generate_csv(self, data, file_name="API_data_fetch", zipped=False):
        named_tuple = time.localtime() # get struct_time
        time_string = time.strftime("-%m-%d-2021-%H-%M", named_tuple)
        df = pd.DataFrame(data)
        file_name = f'{file_name}{time_string}.csv'

        if zipped:
            compression_opts = dict(method='zip',
                                    archive_name=f'{file_name}')
            final_local_url = f'{file_name.split(".")[0]}.zip'
            df.to_csv(final_local_url, index=False,
                      compression=compression_opts)
        else:
            final_local_url = file_name
            df.to_csv(final_local_url)
        return final_local_url

    def upload_to_s3(self, filename):
      """Upload a file to an S3 bucket
      :param file_name: File to upload
      :param bucket: Bucket to upload to
      :param object_name: S3 object name. If not specified then file_name is used
      :return: True if file was uploaded, else False
      """
      bucket = "unbxd-pim-ui"
      region = os.environ['aws_region']
      aws_access_key_id=os.environ['aws_access_key_id']
      aws_secret_access_key=os.environ['aws_secret_access_key']
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

    def upload_csv(self, req_data, input_file_name):
        file_name = self.generate_csv(req_data, input_file_name, True)
        # csv_url = file_name
        csv_url = self.upload_to_s3(file_name)
        return csv_url




class ProductProcessor(object):

    def __init__(self, api_key, reference_id, task_id):
        self.api_key = api_key
        self.task_id = task_id
        self.reference_id = reference_id
        app_user_instance = AppUserPIM(self.api_key)
        self.pim_channel_api = PIMChannelAPI(self.api_key, self.reference_id, group_by_parent=True)



    def insert_product_status(self, pid="", status="SUCCESS", status_desc=""):
        product_status_instance = ProductStatus(self.task_id)

        # if "SUCCESS" not in status:
        #     status_msg = status_desc
        # else:
        #     status_msg = status

        try:
            if status == "SUCCESS":
                product_status_instance.post_success_message(product_id=pid, msg=status_desc)
            elif status == "STARTED":
                product_status_instance.post_started_message(pid)
            else:
                product_status_instance.post_error_message(product_id=pid, msg=status_desc)
        except Exception as e:
            print_exc()
            print(e)


    # 1. Pulls products and variants from PIM
    def iterate_products(self, process_product):
        self.processed_list = []
        try:
            counter = 1
            # pim_channel_api = PIMChannelAPI(self.api_key, self.reference_id, group_by_parent=True)
            total_products = self.pim_channel_api.get()['data'].get('total', 0)
            print(f"Received {total_products} products for the job processing")
        except Exception as e:
            print(e)
            print_exc()
            return

        self.product_counter = 0
        for product in self.pim_channel_api:
            self.product_counter += 1
            try:
                if product is not None:
                    pid = product.get("id") or random.randint(100,9999)
                    # self.insert_product_status(pid,"STARTED" , f"Product processing started for {pid}")
                    proccessed_product, status = process_product(product, self.product_counter)
                    self.processed_list.append(proccessed_product)
#                     self.insert_product_status(pid,status , "Product processing completed")


            except Exception as e:
                print_exc()
                raise e

    def get_processed_products(self):
        return self.processed_list

    def send_to_pim(self, auto_export = False, file_url="", products_list=[], file_name="App_Results_"):
        if file_url :
            print("use file url and send to pim")
            self.pim_channel_api.import_to_pim(file_url)
            print(file_url)
        elif products_list and isinstance(products_list, list) and len(products_list) >0:
            print("convert list of dict to JSON or CSV and")
            file_url = self.pim_channel_api.upload_csv(products_list, file_name)
            self.pim_channel_api.import_to_pim(file_url)
            print(file_url)
        elif auto_export == True:
            file_url = self.pim_channel_api.upload_csv(self.processed_list, "sample_app_response_")
            self.pim_channel_api.import_to_pim(file_url)
            print(file_url)

    def upload_to_s3(self, file_path):
        print("Uploading file to s3")

        uploaded_url = self.pim_channel_api.upload_to_s3(file_path)
        return uploaded_url

    def write_products_template(self,fixed_header, properties_schema=[], header=False ):
        counter = 1
        # transformer = Transformer(product_schema)
        tsv_products = list()
        for product in self.pim_channel_api:
            # product = transformer.transform(product)
            tsv_product = list()
            for schema_key in properties_schema:
                data = product.get(schema_key, '')

                tsv_product.append(data)
            print(tsv_product)
            tsv_products.append(tsv_product)
            counter = counter + 1
            # TODO Manage the product level cleanup and final expected custom channel format

        if header:
            tsv_products.insert(0, properties_schema)
        if fixed_header:
            header_row_counter = 0
            for row in fixed_header:
                tsv_products.insert(header_row_counter, row)
                header_row_counter += 1
        print(tsv_products)
        data = []

        template_outout = write_csv_file(tsv_products)
        # template_op_url = self.upload_to_s3(template_outout)
        return template_outout
