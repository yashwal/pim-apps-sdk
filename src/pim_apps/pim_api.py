import concurrent
from traceback import print_exc
import json
import os
from datetime import datetime
import time
from time import time as time_time, sleep
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
            cache_count=20, slice_id=None, max_slice=None
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
        self.products_total = 0
        self.is_products_split = self.is_products_post_split()

    def count(self):
        response = self.get(count=20)
        if "data" not in response or "total" not in response["data"]:
            raise ValueError("Invalid response returned by PIM")
        return response["data"]["total"]

    def is_products_post_split(self):
        try:
            response = self.get(count=20)
            if "data" not in response or "total" not in response["data"]:
                raise ValueError("Invalid response returned by PIM")

            self.products_total = response["data"]["total"]
            return True if response["data"]["total"] < response["data"]["count"] else False
        except Exception as e:
            # logging.error(e)
            print_exc()
            print(e)
            return False

    def __iter__(self):
        self.scroll_id = None
        self.n = 0
        self.max = -1
        self.iter_max = -1
        return self

    def __next__(self):
        if not self.is_products_split:
            if self.max == -1 or self.n < self.max:
                index = self.n % self.cache_count
                if index == 0:
                    response = self.get(count=self.cache_count, type="SCROLL", scroll_id=self.scroll_id)
                    if "data" not in response or "products" not in response["data"] or "scrollId" not in response[
                        "data"]:
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
        else:
            if self.max == -1 or (self.n < self.max or self.n < self.iter_max):
                index = self.n % self.iter_max
                if index == 0:
                    response = self.get(count=self.cache_count, type="SCROLL", scroll_id=self.scroll_id)
                    if "data" not in response or "products" not in response["data"] or "scrollId" not in response[
                        "data"]:
                        raise ValueError("Invalid response returned by PIM")
                    products = response["data"]["products"]
                    self.scroll_id = response["data"]["scrollId"]
                    self.max = response["data"]["total"]
                    self.iter_max = response["data"]["count"]
                    self.cache = products
                self.n += 1
                if len(self.cache) > index:
                    return self.cache[index]
            else:
                raise StopIteration

    def is_retryable(self, count, page, type, scroll_id, retry_count, message):
        retry_count = retry_count - 1
        if retry_count == 0:
            raise ValueError(message)
        else:
            sleep(15)
            self.get(count=count, page=page, type=type, scroll_id=scroll_id, retry_count=retry_count)

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
            response = requests.post(url, headers=headers, json=req, timeout=180)
        except ConnectionError as e:
            msg = "Pim Product Pull failed because of " + str(e) + "==> Request Object >> " + str(req) \
                  + " for org "
            print(msg)
            self.is_retryable(count, page, type, scroll_id, retry_count, msg)
        time_taken_to_pull_product = (int(round(time_time() * 1000))) - time_before_pull_product

        if response.status_code == 500:
            msg = "Pim Product Pull failed with 500 status " + response.text + "==> Request Object >> " + str(req) \
                  + " for org "
            print(msg)
            print("!!!!!! pim Products pull faied after " + str(time_taken_to_pull_product) + "  : {}".
                  format(str(json.dumps(req))))
            print("!!!!!! pim Products pull faied with error   : {}".
                  format(str(json.dumps(response.text))))
            self.is_retryable(count, page, type, scroll_id, retry_count, msg)
        elif response.status_code != 200:
            msg = f"Pim Product Pull failed with {response.status_code} status {response.text} ==> Request Object >>{str(req)} for org "
            # print(msg)
            print("!!!!!! pim Products pull faied after " + str(time_taken_to_pull_product) + "  : {}".
                  format(str(json.dumps(req))))
            print("!!!!!! pim Products pull faied with error   : {}".
                  format(str(json.dumps(response.text))))
            # raise ValueError(
            #     "Pim Product Pull failed due non " + str(response.status_code) + " status " + response.text
            #     + "==> Request Object >> " + str(req))
            self.is_retryable(count, page, type, scroll_id, retry_count, msg)
        print("@@@@ Finished fetching total of pim Products pull it took " + str(time_taken_to_pull_product) + "  : {}".
              format(str(json.dumps(req))))
        if "data" not in response.json() or "products" not in response.json()["data"]:
            raise ValueError("Pim Product Pull failed due to data expectation")

        if response.status_code != 200:
            raise ValueError(
                "Non 200 status thrown by PIM get product " + response.text + "==> Request Object >> " + str(req))
        return response.json()

    def import_to_pim(self, file_url, custom_reference_id=None):

        url = f"{get_pim_app_domain()}v1/imports"

        reference_id = custom_reference_id or self.reference_id

        payload = json.dumps({
            "url": file_url,  # import_csv_url #import_json_url
            "referenceId": reference_id if reference_id != "-" and reference_id != "" else None
        })
        headers = {
            'Authorization': self.api_key,
            'Content-Type': 'application/json'
        }
        print(f"Requesting URL ---  {url} ")
        print(f"{json.dumps(payload)} --- {json.dumps(headers)}")

        response = requests.request("POST", url, headers=headers, data=payload)

        print(response.text)

    def get_export_details(self):

        url = f"{get_pim_app_domain()}v1/appTriggerInfo?referenceId={self.reference_id}"

        payload = {}
        headers = {
            'Authorization': f'{self.api_key}'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        return json.loads(response.text)

    def update_export_status(self, data):

        url = f"{get_pim_app_domain()}api/v3/channelExports/{self.reference_id}"

        payload = json.dumps(data)
        headers = {
            'Authorization': f'{self.api_key}',
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)

        print(f" >>>>>>>>>>>> Export status updated for  {self.reference_id} ---  {response.text}")
        return json.loads(response.text)

    # @title Enter CSV file name to be generated for the API response and run the cells
    def generate_csv(self, data, file_name="API_data_fetch", zipped=False, index=False):
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

    def upload_to_s3(self, filename):
        """Upload a file to an S3 bucket
        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        bucket = "unbxd-pim-ui"
        region = os.environ['aws_region']
        aws_access_key_id = os.environ['aws_access_key_id']
        aws_secret_access_key = os.environ['aws_secret_access_key']
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
        self.app_user_instance = AppUserPIM(self.api_key)
        self.pim_channel_api = PIMChannelAPI(self.api_key, self.reference_id, group_by_parent=True)
        # self.raw_products_list = []
        self.product_status_instance = ProductStatus(self.task_id)

    def insert_product_status(self, pid="", status="SUCCESS", status_desc=""):

        # if "SUCCESS" not in status:
        #     status_msg = status_desc
        # else:
        #     status_msg = status

        try:
            if status == "SUCCESS":
                self.product_status_instance.post_success_message(product_id=pid, msg=status_desc)
            elif status == "STARTED":
                self.product_status_instance.post_started_message(pid)
            else:
                self.product_status_instance.post_error_message(product_id=pid, msg=status_desc)

        except Exception as e:
            print_exc()
            print(e)

    def get_sorted_products_list(self, include_variants=False):
        print("Sorted Product List")
        all_products = self.fetch_all_pim_products(include_variants)
        sorted_product = sorted(all_products, key=lambda d: d['pimUniqueId'])
        return sorted_product

    # 1. Pulls products and variants from PIM

    def process_pim_product(self, product, process_product):
        # print(f"Processing product no {counter}")
        try:
            if product is not None:
                pid = product.get("id") or random.randint(100, 9999)
                # self.insert_product_status(pid,"STARTED" , f"Product processing started for {pid}")
                proccessed_product, status = process_product(product, self.product_counter)
                self.product_counter += 1
                if status == "SUCCESS":
                    self.success_count += 1
                elif status == "FAILED":
                    self.failed_count += 1
                self.processed_list.append(proccessed_product)
                if self.product_counter % 5 == 0:
                    self.update_export_status(status="EXPORT_IN_PROGRESS", success_count=self.success_count,
                                              failed_count=self.failed_count)

        except Exception as e:
            print_exc()
            error_pid = pid or f"export_pid_{str(time.time())}"
            self.failed_count += 1
            self.insert_product_status(self, pid=error_pid, status="FAILED", status_desc=f"{str(e)}")

    def fetch_all_pim_products(self, include_variants=False):
        raw_products_list = []
        export_data = self.pim_channel_api.get_export_details()
        # export_details = export_data["data"]["metaInfo"]["export"]

        for product in self.pim_channel_api:
            raw_products_list.append(product)
            if include_variants and product["pimProductType"] == "PARENT":
                pim_variants_fetcher = PIMChannelAPI(self.api_key, self.reference_id, group_by_parent=False,
                                                     parent_id=product["pimUniqueId"])
                for v_product in pim_variants_fetcher:
                    raw_products_list.append(v_product)
        return raw_products_list

    def iterate_products(self, process_product, auto_finish=True, multiThread=True):
        self.processed_list = []
        self.product_counter = 0
        self.success_count = 0
        self.failed_count = 0
        ts = f"PIM_ERROR_{time.time()}"
        try:
            counter = 1
            status = True
            total_products = self.pim_channel_api.get()['data'].get('total', 0)
            if total_products > 0:
                raw_products_list = self.fetch_all_pim_products()
            else:
                status = False

            if status:
                # if total_products < 25000:
                print(f"Received {total_products} products for the job processing")

                if multiThread:
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        for product in raw_products_list:
                            executor.submit(self.process_pim_product, product, process_product)
                else:
                    for product in raw_products_list:
                        self.process_pim_product(product, process_product)
            else:
                self.update_export_status(status="PRODUCTS_FAILED", success_count=self.success_count,
                                          failed_count=self.failed_count)
            # for product in self.pim_channel_api:
            #     self.product_counter += 1
            #     self.process_pim_product(product, process_product)
            # try:
            #     if product is not None:
            #         pid = product.get("id") or random.randint(100, 9999)
            #         # self.insert_product_status(pid,"STARTED" , f"Product processing started for {pid}")
            #         proccessed_product, status = process_product(product, self.product_counter)
            #         if status == "SUCCESS":
            #             self.success_count += 1
            #         elif status == "FAILED":
            #             self.failed_count += 1
            #         self.processed_list.append(proccessed_product)
            #         if self.product_counter % 5 == 0:
            #             self.update_export_status(status="EXPORT_IN_PROGRESS", success_count=self.success_count, failed_count=self.failed_count)
            #
            # except Exception as e:
            #     print_exc()
            #     error_pid = pid or f"export_pid_{counter}"
            #     self.insert_product_status(self, pid=error_pid , status="FAILED", status_desc=f"{str(e)}")

            if auto_finish:
                self.update_export_status(status="EXPORTED", success_count=self.success_count,
                                          failed_count=self.failed_count)
            else:
                self.update_export_status(status="PRODUCTS_PROCESSED", success_count=self.success_count,
                                          failed_count=self.failed_count)
            # else:
            #     print("Perform multi threading")
            #     maxSliceCount = 5
            #     slices_map = {}
            #
            #     with concurrent.futures.ThreadPoolExecutor() as executor:
            #         for slice_no in range(0, 5):
            #             print(slice)
            #             slice_name = f"api_slice_{slice_no}"
            #             slices_map[slice_name] = PIMChannelAPI(self.api_key, self.reference_id, group_by_parent=True,
            #                                                  sliceID=slice_no, maxSliceCount=maxSliceCount)
            #             for product in slices_map[slice_name]:
            #                 self.product_counter += 1
            #                 executor.submit(self.process_pim_product, product, process_product)
            #


        except ValueError as e:
            print(e)
            print_exc()
            self.insert_product_status(self, pid=ts, status="FAILED", status_desc=f"{json.dumps(e)}")
            return
        except Exception as e:
            print(e)
            print_exc()
            self.insert_product_status(self, pid=ts, status="FAILED", status_desc=f"{json.dumps(e)}")
            return

    def get_processed_products(self):
        return self.processed_list

    def send_to_pim(self, auto_export=False, file_url="", products_list=[], file_name="App_Results_", custom_reference_id=None):
        if file_url:
            print("use file url and send to pim")
            self.pim_channel_api.import_to_pim(file_url, custom_reference_id)
            print(file_url)
        elif products_list and isinstance(products_list, list) and len(products_list) > 0:
            print("convert list of dict to JSON or CSV and")
            file_url = self.pim_channel_api.upload_csv(products_list, file_name)
            self.pim_channel_api.import_to_pim(file_url, custom_reference_id)
            print(file_url)
        elif auto_export == True:
            file_url = self.pim_channel_api.upload_csv(self.processed_list, "sample_app_response_")
            self.pim_channel_api.import_to_pim(file_url, custom_reference_id)
            print(file_url)

    def upload_to_s3(self, file_path):
        print("Uploading file to s3")

        uploaded_url = self.pim_channel_api.upload_to_s3(file_path)
        return uploaded_url

    def update_export_status(self, status="STARTED", success_file="", failed_file="", success_count=None,
                             failed_count=None):
        data = {
            "status": str(status).upper().strip()
        }
        if success_file:
            data["file_download_links"] = {
                "CSV": success_file
            }
        if failed_file:
            data["failed_file_download_links"] = {
                "CSV": failed_file
            }
        total = self.pim_channel_api.products_total or 0
        if (success_count and success_count > 0) or (failed_count and failed_count > 0):
            data["export_stats"] = {}
            if total and total > 0:
                data["export_stats"]["total"] = total
            if success_count and success_count > 0:
                data["export_stats"]["success"] = success_count
            if failed_count and failed_count > 0:
                data["export_stats"]["failed"] = failed_count
        self.pim_channel_api.update_export_status(data)

    def write_products_template(self, fixed_header, properties_schema=[], header=False, filename="Template_Export.csv",
                                add_parent_rows=False):
        counter = 1
        # transformer = Transformer(product_schema)
        tsv_products = list()
        template_outout = []
        success_count = 0
        failed_count = 0

        try:
            if add_parent_rows:
                self.pim_channel_api = PIMChannelAPI(self.api_key, self.reference_id, group_by_parent=True,
                                                     slice_id=None)
                for product in self.pim_channel_api:
                    # product = transformer.transform(product)
                    try:
                        tsv_product = list()
                        for schema_key in properties_schema:
                            data = product.get(schema_key, '')
                            if data:
                                data = ",".join(data) if isinstance(data, list) else data
                            else:
                                data = str(data)
                            tsv_product.append(data)
                        # print(tsv_product)
                        tsv_products.append(tsv_product)
                        pid = product.get("pimUniqueId") or product.get("id") or product.get("sku") or random.randint(
                            100, 9999)
                        self.insert_product_status(pid, "STARTED", f"Product processing started for {pid}")
                        counter = counter + 1
                        success_count = success_count + 1
                        # TODO Manage the product level cleanup and final expected custom channel format
                    except Exception as e:
                        print(e)
                        print_exc()
                        failed_count = failed_count + 1

            self.pim_channel_api = PIMChannelAPI(self.api_key, self.reference_id, group_by_parent=False, slice_id=None)
            for product in self.pim_channel_api:
                # product = transformer.transform(product)
                try:
                    tsv_product = list()
                    for schema_key in properties_schema:
                        data = product.get(schema_key, '')
                        if data:
                            data = ",".join(data) if isinstance(data, list) else data
                        else:
                            data = str(data)
                        tsv_product.append(data)
                    # print(tsv_product)
                    tsv_products.append(tsv_product)
                    pid = product.get("pimUniqueId") or product.get("id") or product.get("sku") or random.randint(100,
                                                                                                                  9999)
                    self.insert_product_status(pid, "STARTED", f"Product processing started for {pid}")
                    counter = counter + 1
                    success_count = success_count + 1
                    # TODO Manage the product level cleanup and final expected custom channel format
                except Exception as e:
                    print(e)
                    print_exc()
                    failed_count = failed_count + 1

            if header:
                tsv_products.insert(0, properties_schema)
            if fixed_header:
                header_row_counter = 0
                for row in fixed_header:
                    tsv_products.insert(header_row_counter, row)
                    header_row_counter += 1
            # print(tsv_products)
            template_outout = write_csv_file(data=tsv_products, delimiter="\t", filename=filename)
            self.update_export_status(status="PRODUCTS_PROCESSED", success_count=success_count,
                                      failed_count=failed_count)
            # template_op_url = self.upload_to_s3(template_outout)
        except Exception as e:
            print_exc()
            print(e)
        return template_outout
