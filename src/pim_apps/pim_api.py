import concurrent
from traceback import print_exc
import json
import os
import time
from time import time as time_time, sleep
import pandas as pd
import requests
from .utils import get_pepperx_domain, get_pim_domain, get_pim_app_domain, get_a2c_domain, write_csv_file,remove_duplicates_from_list, flatten, slack_notifier
from .pepperx_db import ProductStatus, App, AppUser, AppUserPIM
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
        self.error_cache = {}
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
                    self.error_cache = response.get("productErrors",{})
                    self.cache = products
                self.n += 1
                if len(self.cache) > index or len(self.cache)==0:
                    if len(self.cache)>0:
                        return self.extract_product_errors(self.cache[index])
                    else:
                        return self.extract_product_errors([])
                    # return self.cache[index]
                else:
                    print(f"!!!! Optimisation issue or post transformer issue when only 19 products came or lesser products came")
                    #return ({}, [])
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
                    self.error_cache = response.get("productErrors", {})
                    self.cache = products
                self.n += 1
                if len(self.cache) > 0 or len(self.cache)==0:
                    return self.extract_product_errors(self.cache[index])
                else:
                    return self.extract_product_errors([])
                    # return self.cache[index]
            else:
                raise StopIteration

    def extract_product_errors(self, product):
        if len(product)>0:
            pim_unique_id = product.get("pimUniqueId","")
            if pim_unique_id in list(self.error_cache.keys()):
                self.error_cache[pim_unique_id] = list(map(lambda x:x.replace("|:|","-"),self.error_cache[pim_unique_id]))
                return (product,self.error_cache[pim_unique_id])
            else:
                return (product, [])
        else:
            final_error_list = []
            if len(self.error_cache.keys()):
                for key, value in self.error_cache.items():
                    value = list(map(lambda x:x.replace("|:|","-"),value))
                    final_error_list.append(f"{key}:{value}")
            return [],final_error_list


    def is_retryable(self, count, page, type, scroll_id, retry_count, message):
        retry_count = retry_count - 1
        if retry_count == 0:
            raise ValueError(message)
        else:
            sleep(15)
            self.get(count=count, page=page, type=type, scroll_id=scroll_id, retry_count=retry_count)

    def get(self, count=20, page=1, type="PAGINATION", scroll_id=None, retry_count=1):
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
    def generate_csv(self, data, file_name="API_data_fetch", zipped=False, index=False, separator=",", add_time_stamp = True):
        named_tuple = time.localtime()  # get struct_time
        time_string = ""
        if add_time_stamp:
            time_string = time.strftime("-%m-%d-%y-%H-%M", named_tuple)
        df = pd.DataFrame(data)
        file_name = f'{file_name}{time_string}.csv'

        if zipped:
            compression_opts = dict(method='zip',
                                    archive_name=f'{file_name}')
            final_local_url = f'{file_name.split(".")[0]}.zip'
            df.to_csv(final_local_url, index=index,
                      compression=compression_opts, sep=str(separator))
        else:
            final_local_url = file_name
            df.to_csv(final_local_url, index=index, sep=str(separator))
        return final_local_url

    def upload_to_s3(self, filename, bucket = "unbxd-pim-ui"):
        """Upload a file to an S3 bucket
        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        #bucket = "unbxd-pim-ui"
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

    def upload_csv(self, req_data, input_file_name, separator=",", zipped=True, add_time_stamp = True, bucket = "unbxd-pim-ui"):
        file_name = self.generate_csv(req_data, input_file_name, zipped, separator=separator, add_time_stamp=add_time_stamp)
        # csv_url = file_name
        csv_url = self.upload_to_s3(file_name, bucket)
        return csv_url
    
    def get_import_details(self):

        url = f"{get_pim_app_domain()}v1/appTriggerInfo?referenceId={self.reference_id}"

        payload = {}
        headers = {
            'Authorization': f'{self.api_key}'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        return json.loads(response.text)

    def get_property_mappings(self, adapter_id, page=1, count=500):
        url = f"{get_pim_app_domain()}v1/channelAdapter/{adapter_id}/properties"
        payload = json.dumps({
            "page": page,
            "count": count
        })
        headers = {
            'Authorization': f'{self.api_key}',
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        data = response.json()
        return data


class ProductProcessor(object):

    def __init__(self, api_key, reference_id, task_id):
        self.api_key = api_key
        self.task_id = task_id
        self.reference_id = reference_id
        self.app_user_instance = AppUserPIM(self.api_key)


        self.pim_channel_api = PIMChannelAPI(self.api_key, self.reference_id)
        self.export_data = self.pim_channel_api.get_export_details()
        export_details = self.export_data.get("data", {}).get("metaInfo", {}).get("export", {})
        group_by_parent = export_details.get('product_listing_type', False)
        self.pim_channel_api.group_by_parent = True if group_by_parent == "GROUP_BY_PARENT" else False
        # self.raw_products_list = []
        self.product_status_instance = ProductStatus(self.task_id)
        self.failed_processed_products = []


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

    def get_sorted_products_list(self, include_variants=False, exclude_pim_properties=False):
        print("Sorted Product List")
        sorted_product = []
        all_products, failed_products = self.fetch_all_pim_products(include_variants=include_variants, exclude_pim_properties=exclude_pim_properties)
        if all_products and isinstance(all_products,list):
            sorted_product = sorted(all_products, key=lambda d: d.get('pimUniqueId',''))
        return sorted_product

    # 1. Pulls products and variants from PIM

    def process_pim_product(self, product, process_product):
        # print(f"Processing product no {counter}")
        try:
            if product is not None:
                pid = product.get("id") or random.randint(100, 9999)

                # TODO
                # Check if the task ID and product ID entry is there in the product transaction table
                # If status is success for the entry, skip process product invokation
                # ??? If it is fail, then run process product again
                # If entry is not found, run invoke process_product
                # self.insert_product_status(pid,"STARTED" , f"Product processing started for {pid}")

                task_product_status = self.product_status_instance.get(product.get("pimUniqueId"))

                if len(task_product_status.keys()) == 0:
                    proccessed_product, status = process_product(product, self.product_counter)
                else:
                    proccessed_product = product
                    status = task_product_status.get("type", "")
                    if status == "COMPLETE":
                        status = "SUCCESS"

                self.product_counter += 1
                if status == "SUCCESS":
                    self.success_count += 1
                elif status == "FAILED":
                    self.failed_processed_products.append(proccessed_product)
                    self.failed_count += 1
                self.processed_list.append(proccessed_product)
                if self.product_counter % 5 == 0:
                    self.update_export_status(status="EXPORT_IN_PROGRESS", success_count=self.success_count,
                                              failed_count=self.failed_count)

        except Exception as e:
            print_exc()
            error_pid = pid or f"export_pid_{str(time.time())}"
            self.failed_count += 1
            self.insert_product_status(pid=error_pid, status="FAILED", status_desc=f"{str(e)}")

    def exclude_pim_properties(self, df):
        columns_to_drop = ['pimCreatedAt', 'pimUpdatedAt','pimUniqueId', 'pimProductType','pimParentId']  # List the names of the columns you want to drop
        for col in columns_to_drop:
            if col in df.columns:
                df = df.drop(col, axis=1)
        return df

    def get_property_index(self):
        adapter_id = self.export_data.get("data", {}).get("metaInfo", {}).get("adapter", {}).get('adapterId', None)
        properties = []
        if adapter_id is not None:
            current_prop = self.pim_channel_api.get_property_mappings(adapter_id, page=1)
            total_pages = current_prop.get("data", {}).get("page", None)
            entries = current_prop.get("data", {}).get("entries", [])
            properties.extend(entries)
            if total_pages and total_pages > 1:
                current_page = 2
                while (current_page <= total_pages):
                    current_prop = self.pim_channel_api.get_property_mappings(adapter_id, page=current_page)
                    entries = current_prop.get("data", {}).get("entries", [])
                    properties.extend(entries)
                    current_page += 1


        sorted_properties = sorted(properties, key=lambda x: x['index_pos'])
        sorted_property_names = [prop['property_name'] for prop in sorted_properties]
        return sorted_property_names
    
    def process_and_format_success_products(self, products_link, include_variants=False, exclude_pim_properties=False):
        try:
            sorted_property_name = self.get_property_index()

            try:
                response = requests.get(products_link)
                json_data = response.json()
                df = pd.DataFrame(json_data)
                valid_columns = [col for col in sorted_property_name if col in df.columns]
                additional_columns = [col for col in df.columns if col not in valid_columns]
                final_columns = valid_columns + additional_columns
                df = df[final_columns]
                # df = pd.read_json(products_link)
            except Exception as e:
                print(e)
                print_exc()
                df = pd.DataFrame()


            if df.empty:
                # If empty, return an empty list
                return []
    
            # If group_by_parent is FALSE, then only VARIANT and SOLO products will be sent as siblings
            if not self.pim_channel_api.group_by_parent:
                df_variant = df[df['pimProductType'] == 'VARIANT']
                df_solo = df[df['pimProductType'] == 'SOLO']
                
                if exclude_pim_properties:
                    df_variant = self.exclude_pim_properties(df_variant)
                    df_solo = self.exclude_pim_properties(df_solo)
                    
                df_solo = df_solo.where(pd.notnull(df_solo), None)
                df_solo.fillna('',inplace=True)
                
                df_variant = df_variant.where(pd.notnull(df_variant), None)
                df_variant.fillna('',inplace=True)
                
                final_list = df_variant.to_dict("records") + df_solo.to_dict("records")
                return final_list
    
            # If include_variants is TRUE, then PARENT, VARIANT and SOLO will be sent and PARENT wont have VARIANT in it
            if include_variants:
                if exclude_pim_properties:
                    df = self.exclude_pim_properties(df)
                    
                df = df.where(pd.notnull(df), None)
                final_list = df.to_dict('records')
                return final_list
    
            
            
            # Separate parent, variant, and solo products
            df_parent = df[df['pimProductType'] == 'PARENT']
            df_variant = df[df['pimProductType'] == 'VARIANT']
            df_solo = df[df['pimProductType'] == 'SOLO']
    
            # Remove Nan
            df_parent = df_parent.where(pd.notnull(df_parent), None)
            df_parent.fillna('',inplace=True)
            df_variant = df_variant.where(pd.notnull(df_variant), None)
            df_variant.fillna('',inplace=True)
            df_solo = df_solo.where(pd.notnull(df_solo), None)
            df_solo.fillna('',inplace=True)
    
            
            # If include_variants are FALSE, group_by_parent is TRUE and  exclude_pim_properties is TRUE
            # then, the variant product inside the parent product wouls still have the pim properties
            # please handle this inside the job code
            # in this case, PRENT products will have their VARIANTS inside them in "variants" column. This will also include SOLO products
            if not df_variant.empty:
                grouped_variants = df_variant.groupby('pimParentId').apply(lambda x: x.to_dict('records')).reset_index()
                grouped_variants.columns = ['pimUniqueId', 'variants']
        
                merged_df = pd.merge(df_parent, grouped_variants, on='pimUniqueId', how='left')
        
                merged_df['variants'] = merged_df['variants'].apply(lambda x: x if isinstance(x, list) else [])
            else:
                merged_df = pd.DataFrame()
        
            if exclude_pim_properties:
                merged_df = self.exclude_pim_properties(merged_df)
                df_solo = self.exclude_pim_properties(df_solo)                
            
            parent_list = merged_df.to_dict('records')
            final_list = parent_list + df_solo.to_dict('records') + df_variant.to_dict('records')
    
            return final_list
        except Exception as e:
            print(e)
            print_exc()
            return []

    def process_and_send_errors(self, df):
        try:
            for index, row in df.iterrows():
                product_id = row.get('pimUniqueId', "")
    
                error_messages = []
                if 'pimValidationErrors' in row and pd.notna(row['pimValidationErrors']):
                    error_messages.append(str(row['pimValidationErrors']))
    
                if 'pimPostTransformerErrors' in row and pd.notna(row['pimPostTransformerErrors']):
                    error_messages.append(str(row['pimValidationErrors']))
    
                if 'pimTransformationErrors' in row and pd.notna(row['pimTransformationErrors']):
                    error_messages.append(str(row['pimValidationErrors']))
    
                full_error_message = '|'.join(error_messages)
    
                self.insert_product_status(pid=product_id, status="FAILED",
                                           status_desc=full_error_message)
    
            return df.to_dict("records")
        except Exception as e:
            print(e)
            print_exc()
            return []

    def fetch_all_pim_products(self, include_variants=False, exclude_pim_properties=False):
        raw_products_list = []
        failed_product_list = []
        internal_file_download_link = ""
        internal_failed_file_download_link = ""
        count = 0
        internal_export_status = ""

        while not (internal_file_download_link or internal_failed_file_download_link) and count < 3600 and internal_export_status != "FAILED":
            export_data = self.pim_channel_api.get_export_details()
            export_details = export_data.get("data", {}).get("metaInfo", {}).get("export", {})

            internal_file_download_link = export_details.get('internalPartnerExport', {}).get(
                'internal_file_download_links', {}).get("JSON", "")
            internal_failed_file_download_link = export_details.get('internalPartnerExport', {}).get(
                'internal_failed_file_download_links', {}).get("JSON", "")
            
            internal_export_status = export_details.get('internalPartnerExport', {}).get(
                'internal_export_status', "")
            
            if not (internal_file_download_link or internal_failed_file_download_link):
                time.sleep(10)
                count = count+1
                print("Waiting for internal file to be generated....")
                print(f"Took {count*10} seconds...")

        # export_with_readiness = export_details.get("check_readiness", False)

        try:
            if(internal_file_download_link):
                raw_products_list = self.process_and_format_success_products(internal_file_download_link, include_variants, exclude_pim_properties)
        except Exception as e:
            print(e)
            print_exc()

        try:
            if(internal_failed_file_download_link):
                failed_file_df = pd.read_json(internal_failed_file_download_link)
                failed_product_list = self.process_and_send_errors(failed_file_df)
        except Exception as e:
            print(e)
            print_exc()

        return raw_products_list, failed_product_list

        # TODO len(raw_products_list) == 0 is removed in the below line for etsy Solving Alpha
        # if include_variants and not self.pim_channel_api.group_by_parent:
        #     for product, error in self.pim_channel_api:
        #         if isinstance(product, dict):
        #             if export_with_readiness:
        #                 errorList = product.get("errorList", [])
        #                 errorList = [errorList] if isinstance(errorList, str) else errorList
        #                 if len(error) > 0 or len(errorList) > 0:
        #                     error += errorList
        #                     self.insert_product_status(pid=product.get("pimUniqueId", "pid"), status="FAILED",
        #                                                status_desc="|".join(error))
        #                     product["errors"] = "|".join(error)
        #                     failed_product_list.append(product)
        #                 else:
        #                     raw_products_list.append(product)
        #             else:
        #                 raw_products_list.append(product)
        # raw_products_list = remove_duplicates_from_list(raw_products_list)
        # failed_product_list = remove_duplicates_from_list(failed_product_list)

        # return raw_products_list, failed_product_list

    def iterate_products(self, process_product, auto_finish=True, multiThread=True, include_variants=False, update_product_count = True, export_with_readiness=False, exclude_pim_properties=False):
        self.processed_list = []
        self.failed_processed_products = []
        self.product_counter = 0
        self.success_count = 0
        self.failed_count = 0
        ts = f"PIM_ERROR_{time.time()}"
        try:
            counter = 1
            status = True

            total_products = self.pim_channel_api.get()['data'].get('total', 0)
            if not total_products:
                self.pim_channel_api.group_by_parent = False
                total_products = self.pim_channel_api.get()['data'].get('total', 0)
            if total_products > 0:
                self.pim_channel_api.products_total = total_products
                raw_products_list, failed_products_list = self.fetch_all_pim_products(include_variants, exclude_pim_properties)
            else:
                status = False



            # with open('./raw_products.json', 'w') as f:
            #     f.write(json.dumps(raw_products_list))

            # with open('./raw_products.json', 'r') as f:
            #     raw_products_list = f.read()
            # raw_products_list = json.loads(raw_products_list)
            if status:
                if len(failed_products_list)>0:
                    self.failed_processed_products += failed_products_list
                    self.failed_count += len(failed_products_list)
                    self.update_export_status(status="EXPORT_IN_PROGRESS", success_count=self.success_count,
                                              failed_count=self.failed_count)

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
            #     self.insert_product_status(pid=error_pid , status="FAILED", status_desc=f"{str(e)}")


            if update_product_count:

                if auto_finish:
                    self.update_export_status(status="EXPORTED", success_count=self.success_count,
                                              failed_count=self.failed_count)
                else:
                    self.update_export_status(status="PRODUCTS_PROCESSED", success_count=self.success_count,
                                              failed_count=self.failed_count)
            else:
                self.update_export_status(status="PRODUCTS_PROCESSED", failed_count=self.failed_count)

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

    def send_to_pim(self, auto_export=False, file_url="", products_list=[], file_name="App_Results_",
                    custom_reference_id=None):
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
            file_url = self.pim_channel_api.upload_csv(self.processed_list, file_name)
            self.pim_channel_api.import_to_pim(file_url, custom_reference_id)
            print(file_url)

    def upload_to_s3(self, file_path):
        print("Uploading file to s3")

        uploaded_url = self.pim_channel_api.upload_to_s3(file_path)
        return uploaded_url

    def generate_csv(self, data, file_name="API_data_fetch", zipped=False, index=False, separator=","):
        named_tuple = time.localtime()  # get struct_time
        time_string = time.strftime("-%m-%d-%y-%H-%M", named_tuple)
        df = pd.DataFrame(data)
        file_name = f'{file_name}{time_string}.csv'
    
        if zipped:
            compression_opts = dict(method='zip',
                                    archive_name=f'{file_name}')
            final_local_url = f'{file_name.split(".")[0]}.zip'
            df.to_csv(final_local_url, index=index,
                      compression=compression_opts, sep=str(separator))
        else:
            final_local_url = file_name
            df.to_csv(final_local_url, index=index, sep=str(separator))
        return final_local_url

    def upload_csv(self, req_data, input_file_name, separator=",", zipped=True):
        file_name = self.generate_csv(req_data, input_file_name, zipped, separator=separator)
        # csv_url = file_name
        csv_url = self.upload_to_s3(file_name)
        return csv_url

    def write_failed_file(self, failed_product_list):
        try:
            flattened_failed_list = flatten(failed_product_list)
            df = pd.DataFrame(failed_product_list)
    
            # rearrange columns
            try:
                cols = list(df.columns)
                if "errors" in cols:
                    df["errors"] = str(df.get("errors","")).replace("|", "\n\n")
                if "errors" in cols:
                    cols.remove('errors')
                    cols.sort()
                    cols = ['errors'] + cols
                df = df[cols]
            except ValueError as e:
                print(e)
                print_exc()
                return ""
            file_name = f'failed_products_{self.reference_id}_{str(int(time.time()))}.csv'
            # save to csv
            df.to_csv(file_name, index=False)
            file_url = self.upload_to_s3(file_name)
            return file_url
        except ValueError as e:
            print(e)
            print_exc()
            return ""

    def update_export_status(self, status="STARTED", success_file="", failed_file="", success_count=None,
                             failed_count=None):
        data = {
            "status": str(status).upper().strip()
        }
        if success_file:
            data["file_download_links"] = {}
            if isinstance(success_file, list):
                for file in success_file:
                    extension = file.split(".")[-1].upper()
                    data["file_download_links"][extension] = file
            else:
                data["file_download_links"] = {
                    "CSV": success_file
                }
        if failed_file:
            data["failed_file_download_links"] = {
                "CSV": failed_file
            }
        elif status in ["PRODUCTS_FAILED","EXPORTED","FAILED"] and len(self.failed_processed_products)>0:
            try:
                failed_file_url = self.write_failed_file(self.failed_processed_products)
                data["failed_file_download_links"] = {
                    "CSV": failed_file_url
                }
            except ValueError as e:
                print(e)
                print_exc()


        total = self.pim_channel_api.products_total or 0
        #TODO
        # Need to look into this logic
        # if status in ["PRODUCTS_FAILED", "EXPORTED", "FAILED"] and failed_count and success_count:
            # failed_count += self.failed_count
            # total = failed_count + success_count
            
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
                                add_parent_rows=False, exclude_pim_properties=False):
        counter = 1
        # transformer = Transformer(product_schema)
        tsv_products = list()
        template_outout = []
        self.success_count = 0
        self.failed_count = 0
        self.failed_processed_products = []

        try:
            # if add_parent_rows:
                # self.pim_channel_api = PIMChannelAPI(self.api_key, self.reference_id, group_by_parent=True,
                #                                      slice_id=None)
            all_products_with_variants, failed_products_list = self.fetch_all_pim_products(include_variants=True, exclude_pim_properties=exclude_pim_properties)

            failed_count = len(failed_products_list)
            if failed_count>0:
                self.failed_processed_products = failed_products_list
            for product in all_products_with_variants:
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
                    self.success_count = self.success_count + 1
                    # TODO Manage the product level cleanup and final expected custom channel format
                except Exception as e:
                    print(e)
                    print_exc()
                    product["errors"] = str(e)
                    self.failed_processed_products.append(product)
                    self.failed_count = self.failed_count + 1

            if header:
                tsv_products.insert(0, properties_schema)
            if fixed_header:
                header_row_counter = 0
                for row in fixed_header:
                    tsv_products.insert(header_row_counter, row)
                    header_row_counter += 1
            # print(tsv_products)
            template_outout = write_csv_file(data=tsv_products, delimiter="\t", filename=filename)
            self.update_export_status(status="PRODUCTS_PROCESSED", success_count=self.success_count,
                                      failed_count=self.failed_count)
            # template_op_url = self.upload_to_s3(template_outout)
        except Exception as e:
            print_exc()
            print(e)
        return template_outout


class PIMInstaller():
    def __init__(self):
        pass

    '''
    pim_data: dict -- Must have appCustomId, siteName, orgKey and description
    cookie: str -- _un_sso_id
    slack_params: dict -- Any key value pair which needs to be notified in slack
    
    Returns:
    app_creds: dict, pim_creds: dict
    '''
    def install(self, pim_data, cookie, slack_params={}):
        try:

            payload = dict()
            payload["appCustomId"] = pim_data.get("appCustomId", "")
            payload["siteName"] = pim_data.get("siteName", "")
            payload["description"] = pim_data.get("description", "")
            headers = {
                'content-type': "application/json"
            }

            cookies_obj = {"_un_sso_uid": cookie}
            url = get_pim_app_domain() + "api/v3/" + pim_data.get("orgKey","") + "/register"
            response = requests.request("POST", url, data=json.dumps(payload), headers=headers, cookies=cookies_obj)
            response_data = response.json()

            if response.status_code != 200:
                raise ValueError(
                    "Pim Authorization failed: status: {} body : {}".format(str(response.status_code), response.text)
                )

            if "data" not in response_data or "apiKey" not in response_data["data"]:
                raise ValueError("Pim Authorization failed due to data expectation")

            api_key = response_data.get("data", {}).get("apiKey","")
            org_key = pim_data.get("orgKey")
            # TODO Send these to appusercreds
            org_app_id = response_data.get("data", {}).get("org_app_id")

            channel_id = response_data.get("data", {}).get("channel_id")
            adapter_id = response_data.get("data", {}).get("adapter_id")

            if adapter_id is None:
                print("App already exists, delete the app to reinstall")

            pim_creds = {"api_key": api_key, "org_key": org_key, "site_name": pim_data.get("siteName", ""),
                         "email": pim_data.get("userEmail", "apps@unbxd.com")}

            app_creds = {"org_app_id": org_app_id, "channel_id": channel_id, "adapter_id": adapter_id}
            app_custom_id = pim_data.get('appCustomId', '')
            if api_key:

                slack_params.update(pim_creds)
                slack_notifier(channel="#pim-apps", title="New App Installation", header=f"New App Installed for {app_custom_id}", parameters=slack_params)
            else:
                slack_notifier(channel="#pim-apps", title="New App Installation Fail",
                               header=f"New App Installed failed for {app_custom_id}", parameters=slack_params)
            return app_creds, pim_creds

        except Exception as e:
            print_exc()
            print("PIM install failed")
            string_buffer = io.StringIO()
            print_exc(file=string_buffer)
            traceback_str = string_buffer.getvalue()
            status_msg = f"Got into some error {str(traceback_str)}"
            slack_notifier(channel="#pim-apps", title="New App Installation Fail",
                           header=f"New App Installed failed due to {str(e)}", parameters=slack_params)
            return {"error":f"PIM install failed due to {str(e)}" }, {}

