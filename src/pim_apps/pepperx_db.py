import json
import requests
from traceback import print_exc
from .utils import get_pepperx_domain, get_pim_domain, get_pim_app_domain, get_a2c_domain

class App(object):
    def __init__(self, app_id="", app_name=""):
        self.app_id = app_id
        self.app_name = app_name
        self.get(app_id, app_name)

    def create(self, app_id, name, credentials={}):
        try:
            url = f"{get_pepperx_domain()}api/v1/app_data/"
            payload = json.dumps({
                "app_id": app_id,
                "app_name": name,
                "app_creds": credentials
            })
            headers = {
                'Content-Type': 'application/json'
            }
            # Add another try catch block

            response = requests.request("POST", url, headers=headers, data=payload)
            data = response.text
            if response.status in [200, 201]:
                return data
            else:
                print(response)


        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
        except Exception as e:
            print_exc()
            print(e)

    def get(self, app_id, app_name=""):
        try:
            url = f"{get_pepperx_domain()}api/v1/app_data/?app_id={app_id}"
            headers = {
                'Content-Type': 'application/json'
            }
            response = requests.request("GET", url, headers=headers)
            data = response.text
            print("App.get data", data)
            if response.status_code in [200, 201]:
                app_data = json.loads(data)
                app_data = app_data["data"]
                self.app_creds = app_data["app_data"]["app_creds"]
                self.app_name = app_data["app_data"]["app_name"]
                self.app_id = app_data["app_data"]["app_id"]
                return app_data
            else:
                print(response.json())
                raise Exception

        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
        except Exception as e:
            print_exc()
            print(e)


class AppUser(object):
    def __init__(self, app_id, identifier):
        self.app_id = app_id
        self.identifier = identifier

    def create(self, credentials={}, pim_creds={}):
        try:
            url = f"{get_pepperx_domain()}api/v1/app_user_data/"
            payload = json.dumps({
                "app_id": self.app_id,
                "identifier": self.identifier,
                "user_creds": credentials,
                "pim_creds": pim_creds
            })
            headers = {
                'Content-Type': 'application/json'
            }
            response = requests.request("POST", url, headers=headers, data=payload)
            data = response.text
            if response.status_code in [200, 201]:
                app_data = json.loads(data)
                app_data = app_data["data"]
                self.app_user_creds = app_data["app_user"]["app_creds"]
                self.pim_creds = app_data["app_user"].get("pim_creds", None)
            else:
                return data

        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
        except Exception as e:
            print_exc()
            print(e)

    def get(self):
        try:
            url = f"{get_pepperx_domain()}api/v1/app_user_data/?app_id={self.app_id}&identifier={self.identifier}"
            headers = {
                'Content-Type': 'application/json'
            }
            response = requests.request("GET", url, headers=headers)
            data = response.text
            if response.status_code in [200, 201]:
                app_data = json.loads(data)
                app_data = app_data.get("data")
                return app_data
            else:
                print(response.json())

        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
        except Exception as e:
            print_exc()
            print(e)


class AppUserPIM(object):
    def __init__(self, api_key=""):
        self.api_key = api_key

    def get(self):
        try:
            url = f"{get_pepperx_domain()}api/v1/app_user_pim_data/?api_key={self.api_key}"

            payload = json.dumps({})
            headers = {
                'Content-Type': 'application/json'
            }
            response = requests.request("GET", url, headers=headers, data=payload)
            data = response.text
            # print("User creds & PIM Creds")
            print(response.text)
            # print("API Status")
            print(response.status_code)
            if response.status_code in [200, 201]:
                app_data = json.loads(data)
                app_data = app_data["data"]
                return app_data
            else:
                print(response.json())


        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
        except Exception as e:
            print_exc()
            print(e)


class ProductStatus(object):
    def __init__(self, task_id):
        self.task_id = task_id
        # self.product_trassaction_buffer = []

    def post_started_message(self, product_id=""):
        self.success_msg = []
        self.product_id = product_id
        data = {
            "product_id": product_id,
            "status": "Product processing started",
            "type": "RUNNING"
        }
        self.post_transaction(data)

    def post_success_message(self, product_id="", msg="Product creation/updation succeded"):

        # self.success_msg.append(msg)
        data = {
            "product_id": product_id,
            # "status": ";".join(self.success_msg),
            "status": msg,
            "type": "COMPLETE"
        }
        self.post(data)

    def post_error_message(self, product_id="", msg="Product creation/updation failed"):

        data = {
            "product_id": product_id,
            "status": msg,
            "type": "FAILED"
        }
        self.post(data)

    # def post(self, data):
    #     try:
    #         url = f"{get_pepperx_domain()}api/v1/task/product/transaction/bulk"
    #         batch_data = []
    #         data["task_result_id"] = self.task_id
    #         self.product_trassaction_buffer.append(data)
    #         if len(self.product_trassaction_buffer) % 5 == 0:
    #             batch_data = self.product_trassaction_buffer
    #             self.product_trassaction_buffer = []
    #             print("Sending  bulk  buffer")
    #
    #
    #         if len(batch_data) > 0:
    #             payload = json.dumps({"entries" : batch_data})
    #
    #             headers = {
    #                 'accept': 'application/json',
    #                 'Content-Type': 'application/json'
    #             }
    #             print("Product status update for each product --- >",data)
    #             response = requests.request("POST", url, headers=headers, data=payload)
    #             # print("UPDATED THE MESSAGE SUCCESSFULLY******",response.text)
    #             if response.status_code not in [200, 201]:
    #                 raise ValueError
    #     except Exception as e:
    #         print(e)
    #         print_exc()

    def post(self, data):
        try:
            url = f"{get_pepperx_domain()}api/v1/task/product/transaction"

            data["task_result_id"] = self.task_id
            payload = json.dumps(data)

            headers = {
                'accept': 'application/json',
                'Content-Type': 'application/json'
            }
            print("Product status update for each product --- >",data)
            response = requests.request("POST", url, headers=headers, data=payload)
            # print("UPDATED THE MESSAGE SUCCESSFULLY******",response.text)
            if response.status_code not in [200, 201]:
                raise ValueError
        except Exception as e:
            print(e)
            print_exc()

    def post_transaction(self, data):

        try:
            url = f"{get_pepperx_domain()}api/v1/transaction"

            data["task_result_id"] = self.task_id
            payload = json.dumps(data)

            headers = {
                'accept': 'application/json',
                'Content-Type': 'application/json'
            }
            # print("DATA that was sent to PIM*********",data)
            response = requests.request("POST", url, headers=headers, data=payload)
            # print("UPDATED THE MESSAGE SUCCESSFULLY******",response.text)
            if response.status_code not in [200, 201]:
                raise ValueError
        except Exception as e:
            print(e)
            print_exc()


    def get_task_status(self):

        url = f"{get_pepperx_domain()}api/v1/task/transaction?task_id={self.task_id}"

        payload={}
        headers = {
            'accept': 'application/json'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        print(response.text)
        resp = json.loads(response.text)
        data = resp["data"]

        return data



