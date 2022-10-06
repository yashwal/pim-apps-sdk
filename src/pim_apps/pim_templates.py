import json
import requests
from traceback import print_exc
import os
import time


class ReaperAdapterUtils:
    def __init__(self, cred):
        self.cred = cred

    def get_adapter_info(self, adapter_id):
        url = self.cred["url_prefix"] + "paprika/api/v1/" + self.cred["org_id"] + "/adapters/" + adapter_id + "/info"
        payload = None
        headers = {"Cookie": self.cred["un_sso_id"], "Content-Type": "application/json"}
        response = requests.request("GET", url, headers=headers)
        return json.loads(response.text)

    def is_adapter_present(self, adapter_id):
        url = self.cred["url_prefix"] + "paprika/api/v1/" + "internal" + "/adapters/" + adapter_id + "/property-mapping"
        payload = json.dumps({
            "page": 1,
            "count": 2000,
            "name": "",
            "property_type_filter": []
        })
        headers = {"Cookie": self.cred["un_sso_id"], "Content-Type": "application/json"}
        response = requests.request("POST", url, headers=headers, data=payload)
        return response.status_code

    def get_pim_properties(self):
        url = self.cred["url_prefix"] + "paprika/api/v2/internal/" + self.cred["org_id"] + "/properties/all/filters"
        payload = json.dumps(
            {"name": None, "data_type": None, "property_group_id": None, "with_permission": True, "filter_type": "all",
             "projections": ["id", "name", "data_type", "field_id", "reference_id", "prop_type", "meta_info",
                             "validation_rule", "group", "property_group_id", "searchable", "alias_name",
                             "pim_schema_name"]})
        headers = {"Cookie": self.cred["un_sso_id"], "Content-Type": "application/json"}
        response = requests.request("POST", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        return response_data["data"]["entries"]

    def post_pim_property(self, property):
        url = self.cred["url_prefix"] + "paprika/api/v2/" + self.cred["org_id"] + "/properties"
        payload = json.dumps({"property": {"name": property["property_name"], "data_type": property["data_type"]}})
        headers = {"Cookie": self.cred["un_sso_id"], "Content-Type": "application/json"}
        response = requests.request("POST", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        return response_data["data"]

    def add_enum_value(self, enum_name, field_id):
        url = self.cred["url_prefix"] + "api/v2/" + self.cred["org_id"] + "/categories"
        payload = json.dumps({"name": enum_name, "field_id": field_id, "full_path": enum_name, "has_children": False})
        headers = {"Cookie": self.cred["un_sso_id"], "Content-Type": "application/json"}
        response = requests.request("POST", url, headers=headers, data=payload)
        response_data = json.loads(response.text)

    def get_mappings(self, adapter_id):
        url = self.cred["url_prefix"] + "paprika/api/v1/" + self.cred[
            "org_id"] + "/adapters/" + adapter_id + "/property-mapping"
        payload = json.dumps({
            "page": 1,
            "count": 10000,
            "name": "",
            "property_type_filter": []
        })
        headers = {"Cookie": self.cred["un_sso_id"], "Content-Type": "application/json"}
        try:
            response = requests.request("POST", url, headers=headers, data=payload)
            response_data = json.loads(response.text)
            return response_data["data"]["entries"]
        except:
            return []

    def patch_mappings(self, adapter_id, property_mappings):
        url = self.cred["url_prefix"] + "paprika/api/v1/" + self.cred["org_id"] + "/adapters/" + adapter_id
        payload = json.dumps(property_mappings)
        headers = {"Cookie": self.cred["un_sso_id"], "Content-Type": "application/json"}
        response = requests.request("PATCH", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        print(response_data)

    def create_custom_adapter_id(self, adapter_name, desc="This is auto generated Adapter from file", type="CUSTOM"):
        request_data = {"name": adapter_name, "description": desc, "type": type}
        url = self.cred["url_prefix"] + "paprika/api/v1/" + self.cred["org_id"] + "/adapters"
        payload = json.dumps(
            request_data)
        headers = {"Cookie": self.cred["un_sso_id"], "Content-Type": "application/json"}
        response = requests.request("POST", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        return response_data["data"]["adapter_id"]

    def create_system_adapter_id(self, adapter_name, file_desc="This is an auto generated adapter from file",
                                 adapter_type="SYSTEM"):
        url = self.cred["url_prefix"] + "paprika/api/v1/" + self.cred["org_id"] + "/adapters"
        payload = json.dumps(
            {"name": adapter_name, "description": "This is generated for the category of " + adapter_name,
             "type": adapter_type})
        headers = {"Cookie": self.cred["un_sso_id"], "Content-Type": "application/json"}
        response = requests.request("POST", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        return response_data["data"]["adapter_id"]

    def create_adapter_id(self, adapter_name, file_desc="This is an auto generated adapter from file",
                          adapter_type="CUSTOM", channel_id="", org_app_id="", app_custom_id="", platform_id=""):
        # url = self.cred["url_prefix"] + "paprika/api/v1/" + self.cred["org_id"] + "/adapters"
        payload = {"name": adapter_name, "description": file_desc, "org_id": self.cred["org_id"]}

        url = f"{self.cred['url_prefix']}paprika/api/v1/{self.cred['org_id']}/adapters"

        if adapter_type == "CHANNEL_EXPORT":
            payload["type"] = adapter_type
            payload["channelId"] = channel_id
            payload["org_app_id"] = org_app_id
            payload["is_default"] = False
            payload["app_custom_id"] = app_custom_id

            payload["specifics"] = {}
            # payload["specifics"] = {"org_app_id" : org_app_id}
        elif adapter_type == "PLATFORM":
            payload["type"] = adapter_type
            payload["specifics"] = {}
            payload["specifics"] = {"platform": platform_id}

        payload = json.dumps(payload)
        headers = {"Cookie": self.cred["un_sso_id"], "content-Type": "application/json"}
        print(f"url--> {url}")
        print(f"Headers--> {headers}")
        print(f"payload--> {payload}")
        response = requests.request("POST", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        return response_data["data"]["adapter_id"]

    def patch_adapter_property_mappings_by_id(self, adapter_id, adapter_property_mappings):
        url = self.cred["url_prefix"] + "paprika/api/v1/" + self.cred["org_id"] + "/adapters/" + adapter_id
        payload = adapter_property_mappings
        headers = {"Cookie": self.cred["un_sso_id"], "Content-Type": "application/json"}
        response = requests.request("PATCH", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        print(response_data)

    def create_system_adapter(self, adapter_name, adapter_property_mappings):
        # get hold of adapter id if adapter is existing
        adapter_id = self.system_adapter_by_name(adapter_name)
        # create a new adapter if it's not created before
        if adapter_id == None:
            adapter_id = self.create_system_adapter_id(adapter_name)
        # push mappings data into the provided adapter_id
        print(type(adapter_property_mappings))
        self.patch_adapter_property_mappings_by_id(adapter_id, adapter_property_mappings)

    def copy_adapter(self, source_adapter_id, destination_adapter_id):
        property_mappings = self.get_mappings(source_adapter_id)
        property_mappings_mod = []
        for property_mapping in property_mappings:
            if "adapter_property_id" in property_mapping:
                del property_mapping["adapter_property_id"]
                property_mappings_mod.append(property_mapping)
        self.patch_mappings(destination_adapter_id, property_mappings_mod)

    def delete_all_adapter_properties(self, adapter_id):
        property_mappings = self.get_mappings(adapter_id)
        property_mappings_mod = []
        for property_mapping in property_mappings:
            property_mapping["mapping_type"] = "DELETE"
            property_mappings_mod.append(property_mapping)

        self.patch_mappings(adapter_id, property_mappings_mod)

    def add_properties_to_adapter(self, property_names):
        pass

    def generate_reaper_property(self, adapter_property_name="", alias_name="", is_editable=True, required=False,
                                 is_multivalued=False,
                                 index_pos=1, description="", pim_schema_name="", data_type="",
                                 is_essential_field=False, prop_type=""):
        single_payload = {"required": required,
                          "is_editable": is_editable,
                          "alias_name": alias_name,
                          "data_type": data_type,
                          "adapter_property_name": adapter_property_name,
                          "pim_schema_name": pim_schema_name,
                          "is_essential_field": is_essential_field,
                          "editor_type": "TOOL",
                          "is_multivalued": is_multivalued,
                          "prop_type": prop_type,
                          "index_pos": index_pos,
                          "description": description,
                          "mapping_type": "SIMPLE"}
        return single_payload

    def get_adapter_id(self, request, adapter_desc=None):
        adapter_name = request.get("adapter_name", "")
        adapter_id = request.get("adapter_id", "")
        channel_id = request.get("channel_id", "")
        org_app_id = request.get("org_app_id", "")
        app_custom_id = request.get("app_custom_id", "")
        platform_id = request.get("platform_id", "")
        if adapter_desc:
            adapter_desc = adapter_desc or request.get("description", "")
        elif request.get("description", ""):
            adapter_desc = request.get("description", "")

        if channel_id and org_app_id and adapter_name and app_custom_id:
            adapter_id = self.create_adapter_id(adapter_name, adapter_desc, "CHANNEL_EXPORT", channel_id, org_app_id,
                                                app_custom_id)
        elif platform_id and adapter_name:
            adapter_id = self.create_adapter_id(adapter_name, adapter_desc, "PLATFORM", platform_id=platform_id)
        elif (adapter_id == "" or adapter_id is None) and adapter_name:
            adapter_id = self.custom_adapter_by_name(adapter_name)
            if adapter_id == None:
                adapter_id = self.create_custom_adapter_id(adapter_name, adapter_desc)
            else:
                info_data = {
                    "name": adapter_name,
                    "description": adapter_desc
                }
                self.update_adapter_info(adapter_id, info_data)
        elif adapter_id:
            info_data = {
                "name": adapter_name,
                "description": adapter_desc
            }
            self.update_adapter_info(adapter_id, info_data)

        return adapter_id

    def map_adapter_to_pim(self, adapter_properties):
        # Mapping the final generated adapter properties with PIM Properties
        # get all pim properties
        pim_properties = self.get_pim_properties()
        pim_prop_map = {}
        pim_schema_map = {}
        pim_alias_map = {}
        pim_prop_aliasmap = {}
        for pim_property in pim_properties:
            pim_prop_map[pim_property["name"]] = pim_property["field_id"]
            if "pim_schema_name" in pim_property:
                pim_schema_map[pim_property["pim_schema_name"]] = pim_property["field_id"]
            if "alias_name" in pim_property:
                pim_alias_map[pim_property["alias_name"]] = pim_property["field_id"]

        # PIM propname to field_id map, pim_schema_map & pim_alias_map
        print(pim_prop_map)

        count = 0
        for adapter_property in adapter_properties:
            try:
                if "pim_property_id" in adapter_property or adapter_property["pim_property_id"] != None:
                    print("do nothing")
            except:
                if "mapping_type" in adapter_property and adapter_property["mapping_type"] == "CODE":
                    print("do nothing")
                else:
                    print("do Something")
                    try:
                        if adapter_property["adapter_property_name"] in pim_prop_map:
                            adapter_property["pim_property_id"] = pim_prop_map[
                                adapter_property["adapter_property_name"]] if \
                                adapter_property["adapter_property_name"] in pim_prop_map else None
                            adapter_properties[count].update(
                                {"pim_property_id": adapter_property["pim_property_id"],
                                 "mapping_type": "SIMPLE"})
                        elif adapter_property["adapter_property_name"].lower() in pim_prop_map:
                            adapter_property["pim_property_id"] = pim_prop_map[
                                adapter_property["adapter_property_name"]] if \
                                adapter_property["adapter_property_name"] in pim_prop_map else None
                            adapter_properties[count].update(
                                {"pim_property_id": adapter_property["pim_property_id"],
                                 "mapping_type": "SIMPLE"})
                        elif adapter_property["adapter_property_name"].upper() in pim_prop_map:
                            adapter_property["pim_property_id"] = pim_prop_map[
                                adapter_property["adapter_property_name"]] if \
                                adapter_property["adapter_property_name"] in pim_prop_map else None
                            adapter_properties[count].update(
                                {"pim_property_id": adapter_property["pim_property_id"],
                                 "mapping_type": "SIMPLE"})
                        elif "pim_schema_name" in adapter_property and adapter_property[
                            "pim_schema_name"] in pim_schema_map:
                            adapter_property["pim_property_id"] = pim_schema_map[adapter_property["pim_schema_name"]]
                            adapter_properties[count].update(
                                {"pim_property_id": adapter_property["pim_property_id"],
                                 "mapping_type": "SIMPLE"})
                    except Exception as e:
                        print(e)
                        print_exc()

                    #
                    #
                    # elif adapter_property["adapter_property_name"] in pim_prop_aliasmap:
                    #     adapter_property["pim_property_id"] = pim_prop_aliasmap[adapter_property["adapter_property_name"]]
                    # elif adapter_property["alias_name"] in pim_prop_map:
                    #     adapter_property["pim_property_id"] = pim_prop_map[adapter_property["alias_name"]]
                    # elif adapter_property["alias_name"] in pim_prop_aliasmap:
                    #     adapter_property["pim_property_id"] = pim_prop_aliasmap[adapter_property["alias_name"]]
            count += 1

        return adapter_properties

    def system_adapter_by_name(self, adapter_name):
        url = self.cred["url_prefix"] + "paprika/api/v1/" + self.cred["org_id"] + "/adapterList/SYSTEM"
        payload = json.dumps({"page": 1, "count": 2000, "name": ""})
        headers = {"Cookie": self.cred["un_sso_id"], "Content-Type": "application/json"}
        response = requests.request("POST", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        try:
            count = 0
            while (True):
                if adapter_name == str(response_data["data"]["adapterInfos"][count]["name"]):
                    return response_data["data"]["adapterInfos"][count]["id"]
                count += 1
        except:
            return None

    def custom_adapter_by_name(self, adapter_name):
        url = self.cred["url_prefix"] + "paprika/api/v1/" + self.cred["org_id"] + "/adapterList/CUSTOM"
        payload = json.dumps({"page": 1, "count": 2000, "name": ""})
        headers = {"Cookie": self.cred["un_sso_id"], "Content-Type": "application/json"}
        response = requests.request("POST", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        try:
            count = 0
            while (True):
                if adapter_name == str(response_data["data"]["adapterInfos"][count]["name"]):
                    return response_data["data"]["adapterInfos"][count]["id"]
                count += 1
        except:
            return None

    def get_adapter_list(self):
        adapter_name_list = []
        url = self.cred["url_prefix"] + "paprika/api/v1/" + self.cred["org_id"] + "/adapterList/CUSTOM"
        payload = json.dumps({"page": 1, "count": 2000, "name": ""})
        headers = {"Cookie": self.cred["un_sso_id"], "Content-Type": "application/json"}
        response = requests.request("POST", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        count = 0
        try:
            while count < len(response_data["data"]["adapterInfos"]):
                adapter_name_list.append(response_data["data"]["adapterInfos"][count]["name"])
        except:
            pass

        url = self.cred["url_prefix"] + "paprika/api/v1/" + self.cred["org_id"] + "/adapterList/IMPORT"
        payload = json.dumps({"page": 1, "count": 2000, "name": ""})
        headers = {"Cookie": self.cred["un_sso_id"], "Content-Type": "application/json"}
        response = requests.request("POST", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        count = 0
        try:
            while count < len(response_data["data"]["adapterInfos"]):
                adapter_name_list.append(response_data["data"]["adapterInfos"][count]["name"])
        except:
            pass

        return adapter_name_list

    def update_adapter_info(self, adapter_id, data):

        url = f"{self.cred['url_prefix']}paprika/api/v1/{self.cred['org_id']}/adapters/{adapter_id}/info"

        payload = json.dumps(data)

        headers = {"Cookie": self.cred["un_sso_id"], "Content-Type": "application/json"}

        response = requests.request("POST", url, headers=headers, data=payload)

        print(response.text)

    def create_or_update_adapter(self, request, extracted_list=[], adapter_desc="", auto_map=True):
        status = "IN_PROGRESS"
        try:
            # Create or get adapter id based on adapter name or given id
            reaper_utils = ReaperAdapterUtils(self.cred)
            adapter_id = reaper_utils.get_adapter_id(request, adapter_desc)

            existing_adapter_details = reaper_utils.get_mappings(adapter_id)
            existing_adapter_properties = [x["adapter_property_name"] for x in existing_adapter_details]
            existing_adapter_map = {}
            for item in existing_adapter_details:
                name = item['adapter_property_name']
                existing_adapter_map[name] = item
            if reaper_utils.is_adapter_present(adapter_id) == 200:
                property_mappings = []
                index_pos = 1
                for property_obj in extracted_list:
                    adapter_prop_name = property_obj[
                        "adapter_property_name"] if "adapter_property_name" in property_obj else property_obj
                    if adapter_prop_name not in existing_adapter_properties:
                        property_mappings.append(property_obj)
                    else:
                        print(
                            "Handle for overriding validations rules or any other config related to adapter property object")
                        existing_prop = existing_adapter_map[adapter_prop_name]
                        required_props = dict((k, (property_obj[k] if k in property_obj else None)) for k in
                                              ('validation_rules', 'required', 'pim_schema_name'))
                        merged_prop = existing_prop
                        for k, v in required_props.items():
                            if v is not None:
                                merged_prop[k] = v
                        # merged_prop = dict(property_obj, **required_props)
                        property_mappings.append(merged_prop)

                if len(property_mappings):
                    print("$$$$$ Updating merged existing property with new rules and meta info")
                    reaper_utils.patch_mappings(adapter_id, property_mappings)

            if auto_map and self.cred["org_id"] != "internal":
                print("$$$$$ Auto mapping the adapter with PIM properties based on PSN & match")
                # get updated adapter properties & auto map the adapter based on PIM property names
                updated_adapter_properties = reaper_utils.get_mappings(adapter_id)
                mapped_adapter_properties = reaper_utils.map_adapter_to_pim(updated_adapter_properties)
                reaper_utils.patch_mappings(adapter_id, mapped_adapter_properties)
                print("$$$$$ Done updating mapping of the adapter with PIM properties based on PSN & match")
                status = "SUCCESS"
        except Exception as e:
            print(e)
            status = "FAILED"

        return status