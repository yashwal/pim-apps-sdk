import os

os.environ['PIM_APP_BASE_URL']="https://pim-apps.unbxd.io/pim/"
os.environ['PIM_BASE_URL']="https://pim.unbxd.io/"
os.environ['A2C_BASE_URL']="https://api.api2cart.com/"
os.environ['PEPPERX_URL']="https://pim.unbxd.io/pepperx/"

os.environ['aws_region'] =  "us-west-1"
os.environ['aws_access_key_id'] = "AKIAQF4GD6E5IUQVRSUY"
os.environ['aws_secret_access_key'] =  "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"

class GetDomains:
    def __init__(self):
        pass

    def get_pim_app_domain(self):
        return os.environ['PIM_APP_BASE_URL']


    def get_pim_domain(self):
        return os.environ['PIM_BASE_URL']


    def get_a2c_domain(self):
        return os.environ['A2C_BASE_URL']


    def get_pepperx_domain():
        return os.environ['PEPPERX_URL']