import os

os.environ['A2C_BASE_URL']="https://api.api2cart.com/"

os.environ['PIM_APP_BASE_URL']="https://pim-apps.unbxd.io/pim/"
os.environ['PIM_BASE_URL']="https://pim.unbxd.io/"
os.environ['PEPPERX_URL']="https://pim.unbxd.io/pepperx/"

os.environ['QA_PIM_APP_BASE_URL']="http://pimqa-apps.unbxd.io/pim/"
os.environ['QA_PIM_BASE_URL']="http://pimqa.unbxd.io/"
os.environ['QA_PEPPERX_URL']="https://pimqa.unbxd.io/pepperx/"



def get_pim_app_domain():

    env = os.environ['PEPPERX_ENV']
    url = os.environ['PIM_APP_BASE_URL'] if env == "PROD" else os.environ['QA_PIM_APP_BASE_URL']
    print(f" {env} ---- {url} ")
    return url


def get_pim_domain():
    env = os.environ['PEPPERX_ENV']
    url = os.environ['PIM_BASE_URL'] if env == "PROD" else os.environ['QA_PIM_BASE_URL']
    print(f" {env} ---- {url} ")
    return url


def get_a2c_domain():
    return os.environ['A2C_BASE_URL']


def get_pepperx_domain():
    env = os.environ['PEPPERX_ENV']
    url =  os.environ['PEPPERX_URL'] if env == "PROD" else os.environ['QA_PEPPERX_URL']
    print(f" {env} ---- {url} ")
    return url
