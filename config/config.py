import json
import logging
import os
logging.basicConfig(level=logging.INFO)
basedir = os.path.abspath(os.path.dirname(__file__))
def configs(link = './config/config.json'):
    with open(link, 'r') as f:  # ./config/
        config = json.load(f)
    f.close()
    return config




