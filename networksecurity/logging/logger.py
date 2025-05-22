import logging
import os
from datetime import datetime

LOG_FILE = f'{datetime.now().strftime("%m_%d_%Y_%H_%M_%S")}.log'
LOG_PATH = os.path.join(os.getcwd(), 'logs', LOG_FILE)
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True) 

LOG_FILE_PATH = os.path.join(os.getcwd(), 'logs', LOG_FILE)
logging.basicConfig(
    filename=LOG_FILE_PATH,
    format='[%(asctime)s] %(lineno)d %(levelname)s - %(message)s',
    level=logging.INFO
)

