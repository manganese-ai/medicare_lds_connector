import sys
from dotenv import dotenv_values
import yaml
import os

CONFIG = {
    **dotenv_values('.env'), # load shared development variables
    **dotenv_values('.env.local'), # load local variables
    # **os.environ # override loaded values with environment variables
}