# path_base = 'E:\VNG\Data\DATA\DWHVNG\APEX\MARKETING_TOOL_02_JSON/'
path_base = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON/'

import argparse
import base64
import os


def remove_image_label():
    """Run a label request """
    list_folder = next(os.walk(path_base))[1]
    for folder in list_folder:
        path_folder = os.path.join(path_base, folder)
        file_name = "image_url_"+ folder +".json"
        path_file = os.path.join(path_folder, file_name)
        if os.path.exists(path_file):
            os.remove(path_file)


remove_image_label()