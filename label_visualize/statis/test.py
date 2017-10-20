import os, os.path
#from os.path import splitext, basename, join
import json
import csv
import numpy as np
from itertools import combinations



def down_load_file(photo_link, path_down_load_file):
    import io
    import os

    try:
        from urllib.request import urlretrieve  # Python 3
        from urllib.error import HTTPError,ContentTooShortError
    except ImportError:
        from urllib import urlretrieve  # Python 2



    try:
        from urllib.parse import urlparse  # Python 3
    except ImportError:
        from urlparse import urlparse  # Python 2
    from os.path import splitext, basename, join
    picture_page = photo_link
    disassembled = urlparse(picture_page)
    filename, file_ext = splitext(basename(disassembled.path))
    filename = filename + file_ext
    fullfilename = os.path.join(path_down_load_file, filename)


    #download
    try:
        urlretrieve(photo_link, fullfilename)

    except HTTPError as err:
        return 0
        print(err.code)
    except ContentTooShortError as err:
        #retry 1 times
        try:
            urlretrieve(photo_link, fullfilename)
        except ContentTooShortError as err:
            print(err.code)
            return 0
    return 1


photo_link = ''


if __name__ == '__main__':
    from sys import argv
    script, photo_link = argv

    path_down_load_file = 'C:/Users/ltduo/Desktop/Temp'
    down_load_file(photo_link, path_down_load_file)
    print ("Completed !.......")
