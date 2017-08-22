import cv2
import sys
import time
import os
import json
import io
import base64

from google.cloud.gapic.videointelligence.v1beta1 import enums
from google.cloud.gapic.videointelligence.v1beta1 import (
    video_intelligence_service_client)


def analyze_labels(path):

    list_label = []

    """ Detects labels given a file path. """
    video_client = (video_intelligence_service_client.
                    VideoIntelligenceServiceClient())
    features = [enums.Feature.LABEL_DETECTION]

    with io.open(path, "rb") as movie:
        content_base64 = base64.b64encode(movie.read())

    operation = video_client.annotate_video(
        '', features, input_content=content_base64)
    print('\nProcessing video for label annotations:')

    while not operation.done():
        sys.stdout.write('.')
        sys.stdout.flush()
        time.sleep(15)

    print('\nFinished processing.')

    # first result is retrieved because a single video was processed
    results = operation.result().annotation_results[0]

    for label in results.label_annotations:
        list_label.append(label.description)
        print (label.description)
    return list_label


def create_video(image, video, path_temp):
    import PIL
    from PIL import Image
    import subprocess
    flag = False

    img = Image.open(image)
    temp_image = os.path.join(path_temp, "0.jpg")
    #img.save(temp_image)
    print (image)
    #======== Convert image into .jpg ========
    if (image.find('.jpg') < 0):
        #img = Image.open(image)
        if img.mode != "RGB":
            rgb_im = img.convert('RGB')
        rgb_im.save(temp_image)
        img = Image.open(temp_image)
        flag = True

    #==== Resize image if too heavy ========
    if (os.path.getsize(image) >= (1024 * 1024)):
        print ("scale image....")
        basewidth = 500
        wpercent = (basewidth / float(img.size[0]))
        hsize = int((float(img.size[1]) * float(wpercent)))
        img = img.resize((basewidth, hsize), PIL.Image.ANTIALIAS)
        img.save(temp_image)
        flag = True

    #===== Resize image if height and width is odd ====
    if (img.size[1] % 2) != 0:
        img = img.resize((img.size[0], img.size[1] - 1), PIL.Image.ANTIALIAS)
        img.save(temp_image)
        flag = True
    elif (img.size[0] % 2) != 0:
        img = img.resize((img.size[0] - 1, img.size[1]), PIL.Image.ANTIALIAS)
        img.save(temp_image)
        flag = True

    frame_path = os.path.join(path_temp, "1.jpg")
    img.save(frame_path)

    if flag:
        subprocess.call(["ffmpeg", "-f", "image2", "-r", "1.0/10", "-i", os.path.join(path_temp,  "%d.jpg"),"-vcodec", "mjpeg4", "-vcodec", "libx264", "-y", video])
        #subprocess.call(["ffmpeg", "-f", "image2", "-i", temp_image, video])
    else:
        subprocess.call(["ffmpeg", "-f", "image2", "-r", "1.0/10", "-i", os.path.join(path_temp,  "%d.jpg"),"-vcodec", "mjpeg4", "-vcodec", "libx264", "-y", video])
