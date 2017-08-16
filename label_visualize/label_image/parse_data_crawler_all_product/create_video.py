# import cv2
# import sys
# import time
# import os
# import json
# import io
# import base64

# from google.cloud.gapic.videointelligence.v1beta1 import enums
# from google.cloud.gapic.videointelligence.v1beta1 import (
#     video_intelligence_service_client)


# def analyze_labels(path):

#     list_label = []

#     """ Detects labels given a file path. """
#     video_client = (video_intelligence_service_client.
#                     VideoIntelligenceServiceClient())
#     features = [enums.Feature.LABEL_DETECTION]

#     with io.open(path, "rb") as movie:
#         content_base64 = base64.b64encode(movie.read())

#     operation = video_client.annotate_video(
#         '', features, input_content=content_base64)
#     print('\nProcessing video for label annotations:')

#     while not operation.done():
#         sys.stdout.write('.')
#         sys.stdout.flush()
#         time.sleep(15)

#     print('\nFinished processing.')

#     # first result is retrieved because a single video was processed
#     results = operation.result().annotation_results[0]

#     for label in results.label_annotations:
#         list_label.append(label.description)
#         print (label.description)
#     return list_label

# def create_video(image, video, flag):
#     size = os.path.getsize(image)
#     n = 20
#     if size > 1024 * 500:
#         n = 25
#     else:
#         n = 75
#     frame = cv2.imread(image)
#     # cv2.imshow('video',frame)
#     try:
#         height, width, channels = frame.shape
#     except AttributeError:
#         flag = False
#         print ("=============================================================")
#         return flag

#     # Define the codec and create VideoWriter object
#     fourcc = cv2.VideoWriter_fourcc(*'mp4v') # Be sure to use lower case
#     out = cv2.VideoWriter(video, fourcc, 20.0, (width, height))
#     for i in range(n):
#         frame = cv2.imread(image)

#         out.write(frame) # Write out frame to video

#         # cv2.imshow('video',frame)
#         if (cv2.waitKey(1) & 0xFF) == ord('q'): # Hit `q` to exit
#             break

#     # Release everything if job is finished
#     out.release()
#     cv2.destroyAllWindows()
