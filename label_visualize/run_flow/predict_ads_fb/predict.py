import compare_label as cp
import os, os.path
import pandas as pd
import json



def get_image_label_from_cloud_vision(photo_file):
    import io
    import os
    import time

    # Imports the Google Cloud client library
    from google.cloud import vision

    list_label = []
    # Instantiates a client
    vision_client = vision.Client()
    if os.path.exists(photo_file):
        if (os.path.getsize(photo_file) >= (1024 * 1024 * 4)):
            import PIL
            from PIL import Image
            print ("scale iamge....")

            img = Image.open(photo_file)
            basewidth = 1300
            wpercent = (basewidth / float(img.size[0]))
            hsize = int((float(img.size[1]) * float(wpercent)))
            img = img.resize((basewidth, hsize), PIL.Image.ANTIALIAS)
            img.save(photo_file)
        try:
            # Loads the image into memory
            with io.open(photo_file, 'rb') as image_file:
                content = image_file.read()
                image = vision_client.image(
                    content=content)

            # Performs label detection on the image file
            try:
                labels = image.detect_labels()
            except google.gax.errors.RetryError as err:
                print ("errors gax 1")
                #retry 1 times
                time.sleep(5)
                try:
                    labels = image.detect_labels()
                except google.gax.errors.RetryError as err:
                    print ("errors gax 2")
                    #retry 1 times
                    print(err.code)
                except:
                    print("Unknown Error")
            except:
                print("Unknown Error")

            for label in labels:
                list_label.append(label.description)
            # [END vision_quickstart]
        except IOError as e:
            # you can print the error here, e.g.
            print(str(e))
        except:
            print("Unknown Error try get label")
    return list_label

def predict(path_file, image , path_content_crawler, product, percent_train, percent_test, number_relationship):
    list_folder = next(os.walk(path_content_crawler))[1]
    exists = False
    name = ""
    if image == '1':
        name = 'image'
    else:
        name = 'video'
    for folder in list_folder:
        if folder == product:
            path_percent = os.path.join(path_content_crawler, folder + '/' + folder + '_' + name +'.csv')
            path_relationship = os.path.join(path_content_crawler, folder + '/' + folder + '_' + name +'_relationship.csv')
            #================== Create model =====================
            list_bigger, label_relationship = cp.create_dataset_predict(path_percent, path_relationship, percent_train, number_relationship)
            #================== Get label image ==================
            label_image = get_image_label_from_cloud_vision(path_file)
            print (label_image)
            #================= Predict ===========================
            flag = cp.check_percent(label_image, list_bigger, label_relationship, percent_test)
            print (flag)
            exists = True
    if not exists:
        print ("Product chua co data train.....!")

# path_image = 'C:/Users/CPU10145-local/Desktop/example'
path_content_crawler = '/u01/oracle/oradata/APEX/MARKETING_TOOL_03/Json_data_crawler'
path_crawler = '/u01/oracle/oradata/APEX/MARKETING_TOOL_03'
# path_content_crawler = 'C:/Users/CPU10145-local/Desktop/Server/MARKETING_TOOL_03/Json_data_crawler'
# path_crawler = 'C:/Users/CPU10145-local/Desktop/Server/MARKETING_TOOL_03'

percent_test = 85
percent_train = 80
number_relationship = 3

if __name__ == '__main__':
    from sys import argv
    #============= Tao thu muc tam, khi predict anh tren web ==============
    path = os.path.join(path_crawler, 'temp')
    if not os.path.exists(path):
        os.makedirs(path)
    #======================================================================

    script, product, type_, image_name, url = argv
    if url == '1':
        path_file = cp.down_load_file(image_name, path)
    else:
        # path_file = os.path.join(path_image, image_name)
        path_file = image_name
    print (path_file)
    predict(path_file, type_, path_content_crawler, product, percent_train, percent_test, number_relationship)
