# Import
import os, os.path
import io
import json


def get_label_vision(photo_file):  
    """
        Get label to google cloud vision API
        + Input: path file name
        + Output: list label 
    """
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

def get_json_from_folder_image(path_file, folder):
    """
        Get all image (.png and .jpg) from folder and get label of this image.
    """
    list_json = []
    for root, dirs, files in os.walk(path_file):            
        for file in files:
            if '/' in file:         
                file = file.decode('unicode')
            if (file[-4:] == '.jpg') or (file[-4:] == '.png'):
                file_name = os.path.join(root, file)
                name = folder + '/' + file
                if (os.path.exists(file_name)):
                    size = os.path.getsize(file_name)
                    if (size >= (1024 * 5)):        
                        list_label = get_label_vision(file_name)           
                        image = {
                                'file_name': name,
                                'image_label': list_label
                        }
                        list_json.append(image)
    return list_json

def get_image_folder_convert_to_json(path_in, path_out):
    """
        Get label image of each product crawler and save to file json
        Note: 
        + Input: ../Sources (contain folder crawler or each product)
        + Output: ../Json_and_report
    """
    list_folder = next(os.walk(path_in))[1]
    for folder in list_folder:  
        path_folder = os.path.join(path_in, folder)
        print(path_folder)
        list_json = get_json_from_folder_image(path_folder, folder)
        print("==============================")
        file_name = str(folder) + '.json'
        path_folder_out = os.path.join(path_out,folder)
        file_json_out = os.path.join(path_folder_out, file_name)
        final_json = {}
        final_json['sample_json'] = list_json
        if not os.path.exists(path_folder_out):
            os.makedirs(path_folder_out)
        with open(file_json_out, 'w') as f:
            json.dump(final_json, f) 

path_in = "C:/Users/CPU10145-local/Desktop/Python Envirement/Data_product/Sources"
path_out = "C:/Users/CPU10145-local/Desktop/Python Envirement/Data_product/Json_and_report"
get_image_folder_convert_to_json(path_in, path_out)