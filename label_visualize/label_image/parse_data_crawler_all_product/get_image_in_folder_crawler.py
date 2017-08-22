#Get list labels of folder ò images

# Import
import os, os.path
import io
import json
import create_video as create_video



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


def get_json_from_folder_image(path_file, folder, list_json_, path_video, path_image_temp, flag):
    list_json = list_json_
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
                        # Create video
                        video_label = []
                        if flag:
                            try:
                                create_video.create_video(file_name, path_video, path_image_temp)
                                try:
                                    video_label = create_video.analyze_labels(path_video)
                                except Exception as e:
                                    print (e)
                                print (video_label)
                            except Exception as e:
                                video_label = []
                                print (e)
                            print ("Create ok.....!")
                            
                        # Get label image
                        list_label = []
                        list_label = get_image_label_from_cloud_vision(file_name)
                        print (list_label)

                        image = {
                                'file_name': name,
                                'image_label': list_label,
                                'video_label': video_label
                        }
                        list_json.append(image)
    return list_json


def get_folder_product_from_excel(file_json_mapping):
    import json

    list_folder_product = []
    with open (file_json_mapping,'r') as f:
        data = json.load(f)
        for value in data['my_json']:
            list_folder_product.append([str(value['product_id']), str(value['product_name']), list(value['list_folder'])])
    return list_folder_product

def get_image_folder_convert_to_json(path_in, path_out, file_json_mapping):
    flag = True
    if not os.path.exists(path_out):
        os.makedirs(path_out)
    list_folder_product = get_folder_product_from_excel(file_json_mapping)
    #================ Create folder temp ==================
    folder_temp = os.path.join(path_in, 'temp')
    path_video = os.path.join(folder_temp, 'video_temp.mp4')
    for product in list_folder_product:
        if product[0] != '267' and product[0] != '264' and product[0] != '257':
            print ("================================ Product : " + product[0] + " ===================================")
            list_json = []
            for folder in product[2]:
                print (folder)
                path_folder = os.path.join(path_in, folder)
                print(path_folder)
                list_json = get_json_from_folder_image(path_folder, folder, list_json, path_video, folder_temp, flag)
                print("==============================")

            file_name = product[0] + '.json'
            path_folder_out = os.path.join(path_out, product[0])
            file_json_out = os.path.join(path_folder_out, file_name)
            final_json = {}
            final_json['sample_json'] = list_json
            if not os.path.exists(path_folder_out):
                os.makedirs(path_folder_out)
            with open(file_json_out, 'w') as f:
                json.dump(final_json, f)


path_in = "/u01/oracle/oradata/APEX/MARKETING_TOOL_03"
path_out = "/u01/oracle/oradata/APEX/MARKETING_TOOL_03/Json_data_crawler"
file_json_mapping = '/home/marketingtool/Workspace/Python/no-more-weekend/label_visualize/label_image/parse_data_crawler_all_product/mapping_folder_crawler.json'
get_image_folder_convert_to_json(path_in, path_out, file_json_mapping)
