
"""
    Project : Online marketing tool - Audit content - Audit image
    Company : VNG Corporation

    Description: Call gcloud vision API to get image lable
    
    Examples of Usage:
        python get_image_lable.py 2016-10-01 2017-06-29
"""



import argparse
import base64
import os

#base_dir="/home/leth/Workspace/Python/python3/parse_csv/sources/"
# path_base = 'C:/Users/CPU10145-local/Desktop/Python Envirement/Data/DWHVNG/APEX/test'
# path_base = 'E:\VNG\Data\DATA\DWHVNG\APEX\MARKETING_TOOL_02_JSON/'

def label(photo_link, g_vdate, path_base):
    # [START vision_quickstart]
    import io
    import os
    import time
    #because limitations, each request should wait
    #wait 3 seconds
    #time.sleep(5)


    #import argparse
    #import base64

    #import googleapiclient.discovery

    #
    try:
        from urllib.request import urlretrieve  # Python 3
        from urllib.error import HTTPError,ContentTooShortError
    except ImportError:
        from urllib import urlretrieve  # Python 2
    #import urllib.request


    try:
        from urllib.parse import urlparse  # Python 3
    except ImportError:
        from urlparse import urlparse  # Python 2

    from os.path import splitext, basename, join


    # Imports the Google Cloud client library
    from google.cloud import vision

    pdate=g_vdate #global variable

    #return
    list_label=[]

    # Instantiates a client
    vision_client = vision.Client()


    picture_page = photo_link
    disassembled = urlparse(picture_page)
    filename, file_ext = splitext(basename(disassembled.path))

    #Dev env
    #base_dir="/home/leth/Workspace/Python/python3/parse_csv/sources/"
    #Prod env
    # base_dir='E:\\VNG\\Python Envirement\\Data\\DWHVNG\\APEX\\MARKETING_TOOL_02_JSON\\'+pdate+"\\images"
    base_dir = join(path_base, pdate)
    base_dir=base_dir +"/images"

    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    fullfilename = join(base_dir, filename+file_ext)
    #fullfilename = join("resources", filename+file_ext)

    ### Nếu có ảnh thì không cần download
    if not os.path.exists( fullfilename ):
        #download
        print ("download ")
        try:
            urlretrieve(photo_link, fullfilename)
            # print (fullfilename)
        except KeyboardInterrupt as ki:
            raise ki

        except HTTPError as err:
            # Nếu không tồn tải ảnh với url thì xem như list_label = []
            list_label = []
            print(err.code)
        except ContentTooShortError as err:
            #retry 1 times
            print ("errors missing content")
            print (photo_link)
            try:
                urlretrieve(photo_link, fullfilename)
            except ContentTooShortError as err:
                # Không xử lý được except thì xem như list_label = []
                list_label = []
                print ("don't fix errors missing content")
                print (photo_link)
                print(err.code)
            except:
                print("Unknown Error try download")
        except:
            list_label = []
            print("Unknown Error try download")
            print (photo_link)
            #if err.code == 404:
                #<whatever>
            #else:
           #raise

    photo_file=fullfilename
    if os.path.exists( photo_file ):
        if (os.path.getsize(photo_file) >= (1024 * 1024 * 4)):
            import PIL
            from PIL import Image
            print ("scale iamge....")
            print (photo_link)
            img = Image.open(photo_file)
            basewidth = 1300
            wpercent = (basewidth / float(img.size[0]))
            hsize = int((float(img.size[1]) * float(wpercent)))
            img = img.resize((basewidth, hsize), PIL.Image.ANTIALIAS)
            img.save(photo_file)
        try:
            #print(photo_file)

            # The name of the image file to annotate
            #file_name = os.path.join(
            #    os.path.dirname(__file__),
            #    'resources/wakeupcat.jpg')

            # Loads the image into memory
            with io.open(fullfilename, 'rb') as image_file:
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
            print (photo_link)
    # list_label = []
    # ======== Add hash code for file
    if os.path.exists(fullfilename):
        import hashlib
        result = hashlib.md5(open(fullfilename,'rb').read()).hexdigest()
    print (list_label)
    return (list_label, result)


def get_labled_image_url(pdate, path_base):

    import os, os.path
    #from os.path import splitext, basename, join
    import io
    import json

    from datetime import datetime , timedelta, date

    try:
        from urllib.parse import urlparse  # Python 3
    except ImportError:
        from urlparse import urlparse  # Python 2

    #Dev env
    #base_dir="/home/leth/Workspace/Python/python3/parse_csv/sources/"
    #Prod env
    # base_dir='E:\\VNG\\Python Envirement\\Data\\DWHVNG\\APEX\\MARKETING_TOOL_02_JSON\\'
    base_dir = path_base


    delta=31
    vdate=datetime.strptime(pdate, '%Y-%m-%d').date()

    list_image_json = []
    json_count =0

    # de lui 30 ngay
    for i in range(int (delta)):
        #print( vdate - timedelta(i))
        single_date= vdate - timedelta(i)
        wrk_dir=os.path.join(base_dir, single_date.strftime('%Y-%m-%d'))
        image_url_file_name = "image_url_"+single_date.strftime('%Y-%m-%d')+".json"
        image_url_file= os.path.join(wrk_dir, image_url_file_name)
        # print(image_url_file)
        if os.path.exists( image_url_file ) and os.stat(image_url_file).st_size  > 0  :
            try:
                with open (image_url_file,'r') as file_json:
                    reader=json.load(file_json)
                    #v1 reader is list
                    #V2 reader is dict
                    #print(str(type(reader))

                    if isinstance(reader,list):
                        for row in reader:
                            row['labeled_date']=single_date.strftime('%Y-%m-%d')
                            list_image_json.append(row)
                            json_count+=1

                    else:#"<class 'dict'>":
                        for row in reader['my_json']:
                            row['labeled_date']=single_date.strftime('%Y-%m-%d')
                            list_image_json.append(row)
                            json_count+=1
                    # for row in list_image_json:
                    #     print (list_image_json)
                    # print ("==========================================================")



            except IOError as e:
                # you can print the error here, e.g.
                print(str(e))





    # de toi
    for i in range(int (delta)):
        #print( vdate + timedelta(i))
        single_date= vdate + timedelta(i+1) #prevent dup
        wrk_dir=os.path.join(base_dir, single_date.strftime('%Y-%m-%d'))
        image_url_file_name = "image_url_"+single_date.strftime('%Y-%m-%d')+".json"
        image_url_file= os.path.join(wrk_dir, image_url_file_name)
        #print(image_url_file)

        if os.path.exists( image_url_file ) and os.stat(image_url_file).st_size  > 0  :
            try:
                with open (image_url_file,'r') as file_json:
                    reader=json.load(file_json)

                    if isinstance(reader,list) :
                        for row in reader:
                            row['labeled_date']=single_date.strftime('%Y-%m-%d')
                            list_image_json.append(row)
                            json_count+=1

                    else:#"<class 'dict'>":
                        for row in reader['my_json']:
                            row['labeled_date']=single_date.strftime('%Y-%m-%d')
                            list_image_json.append(row)
                            json_count+=1

            except IOError as e:
                # you can print the error here, e.g.
                print(str(e))

    print("Image labeled in range: " + str(json_count))


    return list_image_json

def check_link_image_in_list_link(link, list_link):
    try:
        from urllib.parse import urlparse  # Python 3
    except ImportError:
        from urlparse import urlparse  # Python 2
    for l in list_link:
        disassembled1 = urlparse(link["image_url"])
        disassembled2 = urlparse(l["image_url"])

        #if image["image_url"] ==  j["image_url"]:
        if disassembled1.netloc == disassembled2.netloc and disassembled1.path == disassembled2.path  and disassembled1.params == disassembled2.params:
            return False
    return True

def label_ads_creatives_json_audit_content(pdate, path_base):

    import os, os.path
    #from os.path import splitext, basename, join
    import io
    import json

    try:
        from urllib.parse import urlparse  # Python 3
    except ImportError:
        from urlparse import urlparse  # Python 2

    #Dev env
    #base_dir="/home/leth/Workspace/Python/python3/parse_csv/sources/"
    #Prod env
    # base_dir='E:\\VNG\\Python Envirement\\Data\\DWHVNG\\APEX\\MARKETING_TOOL_02_JSON/'
    base_dir = path_base
    # base_dir="/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON/"
    wrk_dir=os.path.join(base_dir, pdate)
    ads_creatives_file_name = "ads_creatives_"+pdate+".json"
    image_url_file_name = "image_url_"+pdate+".json"
    image_url_file= os.path.join(wrk_dir, image_url_file_name)
    ads_creatives_audit_content_file_name = "ads_creatives_audit_content_"+pdate+".json"
    ads_creatives_audit_content_file= os.path.join(wrk_dir, ads_creatives_audit_content_file_name)

    list_file = []
    list_json = []
    list_image_json = []

    #audit content object
    list_audit_context = ['image_url','link','video_id','message']
    print (ads_creatives_audit_content_file)

    try:
        #get all data
        with open (ads_creatives_audit_content_file,'r') as file_json:
            reader=json.load(file_json)
            for row in reader['my_json']:
                list_json.append(row)
        # de han che so luong call api, cac url can duoc kiem tra da co chua
        # trong file image_url_lablel_yyyymmdd.json
        # phat sinh truong hop url chi khac nhau tham so query, nen se lay netloc + path de compare
        # de han che them, kiem tra truoc sau 30 ngay de lay data --> chay lan luot
        list_image_json = get_labled_image_url(pdate, path_base)

        print("Number json of day: " + str(len(list_json)))
        # list image label trong ngay hien tai duoc giu lai de ghi xuong lai
        # neu phat sinh se append vao
        list_image_json_today = []
        json_count =0
        for image in list_image_json:
            if image["labeled_date"] == pdate :
                image_url_json={
                             "image_url"    : image["image_url"]
                            ,"image_label"  : image["image_label"]
                            #,"labeled_date" : pdate
                            }
                #append
                list_image_json_today.append(image_url_json)
                json_count+=1
        print("Image labeled this day in range: " + str(json_count))

        # print (len(list_image_json_today))
        # for image in list_image_json_today:
        #     print (image)
        # print ("---------------------------------------------------")

        position_json=0
        for i in list_json:
            #print(i[0])

            #image_urls
            position_image=0
            #for j in i[0]['audit_content']['image_urls']:
            for j in i['audit_content']['image_urls']:
                #check exists
                exists = False
                x=0
                y=-1 #null_position
                image_label=[]
                flag = False
                for image in list_image_json:
                    #print(type(image))
                    #if image["image_url"] ==  i["image_url"] and image["image_label"] !="":
                    # phat sinh truong hop url chi khac nhau tham so query, nen se lay netloc + path + params de compare

                    disassembled1 = urlparse(image["image_url"])
                    disassembled2 = urlparse(j["image_url"])

                    #if image["image_url"] ==  j["image_url"]:
                    if disassembled1.netloc == disassembled2.netloc and disassembled1.path == disassembled2.path  and disassembled1.params == disassembled2.params :
                        exists = True
                        if len(image["image_label"])==0:
                            flag = True
                        y=x

                        image_label=image["image_label"]
                        break
                    x+=1
                #

                if exists == False:
                    #get label
                    image_label=label(j["image_url"], pdate, path_base)
                    #image_label="a"

                    #create dict
                    image_url_json={
                                     "image_url"    : j["image_url"]
                                    ,"image_label"  : image_label
                                    #,"labeled_date" : pdate
                                    }
                    #append
                    list_image_json.append(image_url_json)
                else:
                # exist = True
                    if flag :
                        # get value
                        image_label=label(j["image_url"], pdate, path_base)
                        image_url_json={
                                         "image_url"    : j["image_url"]
                                        ,"image_label"  : image_label
                                        #,"labeled_date" : pdate
                                        }
                    else:
                        image_url_json={
                                         "image_url"    : j["image_url"]
                                        ,"image_label"  : image_label
                                        #,"labeled_date" : pdate
                                        }

                    #update value
                    list_image_json[y]["image_label"]=image_label

                if len(image_url_json['image_label']) > 0:
                    if (check_link_image_in_list_link(image_url_json, list_image_json_today)):
                        list_image_json_today.append(image_url_json)

                #label(image_url)
                #list_json[position_json][0]['audit_content']['image_urls'][position_image]["image_label"]=image_label
                list_json[position_json]['audit_content']['image_urls'][position_image]["image_label"]=image_label

                position_image+=1

            position_json+=1
            #write imeediate to prevent error
            #print(list_json[0][0]['audit_content']['image_urls'])

            final_json_today={}
            final_json_today['my_json']=list_image_json_today
            with open (image_url_file,'w') as f:
                json.dump(final_json_today,f)

        final_json={}
        final_json['my_json']=list_json
        with open (ads_creatives_audit_content_file,'w') as f:
            json.dump(final_json,f)
    except (FileNotFoundError, IOError):
        print("Wrong file or file path")




# Lua chon chay tung ngay

def main(date_, to_date_):
    """Run a label request """
    from datetime import datetime , timedelta, date
    path = '/u01/oracle/oradata/APEX/MARKETING_TOOL_02'
    path_json ="/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON/"
    list_folder = next(os.walk(path))[1]
    # date_ = '2017-05-06'
    # to_date_ = '2017-06-29'
    date = datetime.strptime(date_, '%Y-%m-%d').date()
    to_date = datetime.strptime(to_date_, '%Y-%m-%d').date()
    for folder in list_folder:
        # ---------- Check cac folder khong phai date ------------------
        if folder[:4].isdigit():
            d = datetime.strptime(folder, '%Y-%m-%d').date()
            if d <= to_date and d >= date:
                try:
                    # parse_ads_creatives_csv_to_json(folder, path, path_json)
                    #analyze_ads_creatives_json(vdate)
                    # parse_ads_creatives_json_audit_content(folder, path_json)
                    label_ads_creatives_json_audit_content(folder, path_json)
                except:
                    print ("Date error: %s" %folder)

if __name__ == '__main__':
    from sys import argv
    script, start_date, end_date = argv
    main(start_date, end_date)
