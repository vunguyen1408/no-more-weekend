Run code in folder: 		Server : 10.60.1.17
/home/marketingtool/Workspace/Python/no-more-weekend/label_visualize/run_flow
Link git : https://github.com/japonikaa/no-more-weekend

Data:
Source	/u01/oracle/oradata/APEX/MARKETING_TOOL_02
  /u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON
  1.  Create data Json
From data :
/u01/oracle/oradata/APEX/MARKETING_TOOL_02
To :
/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON
python parse_json_audit_content.py 2017-10-01 2017-10-15

  2.  Mapping product cho các Ads
python mapping_product.py 2017-10-01 2017-10-15

3.  Audit image (Download + get lable image )
1	python get_image_lable.py 2016-10-01 2017-06-29
  python get_image_lable.py 2016-10-01 2017-06-29

2	Call self API


4. Audit video + audio (download video, convert audio, get text)
1	Create file video_url
  python create_json_video_url.py  2017-10-01 2017-10-15

2	Download video
  python download_video.py 2016-10-01 2017-06-29

3	Convert video to audio
  python video_to_audio.multiprocess.py 2016-10-01 2017-06-29

4	Dectect audio
  python detect_audio.py 2016-10-01 2017-06-29

5	Convert video to image
  python video_to_image.py 2016-10-01 2017-06-29

7	Dectect video image label
  python detect_video_image_text.py 2016-10-01 2017-06-29

8	Dectect video image text
  python detect_video_image_label.py 2016-10-01 2017-06-29


5. Audit (folder code no-more-weekend/label_visualize/run_flow /predict_ads_fb)
python predict.py 2016-10-01 2017-06-29

6. Insert data audit to database
python insert_audit_ads_to_data.py 2016-10-01 2017-06-29
