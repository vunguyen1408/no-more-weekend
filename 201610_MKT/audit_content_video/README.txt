Link git : https://github.com/japonikaa/no-more-weekend
Run code in folder: 		Server : 10.60.1.17
/home/marketingtool/Workspace/Python/no-more-weekend/201610_MKT/audit_content_video

Data Source	/u01/oracle/oradata/APEX/MARKETING_TOOL_02

#Download
python create_json_video_url.py  2017-06-01 2017-06-01
python download_video.py 2017-06-01 2017-06-01

#Audio
python video_to_audio_wav.mp.py "/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON" 2017-06-01 2017-06-01 4
python detect_video_audio_text.mp.py "/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON" 2017-06-01 2017-06-01 4

#Image
python video_to_image.mp.py "/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON" 2017-06-01 2017-06-01 4
python detect_video_image_text.mp.py "/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON" 2017-06-01 2017-06-01 4
python detect_video_image_local_label.mp.py "/u01/oracle/oradata/APEX/MARKETING_TOOL_02_JSON" 2017-06-01 2017-06-01 4
