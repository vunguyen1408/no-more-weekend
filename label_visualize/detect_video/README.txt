Thư mục gồm 4 file:
+ create_json_video_url.py : Lưu thông tin các video url có video của từng ngày vào file video_urls.json
+ detect_video.py : Duyệt file json video_urls.json để get label, sau đó lưu lại vào file này. Rồi cập nhật lại vào data (ads_creatives_audit_content_*****)
+ download_video.py : Thực hiện việc down load video
+ frame_image_of_video.py : Cắt frame ảnh của một video