Thực hiện download và get label cho các ad:
PHẦN CODE CẦN CHO QUÁ TRÌNH CHẠY PREDICT
- create_json_video_url: Tạo file json "video_url" từ data json facebook. Nhằm lưu tất cả các video của ngày đó trong một file.
- download_video: Thực hiện detect ra link video, get link chứa file.mp4, down load file video về lưu vào folder videos của từng ngày
- detect_video_30_day: Thực hiện duyệt file "video_url" để get label video, sau đó update label video vào file ads_creatives_audit_content
