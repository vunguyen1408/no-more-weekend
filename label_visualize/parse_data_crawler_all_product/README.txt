 Thực hiện parse data crawler:
 PHẦN CODE CẦN CHO QUÁ TRÌNH CHẠY PREDICT
 - mapping_folder_crawler: file chứa thông tin mapping thư mục với product_id
 - get_image_in_folder_crawler, create_video: Tổng hợp thông tin các ảnh (file name, image label, video label) thành 1 file product_id.json


 PHẦN CODE ĐỂ QUAN SÁT XÂY DỰNG MODEL
 - get_content_crawler, caculator_crawler: đọc từ file product_id.json để thông kê theo image
- get_content_video, caculator_video: đọc từ file product_id.json để thông kê theo video
