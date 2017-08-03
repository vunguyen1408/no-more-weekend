Thư mục gồm 3 file:
+ caculator_crawler.py : Tính các thông tin label của file json, tạo file excel vào thư mục có tên tương ứng của product trong folder "Json_and_report"
+ get_content_crawler.py : Rút trích data từ các file json mà file get_image_in_folder_crawler.py tạo ra, dùng cho việc tính toán thông tin, và predict
+ get_image_in_folder_crawler.py : Quét thư mục crawler từ web, chọn các image, dùng API get label và lưu thông tin vào file json trong folder của thư mục cùng tên ở folder "Json_and_report"