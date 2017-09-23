======================= campaign_mapping_plan ===================
Thực hiện mapping campaign từng account trên từng ngày với plan.
------------------------------------------------
- MapAccountWithCampaign (): Mapping một list campaign (đoc từ
Json của Account/ngày) với list plan. Thu được 1 list campaign 
và một list plan có lưu thông tin mapping.

- MapData(): Truy vấn list plan từ Oracle, đọc (truy vấn) list
Product Alias cùng list campaign của Account/ngày. Thực hiện 
mapping và ghi file kết quả cùng thư mục Json campaign của
Account/ngày.

- MergerDataAccount(): Thực hiện bước merge  data đã mapping từng ngày của các Account đến thư mục DATA_MAPPING theo từng ngày. 

- DataFinalDate(): Thực hiện bước merge data trong thư mục DATA_MAPPING ở từng ngày thành một file tổng hợp.

- MapWithDate(): Mapping data một ID AdWord trong nh ngày.

- DataFinal(): MThực hiện bước merge data trong thư mục DATA_MAPPING trong nhiều ngàythành một file tổng hợp