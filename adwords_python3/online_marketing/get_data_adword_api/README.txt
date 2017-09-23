============================ get_account ====================================
Gọi API AdWord, lấy danh sách các Account (name, id) của một MCC Account 
(setting ở file googleads.yaml)
--------------------------------------------------
- GetAllAcount() : Gọi API lấy nội dung danh sách các Account. Pasre 
cấu trúc lấy về được thành dạng Account cây. Gọi hàm
SaveAccountTree().
- SaveAccountTree(): Chuyển từ dạng cây sang list Account.
--------------------------------------------------
Kết quả: file.json chứa danh sách các Account (thông tin một Account 
gồm Name, Id, các Account con).


============================ download_report =================================
Gọi API AdWord, download report trên ngày cho một ID Account. Parse data sang 
Json và lưu.
-----------------------------------------------------------
- TSVtoJson() : Parse data download về sang định dạng Json
- DownloadCampaignOfCustomer() : Download campaign của ID Account 
- DownloadAdGroupOfCampaign() : Download AdGroup của ID Account 
- DownloadAdsOfAdGroup() : Download Ads của ID Account 
- DownloadOnDate(): Download Campaign, AdGroup, Ads của Account/
ngày
- GetCampainForAccount(): Download Campaign, AdGroup, Ads của Account/
nhiều ngày
-----------------------------------------------------------
Kết quả: Download các file report của Campaign, AdGroup, Ads của một 
Account, parse thành json, lưu lại.