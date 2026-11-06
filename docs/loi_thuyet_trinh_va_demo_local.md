# Lời thuyết trình và hướng dẫn demo local

Bản dùng để đọc khi báo cáo, thay cho kịch bản review tổng thể trước đó. Trọng tâm: ingestion, Data Engineering, SQL và chạy Python từ máy local. Không trình bày Power BI, điều phối Lambda hoặc mart chi nhánh. Những dòng trong ngoặc vuông là hướng dẫn thao tác, không đọc thành lời.

## A. Lời thuyết trình hoàn chỉnh, khoảng 12–15 phút

### 1. Mở đầu và bài toán

Em chào thầy và các bạn. Hôm nay nhóm em xin trình bày dự án xây dựng hệ thống thu thập, xử lý và tổ chức dữ liệu ngân hàng phục vụ phân tích.

Trong một hệ thống ngân hàng, dữ liệu khách hàng, tài khoản, thẻ, giao dịch và khoản vay có quan hệ với nhau nhưng nằm ở nhiều bảng khác nhau. Để trả lời những câu hỏi như khách hàng nào có tổng chi tiêu cao nhất, merchant nào có nhiều giao dịch nhất, hay tổng giá trị giao dịch thay đổi theo từng tháng như thế nào, chúng ta cần một luồng xử lý thống nhất trước khi viết các truy vấn phân tích.

Vì vậy, mục tiêu của nhóm là xây dựng một pipeline batch: thu thập dữ liệu nguồn, lưu lại dữ liệu raw, chuẩn hóa và nạp vào staging, chuyển đổi thành warehouse, sau đó tổng hợp thành data marts. Trong buổi báo cáo này, nhóm em chạy các chương trình Python trực tiếp từ máy local và dùng SQL để minh chứng kết quả.

Ở đây, local là nơi thực thi chương trình Python. Chương trình vẫn đọc dữ liệu từ S3 và kết nối PostgreSQL theo cấu hình của dự án.

### 2. Dữ liệu đầu vào và kiến trúc

[Mở scripts/2_clean_load_staging.py, tìm TABLE_CONFIGS; có thể mở thêm sql/01_create_staging_tables.sql.]

Dữ liệu nhóm sử dụng là bộ dữ liệu ngân hàng tổng hợp từ Kaggle. Đây là dữ liệu mô phỏng phục vụ học tập. Bộ dữ liệu được tổ chức thành bảy nhóm: khách hàng, tài khoản, thẻ, merchant, chi nhánh, khoản vay và giao dịch.

Trong đó, customer_id liên kết khách hàng với tài khoản và khoản vay; account_id liên kết tài khoản với giao dịch và thẻ; merchant_id xác định đơn vị liên quan đến giao dịch.

Trước khi xây dựng các câu JOIN, nhóm xác định mức chi tiết của từng bảng. Ví dụ, một dòng trong bảng giao dịch biểu diễn một giao dịch, còn một dòng trong bảng khoản vay biểu diễn một hợp đồng vay. Việc xác định mức chi tiết giúp nhóm tránh cộng lặp số tiền khi kết hợp các quan hệ một-nhiều.

Luồng xử lý gồm bốn chặng chính: Kaggle đến S3 raw; S3 raw đến PostgreSQL staging; staging đến warehouse; cuối cùng là warehouse đến data marts. Python đảm nhiệm thu thập, đọc file và nạp dữ liệu; SQL đảm nhiệm các bước chuẩn hóa nghiệp vụ, ánh xạ khóa và tổng hợp.

### 3. Data ingestion: thu thập dữ liệu từ Kaggle

[Mở scripts/1_ingest_kaggle_to_s3.py:94, hàm download_kaggle_dataset.]

Phần đầu tiên nhóm muốn trình bày kỹ là data ingestion. Chương trình xác thực với Kaggle, tải dataset về một thư mục tạm và giải nén. Sau đó, hàm discover_source_files tìm các file CSV và SQLite trong thư mục dữ liệu.

[Chuyển đến dòng 114, hàm export_transactions_from_sqlite.]

Một tình huống chương trình có xử lý là dữ liệu giao dịch chưa có sẵn dưới dạng CSV nhưng được cung cấp trong SQLite. Khi đó, chương trình tìm bảng transactions và xuất dữ liệu sang CSV.

Điểm nhóm chú trọng là chương trình đọc bảng transactions theo từng chunk 100.000 dòng. Thay vì đưa toàn bộ bảng vào bộ nhớ cùng một lúc, chương trình xử lý từng phần và ghi lần lượt xuống file. Chunk đầu tiên ghi kèm header; các chunk tiếp theo được nối vào file đã tạo.

[Nhấn rõ: chunk 100.000 dòng là tại bước SQLite → CSV.]

Sau khi có các file nguồn, chương trình upload lên S3. Đường dẫn được tổ chức theo tên bảng và ngày ingestion, ví dụ raw/transactions/ingest_date=YYYY-MM-DD/transactions.csv. Ngày ingestion thể hiện ngày thu thập dữ liệu; nó khác với ngày phát sinh giao dịch nằm trong nội dung file.

[Chuyển đến dòng 144, hàm ingest_all_tables_to_s3, rồi dòng 195, hàm upload_manifest.]

Mỗi lần chạy được cấp một batch ID. Chương trình ghi một manifest chứa thông tin dataset, thời điểm chạy, đường dẫn file, số dòng, dung lượng, checksum SHA-256 và trạng thái xử lý. Manifest giúp nhóm truy lại những file đã được đưa vào raw trong một lần chạy.

Như vậy, đầu ra của ingestion là dữ liệu raw trên S3 cùng thông tin audit. Tầng raw tách việc thu thập khỏi việc biến đổi: sau khi đã có dữ liệu nguồn, các bước xử lý phía sau có thể đọc lại từ raw mà không cần tải lại Kaggle cho mỗi lần chạy.

### 4. Cleaning và nạp staging

[Mở scripts/2_clean_load_staging.py:107, hàm clean_dataframe.]

Sau ingestion, chương trình đọc dữ liệu từ S3 và nạp vào schema staging của PostgreSQL. Nhóm khai báo cấu hình từng bảng trong TABLE_CONFIGS, bao gồm tên bảng, danh sách cột, kiểu dữ liệu và các cột cần chuẩn hóa ngày tháng.

Ở bước cleaning, chương trình loại bỏ khoảng trắng thừa ở các cột chuỗi và chuyển các cột ngày, thời gian về dạng thống nhất trước khi nạp. Staging đóng vai trò tiếp nhận dữ liệu để các bước SQL phía sau có đầu vào rõ ràng và có thể kiểm tra bằng truy vấn.

[Chuyển đến dòng 127, hàm copy_df_to_postgres, rồi dòng 175.]

Với transactions, chương trình tiếp tục đọc CSV theo chunk, ở bước này là 250.000 dòng mỗi chunk. Mỗi chunk sau khi xử lý được chuyển thành buffer trong bộ nhớ và nạp bằng COPY FROM STDIN của PostgreSQL.

Nhóm lựa chọn COPY vì đây là thao tác nạp theo lô. Thay vì gửi một câu INSERT cho mỗi bản ghi, chương trình truyền dữ liệu của cả chunk vào database. Điểm chính ở đây là kết hợp chunk để kiểm soát lượng dữ liệu xử lý trong bộ nhớ và COPY để thực hiện bulk load.

Hai kích thước chunk thuộc hai công đoạn khác nhau: 100.000 dòng ở bước xuất SQLite, và 250.000 dòng ở bước đọc CSV để nạp staging.

Sau khi nạp, chương trình truy vấn COUNT để hiển thị số bản ghi trong các bảng staging. Nhóm dùng các số thực tế này để kiểm tra kết quả từng lần chạy.

### 5. Xây dựng warehouse và ánh xạ khóa

[Mở sql/02_create_warehouse_tables.sql:24.]

Tiếp theo, nhóm chuyển dữ liệu staging thành warehouse theo mô hình chiều. Warehouse gồm sáu dimension là ngày, khách hàng, tài khoản, thẻ, merchant và chi nhánh; hai fact là fact_transactions và fact_loans.

Dimension chứa thông tin mô tả; fact chứa sự kiện và số đo. Ví dụ, fact_transactions chứa số tiền và thời gian của từng giao dịch, cùng các khóa liên kết đến dimension để phân tích theo khách hàng, tài khoản hoặc merchant.

Trong dimension, nhóm giữ ID nguồn như customer_id để tra cứu và tạo khóa nội bộ như customer_key để fact tham chiếu. Vì vậy, thứ tự xử lý là nạp dimension trước, sau đó mới nạp fact.

[Mở sql/03_insert_dimensions.sql:51, CTE validated_customers.]

Ở bước nạp dimension, SQL tiếp tục thực hiện các quy tắc chuẩn hóa nghiệp vụ như chuẩn hóa tên, chuyển email về chữ thường, kiểm tra cấu trúc email và kiểm tra miền điểm tín dụng. Những quy tắc này được viết tập trung trong SQL để có thể đọc và kiểm tra trực tiếp.

[Mở sql/04_insert_facts.sql:6.]

Đối với fact_transactions, câu SQL được chia thành các CTE. CTE đầu tiên chọn các giao dịch đáp ứng điều kiện hiện tại của dự án, chẳng hạn có các ID bắt buộc, có ngày giao dịch và amount lớn hơn 0. Các CTE tiếp theo tra cứu khóa dimension và bổ sung thông tin trước khi INSERT vào fact.

Khi một ID có trong giao dịch nhưng chưa tra được dimension tương ứng, code dùng LEFT JOIN và COALESCE để chuyển về bản ghi UNKNOWN. Mục đích là phân biệt giao dịch chưa ánh xạ được với giao dịch đã ánh xạ đầy đủ, đồng thời duy trì tham chiếu đến dimension.

Trong phiên bản chạy local, warehouse được làm mới và nạp lại từ staging. Đây là cơ chế full refresh, phù hợp với phạm vi xử lý batch mà nhóm đang trình bày.

### 6. Tổng hợp data marts bằng SQL

[Mở sql/06_create_data_marts.sql:14, phần customer_360. Không mở phần branch mart trong flow chính.]

Sau warehouse, nhóm tạo các bảng tổng hợp để trả lời câu hỏi nghiệp vụ thuận tiện hơn. Ví dụ chính là Customer 360: kết hợp thông tin khách hàng với số tài khoản, tổng số dư, số giao dịch, tổng chi tiêu và thông tin khoản vay.

Điểm quan trọng trong câu SQL này là nhóm không JOIN trực tiếp toàn bộ dữ liệu chi tiết của tài khoản, giao dịch và khoản vay. Nhóm tổng hợp từng nhánh về một dòng cho mỗi khách hàng, sau đó mới JOIN các kết quả lại.

Ví dụ, một khách hàng có hai giao dịch và ba khoản vay. Nếu JOIN trực tiếp hai bảng chi tiết theo khách hàng, có thể tạo sáu dòng, khiến số tiền giao dịch bị cộng nhiều lần. Việc aggregate từng nhánh trước JOIN giúp tránh hiện tượng nhân dòng này.

[Chỉ ba CTE customer_accounts, customer_spending, customer_loans và các LEFT JOIN cuối câu.]

Ngoài ra, merchant analytics tổng hợp số lượng giao dịch, tổng volume và giá trị giao dịch trung bình theo merchant. Phần tổng hợp khoản vay cho phép xem phân bố giá trị hợp đồng theo các nhóm điểm tín dụng.

Khi trình bày khoản vay, nhóm sử dụng khái niệm tổng giá trị khoản vay trong dữ liệu. Để xác định dư nợ thực tế còn lại cần thêm thông tin hoàn trả, vốn chưa nằm trong phạm vi nguồn hiện tại.

### 7. Kiểm tra và truy vấn minh chứng

[Mở tests/test_data_quality.py, chỉ tên các hàm test, sau đó chuyển Query Tool.]

Bên cạnh tạo bảng, nhóm xây dựng các phép kiểm tra về số lượng bản ghi, tính duy nhất của khóa, quan hệ fact-dimension, miền giá trị và đối soát dữ liệu tổng hợp.

Ví dụ, tổng số giao dịch ở merchant mart cần khớp với số giao dịch trong fact. Nhóm cũng có thể viết query đối soát tổng tiền giữa hai tầng. Cách kiểm tra này giúp xác định phép tổng hợp có làm mất hoặc nhân bản giao dịch hay không.

Để minh chứng kết quả, em sẽ chạy ba loại truy vấn: xem quy mô dữ liệu ở từng tầng, truy vết một giao dịch sau khi liên kết dimension và lấy top khách hàng hoặc merchant theo chỉ tiêu cụ thể.

[Chạy Q01, Q02, Q04 của sql/07_presentation_readonly_queries.sql hoặc các query trong phần B bên dưới.]

### 8. Kết thúc

Qua dự án, nhóm em đã thực hiện luồng xử lý từ thu thập dữ liệu nguồn đến chuẩn hóa, nạp staging, xây dựng warehouse và tổng hợp data marts. Các kỹ thuật chính gồm đọc dữ liệu theo chunk, bulk load bằng COPY, ánh xạ khóa dimension, tổng hợp đúng mức chi tiết và đối soát bằng SQL.

Phần em xin nhấn mạnh là cách tổ chức luồng dữ liệu và kiểm soát tính đúng của các phép tổng hợp. Từ dữ liệu sau xử lý, nhóm có thể trả lời các câu hỏi như top khách hàng theo chi tiêu, top merchant theo volume và xu hướng giao dịch theo thời gian. Em xin kết thúc phần trình bày và sẵn sàng chạy demo hoặc thực hiện các truy vấn theo yêu cầu của thầy.

## B. Khi thầy yêu cầu chạy demo trên local

### 1. Câu trả lời mở đầu

“Dạ được thầy. Em sẽ chạy các script Python trực tiếp trên máy theo thứ tự ingestion, staging, warehouse và data marts. Chương trình vẫn sử dụng nguồn raw trên S3 và PostgreSQL đã cấu hình. Sau mỗi bước, em sẽ chỉ kết quả nạp và chạy SQL kiểm tra.”

### 2. Chuẩn bị trước buổi báo cáo

- Dùng database demo đã khởi tạo bảng và dim_date, được phép làm mới dữ liệu. Các script staging/warehouse/marts có thao tác truncate hoặc drop/recreate. Không chạy lại trên database có dữ liệu cần giữ.
- `.env` của dự án cần có thông tin Kaggle, bucket/region, thông tin kết nối PostgreSQL và quyền AWS phù hợp. Không hiển thị nội dung `.env` khi chia sẻ màn hình.
- Chuẩn bị kết nối PostgreSQL trong pgAdmin hoặc DBeaver. Trước khi demo, kiểm tra cả SQL client và Python cùng trỏ đúng database demo.
- Có mạng để truy cập Kaggle, S3 và database nếu database ở xa. Phiên bản hiện tại không có chế độ offline hoàn toàn.
- Chạy thử toàn bộ quy trình trước buổi bảo vệ. Thời gian tải, nạp và build mart phụ thuộc mạng/DB; không hứa số phút khi chưa đo.
- Chuẩn bị môi trường trước buổi báo cáo. Các pin trùng/xung đột đã được loại khỏi requirements.txt; việc cài mới toàn bộ dependencies chưa được kiểm chứng. Dùng venv đã chuẩn bị và thử thành công.

### 3. Mở PowerShell ở thư mục gốc

```powershell
Set-Location 'D:\dwh\Cloud-Native-Banking-Analytics-Platform'
Test-Path .\venv\Scripts\python.exe
Test-Path .\.env
.\venv\Scripts\python.exe -c "import pandas, psycopg2, boto3, dotenv; print('Core imports OK')"
```

Hai Test-Path cần trả True; lệnh cuối cần in Core imports OK. Lệnh import không kiểm tra đăng nhập Kaggle, quyền S3, kết nối DB hoặc bảng đã khởi tạo. Gọi thẳng Python trong venv nên không cần Activate.ps1.

### 4. Demo ingestion

Nói: “Đầu tiên em chạy bước thu thập. Script sẽ tải dataset, xử lý CSV hoặc SQLite rồi đưa dữ liệu raw lên S3.”

```powershell
.\venv\Scripts\python.exe scripts\1_ingest_kaggle_to_s3.py
```

Chờ lệnh kết thúc. Chỉ tiếp tục nếu thành công. Có thể kiểm tra mã thoát ngay sau từng script:

```powershell
$LASTEXITCODE
```

Mã 0 nghĩa tiến trình thoát bình thường; vẫn đọc log và kiểm tra đầu ra. Chỉ các script có raise/exit phù hợp mới phản ánh lỗi bằng mã thoát; không suy rộng sang init_db.py.

Chỉ trên log: batch ID, ingest date theo UTC, đường dẫn upload, số dòng mỗi bảng và trạng thái manifest cuối. Không nói SQLite fallback đã xảy ra nếu lần chạy thực tế có transactions.csv sẵn.

Nếu nguồn raw đã chuẩn bị và thầy chỉ muốn xem biến đổi, nói rõ: “Dữ liệu raw đã được nhóm thu thập trước, em sẽ demo từ bước đọc raw và nạp staging.” Khi đó bỏ bước download, không nhận là vừa demo download thành công.

### 5. Demo staging

Chọn ngày partition đã upload thành công, ghi vào biến môi trường để cố định đầu vào. Ví dụ dưới đây chỉ là cú pháp, thay YYYY-MM-DD bằng ngày thật trong log; không chạy nguyên placeholder.

```powershell
$env:INGEST_DATE = 'YYYY-MM-DD'
.\venv\Scripts\python.exe scripts\2_clean_load_staging.py
```

Không đặt biến thì code dùng INGEST_DATE trong cấu hình hoặc tự tìm ngày raw mới nhất từ branches. Để demo có thể lặp lại, nên chọn rõ ngày đã kiểm tra.

Nói trước khi chạy: “Bước này đọc dữ liệu raw, chuẩn hóa các cột và dùng COPY để nạp staging. Transactions được xử lý từng chunk 250.000 dòng.”

Trong lúc chạy, chỉ log số dòng tích lũy theo chunk. Khi kết thúc, mở SQL client:

```sql
SELECT COUNT(*) AS staging_transactions
FROM staging.stg_transactions;
```

Nói: “Đây là số giao dịch thực tế đã nạp vào staging trong lần demo.”

### 6. Demo warehouse

```powershell
.\venv\Scripts\python.exe scripts\3_build_warehouse.py
```

Nói trước khi chạy: “Em chuyển staging sang mô hình chiều. Script nạp các dimension nghiệp vụ trước, sau đó lookup khóa để nạp hai bảng fact. Dimension ngày đã được chuẩn bị khi khởi tạo.”

Khi lệnh thành công, chạy:

```sql
SELECT COUNT(*) AS fact_transactions,
       SUM(amount_usd) AS total_amount_usd
FROM warehouse.fact_transactions;
```

Sau đó truy vết:

```sql
SELECT t.transaction_id, t.amount_usd, t.transaction_date,
       c.customer_id, c.full_name,
       a.account_id, m.merchant_id, m.merchant_name
FROM warehouse.fact_transactions t
LEFT JOIN warehouse.dim_customer c ON c.customer_key = t.customer_key
LEFT JOIN warehouse.dim_account a ON a.account_key = t.account_key
LEFT JOIN warehouse.dim_merchant m ON m.merchant_key = t.merchant_key
ORDER BY t.transaction_key
LIMIT 5;
```

Nói: “Kết quả này thể hiện một giao dịch ở fact đã liên kết được với thông tin từ các dimension.” Nếu có UNKNOWN, giải thích đúng là chưa ánh xạ được; không gọi đó là lỗi mất giao dịch hoặc mặc định là khách ngoài hệ thống.

### 7. Demo data marts

```powershell
.\venv\Scripts\python.exe scripts\4_build_marts.py
```

Script gốc hiện tạo cả bốn mart. Phần trình bày và query nghiệp vụ chỉ chọn Customer 360, merchant analytics và phân bố khoản vay; không dùng mart chi nhánh làm bằng chứng kết quả đúng.

Nói: “Tiếp theo em tổng hợp dữ liệu để trả lời câu hỏi nghiệp vụ. Ví dụ, em lấy năm khách hàng có tổng chi tiêu cao nhất.”

```sql
SELECT customer_id, full_name, total_transactions, total_spend_usd
FROM mart.customer_360
ORDER BY total_spend_usd DESC, customer_id
LIMIT 5;
```

Nói tiếp: “Với merchant, em có thể lấy năm đơn vị có tổng giá trị giao dịch cao nhất.”

```sql
SELECT merchant_id, merchant_name,
       total_transactions, total_sales_volume_usd
FROM mart.merchant_analytics
WHERE merchant_id <> 'UNKNOWN'
ORDER BY total_sales_volume_usd DESC, merchant_id
LIMIT 5;
```

Chốt: “Như vậy, dữ liệu đã đi từ raw qua staging, warehouse và bảng tổng hợp. Các kết quả top 5 được tính trực tiếp từ dữ liệu sau xử lý.”

### 8. Kiểm tra chất lượng nếu thầy yêu cầu

```powershell
.\venv\Scripts\python.exe tests\test_data_quality.py
```

Đây là bộ kiểm tra DB hiện có. Có thể chạy độc lập để kiểm tra dữ liệu đã chuẩn bị mà không nạp lại. Đọc đúng số PASSED/FAILED thực tế; không hứa chắc 7/7 trước khi chạy thử.

Unit cleaning là lệnh khác:

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_clean_logic.py -q -s -p no:cacheprovider
```

Lỗi ngày hỗn hợp và import do đổi tên script đã được sửa. Bộ unit test hiện tại có 9 ca cleaning và 5 ca điều phối runner với các bước giả lập; đã chạy đạt 14/14. Chạy lại bằng lệnh bên dưới. Đây chưa phải kết quả bộ kiểm tra database hoặc bằng chứng pipeline chạy end-to-end.

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_clean_logic.py tests\test_pipeline_runner.py -q
```

Ngày mơ hồ như 03/04/2023 mặc định hiểu theo ngày/tháng; cấu hình `dayfirst=False` nếu nguồn sử dụng tháng/ngày. Ngày ISO được parse riêng để tránh đảo tháng/ngày. Ngày không hợp lệ vẫn chuyển thành giá trị thiếu.

## C. Các lệnh không dùng làm đường demo chính

`scripts/run_pipeline_e2e.py` thực sự chạy local nhưng có thêm notifier SNS, không bao gồm tải Kaggle. Muốn trình bày đúng phạm vi hiện tại thì dùng từng script như phần B, không cần dùng runner tổng.

`scripts/init_db.py` chỉ dành cho chuẩn bị một database demo mới/rỗng: nó chạy DDL có DROP SCHEMA warehouse CASCADE, nên không dùng như bước bắt đầu mỗi buổi demo. Database PostgreSQL phải được tạo và cấu hình trước; script tạo schema/bảng và dim_date, không tạo server/database. Chưa kiểm chứng script khởi tạo chạy thành công trong phiên này.

Lệnh chỉ dùng một lần khi đã chọn đúng database demo được phép khởi tạo:

```powershell
.\venv\Scripts\python.exe scripts\init_db.py
```

`scripts/demo_incremental_batch.py` tự sinh và insert trực tiếp vào fact. Nó không minh chứng ingestion từ Kaggle qua S3/staging nên không sử dụng cho flow này.

## D. Nếu chỉ có 3–5 phút demo

Nếu thầy muốn chạy pipeline thật: dùng raw đã chuẩn bị, chạy staging → warehouse → marts; thời gian hoàn tất cần đo trước và có thể vượt thời lượng. Không thay bằng query rồi nhận là đã chạy lại pipeline.

Nếu thầy muốn xem kết quả local: nói rõ dùng dữ liệu đã xử lý trước, chạy lần lượt COUNT ở staging/fact, query truy vết, top 5 khách hàng và top 5 merchant. Đây là demo query đầu ra, không phải demo chạy mới ingestion.

## E. Khi demo gặp lỗi

- AccessDenied/không đăng nhập được Kaggle: dừng bước ingestion, kiểm tra credential/quyền ngoài màn hình trình chiếu. Chỉ chuyển sang raw cũ nếu raw thật có sẵn và nói rõ sử dụng batch đã chuẩn bị.
- NoSuchKey: kiểm tra INGEST_DATE và đường dẫn raw của cả các bảng. Không sửa ngày giao dịch để chữa ngày partition.
- Timeout/connection refused: kiểm tra kết nối DB, mạng và database demo; import thư viện thành công không chứng minh DB hoạt động.
- Thiếu relation hoặc dim_date: kiểm tra bước chuẩn bị schema. Không chạy init_db.py vội trên database hiện tại.
- Script có exception: dừng, đọc bước lỗi; không tiếp tục mart rồi nhận dữ liệu là batch mới hoàn chỉnh.

## F. Câu hỏi thực tế có thể nối tiếp demo

| Thầy yêu cầu | Cách trả lời/thao tác |
|---|---|
| Top 5 ngân hàng? | Nguồn mô phỏng một hệ thống, không có danh mục nhiều ngân hàng. Có thể làm top customer/merchant/account. |
| Không dùng mart, lấy top 5 customer từ fact? | JOIN fact_transactions với dim_customer, GROUP BY customer, SUM amount rồi ORDER BY DESC LIMIT 5. |
| Top theo số lần thay vì số tiền? | Đổi SUM(amount_usd) thành COUNT(*) khi query trực tiếp fact, hoặc dùng total_transactions ở mart. |
| Lấy riêng một tháng? | WHERE timestamp >= đầu tháng AND timestamp < đầu tháng sau, rồi aggregate. |
| Lấy top 3 merchant mỗi tháng? | Aggregate tháng/merchant, ROW_NUMBER PARTITION BY tháng, lọc rn <= 3. |
| Mỗi khách có bao nhiêu account? | GROUP BY customer_id trong staging accounts; COUNT DISTINCT account_id. |
| Khách chưa giao dịch? | Từ dim_customer dùng NOT EXISTS trên fact_transactions. |
| Vì sao dùng HAVING? | Lọc nhóm sau aggregate, ví dụ khách có ít nhất hai account. |
| Làm sao biết query không cộng trùng? | Giải thích grain và aggregate trước JOIN, rồi đối soát count/tổng tiền. |
| Có dữ liệu thời gian thực không? | Phiên bản trình bày là batch/full refresh. |

## G. Ghi nhận kiểm chứng tài liệu

Đã đọc lại script nguồn, kiểm tra các thư viện pandas/psycopg2/boto3/dotenv import được trong venv hiện có. Không chạy download, upload, truncate, build warehouse/marts hoặc gửi SNS trong phiên soạn tài liệu. Các lệnh demo được đối chiếu với entrypoint source, chưa được xác nhận chạy end-to-end trên dịch vụ thật.
