# BÁO CÁO THỰC HÀNH HADOOP ONLINE
## PHÂN TÍCH ĐƠN HÀNG THƯƠNG MẠI ĐIỆN TỬ VỚI HDFS VÀ MAPREDUCE STREAMING

---

### THÔNG TIN BÀI LÀM
- **Môn học:** Dữ liệu lớn (Big Data)
- **Họ và tên:** Nguyễn Đức Duy
- **MSSV:** A48496
- **Chủ đề:** Chapter 2 - Hadoop Ecosystem (HDFS, MapReduce, YARN)
- **Tập dữ liệu:** `orders_2026_09.csv` (1,500 giao dịch đơn hàng tháng 09/2026)
- **Quy ước nộp bài:** `A48496_NguyenDucDuy_HadoopLab`

---

## 1. MỤC TIÊU VÀ PHẠM VI THỰC HÀNH
1. **Mục tiêu kỹ thuật:**
   - Nắm vững quy trình cấu hình và làm việc với hệ thống tệp phân tán HDFS (`mkdir`, `put`, `ls`, `head`, `cat`, `get`).
   - Hiểu rõ luồng xử lý dữ liệu của MapReduce: Input Split -> Mapper -> Shuffle & Sort -> Reducer -> Output.
   - Viết chương trình Mapper và Reducer bằng Python và thực thi thông qua Hadoop Streaming API.
2. **Mục tiêu nghiệp vụ:**
   - Phân tích dữ liệu đơn hàng thương mại điện tử để tìm ra nhóm sản phẩm mang lại doanh thu cao nhất (chỉ tính giao dịch thành công - `SUCCESS`).
   - Đưa ra nhận định kinh doanh giúp tối ưu chiến lược tồn kho, phân bổ kênh bán hàng và giao nhận.

---

## 2. TRẢ LỜI CÁC CÂU HỎI TRỌNG TÂM CỦA BÀI THỰC HÀNH

### 2.1. Mô tả dữ liệu đầu vào
- Tập dữ liệu `orders_2026_09.csv` có dung lượng xấp xỉ 118 KB gồm **1,500 bản ghi** giao dịch mua hàng.
- Mỗi bản ghi gồm 11 trường thông tin:
  1. `transaction_id`: Mã giao dịch duy nhất (VD: `TX000001`).
  2. `order_date`: Ngày đặt hàng (trong tháng 09/2026).
  3. `order_time`: Thời điểm đặt hàng.
  4. `province`: Tỉnh/thành phố nơi nhận hàng (Hà Nội, TP.HCM, Đà Nẵng, Cần Thơ,...).
  5. `category`: Nhóm sản phẩm (Electronics, Home, Fashion, Sport, Beauty, Toy, Grocery, Book).
  6. `channel`: Kênh đặt hàng (`web`, `mobile`, `partner`).
  7. `quantity`: Số lượng mặt hàng trong đơn.
  8. `unit_price`: Đơn giá mặt hàng (VND).
  9. `amount`: Tổng số tiền thanh toán của giao dịch (VND).
  10. `status`: Trạng thái xử lý giao dịch (`SUCCESS`, `FAILED`, `RETURNED`).
  11. `shipping_days`: Thời gian giao hàng tính theo ngày.
- **Thống kê trạng thái giao dịch:**
  - `SUCCESS`: **1,240** giao dịch (chiếm 82.67%).
  - `FAILED`: **142** giao dịch (chiếm 9.47%).
  - `RETURNED`: **118** giao dịch (chiếm 7.87%).

### 2.2. Cơ chế Mapper phát sinh cặp Key – Value
- Mapper (`mapper_revenue.py`) đọc từng dòng dữ liệu từ luồng vào chuẩn (`sys.stdin`).
- Mapper kiểm tra trường `status`. Nếu `status == 'SUCCESS'`, Mapper trích xuất hai trường:
  - **Key:** `category` (tên ngành hàng).
  - **Value:** `amount` (doanh thu tương ứng của đơn hàng).
- Mapper in ra luồng chuẩn (`sys.stdout`) theo cấu trúc ngăn cách bởi ký tự Tab: `<category>\t<amount>`.
- Các giao dịch có trạng thái `FAILED` hoặc `RETURNED` sẽ bị loại bỏ hoàn toàn, không đưa vào luồng tính doanh thu.

### 2.3. Cơ chế Shuffle / Sort và Reducer cộng dồn doanh thu
- **Giai đoạn Shuffle & Sort của Hadoop:** 
  Hệ thống Hadoop tự động thu thập toàn bộ các cặp `<key, value>` từ các node chạy Mapper, phân vùng (partition) và sắp xếp (sort) theo thứ tự từ điển của `key`. Nhờ đó, tất cả các bản ghi có cùng `category` sẽ được gom lại liền kề nhau và đưa tới cùng một Reducer.
- **Giai đoạn Reducer (`reducer_revenue.py`):**
  Reducer đọc dữ liệu tuần tự theo từng dòng từ `sys.stdin`. Nhờ dữ liệu đã được sắp xếp trước, Reducer duy trì biến `current_category` và biến tổng `current_total`. Mỗi khi gặp một bản ghi có cùng category, Reducer cộng dồn `amount` vào biến tổng; khi gặp một category mới, Reducer xuất kết quả của category trước đó `<current_category>\t<current_total>` rồi khởi tạo lại biến tổng cho category mới.

### 2.4. Bảng kết quả Top 3 nhóm sản phẩm và nhận xét kinh doanh
Dưới đây là kết quả trích xuất doanh thu các ngành hàng đạt trạng thái `SUCCESS`:

| Hạng | Ngành hàng (Category) | Số lượng đơn hàng | Doanh thu (VND) | Tỷ trọng doanh thu |
| :---: | :--- | :---: | :---: | :---: |
| **1** | **Electronics** | **296** | **3,703,769,311** | **76.94%** |
| **2** | **Home** | **110** | **410,315,065** | **8.52%** |
| **3** | **Fashion** | **233** | **218,597,763** | **4.54%** |
| 4 | Sport | 78 | 166,194,354 | 3.45% |
| 5 | Beauty | 119 | 111,032,003 | 2.31% |
| 6 | Toy | 74 | 83,881,779 | 1.74% |
| 7 | Grocery | 229 | 80,983,147 | 1.68% |
| 8 | Book | 101 | 39,306,022 | 0.82% |
| **Tổng** | **Toàn bộ ngành hàng** | **1,240** | **4,814,079,444** | **100.00%** |

#### Nhận xét kinh doanh (5 – 7 dòng):
1. **Ngành hàng Điện tử (Electronics) là trụ cột doanh thu vượt trội**, chiếm tới gần 77% tổng doanh thu toàn sàn dù số lượng đơn hàng (296 đơn) chỉ đứng thứ nhất sít sao so với Fashion và Grocery. Điều này phản ánh giá trị trung bình trên mỗi đơn hàng (AOV) của đồ điện tử là rất cao (khoảng 12.5 triệu VND/đơn). Do đó, doanh nghiệp cần ưu tiên hàng đầu về bảo quản kho bãi, chính sách bảo hành chính hãng và bảo hiểm hàng hóa khi vận chuyển.
2. **Ngành hàng Gia dụng (Home) xếp thứ 2 về doanh thu** (410 triệu VND) dù số lượng đơn hàng tương đối khiêm tốn (110 đơn), chứng tỏ sức mua các sản phẩm nội thất/tiện ích gia đình có tiềm năng sinh lời rất lớn.
3. **Ngành hàng Thời trang (Fashion) đứng thứ 3** với 218.6 triệu VND và số lượng đơn hàng lớn (233 đơn), cho thấy đây là nhóm hàng tiêu dùng thiết yếu với tần suất đặt mua cao. Doanh nghiệp nên đẩy mạnh các chương trình bán chéo (cross-selling) giữa Fashion và Beauty để gia tăng kích thước giỏ hàng.
4. **Các ngành hàng tiêu dùng nhanh (Grocery) và Sách (Book)** có đơn giá thấp nên tỷ trọng doanh thu nhỏ (< 2%), đóng vai trò là sản phẩm dẫn lưu lượng người dùng (traffic) vào sàn.

### 2.5. Hạn chế của mô hình xử lý Batch (Xử lý theo lô)
- **Độ trễ cao (High Latency):** MapReduce được thiết kế để xử lý lượng dữ liệu khổng lồ theo từng mẻ định kỳ (theo giờ, theo ngày, hoặc theo tháng). Toàn bộ dữ liệu của chu kỳ phải được tập hợp đầy đủ trước khi thực thi job, dẫn đến việc báo cáo không phản ánh được tình trạng doanh thu tức thời (real-time).
- **Chi phí I/O lớn:** Kết quả trung gian giữa các Mapper được ghi xuống đĩa cục bộ trước khi truyền qua mạng và nạp vào Reducer, khiến tốc độ xử lý chậm hơn nhiều so với các engine tính toán trong bộ nhớ (in-memory) như Apache Spark.
- **Giải pháp liên hệ:** Đối với các bài toán cần phát hiện gian lận thanh toán hoặc điều chỉnh giá bán linh hoạt theo phút, hệ thống cần kết hợp kiến trúc Lambda/Kappa với các công nghệ Stream Processing như Apache Kafka, Apache Flink hoặc Spark Streaming.

---

## 3. MINH CHỨNG THỰC THI KỸ THUẬT VÀ LOG LỆNH

### Bước 1: Khởi động hệ thống tệp HDFS
```bash
hdfs --daemon start namenode
hdfs --daemon start datanode
jps
```
**Kết quả hiển thị:**
```text
3124 NameNode
3250 DataNode
3412 Jps
```

### Bước 2: Tạo thư mục và đưa dữ liệu lên HDFS
```bash
hadoop fs -mkdir -p /user/student/ecommerce/input
hadoop fs -mkdir -p /user/student/ecommerce/output
hadoop fs -put -f orders_2026_09.csv /user/student/ecommerce/input/
hadoop fs -ls /user/student/ecommerce/input
```
**Kết quả hiển thị:**
```text
Found 1 items
-rw-r--r--   1 student supergroup     118934 2026-10-14 05:12 /user/student/ecommerce/input/orders_2026_09.csv
```

### Bước 3: Kiểm tra nội dung đầu tệp trên HDFS
```bash
hadoop fs -head /user/student/ecommerce/input/orders_2026_09.csv
```
**Kết quả hiển thị:**
```text
transaction_id,order_date,order_time,province,category,channel,quantity,unit_price,amount,status,shipping_days
TX000001,9/30/2026,2:25:00,Ha Noi,Beauty,web,1,294947,294947,SUCCESS,2
TX000002,9/23/2026,23:58:00,Da Nang,Grocery,mobile,1,265514,265514,SUCCESS,3
TX000003,9/5/2026,15:38:00,Ha Noi,Electronics,partner,1,12741238,12741238,SUCCESS,3
```

### Bước 4: Chạy Hadoop Streaming
```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
  -files mapper_revenue.py,reducer_revenue.py \
  -mapper "python3 mapper_revenue.py" \
  -reducer "python3 reducer_revenue.py" \
  -input /user/student/ecommerce/input/orders_2026_09.csv \
  -output /user/student/ecommerce/output/revenue_by_category
```

### Bước 5: Xem tệp đầu ra và lấy Top 3
```bash
hadoop fs -ls /user/student/ecommerce/output/revenue_by_category
hadoop fs -cat /user/student/ecommerce/output/revenue_by_category/part-*
```
**Nội dung tệp `part-00000`:**
```text
Beauty	111032003
Book	39306022
Electronics	3703769311
Fashion	218597763
Grocery	80983147
Home	410315065
Sport	166194354
Toy	83881779
```

Lệnh trích xuất Top 3 doanh thu giảm dần:
```bash
sort -k2 -nr part-00000 | head -3
```
**Kết quả:**
```text
Electronics	3703769311
Home	410315065
Fashion	218597763
```

---

## 4. KẾT QUẢ CÁC NỘI DUNG MỞ RỘNG (BONUS)

### Mở rộng 1: Phân tích doanh thu theo Tỉnh/Thành phố (`province`)
- Triển khai qua `mapper_province.py` và `reducer_province.py`.
- **Bảng xếp hạng doanh thu theo địa bàn:**
  1. **TP. Hồ Chí Minh:** 1,190,407,002 VND
  2. **Hà Nội:** 1,057,988,317 VND
  3. **Khánh Hòa:** 398,053,480 VND
  4. **Bình Dương:** 392,815,889 VND
  5. **Đồng Nai:** 390,511,879 VND
  6. **Hải Phòng:** 388,334,931 VND
  7. **Đà Nẵng:** 347,732,676 VND
  8. **Cần Thơ:** 305,756,747 VND
  9. **Lâm Đồng:** 255,836,230 VND
  10. **Bắc Ninh:** 86,642,293 VND
- **Nhận định:** Hai trung tâm kinh tế lớn là Hà Nội và TP.HCM đóng góp hơn 46.7% tổng doanh thu toàn quốc.

### Mở rộng 2: Phân tích tỷ lệ đơn hàng Thất bại (`FAILED`) theo Kênh bán
- Triển khai qua `mapper_channel_failed.py` và `reducer_channel_failed.py`.
- **Kết quả:**
  - Kênh `partner`: 49 đơn thất bại / 497 đơn (tỷ lệ: **9.86%**)
  - Kênh `mobile`: 48 đơn thất bại / 507 đơn (tỷ lệ: **9.47%**)
  - Kênh `web`: 45 đơn thất bại / 496 đơn (tỷ lệ: **9.07%**)
- **Nhận định:** Kênh đối tác (`partner`) có tỷ lệ đơn thất bại cao nhất, cần kiểm tra lại cơ chế đồng bộ API thanh toán và kết nối tồn kho giữa sàn với các đối tác liên kết.

### Mở rộng 3: Ứng dụng Combiner tối ưu băng thông mạng
- Triển khai `combiner_revenue.py` vào giữa bước Mapper và Reducer (`-combiner "python3 combiner_revenue.py"`).
- **Lợi ích:** Combiner thực hiện tính tổng sơ bộ (pre-aggregation) ngay trên node phát sinh Mapper, giảm thiểu số lượng bản ghi gửi qua mạng trong pha Shuffle từ 1,240 cặp key-value xuống chỉ còn 8 cặp key-value đại diện cho 8 nhóm ngành hàng, giúp tăng tốc độ mạng đáng kể khi xử lý các cụm phân tán nhiều Terabytes.

### Mở rộng 4: Đối soát tính đúng đắn 100% với Python cơ sở (`verify_pandas.py`)
- Kết quả kiểm chứng tự động cho thấy toàn bộ các giá trị của 8 ngành hàng tính bằng MapReduce khớp hoàn toàn 100% với giá trị tính toán bằng thuật toán chuẩn (`is_identical = True`).

---

## 5. TỔNG KẾT
Bài thực hành đã hoàn thành trọn vẹn toàn bộ các yêu cầu từ tài liệu hướng dẫn cơ bản đến bài toán phân tích thực tế và các phần mở rộng chuyên sâu. Toàn bộ mã nguồn đã được tối ưu, tuân thủ nguyên tắc lập trình sạch và sẵn sàng cho việc kiểm tra, đánh giá.
