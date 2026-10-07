# Hadoop Lab 1: HDFS + MapReduce trên Google Colab

Bài thực hành môn Dữ liệu lớn (Chapter 2: Hadoop Ecosystem). Triển khai HDFS dạng pseudo-distributed trên Google Colab, sau đó chạy Hadoop Streaming bằng Python để tính doanh thu theo nhóm sản phẩm từ dữ liệu đơn hàng tháng 09/2026.

## Bài toán
Nhóm sản phẩm (`category`) nào có tổng doanh thu cao nhất, chỉ tính các giao dịch có `status = SUCCESS`?

## Dữ liệu
`orders_2026_09.csv`: 1.500 giao dịch, 11 cột
(`transaction_id, order_date, order_time, province, category, channel, quantity, unit_price, amount, status, shipping_days`).
Trạng thái: SUCCESS 1.240, FAILED 142, RETURNED 118.

## Luồng xử lý
```
orders_2026_09.csv -> HDFS -> Mapper -> Shuffle/Sort -> Reducer -> Output (HDFS)
```
- **Mapper** (`mapper_revenue.py`): đọc từng dòng CSV, bỏ dòng tiêu đề, nếu `status = SUCCESS` thì phát cặp `category -> amount`.
- **Reducer** (`reducer_revenue.py`): nhận dữ liệu đã gom theo `category` và cộng dồn `amount`.

## Cấu trúc repo
```
.
├── Hadoop_Ecommerce_Lab.ipynb    # Notebook chạy toàn bộ lab trên Colab
├── mapper_revenue.py
├── reducer_revenue.py
├── orders_2026_09.csv
├── result_revenue_by_category.txt
└── README.md
```

## Cách chạy
1. Mở `Hadoop_Ecommerce_Lab.ipynb` trên [Google Colab](https://colab.research.google.com) (Runtime: CPU).
2. Chạy tuần tự các ô từ Phần 0 đến Phần 6 (cài Hadoop 3.5.0, cấu hình, khởi động HDFS, upload CSV, chạy Streaming).
3. Lưu ý: format NameNode chỉ chạy 1 lần. Colab là môi trường tạm thời, runtime reset sẽ mất dữ liệu HDFS.

Lệnh chính:
```bash
hdfs dfs -put -f orders_2026_09.csv /user/student/ecommerce/input/
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files file:///content/mapper_revenue.py,file:///content/reducer_revenue.py \
  -mapper "python3 mapper_revenue.py" \
  -reducer "python3 reducer_revenue.py" \
  -input /user/student/ecommerce/input/orders_2026_09.csv \
  -output /user/student/ecommerce/output/revenue_by_category
```

## Kết quả
Tổng doanh thu SUCCESS: 4.814.079.444 VND.

| Hạng | Category | Doanh thu (VND) | Tỷ trọng |
|---|---|---|---|
| 1 | Electronics | 3.703.769.311 | 76,94% |
| 2 | Home | 410.315.065 | 8,52% |
| 3 | Fashion | 218.597.763 | 4,54% |

**Nhận xét:** Electronics chiếm khoảng 77% doanh thu, cần ưu tiên tồn kho, khuyến mại và giao hàng cho nhóm này. Doanh thu phụ thuộc nặng vào một nhóm nên có rủi ro tập trung. Hạn chế của xử lý batch: kết quả chỉ có sau khi job chạy xong, không theo thời gian thực.

## Công nghệ
Apache Hadoop 3.5.0 (HDFS, MapReduce local runner), Hadoop Streaming, Python 3, Google Colab.

## Tác giả
- Họ tên: Nguyễn Đức Duy
- MSSV: A48496
