========================================================================
BÀI THỰC HÀNH HADOOP ONLINE: PHÂN TÍCH ĐƠN HÀNG THƯƠNG MẠI ĐIỆN TỬ
Môn học: Dữ liệu lớn (Big Data) - Chapter 2: Hadoop Ecosystem
========================================================================

1. DANH SÁCH FILE TRONG THƯ MỤC LAB:
- orders_2026_09.csv: Bộ dữ liệu đơn hàng tháng 09/2026 (1,500 giao dịch).
- mapper_revenue.py: Chương trình Mapper lọc SUCCESS và phát sinh key-value (category, amount).
- reducer_revenue.py: Chương trình Reducer cộng dồn doanh thu theo từng category.
- combiner_revenue.py: Chương trình Combiner tổng hợp sơ bộ trước bước Shuffle để giảm I/O mạng.
- mapper_province.py & reducer_province.py: Mã nguồn mở rộng phân tích doanh thu theo tỉnh/thành phố.
- mapper_channel_failed.py & reducer_channel_failed.py: Mã nguồn mở rộng tính tỷ lệ đơn FAILED theo kênh.
- verify_pandas.py: Script kiểm chứng tính khớp dữ liệu 100% giữa Python cơ sở và MapReduce.
- Hadoop_Ecommerce_Lab.ipynb: Jupyter Notebook hoàn chỉnh từng bước chạy trên Google Colab / Linux.
- part-00000: Kết quả MapReduce chuẩn theo định dạng Hadoop.
- result_revenue_by_category.txt: File kết quả trích xuất từ part-00000.
- top3_revenue.txt: Bảng xếp hạng Top 3 nhóm sản phẩm có doanh thu cao nhất.
- result_revenue_by_province.txt: Kết quả phân tích doanh thu theo tỉnh thành.
- result_failed_by_channel.txt: Kết quả tỷ lệ thất bại theo từng kênh bán hàng.
- BAO_CAO_THUC_HANH_HADOOP.md: Báo cáo chi tiết đầy đủ 5 câu hỏi lý thuyết, minh chứng kỹ thuật và nhận xét kinh doanh.
- README.txt: Tệp hướng dẫn này.

2. CÁCH THỰC THI PIPELINE MAPREDUCE CỤC BỘ / TERMINAL:
cat orders_2026_09.csv | python3 mapper_revenue.py | sort | python3 reducer_revenue.py

3. CÁCH THỰC THI TRÊN HADOOP STREAMING (HDFS):
hadoop fs -mkdir -p /user/student/ecommerce/input
hadoop fs -put -f orders_2026_09.csv /user/student/ecommerce/input/
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
  -files mapper_revenue.py,reducer_revenue.py \
  -mapper "python3 mapper_revenue.py" \
  -reducer "python3 reducer_revenue.py" \
  -input /user/student/ecommerce/input/orders_2026_09.csv \
  -output /user/student/ecommerce/output/revenue_by_category

4. KẾT QUẢ TOP 3 NHÓM SẢN PHẨM CÓ DOANH THU CAO NHẤT (GIAO DỊCH SUCCESS):
Hạng 1: Electronics   - Doanh thu: 3,703,769,311 VND (296 đơn hàng, chiếm ~76.94% tổng doanh thu)
Hạng 2: Home          - Doanh thu:   410,315,065 VND (110 đơn hàng, chiếm ~8.52% tổng doanh thu)
Hạng 3: Fashion       - Doanh thu:   218,597,763 VND (233 đơn hàng, chiếm ~4.54% tổng doanh thu)
Tổng doanh thu SUCCESS: 4,814,079,444 VND (trên tổng số 1,240 đơn SUCCESS)
