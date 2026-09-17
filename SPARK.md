# SPARK.md — Mô hình xử lý trên Apache Spark

> Xem môi trường & cách cài đặt chung tại [`README.md`](./README.md). File này chỉ tập trung vào phần kỹ thuật riêng của Spark.

## 1. Sơ đồ thuật toán tổng quát

```
input.txt
   │
   ▼
SparkContext.textFile()      → Đọc dữ liệu
   │
   ▼
flatMap()                    → Tách từng dòng thành các từ riêng biệt
   │
   ▼
map()                        → Tính độ dài, phân loại, tạo cặp (Nhóm, 1)
   │
   ▼
filter()                     → Loại bỏ các giá trị không hợp lệ (None)
   │
   ▼
reduceByKey()                → Gom nhóm, cộng dồn số lượng
   │
   ▼
saveAsTextFile() / collect() → Xuất kết quả
```

Toàn bộ pipeline chạy **in-memory** (dữ liệu giữ trong RAM giữa các bước biến đổi), Spark chỉ ghi ra đĩa ở bước cuối (`saveAsTextFile`).

---

## 2. Các bước triển khai (chạy tay)

### Bước 1 — Tạo file dữ liệu đầu vào `input.txt`

```bash
cat > input.txt <<EOF
I am a student
Hadoop is a distributed system
Java programming language
Spark is fast
Python is easy
Database administration
EOF
```

Kiểm tra lại nội dung:

```bash
cat input.txt
```

### Bước 2 — Tạo file chương trình `wordlength.py`

```python
from pyspark import SparkConf, SparkContext
import re
import sys

conf = SparkConf().setAppName("WordLength")
sc = SparkContext(conf=conf)

input_path = sys.argv[1]
output_path = sys.argv[2]

lines = sc.textFile(input_path)
words = lines.flatMap(lambda line: line.split())

def classify(word):
    word = re.sub(r'[^a-zA-Z0-9À-ỹ]', '', word)
    if not word:
        return None
    length = len(word)
    if length == 1:
        category = "Rat nho"
    elif 2 <= length <= 4:
        category = "Nho"
    elif 5 <= length <= 9:
        category = "Trung binh"
    else:
        category = "Lon"
    return (category, 1)

result = words.map(classify).filter(lambda x: x is not None).reduceByKey(lambda a, b: a + b)
result.saveAsTextFile(output_path)

print("===== WORD LENGTH RESULT =====")
for category, count in result.collect():
    print(category, count)

sc.stop()
```

**Giải thích từng thành phần:**

| Hàm | Vai trò |
|---|---|
| `sc.textFile()` | Đọc file `input.txt` (hoặc cả 1 thư mục nhiều file), tạo RDD, dữ liệu được chia thành các partition để xử lý song song |
| `flatMap()` | Tách mỗi dòng thành danh sách các từ, "làm phẳng" kết quả thành 1 RDD chứa từng từ riêng lẻ |
| `classify()` | Loại bỏ ký tự không phải chữ/số, xác định độ dài từ, ánh xạ sang 1 trong 4 nhóm |
| `map()` | Áp dụng `classify()` lên từng từ, sinh ra cặp `(Nhóm, 1)` — đây là bước **Map** trong MapReduce |
| `filter()` | Loại bỏ các phần tử `None` (từ rỗng sau khi làm sạch ký tự) |
| `reduceByKey()` | Gom các cặp có cùng khóa (Nhóm), cộng dồn giá trị — đây là bước **Reduce** trong MapReduce |
| `saveAsTextFile()` | Ghi kết quả cuối cùng ra thư mục `output` |

### Bước 3 — Chạy chương trình trên Spark Standalone

```bash
spark-submit wordlength.py input.txt output
```

Trong đó:
- `wordlength.py` — chương trình xử lý
- `input.txt` — file dữ liệu đầu vào (có thể thay bằng đường dẫn 1 thư mục chứa nhiều file `.txt`)
- `output` — thư mục lưu kết quả (Spark tự tạo, **không được tồn tại sẵn** trước khi chạy — nếu đã có, xóa bằng `rm -rf output` trước khi chạy lại)

### Bước 4 — Kiểm tra và đọc kết quả

```bash
ls -l output
cat output/part-*
```

Kết quả in ra màn hình ngay khi chương trình chạy xong (từ lệnh `print()` trong code):

```
===== WORD LENGTH RESULT =====
Rat nho 3
Nho 9
Trung binh 7
Lon 3
```

> **Lưu ý:** Thứ tự các nhóm trong file kết quả có thể thay đổi giữa các lần chạy do Spark xử lý dữ liệu theo cơ chế phân tán (song song trên nhiều partition), không đảm bảo thứ tự tuyến tính.

---

## 3. Chạy qua giao diện Web (engine = Spark)

Khi chọn **Spark** trên UI (xem `README.md` mục 6), luồng xử lý phía sau:

```
Upload file → Flask lưu vào thư mục tạm → spark-submit wordlength.py <thư_mục> <output>
           → Spark tự đọc, xử lý, xuất kết quả trong 1 process
```

Backend gọi trực tiếp:

```python
subprocess.run(["spark-submit", SPARK_SCRIPT, tmp_dir, output_dir], ...)
```

`wordlength.py` đọc được thẳng cả **thư mục** (`tmp_dir`) chứa nhiều file `.txt` cùng lúc nhờ `sc.textFile()` hỗ trợ input là thư mục, không cần gộp file thủ công.

---

## 4. Xử lý sự cố riêng Spark

| Lỗi | Nguyên nhân | Cách khắc phục |
|---|---|---|
| `spark-submit: command not found` | Chưa activate venv, hoặc venv đã bị xóa | `source .venv/bin/activate`; nếu venv đã xóa, tạo lại theo `README.md` mục 4.1 |
| `ModuleNotFoundError: No module named 'pyspark'` | Chạy bằng `python3 wordlength.py ...` thay vì `spark-submit` | Luôn chạy qua `spark-submit`, không dùng `python3` trực tiếp |
| `output already exists` | Thư mục `output` đã tồn tại từ lần chạy trước | `rm -rf output` rồi chạy lại |
| Không thấy `part-*` nào trong output | Sai đường dẫn input, RDD rỗng | Kiểm tra lại nội dung file/thư mục input |