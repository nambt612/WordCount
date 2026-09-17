# HADOOP.md — Mô hình xử lý trên Hadoop MapReduce (Streaming)

> Xem môi trường & cách cài đặt chung tại [`README.md`](./README.md). File này chỉ tập trung vào phần kỹ thuật riêng của Hadoop.

Khi chọn engine **Hadoop**, dữ liệu phải đi qua **HDFS** (lưu trữ phân tán) và job chạy dưới dạng **MapReduce cổ điển** được điều phối bởi **YARN**, khác hẳn cơ chế in-memory tập trung của Spark (xem so sánh chi tiết ở mục 4).

---

## 1. Sơ đồ kiến trúc tổng thể

```
                          ┌─────────────────────────────┐
                          │   Client (hadoop jar /       │
                          │   Flask app)                  │
                          └───────────────┬───────────────┘
                                          │
                     hdfs dfs -put         │
                     (copy file input)     │
                                          ▼
        ┌─────────────────────────────────────────────────────┐
        │                        HDFS                          │
        │  (Hadoop Distributed File System)                    │
        │                                                       │
        │   NameNode (metadata, quản lý vị trí block)           │
        │        │                                              │
        │        ├── DataNode 1 ── block input.txt (phần 1)     │
        │        ├── DataNode 2 ── block input.txt (phần 2)     │
        │        └── DataNode 3 ── block input.txt (phần 3)     │
        │             (mỗi block được nhân bản - replication)   │
        └─────────────────────────────────────────────────────┘
                                          │
                     hadoop jar hadoop-streaming.jar
                                          │
                                          ▼
        ┌─────────────────────────────────────────────────────┐
        │                        YARN                          │
        │        (điều phối tài nguyên & lập lịch job)          │
        │                                                       │
        │   ResourceManager                                     │
        │        │                                              │
        │        ├── NodeManager (Node 1) → chạy Map Task 1     │
        │        ├── NodeManager (Node 2) → chạy Map Task 2     │
        │        └── NodeManager (Node 3) → chạy Map Task 3     │
        └─────────────────────────────────────────────────────┘
                                          │
                              (song song trên từng block)
                                          ▼
        ┌─────────────────────────────────────────────────────┐
        │                     MAP PHASE                        │
        │   mapper.py chạy độc lập trên từng Map Task           │
        │   Input:  1 dòng văn bản                              │
        │   Output: (Nhóm, 1) cho từng từ                       │
        └─────────────────────────────────────────────────────┘
                                          │
                         Shuffle & Sort (Hadoop tự động
                         gom các cặp có cùng key lại gần nhau)
                                          ▼
        ┌─────────────────────────────────────────────────────┐
        │                    REDUCE PHASE                      │
        │   reducer.py nhận các cặp (Nhóm, [1,1,1,...]) đã sort │
        │   Output: (Nhóm, tổng_số_lượng)                       │
        └─────────────────────────────────────────────────────┘
                                          │
                     ghi kết quả ra HDFS
                                          ▼
        ┌─────────────────────────────────────────────────────┐
        │         HDFS_OUTPUT_DIR/part-00000, part-00001...    │
        └─────────────────────────────────────────────────────┘
                                          │
                     hdfs dfs -cat (đọc kết quả)
                                          ▼
                              Kết quả hiển thị cho người dùng
```

---

## 2. Vai trò từng thành phần

| Thành phần | Vai trò |
|---|---|
| **HDFS NameNode** | Quản lý metadata: file nào gồm những block nào, block nằm ở DataNode nào. Không lưu dữ liệu thật |
| **HDFS DataNode** | Lưu trữ block dữ liệu thật (mặc định mỗi block được nhân bản 3 lần trên các node khác nhau — replication factor = 3) để đảm bảo chịu lỗi |
| **YARN ResourceManager** | Nhận job, quyết định phân bổ tài nguyên (CPU/RAM) cho job trên toàn cluster |
| **YARN NodeManager** | Chạy trên từng worker node, thực thi các Task (Map Task / Reduce Task) được ResourceManager giao |
| **Mapper (`mapper.py`)** | Chạy phân tán, mỗi Map Task xử lý 1 phần dữ liệu (thường là 1 block HDFS), sinh cặp `(key, value)` |
| **Shuffle & Sort** | Bước trung gian do chính Hadoop tự động thực hiện: gom tất cả các cặp có cùng `key` từ mọi Mapper lại với nhau, sắp xếp theo key, rồi chuyển tới đúng Reducer phụ trách key đó |
| **Reducer (`reducer.py`)** | Nhận dữ liệu đã gom nhóm theo key, tổng hợp (cộng dồn số lượng), ghi kết quả cuối cùng |

---

## 3. Mã nguồn Mapper và Reducer

### 3.1 `mapper.py`

```python
#!/usr/bin/env python3
import sys
import re


def classify(word):
    word = re.sub(r'[^a-zA-Z0-9À-ỹ]', '', word)
    if not word:
        return None
    length = len(word)
    if length == 1:
        return "Rat nho"
    elif 2 <= length <= 4:
        return "Nho"
    elif 5 <= length <= 9:
        return "Trung binh"
    else:
        return "Lon"


for line in sys.stdin:
    for word in line.strip().split():
        category = classify(word)
        if category:
            print(f"{category}\t1")
```

Mapper đọc từng dòng từ `stdin` (Hadoop tự động đưa từng dòng dữ liệu vào), tách từ, phân loại, in ra `Nhóm<TAB>1` — đây chính là cặp `(key, value)` của bước Map.

### 3.2 `reducer.py`

```python
#!/usr/bin/env python3
import sys

current_category = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        category, count = line.split("\t", 1)
        count = int(count)
    except ValueError:
        continue

    if current_category == category:
        current_count += count
    else:
        if current_category is not None:
            print(f"{current_category}\t{current_count}")
        current_category = category
        current_count = count

if current_category is not None:
    print(f"{current_category}\t{current_count}")
```

Reducer nhận dữ liệu **đã được Hadoop sort theo key** ở bước Shuffle & Sort, nên chỉ cần duyệt tuần tự và cộng dồn khi gặp key giống dòng trước — đây là kỹ thuật chuẩn của Hadoop Streaming reducer (không cần tự gom nhóm bằng dictionary).

---

## 4. Chạy tay bằng dòng lệnh

```bash
# 1. Copy file input lên HDFS
hdfs dfs -mkdir -p /user/master/wordlength_input
hdfs dfs -put input.txt /user/master/wordlength_input

# 2. Xóa output cũ nếu có (Hadoop yêu cầu output chưa tồn tại)
hdfs dfs -rm -r -f /user/master/wordlength_output

# 3. Chạy Hadoop Streaming job
hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -input /user/master/wordlength_input \
  -output /user/master/wordlength_output \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -file mapper.py \
  -file reducer.py

# 4. Đọc kết quả
hdfs dfs -cat /user/master/wordlength_output/part-*
```

> Đường dẫn `hadoop-streaming-*.jar` khác nhau tùy bản Hadoop cài đặt — tìm đúng đường dẫn bằng: `find / -name "hadoop-streaming*.jar" 2>/dev/null`

---

## 5. Chạy qua giao diện Web (engine = Hadoop)

Khi chọn **Hadoop** trên UI (xem `README.md` mục 6), luồng xử lý phía sau (thực hiện bởi `app_wordlength.py`, hàm `run_hadoop()`):

```
Upload file → Flask lưu vào thư mục tạm
           → hdfs dfs -put (copy file lên HDFS)
           → hadoop jar hadoop-streaming.jar -mapper mapper.py -reducer reducer.py
             (Map phase và Reduce phase chạy phân tán trên Hadoop Cluster/YARN)
           → hdfs dfs -cat (đọc kết quả part-* từ HDFS)
```

Backend tự động dọn dẹp và tạo lại `HDFS_INPUT_DIR`/`HDFS_OUTPUT_DIR` mỗi lần chạy để tránh lỗi "output already exists".

---

## 6. So sánh Hadoop MapReduce với Spark trong bài toán này

| Tiêu chí | Hadoop MapReduce (Streaming) | Spark |
|---|---|---|
| Nơi lưu dữ liệu trung gian | Ghi tạm ra **đĩa** giữa các bước (Map → Shuffle → Reduce) | Xử lý chủ yếu **in-memory** (RAM), chỉ ghi đĩa khi cần |
| Mô hình lập trình | 2 giai đoạn cố định: Map và Reduce, giao tiếp qua `stdin`/`stdout` (Streaming) | Linh hoạt hơn: chuỗi phép biến đổi `flatMap → map → filter → reduceByKey` trong cùng 1 chương trình |
| Tốc độ (với job nhỏ, chạy 1 lần) | Chậm hơn — overhead khởi tạo JVM cho từng Task, đọc/ghi đĩa nhiều lần | Nhanh hơn đáng kể nhờ giữ dữ liệu trong bộ nhớ giữa các bước |
| Điều phối tài nguyên | YARN | Có thể dùng YARN, hoặc tự quản lý (Standalone / `local[*]`) |
| Vị trí dữ liệu đầu vào | Bắt buộc phải nằm trên **HDFS** (hoặc file system phân tán khác) | Có thể đọc trực tiếp từ local file system (`local[*]`) hoặc HDFS |
| Độ phù hợp | Phù hợp với các job batch xử lý 1 lần, không cần lặp lại nhiều lần trên cùng dữ liệu | Phù hợp hơn khi cần xử lý lặp lại (iterative), ví dụ thuật toán Machine Learning (train nhiều epoch) |

> **Gợi ý khi thuyết trình:** Đây chính là lý do lịch sử vì sao Spark ra đời sau và dần thay thế Hadoop MapReduce cho nhiều use-case — Spark giữ nguyên mô hình phân tán dựa trên HDFS/YARN của Hadoop, nhưng loại bỏ phần lớn chi phí ghi/đọc đĩa giữa các bước xử lý.

---

## 7. Lệnh kiểm tra trạng thái Hadoop cluster (hữu ích khi demo)

```bash
# Xem trạng thái các DataNode trong HDFS
hdfs dfsadmin -report

# Xem danh sách file/thư mục trên HDFS
hdfs dfs -ls /user/master/

# Xem các job đang chạy / đã chạy trên YARN
yarn application -list

# Xem thông tin cluster YARN (tổng CPU/RAM, số node)
yarn node -list
```

> **Gợi ý khi demo/thuyết trình:** Mở song song UI quản trị Hadoop (YARN ResourceManager, thường ở `http://<namenode>:8088`, hoặc HDFS NameNode UI ở `:9870`) — khi chạy job Hadoop, 1 MapReduce job sẽ xuất hiện thật sự trên đó, là bằng chứng trực quan cho thấy Hadoop cluster đang thực sự xử lý dữ liệu.

---

## 8. Xử lý sự cố riêng Hadoop

| Lỗi | Nguyên nhân | Cách khắc phục |
|---|---|---|
| `FileSystem file:/// is not an HDFS file system` khi chạy `hdfs dfsadmin -report` | `core-site.xml` chưa cấu hình `fs.defaultFS`, HDFS chưa từng được thiết lập | Xem mục 9 "Cấu hình HDFS/YARN lần đầu" bên dưới |
| `jps` chỉ thấy `Jps`, không có `NameNode`/`DataNode` | HDFS chưa được khởi động (`start-dfs.sh` chưa chạy, hoặc chạy lỗi) | Xem mục 9 bên dưới |
| `start-dfs.sh` báo `ssh: connect to host localhost port 22: Connection refused` | SSH server chưa cài/chưa chạy trên máy — Hadoop cần SSH vào chính `localhost` kể cả single-node | Xem mục 9, bước "Cài đặt SSH server" |
| Không copy được file lên HDFS | HDFS chưa chạy hoặc chưa có quyền ghi vào `HDFS_INPUT_DIR` | Kiểm tra `hdfs dfsadmin -report`; thử `hdfs dfs -mkdir -p <thư_mục>` tay trước |
| Lỗi khi chạy `hadoop jar ...` | Sai đường dẫn `HADOOP_STREAMING_JAR`, hoặc mapper/reducer không có quyền thực thi | Tìm đúng đường dẫn: `find / -name "hadoop-streaming*.jar" 2>/dev/null`; `chmod +x mapper.py reducer.py` |
| `python3` trong mapper/reducer không tìm thấy trên các node worker | Các node trong cluster không có Python3 cùng đường dẫn | Cài Python3 đồng bộ trên mọi node, hoặc dùng đường dẫn Python tuyệt đối trong `-mapper`/`-reducer` |
| Job chạy nhưng không có kết quả | Thư mục `HDFS_OUTPUT_DIR` đã tồn tại từ lần chạy trước (Hadoop yêu cầu output phải chưa tồn tại) | `hdfs dfs -rm -r -f <thư_mục_output>` rồi chạy lại (backend qua UI đã tự làm bước này) |
| UI báo `Lỗi kết nối: NetworkError when attempting to fetch resource`, dù server Flask vẫn đang chạy không báo lỗi | Flask chạy với `debug=True` (auto-reload) — cơ chế `-file` của Hadoop Streaming khi copy `mapper.py`/`reducer.py` vào working directory vô tình cập nhật timestamp của 2 file này, khiến Flask hiểu nhầm là code bị sửa và **tự restart server giữa chừng lúc đang xử lý request**, làm đứt kết nối HTTP | Thêm `use_reloader=False` vào `app.run(...)` trong `app_wordlength.py`: `app.run(host="0.0.0.0", port=5001, debug=True, use_reloader=False)` |

---

## 9. Cấu hình HDFS/YARN lần đầu (máy chưa từng chạy Hadoop)

Nếu `hdfs dfsadmin -report` báo lỗi `FileSystem file:/// is not an HDFS file system`, hoặc `jps` không thấy tiến trình Hadoop nào ngoài chính nó — nghĩa là Hadoop mới chỉ được giải nén, chưa từng được cấu hình/khởi động. Làm theo thứ tự sau (chỉ cần làm **1 lần**, trừ bước khởi động ở cuối cần lặp lại mỗi khi mở máy):

### 9.1 Cấu hình `core-site.xml`

```bash
nano $HADOOP_HOME/etc/hadoop/core-site.xml
```

Nội dung:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

### 9.2 Cấu hình `hdfs-site.xml`

```bash
nano $HADOOP_HOME/etc/hadoop/hdfs-site.xml
```

Nội dung (ví dụ với `HADOOP_HOME=/home/master/hadoop`, sửa đường dẫn cho đúng máy bạn):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/home/master/hadoop/data/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/home/master/hadoop/data/datanode</value>
  </property>
</configuration>
```

> `dfs.replication = 1` vì đây là cấu hình single-node (chỉ 1 DataNode) — multi-node cluster thường dùng giá trị 3.

Tạo thư mục lưu dữ liệu:

```bash
mkdir -p /home/master/hadoop/data/namenode
mkdir -p /home/master/hadoop/data/datanode
```

### 9.3 Cài đặt SSH server (bắt buộc, kể cả single-node)

Hadoop dùng SSH để tự kết nối vào `localhost` khi khởi động các tiến trình, kể cả khi chạy trên 1 máy duy nhất:

```bash
sudo apt update
sudo apt install openssh-server -y
sudo systemctl enable ssh
sudo systemctl start ssh
```

Cấu hình SSH không cần mật khẩu tới chính mình:

```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
```

Test (lần đầu sẽ hỏi xác nhận fingerprint, gõ `yes`; sau đó không được hỏi password nữa):

```bash
ssh localhost
exit
```

### 9.4 Format NameNode (chỉ làm 1 lần duy nhất)

```bash
hdfs namenode -format
```

Xác nhận bằng `Y` nếu được hỏi. Thấy dòng `Storage directory ... has been successfully formatted` là thành công.

> **Cảnh báo:** Không format lại NameNode sau khi đã có dữ liệu thật trên HDFS — thao tác này xóa sạch metadata, dữ liệu cũ sẽ không truy cập được nữa.

### 9.5 Khởi động HDFS và YARN

```bash
start-dfs.sh
start-yarn.sh
jps
```

Kết quả `jps` phải thấy đủ 5 tiến trình: `NameNode`, `DataNode`, `SecondaryNameNode`, `ResourceManager`, `NodeManager`.

### 9.6 Xác nhận HDFS hoạt động

```bash
hdfs dfsadmin -report
hdfs dfs -mkdir -p /user/master
hdfs dfs -ls /
```

> **Lưu ý:** Từ lần sau, chỉ cần chạy lại bước 9.5 (`start-dfs.sh` + `start-yarn.sh`) mỗi khi khởi động lại máy — không cần lặp lại format hay sửa file cấu hình.

---