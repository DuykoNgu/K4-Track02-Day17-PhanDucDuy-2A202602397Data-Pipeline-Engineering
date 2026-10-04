# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Phan Đức Duy / 2A202602397
**Repo:** https://github.com/DuykoNgu/K4-Track02-Day17-PhanDucDuy-2A202602397Data-Pipeline-Engineering
**Commit bài nộp:** `140ef6d`
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Antigravity IDE (Gemini 3.8 Flash) — hỗ trợ phân tích nguyên nhân gốc rễ của các contract fail, thiết kế logic upsert LSN guard, cấu hình lookback window, xử lý CDC delete và hoàn thành bonus LLM label cache.
**Nguồn tham khảo khác (nếu có):** Slide bài giảng K4 Day 17 (Data Pipeline Engineering, Medallion Architecture, Debezium CDC, dbt merge/microbatch).

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets has exactly one row per ticket_id` FAIL (24 rows cho 12 tickets); trạng thái T-91 bị sai khi rerun. | `gold_feature_daily reconciles with a full recompute` FAIL; event 08-12 của u05 đến ngày 08-15 bị bỏ sót. | `deleted ticket T-97 is a tombstone` FAIL; T-97 vẫn còn trong snapshot `v2026-08-16` và doc chunks. |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT INTO` tuần tự mỗi ngày, không deduplicate theo khoá và không có LSN guard. | `LOOKBACK_DAYS = 0` chỉ tính ngày hiện tại, không recompute lại partition quá khứ khi có event gửi muộn. | Bản tin CDC Debezium xoá (`op='d'`) có `after=null`, code đọc `after->ticket_id` bị NULL nên bị mệnh đề WHERE lọc bỏ. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: DELETE bản ghi cũ khi `c._lsn > t._lsn`, sau đó INSERT bản ghi mới nếu chưa tồn tại. | `pipeline/config.py`: Đặt `LOOKBACK_DAYS = 3` tương ứng với $\lceil P_{99} \rceil$ đo được từ Bronze. | `pipeline/staging.py`: Dùng `COALESCE(after->ticket_id, before->ticket_id, key->ticket_id)` để giữ lại event xoá. |
| **Khái niệm trên slide** | Silver — có khoá; Bốn cách viết idempotent; LSN guard chống replay batch cũ đè dữ liệu mới. | Data về muộn (Late data); Event time vs Ingestion time; Lookback window = ceil(P99) đo từ Bronze. | CDC log-based; Xoá phải lan truyền (Tombstone propagation); Snapshot point-in-time loại trừ tombstone. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: `silver_tickets` mô hình hoá trạng thái thực thể hiện tại nên cần MERGE có bảo vệ LSN, còn `gold_feature_daily` là bảng tổng hợp aggregate theo ngày nên ghi đè toàn bộ phân vùng vừa triệt tiêu duplicate vừa tự động cập nhật late data an toàn.
- Tombstone thay vì xoá hẳn hàng trong Silver: Tombstone lưu lại dấu vết xoá kèm LSN giúp lan truyền thao tác xoá xuống các tầng Gold/RAG, đồng thời ngăn dữ liệu bị hồi sinh (re-insertion) nếu log CDC cũ bị replay.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo tính bất biến và khả năng tái lập (reproducibility) tuyệt đối cho các mô hình AI/ML trong quá khứ mà không bị rò rỉ dữ liệu tương lai (data leakage).
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Tập dữ liệu vừa vặn trên một máy đơn, DuckDB chạy in-process đạt hiệu năng xử lý vectorized cực cao và không tốn chi phí quản lý cụm, serialize hay độ trễ khởi động của Spark.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   - **Xử lý:** Áp dụng kỹ thuật **Pseudonymization kết hợp Crypto-shredding**: Tách toàn bộ định danh và PII sang bảng riêng biệt được mã hoá khoá bí mật theo từng người dùng; các snapshot training chỉ lưu ID ẩn danh hoặc token mã hoá. Khi người dùng yêu cầu xoá dữ liệu theo GDPR, ta chỉ cần hủy khoá giải mã của họ (Crypto-shredding) — toàn bộ dữ liệu trong các snapshot cũ trở thành văn bản rác vô nghĩa không thể khôi phục mà không làm hỏng cấu trúc file snapshot bất biến. Đồng thời, thiết lập chính sách lưu trữ (Data Retention Policy) tự động dọn dẹp các snapshot cũ sau một chu kỳ cố định (ví dụ 60–90 ngày).

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   - **Xử lý:** Đặt chốt tại **tầng Silver** (ngay trước khi dữ liệu được làm sạch đưa vào warehouse). Bổ sung mô hình **NER (Named Entity Recognition)** tiếng Việt (như PhoBERT-NER / spaCy-vi) hoặc một mô hình LLM nhỏ chạy cục bộ để nhận diện các thực thể `PER` (Person), kết hợp danh sách từ khoá định danh người dùng từ bảng User master.
   - **Đo lường:** Xây dựng một tập dữ liệu kiểm thử vàng (Gold evaluation set) có gán nhãn PII thủ công bởi con người; đánh giá pipeline bằng hai chỉ số: **Precision** (độ chuẩn xác để tránh che nhầm từ ngữ thông thường) và **Recall** (độ bao phủ, bắt buộc tối ưu Recall > 99.5% để đảm bảo không lọt bất kỳ dữ liệu định danh nào ra ngoài).

## 5. Output (dán nguyên văn)

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
..................................                                       [100%]
34 passed in 1.46s

$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
15:39:58  Running with dbt=1.12.5
15:39:58  Registered adapter: duckdb=1.11.0
15:39:59  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
15:39:59  
15:39:59  Concurrency: 1 threads (target='dev')
15:39:59  
15:39:59  1 of 19 START sql view model main.stg_events ................................... [RUN]
15:39:59  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.05s]
15:39:59  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
15:39:59  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.02s]
15:39:59  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
15:39:59  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.08s]
15:39:59  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
15:39:59  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.08s]
15:39:59  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
15:39:59  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.09s]
15:39:59  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
15:39:59  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.03s]
15:39:59  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
15:39:59  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.01s]
15:39:59  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
15:39:59  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.01s]
15:39:59  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
15:39:59  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.02s]
15:39:59  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
15:39:59  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
15:39:59  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
15:39:59  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
15:39:59  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
15:39:59  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
15:39:59  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
15:39:59  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.01s]
15:39:59  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
15:39:59  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
15:39:59  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
15:39:59  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.01s]
15:39:59  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
15:39:59  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
15:39:59  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.03s]
15:39:59  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
15:39:59  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.03s]
15:39:59  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
15:39:59  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.02s]
15:39:59  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
15:39:59  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.02s]
15:39:59  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
15:39:59  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.02s]
15:39:59  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
15:39:59  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.02s]
15:40:00  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
15:40:00  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.01s]
15:40:00  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.16s]
15:40:00  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
15:40:00  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
15:40:00  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
15:40:00  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.01s]
15:40:00  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
15:40:00  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.01s]
15:40:00  
15:40:00  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 0.86 seconds (0.86s).
15:40:00  
15:40:00  Completed successfully
15:40:00  
15:40:00  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree

$ make bonus-llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```
