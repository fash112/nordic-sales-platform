# Nordic Sales Data Platform

**Oladayo Fasokun** — Data & Cloud Engineer

A medallion-architecture data platform reconciling sales data from four Nordic
countries into a single analytics-ready star schema.

**Runs end to end on a laptop.** No Azure subscription needed to see it work.

---

## The problem

Four countries, four ERP exports, four incompatible formats. Nobody can answer
"what did we sell last quarter" without someone spending a day in Excel.

|  | Sweden | Norway | Denmark | Finland |
| --- | --- | --- | --- | --- |
| Format | CSV `;` | CSV `,` | JSON lines | TSV |
| Encoding | UTF-8 | **latin-1** | UTF-8 | **UTF-8 BOM** |
| Date | `2024-05-22` | `22.05.2024` | `22/05/2024` | `20240522` |
| Decimal | `4 802,03` | `4802.03` | `4.802,03` | `4802,03` |
| Currency | SEK | NOK | DKK | EUR |
| Null marker | *empty* | `NULL` | `null` | `-` |
| Customer column | `cust_id` | `KundeNr` | `kunde_nr` | `asiakas_id` |

Every one of those differences is a way to get the wrong number quietly.

---

## Architecture

```
  raw/                    ADF                Databricks           dbt
  ├── sweden/*.csv    ──► Copy ──► bronze/  ──► PySpark  ──► silver/ ──► Gold star schema
  ├── norway/*.csv        (per      (as        reconcile     (typed,      ├── fct_sales
  ├── denmark/*.jsonl      country)  received)  + dedupe      quality-    ├── dim_customer
  └── finland/*.tsv                            + assess      assessed)   ├── dim_product
                                                                          └── dim_date
```

**One tool owns each layer.** This is the design decision the whole project
turns on:

| Layer | Owner | Why |
| --- | --- | --- |
| Orchestration | **ADF** | Moves files, triggers compute. No transformation logic, so pipeline JSON stays diffable. |
| Bronze → Silver | **PySpark** | Schema reconciliation across four formats is genuinely distributed-processing work. |
| Silver → Gold | **dbt** | Dimensional modelling and testing is what dbt exists for. Tests live beside the models they guard. |

Splitting it any other way gives you two places that transform data and no
answer to "which one actually runs".

---

## Running it

```bash
pip install pyspark dbt-duckdb

# 1. Generate the divergent source files
python src/generate/generate_sources.py --rows 4000 --seed 42

# 2. Bronze -> Silver: reconcile, dedupe, assess quality
python src/silver/build_silver.py --raw data/raw --out data/silver

# 3. Silver -> Gold: build the star schema and run every test
cd dbt && DBT_PROFILES_DIR=. dbt build
```

Verified output on a clean run:

```
silver rows: 16,000
dq failures: 0 (0.00%)

Done. PASS=42 WARN=0 ERROR=0 SKIP=0 TOTAL=42
```

---

## Data quality

Silver **flags** bad rows; it never drops them. A dropped row is invisible, a
flagged row is countable. Nine checks run on every record:

`missing_order_id` · `unparseable_date` · `unparseable_price` ·
`negative_price` · `missing_quantity` · `zero_quantity` · `missing_customer` ·
`missing_sku` · `encoding_damage`

The gate is applied in exactly one place — `stg_sales` — so there is a single
answer to "why is this row missing from the report".

`encoding_damage` deserves a note: it detects the U+FFFD replacement character
surviving into a customer name, which means an encoding was mis-declared
somewhere upstream. It is the check most likely to catch a real production
incident, because mojibake passes every type check and every not-null test.

**42 dbt tests** cover grain, referential integrity, accepted values, and a
revenue reconciliation between the gate and the fact table. That last one is
the important one — row-count tests miss a uniform fan-out and grain tests miss
silent loss. Comparing the summed measure on both sides catches both.

---

## Modelling decisions worth defending

**`dim_customer` is keyed on `(country, customer_no)`, not `customer_no`.**
Customer numbers are unique only within a country's ERP. In the generated data,
**401 customer numbers appear in more than one country** — keying on the number
alone would have merged 401 pairs of unrelated businesses, and the row counts
would still have looked plausible.

**`dim_product` is keyed on `sku` alone.** One catalogue is sold in all four
markets, so the country qualifier that `dim_customer` needs would only create
duplication here. Applying the same keying rule to every dimension out of habit
is how you get either false merges or needless fan-out.

**Returns stay in the fact as negative quantities.** Filtering them inflates
revenue, and nobody notices until quarter close. Any `sum(revenue)` is
therefore net of returns by default; gross is available via `is_return = false`.

**Grain is declared and enforced.** `fct_sales` is one row per
`(country, order_id)`, asserted by a singular test. Undeclared grain is the most
common cause of double-counted revenue.

---

## Two real bugs found while building this

Both were caught by running the code rather than reading it, and both are the
kind that produce wrong numbers rather than error messages.

**1. `regexp_replace` with a `.` separator.** Stripping the Danish thousands
separator with `regexp_replace(col, ".", "")` matched *every character* — `.`
is a regex wildcard — reducing `4.476,86` to an empty string. All 4,000 Danish
rows failed as `unparseable_price`. Fixed by using `translate`, which matches
literal characters. Had the quality layer not flagged it, Denmark would have
silently contributed zero revenue.

**2. A bare `.cast()` under Spark's ANSI mode.** One malformed price aborted the
entire job instead of being recorded as a quality failure — defeating the whole
flag-don't-drop design. Fixed by validating with a regex before casting, so bad
values become NULL and get counted.

---

## Production gaps

- **FX rates are hardcoded.** Production joins to a rate table keyed on date;
  the current static map is explicit in the code rather than hidden, but it is
  still wrong for any historical analysis.
- **No incremental loads.** Silver rebuilds in full each run. Fine at this
  volume, wrong at real volume — the fix is a merge on `(country, order_id)`.
- **No SCD Type 2 anywhere.** `dim_customer` overwrites names. If the business
  needs "what was this customer called when the order was placed", that changes.
- **No orchestration retries beyond ADF defaults**, and no alerting on quality
  failures beyond the pipeline failing.
- **Bronze has no retention policy.** It will grow indefinitely.

---

## Layout

```
src/generate/generate_sources.py   synthetic sources, divergence intentional
src/silver/build_silver.py         PySpark reconciliation + quality assessment
dbt/models/staging/stg_sales.sql   the quality gate
dbt/models/marts/                  star schema + 42 tests
dbt/tests/                         grain and revenue reconciliation
adf/pipeline/                      orchestration
adf/dataset/, adf/linkedService/   parameterised sources, MSI auth
```
