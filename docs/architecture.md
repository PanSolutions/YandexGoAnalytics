# Architecture Overview

## Medallion Lakehouse Layer
1. **Landing Zone (Volumes):**
   * `/Volumes/<catalog>/raw_files/landing/avro`
   * `/Volumes/<catalog>/raw_files/landing/parquet`
   * `/Volumes/<catalog>/raw_files/landing/drivers`
2. **Bronze Layer (Delta Lake):**
   * `bronze.users`
   * `bronze.taxi`
   * `bronze.drivers`