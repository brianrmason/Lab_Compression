# Labcompress

A C++ tool for compressing and querying lab instrument utilization data exported from iC Data Center.

## Overview

Labcompress reads CSV experiment history files and compresses them into a compact binary format using column-aware encoding strategies. The compressed binary can then be queried directly to produce instrument utilization reports.

## Binary Format (v2)

| Section       | Encoding                                      |
|---------------|-----------------------------------------------|
| Header        | Magic number (`0x5554494C`), version, row/col counts |
| Schema        | Column names (length-prefixed) + type tags    |
| Dictionaries  | Per-column string-to-ID mappings for categoricals |
| Row data      | Column-typed encoding (see below)             |

**Column encodings:**

- **CATEGORICAL** — Dictionary-encoded with varint IDs (falls back to length-prefixed string if dictionary is skipped)
- **TIMESTAMP** — Absolute 8-byte epoch seconds
- **INTEGER** — Variable-length (varint) encoding
- **FLOAT** — Raw 8-byte double
- **STRING** — Length-prefixed (2-byte length + data)
- **BOOLEAN** — Single byte

## Building

Requires C++20 and CMake 3.20+. Tested with MSVC (Visual Studio 2022).

```bash
cmake -S . -B build_v2 -G "Visual Studio 17 2022"
cmake --build build_v2 --config Release
```

Or use the included build script:

```bash
build.bat
```

The executable is produced at `build_v2/Release/labcompress.exe`.

## Usage

### Analyze a CSV file

Infer column types and suggest compression strategies:

```
labcompress analyze <csv_file>
```

### Compress CSV to binary

```
labcompress compress <csv_file> <output.bin>
```

### Query the binary

**Instrument utilization report** (all instruments):

```
labcompress query <binary_file> instrument
```

**Filter by instrument name and/or year:**

```
labcompress query <binary_file> instrument <name_filter> <year>
```

**Find underutilized instruments** (below a given percentage threshold for a specific year):

```
labcompress query <binary_file> underutilized <threshold_%> <year>
```

### Example

```
labcompress compress ExperimentHistory.csv utilization.bin
labcompress query utilization.bin instrument "" 2025
labcompress query utilization.bin underutilized 10 2025
```

## Project Structure

```
include/
  schemaparser.hpp   # Shared types, format constants, CSV parsing
  compressor.hpp     # Compression engine
  queryengine.hpp    # Binary decoder and query interface
src/
  schemaparser.cpp   # CSV parsing, type inference, schema analysis
  compressor.cpp     # CSV-to-binary encoder
  queryengine.cpp    # Binary-to-record decoder, utilization queries
  main.cpp           # CLI entry point
```

## Performance

Typical compression on experiment history data (~10,600 records, 42 columns):

- Input: ~5.3 MB (CSV)
- Output: ~2.7 MB (binary)
- Compression ratio: ~50%
