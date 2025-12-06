# s3-full-index
Download a complete, XML-formatted object index for a *public* AWS S3 bucket, handling pagination (beyond the first 1000 objects).

## Description

This script:
- Uses the unauthenticated ListObjectsV2 S3 REST API ('list-type=2').
- Follows 'IsTruncated' + 'NextContinuationToken' to fetch all pages.
- Produces a single XML file containing all '<Contents>' entries.
- Requires no AWS credentials and no logging facilities.
- Adheres to PEP 8, PEP 20, and includes type hints and docstrings.
- Designed for Python 3.11+.

Security & robustness notes:
- Normalizes bucket input (bare name, virtual-hosted URL, or path-style URL).
- Validates bucket names strictly.
- Automatically uses path-style HTTPS for dotted bucket names to avoid TLS
  hostname mismatch (S3 wildcard cert only matches one label).
- Implements exponential backoff for transient HTTP failures (429/5xx).
- Streams XML output (no large in-memory lists).

## Prerequisites

Requires Python 3.x (preferrably 3.11+) and uses the following libraries:

* annotations (future)
* argparse
* html
* re
* sys
* time
* dataclasses
* typing
* collections.abc
* urllib.error
* urllib.parse
* urllib.request
* xml.etree.ElementTree

## How to Use

Help file:
```
Usage: s3_full_index.py [-h] --bucket BUCKET [--output OUTPUT] [--region REGION] [--prefix PREFIX] [--delimiter DELIMITER] [--endpoint ENDPOINT] [--timeout TIMEOUT] [--max-retries MAX_RETRIES]

Download a complete XML-formatted object index from a *public* AWS S3 bucket (handles pagination beyond 1000 objects).

options:
  -h, --help                  show this help message and exit
  --bucket BUCKET             Public S3 bucket (bare name or URL). Examples: 'graphics.jsonline.com', 'https://graphics.jsonline.com.s3.amazonaws.com', 'https://s3.amazonaws.com/graphics.jsonline.com'.
  --output OUTPUT             Output XML file path (default: s3_index_<bucket>.xml).
  --region REGION             Optional AWS region (e.g., 'us-east-1').
  --prefix PREFIX             Optional key prefix filter (e.g., 'some/path/').
  --delimiter DELIMITER       Optional delimiter (commonly '/').
  --endpoint ENDPOINT         Optional endpoint override (e.g., 'https://s3.amazonaws.com'). For dotted buckets, path-style will be used with the given host.
  --timeout TIMEOUT           Per-request timeout in seconds (default: 15).
  --max-retries MAX_RETRIES   Max retries for transient HTTP failures (default: 5).
```

Usage:
```
python s3_full_index.py --bucket graphics.jsonline.com \
    --output graphics.jsonline.com.index.xml \
    [--region us-east-1] \
    [--prefix some/path/] \
    [--delimiter /] \
    [--endpoint https://s3.amazonaws.com] \
    [--timeout 15] \
    [--max-retries 5]
```

Example given:
```
./s3_full_index.py \
  --bucket graphics.jsonline.com \
  --endpoint https://s3.amazonaws.com \
  --output graphics.jsonline.com.index.xml
```

## Built With

* [Python](https://www.python.org) designed by Guido van Rossum

## Author

**Rick Pelletier** - [Gannett Co., Inc. (USA Today Network)](https://www.usatoday.com/)

