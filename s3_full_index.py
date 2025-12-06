#!/usr/bin/env python3

"""
s3_full_index.py

Download a complete, XML-formatted object index for a *public* AWS S3 bucket,
handling pagination (beyond the first 1000 objects).

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

Usage:
    python s3_full_index.py --bucket graphics.jsonline.com \
        --output graphics.jsonline.com.index.xml \
        [--region us-east-1] \
        [--prefix some/path/] \
        [--delimiter /] \
        [--endpoint https://s3.amazonaws.com] \
        [--timeout 15] \
        [--max-retries 5]

Example given:
    ./s3_full_index.py \
        --bucket graphics.jsonline.com \
        --endpoint https://s3.amazonaws.com \
        --output graphics.jsonline.com.index.xml

To extract a file list:
    xmllint --xpath '//*[local-name()="Key"]/text()' \
        graphics.jsonline.com.index.xml > files.txt

Linter: ruff check s3_full_index.py --extend-select F,B,UP
"""

from __future__ import annotations
import argparse
import html
import re
import sys
import time
from dataclasses import dataclass
from typing import BinaryIO
from collections.abc import Iterator
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse, quote
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

# ------------------------------ Data Models -------------------------------- #

@dataclass(frozen=True)
class S3Object:
    """Representation of an S3 object's metadata for XML index output."""
    key: str
    last_modified: str
    etag: str
    size: int
    storage_class: str | None = None

# ------------------------------- Utilities --------------------------------- #

# Label: lowercase alphanum, may contain hyphens, not start/end with hyphen.
_BUCKET_LABEL_RE = re.compile(r"^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)$")

def normalize_bucket_arg(raw: str) -> str:
    """
    Normalize a user-provided bucket argument to a bare bucket name.

    Accepts:
      - Bare bucket name: 'my-bucket'
      - Virtual-hosted URL/host: 'https://my-bucket.s3.amazonaws.com'
      - Path-style URL: 'https://s3.amazonaws.com/my-bucket'

    Returns:
        Bare bucket name (e.g., 'my-bucket').

    Raises:
        ValueError: if the bucket cannot be determined safely.
    """
    if not raw:
        raise ValueError("Bucket argument is empty.")

    val = raw.strip()

    if val.startswith(("http://", "https://")):
        parsed = urlparse(val)
        host = parsed.netloc.strip().rstrip(".")
        path = parsed.path or ""
    else:
        parsed = None
        host = val.strip().rstrip(".")
        path = ""

    # Virtual-hosted pattern: "<bucket>.s3[.<region>].amazonaws.com"
    s3_pos = host.find(".s3.")

    if s3_pos > 0:
        bucket = host[:s3_pos]
        if not bucket:
            raise ValueError("Could not extract bucket from virtual-hosted S3 host.")
        return bucket

    # Path-style: "...amazonaws.com/<bucket>"
    if parsed and host.endswith("amazonaws.com"):
        bucket = path.lstrip("/").split("/", 1)[0] if path else ""

        if not bucket:
            raise ValueError("Could not extract bucket from path-style S3 URL.")

        return bucket

    # Otherwise treat as bare bucket name or host that *is* the bucket.
    return host

def validate_bucket_name(bucket: str) -> None:
    """
    Validate S3 bucket name per DNS-style rules (simplified but safe).

    Rules:
      - total length 3..63
      - labels separated by dots
      - each label: lowercase letters, digits, hyphens; cannot start/end with hyphen
      - must not look like an IPv4 address (e.g., '1.2.3.4')
    """
    if not (3 <= len(bucket) <= 63):
        raise ValueError("Bucket name length must be between 3 and 63 characters.")

    labels = bucket.split(".")

    if any(len(lbl) == 0 for lbl in labels):
        raise ValueError("Bucket name contains empty label between dots.")

    for lbl in labels:
        if not _BUCKET_LABEL_RE.match(lbl):
            raise ValueError(
                "Bucket labels must be lowercase letters, digits or hyphens, "
                "and cannot start or end with a hyphen."
            )

    # Disallow IPv4-like names (e.g., 1.2.3.4)
    if len(labels) == 4 and all(lbl.isdigit() for lbl in labels):
        raise ValueError("Bucket name must not be an IPv4-style dotted numeric.")

@dataclass(frozen=True)
class EndpointPlan:
    """
    Plan describing how to construct list URLs:
      - host: the base host (e.g., 's3.amazonaws.com' or 'bucket.s3.amazonaws.com')
      - path_style: whether to use path-style ('/bucket') vs virtual-hosted
    """
    host: str
    path_style: bool
    scheme: str = "https"

def choose_endpoint_plan(bucket: str,
                         region: str | None,
                         user_endpoint: str | None) -> EndpointPlan:
    """
    Choose an endpoint plan that avoids TLS hostname mismatch for dotted buckets.

    Logic:
      - If the bucket contains a dot ('.'), use path-style with HTTPS:
          host = 's3.amazonaws.com' or 's3.<region>.amazonaws.com'
          URL will be: https://host/<bucket>?list-type=2&...
      - Else (no dots), use virtual-hosted style:
          host = '<bucket>.s3.amazonaws.com' or '<bucket>.s3.<region>.amazonaws.com'
      - If 'user_endpoint' is provided, adapt:
          - If 'user_endpoint' host starts with '<bucket>.', treat as virtual-hosted and do not prepend again.
          - Else, for dotted buckets, use path-style on the provided host.
          - Else, prepend '<bucket>.' to the provided host for virtual-hosted.

    Returns:
        EndpointPlan
    """
    dotted = "." in bucket

    def _aws_host_for_region() -> str:
        return f"s3.{region}.amazonaws.com" if region else "s3.amazonaws.com"

    if user_endpoint:
        if not user_endpoint.startswith(("http://", "https://")):
            raise ValueError("Custom endpoint must include scheme (http/https).")

        scheme, rest = user_endpoint.split("://", 1)
        host = rest.split("/", 1)[0].strip().rstrip(".")
        # Any path component on user_endpoint is ignored for safety.

        if dotted:
            # Force path-style for dotted buckets to avoid TLS mismatch.
            return EndpointPlan(host=host, path_style=True, scheme=scheme)

        # Non-dotted buckets can safely use virtual-hosted style:
        if host.startswith(f"{bucket}."):
            return EndpointPlan(host=host, path_style=False, scheme=scheme)

        return EndpointPlan(host=f"{bucket}.{host}", path_style=False, scheme=scheme)

    # No custom endpoint
    if dotted:
        # Use path-style on regional/global S3 host
        return EndpointPlan(host=_aws_host_for_region(), path_style=True)

    # Use virtual-hosted style
    if region:
        return EndpointPlan(host=f"{bucket}.s3.{region}.amazonaws.com", path_style=False)

    return EndpointPlan(host=f"{bucket}.s3.amazonaws.com", path_style=False)

def build_list_url(plan: EndpointPlan,
                   bucket: str,
                   prefix: str | None,
                   delimiter: str | None,
                   continuation_token: str | None) -> str:
    """
    Construct the ListObjectsV2 URL with appropriate query parameters,
    supporting both path-style and virtual-hosted plans.

    Args:
        plan: EndpointPlan selected via choose_endpoint_plan.
        bucket: Bucket name (for path-style placement).
        prefix: Optional key prefix filter.
        delimiter: Optional delimiter (commonly '/').
        continuation_token: Token from previous response to fetch next page.

    Returns:
        Fully-qualified HTTPS URL string for ListObjectsV2.
    """
    params: dict[str, str] = {"list-type": "2", "max-keys": "1000"}

    if prefix:
        params["prefix"] = prefix

    if delimiter:
        params["delimiter"] = delimiter

    if continuation_token:
        params["continuation-token"] = continuation_token

    base = f"{plan.scheme}://{plan.host}"

    if plan.path_style:
        # Path-style requires bucket as first path segment (URL-encoded).
        base = f"{base}/{quote(bucket, safe='')}"

    # S3 ListObjectsV2 is a GET on bucket endpoint with query params.
    return f"{base}?{urlencode(params)}"

def http_get(url: str,
             timeout: float,
             max_retries: int) -> bytes:
    """
    Perform a resilient HTTP GET with exponential backoff for transient failures.

    Retries on:
    - URLError
    - HTTPError with 429, 500, 502, 503, 504
    """
    backoff = 0.75  # seconds
    attempt = 0
    last_exc: Exception | None = None

    headers = {
        "User-Agent": "s3-full-index/1.2 (+https://example.org)",
        "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
    }

    while attempt <= max_retries:
        req = Request(url, headers=headers, method="GET")

        try:
            with urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except HTTPError as he:
            if he.code in {429, 500, 502, 503, 504} and attempt < max_retries:
                time.sleep(backoff)
                backoff = min(backoff * 2, 10.0)
                attempt += 1
                last_exc = he
                continue
            raise
        except URLError as ue:
            if attempt < max_retries:
                time.sleep(backoff)
                backoff = min(backoff * 2, 10.0)
                attempt += 1
                last_exc = ue
                continue
            raise
        except Exception as exc:  # Defensive catch-all
            if attempt < max_retries:
                time.sleep(backoff)
                backoff = min(backoff * 2, 10.0)
                attempt += 1
                last_exc = exc
                continue
            raise

    if last_exc:
        raise last_exc

    raise RuntimeError("http_get: unexpected fallthrough without response")

# ---------------------------- XML Processing -------------------------------- #

def _detect_namespace(root: ET.Element) -> str | None:
    """Detect the XML namespace URI from the root element."""
    if root.tag.startswith("{"):
        ns_uri = root.tag.split("}", 1)[0].strip("{")
        return ns_uri

    return None

def iter_objects_from_xml(xml_bytes: bytes) -> tuple[Iterator[S3Object], bool, str | None]:
    """
    Parse a single ListObjectsV2 XML page and yield S3Object entries.
    Handles XML namespaces emitted by S3.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise RuntimeError(f"Failed to parse S3 ListObjectsV2 XML: {exc}") from exc

    ns_uri = _detect_namespace(root)

    def _q(tag: str) -> str:
        return f"{{{ns_uri}}}{tag}" if ns_uri else tag

    # Pagination flags
    is_trunc_el = root.find(_q("IsTruncated"))
    is_truncated = (is_trunc_el is not None and (is_trunc_el.text or "").strip().lower() == "true")
    next_token_el = root.find(_q("NextContinuationToken"))
    next_token = (next_token_el.text or "").strip() if next_token_el is not None else None
    contents_iter = root.findall(_q("Contents"))

    def _to_obj(el: ET.Element) -> S3Object | None:
        key_el = el.find(_q("Key"))
        last_el = el.find(_q("LastModified"))
        etag_el = el.find(_q("ETag"))
        size_el = el.find(_q("Size"))
        sc_el = el.find(_q("StorageClass"))

        if key_el is None or last_el is None or size_el is None:
            return None

        key = (key_el.text or "")
        last = (last_el.text or "")
        etag_raw = (etag_el.text or "") if etag_el is not None else ""
        etag = etag_raw.strip().strip('"')

        try:
            size = int((size_el.text or "0").strip())
        except ValueError:
            size = 0

        storage_class = (sc_el.text or None) if sc_el is not None else None

        return S3Object(
            key=key,
            last_modified=last,
            etag=etag,
            size=size,
            storage_class=storage_class,
        )

    def _iter() -> Iterator[S3Object]:
        for el in contents_iter:
            obj = _to_obj(el)
            if obj is not None:
                yield obj

    return _iter(), is_truncated, next_token

# ---------------------------- Output (XML) ---------------------------------- #

def begin_xml(out: BinaryIO, bucket: str, prefix: str | None, delimiter: str | None) -> None:
    """Write the XML prolog and opening ListBucketResult envelope."""
    out.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
    out.write(b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n')
    out.write(f"  <Name>{html.escape(bucket)}</Name>\n".encode())
    out.write(f"  <Prefix>{html.escape(prefix or '')}</Prefix>\n".encode())
    out.write(f"  <Delimiter>{html.escape(delimiter or '')}</Delimiter>\n".encode())

def write_object(out: BinaryIO, obj: S3Object) -> None:
    """Append a single '<Contents>' block to the output stream."""
    out.write(b"  <Contents>\n")
    out.write(f"    <Key>{html.escape(obj.key)}</Key>\n".encode())
    out.write(f"    <LastModified>{html.escape(obj.last_modified)}</LastModified>\n".encode())
    out.write(f"    <ETag>\"{html.escape(obj.etag)}\"</ETag>\n".encode())
    out.write(f"    <Size>{obj.size}</Size>\n".encode())

    if obj.storage_class:
        out.write(f"    <StorageClass>{html.escape(obj.storage_class)}</StorageClass>\n".encode())

    out.write(b"  </Contents>\n")

def end_xml(out: BinaryIO) -> None:
    """Close the ListBucketResult envelope."""
    out.write(b"  <IsTruncated>false</IsTruncated>\n")
    out.write(b"</ListBucketResult>\n")

# ------------------------------ Orchestration ------------------------------- #

def fetch_full_index(bucket_arg: str,
                     output_path: str,
                     region: str | None,
                     prefix: str | None,
                     delimiter: str | None,
                     endpoint: str | None,
                     timeout: float,
                     max_retries: int) -> None:
    """
    Fetch all pages from a public S3 bucket and write a single XML index.

    Streams the XML output directly to 'output_path', avoiding large in-memory lists.
    """
    bucket = normalize_bucket_arg(bucket_arg)
    validate_bucket_name(bucket)
    plan = choose_endpoint_plan(bucket, region, endpoint)

    continuation_token: str | None = None

    with open(output_path, "wb") as out:
        begin_xml(out, bucket=bucket, prefix=prefix, delimiter=delimiter)

        page_count = 0
        obj_count = 0

        while True:
            url = build_list_url(
                plan=plan,
                bucket=bucket,
                prefix=prefix,
                delimiter=delimiter,
                continuation_token=continuation_token,
            )
            xml_bytes = http_get(url=url, timeout=timeout, max_retries=max_retries)
            objects_iter, is_truncated, next_token = iter_objects_from_xml(xml_bytes)

            for obj in objects_iter:
                write_object(out, obj)
                obj_count += 1

            page_count += 1

            if is_truncated and not next_token:
                raise RuntimeError(
                    "S3 response indicates truncation but no NextContinuationToken "
                    "was provided. Aborting to avoid incomplete results."
                )

            if not is_truncated:
                break

            continuation_token = next_token

        end_xml(out)

    print(f"Completed: {obj_count} objects across {page_count} page(s).")
    print(f"Written XML index to: {output_path}")

# ---------------------------------- CLI ------------------------------------- #

def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Download a complete XML-formatted object index from a *public* "
            "AWS S3 bucket (handles pagination beyond 1000 objects)."
        )
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help=(
            "Public S3 bucket (bare name or URL). Examples: "
            "'graphics.jsonline.com', "
            "'https://graphics.jsonline.com.s3.amazonaws.com', "
            "'https://s3.amazonaws.com/graphics.jsonline.com'."
        ),
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output XML file path (default: s3_index_<bucket>.xml).",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="Optional AWS region (e.g., 'us-east-1').",
    )
    parser.add_argument(
        "--prefix",
        default=None,
        help="Optional key prefix filter (e.g., 'some/path/').",
    )
    parser.add_argument(
        "--delimiter",
        default=None,
        help="Optional delimiter (commonly '/').",
    )
    parser.add_argument(
        "--endpoint",
        default=None,
        help=(
            "Optional endpoint override (e.g., 'https://s3.amazonaws.com'). "
            "For dotted buckets, path-style will be used with the given host."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="Per-request timeout in seconds (default: 15).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Max retries for transient HTTP failures (default: 5).",
    )
    args = parser.parse_args(argv)

    normalized_bucket = normalize_bucket_arg(args.bucket)

    if args.output is None:
        args.output = f"s3_index_{normalized_bucket}.xml"

    return args

def main(argv: list[str] | None = None) -> int:
    """Entrypoint for CLI execution."""
    args = parse_args(argv)

    try:
        fetch_full_index(
            bucket_arg=args.bucket,
            output_path=args.output,
            region=args.region,
            prefix=args.prefix,
            delimiter=args.delimiter,
            endpoint=args.endpoint,
            timeout=args.timeout,
            max_retries=args.max_retries,
        )
    except (ValueError, URLError, HTTPError, RuntimeError) as err:
        print(f"Error: {err}", file=sys.stderr)

        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())

# end of script
