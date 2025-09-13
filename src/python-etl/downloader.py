# src/downloader.py
"""
File downloader with advanced redirect handling for infloww.com reports.
Handles SendGrid wrapped links, meta refreshes, JavaScript redirects, etc.
"""

import os
import re
import json
import time
import logging
import requests
from typing import Optional, Tuple
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote, urljoin
from bs4 import BeautifulSoup

from exceptions import DownloadError

logger = logging.getLogger(__name__)

XLSX_MAGIC = b"PK\x03\x04"  # XLSX files are ZIP containers


def safe_filename(name: str, default: str = "file") -> str:
    """Create a safe filename from a string."""
    s = (name or default).strip()
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("_")
    return s or default


def _extract_param_url(u: str) -> Optional[str]:
    """Pull embedded URL from common query params."""
    try:
        p = urlparse(u)
        qs = parse_qs(p.query or "")
        for k in ("url", "q", "u", "upn", "target", "redirect", "redir", "r", "link"):
            if k in qs and qs[k]:
                cand = unquote(unquote(qs[k][0]))
                if cand.startswith(("http://", "https://")):
                    return cand
    except Exception:
        pass
    return None


def _unwrap_sendgrid(u: str) -> str:
    """Decode SendGrid `ls/click?upn=...` tracking links to the real target."""
    if "sendgrid.net/ls/click" not in (u or ""):
        return u
    try:
        p = urlparse(u)
        qs = parse_qs(p.query or "")
        raw = (qs.get("upn", [""])[0] or "").strip()
        if not raw:
            return u
        raw = unquote(unquote(raw))
        # Decode common %-like encodings used inside upn
        for a, b in (("-2F", "/"), ("-3D", "="), ("-26", "&"), ("-3A", ":"), ("-2B", "+"), ("-20", " ")):
            raw = raw.replace(a, b)
        # Prefer an .xlsx, else a plausible download host
        cands = re.findall(r'https?://[^\s<>"\']+', raw)
        for c in cands:
            if c.lower().endswith(".xlsx"):
                logger.info(f"  Found direct .xlsx link in SendGrid: {c[:100]}")
                return c
        for c in cands:
            lc = c.lower()
            if any(k in lc for k in ("infloww", "amazonaws.com", "cloudfront", "storage.googleapis.com", "download")):
                logger.info(f"  Found download host in SendGrid: {c[:100]}")
                return c
        return cands[0] if cands else u
    except Exception as e:
        logger.warning(f"  SendGrid unwrap failed: {e}")
        return u


def _meta_refresh_url(html: str, base_url: str) -> Optional[str]:
    """Extract URL from meta refresh tag."""
    soup = BeautifulSoup(html, "html.parser")
    for meta in soup.find_all("meta"):
        if (meta.get("http-equiv") or "").lower() == "refresh":
            content = meta.get("content") or ""
            m = re.search(r'url=([^;]+)', content, flags=re.I)
            if m:
                return urljoin(base_url, m.group(1).strip(' "\''))
    og = soup.find("meta", attrs={"property": "og:url", "content": True})
    if og and (og["content"] or "").lower().endswith(".xlsx"):
        return urljoin(base_url, og["content"])
    return None


def _js_redirect_url(html: str, base_url: str) -> Optional[str]:
    """Extract URL from JavaScript redirects."""
    pats = [
        r"location\.(?:href|assign|replace)\s*=\s*['\"]([^'\"]+?\.xlsx[^'\"]*)['\"]",
        r"window\.open\(\s*['\"]([^'\"]+?\.xlsx[^'\"]*)['\"]",
    ]
    for pat in pats:
        m = re.search(pat, html, flags=re.I)
        if m:
            return urljoin(base_url, m.group(1))
    m = re.search(r'https?://[^\s\'"<>]+?\.xlsx[^\s\'"<>]*', html, flags=re.I)
    return m.group(0) if m else None


def _anchor_xlsx_url(html: str, base_url: str) -> Optional[str]:
    """Find .xlsx links in anchor tags."""
    soup = BeautifulSoup(html, "html.parser")
    # <a href="...xlsx">
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".xlsx"):
            return urljoin(base_url, href)
    # Button with onclick
    for btn in soup.find_all("button"):
        oc = (btn.get("onclick") or "")
        m = re.search(r"location\.href\s*=\s*['\"]([^'\"]+?\.xlsx)[\"']", oc, flags=re.I)
        if m:
            return urljoin(base_url, m.group(1))
    return None


def _is_xlsx(first4: bytes, content_type: str, url: str) -> bool:
    """Check if content is an XLSX file."""
    if not first4:
        return False
    ct = (content_type or "").lower()
    if "officedocument.spreadsheetml.sheet" in ct:
        return first4.startswith(XLSX_MAGIC)
    if "application/zip" in ct or "application/octet-stream" in ct:
        return first4.startswith(XLSX_MAGIC)
    if url.lower().endswith(".xlsx"):
        return first4.startswith(XLSX_MAGIC)
    return False


def download_file(url: str, page_name: str, msg_id: str, download_dir: str = "/tmp") -> str:
    """
    Download Excel file from URL with advanced redirect handling.
    
    Args:
        url: Download URL (may be wrapped by SendGrid)
        page_name: Creator page name for filename
        msg_id: Gmail message ID
        download_dir: Directory to save file
    
    Returns:
        Path to downloaded file
    
    Raises:
        DownloadError: If download fails
    """
    if not url:
        raise DownloadError("No URL provided for download")

    os.makedirs(download_dir, exist_ok=True)
    safe = safe_filename(page_name or "Unknown")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_path = os.path.join(download_dir, f"{safe}_{timestamp}.xlsx")

    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream;q=0.9,text/html;q=0.5,*/*;q=0.1",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://mail.google.com/"
    })

    logger.info(f"  Starting download process for {page_name}")
    
    # Unwrap obvious wrappers
    original_url = url
    url = _unwrap_sendgrid(url) or url
    if url != original_url:
        logger.info(f"  Unwrapped SendGrid URL")
    
    inner = _extract_param_url(url)
    if inner:
        logger.info(f"  Extracted inner URL from params")
        url = inner

    # First attempt - direct download
    logger.info(f"  Attempting direct download...")
    try:
        r = sess.get(url, allow_redirects=True, timeout=30, stream=True)
        r.raise_for_status()
        
        # Stream and check first chunk
        it = r.iter_content(8192)
        first = next(it, b"")
        
        if _is_xlsx(first, r.headers.get("Content-Type", ""), r.url):
            logger.info(f"  ✅ Got XLSX on first attempt!")
            with open(local_path, "wb") as f:
                if first:
                    f.write(first)
                for chunk in it:
                    if chunk:
                        f.write(chunk)
            return local_path
        
        # If not XLSX, it's probably HTML with redirects
        logger.info(f"  Got HTML landing page, parsing for redirects...")
        try:
            body = (first + b"".join(it)).decode(r.encoding or "utf-8", "replace")
        except Exception:
            body = ""
        
        # Check ?url= param on the final URL
        p = urlparse(r.url)
        qs = parse_qs(p.query or "")
        if "url" in qs and qs["url"]:
            cand = unquote(unquote(qs["url"][0]))
            logger.info(f"  Found URL in query params, trying: {cand[:100]}")
            r2 = sess.get(cand, allow_redirects=True, timeout=30, stream=True)
            r2.raise_for_status()
            it2 = r2.iter_content(8192)
            f2 = next(it2, b"")
            if _is_xlsx(f2, r2.headers.get("Content-Type", ""), r2.url):
                logger.info(f"  ✅ Got XLSX from query param URL!")
                with open(local_path, "wb") as f:
                    if f2: f.write(f2)
                    for c in it2:
                        if c: f.write(c)
                return local_path
        
        # Parse HTML for various redirect types
        for fn, name in [
            (_anchor_xlsx_url, "anchor tag"),
            (_meta_refresh_url, "meta refresh"),
            (_js_redirect_url, "JavaScript redirect")
        ]:
            nxt = fn(body, r.url)
            if not nxt:
                continue
            logger.info(f"  Found {name}: {nxt[:100]}")
            r3 = sess.get(nxt, allow_redirects=True, timeout=30, stream=True)
            r3.raise_for_status()
            it3 = r3.iter_content(8192)
            f3 = next(it3, b"")
            if _is_xlsx(f3, r3.headers.get("Content-Type", ""), r3.url):
                logger.info(f"  ✅ Got XLSX from {name}!")
                with open(local_path, "wb") as f:
                    if f3: f.write(f3)
                    for c in it3:
                        if c: f.write(c)
                return local_path
        
        # Try JSON response
        try:
            data = json.loads(body)
            if isinstance(data, dict):
                for k in ("url", "href", "downloadUrl", "download_url", "file_url"):
                    v = data.get(k)
                    if isinstance(v, str) and v.startswith(("http://", "https://")):
                        logger.info(f"  Found URL in JSON['{k}']: {v[:100]}")
                        r4 = sess.get(v, allow_redirects=True, timeout=30, stream=True)
                        r4.raise_for_status()
                        it4 = r4.iter_content(8192)
                        f4 = next(it4, b"")
                        if _is_xlsx(f4, r4.headers.get("Content-Type", ""), r4.url):
                            logger.info(f"  ✅ Got XLSX from JSON URL!")
                            with open(local_path, "wb") as f:
                                if f4: f.write(f4)
                                for c in it4:
                                    if c: f.write(c)
                            return local_path
        except Exception:
            pass
        
        raise DownloadError(f"Could not find XLSX file after following all redirects for {page_name}")
        
    except requests.RequestException as e:
        raise DownloadError(f"Network error downloading file: {e}")
    except Exception as e:
        raise DownloadError(f"Unexpected error: {e}")


def validate_excel_file(file_path: str) -> bool:
    """Validate that file is a valid Excel file."""
    try:
        # Check file exists and has content
        if not os.path.exists(file_path):
            return False
        
        file_size = os.path.getsize(file_path)
        if file_size < 1000:  # Excel files are usually > 1KB
            logger.warning(f"File too small: {file_size} bytes")
            return False
        
        # Check magic bytes
        with open(file_path, 'rb') as f:
            header = f.read(4)
            if not header.startswith(XLSX_MAGIC):
                logger.warning(f"Invalid file header: {header.hex()}")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating file: {e}")
        return False