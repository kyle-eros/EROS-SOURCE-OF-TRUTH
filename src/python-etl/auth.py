# src/auth.py
from __future__ import annotations

import os
import json
import logging
from googleapiclient.discovery import build
from google.auth import default as adc
from google.auth.transport.requests import Request
from google.auth import iam
from google.oauth2 import service_account

from config import Config

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
TOKEN_URI = "https://oauth2.googleapis.com/token"

def get_gmail_service(cfg: Config = None):
    """
    Build and return an authorized Gmail API client.
    
    Args:
        cfg: Config instance. If None, creates a new one.
    
    Order of auth preference:
      1) Secret injected into env var GMAIL_SA_JSON (Secret Manager --set-secrets)
      2) Service account JSON file at GMAIL_SERVICE_ACCOUNT_FILE (if present)
      3) ADC + with_subject (if directly supported)
      4) ADC + IAM Signer fallback using SERVICE_ACCOUNT_EMAIL (domain-wide delegation)
    """
    # Use provided config or create new instance
    if cfg is None:
        cfg = Config()
    
    if not cfg.TARGET_GMAIL_USER:
        raise ValueError("TARGET_GMAIL_USER must be set in configuration")
    
    logger.info(f"Attempting to authenticate for Gmail user: {cfg.TARGET_GMAIL_USER}")
    
    # 1) Secret injected via env (recommended for Cloud Run)
    json_env = os.getenv("GMAIL_SA_JSON")
    if json_env:
        try:
            logger.debug("Trying authentication via GMAIL_SA_JSON environment variable")
            info = json.loads(json_env)
            creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            creds = creds.with_subject(cfg.TARGET_GMAIL_USER)
            logger.info("✅ Authenticated via Secret Manager (GMAIL_SA_JSON)")
            return build("gmail", "v1", credentials=creds, cache_discovery=False)
        except Exception as e:
            logger.debug(f"GMAIL_SA_JSON auth failed: {e}")
            # Fall through to other methods
    
    # 2) Service account key file on disk
    sa_file = cfg.GMAIL_SERVICE_ACCOUNT_FILE
    if sa_file and os.path.isfile(sa_file):
        try:
            logger.debug(f"Trying authentication via service account file: {sa_file}")
            with open(sa_file, "r", encoding="utf-8") as f:
                info = json.load(f)
            creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            creds = creds.with_subject(cfg.TARGET_GMAIL_USER)
            logger.info(f"✅ Authenticated via service account file: {sa_file}")
            return build("gmail", "v1", credentials=creds, cache_discovery=False)
        except Exception as e:
            logger.debug(f"Service account file auth failed: {e}")
    
    # 3) ADC: Application Default Credentials
    try:
        logger.debug("Trying ADC with direct subject delegation")
        base_creds, _ = adc(scopes=SCOPES)
        creds = base_creds.with_subject(cfg.TARGET_GMAIL_USER)
        logger.info("✅ Authenticated via ADC with direct delegation")
        return build("gmail", "v1", credentials=creds, cache_discovery=False)
    except Exception as e:
        logger.debug(f"ADC direct delegation failed: {e}")
    
    # 4) ADC + IAM Signer fallback (for domain-wide delegation)
    sa_email = cfg.SERVICE_ACCOUNT_EMAIL
    if not sa_email:
        raise RuntimeError(
            "Unable to authenticate. Please provide one of:\n"
            "1. GMAIL_SA_JSON environment variable (recommended)\n"
            "2. GMAIL_SERVICE_ACCOUNT_FILE path to JSON key\n"
            "3. SERVICE_ACCOUNT_EMAIL for IAM Signer method"
        )
    
    try:
        logger.debug(f"Trying ADC + IAM Signer with service account: {sa_email}")
        
        # Get ADC credentials with IAM permissions
        base_creds, _ = adc(scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/iam"
        ])
        
        # Refresh if needed
        req = Request()
        if hasattr(base_creds, "valid") and not base_creds.valid:
            base_creds.refresh(req)
        
        # Create IAM signer
        signer = iam.Signer(req, base_creds, sa_email)
        
        # Create delegated credentials
        creds = service_account.Credentials(
            signer=signer,
            service_account_email=sa_email,
            token_uri=TOKEN_URI,
            scopes=SCOPES,
            subject=cfg.TARGET_GMAIL_USER,
        )
        
        logger.info(f"✅ Authenticated via ADC + IAM Signer with {sa_email}")
        return build("gmail", "v1", credentials=creds, cache_discovery=False)
        
    except Exception as e:
        logger.error(f"All authentication methods failed. Last error: {e}")
        raise RuntimeError(
            f"Failed to authenticate Gmail API for user {cfg.TARGET_GMAIL_USER}. "
            f"Last attempted method: IAM Signer with {sa_email}"
        ) from e


def test_gmail_connection(cfg: Config = None):
    """Test Gmail connection by fetching user profile"""
    try:
        service = get_gmail_service(cfg)
        profile = service.users().getProfile(userId='me').execute()
        logger.info(f"✅ Gmail connection successful! Messages: {profile.get('messagesTotal', 0)}")
        return True
    except Exception as e:
        logger.error(f"❌ Gmail connection failed: {e}")
        return False