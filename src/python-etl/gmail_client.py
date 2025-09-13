# src/gmail_client.py
"""
Gmail API client for fetching OnlyFans mass message history reports.
Handles email fetching, parsing, and link extraction.
"""

import base64
import re
import logging
from typing import Optional, Tuple, List, Dict, Any
from datetime import datetime, timedelta
from email import policy
from email.parser import BytesParser

from bs4 import BeautifulSoup
from googleapiclient.errors import HttpError

from config import Config
from auth import get_gmail_service
from exceptions import (
    GmailError, 
    NoMessagesFoundError, 
    MessageFetchError,
    NoDownloadUrlFound
)

logger = logging.getLogger(__name__)


class GmailClient:
    """
    Gmail API client for fetching infloww.com report emails.
    """
    
    def __init__(self, cfg: Config):
        """
        Initialize Gmail client with configuration.
        
        Args:
            cfg: Configuration object with Gmail settings
        """
        self.cfg = cfg
        self.service = get_gmail_service(cfg)
        logger.info(f"Gmail client initialized for {cfg.TARGET_GMAIL_USER}")
    
    def list_report_messages(
        self, 
        max_results: Optional[int] = None,
        after_timestamp: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        List all messages matching the search query.
        
        Args:
            max_results: Maximum number of messages to return
            after_timestamp: Only return messages after this timestamp (milliseconds)
        
        Returns:
            List of message objects with id and threadId
        
        Raises:
            NoMessagesFoundError: If no messages match the query
            GmailError: For other Gmail API errors
        """
        try:
            user_id = "me"
            query = self.cfg.GMAIL_SEARCH_QUERY
            
            # Add timestamp filter if provided
            if after_timestamp:
                # Convert milliseconds to seconds for Gmail query
                after_date = datetime.fromtimestamp(after_timestamp / 1000)
                date_str = after_date.strftime("%Y/%m/%d")
                query += f" after:{date_str}"
            
            logger.info(f"Searching Gmail with query: {query}")
            
            messages = []
            page_token = None
            
            while True:
                try:
                    # Make API call
                    if page_token:
                        response = self.service.users().messages().list(
                            userId=user_id,
                            q=query,
                            pageToken=page_token
                        ).execute()
                    else:
                        response = self.service.users().messages().list(
                            userId=user_id,
                            q=query
                        ).execute()
                    
                    # Add messages from this page
                    if 'messages' in response:
                        messages.extend(response['messages'])
                        logger.debug(f"Found {len(response['messages'])} messages on this page")
                    
                    # Check if we've hit max_results
                    if max_results and len(messages) >= max_results:
                        messages = messages[:max_results]
                        break
                    
                    # Check for next page
                    page_token = response.get('nextPageToken')
                    if not page_token:
                        break
                        
                except HttpError as e:
                    logger.error(f"Gmail API error: {e}")
                    raise GmailError(f"Failed to list messages: {e}")
            
            if not messages:
                logger.warning("No messages found matching the search query")
                raise NoMessagesFoundError("No infloww.com report emails found")
            
            logger.info(f"Found {len(messages)} total messages")
            return messages
            
        except NoMessagesFoundError:
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing messages: {e}")
            raise GmailError(f"Failed to list messages: {e}")
    
    def fetch_message(self, message_id: str) -> Tuple[int, str]:
        """
        Fetch a specific message by ID.
        
        Args:
            message_id: Gmail message ID
        
        Returns:
            Tuple of (internal_date_ms, email_content_html)
        
        Raises:
            MessageFetchError: If message cannot be fetched
        """
        try:
            logger.debug(f"Fetching message {message_id}")
            
            user_id = "me"
            message = self.service.users().messages().get(
                userId=user_id,
                id=message_id,
                format='full'
            ).execute()
            
            # Get internal date (milliseconds since epoch)
            internal_date = int(message.get('internalDate', 0))
            
            # Extract HTML content
            html_content = self._extract_html_content(message)
            
            if not html_content:
                logger.warning(f"No HTML content found in message {message_id}")
                # Try to get plain text as fallback
                html_content = self._extract_plain_text(message)
            
            logger.debug(f"Fetched message {message_id} with date {internal_date}")
            return internal_date, html_content
            
        except HttpError as e:
            logger.error(f"Gmail API error fetching message {message_id}: {e}")
            raise MessageFetchError(message_id, str(e))
        except Exception as e:
            logger.error(f"Unexpected error fetching message {message_id}: {e}")
            raise MessageFetchError(message_id, str(e))
    
    def _extract_html_content(self, message: Dict[str, Any]) -> Optional[str]:
        """Extract HTML content from a Gmail message."""
        payload = message.get('payload', {})
        
        # Check for raw message
        if 'raw' in message:
            try:
                raw_data = base64.urlsafe_b64decode(message['raw'].encode('utf-8'))
                parser = BytesParser(policy=policy.default)
                email_msg = parser.parsebytes(raw_data)
                
                if email_msg.is_multipart():
                    for part in email_msg.iter_parts():
                        if part.get_content_type() == 'text/html':
                            return part.get_content()
                else:
                    if email_msg.get_content_type() == 'text/html':
                        return email_msg.get_content()
            except Exception as e:
                logger.debug(f"Failed to parse raw message: {e}")
        
        # Check payload body
        if payload.get('body', {}).get('data'):
            try:
                data = payload['body']['data']
                decoded = base64.urlsafe_b64decode(data.encode('utf-8'))
                return decoded.decode('utf-8', 'ignore')
            except Exception as e:
                logger.debug(f"Failed to decode body data: {e}")
        
        # Check parts
        parts = payload.get('parts', [])
        for part in parts:
            if part.get('mimeType') == 'text/html':
                data = part.get('body', {}).get('data')
                if data:
                    try:
                        decoded = base64.urlsafe_b64decode(data.encode('utf-8'))
                        return decoded.decode('utf-8', 'ignore')
                    except Exception as e:
                        logger.debug(f"Failed to decode part data: {e}")
            
            # Check nested parts
            if 'parts' in part:
                for nested_part in part['parts']:
                    if nested_part.get('mimeType') == 'text/html':
                        data = nested_part.get('body', {}).get('data')
                        if data:
                            try:
                                decoded = base64.urlsafe_b64decode(data.encode('utf-8'))
                                return decoded.decode('utf-8', 'ignore')
                            except Exception as e:
                                logger.debug(f"Failed to decode nested part: {e}")
        
        return None
    
    def _extract_plain_text(self, message: Dict[str, Any]) -> Optional[str]:
        """Extract plain text content as fallback."""
        payload = message.get('payload', {})
        
        # Check parts for plain text
        parts = payload.get('parts', [])
        for part in parts:
            if part.get('mimeType') == 'text/plain':
                data = part.get('body', {}).get('data')
                if data:
                    try:
                        decoded = base64.urlsafe_b64decode(data.encode('utf-8'))
                        return decoded.decode('utf-8', 'ignore')
                    except Exception:
                        pass
        
        return None
    
    def extract_report_link_and_page(
        self, 
        email_content: str,
        message_id: str = None
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract download link and page name from email content.
        
        Args:
            email_content: HTML content of the email
            message_id: Message ID for logging
        
        Returns:
            Tuple of (download_url, page_name)
            
        Raises:
            NoDownloadUrlFound: If no download link is found
        """
        if not email_content:
            logger.warning(f"Empty email content for message {message_id}")
            raise NoDownloadUrlFound(message_id=message_id)
        
        # Parse HTML
        soup = BeautifulSoup(email_content, "html.parser")
        
        # Find download link
        download_url = self._find_download_link(soup)
        if not download_url:
            logger.warning(f"No download link found in message {message_id}")
            raise NoDownloadUrlFound(message_id=message_id)
        
        # Extract page name
        page_name = self._extract_page_name(soup, email_content)
        
        logger.info(f"Extracted: URL={download_url[:50]}..., Page='{page_name}'")
        return download_url, page_name
    
    def _find_download_link(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Find the download link in the email HTML.
        
        Priority:
        1. Links with text containing 'download'
        2. Links with infloww.com domain
        3. Any link that looks like a redirect/tracking link
        """
        # Look for links with 'download' in text
        for link in soup.find_all('a', href=True):
            link_text = (link.get_text() or '').strip().lower()
            if 'download' in link_text:
                logger.debug(f"Found download link with text: {link_text}")
                return link['href']
        
        # Look for infloww.com links
        for link in soup.find_all('a', href=True):
            href = link['href']
            if 'infloww.com' in href.lower():
                logger.debug(f"Found infloww.com link")
                return href
        
        # Look for tracking/redirect links (SendGrid, etc.)
        for link in soup.find_all('a', href=True):
            href = link['href']
            if any(domain in href.lower() for domain in ['sendgrid.net', 'click', 'track']):
                logger.debug(f"Found tracking link")
                return href
        
        # Last resort: return first link
        first_link = soup.find('a', href=True)
        if first_link:
            logger.debug(f"Using first link found as fallback")
            return first_link['href']
        
        return None
    
    def _extract_page_name(self, soup: BeautifulSoup, html_content: str) -> str:
        """
        Extract the page/model name from email content.
        
        Looking for patterns like:
        - "report you requested for [NAME] is now ready"
        - "mass message history report for [NAME]"
        - "OF mass message history report you requested for [NAME] Paid is now ready"
        """
        # Get all text from email
        email_text = soup.get_text(" ", strip=True)
        
        # Clean up the text
        email_text = " ".join(email_text.split())
        
        # Pattern list (order matters - most specific first)
        patterns = [
            # "report you requested for Titty Talia Paid is now ready"
            r'report\s+you\s+requested\s+for\s+(.+?)\s+is\s+now\s+ready',
            # "report for Titty Talia Paid is now"
            r'report\s+for\s+(.+?)\s+is\s+now',
            # "requested for Titty Talia Paid"
            r'requested\s+for\s+(.+?)(?:\s+is|\s+now|$)',
            # "history report for Titty Talia Paid"
            r'history\s+report\s+for\s+(.+?)(?:\s+is|\s+now|$)',
            # "for Titty Talia Paid"
            r'for\s+([A-Z][a-zA-Z\s]+(?:Paid|Free|VIP)?)\s+is',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, email_text, re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                # Clean up the name
                name = self._clean_page_name(name)
                if name and name != "Unknown":
                    logger.debug(f"Extracted page name: '{name}' using pattern: {pattern}")
                    return name
        
        # Fallback: Look for specific format in subject line
        # The subject might be in the email headers
        subject_match = re.search(
            r'Subject:.*?for\s+(.+?)\s+(?:is|now)', 
            html_content, 
            re.IGNORECASE
        )
        if subject_match:
            name = self._clean_page_name(subject_match.group(1))
            if name and name != "Unknown":
                logger.debug(f"Extracted page name from subject: '{name}'")
                return name
        
        logger.warning("Could not extract page name from email")
        return "Unknown"
    
    def _clean_page_name(self, name: str) -> str:
        """
        Clean the extracted page name.
        
        Examples:
        - "Titty Talia Paid" -> "Titty Talia Paid"
        - "Diana Grace's" -> "Diana Grace"
        """
        if not name:
            return "Unknown"
        
        # Remove possessive
        name = re.sub(r"[''']s\b", "", name)
        
        # Remove special characters but keep spaces and basic punctuation
        name = re.sub(r'[^\w\s\-]', '', name)
        
        # Clean up whitespace
        name = " ".join(name.split())
        
        # Ensure it's not empty
        if not name or name.lower() in ['unknown', 'none', 'null', '']:
            return "Unknown"
        
        return name
    
    def get_message_details(self, message_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a message.
        
        Returns dict with:
        - id: Message ID
        - threadId: Thread ID
        - labelIds: List of label IDs
        - subject: Email subject
        - from: Sender email
        - date: Internal date (ms)
        - snippet: Email snippet
        """
        try:
            message = self.service.users().messages().get(
                userId="me",
                id=message_id,
                format='metadata',
                metadataHeaders=['Subject', 'From', 'Date']
            ).execute()
            
            headers = {h['name']: h['value'] 
                      for h in message.get('payload', {}).get('headers', [])}
            
            return {
                'id': message.get('id'),
                'threadId': message.get('threadId'),
                'labelIds': message.get('labelIds', []),
                'subject': headers.get('Subject', ''),
                'from': headers.get('From', ''),
                'date': int(message.get('internalDate', 0)),
                'snippet': message.get('snippet', '')
            }
            
        except Exception as e:
            logger.error(f"Failed to get message details: {e}")
            return {}
    
    def mark_as_processed(self, message_id: str) -> bool:
        """
        Mark a message as processed by adding a label.
        
        Args:
            message_id: Gmail message ID
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create or get the "Processed" label
            label_name = "ETL_Processed"
            label_id = self._get_or_create_label(label_name)
            
            if not label_id:
                logger.warning(f"Could not create label {label_name}")
                return False
            
            # Add label to message
            self.service.users().messages().modify(
                userId="me",
                id=message_id,
                body={'addLabelIds': [label_id]}
            ).execute()
            
            logger.debug(f"Marked message {message_id} as processed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark message as processed: {e}")
            return False
    
    def _get_or_create_label(self, label_name: str) -> Optional[str]:
        """Get or create a Gmail label."""
        try:
            # List existing labels
            labels = self.service.users().labels().list(userId="me").execute()
            
            for label in labels.get('labels', []):
                if label['name'] == label_name:
                    return label['id']
            
            # Create new label
            label_object = {
                'name': label_name,
                'labelListVisibility': 'labelShow',
                'messageListVisibility': 'show'
            }
            
            created_label = self.service.users().labels().create(
                userId="me",
                body=label_object
            ).execute()
            
            logger.info(f"Created new label: {label_name}")
            return created_label['id']
            
        except Exception as e:
            logger.error(f"Failed to get/create label: {e}")
            return None