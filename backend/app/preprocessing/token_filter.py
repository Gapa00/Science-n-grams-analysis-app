# File: app/preprocessing/token_filter.py

import re
from typing import Set, Dict, List, Tuple

class SafeTokenFilter:
    """
    Conservative token filter that only removes n-grams consisting entirely of meaningless artifacts.
    Uses n-gram-level filtering instead of word-level filtering.
    """
    
    def __init__(self):
        # ONLY purely HTML artifacts (very conservative)
        self.html_xml_artifacts = {
            "lt", "gt", "amp", "nbsp", "quot", "href", "src", "html", "xml", 
            "br", "div", "span"
        }
        
        # MathML patterns - clearly artifacts
        self.mathml_pattern = re.compile(r'^(mml:|m:)[a-z]+$')
        self.mathml_tokens = {
            "mathml", "xmlns", "xmlns:mml", "xmlns:xlink", "mathvariant",
            "tex", "inline", "xmlns", "mml", "xlink", "x0d"
        }
        
        # URL fragments - clearly not scientific
        self.url_fragments = {
            "http", "https", "www", "w3", "doi", "url", "uri", "org", "com", "edu", "gov", "net",
            "informationhttps", "2020https", "2017https", "2016https", "2021https", 
            "2019https", "2018https", "2023https", "2022https"
        }
        
        # VERY conservative foreign stopwords - only the most obvious, high-frequency ones
        # Removed ALL potentially scientific terms like "data", "model", "analysis", etc.
        self.safe_foreign_stopwords = {
            # Spanish - only the most common articles/prepositions
            "de", "la", "el", "en", "del", "los", "las", "para", "por", "con", "al", "se", "le",
            
            # French - only the most common articles/prepositions  
            "le", "de", "la", "les", "du", "des", "en", "dans", "sur", "avec", "pour", "par",
            
            # German - only the most common articles/prepositions
            "der", "die", "das", "den", "dem", "des", "und", "von", "zu", "mit", "bei", "nach",
            
            # Italian - only the most common articles/prepositions
            "il", "di", "la", "del", "da", "al", "con", "per", "nella", "dalla", "sulla",
            
            # Portuguese - only the most common articles/prepositions
            "de", "da", "do", "em", "para", "por", "com", "na", "no", "dos", "das",
            
            # Dutch - only the most common articles/prepositions
            "de", "het", "van", "een", "in", "op", "voor", "met", "bij", "aan", "door",
            
            # Indonesian - ONLY linguistic particles, NO scientific terms
            "yang", "dan", "di", "ke", "dari", "dalam", "untuk", "pada", "dengan", "oleh",
            "adalah", "akan", "telah", "sudah", "juga", "ini", "itu"
        }
        
        # Combine all stopword categories for easy checking
        self.all_stopwords = (
            self.html_xml_artifacts | self.mathml_tokens | 
            self.url_fragments | self.safe_foreign_stopwords
        )
        
        # Non-Latin script pattern (artifacts from non-English papers)
        self.non_latin_pattern = re.compile(r'[\u0E00-\u0E7F\u1100-\u11FF\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF\u0600-\u06FF]')
        
        # URL-like patterns
        self.url_pattern = re.compile(r'(://|\.org|\.com|\.edu|\.gov|\.net)')
    
    def is_stopword_token(self, token: str) -> bool:
        """Check if a single token is a stopword/artifact."""
        if not token or not isinstance(token, str):
            return True
            
        token_lower = token.lower().strip()
        
        # Empty or whitespace-only
        if not token_lower:
            return True
            
        # HTML/XML artifacts
        if token_lower in self.html_xml_artifacts:
            return True
            
        # MathML patterns and tokens
        if (self.mathml_pattern.match(token_lower) or 
            token_lower in self.mathml_tokens):
            return True
            
        # URL fragments and patterns
        if (token_lower in self.url_fragments or 
            self.url_pattern.search(token_lower)):
            return True
            
        # Safe foreign stopwords
        if token_lower in self.safe_foreign_stopwords:
            return True
            
        # Non-Latin script characters
        if self.non_latin_pattern.search(token_lower):
            return True
            
        return False
    
    def filter_ngram(self, ngram: str) -> str:
        """
        N-gram level filtering: Keep entire n-gram if it contains ANY non-stopword,
        drop entire n-gram if it consists ONLY of stopwords.
        """
        if not ngram:
            return ""
            
        words = ngram.split()
        if not words:
            return ""
        
        # Check if ANY word is NOT a stopword
        has_non_stopword = False
        for word in words:
            if not self.is_stopword_token(word):
                has_non_stopword = True
                break
        
        # If n-gram contains at least one non-stopword, keep the entire n-gram
        if has_non_stopword:
            return ngram
        
        # If ALL words are stopwords, drop the entire n-gram
        return ""
    
    def filter_ngram_with_tracking(self, ngram: str) -> Tuple[str, bool]:
        """
        Filter n-gram and track if it was changed.
        
        Returns:
            (filtered_ngram, was_dropped): 
            - filtered_ngram: The original n-gram or empty string
            - was_dropped: True if the n-gram was completely dropped
        """
        if not ngram:
            return "", True
            
        filtered_result = self.filter_ngram(ngram)
        was_dropped = (filtered_result == "")
        
        return filtered_result, was_dropped
    
    def get_filter_stats(self) -> Dict[str, int]:
        """Return statistics about filter rules."""
        return {
            "html_xml_artifacts": len(self.html_xml_artifacts),
            "mathml_tokens": len(self.mathml_tokens),
            "url_fragments": len(self.url_fragments),
            "safe_foreign_stopwords": len(self.safe_foreign_stopwords),
            "total_explicit_tokens": len(self.all_stopwords)
        }