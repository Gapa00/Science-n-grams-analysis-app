# File: app/preprocessing/resolver.py
import json, os
import requests
from settings import CacheFiles, APIConfig

class SubfieldHierarchyResolver:
    def __init__(self):
        self.cache_path = CacheFiles.SUBFIELDS_CACHE_PATH
        self.cache = self._load_cache()

    def _load_cache(self):
        if os.path.exists(self.cache_path):
            with open(self.cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save_cache(self):
        with open(self.cache_path, "w", encoding="utf-8") as f:
            json.dump(self.cache, f, indent=2)

    def _fetch_all_subfields(self):
        subfields = []
        page = 1
        while True:
            url = f"{APIConfig.OPENALEX_SUBFIELDS_BASE}?page={page}&per_page={APIConfig.MAX_PER_PAGE}"
            try:
                response = requests.get(
                    url,
                    headers={"User-Agent": APIConfig.USER_AGENT},
                    timeout=APIConfig.REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                data = response.json()
                if "results" not in data:
                    break
                subfields.extend(data["results"])
                if len(data["results"]) < APIConfig.MAX_PER_PAGE:
                    break
                page += 1
            except Exception as e:
                print(f"❌ Error fetching OpenAlex subfields: {e}")
                break
        return subfields

    def resolve_subfields(self, subfield_urls: set) -> dict:
        uncached = [url for url in subfield_urls if url not in self.cache]
        if uncached:
            fetched = self._fetch_all_subfields()
            for entry in fetched:
                sid = entry.get("id")
                self.cache[sid] = {
                    "subfield": entry.get("display_name"),
                    "field": entry.get("field", {}).get("display_name", ""),
                    "domain": entry.get("domain", {}).get("display_name", "")
                }
            self._save_cache()

        return {url: self.cache.get(url, {"subfield": "", "field": "", "domain": ""}) for url in subfield_urls}
