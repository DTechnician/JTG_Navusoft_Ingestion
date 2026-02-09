import json
import time
import requests
from requests.exceptions import ReadTimeout


class NavusoftClient:

    def __init__(self, base_url, runquery_endpoint, sessionId):
        self.base_url = base_url.rstrip("/")
        self.runquery_endpoint = runquery_endpoint
        self.sessionId = sessionId

        if not sessionId:
            raise ValueError("API key must be provided for API key authentication")

    # -------------------------------------------------
    # Entity metadata
    # -------------------------------------------------
    def _get_entity_columns(self, entity_name):
        """
        Fetch column list for an entity using Navusoft entities metadata
        """
        url = f"{self.base_url}/rest/external/navu/entities/"
        headers = {
            "sessionId": self.sessionId,
            "Accept": "application/json"
        }

        response = requests.get(url, headers=headers, timeout=(10, 60))
        response.raise_for_status()
        entities = response.json()

        entity_info = next(
            (e for e in entities if e.get("id") == entity_name),
            None
        )

        if not entity_info:
            raise ValueError(f"Entity '{entity_name}' not found in Navusoft metadata")

        fields = entity_info.get("fields", [])
        columns = [f["columnName"] for f in fields if "columnName" in f]

        if not columns:
            raise ValueError(f"No columns found for entity '{entity_name}'")

        return columns

    # -------------------------------------------------
    # Query helpers
    # -------------------------------------------------
    def _build_display_fields(self, entity, entity_columns):
        return [
            {
                "queryBuilderEntityId": entity,
                "columnName": col,
                "columnDisplayName": col,
                "displaysequence": idx
            }
            for idx, col in enumerate(entity_columns, start=1)
        ]

    # -------------------------------------------------
    # Data fetch (with retries + batching)
    # -------------------------------------------------
    def fetch(
        self,
        entity,
        entity_columns=None,
        filter_expr=None,
        retries=3
    ):
        """
        Fetch data from Navusoft with:
        - large read timeout
        - batching via top_count
        - retry + backoff
        """
        if not entity_columns:
            entity_columns = self._get_entity_columns(entity)

        headers = {
            "sessionId": self.sessionId,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        payload = {
            "rootEntity": {
                "id": entity,
                "entityId": entity,
                "alias": ""
            },
            "query": {},
            "displayFields": self._build_display_fields(entity, entity_columns)
        }

        if filter_expr:
            payload["query"]["filter"] = filter_expr

        url = f"{self.base_url}{self.runquery_endpoint}"

        for attempt in range(1, retries + 1):
            try:
                response = requests.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=(10, 300)  # connect, read
                )
                response.raise_for_status()

                return response.json().get("resultData", [])

            except ReadTimeout:
                print(
                    f"⚠ Read timeout fetching '{entity}' "
                    f"(attempt {attempt}/{retries})"
                )
                if attempt == retries:
                    raise
                time.sleep(5 * attempt)

    # -------------------------------------------------
    # Connection test
    # -------------------------------------------------
    def test_connection(self, entity="v_query_workorder"):
        print(f"Testing connection for entity '{entity}'...")
        try:
            rows = self.fetch(entity=entity, top_count=1)
            if rows:
                print(f"✅ Connection successful. Fetched {len(rows)} row(s).")
            else:
                print("⚠ Connection successful, but no data returned.")
        except Exception as e:
            print(f"❌ Connection failed: {e}")
