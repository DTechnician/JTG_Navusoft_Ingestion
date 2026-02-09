import requests

def get_navusoft_entities(base_url, session_id):
    """
    Fetch list of entities from Navusoft
    """
    url = f"{base_url}/rest/external/navu/entities/"
    headers = {
        "sessionId": session_id,
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    entities = response.json()

    # Return only entity IDs (entity names used by runquery)
    return [e.get("id") for e in entities if e.get("id")]

def _get_entity_columns(base_url, session_id, entity_name):
        """
        Get all columns for an entity dynamically using the entities endpoint
        """
        url = f"{base_url}/rest/external/navu/entities/"
        headers = {
            "sessionId": session_id,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        all_entities = response.json()

        # Find the entity
        entity_info = next(
            (e for e in all_entities if e.get("id") == entity_name or e.get("entityId") == entity_name),
            None
        )
        if not entity_info:
            raise ValueError(f"Entity '{entity_name}' not found in Navusoft metadata")

        # Extract field names
        columns = [f["columnName"] for f in entity_info.get("fields", [])]
        if not columns:
            raise ValueError(f"No columns found for entity '{entity_name}'")
        return columns
if __name__ == "__main__":
    BASE_URL = "https://johntogo.navusoft.net"
    SESSION_ID = "External NkM3MzdCQ0ItQzNFNC00RDk5LUI2MEItQzdCRUY1MkEyMzAyOjEwNDA="

    ##return entity list
    #entities = get_navusoft_entities(BASE_URL, SESSION_ID)
    #print(f"Found {len(entities)} entities:")
    #for e in entities:
    #    print(e)

    ##return column list
    entity_columns = _get_entity_columns(BASE_URL, SESSION_ID, 'v_query_account_and_site')
    for e in entity_columns:
        print(e)