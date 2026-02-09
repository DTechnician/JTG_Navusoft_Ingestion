import requests

url = "https://johntogo.navusoft.net/rest/external/navu/runquery"

HEADERS: {
"sessionId": "External NkM3MzdCQ0ItQzNFNC00RDk5LUI2MEItQzdCRUY1MkEyMzAyOjEwNDA=",
"Content-Type": "application/json",
"Accept": "application/json"
}
PAYLOAD: {
  "topCount": 500,
  "rootEntity": {
    "id": "v_query_workorder",
    "entityId": "v_query_workorder",
    "alias": ""
  },
  "query": {},
  "displayFields": [
    {
      "queryBuilderEntityId": "v_query_workorder",
      "columnName": "workordernumber",
      "columnDisplayName": "workordernumber",
      "displaysequence": 1
    },
    {
      "queryBuilderEntityId": "v_query_workorder",
      "columnName": "siteservice_id",
      "columnDisplayName": "siteservice_id",
      "displaysequence": 2
    },
    {
      "queryBuilderEntityId": "v_query_workorder",
      "columnName": "site_id",
      "columnDisplayName": "site_id",
      "displaysequence": 3
    }
  ]
}

resp = requests.post(url, headers=headers, json=payload)
print("Status:", resp.status_code)
print(resp.text)

