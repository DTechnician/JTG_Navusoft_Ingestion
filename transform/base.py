from datetime import datetime

def enrich(rows, entity):
    extracted_at = datetime.utcnow().isoformat()
    for r in rows:
        r["_entity"] = entity
        r["_extracted_at"] = extracted_at
    return rows


