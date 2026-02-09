from extract.navusoft_client import NavusoftClient
import yaml

# Load config
with open("config/navusoft.yml") as f:
    cfg = yaml.safe_load(f)

client = NavusoftClient(
    base_url=cfg["api"]["base_url"],
    runquery_endpoint=cfg["api"]["runquery_endpoint"],
    sessionId=cfg["api"]["sessionId"]
)

client.test_connection()
