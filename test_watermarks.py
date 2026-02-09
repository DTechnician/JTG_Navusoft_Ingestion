import snowflake.connector
from cryptography.hazmat.primitives import serialization

with open("snowflake_key.pem", "rb") as key:
    private_key = serialization.load_pem_private_key(
        key.read(),
        password=None,
    )

pkb = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

ctx = snowflake.connector.connect(
    user="DTECHNICIAN",
    account="CTYLHGR-EWC26814",
    private_key=pkb,
    warehouse="COMPUTE_WH",
    database="DB_LANDING_NAVUSOFT",
    schema="CONTROL",
)

cs = ctx.cursor()
cs.execute("SELECT CURRENT_USER(), CURRENT_TIMESTAMP()")
print(cs.fetchone())

cs.close()
ctx.close()

print("✅ Key-pair authentication works!")
