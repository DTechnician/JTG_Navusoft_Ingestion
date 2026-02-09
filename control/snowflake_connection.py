import snowflake.connector
from cryptography.hazmat.primitives import serialization

class SnowflakeConnection:
    def __init__(self, cfg: dict):
        with open(cfg["private_key_path"], "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None
            )

        private_key_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        self.conn = snowflake.connector.connect(
            user=cfg["user"],
            account=cfg["account"],
            private_key=private_key_der,
            warehouse=cfg["warehouse"],
            database=cfg["database"],
            role=cfg["role"]
        )

    def cursor(self):
        return self.conn.cursor()

    def close(self):
        self.conn.close()
