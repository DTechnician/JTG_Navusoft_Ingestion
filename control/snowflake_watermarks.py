import snowflake.connector
from datetime import timedelta, datetime
from typing import Optional


class SnowflakeWatermarks:
    """
    Soft watermark manager.

    Watermarks are used ONLY as query hints.
    They must never be treated as authoritative cutoffs.
    """

    def __init__(self, sf_conn):
        self.conn = sf_conn.conn

    def get_soft(
        self,
        source_system: str,
        entity_name: str,
        overlap: Optional[timedelta] = None
    ):
        """
        Returns a soft watermark value.

        If overlap is provided, the returned value is:
            LAST_VALUE - overlap

        If no watermark exists, returns None.
        """
        sql = """
            SELECT LAST_VALUE
            FROM CONTROL.INGEST_WATERMARKS
            WHERE SOURCE_SYSTEM = %s
              AND ENTITY_NAME = %s
        """

        with self.conn.cursor() as cur:
            cur.execute(sql, (source_system, entity_name))
            row = cur.fetchone()

        if not row or not row[0]:
            return None

        last_value = row[0]

        # Apply overlap window (softening)
        if overlap and isinstance(last_value, datetime):
            return last_value - overlap

        return last_value

    def record_observed_max(
        self,
        source_system: str,
        entity_name: str,
        incremental_field: str,
        observed_max_value
    ):
        """
        Records the maximum value OBSERVED in a run.

        This does NOT mean ingestion is complete.
        """
        sql = """
        MERGE INTO CONTROL.INGEST_WATERMARKS t
        USING (
            SELECT
                %s AS SOURCE_SYSTEM,
                %s AS ENTITY_NAME,
                %s AS INCREMENTAL_FIELD,
                %s AS LAST_VALUE
        ) s
        ON t.SOURCE_SYSTEM = s.SOURCE_SYSTEM
           AND t.ENTITY_NAME = s.ENTITY_NAME
        WHEN MATCHED AND s.LAST_VALUE > t.LAST_VALUE THEN
            UPDATE SET
                LAST_VALUE = s.LAST_VALUE,
                UPDATED_AT = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (
                SOURCE_SYSTEM,
                ENTITY_NAME,
                INCREMENTAL_FIELD,
                LAST_VALUE,
                UPDATED_AT
            )
            VALUES (
                s.SOURCE_SYSTEM,
                s.ENTITY_NAME,
                s.INCREMENTAL_FIELD,
                s.LAST_VALUE,
                CURRENT_TIMESTAMP
            )
        """

        with self.conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    source_system,
                    entity_name,
                    incremental_field,
                    observed_max_value,
                ),
            )
