import uuid
from datetime import datetime
# from typing import Any, Dict, List

from lib.pg import PgConnect

class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def cdm_user_category_counters_insert(self,
                         user_id:uuid,
                         category_id:uuid,
                         category_name:str
                         ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_category_counters (
                        user_id,
                        category_id,
                        category_name,
                        order_cnt
                        )
                        VALUES(
                        %(user_id)s, 
                        %(category_id)s, 
                        %(category_name)s, 
                        1
                        )
                        ON CONFLICT (user_id, category_id) DO UPDATE
                        SET
                            order_cnt = cdm.user_category_counters.order_cnt + 1,
                            category_name = EXCLUDED.category_name
                    """,
                    {
                        'user_id': user_id,
                        'category_id': category_id,
                        'category_name': category_name
                    }
                )

    def cdm_user_product_counters_insert(self,
                         user_id:uuid,
                         category_id:uuid,
                         product_name:str
                         ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_product_counters (
                        user_id,
                        product_id,
                        product_name,
                        order_cnt
                        )
                        VALUES(
                        %(user_id)s, 
                        %(product_id)s, 
                        %(product_name)s, 
                        1
                        )
                        ON CONFLICT (user_id, product_id) DO UPDATE
                        SET
                            order_cnt = cdm.user_product_counters.order_cnt + 1,
                            product_name = EXCLUDED.product_name
                    """,
                    {
                        'user_id': user_id,
                        'product_id': category_id,
                        'product_name': product_name
                    }
                )