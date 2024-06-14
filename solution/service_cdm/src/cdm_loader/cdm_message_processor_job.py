from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect.kafka_connectors import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger,
                 ) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = batch_size #100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            self._cdm_repository.cdm_user_category_counters_insert(
                UUID(msg["user_id"]),
                UUID(msg["category_id"]),
                msg["category_name"],
            )

            self._logger.info(f"{datetime.utcnow()}: cdm_user_category_counters loaded")

            self._cdm_repository.cdm_user_product_counters_insert(
                UUID(msg["user_id"]),
                UUID(msg["product_id"]),
                msg["product_name"],
            )

            self._logger.info(f"{datetime.utcnow()}: cdm_user_product_counters loaded")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
