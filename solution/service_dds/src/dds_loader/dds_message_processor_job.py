from datetime import datetime
from logging import Logger
import uuid


from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self,
                consumer: KafkaConsumer,
                producer: KafkaProducer,
                dds_repository: DdsRepository,
                batch_size: int,
                logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size #30
        self._logger = logger
        
    def run(self) -> None:

        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            if msg["object_type"] == "order":
                load_src = 'stg_order_events'
                # load_dt = datetime.utcnow()

                # Парсим заказ
                order = msg['payload']    

                order_id = order["id"]
                hash_order_id = uuid.uuid3(uuid.NAMESPACE_X500, str(order_id))
                order_dt = order["date"]
                order_cost = order["cost"]
                order_payment = order["payment"]
                order_status = order["status"]

                restaurant_id = order["restaurant"]["id"]
                hash_restaurant_id = uuid.uuid3(uuid.NAMESPACE_X500, str(restaurant_id))
                restaurant_name = order["restaurant"]["name"]

                user_id = order["user"]["id"]
                hash_user_id = uuid.uuid3(uuid.NAMESPACE_X500, str(user_id))
                user_name = order["user"]["name"]
                user_login = order["user"]["login"]
                
                products = order["products"]

                # Загружаем хабы, линки и сателлиты шапок заказов
                self._dds_repository.h_order_insert(
                    hash_order_id,
                    order_id,
                    order_dt,
                    datetime.utcnow(),
                    load_src
                )

                self._logger.info(f"{datetime.utcnow()}: h_order loaded")

                self._dds_repository.h_user_insert(
                    hash_user_id,
                    user_id,
                    datetime.utcnow(),
                    load_src
                )

                self._logger.info(f"{datetime.utcnow()}: h_user loaded")

                self._dds_repository.h_restaurant_insert(
                    hash_restaurant_id,
                    restaurant_id,
                    datetime.utcnow(),
                    load_src
                )

                self._logger.info(f"{datetime.utcnow()}: h_restaurant loaded")

                self._dds_repository.l_order_user_insert(
                        uuid.uuid3(uuid.NAMESPACE_X500, str(hash_order_id) + str(hash_user_id)),
                        hash_order_id,
                        hash_user_id,
                        datetime.utcnow(),
                        load_src
                    )
                
                self._logger.info(f"{datetime.utcnow()}: l_order_user loaded")

                self._dds_repository.s_order_cost_insert(
                        hash_order_id,
                        order_payment,
                        order_cost,
                        datetime.utcnow(),
                        load_src,
                        uuid.uuid3(uuid.NAMESPACE_X500, str(hash_order_id) + str(order_payment) + str(order_cost) + str(datetime.utcnow()) + str(load_src))
                    )
                
                self._logger.info(f"{datetime.utcnow()}: s_order_cost loaded")

                self._dds_repository.s_order_status_insert(
                        hash_order_id,
                        order_status,
                        datetime.utcnow(),
                        load_src,
                        uuid.uuid3(uuid.NAMESPACE_X500, str(hash_order_id) + str(order_status) + str(datetime.utcnow()) + str(load_src))
                    )
                
                self._logger.info(f"{datetime.utcnow()}: s_order_status loaded")

                self._dds_repository.s_restaurant_names_insert(
                        hash_restaurant_id,
                        restaurant_name,
                        datetime.utcnow(),
                        load_src,
                        uuid.uuid3(uuid.NAMESPACE_X500, str(hash_restaurant_id) + str(restaurant_name) + str(datetime.utcnow()) + str(load_src))
                    )
                
                self._logger.info(f"{datetime.utcnow()}: s_restaurant_names loaded")

                self._dds_repository.s_user_names_insert(
                        hash_user_id,
                        user_name,
                        user_login,
                        datetime.utcnow(),
                        load_src,
                        uuid.uuid3(uuid.NAMESPACE_X500, str(hash_user_id) + str(user_name) + str(user_login) + str(datetime.utcnow()) + str(load_src))
                    )
                
                self._logger.info(f"{datetime.utcnow()}: s_user_names loaded")

                # Разворачиваем и загружаем хабы, линки и сателлиты деталок заказа
                for product in products:

                    product_id = product["id"]
                    product_name = product["name"]
                    hash_product_id = uuid.uuid3(uuid.NAMESPACE_X500, str(product_id))
                    category_name = product["category"]
                    hash_category_name = uuid.uuid3(uuid.NAMESPACE_X500, str(category_name))

                    self._dds_repository.h_category_insert(
                        hash_category_name,
                        category_name,
                        datetime.utcnow(),
                        load_src
                    )
                                    
                    self._logger.info(f"{datetime.utcnow()}: h_category loaded")

                    self._dds_repository.h_product_insert(
                        hash_product_id,
                        product_id,
                        datetime.utcnow(),
                        load_src
                    )
                                    
                    self._logger.info(f"{datetime.utcnow()}: h_product loaded")

                    self._dds_repository.l_product_category_insert(
                        uuid.uuid3(uuid.NAMESPACE_X500, str(hash_category_name) + str(hash_product_id)),
                        hash_category_name,
                        hash_product_id,
                        datetime.utcnow(),
                        load_src
                    )
                                    
                    self._logger.info(f"{datetime.utcnow()}: l_product_category loaded")
                    
                    self._dds_repository.l_order_product_insert(
                        uuid.uuid3(uuid.NAMESPACE_X500, str(hash_order_id) + str(hash_product_id)),
                        hash_order_id,
                        hash_product_id,
                        datetime.utcnow(),
                        load_src
                    )
                                    
                    self._logger.info(f"{datetime.utcnow()}: l_order_product loaded")
                    
                    self._dds_repository.l_product_restaurant_insert(
                        uuid.uuid3(uuid.NAMESPACE_X500, str(hash_restaurant_id) + str(hash_product_id)),
                        hash_restaurant_id,
                        hash_product_id,
                        datetime.utcnow(),
                        load_src
                    )
                                    
                    self._logger.info(f"{datetime.utcnow()}: l_product_restaurant loaded")
                    
                    self._dds_repository.s_product_names_insert(
                        hash_product_id,
                        product_name,
                        datetime.utcnow(),
                        load_src,
                        uuid.uuid3(uuid.NAMESPACE_X500, str(hash_product_id) + str(product_name) + str(datetime.utcnow()) + str(load_src))
                    )
                                    
                    self._logger.info(f"{datetime.utcnow()}: s_product_names loaded")

                    # Формируем сообщение и отправляем в Kafka
                    dst_msg = {
                        "user_id":str(hash_user_id),
                        "category_id":str(hash_category_name),
                        "category_name":category_name,
                        "product_id":str(hash_product_id),
                        "product_name":product_name
                    }

                    self._producer.produce(dst_msg)
                    self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
            