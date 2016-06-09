__author__ = 'Marlon Abeykoon'

from elasticsearch import Elasticsearch
import threading
import random
import datetime
import uuid

ES_HOST = {"host" : "104.155.232.234", "port" : 9200}

INDEX_NAME = 'superstore_sales'
TYPE_NAME = 'superstore_sales'

ID_FIELD = 'id'



es = Elasticsearch(hosts = [ES_HOST])

if es.indices.exists(INDEX_NAME):
    print("deleting '%s' index..." % (INDEX_NAME))
    res = es.indices.delete(index = INDEX_NAME)
    print(" response: '%s'" % (res))

request_body = {
    "settings" : {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
}

#i = 0
order_priority = ['High', 'Low', 'Medium']
ship_mode = ['Delivery Truck', 'Regular Air', 'Express Air']
province = ['Quebec', 'Ontario', 'Manitoba', 'Yukon', 'Saskachewan', 'British Columbia', 'New Brunswick', 'Alberta',
'Nova Scotia', 'Prince Edward Island', 'Nunavut', 'Northwest Territories', 'Newfoundland']
region = ['Quebec', 'Ontario', 'Prarie', 'Yukon', 'West', 'Atlantic', 'Nunavut', 'Northwest Territories']
customer_segment = ['Consumer', 'Home Office', 'Corporate', 'Small Business']
product_category = ['Furniture', 'Office Supplies', 'Technology']
product_container = ['Small Box', 'Small Pack', 'Jumbo Drum', 'Wrap Bag', 'Jumbo Box', 'Medium Box', 'Large Box']


def data_generator():
    while True:
        bulk_data = []
        data_dict = {}
        id = uuid.uuid4()

        op_dict = {
            "index": {
                "_index": INDEX_NAME,
                "_type": TYPE_NAME,
                "_id": id
            }
        }
        data_dict = {
            'id' : id,
            'order_priority' : random.choice(order_priority),
            'order_quantity' : random.randint(0, 50),
            'sales' : float("{0:.2f}".format(random.uniform(10.00, 5000.00))),
            'ship_mode' : random.choice(ship_mode),
            'unit_price' : float("{0:.2f}".format(random.uniform(10.00, 1500.00))),
            'province' : random.choice(province),
            'region' : random.choice(region),
            'customer_segment' : random.choice(customer_segment),
            'product_category' : random.choice(product_category),
            'product_container' : random.choice(product_container),
            'date_time' : datetime.datetime.utcnow()
        }
        bulk_data.append(op_dict)
        bulk_data.append(data_dict)

        print("bulk indexing...")
        res = es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)
        print res


t1 = threading.Thread(target=data_generator, args=())
t1.start()
t2 = threading.Thread(target=data_generator, args=())
t2.start()