from pyspark.sql import SparkSession
import random as rn
from faker import Faker

Fak = Faker('ru_RU')
Product = ['молоко', 'конфеты', 'торт', 'картошка', 'мясо']
N = 1000
# Создаем объект SparkSession
spark = SparkSession.builder .appName('ex1') .getOrCreate()
df = spark.createDataFrame([{
            "Дата":Fak.date_between(start_date='-1y', end_date='today'),
            "UserID": rn.randint(10000, 99999),
            "Продукт": rn.choice(Product),
            "Количество": rn.uniform(15, 1000),
            "Цена": rn.uniform(20, 1500),}
        for _ in range(N)])
df.show()
df.write.csv('baf23.csv')
# df.write.csv('baf.csv')
# df.to_csv("data/fake.csv", index=False)
# Останавливаем SparkSession 
spark.stop()