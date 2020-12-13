# coding: utf-8

import pandas as pd
import sqlite3


def create_customers_table(connection):
    """Пересоздает таблицу customers, используя соединение connection"""
    cur = connection.cursor()

    cur.execute('DROP TABLE IF EXISTS customers')

    cur.execute("""
        CREATE TABLE customers(
            user_id INTEGER
        );
    """)

    connection.commit()


def build_customers_stats(connection):
    """
    Фильтрует таблицу statuses по покупателям.
    Список уникальных user_id и записывается в таблицу customers.
    """
    cur = connection.cursor()

    cur.execute("""
        INSERT INTO customers
        SELECT DISTINCT user_id 
        FROM statuses 
        WHERE purchase_status = "DELIVERED";
    """)

    connection.commit()


def main():
    connection = sqlite3.connect('../ecommerce.db')

    create_customers_table(connection)
    build_customers_stats(connection)

    print(pd.read_sql('select * from customers;', connection))


if __name__ == '__main__':
    main()
