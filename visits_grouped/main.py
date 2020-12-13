# coding: utf-8

import pandas as pd
import sqlite3


def create_visits_grouped_table(connection):
    """Пересоздает таблицу visits_grouped, используя соединение connection"""
    cur = connection.cursor()

    cur.execute('DROP TABLE IF EXISTS visits_grouped')

    cur.execute("""
        CREATE TABLE visits_grouped(
            user_id INTEGER,
            source STRING,
            purchases INTEGER
        );
    """)

    connection.commit()


def build_visits_stats(connection):
    """
    Считает сумму столбца goal_purchase таблицы visits в разбивке по столбцам user_id, source.
    Результат сортируется по убыванию суммы покупок и записывается в таблицу visits_grouped.
    """
    cur = connection.cursor()

    cur.execute("""
        INSERT INTO visits_grouped
        SELECT user_id, source, sum(goal_purchase) AS purchases 
        FROM visits 
        WHERE goal_purchase > 0 
        GROUP BY user_id, source 
        ORDER BY purchases DESC;
    """)

    connection.commit()


def main():
    connection = sqlite3.connect('../ecommerce.db')

    create_visits_grouped_table(connection)
    build_visits_stats(connection)

    print(pd.read_sql('select * from visits_grouped;', connection))


if __name__ == '__main__':
    main()
