# coding: utf-8

import pandas as pd
import sqlite3


def sales_by_sources_report(connection):
    """
    Формирует отчет по продажам в разбивке по источникам трафика.
    Возвращает pandas dataframe
    """

    sql_query = """
        SELECT source, purchases 
        FROM (
            SELECT source, purchases from visits_grouped
            INNER JOIN customers ON visits_grouped.user_id = customers.user_id
        )
        GROUP BY source
        ORDER BY purchases DESC;
    """

    df = pd.read_sql(sql_query, connection)
    df.columns = ['source', 'purchases']
    return df


def main():
    connection = sqlite3.connect('../ecommerce.db')

    report = sales_by_sources_report(connection)
    print(report)


if __name__ == '__main__':
    main()
