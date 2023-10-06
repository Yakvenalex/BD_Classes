import asyncio
import logging
from typing import List, Dict, Union, Any
import sqlite3

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger_sqlite = logging.getLogger(__name__)


class SqliteHandler:
    def __init__(self, db_file: str):
        self.connection = None
        self.db_file = db_file

    async def connect(self):
        if not self.connection:
            self.connection = sqlite3.connect(self.db_file)

    async def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    async def create_tables(self, table_info: dict):
        """
        Создает таблицу в базе данных SQLite.

        :param table_info: Словарь с информацией о таблице.
            Пример:
            {
                'name_table': 'users',
                'primary_key': ('id', 'INTEGER', 'PRIMARY KEY'),
                'fields': {
                    'username': 'TEXT',
                    'email': 'TEXT',
                    'age': 'INTEGER'
                }
            }
        """
        table_name = table_info['name_table']
        primary_key = table_info.get('primary_key', None)
        fields = table_info.get('fields', {})

        await self.connect()

        try:
            cursor = self.connection.cursor()

            query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            if primary_key:
                query += f"{primary_key[0]} {primary_key[1]} {primary_key[2]},"
            for field_name, field_type in fields.items():
                query += f"{field_name} {field_type},"
            query = query.rstrip(',') + ")"
            cursor.execute(query)

            self.connection.commit()
            logger_sqlite.info(f"Table '{table_name}' successfully created or already exists.")
            return "ok"
        except Exception as e:
            logger_sqlite.error(f"Error creating table: {e}")
            return f"Error creating table: {e}"
        finally:
            await self.close()

    async def mass_update(
            self, table_name: str, data_list: List[Dict[str, Union[Dict[str, int], Dict[str, str]]]],
            batch_size: int = 1000) -> None:
        """
        Пакетное обновление записей в таблице SQLite.

        :param table_name: Название обновляемой таблицы.
        :param data_list: Список словарей с данными для обновления.
            Каждый словарь содержит два ключа: 'where' (условие для выбора записей) и 'update'
            (словарь с данными для обновления).
            Пример:
            [{'where': {'id': 1}, 'update': {'name': 'John', 'age': 25}},
             {'where': {'id': 2}, 'update': {'name': 'Jane', 'age': 30}}]
        :param batch_size: Размер пакета для выполнения операции обновления пакетами.
        :return: None
        """
        await self.connect()

        try:
            cursor = self.connection.cursor()

            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                for data in batch:
                    where_dict = data['where']
                    update_dict = data['update']

                    # Form WHERE clause
                    where_clause = ' AND '.join(
                        f"{k} = ?" for k in where_dict.keys()
                    )

                    # Form data for WHERE
                    where_values = list(where_dict.values())

                    # Form UPDATE clause
                    update_clause = ', '.join(
                        f"{k} = ?" for k in update_dict.keys()
                    )

                    # Form data for UPDATE
                    update_values = list(update_dict.values())

                    # Form the final SQL query
                    query = f'UPDATE {table_name} SET {update_clause} WHERE {where_clause}'
                    print(query)

                    # Combine data for WHERE and UPDATE into one list
                    data_values = update_values + where_values
                    print(data_values)

                    try:
                        # Execute the query
                        cursor.execute(query, tuple(data_values))
                    except Exception as e:
                        logger_sqlite.error(f"Error updating records: {e}")

            self.connection.commit()
        except Exception as e:
            logger_sqlite.error(f"Error updating records: {e}")
        finally:
            await self.close()

    async def insert_into_table_bulk(self, table_name, records, batch_size=1000):
        """
        Быстрая вставка новых записей в таблицу SQLite с использованием пакетной вставки.

        :param table_name: Название таблицы, в которую добавляются записи.
        :param records: Список словарей, представляющих записи для вставки.
                        Каждый словарь должен иметь ключи, соответствующие названиям столбцов,
                        и значения для соответствующих столбцов.
        :param batch_size: Размер пакета для выполнения операции вставки пакетами.
        """
        await self.connect()

        try:
            cursor = self.connection.cursor()

            columns = list(records[0].keys()) if records else []
            values_placeholder = ', '.join('?' for _ in range(len(columns)))
            query = f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({values_placeholder})'

            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                values = [tuple(record[col] for col in columns) for record in batch]
                cursor.executemany(query, values)

            self.connection.commit()
            logger_sqlite.info(f'{len(records)} records added to the table {table_name}')
            return "ok"
        except Exception as e:
            logger_sqlite.error(f"Error adding records: {e}")
            return f"Error adding records: {e}"
        finally:
            await self.close()

    async def select_data(
            self,
            table_name: str,
            query_params: Dict[str, Union[List[str], List[Dict[str, Any]]]] = {},
            batch_size: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Выполняет операцию SELECT на основе указанных параметров и возвращает результат в виде списка словарей.

        :param table_name: Название таблицы, из которой нужно выбрать данные.
        :param query_params: Словарь с параметрами запроса.
            Пример:
            {
                'find_data': ['us_id', 'email'],
                'where_conditions': [{'age': 30, 'city': 'New York', 'name': None}]
            }
        :param batch_size: Размер пакета для выполнения операции SELECT пакетами.
        :return: Список словарей с результатами запроса.
        """
        await self.connect()

        find_data = query_params.get('find_data', ['*'])
        where_conditions_list = query_params.get('where_conditions', [])

        query, condition_values = self.build_select_query(table_name, find_data, where_conditions_list, batch_size)

        try:
            cursor = self.connection.cursor()
            result = cursor.execute(query, tuple(condition_values)).fetchall()

            # Check if the result is a list of tuples, each representing a row
            if isinstance(result, list) and all(isinstance(row, tuple) for row in result):
                # Convert each row tuple to a dictionary
                result_list = [dict(zip(find_data, row)) for row in result]
                return result_list
            else:
                logger_sqlite.error(f"Unexpected result format for SELECT query: {result}")
                return []
        except Exception as e:
            logger_sqlite.error(f"Error executing SELECT query: {e}")
            raise
        finally:
            await self.close()

    def build_select_query(self, table_name, find_data, where_conditions_list, batch_size):
        query = f"SELECT {', '.join(find_data)} FROM {table_name}"
        condition_values = []

        if where_conditions_list:
            where_clauses = []

            for conditions in where_conditions_list:
                if conditions:
                    where_clauses.append(
                        ' AND '.join(
                            f"{key} = ?" if value is not None else f"{key} IS NULL"
                            for key, value in conditions.items()
                        )
                    )

                    condition_values.extend(value for value in conditions.values() if value is not None)

            if where_clauses:
                query += f" WHERE {' OR '.join(f'({clause})' for clause in where_clauses)}"

                if batch_size:
                    query += f" LIMIT {batch_size}"

        return query, condition_values

    async def delete_data(self, table_name: str, where_conditions: Dict[str, Any]) -> str:
        """
        Удаляет записи из таблицы SQLite на основе условий.

        :param table_name: Название таблицы, из которой нужно удалить записи.
        :param where_conditions: Словарь с условиями для фильтрации удаляемых записей в формате {имя_колонки: значение}.
        """
        try:
            await self.connect()

            cursor = self.connection.cursor()
            query = f"DELETE FROM {table_name}"

            if where_conditions:
                query += " WHERE "
                where_columns = [f"{key} = ?" for key in where_conditions.keys()]
                query += ' AND '.join(where_columns)

            cursor.execute(query, tuple(where_conditions.values()))
            self.connection.commit()
            logger_sqlite.info(f"Records in the table '{table_name}' successfully deleted.")
            return "ok"
        except Exception as e:
            logger_sqlite.error(f"Error deleting data: {e}")
            return f"Error deleting data: {e}"
        finally:
            await self.close()

    async def delete_all_data_from_table(self, table_name):
        """
        Deletes all records from the SQLite table.

        :param table_name: The name of the table from which to delete all records.
        """
        await self.connect()

        try:
            cursor = self.connection.cursor()
            query = f'DELETE FROM {table_name}'
            cursor.execute(query)

            self.connection.commit()
            logger_sqlite.info(f'All records deleted from the table {table_name}')
            return "ok"
        except Exception as e:
            logger_sqlite.error(f"Error deleting records: {e}")
            return "error"

    async def count_records(self, table_name, where_conditions: dict = {}):
        if not table_name or not isinstance(where_conditions, dict):
            raise ValueError('Invalid parameters')

        await self.connect()

        try:
            cursor = self.connection.cursor()
            where_clause = ' AND '.join(f'{col} = ?' for col in where_conditions)
            query = f'SELECT COUNT(*) FROM {table_name}'

            if where_clause:
                query += f' WHERE {where_clause}'

            null_col = [col for col, value in where_conditions.items() if value is None]

            if null_col:
                query += f' OR {null_col[0]} IS NULL'

            count = cursor.execute(query, tuple(where_conditions.values())).fetchval()
            logger_sqlite.info(f'{count} results in the database for the specified query')
            return count

        except Exception as e:
            logger_sqlite.error(f"Error counting records: {e}")
            raise
        finally:
            await self.close()