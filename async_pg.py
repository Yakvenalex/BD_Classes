import logging
from typing import List, Dict, Union, Any
import asyncpg
from decouple import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger_db = logging.getLogger(__name__)


class PostgresHandler:
    def __init__(self):
        self.pool = None
        self.CONNECT_USR = config('PG_LINK')

    async def connect(self):
        if not self.pool:
            self.pool = await asyncpg.create_pool(dsn=self.CONNECT_USR)

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def create_tables(self, table_info: dict):
        """
        Создает таблицу в базе данных.

        :param table_info: Словарь с информацией о таблице.
            Пример:
            {
                'name_table': 'users',
                'primary_key': ('id', 'SERIAL', 'PRIMARY KEY'),
                'fields': {
                    'username': 'VARCHAR(50)',
                    'email': 'VARCHAR(100)',
                    'age': 'INT'
                }
            }
        """
        table_name = table_info['name_table']
        primary_key = table_info.get('primary_key', None)
        fields = table_info.get('fields', {})

        await self.connect()

        async with self.pool.acquire() as conn:
            try:
                query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
                if primary_key:
                    query += f"{primary_key[0]} {primary_key[1]} {primary_key[2]},"
                for field_name, field_type in fields.items():
                    query += f"{field_name} {field_type},"
                query = query.rstrip(',') + ")"
                await conn.execute(query)
                logger_db.info(f"Table '{table_name}' successfully created or already exists.")
                return "ok"
            except Exception as e:
                logger_db.error(f"Error creating table: {e}")
                return f"Error creating table: {e}"

    async def mass_update(
            self, table_name: str, data_list: List[Dict[str, Union[Dict[str, int], Dict[str, str]]]],
            batch_size: int = 1000) -> None:
        """
        Пакетное обновление записей в таблице.

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

        async with self.pool.acquire() as conn:
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                for data in batch:
                    where_dict = data['where']
                    update_dict = data['update']

                    # Form WHERE clause
                    where_clause = ' AND '.join(
                        f"{k} = ${i + 1}" for i, k in enumerate(where_dict.keys())
                    )

                    # Form data for update
                    update_clause = ', '.join(
                        f"{k} = ${len(where_dict) + i + 1}" for i, k in enumerate(update_dict.keys())
                    )

                    # Form the final SQL query
                    query = f'UPDATE {table_name} SET {update_clause} WHERE {where_clause}'

                    # Combine data for WHERE and UPDATE into one list
                    data_values = list(where_dict.values()) + list(update_dict.values())

                    try:
                        # Execute the query
                        await conn.execute(query, *data_values)
                    except Exception as e:
                        logger_db.error(f"Error updating records: {e}")

    async def insert_into_table_bulk(self, table_name, records, batch_size=1000):
        """
        Быстрая вставка новых записей в таблицу с использованием пакетной вставки.

        :param table_name: Название таблицы, в которую добавляются записи.
        :param records: Список словарей, представляющих записи для вставки.
                        Каждый словарь должен иметь ключи, соответствующие названиям столбцов,
                        и значения для соответствующих столбцов.
        :param batch_size: Размер пакета для выполнения операции вставки пакетами.
        """
        await self.connect()

        async with self.pool.acquire() as conn:
            try:
                columns = list(records[0].keys()) if records else []
                values_placeholder = ', '.join(f'${i + 1}' for i in range(len(columns)))
                query = f'''INSERT INTO {table_name} ({', '.join(columns)})
                            VALUES ({values_placeholder})'''

                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]
                    values = [tuple(record[col] for col in columns) for record in batch]
                    await conn.executemany(query, values)

                logger_db.info(f'{len(records)} records added to the table {table_name}')
                return "ok"
            except Exception as e:
                logger_db.error(f"Error adding records: {e}")
                return f"Error adding records: {e}"

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
            async with self.pool.acquire() as conn:
                result = await conn.fetch(query, *condition_values)

                result_list = [dict(row) for row in result]
                return result_list
        except Exception as e:
            logger_db.error(f"Error executing SELECT query: {e}")
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
                            f"{key} = ${len(condition_values) + i + 1}" if value is not None else f"{key} IS NULL"
                            for i, (key, value) in enumerate(conditions.items())
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
        Удаляет записи из таблицы на основе условий.
        :param table_name: Название таблицы, из которой нужно удалить записи.
        :param where_conditions: Словарь с условиями для фильтрации удаляемых записей в формате {имя_колонки: значение}.
        """
        try:
            await self.connect()

            async with self.pool.acquire() as conn:
                # Generate SQL query for deleting data
                query = f"DELETE FROM {table_name}"

                if where_conditions:
                    query += " WHERE "
                    where_columns = [f"{key} = ${i + 1}" for i, key in enumerate(where_conditions.keys())]
                    query += ' AND '.join(where_columns)

                # Execute the query with data
                await conn.execute(query, *where_conditions.values())
                logger_db.info(f"Records in the table '{table_name}' successfully deleted.")
                return "ok"
        except Exception as e:
            logger_db.error(f"Error deleting data: {e}")
            return f"Error deleting data: {e}"

    async def delete_all_data_from_table(self, table_name):
        """
        Deletes all records from the table.

        :param table_name: The name of the table from which to delete all records.
        """
        await self.connect()

        async with self.pool.acquire() as conn:
            try:
                query = f'DELETE FROM {table_name}'
                result = await conn.execute(query)

                logger_db.info(f'{result} records deleted from the table {table_name}')
                return "ok"
            except Exception as e:
                logger_db.error(f"Error deleting records: {e}")
                return "error"

    async def count_records(self, table_name, where_conditions: dict = {}):
        if not table_name or not isinstance(where_conditions, dict):
            raise ValueError('Invalid parameters')

        await self.connect()

        try:
            async with self.pool.acquire() as conn:
                where_clause = ' AND '.join(f'{col} = ${i + 1}' for i, col in enumerate(where_conditions))
                query = f'SELECT COUNT(*) FROM {table_name}'

                if where_clause:
                    query += f' WHERE {where_clause}'

                # Get the column name with a NULL value
                null_col = [col for col, value in where_conditions.items() if value is None]

                if null_col:
                    query += f' OR {null_col[0]} IS NULL'
                count = await conn.fetchval(query, *where_conditions.values())
                logger_db.info(f'{count} results in the database for the specified query')
                return count

        except Exception as e:
            logger_db.error(f"Error counting records: {e}")
            raise