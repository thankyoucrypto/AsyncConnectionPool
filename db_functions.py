import asyncio
import sqlite3
import datetime


class AsyncConnectionPool:
    def __init__(self, max_connections=1, database=None):
        """
          Инициализация пула асинхронных соединений с базой данных.

          :param max_connections: Максимальное количество соединений, которые могут быть открыты одновременно.
                                 По умолчанию 1, чтобы избежать блокировок базы данных.
          :param database: Имя файла базы данных SQLite. Если не указано, будет использовано значение None.
          """

        self.max_connections = max_connections  # Максимальное количество соединений
        self.database = database  # Имя базы данных
        self.available_connections = asyncio.Queue(max_connections)  # Очередь для доступных соединений
        self.semaphore = asyncio.Semaphore(max_connections)  # Ограничение по количеству одновременно активных соединений
        self.waiting_requests = 0  # Счётчик ожидающих запросов

    async def initialize(self):
        # Инициализация пула соединений
        for _ in range(self.max_connections):
            connection = await self.create_connection()  # Создание соединения
            await self.available_connections.put(connection)  # Добавление соединения в очередь
        print(f"Успешно создан пул соединений с max={self.max_connections} соединениями.")

    async def create_connection(self):
        # Создание нового соединения с базой данных
        connection = sqlite3.connect(self.database)
        return connection

    async def acquire_connection(self):
        # Запрос на получение соединения
        if self.available_connections.empty():
            self.waiting_requests += 1  # Увеличиваем счётчик ожидающих запросов
            print(f"Получен запрос,  Доступно соединений: {self.available_connections.qsize()}. Добавляем в очередь. В очереди: {self.waiting_requests}")

        await self.semaphore.acquire()  # Запрашиваем разрешение на использование соединения
        connection = await self.available_connections.get()  # Получаем соединение из очереди

        self.waiting_requests = max(0, self.waiting_requests - 1)  # Уменьшаем счётчик ожидающих
        print(f"\nСоединение установлено.  Доступно соединений: {self.available_connections.qsize()}. Обрабатываем.. В очереди: {self.waiting_requests}")

        return connection

    async def release_connection(self, connection):
        # Освобождение соединения
        print('Явная задержка 5 сек перед освобождением для наглядности очереди')
        await asyncio.sleep(5)  # Задержка для демонстрации одижания другими соединениями в очереди

        connection.commit() # Коммит БД (save)
        await self.available_connections.put(connection)  # Освобождаем соединение в очередь
        print(f"Соединение освобождено. Доступно соединений: {self.available_connections.qsize()}. В очереди: {self.waiting_requests}")
        self.semaphore.release()  # Освобождаем семафор

    async def close_all_connections(self):
        # Закрытие всех соединений
        while not self.available_connections.empty():
            connection = await self.available_connections.get()  # Получаем соединение из очереди
            connection.close()  # Закрываем соединение
        print("Все соединения закрыты.")

    async def create_database(self):
        # Создание базы данных и таблицы
        connection = await self.acquire_connection()  # Получаем соединение
        try:
            cursor = connection.cursor()  # Создаем курсор для выполнения SQL-запросов
            # Создание таблиц с абстрактными полями
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS entities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    created_at DATE NOT NULL
                )
            ''')
        finally:
            await self.release_connection(connection)  # Освобождаем соединение

    async def example_insert(self, name, description):
        # Вставка новой сущности в таблицу
        connection = await self.acquire_connection()  # Получаем соединение
        try:
            cursor = connection.cursor()  # Создаем курсор для выполнения SQL-запросов
            cursor.execute('''INSERT INTO entities (name, description, created_at)
                              VALUES (?, ?, ?)''', (name, description, datetime.date.today()))  # Выполняем вставку
        finally:
            await self.release_connection(connection)  # Освобождаем соединение

    async def example_update(self, entity_id, new_description):
        # Обновление существующей сущности по ID
        connection = await self.acquire_connection()  # Получаем соединение
        try:
            cursor = connection.cursor()  # Создаем курсор для выполнения SQL-запросов
            cursor.execute('''UPDATE entities SET description = ? WHERE id = ?''', (new_description, entity_id))  # Выполняем обновление
        finally:
            await self.release_connection(connection)  # Освобождаем соединение

    async def example_select(self):
        # Выборка всех сущностей из таблицы
        connection = await self.acquire_connection()  # Получаем соединение
        try:
            cursor = connection.cursor()  # Создаем курсор для выполнения SQL-запросов
            results = cursor.execute('''SELECT * FROM entities''').fetchall()  # Выполняем выборку
            return results  # Возвращаем результаты
        finally:
            await self.release_connection(connection)  # Освобождаем соединение


# Пример использования
async def main():
    pool = AsyncConnectionPool(database='example.db')  # Создаем пул соединений
    await pool.initialize()  # Инициализируем пул

    # Создание базы данных и таблицы
    print('\n_______________ Запускаем 1 задачу: создание БД _______________')
    await pool.create_database()


    print('\n_______________ Запускаем одновременно 5 задач _______________')
    tasks = []
    tasks.append(pool.example_insert("Entity1", "Описание сущности 1"))  # Задача вставки первой сущности
    tasks.append(pool.example_insert("Entity2", "Описание сущности 2"))  # Задача вставки второй сущности
    tasks.append(pool.example_insert("Entity3", "Описание сущности 3"))  # Задача вставки второй сущности
    tasks.append(pool.example_update(1, "Обновленное описание для сущности 1"))  # Задача обновления первой сущности
    tasks.append(pool.example_select())  # Задача выборки всех сущностей
    await asyncio.gather(*tasks)  # Выполняем все задачи одновременно

    # Закрытие пула соединений
    await pool.close_all_connections()


# Запуск
asyncio.run(main())
