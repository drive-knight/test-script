import psycopg2
from psycopg2 import OperationalError
from collections import Counter


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


connection = create_connection(
    "task", "postgres", "123456", "127.0.0.1", "5432"
)


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def to_fixed(num, digits=0):
    return f"{num:.{digits}f}"


select_movies = "SELECT movie FROM views"
select_users = "SELECT name FROM views"
select_all = "SELECT name, movie FROM views"
movies = execute_read_query(connection, select_movies)
users = execute_read_query(connection, select_users)
table = execute_read_query(connection, select_all)


name, movie, rating = None, None, None
user_rec = None
current = 1  # выбранный пользователь
next = 0  # с кем сравнивается
new_dict = {}
out_table = []  # итоговый вывод
maximum = 0
n = 1


for count, m in enumerate(movies):  # индексирование пользователей с фильмами
    new_dict[n] = m
    n += 1

while current != len(table):
    while next != len(table):
        current_movies = new_dict.get(current)
        templist = []
        if next == 0:
            for k, v in dict(table).items():  # запись имя выбранного пользователя
                if v in current_movies:
                    name = k
                    templist.append(k)
                    break
        if next == current:
            next += 1
        if current == 1 and next == 0:
            next += 1
        if next + 1 == current:
            next += 1
        selected_movies = new_dict.get(current) + new_dict.get(next + 1)
        res = Counter(f'{selected_movies[0]} {selected_movies[1]}')
        try:
            del res[' ']

            del res[',']
        except KeyError:
            pass
        max_coincidence = 0
        for _ in res.values():  # подсчет максимального совпадения
            if _ >= 2:
                max_coincidence += 1
        next += 1
        if max_coincidence > maximum:  # нахождение максимального совпадения среди всех элементов
            maximum = max_coincidence
            quanity = 0
            for k, v in res.items():
                if v == 1 and k in str({selected_movies[1]}):  # рекомендация фильма
                    movie = k[0]
                    templist.append(k[0])
                    break
            for _ in new_dict.get(current):  # вычисление рейтинга
                string = _
                string = string.replace(',', '')
                string = string.replace(' ', '')
                quanity = len(string)
            rating = max_coincidence / quanity * 100
            templist.append(rating)
        if next == len(new_dict):  # переход к следующему пользователю
            next = 0
            maximum = 0
            out_table.append(tuple([name, movie, to_fixed(rating, 2)]))
            break
    current += 1


user_records = ", ".join(["%s"] * len(out_table))

insert_query = (
    f"INSERT INTO output (username, rec_movie, rating) VALUES {user_records}"
)

connection.rollback()
connection.autocommit = True
cursor = connection.cursor()
cursor.execute(insert_query, out_table)