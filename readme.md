## Тестовое задание на позицию Python QA engineer

### Описание проекта

В данном тестовом задании были реализованы простейшие тесты на DML и DDL операции с таблицами.
Тесты можно найти в файлах `tests/test_dml.py` и `tests/test_ddl.py` соответственно.
Файл `test_create_table.py` содержит smoke-автотесты на создание таблиц.
Файл `conftest.py` содержит все важные фикстуры и хуки, используемые в тестах.

В директории `common` можно найти инкапсулированную бизнес-логику для работы с сырыми sql запросами
на основе адаптера psycopg3, а также вспомогательные модули для разработки автотестов.

### Деплой проекта

Для разворачивания проекта вам понадобится Docker и Docker Compose.

+ **Клонируем репозиторий**

```console
$ git clone https://github.com/Domochevskyy/test_db.git
```

+ **Переходим в главную директорию проекта**

```console
$ cd test_db
```

+ **Билдим образы и запускаем контейнеры в attach режиме**

```console
$ docker compose up --build
```

Для того чтобы остановить контейнеры, нужно нажать сочетание клавиш `Ctrl + C`

+ **Либо можно запустить контейнеры в режиме демона**

```console
$ docker compose up --build --detach
```

+ **Посмотреть результат тест-рана можно через логи контейнера**

```console
$ docker compose logs tests
```

+ **Удалить все используемые в проекте контейнеры**

```console
$ docker compose down
```

+ **Удалить все используемые в проекте имаджи**

```console
$ docker rmi tests task_test-tests:latest test_db-tests:latest postgres:15.2
```

Перед этим необходимо убедиться, что все контейнеры, порожденные этими имаджами, остановлены

+ **Удалить созданную докером сеть**

```console
$ docker network rm test_db_default
```
