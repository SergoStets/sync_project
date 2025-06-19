# Синхронизация баз данных и файлов

Скрипт синхронизирует таблицу `machine_samples` и директории между двумя серверами.

## Установка

1. Установите зависимости:
        pip install psycopg2-binary

2. Создайте файл config/config.json по образцу config_sample.json.

3. Запуск:
        python main.py