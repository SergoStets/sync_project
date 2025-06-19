import json
import psycopg2
import subprocess
from pathlib import Path

# Загрузка конфигурации
CONFIG_PATH = Path("config/config.json")

with open(CONFIG_PATH) as f:
    config = json.load(f)

db1 = config["db1"]
db2 = config["db2"]
ssh_user = config["ssh"]["user"]
ssh_password = config["ssh"]["password"]
remote_path = config["ssh"]["remote_path"]

def connect_db(db_conf):
    conn = psycopg2.connect(
        host=db_conf['host'],
        port=db_conf['port'],
        dbname=db_conf['dbname'],
        user=db_conf['user'],
        password=db_conf['password']
    )
    return conn

def get_samples(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, file_name, created_at, name, comment, call_id, is_active, to_delete, sample_type_id 
            FROM *****************
            WHERE is_active = true;
        """)
        return cur.fetchall()

def insert_sample(conn, sample):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO *****************
            (id, file_name, created_at, name, comment, call_id, is_active, to_delete, sample_type_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """, sample)
    conn.commit()

def sync_directories(source_host, dest_host, remote_path, ssh_user, ssh_password):
    rsync_command = [
        'sshpass', '-p', ssh_password,
        'rsync', '-avz', '--delete',
        '-e', 'ssh -o StrictHostKeyChecking=no',
        f'{ssh_user}@{source_host}:{remote_path}/',
        f'{ssh_user}@{dest_host}:{remote_path}/'
    ]
    print(f"Синхронизация файлов с {source_host} на {dest_host}")
    subprocess.run(rsync_command, check=True)

def main():
    conn1 = connect_db(db1)
    conn2 = connect_db(db2)

    samples1 = get_samples(conn1)
    samples2 = get_samples(conn2)

    samples1_dict = {row[0]: row for row in samples1}
    samples2_dict = {row[0]: row for row in samples2}

    to_copy_to_db2 = set(samples1_dict.keys()) - set(samples2_dict.keys())
    to_copy_to_db1 = set(samples2_dict.keys()) - set(samples1_dict.keys())

    print(f"Need to copy {len(to_copy_to_db2)} samples to DB2")
    print(f"Need to copy {len(to_copy_to_db1)} samples to DB1")

    for sample_id in to_copy_to_db2:
        insert_sample(conn2, samples1_dict[sample_id])

    for sample_id in to_copy_to_db1:
        insert_sample(conn1, samples2_dict[sample_id])

    conn1.close()
    conn2.close()
    print("Синхронизация баз данных завершена.")

    try:
        sync_directories(db1["host"], db2["host"], remote_path, ssh_user, ssh_password)
        sync_directories(db2["host"], db1["host"], remote_path, ssh_user, ssh_password)
        print("Синхронизация файлов завершена.")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при синхронизации директорий: {e}")

if __name__ == "__main__":
    main()
