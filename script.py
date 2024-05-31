import paramiko
import os
import tarfile
import time
from tqdm import tqdm

def create_tarfile(source_dir, output_filename):
    with tarfile.open(output_filename, "w:gz") as tar:
        # Определяем количество файлов для создания прогресс-бара
        num_files = sum(len(files) for _, _, files in os.walk(source_dir))
        with tqdm(total=num_files, desc='Архивация', unit='файл') as pbar:
            for root, _, files in os.walk(source_dir):
                for file in files:
                    filepath = os.path.join(root, file)
                    tar.add(filepath, arcname=os.path.relpath(filepath, source_dir))
                    pbar.update(1)

def create_ssh_client(server, port, user, password):
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server, port, user, password)
    return ssh

def transfer_file(local_file_path, remote_file_path, ssh_client):
    ftp_client = ssh_client.open_sftp()
    file_size = os.path.getsize(local_file_path)  # Получаем размер файла в байтах
    file_size_gb = file_size / (1024 * 1024 * 1024)  # Конвертируем размер файла в гигабайты

    with tqdm(total=file_size_gb, desc='Передача', unit='GB') as pbar:
        def progress(transferred, total):
            pbar.update((transferred - pbar.n * (1024 * 1024 * 1024)) / (1024 * 1024 * 1024))
        ftp_client.put(local_file_path, remote_file_path, callback=progress)
    ftp_client.close()

def create_remote_directory(remote_dir, ssh_client):
    command = f"mkdir -p {remote_dir}"
    ssh_client.exec_command(command)

def extract_tarfile(remote_file_path, remote_dir, ssh_client):
    command = f"tar -xzf {remote_file_path} -C {remote_dir} && rm {remote_file_path}"
    stdin, stdout, stderr = ssh_client.exec_command(command)
    print(stdout.read().decode())
    print(stderr.read().decode())

def sync_directories(local_directory_path, remote_directory_path, server, port, user, password):
    start_time = time.time()  # Запускаем таймер
    
    # Создаем временный архив локальной директории
    tar_file = "archive.tar.gz"
    create_tarfile(local_directory_path, tar_file)

    # Подключаемся к SSH
    ssh = create_ssh_client(server, port, user, password)

    # Создаем целевую директорию на удаленном сервере, если она не существует
    create_remote_directory(remote_directory_path, ssh)

    # Определяем полный путь к архиву на удаленном сервере
    remote_tar_path = os.path.join(remote_directory_path, tar_file)

    # Передаем архив на удаленный сервер
    transfer_file(tar_file, remote_tar_path, ssh)

    # Проверяем, что файл был передан и существует
    ftp_client = ssh.open_sftp()
    try:
        ftp_client.stat(remote_tar_path)
        print(f"Файл {remote_tar_path} успешно передан на удаленный сервер.")
    except FileNotFoundError:
        print(f"Файл {remote_tar_path} не найден на удаленном сервере")
        return
    finally:
        ftp_client.close()

    # Проверяем содержимое удаленной директории перед распаковкой
    command = f"ls -a {remote_directory_path}"
    stdin, stdout, stderr = ssh.exec_command(command)
    print("Содержимое удаленной директории перед распаковкой:")
    print(stdout.read().decode())
    print(stderr.read().decode())

    # Распаковываем архив на удаленном сервере
    extract_tarfile(remote_tar_path, remote_directory_path, ssh)

    # Закрываем SSH-соединение
    ssh.close()

    # Удаляем временный архив
    os.remove(tar_file)

    end_time = time.time()  # Завершаем таймер
    elapsed_time = (end_time - start_time) / 100 # Вычисляем затраченное время
    print(f"Копирование завершено за {elapsed_time:.2f} минут.")

# Параметры подключения
server = '192.168.150.168'  # IP-адрес Ubuntu-машины
port = 2212  # Порт SSH
user = 'expert'  # Имя пользователя на Ubuntu
password = '123'  # Пароль пользователя

# Пути к директориям
local_directory_path = 'E:\\adb'  # Локальная директория на Windows
remote_directory_path = '/home/expert/Desktop/BackUp_SQL/'  # Целевой путь на Ubuntu

# Синхронизация директорий
sync_directories(local_directory_path, remote_directory_path, server, port, user, password)
