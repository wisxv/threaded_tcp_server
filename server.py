import argparse
import sys
from pathlib import Path
import logging
import socket
import threading
import json
import signal
import shutil
from typing import Callable, Dict, Any, Optional

# настройка логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# логирование в консоль
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
# логирование в файл
file_handler = logging.FileHandler('server.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def get_args() -> argparse.Namespace:
    # получение аргументов из командной строки
    parser = argparse.ArgumentParser(description='TCP Server')
    parser.add_argument('-q', '--quarantine-directory',
                        type=Path,
                        default=default_quarantine_dir,
                        help='Directory for quarantine')
    parser.add_argument('-t', '--threads-max',
                        type=int, default=default_max_threads_count,
                        help='Maximum number of threads. Default is 4')

    args = parser.parse_args()
    return args


class ThreadedTCPServer:
    """
    Многопоточный tcp-сервер
    """
    def __init__(self, ip: str = '127.0.0.1', port: int = 65432, max_threads: int = 4) -> None:
        self.host: str = ip
        self.port: int = port

        # флаг завершения работы
        self.shutdown_event: threading.Event = threading.Event()
        # Семафор для ограничения максимального числа одновременно работающих клиентских потоков
        self.__thread_semaphore: threading.Semaphore = threading.Semaphore(max_threads)
        self.__threads: list[threading.Thread] = []  # список клиентских потоков
        self.__server_socket: Optional[socket.socket] = None

        # поддерживаемые команды, вызываемые ими функции
        self.commands: Dict[str, Callable] = {
            "CheckLocalFile": check_local_file,
            "QuarantineLocalFile": quarantine_local_file,
        }

    def handle_client(self, conn: socket.socket, addr: tuple) -> None:
        """
        Обрабатывает подключение клиентов

        :param conn: сокет подключения клиента
        :param addr: адрес клиента
        :return: None
        """
        logger.info(f'Connected: {addr}')
        buffer = ""
        buffer_limit = 1024 * 1024  # 1 МБ

        try:
            while not self.shutdown_event.is_set():
                # чтение данных из сокета
                data = conn.recv(1024)
                if not data:
                    break

                # добавление новых данных в буфер
                buffer += data.decode()

                # проверка размера буфера
                if len(buffer) > buffer_limit:
                    logger.warning(f"Buffer overflow: Clearing buffer. Current size: {len(buffer)} bytes")
                    buffer = ""
                    continue

                # попытка парсинга ожидаемой структуры json
                try:
                    logger.debug('Parsing json')
                    message = json.loads(buffer)
                    buffer = ""  # очистка буфера после успешного парсинга
                    command = message.get("name")
                    params = message.get("params")

                    # проверка команды
                    if command in self.commands:
                        try:
                            # вызов команды, запись результата
                            result = self.commands[command](params)
                            logger.info(f'Executed: {self.commands[command].__name__} with {params}')
                            response = result
                        except Exception as e:
                            logger.error(f'Error executing {self.commands[command]}: {e}')
                            response = {"status": False, "result": "error"}
                    else:
                        logger.error(f'Wrong command: {command}')
                        response = {"status": False, "result": "wrong_command"}

                    # отправка ответа о статусе выполнения
                    conn.sendall(json.dumps(response).encode())
                except json.JSONDecodeError:
                    # Если произошла ошибка декодирования JSON, значит данные ещё не полные.
                    # Продолжим получать данные.
                    continue
        finally:
            # note_1
            logger.debug("Closing connection")
            conn.close()
            logger.debug("Releasing semaphore")
            self.__thread_semaphore.release()
            logger.info(f'Disconnected: {addr}')
            # Удаление завершенного потока из списка
            self.__threads = [t for t in self.__threads if t.is_alive()]

    def start_server(self) -> None:
        """
        Запускает сервер, который принимает входящие TCP-соединения.

        :return: None
        """
        # создание сокета
        self.__server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # привязка сокета к порту
        self.__server_socket.bind((self.host, self.port))
        # прослушивание сокета
        self.__server_socket.listen()
        # Установка тайм-аута для цикла accept
        self.__server_socket.settimeout(1)

        # note_2
        logger.info("Waiting for clients")

        while not self.shutdown_event.is_set():
            try:
                # принять соединение
                conn, addr = self.__server_socket.accept()
                # уменьшить счетчик семафора
                logger.debug('Acquiring semaphore')
                self.__thread_semaphore.acquire()
                # создать отдельный поток для клиента
                client_thread = threading.Thread(target=self.handle_client, args=(conn, addr))
                self.__threads.append(client_thread)  # Добавляем поток в список
                client_thread.start()
            except socket.timeout:
                continue
            except socket.error as e:
                if self.shutdown_event.is_set():
                    break
                else:
                    logger.error(f"Can't accept connection: {e}")

    def sigint_handler(self, _signum: int, _frame: Optional[Any]) -> None:
        """
        Обработчик сигнала прерывания (SIGINT) для завершения работы сервера.

        :return: None
        """
        logger.info('Got SIGINT, shutting down')
        self.shutdown_event.set()

        # разрыв спящего состояния accept()
        if self.__server_socket:
            self.__server_socket.close()

    def run(self) -> None:
        # Установка обработчика сигнала
        signal.signal(signal.SIGINT, self.sigint_handler)
        # Запуск основного потока сервера
        server_thread = threading.Thread(target=self.start_server)
        server_thread.start()
        # Цикл ожидания завершения
        while not self.shutdown_event.is_set():
            try:
                logger.debug("Pausing signal")
                # note_3
                signal.pause()
            except Exception as e:
                logger.debug(f"Exception while pausing: {e}")
                break

        logger.debug("Waiting main thread to shut down")
        server_thread.join()
        # note_4
        logger.debug("Waiting child threads to shut down")
        for thread in self.__threads:
            thread.join()
        logger.info("All threads shut down gracefully")


def check_and_create_directory(path: Path, default: Path) -> Path:
    # обработка заданного пути карантина
    if path.is_dir():  # уже есть
        logger.debug(f"Quarantine directory already exists")
        return path
    try:  # еще нет
        logger.debug(f"Creating quarantine directory")
        path.mkdir(parents=True, exist_ok=True)
        return path
    except Exception as e:
        logger.error(f"Can't create directory {path}, trying to use default: {e}")

        # Попытка создать директорию по умолчанию
        if default.is_dir():  # уже есть
            logger.debug("Default quarantine directory already exists")
            return default
        try:
            logger.debug("Creating default quarantine directory")
            default.mkdir(parents=True, exist_ok=True)
            return default
        except Exception as ex:
            logger.error(f"Can't create default directory {default}, exiting program: {ex}")
            sys.exit(1)  # Выход из программы


def check_local_file(params: dict) -> dict:
    """
    Проверяет указанный файл на наличие заданной сигнатуры.

    :param params: словарь параметров, полученных от клиента
    :return:
    """

    # Получить исходный файл из параметров
    original_file = Path(params.get('file_path'))
    signature = params.get("signature")

    # проверка на None
    if any((i is None) for i in (original_file, signature)):
        logger.info(f'Incomplete parameters: path={original_file}, signature={signature}')
        return {"status": False, "result": f"incomplete_parameters"}

    # существование файла
    if (not original_file.exists()) or (not original_file.is_file()):
        logger.error(f'No such file: {original_file}')
        return {"status": False, "result": f"no_such_file"}

    # корректность сигнатуры
    # проверка длины
    if not (len(signature) <= max_signature_len * 2):
        logger.info(f"Too big signature: {len(signature) // 2}/{max_signature_len} (given/allowed Kilobytes)")
        return {"status": False, "result": f"too_big_signature"}
    try:
        # Сигнатура - строковое представление байтов в hex виде?
        signature = bytes.fromhex(signature)
    except ValueError:
        logger.error(f"Can't represent given signature as string of hex digits: {signature}")
        return {"status": False, "result": f"not_a_signature"}

    try:
        offsets = []
        with open(original_file, "rb") as f:
            data = f.read()
            offset = data.find(signature)
            while offset != -1:
                offsets.append(offset)
                offset = data.find(signature, offset + 1)
        return {"status": True, "result": offsets}  # можно возвращать None если не найдены
    except Exception as e:
        return {"status": False, "result": f"error: {e}"}


def quarantine_local_file(params: dict) -> dict:
    """
    Перемещение локального файла в указанную директорию

    :param params: словарь параметров, полученных от клиента
    :return: Статус выполнения и результат
    """
    try:
        # note_5
        # Получить исходный файл из параметров
        original_file = Path(params.get('file_path'))

        if not original_file.is_file():
            return {"status": False, "result": "no_such_file"}

        # Назначение нового пути с тем же именем файла в директории карантина
        quarantine_file_path = quarantine_directory / original_file.name
        # Перемещение файла
        shutil.move(str(original_file), str(quarantine_file_path))

        return {"status": True, "result": "moved"}

    except Exception as e:
        logger.error(f"Error quarantining file: {e}")
        return {"status": False, "result": "error"}


if __name__ == "__main__":
    # максимальный размер сигнатуры в Кб
    max_signature_len = 1024
    # сетевые настройки сервера
    host = '127.0.0.1'
    port = 65432

    # директория карантина и кол-во потоков по умолчанию
    default_quarantine_dir = Path('./quarantine')
    default_max_threads_count = 4

    # получение аргументов из командной строки
    args = get_args()
    threads = args.threads_max
    quarantine_directory = check_and_create_directory(args.quarantine_directory, default_quarantine_dir)

    logger.info(f"Quarantine directory: {quarantine_directory}")
    logger.info(f"Max threads: {threads}")

    # запуск сервера
    server = ThreadedTCPServer(port=port,
                               ip=host,
                               max_threads=threads)
    server.run()
