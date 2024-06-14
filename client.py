import argparse
import sys
import socket
import json


def send_command_to_server(command: str, host: str = '127.0.0.1', port: int = 65432, params: dict = None, ) -> dict:
    """
    Отправляет команду на сервер и возвращает ответ.

    :param command: команда для отправки
    :param params: параметры команды
    :param host: ip сервера
    :param port: порт сервера
    :return: ответ сервера в виде словаря
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))

            # Формируем сообщение в формате JSON
            message = {"name": command, "params": params}
            s.sendall(json.dumps(message).encode())

            # Получаем ответ от сервера
            data = s.recv(1024)
            response = json.loads(data.decode())
            return response
    except ConnectionRefusedError:
        return {"status": False, "message": "connection_refused"}
    except Exception as e:
        return {"status": False, "message": str(e)}


def is_path_synthax(path: str) -> bool:
    """
    Проверяет строку: похожа ли она на путь до файла по синтаксису

    :param path: строка, которую необходимо проверить
    :return: True если синтаксис похож на путь до файла, иначе False
    """
    if (path.startswith('.') or path.startswith('/')) and not (path[-1] == '/'):
        return True
    else:
        return False


def main():
    # данные сервера: можно получать из командной строки, но в задании не сказано, что клиент должен это делать
    host = '127.0.0.1'
    port = 65432
    # значение максимальной длины сигнатуры в Кб
    max_signature_len = 1024

    parser = argparse.ArgumentParser(description='Client to either check a remote file or quarantine it')

    # Сохраняем оригинальную версию print_help
    original_print_help = parser.print_help

    # Переопределяем метод print_help
    def custom_print_help():
        original_print_help()
        print("\nSubcommand arguments:")
        for subcommand, subparser in subparsers.choices.items():
            print(f"\n{subcommand}:\n" + subparser.format_help())

    parser.print_help = custom_print_help

    subparsers = parser.add_subparsers(dest='command', help='Available subcommands')

    # Подкоманда check-file
    check_parser = subparsers.add_parser('check-file', help='Check the remote file for signature.')
    check_parser.add_argument('-f', '--file', type=str, required=True,
                              help='Path to the local file on the remote server.')
    check_parser.add_argument('-s', '--signature', type=str, required=True,
                              help="Hex string signature to check against the file (up to 1024 bytes). "
                                   "Example: 774066 (for 'w@f' in UTF-8)")

    # Подкоманда quarantine-file
    quarantine_parser = subparsers.add_parser('quarantine-file', help='Quarantine the local file.')
    quarantine_parser.add_argument('-f', '--file', type=str, required=True,
                                   help='Path to the local file to quarantine. '
                                        'Formats: '
                                        '    /path/to/file<.extension> '
                                        '    ./current/server/dir/file<.extension>')

    args = parser.parse_args()

    # Если не указана команда, напечатать помощь
    if not args.command:
        parser.print_help()

    if args.command == 'check-file':
        file_path = args.file
        signature = args.signature

        # Базовая проверка корректности синтаксиса пути файла
        # Недопустимые символы в пути отличаются для Win и Linux - Отлавливается на стороне сервера.
        if is_path_synthax(file_path):
            # Длина сигнатуры 1024 Кб? (*2 потому что байты в hex виде занимают 2 символа)
            if len(signature) <= max_signature_len * 2:
                try:
                    # Сигнатура - строковое представление байтов в hex виде?
                    bytes.fromhex(signature)
                except ValueError:
                    print(f"Wrong signature")
                    custom_print_help()
                    sys.exit(1)

                params = {"file_path": file_path,
                          "signature": signature}
                response = send_command_to_server(command="CheckLocalFile",
                                                  host="127.0.0.1",
                                                  port=65432,
                                                  params=params)
                print(response)

    elif args.command == 'quarantine-file':
        file_path = args.file
        if is_path_synthax(file_path):
            params = {"file_path": file_path}
            response = send_command_to_server(command="QuarantineLocalFile",
                                              host="127.0.0.1",
                                              port=65432,
                                              params=params)
            print(response)
    else:
        custom_print_help()


if __name__ == "__main__":
    main()
