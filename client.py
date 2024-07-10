import json
import os
from os.path import getsize, splitext, basename
import hashlib
import argparse
import struct
import time
from socket import *
from tqdm import tqdm

OP_SAVE, OP_DELETE, OP_GET, OP_UPLOAD, OP_DOWNLOAD, OP_BYE, OP_LOGIN, OP_ERROR = 'SAVE', 'DELETE', 'GET', 'UPLOAD', 'DOWNLOAD', 'BYE', 'LOGIN', "ERROR"
TYPE_FILE, TYPE_DATA, TYPE_AUTH, DIR_EARTH = 'FILE', 'DATA', 'AUTH', 'EARTH'
FIELD_OPERATION, FIELD_DIRECTION, FIELD_TYPE, FIELD_USERNAME, FIELD_PASSWORD, FIELD_TOKEN = 'operation', 'direction', 'type', 'username', 'password', 'token'
FIELD_KEY, FIELD_SIZE, FIELD_TOTAL_BLOCK, FIELD_MD5, FIELD_BLOCK_SIZE = 'key', 'size', 'total_block', 'md5', 'block_size'
FIELD_STATUS, FIELD_STATUS_MSG, FIELD_BLOCK_INDEX = 'status', 'status_msg', 'block_index'
DIR_REQUEST, DIR_RESPONSE = 'REQUEST', 'RESPONSE'


def _argparse():
    parse = argparse.ArgumentParser()
    parse.add_argument("--server_ip", default='', action='store', required=False, dest="ip",
                       help="The IP address bind to the server. Default bind all IP.")
    parse.add_argument("--port", default='1379', action='store', required=False, dest="port",
                       help="The port that server listen on. Default is 1379.")
    parse.add_argument("--id", default='', action='store', required=False, dest='id', help="Student ID.")
    parse.add_argument("--f", default='', action='store', required=False, dest='file_path',
                       help="The path to the file in PC. Default upload no file.")
    return parse.parse_args()


def get_file_md5(filename):
    m = hashlib.md5()
    with open(filename, 'rb') as fid:
        while True:
            d = fid.read(2048)
            if not d:
                break
            m.update(d)
    return m.hexdigest()


def connect_to_server(ip, port):
    client_socket = socket(AF_INET, SOCK_STREAM)
    client_socket.connect((ip, int(port)))
    return client_socket


def make_packet(json_data, bin_data=None):
    j = json.dumps(dict(json_data), ensure_ascii=False)
    j_len = len(j)
    if bin_data is None:
        return struct.pack('!II', j_len, 0) + j.encode()
    else:
        return struct.pack('!II', j_len, len(bin_data)) + j.encode() + bin_data


def make_request_packet(data_type, operation, json_data, bin_data=None):
    json_data[FIELD_TYPE] = data_type
    json_data[FIELD_OPERATION] = operation
    json_data[FIELD_DIRECTION] = DIR_REQUEST
    return make_packet(json_data, bin_data)


def get_tcp_packet(conn):
    bin_data = b''
    while len(bin_data) < 8:
        data_rec = conn.recv(8)
        if data_rec == b'':
            time.sleep(0.01)
        if data_rec == b'':
            return None, None
        bin_data += data_rec
    data = bin_data[:8]
    bin_data = bin_data[8:]
    j_len, b_len = struct.unpack('!II', data)
    while len(bin_data) < j_len:
        data_rec = conn.recv(j_len)
        if data_rec == b'':
            time.sleep(0.01)
        if data_rec == b'':
            return None, None
        bin_data += data_rec
    j_bin = bin_data[:j_len]

    try:
        json_data = json.loads(j_bin.decode())
    except Exception as ex:
        return None, None

    bin_data = bin_data[j_len:]
    while len(bin_data) < b_len:
        data_rec = conn.recv(b_len)
        if data_rec == b'':
            time.sleep(0.01)
        if data_rec == b'':
            return None, None
        bin_data += data_rec
    return json_data, bin_data


def authorization(client_socket, make_request_data, get_tcp_packet, user_id):
    password = hashlib.md5(user_id.encode()).hexdigest()
    client_socket.send(
        make_request_data(
            TYPE_AUTH, OP_LOGIN,
            {
                FIELD_USERNAME: user_id,
                FIELD_PASSWORD: password
            }
        )
    )
    json_data, bin_data = get_tcp_packet(client_socket)
    if json_data[FIELD_STATUS] != 200:
        print(
            f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())} [ERROR] An error has occurred: Status Code: {json_data[FIELD_STATUS]}, Details: {json_data[FIELD_STATUS_MSG]}')
    else:
        token = json_data[FIELD_TOKEN]
        print(f"Token: {token}")
    return token


def upload_file(client_socket, file_path, make_request_data, get_tcp_packet, token):
    if os.path.exists(file_path) is False:
        print(
            f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())} [ERROR] The file path {file_path} does not exist.')
        return
    with open(file_path, 'rb') as file:
        file_size = getsize(file_path)
        filename, extension_name = splitext(basename(file_path))
        key = filename + extension_name
        client_socket.send(
            make_request_data(
                TYPE_FILE, OP_SAVE,
                {
                    FIELD_TOKEN: token,
                    FIELD_KEY: key,
                    FIELD_SIZE: file_size
                }
            )
        )
    json_data, bin_data = get_tcp_packet(client_socket)
    if json_data[FIELD_STATUS] != 200:
        print(
            f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())} [ERROR] An error has occurred: Status Code: {json_data[FIELD_STATUS]}, Details: {json_data[FIELD_STATUS_MSG]}')
        return
    else:
        print(
            f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())} [INFO] Apply successfully! Here is the upload plan: Key: {json_data[FIELD_KEY]}, Block Size: {json_data[FIELD_BLOCK_SIZE]}, Total Blocks: {json_data[FIELD_TOTAL_BLOCK]}.')

    key = json_data[FIELD_KEY]
    block_size = json_data[FIELD_BLOCK_SIZE]
    total_blocks = json_data[FIELD_TOTAL_BLOCK]

    block_index = 0
    upload_bar = tqdm(total=total_blocks, desc='Uploading Process', leave=True)
    start = time.perf_counter()
    with open(file_path, 'rb') as file:
        while block_index < total_blocks:
            block_data = file.read(block_size)
            data_size = len(block_data)
            if not block_data:
                break
            client_socket.send(
                make_request_data(
                    TYPE_FILE, OP_UPLOAD,
                    {
                        FIELD_TOKEN: token,
                        FIELD_KEY: key,
                        FIELD_SIZE: data_size,
                        FIELD_BLOCK_INDEX: block_index
                    }, block_data
                )
            )
            json_data, bin_data = get_tcp_packet(client_socket)
            if json_data[FIELD_STATUS] == 200:
                upload_bar.update(1)
                if block_index < total_blocks - 1:
                    block_index += 1
                    continue
                else:
                    md5 = get_file_md5(file_path)
                    if md5 == json_data[FIELD_MD5]:
                        upload_bar.close()
                        print(
                            f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())} [INFO] The MD5 value {json_data[FIELD_MD5]} is right.')
                        print(
                            f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())} [INFO] The server has received the file properly.')
                        break
                    else:
                        print(
                            f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())} [ERROR] The MD5 value {json_data[FIELD_MD5]} is wrong.')
                        break
            else:
                print(
                    f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())} [ERROR] An error has occurred: Status Code: {json_data[FIELD_STATUS]}, Details: {json_data[FIELD_STATUS_MSG]}')
                break
    end = time.perf_counter()
    exe_time = end - start
    avg_speed = (file_size / exe_time) / 1048576.0
    print(
        f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())} [INFO] The upload operation has successfully cpmpleted.')
    print(
        f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())} [INFO] The total upload-processing time: {exe_time:.4f}s.')
    print(
        f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())} [INFO] The average processing speed: {avg_speed:.4f}MB/s')


def main():
    parser = _argparse()
    ip = parser.ip
    port = parser.port
    student_id = parser.id
    file_path = parser.file_path

    client_socket = connect_to_server(ip, port)

    token = authorization(client_socket, make_request_packet, get_tcp_packet, student_id)
    upload_file(client_socket, file_path, make_request_packet, get_tcp_packet, token)

    client_socket.close()


if __name__ == '__main__':
    main()