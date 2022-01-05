import socket


def get_port_used(ip, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((ip, port))
        return True
    except Exception as e:
        return False
    finally:
        s.close()


def get_server_port(service_port):
    for i in range(20):
        if not get_port_used("127.0.0.1", service_port):
            return service_port
        service_port += 1
    raise Exception(f"{service_port - 20}-{service_port - 1}的端口全被你起了")


def get_server_ip():
    server_ip = [
        (s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in
        [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]
    return server_ip
