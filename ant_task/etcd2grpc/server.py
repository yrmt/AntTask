import argparse
import json
import time
import uuid
from concurrent import futures

import aioredis
import grpc
import redis

from ant_task.etcd2grpc import ant_pb2, ant_pb2_grpc, etcd
from ant_task.exception import AntTaskException
from ant_task.utils import get_server_port, get_server_ip


def hahaha():
    time.sleep(1)
    return {"name": "zhihu", "value": "adsfasdf"}


class AntRpcServer(ant_pb2_grpc.AntRpcServerServicer):
    def __init__(self, server_url, token_list: list, redis_url):
        self.server_url = server_url
        self.token_list = token_list
        self.redis_client = redis.from_url(redis_url, encoding="utf-8", decode_responses=True)

    def run(self, request, context):
        request_data = json.loads(request.request_data)
        print("** start...", request_data, request.token, request.func, request.channel_name, request.dag_name)
        if request.token not in self.token_list:
            return ant_pb2.AntResponse(code="6", msg="无效的token", response_data=None)
        self.token_list.remove(request.token)
        try:
            result = hahaha()
            return ant_pb2.AntResponse(code="1", msg=None, response_data=json.dumps(result))
        except AntTaskException as e:
            return ant_pb2.AntResponse(code=e.level, msg=e.dialect_msg, response_data=json.dumps(e.attach_data))
        except Exception as e:
            return ant_pb2.AntResponse(code="6", msg=str(e), response_data=None)
        finally:
            token = str(uuid.uuid4())
            self.token_list.append(token)
            print("** end...", token)
            self.redis_client.rpush(self.server_url, token)


class RpcEtcdServer(object):
    token_list = None

    def __init__(self, redis_pool, server_url: str, etcd_key, etcd_host='localhost', etcd_port=2379):
        self.etcd_host = etcd_host
        self.etcd_port = etcd_port
        self.etcd_key = etcd_key
        self.server_url = server_url
        self.redis_pool = redis_pool
        self.etcd = etcd.Etcd(host=self.etcd_host, port=self.etcd_port, timeout=1)

    async def set_token(self, max_count):
        token_tmp = [str(uuid.uuid4()) for _ in range(max_count)]
        self.token_list = token_tmp
        await self.redis_pool.delete(self.server_url)
        await self.redis_pool.lpush(self.server_url, *token_tmp)

    def start(self):
        self.etcd.add_server(self.etcd_key, self.server_url)

    async def __aenter__(self):
        server_started_list = self.etcd.get_all_server(self.etcd_key)
        if self.server_url in server_started_list:
            raise Exception("服务已被注册")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.etcd.reduce_server(self.etcd_key, self.server_url)
        await self.redis_pool.delete(self.server_url)


async def run_rpc_server(
        service_port=8000,
        max_workers=None,
        redis_url="redis://127.0.0.1",
        etcd_url="127.0.0.1:2379",
        etcd_key='/AntTask/grpc'
):
    # 获取服务url
    service_port = get_server_port(service_port)
    server_url = f"{get_server_ip()}:{get_server_port(service_port)}"
    # etcd
    etcd_sp = etcd_url.split(":")
    etcd_host = etcd_sp[0]
    if len(etcd_sp) == 1:
        etcd_port = 2379
    else:
        etcd_port = int(etcd_sp[1])
    redis_client = aioredis.from_url(redis_url, encoding="utf-8", decode_responses=True)
    async with RpcEtcdServer(
            server_url=server_url,
            redis_pool=redis_client,
            etcd_host=etcd_host, etcd_port=etcd_port, etcd_key=etcd_key
    ) as rs:
        print(f'service {server_url} start...')
        pool = futures.ThreadPoolExecutor(max_workers=max_workers)
        grpc_server = grpc.server(pool)
        await rs.set_token(pool._max_workers)
        ant_pb2_grpc.add_AntRpcServerServicer_to_server(AntRpcServer(server_url, rs.token_list, redis_url), grpc_server)
        grpc_server.add_insecure_port(f'[::]:{service_port}')
        grpc_server.start()
        rs.start()
        print("serveice started")
        try:
            grpc_server.wait_for_termination()
        except KeyboardInterrupt:
            grpc_server.stop(0)


def parse_args():
    batch_parser = argparse.ArgumentParser(description="server参数")
    batch_parser.add_argument('-p', "--port", type=int, help="端口")
    batch_parser.add_argument('-w', "--worker", type=int, help="工作者")
    args = batch_parser.parse_args()
    if args.port is None:
        args.port = 8000
    return {"service_port": args.port, "max_workers": args.worker}
