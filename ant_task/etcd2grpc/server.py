import argparse
import json
import time
import uuid
from concurrent import futures

import aioredis
import grpc
import redis

from ant_task.etcd2grpc import ant_pb2, ant_pb2_grpc, etcd
from ant_task.exception import AntTaskException, ERROR_LEVEL_DICT_RE
from ant_task.utils import get_server_port, get_server_ip


class AntRpcServer(ant_pb2_grpc.AntRpcServerServicer):
    def __init__(self, task_node: dict, server_url, token_list: list, redis_url, log_file):
        self.server_url = server_url
        self.token_list = token_list
        self.log_file = log_file
        self.redis_client = redis.from_url(redis_url, encoding="utf-8", decode_responses=True)
        self.task_nodes = task_node

    def run(self, request, context):
        if request.token not in self.token_list:
            return ant_pb2.AntResponse(code="6", msg="无效的token", response_data=None)
        self.token_list.remove(request.token)
        log = None
        try:
            task_dict = json.loads(request.task)
            task_cls = self.task_nodes.get(task_dict.get("task_name"), None)
            if not task_cls:
                return ant_pb2.AntResponse(code="6", msg=f"任务`{task_dict.get('task_name')}`未注册", response_data=None)
            task = task_cls()
            task.load(task_dict)
            task.log_file = self.log_file
            task.log_key = f"{task_dict.get('task_name')}|{request.token}"
            task.log_file_end = ".rpc.log"
            log = task.get_log()
            log.info(f"[start] {request.request_data} {request.task}")
            result = task(request.request_data)
            return ant_pb2.AntResponse(code="1", msg=None, response_data=json.dumps(result))
        except AntTaskException as e:
            return ant_pb2.AntResponse(
                code=str(ERROR_LEVEL_DICT_RE.get(e.level)), msg=e.dialect_msg,
                response_data=json.dumps(e.attach_data if e.attach_data else {}))
        except Exception as e:
            return ant_pb2.AntResponse(code="6", msg=str(e), response_data=None)
        finally:
            token = str(uuid.uuid4())
            self.token_list.append(token)
            self.redis_client.rpush(self.server_url, token)
            if log:
                log.info(f"[end]")


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
        task_node,
        log_file,
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
        ant_pb2_grpc.add_AntRpcServerServicer_to_server(
            AntRpcServer(task_node, server_url, rs.token_list, redis_url, log_file), grpc_server)
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
