import asyncio
import copy
import json
import math
import random
from typing import Iterable

import aioredis
import grpc

from ant_task.etcd2grpc import ant_pb2_grpc, ant_pb2, etcd
from ant_task.exception import AntTaskException
from ant_task.task import Task


class RpcClient(etcd.Etcd):
    _listener = None

    def __enter__(self):
        self.server_list = self.get_all_server(self.etcd_key)
        self._listener = self.add_watch_callback(self.etcd_key, self._update_server)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._listener:
            self.cancel_watch(self._listener)

    def __init__(self, redis_url: str, etcd_key, log, etcd_host='localhost', etcd_port=2379, chunksize=None, ):
        self.redis_client = aioredis.from_url(redis_url, encoding="utf-8", decode_responses=True)
        self.etcd_ip = etcd_host
        self.etcd_port = etcd_port
        self.etcd_key = etcd_key
        self.chunksize = chunksize
        self.log = log
        super().__init__(host=etcd_host, port=etcd_port)

    async def get_one_server(self):
        if len(self.server_list) <= 0:
            raise AntTaskException(level=6, dialect_msg=f"无可用服务端")
        for _ in range(60 * 60 * 3):
            server_list = copy.copy(self.server_list)
            for i in range(len(server_list)):
                choice_server = random.choice(server_list)
                token = await self.redis_client.lpop(choice_server)
                if token:
                    return choice_server, token
                server_list.remove(choice_server)
            await asyncio.sleep(2)
        raise AntTaskException(level=6, dialect_msg=f"服务端长时间满载")

    def _update_server(self, event_list):
        server_list = json.loads(event_list.events[0].value)
        if not isinstance(server_list, list):
            raise AntTaskException(level=6, dialect_msg=f"etcd获取数据类型异常`{type(server_list)}`")
        self.server_list = server_list

    async def run(self, task_str, request_datas):
        chunksize = self.chunksize
        if not isinstance(request_datas, Iterable):
            result = await self.grpc_runner(task_str, None)
            return [result]
        if not chunksize:
            server_list = self.get_all_server(self.etcd_key)
            server_activate_len = 0
            for server in server_list:
                len_server = await self.redis_client.llen(server)
                server_activate_len += len_server
            chunksize = max(1, math.floor(server_activate_len * 0.8))
        result = []
        batch_count = math.ceil(len(list(request_datas)) / chunksize)
        self.log.debug(f"数据共计:{len(list(request_datas))}条,已被分成:{batch_count}次运行.")
        for i in range(batch_count):
            task_list = []
            for request_data in request_datas[i * chunksize:(i + 1) * chunksize]:
                task_list.append(asyncio.create_task(self.grpc_runner(task_str, request_data)))
            result_i = await asyncio.gather(*task_list)
            result.extend(result_i)
        return result

    async def grpc_runner(self, task_str, request_data):
        server_url, token = await self.get_one_server()
        self.log.debug(f"[{server_url}|{token}|rpc|start] {task_str}")
        try:
            async with grpc.aio.insecure_channel(server_url) as channel:
                stub = ant_pb2_grpc.AntRpcServerStub(channel)
                response = await stub.run(
                    ant_pb2.AntRequest(
                        task=task_str, token=token,
                        request_data=json.dumps(request_data) if request_data else None
                    ))
                if response.code > "2":
                    self.log.error(f"[{server_url}|{token}|rpc|end] rpc_end:code={response.code},msg={response.msg}")
                    raise AntTaskException(
                        level=int(response.code), dialect_msg=response.msg, attach_data=response.response_data)
                elif response.code == "2":
                    self.log.info(f"[{server_url}|{token}|rpc|end] rpc_end:code={response.code},msg={response.msg}")
                else:
                    self.log.debug(f"[{server_url}|{token}|rpc|end] rpc_end:code={response.code},msg={response.msg}")
                return json.loads(response.response_data) if response.response_data else None
        except Exception as e:
            self.log.exception(e)
            raise e


def run_rpc(task: Task, request_datas):
    loop = asyncio.get_event_loop()
    with RpcClient(
            redis_url="redis://127.0.0.1",
            chunksize=None, log=task.get_log(),
            etcd_key='/AntTask/grpc', etcd_host="127.0.0.1", etcd_port=2379,
    ) as ec:
        run = ec.run(task.dump(), request_datas)
        if loop.is_running():
            loop.create_task(run)
        else:
            loop.run_until_complete(run)
