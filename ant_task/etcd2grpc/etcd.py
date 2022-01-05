import json
import etcd3
from ant_task.exception import AntTaskException


class Etcd(etcd3.Etcd3Client):
    def get_all_server(self, key, **kwargs) -> list:
        values, _ = self.get(key, **kwargs)
        values_list = []
        if values is not None:
            values_list = json.loads(values.decode())
            if not isinstance(values_list, list):
                raise AntTaskException(level=6, dialect_msg=f"etcd获取数据类型异常`{type(values_list)}`")
        return values_list

    def add_server(self, key, server_url: str):
        with self.lock(key):
            v_tmp = self.get_all_server(key)
            if server_url not in v_tmp:
                v_tmp.append(server_url)
                self.put(key, json.dumps(v_tmp).encode())
            else:
                raise AntTaskException(level=6, dialect_msg=f"服务注册信息异常`{server_url}节点已存在`")

    def reduce_server(self, key, server_url):
        with self.lock(key):
            v_tmp = self.get_all_server(key)
            if server_url in v_tmp:
                v_tmp.remove(server_url)
                self.put(key, json.dumps(v_tmp).encode())
            else:
                raise AntTaskException(level=6, dialect_msg=f"服务注册信息异常`{server_url}节点不存在`")
