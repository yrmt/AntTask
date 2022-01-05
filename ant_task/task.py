import json
import logging
import os

from ant_task.log import MainLog
from ant_task.exception import AntTaskException


class Task(object):
    """
        单个任务执行方法，环境配置
    """
    env: dict
    run = None
    task_name = None
    channel_name = None
    dag_name = None
    threading_pool_size = None
    chunk_size = None
    _mlog = None
    _log = None
    dump_keys = ["task_name", "dag_name", "channel_name", "log_level", "log_key", "env"]
    log_file_end = ".log"

    def __init__(
            self, dag_name: str = None, channel_name: str = None,
            log_file: str = None, log_stream: bool = False, log_level=logging.DEBUG, log_key: str = None
    ):
        self.log_key = log_key if log_key else self.task_name
        self.dag_name = dag_name
        self.channel_name = channel_name
        self.log_file = log_file
        self.log_stream = log_stream
        self.log_level = log_level

    def check_run_before(self):
        """ 批前检查 """
        return True, 0, "批前检查默认通过"

    def param(self):
        """
            参数 返回 None 无任何参数调用一次
                    [] 不调用
                    [{"index":1},{"index":2},{"index":3}]
        :return:
        """
        return None

    def __call__(self, request_data):
        if self.run is None:
            raise NotImplementedError
        try:
            if request_data:
                return self.run(**json.loads(request_data))
            else:
                return self.run()
        except AntTaskException as ae:
            self.get_log().error(f"入参:{request_data},level:{ae.level} msg:{ae.log_msg}")
            raise ae
        except Exception as e:
            self.get_log().error(f"入参:{request_data}")
            self.get_log().exception(e)
            raise e

    def get_log(self):
        if not self._mlog:
            self._mlog = MainLog("AntTask.task")
            if self.log_stream:
                self._mlog.add_stream_handler(self.log_level)
            if self.log_file:
                self._mlog.add_file_handler(
                    os.path.join(self.log_file, f"{self.dag_name}-{self.channel_name}{self.log_file_end}"),
                    self.log_level)
        if not self._log:
            self._log = self._mlog.get_log({"log_key": self.log_key})
        return self._log

    def dump(self):
        return json.dumps({str(k): getattr(self, str(k)) for k in self.dump_keys}, ensure_ascii=False)

    def load(self, task_str):
        task_str = json.loads(task_str) if isinstance(task_str, str) else task_str
        for k, v in task_str.items():
            setattr(self, k, v)
