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

    def __init__(self, dag_name, channel_name,
                 log_file: str = None, log_stream: bool = False, log_level=logging.DEBUG):
        self.dag_name = dag_name
        self.channel_name = channel_name
        self.log_file = log_file
        self.log_stream = log_stream
        self.log_level = log_level

    def check_run_before(self):
        """ 批前检查 """
        return True, 0, "批前检查默认通过"

    def param(self):
        return None

    def __call__(self, *args, **kwargs):
        if self.run is None:
            raise NotImplementedError
        try:
            self.run(*args, **kwargs)
        except AntTaskException as ae:
            print(f"{self.task_name}异常, 参数:{args},{kwargs}, {ae.level}--{ae.dialect_msg}")
            raise ae
        except Exception as e:
            print(f"{self.task_name}异常, 参数:{args},{kwargs}, {e}")
            raise e

    def get_log(self, group_dict: dict = None):
        if not self._mlog:
            self._mlog = MainLog("AntTask.task")
            if self.log_stream:
                self._mlog.add_stream_handler(self.log_level)
            if self.log_file:
                self._mlog.add_file_handler(
                    os.path.join(self.log_file, f"{self.dag_name}-{self.channel_name}.log"), self.log_level)
        if not self._log:
            self._log = self._mlog.get_log(group_dict)
        return self._log
