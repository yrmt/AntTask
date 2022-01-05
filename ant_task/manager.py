import logging
import os
from collections import Iterable
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, as_completed
from multiprocessing import Pool

from ant_task import MainLog
from ant_task.dag import Dag
from ant_task.etcd2grpc.client import run_rpc
from ant_task.exception import AntTaskException
from ant_task.task import Task


def run_single(task: Task, iterlist):
    for i in iterlist:
        task(i)


def run_iter_thread(task: Task, param):
    with ThreadPoolExecutor(task.threading_pool_size) as executer:
        result = [executer.submit(task, p) for p in param]
        wait(result, return_when=ALL_COMPLETED)
        for f in as_completed(result):
            f.result()


class TaskManger(object):
    _executer = None

    def task_runner(self, task_cls, run_type, dag_name, channel_name):
        task = task_cls(dag_name, channel_name, self.log_file, self.log_stream, self.log_level)
        check_pass, check_level, check_result = task.check_run_before()
        if not check_pass:
            raise AntTaskException(level=check_level, dialect_msg=f"{task.task_name}任务检测不通过:{check_result}")
        param = task.param()
        if not isinstance(param, Iterable) and run_type != "rpc":
            return self.executer.apply_async(task)
        if run_type == "thread":
            return self.executer.apply_async(run_iter_thread, args=(task, param))
        elif run_type == "process":
            return self.executer.map_async(task, param, chunksize=task.chunk_size)
        elif run_type == "single":
            return self.executer.apply_async(run_single, args=(task, param))
        elif run_type == "rpc":
            return self.executer.apply_async(run_rpc, args=(task, param))
        else:
            raise AntTaskException(level=6, dialect_msg=f"运行类型无`{run_type}`此方法")

    def dag_runner(self, dag: Dag):
        log_key = f"{dag.dag_name}|{dag.channel_name}" + f"|{dag.status.key()}" if dag.status.key() else ""
        log = self._mlog.get_log({"log_key": log_key})
        dag.status.set_some(dag.dag_name, dag.channel_name, log, dag.run_type)
        dag.status.dag_init(dag)
        dag_run_status = dag.status.dag_start()
        if dag_run_status == dag.status.RUN_SKIP:
            return
        for tfs in dag.task_flower:
            results = {}
            for task in tfs:
                task_run_status = dag.status.task_start(task.task_name)
                if task_run_status == dag.status.RUN_NORMAL or task_run_status is None:
                    t_result = self.task_runner(task, dag.run_type, dag.dag_name, dag.channel_name)
                    results[task.task_name] = t_result
            is_down = False
            for name, result in results.items():
                try:
                    result.get()
                    dag.status.task_end(name, None)
                except Exception as e:
                    dag.status.task_end(name, e)
                    is_down = True
            if is_down:
                return
        dag.status.dag_end()

    def __init__(
            self,
            log_file: str = None,
            log_stream: bool = False,
            pool_size=None,
            log_level=logging.DEBUG,
            log_dag_name="dag.log",
    ):
        self.process_pool_size = pool_size
        self.log_file = log_file
        self.log_stream = log_stream
        self.log_level = log_level
        self._mlog = MainLog("AntTask.dag")
        if log_stream:
            self._mlog.add_stream_handler(log_level)
        if log_file:
            self._mlog.add_file_handler(os.path.join(log_file, log_dag_name), log_level)

    @property
    def executer(self):
        if not self._executer:
            self._executer = Pool(self.process_pool_size)
        return self._executer

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._executer:
            self._executer.__exit__(exc_type, exc_value, traceback)
