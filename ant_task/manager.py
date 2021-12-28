from ant_task import Dag, AntTaskException, Task
from collections import Iterable
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, as_completed
from multiprocessing import Pool


def run_iter(task: Task, iterlist):
    for i in iterlist:
        task(i)


def run_iter_thread(task: Task, param):
    with ThreadPoolExecutor(task.threading_pool_size) as executer:
        result = [executer.submit(task, p) for p in param]
        wait(result, return_when=ALL_COMPLETED)
        for f in as_completed(result):
            f.result()


class TaskManger(object):
    process_executer = None

    def task_runner(self, task_cls):
        task = task_cls()
        check_pass, check_level, check_result = task.check_run_before()
        param = task.param()
        if not check_pass:
            raise AntTaskException(level=check_level, dialect_msg=f"{task.task_name}任务检测不通过:{check_result}")
        if not isinstance(param, Iterable):
            return self.process_executer.apply_async(task)
        if task.run_type == "thread":
            return self.process_executer.apply_async(run_iter_thread, args=(task, param))
        elif task.run_type == "process":
            return self.process_executer.map_async(task, param, chunksize=task.process_chunk_size)
        elif task.run_type == "single" or task.run_type is None:
            return self.process_executer.apply_async(run_iter, args=(task, param))
        else:
            raise AntTaskException(level=6, dialect_msg=f"运行类型无`{task.run_type}`此方法")

    def dag_runner(self, dag: Dag):
        dag.status.set_name(dag.dag_name, dag.channel_name)
        dag.status.dag_init(dag)
        dag_run_status = dag.status.dag_start()
        if dag_run_status == dag.status.RUN_SKIP:
            return
        for tfs in dag.task_flower:
            results = {}
            for task in tfs:
                task_run_status = dag.status.task_start(task.task_name)
                if task_run_status == dag.status.RUN_NORMAL or task_run_status is None:
                    t_result = self.task_runner(task)
                    results[task.task_name] = t_result
            for name, result in results.items():
                try:
                    result.get()
                    dag.status.task_end(name, None)
                except Exception as e:
                    dag.status.task_end(name, e)
        dag.status.dag_end()

    def __init__(self, pool_size=None):
        self.process_pool_size = pool_size

    def __enter__(self):
        if not self.process_executer:
            self.process_executer = Pool(self.process_pool_size)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.process_executer:
            self.process_executer.__exit__(exc_type, exc_value, traceback)
