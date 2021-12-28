from ant_task import AntTaskException


class Task(object):
    """
        单个任务执行方法，环境配置
    """

    RUN_TYPE_SINGLE = "single"
    RUN_TYPE_THREAD = "thread"
    RUN_TYPE_PROCESS = "process"

    env: dict
    run = None
    run_type = None  # ("single", "thread", "process", None= "single")
    task_name = None
    channel_name = None
    threading_pool_size = None
    threading_chunk_size = None
    process_chunk_size = None

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
