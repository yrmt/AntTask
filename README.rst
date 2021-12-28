AntTask
=====

AntTask 批量任务处理轮子


定义Task
----------
.. code-block:: text

    from ant_task import Task

    # 任务节点定义需继承自 ant_task.Task
    class DemoFunc(Task):
        task_name = "demo_01" # 设置任务名称

        # 执行方式 不配置默认按照单线程执行。
        # 可选方式  RUN_TYPE_SINGLE:单线程模式, RUN_TYPE_THREAD 多线程模式， RUN_TYPE_PROCESS 多进程模式
        run_type = Task.RUN_TYPE_THREAD
        # 如果为 RUN_TYPE_THREAD 模式执行，线程数量可配置
        threading_pool_size = 5

        # 环境变量
        env = {"start_index": 0, "end_index": 2, "step_index": 1}

        # 执行方法
        def run(self, index):
            print(f"DemoFunc env:{self.env} index:{index}, thread_id:{threading.get_ident()}, process_id:{os.getpid()}")

        # 参数生成器，如不重写，返回值将作为run参数调用
        def param(self):
            return range(self.env.get("start_index"), self.env.get("end_index"), self.env.get("step_index"))

----------
后面的详见 ant_task.tests.test_task
----------

