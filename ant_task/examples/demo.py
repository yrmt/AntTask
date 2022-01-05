import datetime
import threading
import time
import os

from ant_task import DBStatus
from ant_task.examples.db import Session, engine
from ant_task.status import Base, TaskModule
from ant_task.exception import AntTaskException
from ant_task.task import Task
from ant_task.register import Register


class DemoFunc(Task):
    task_name = "test_01"
    threading_pool_size = 5
    env = {"start_index": 0, "end_index": 2, "step_index": 1}

    def run(self, index):
        log = self.get_log()
        time.sleep(1)
        log.info(f"DemoFunc env:{self.env} index:{index}, thread_id:{threading.get_ident()}, process_id:{os.getpid()}")

    def param(self):
        return range(self.env.get("start_index"), self.env.get("end_index"), self.env.get("step_index"))


class DemoFunc2(Task):
    task_name = "test_02"
    env = {"sftp_path": 10}

    def run(self):
        time.sleep(1)
        print(f"DemoFunc2 env:{self.env}, thread_id:{threading.get_ident()}, process_id:{os.getpid()}")
        raise AntTaskException(level=6, dialect_msg="xixi")


class DemoFunc3(Task):
    task_name = "test_03"
    env = {"http_path": 0}

    def run(self):
        time.sleep(1)
        print(f"DemoFunc3 env:{self.env}, thread_id:{threading.get_ident()}, process_id:{os.getpid()}")


register = Register()
register.task([DemoFunc, DemoFunc2, DemoFunc3])  # 注册任务

Base.metadata.create_all(engine, tables=[m.__table__ for m in [TaskModule]])
now_day = datetime.datetime.now().strftime("%Y%m%d")
session = Session()
# log_db_status = LogDbStatus(session, now_day, log_path="./log", stream=True)
# log_status = LogStatus(now_day, log_path="./log", stream=True)
db_status = DBStatus(session, batch_key=now_day)
register.status([db_status])  # 注册状态控制器
