import datetime
import threading
import time
import os

from ant_task import Dag, AntTaskException, TaskManger, Task, Register


class DemoFunc(Task):
    task_name = "test_01"
    run_type = Task.RUN_TYPE_THREAD
    threading_pool_size = 5
    env = {"start_index": 0, "end_index": 2, "step_index": 1}

    def run(self, index):
        time.sleep(1)
        print(f"DemoFunc env:{self.env} index:{index}, thread_id:{threading.get_ident()}, process_id:{os.getpid()}")

    def param(self):
        return range(self.env.get("start_index"), self.env.get("end_index"), self.env.get("step_index"))


class DemoFunc2(Task):
    task_name = "test_02"
    run_type = Task.RUN_TYPE_SINGLE
    env = {"sftp_path": 10}

    def run(self):
        time.sleep(1)
        print(f"DemoFunc2 env:{self.env}, thread_id:{threading.get_ident()}, process_id:{os.getpid()}")
        raise AntTaskException(level=6, dialect_msg="xixi")


class DemoFunc3(Task):
    task_name = "test_03"
    run_type = Task.RUN_TYPE_SINGLE
    env = {"http_path": 0}

    def run(self):
        time.sleep(1)
        print(f"DemoFunc3 env:{self.env}, thread_id:{threading.get_ident()}, process_id:{os.getpid()}")


def register_task(register):
    register.task([DemoFunc, DemoFunc2, DemoFunc3])


def register_status(register):
    from ant_task import DBStatus, LogStatus, LogDbStatus
    from ant_task.examples.db import Session, engine
    from ant_task.status import Base, TaskModule
    Base.metadata.create_all(engine, tables=[m.__table__ for m in [TaskModule]])
    now_day = datetime.datetime.now().strftime("%Y%m%d")
    session = Session()
    log_db_status = LogDbStatus(session, now_day, log_path="./log", stream=True)
    log_status = LogStatus(now_day, log_path="./log", stream=True)
    db_status = DBStatus(session, batch_key=now_day)
    register.status([log_db_status, log_status, db_status])


if __name__ == '__main__':
    register = Register()
    register_task(register)
    register_status(register)
    dag = Dag({
        "dag_name": "测试dag",
        "channel_name": "测试渠道",
        "status": "log_db",
        "task": ["test_01", ["test_03", "test_02"]]
    }, register)
    with TaskManger() as tm:
        tm.dag_runner(dag)
