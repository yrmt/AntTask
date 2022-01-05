import datetime
import logging

from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, Session

from ant_task import AntTaskException


class Status(object):
    # 状态三种类型 在 dag_start，task_start中返回，如不返回则按照`RUN_NORMAL`模式执行
    # RUN_RETRY = 2
    RUN_NORMAL = 0
    RUN_SKIP = 1
    status_name: str = "default"

    dag_name: str
    channel_name: str
    log: logging.Logger
    run_type: str

    def set_some(self, dag_name, channel_name, log, run_type):
        """ dag启动前 会自动调用该方法设置dag_name 和channel_name
        :param dag_name:
        :param channel_name:
        :param log:
        :param run_type:
        :return:
        """
        self.log = log
        self.dag_name = dag_name
        self.channel_name = channel_name
        self.run_type = run_type

    def dag_init(self, dag):
        """
            初始化任务图
        :return:
        """
        pass

    def dag_start(self):
        """
            任务图执行开始
        :return:
        """
        pass

    def dag_end(self):
        """
            任务图执行结束
        :return:
        """
        pass

    def task_start(self, task_name):
        """
            任务开始
        :param task_name:
        :return:
        """
        pass

    def task_end(self, task_name, err):
        """
            任务结束
        :param task_name:
        :param err: 异常
        :return:
        """
        pass


Base = declarative_base()


class TaskModule(Base):
    __tablename__ = "ant_task"
    __table_args__ = {'sqlite_autoincrement': True}
    __doc__ = "任务db"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dag_name = Column(String(64), comment="批量名称")
    task_name = Column(String(64), comment="任务名")
    channel_name = Column(String(64), comment="渠道名")
    sort_no = Column(Integer, comment="排序")
    batch_key = Column(String(8), comment="批量唯一表示")

    status = Column(String(8), comment="状态 init|run|success|error")
    start_date = Column(DateTime, comment="开始时间")
    end_date = Column(DateTime, comment="结束时间")
    result_msg = Column(String(255), comment="结果")


class DBStatus(Status):
    status_name = "default_db"
    batch_key: str

    def __init__(self, session: Session, batch_key):
        self.session = session
        self.batch_key = batch_key

    def dag_init(self, dag):
        result = self.session.query(TaskModule).filter(
            TaskModule.dag_name == self.dag_name,
            TaskModule.channel_name == self.channel_name,
            TaskModule.batch_key == self.batch_key
        ).all()
        if len(result) == 0:
            self.log.debug(f"任务流初始化")
            tm_list = []
            for index, task_list in enumerate(dag.task_flower):
                sort_no = index + 1
                for task in task_list:
                    tm = TaskModule()
                    tm.dag_name = self.dag_name
                    tm.channel_name = self.channel_name
                    tm.task_name = task.task_name
                    tm.sort_no = sort_no
                    tm.batch_key = self.batch_key
                    tm.status = "init"
                    tm_list.append(tm)
            self.session.add_all(tm_list)
            self.session.commit()
        else:
            self.log.debug(f"任务流已初始化")
            for index, task_list in enumerate(dag.task_flower):
                sort_no = index + 1
                sort_no_result = set([t.task_name for t in filter(lambda x: x.sort_no == sort_no, result)])
                sort_no_result_local = set([t.task_name for t in task_list])
                diff_result = sort_no_result_local ^ sort_no_result
                if len(diff_result) != 0:
                    raise AntTaskException(level=6, dialect_msg=f"本地任务与数据库任务不匹配:{diff_result}")

    def dag_end(self):
        self.log.info(f"任务流执行成功")

    def task_start(self, task_name):
        cur_status = self.session.query(TaskModule).filter(
            TaskModule.dag_name == self.dag_name,
            TaskModule.channel_name == self.channel_name,
            TaskModule.batch_key == self.batch_key,
            TaskModule.task_name == task_name
        ).first()
        if not cur_status:
            raise AntTaskException(level=6, dialect_msg=f"任务实例`{task_name}`启动更新失败: db无查询结果")
        if cur_status.status in ("init", "error"):
            cur_status.start_date = datetime.datetime.now()
            cur_status.status = "run"
            cur_status.result_msg = None
            self.session.commit()
            self.log.debug(f"任务{task_name} 开始以{self.run_type}方式执行")
            return self.RUN_NORMAL
        elif cur_status.status == "run":
            raise AntTaskException(level=3, dialect_msg=f"任务实例`{task_name}`已在执行中，dag将终止")
        elif cur_status.status == "success":
            self.log.debug(f"任务{task_name} 成功跳过执行")
            return self.RUN_SKIP
        else:
            raise AntTaskException(level=6, dialect_msg=f"任务实例`{task_name}`启动失败: db状态未知`{cur_status.status}`")

    def task_end(self, task_name, err):
        cur_status = self.session.query(TaskModule).filter(
            TaskModule.dag_name == self.dag_name,
            TaskModule.channel_name == self.channel_name,
            TaskModule.batch_key == self.batch_key,
            TaskModule.task_name == task_name
        ).first()
        if not cur_status:
            raise AntTaskException(level=6, dialect_msg=f"任务实例`{task_name}`结束更新失败: db无查询结果")
        if cur_status.status != "run":
            raise AntTaskException(level=6, dialect_msg=f"任务实例`{task_name}`结束更新失败，db状态`{cur_status.status}` != `run`")
        if err:
            cur_status.status = 'error'
            if isinstance(err, AntTaskException):
                if err.level in ("notice", "none"):
                    cur_status.status = 'success'
                cur_status.result_msg = str(err.dialect_msg)[:200]
                self.log.error(f"任务{task_name} 执行失败 失败原因:{err.dialect_msg}")
            else:
                self.log.error(f"任务{task_name} 执行失败 未归类的异常")
                self.log.exception(err)
                cur_status.result_msg = str(err)[:200]
        else:
            cur_status.status = 'success'
        cur_status.end_date = datetime.datetime.now()
        self.session.commit()
