import datetime
import os

from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, Session

from ant_task import AntTaskException, MainLog
import logging


class Status(object):
    # 状态三种类型 在 dag_start，task_start中返回，如不返回则按照`RUN_NORMAL`模式执行
    # RUN_RETRY = 2
    RUN_NORMAL = 0
    RUN_SKIP = 1
    status_name: str = "default"

    dag_name: str
    channel_name: str

    def set_name(self, dag_name, channel_name):
        """ dag启动前 会自动调用该方法设置dag_name 和channel_name
        :param dag_name:
        :param channel_name:
        :return:
        """
        self.dag_name = dag_name
        self.channel_name = channel_name

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
    status_name = "db"
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
            for index, task_list in enumerate(dag.task_flower):
                sort_no = index + 1
                sort_no_result = set([t.task_name for t in filter(lambda x: x.sort_no == sort_no, result)])
                sort_no_result_local = set([t.task_name for t in task_list])
                diff_result = sort_no_result_local ^ sort_no_result
                if len(diff_result) != 0:
                    raise AntTaskException(level=6, dialect_msg=f"本地任务与数据库任务不匹配:{diff_result}")

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
            return self.RUN_NORMAL
        elif cur_status.status == "run":
            raise AntTaskException(level=3, dialect_msg=f"任务实例`{task_name}`已在执行中，dag将终止")
        elif cur_status.status == "success":
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
            cur_status.end_date = datetime.datetime.now()
            if isinstance(err, AntTaskException):
                cur_status.result_msg = str(err.dialect_msg)[:255]
            else:
                cur_status.result_msg = str(err)[:255]
        else:
            cur_status.status = 'success'
        self.session.commit()


class LogStatus(Status):
    status_name = "log"
    _log = None

    def __init__(self, batch_key, project_name="AntTask", log_path=None, stream=False, log_level=logging.DEBUG):
        self.batch_key = batch_key
        self._mlog = MainLog(project_name)
        self.log_path = log_path
        self.stream = stream
        self.log_level = log_level

    def set_name(self, dag_name, channel_name):
        super(LogStatus, self).set_name(dag_name, channel_name)
        if self.log_path:
            self._mlog.add_file_handler(
                os.path.join(self.log_path, f"{dag_name}_{channel_name}_{self.batch_key}.log"),
                log_level=self.log_level
            )
        if self.stream:
            self._mlog.add_stream_handler(log_level=self.log_level)

    @property
    def log(self):
        if not self._log:
            self._log = self._mlog.get_log()
        return self._log

    def dag_init(self, dag):
        self.log.info(f"dag_init")

    def dag_start(self):
        self.log.info(f"dag_start")

    def dag_end(self):
        self.log.info(f"dag_end")

    def task_start(self, task_name):
        self.log.info(f"{task_name} task_start")

    def task_end(self, task_name, err):
        self.log.info(f"{task_name} task_end")
        if err:
            if isinstance(err, AntTaskException):
                self.log.error(str(err.log_msg))
            else:
                self.log.error(err)


class LogDbStatus(LogStatus, DBStatus):
    status_name = "log_db"

    def __init__(self, session: Session, batch_key, project_name="AntTask", log_path=None, stream=False,
                 log_level=logging.DEBUG):
        super(LogDbStatus, self).__init__(batch_key, project_name, log_path, stream, log_level)
        super(LogStatus, self).__init__(session, batch_key)

    def dag_init(self, dag):
        super(LogDbStatus, self).dag_init(dag)
        return super(LogStatus, self).dag_init(dag)

    def dag_start(self):
        db_result = super(LogStatus, self).dag_start()
        if db_result in (self.RUN_NORMAL, None):
            super(LogDbStatus, self).dag_start()
        return db_result

    def dag_end(self):
        super(LogDbStatus, self).dag_end()
        return super(LogStatus, self).dag_end()

    def task_start(self, task_name):
        db_result = super(LogStatus, self).task_start(task_name)
        if db_result in (self.RUN_NORMAL, None):
            super(LogDbStatus, self).task_start(task_name)
        return db_result

    def task_end(self, task_name, err):
        super(LogDbStatus, self).task_end(task_name, err)
        return super(LogStatus, self).task_end(task_name, err)
