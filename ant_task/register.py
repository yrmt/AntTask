from ant_task.task import Task
from ant_task.exception import AntTaskException
from ant_task.status import Status


class Register(object):
    task_node = {}
    check_node = {}
    status_node = {}

    def _task_one(self, task_class):
        if not issubclass(task_class, Task):
            raise AntTaskException(level=6, dialect_msg=f"任务节点注册失败,类型不符.")
        if task_class.task_name in self.task_node.keys():
            raise AntTaskException(level=6, dialect_msg=f"重复任务添加:{task_class.task_name}")
        if task_class.task_name is None:
            raise AntTaskException(level=6, dialect_msg=f"任务名称不得为空:{task_class}")
        self.task_node[task_class.task_name] = task_class

    def _status_one(self, status_obj):
        if not isinstance(status_obj, Status):
            raise AntTaskException(level=6, dialect_msg=f"状态节点注册失败,类型不符.")
        if status_obj.status_name in self.status_node.keys():
            raise AntTaskException(level=6, dialect_msg=f"重复状态添加:{status_obj.status_name}")
        if status_obj.status_name is None:
            raise AntTaskException(level=6, dialect_msg=f"状态名称不得为空:{status_obj}")
        self.status_node[status_obj.status_name] = status_obj

    def task(self, task_class_list):
        if isinstance(task_class_list, list):
            for task_class in task_class_list:
                self._task_one(task_class)
        else:
            self._task_one(task_class_list)

    def status(self, status_obj_list):
        if isinstance(status_obj_list, list):
            for status_obj in status_obj_list:
                self._status_one(status_obj)
        else:
            self._status_one(status_obj_list)
        if "default" not in self.status_node.keys():
            self.status_node["default"] = Status()
