from ant_task import AntTaskException, Register, Status


class Dag(object):
    dag_name: str
    channel_name: str
    task_flower = []
    status: Status

    def __init__(self, dag_config, register: Register):
        self._register = register
        if isinstance(dag_config, dict):
            self.load_py(py_dict=dag_config)

    def load_json(self, json_path):
        pass

    def load_py(self, py_dict: dict):
        if "channel_name" not in py_dict.keys() or py_dict.get("channel_name", "") == "":
            raise AntTaskException(level=6, dialect_msg=f"渠道名称`channel_name`不得为空")
        self.channel_name = py_dict.get("channel_name")
        if "dag_name" not in py_dict.keys() or py_dict.get("dag_name", "") == "":
            raise AntTaskException(level=6, dialect_msg=f"渠道名称`dag_name`不得为空")
        self.dag_name = py_dict.get("dag_name")
        if "status" not in py_dict.keys() or py_dict.get("status", "") == "":
            self.status = Status()
        else:
            if py_dict.get("status") not in self._register.status_node:
                raise AntTaskException(level=6, dialect_msg=f"状态控制器`{py_dict.get('status')}`未注册")
            else:
                self.status = self._register.status_node.get(py_dict.get("status"))
        if "task" not in py_dict.keys() or py_dict.get("task", "") == "":
            raise AntTaskException(level=6, dialect_msg=f"任务配置不能为空")
        if not isinstance(py_dict.get("task"), list):
            raise AntTaskException(level=6, dialect_msg=f"任务节点需为list")
        elif isinstance(py_dict.get("task"), list):
            for task_list in py_dict.get("task"):
                if isinstance(task_list, str):
                    if task_list not in self._register.task_node.keys():
                        raise AntTaskException(level=6, dialect_msg=f"任务`{task_list}`未注册")
                    self.task_flower.append([self._register.task_node.get(task_list)])
                elif isinstance(task_list, list):
                    sub_task_list = []
                    for t in task_list:
                        if not isinstance(t, str):
                            raise AntTaskException(level=6, dialect_msg=f"{t}类型仅能为`str`, 输入类型为:{type(t)}")
                        if t not in self._register.task_node.keys():
                            raise AntTaskException(level=6, dialect_msg=f"任务`{t}`未注册")
                        sub_task_list.append(self._register.task_node.get(t))
                    self.task_flower.append(sub_task_list)
