from ant_task import Dag, TaskManger
from ant_task.examples.demo import register


def run():
    # 代码配置模式
    # dag = Dag({
    #     "dag_name": "测试dag",
    #     "channel_name": "测试渠道",
    #     "status": "log_db",
    #     "task": ["test_01", ["test_03", "test_02"]]
    # }, register)

    # 配置文件加载模式
    dag = Dag('./ant_task/examples/dag_local.json', register)
    with TaskManger(log_file='./log', log_stream=True) as tm:
        tm.dag_runner(dag)


if __name__ == '__main__':
    run()
