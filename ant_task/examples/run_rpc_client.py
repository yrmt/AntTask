from ant_task import Dag, TaskManger
from ant_task.examples.demo import register


# def run_only_rpc():
#     from ant_task.etcd2grpc.client import run_rpc
#     response = run_rpc(
#         "default_channel", "default_dag", "default_task",
#         [{"code": "00001", "dd": "asdf"}, {"code": "00001", "dd": "asdf"}]
#     )
#     print(f"返回值:{response}")


def run_dag_rpc():
    dag = Dag('./ant_task/examples/dag_rpc.json', register)
    with TaskManger(log_file='./log', log_stream=True) as tm:
        tm.dag_runner(dag)


if __name__ == '__main__':
    run_dag_rpc()
