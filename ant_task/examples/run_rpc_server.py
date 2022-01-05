import asyncio

from ant_task.etcd2grpc.server import run_rpc_server, parse_args


def main():
    from ant_task.examples.demo import register
    asyncio.run(run_rpc_server(register.task_node, "./log", **parse_args()))


if __name__ == '__main__':
    main()
