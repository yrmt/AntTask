import asyncio

from ant_task.etcd2grpc.server import run_rpc_server, parse_args


def main():
    asyncio.run(run_rpc_server(**parse_args()))


if __name__ == '__main__':
    main()
