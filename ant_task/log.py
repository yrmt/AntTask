import logging
import os


class CustomAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        add_kw = "".join([f"[{s}]" for s in self.extra.values()])
        return f"{add_kw} {msg}", kwargs


class MainLog(object):
    __instance = {}
    formatter = logging.Formatter(
        '[%(levelname)s][%(asctime)s][%(filename)s:%(lineno)s]%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    def __new__(cls, *args, **kwargs):
        if f"{args[0]}" not in MainLog.__instance.keys():
            MainLog.__instance[f"{args[0]}"] = object.__new__(cls)
        return MainLog.__instance[f"{args[0]}"]

    def __init__(self, project_name):
        self.root_logger = logging.getLogger(f"{project_name}")
        self.root_logger.propagate = False
        self.root_logger.setLevel(logging.DEBUG)

    def add_file_handler(self, log_path, log_level=logging.DEBUG):
        """
        :return:
        """
        if len(list(filter(
                lambda x: x.__class__ == logging.FileHandler and x.baseFilename == log_path,
                self.root_logger.handlers
        ))) != 0:
            return
        if not os.path.exists(os.path.dirname(log_path)):
            os.makedirs(os.path.dirname(log_path))
        fh = logging.FileHandler(filename=log_path)
        fh.setFormatter(MainLog.formatter)
        fh.setLevel(log_level)
        self.root_logger.addHandler(fh)

    def add_stream_handler(self, log_level=logging.DEBUG):
        if len(list(filter(lambda x: x.__class__ == logging.StreamHandler, self.root_logger.handlers))) != 0:
            return
        fb = logging.StreamHandler()
        fb.setFormatter(MainLog.formatter)
        fb.setLevel(log_level)
        self.root_logger.addHandler(fb)

    def get_log(self, group_dict: dict = None):
        return CustomAdapter(self.root_logger, group_dict if group_dict else {})


if __name__ == '__main__':
    import uuid

    mlog = MainLog('AntTask')
    mlog.add_file_handler("./log/test.log")
    mlog.add_stream_handler()
    log = mlog.get_log(group_dict={"task_id": str(uuid.uuid4())})
    log.debug("asdf1a")
    log.info("asdfa")
    log.warning("asdfa")
    log.error("asdfa")
    log = mlog.get_log()
    log.debug("asdf1a")
    log.info("asdfa")
    log.warning("asdfa")
    log.error("asdfa")
