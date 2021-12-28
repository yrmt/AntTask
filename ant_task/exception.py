ERROR_LEVEL_DICT = {6: "fatal", 5: "critical", 4: "error", 3: "warning", 2: "notice", 1: "none"}


class AntTaskException(Exception):
    def __init__(
            self, level=6, retry: bool = False,
            dialect_msg: str = None, log_msg: str = None,
            attach_data=None
    ):
        """
        :param level:
              - 6: fatal 致命异常
              - 5: critical 关键
              - 4: error 错误
              - 3: warning 警告
              - 2: notice 通知
              - 1: none 无
        :param retry:
        :param dialect_msg:
        :param log_msg:
        :param attach_data:
        """
        self.level = level if isinstance(level, str) else ERROR_LEVEL_DICT.get(level, 6)
        self.retry = retry
        self.dialect_msg = dialect_msg
        self.log_msg = log_msg if log_msg else dialect_msg
        self.attach_data = attach_data
