import logging

class ColoredTerminalLogger(logging.Logger):
    """Logger that extends the basic logger's features to include ANSI color escape codes."""
    FAIL = "\033[38;5;9m"
    INFO = "\033[38;5;244m"
    WARN = "\033[38;5;214m"
    RESET = "\033[0m"
    DEBUG = "\033[38;5;27m"
    BOLD = "\033[1m"
    UNBOLD = "\033[21m"
    ULINE = "\033[4m"
    UNLINE = "\033[24m"

    @staticmethod
    def test():
        print(ColoredTerminalLogger.FAIL + "FAILURE!!!" + ColoredTerminalLogger.RESET)
        print(ColoredTerminalLogger.WARN + "WARNING!" + ColoredTerminalLogger.RESET)
        print(ColoredTerminalLogger.INFO + "Things you might need to know." + ColoredTerminalLogger.RESET)
        print(ColoredTerminalLogger.RESET + "Everything is fine." + ColoredTerminalLogger.RESET)
        print(ColoredTerminalLogger.DEBUG + "Anything and everything, here." + ColoredTerminalLogger.RESET)
        print(
            ColoredTerminalLogger.BOLD + "REALLY IMPORTANT." + ColoredTerminalLogger.UNBOLD + " Or not." + ColoredTerminalLogger.RESET)
        print(
            ColoredTerminalLogger.ULINE + "Read this." + ColoredTerminalLogger.UNLINE + " Or don't." + ColoredTerminalLogger.RESET)

    def info(self, msg: str, *args, **kwargs):
        if (self.getEffectiveLevel() <= logging.INFO):
            logging.Logger.info(self, ColoredTerminalLogger.INFO + msg + ColoredTerminalLogger.RESET, *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs):
        if (self.getEffectiveLevel() <= logging.CRITICAL):
            logging.Logger.critical(self, ColoredTerminalLogger.FAIL + msg + ColoredTerminalLogger.RESET, *args,
                                    **kwargs)

    def debug(self, msg: str, *args, **kwargs):
        if (self.getEffectiveLevel() <= logging.DEBUG):
            logging.Logger.debug(self, ColoredTerminalLogger.DEBUG + msg + ColoredTerminalLogger.RESET, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> str:
        if (self.getEffectiveLevel() <= logging.WARNING):
            logging.Logger.warning(self, ColoredTerminalLogger.WARN + msg + ColoredTerminalLogger.RESET, *args,
                                   **kwargs)

    def error(self, msg: str, *args, **kwargs) -> str:
        if (self.getEffectiveLevel() <= logging.ERROR):
            logging.Logger.error(self, ColoredTerminalLogger.FAIL + msg + ColoredTerminalLogger.RESET, *args, **kwargs)
