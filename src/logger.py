import os, sys, pathlib, logging, logging.config, time, datetime, socket
from logging.handlers import RotatingFileHandler


project_folder_path = str(pathlib.Path(__file__).resolve().parents[1])
sys.path.append(project_folder_path)


log_format = "%(asctime)-15s %(levelname)-5s [%(filename)s:%(lineno)d] %(message)s"
log_level = "DEBUG"

logger_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {
            "format": log_format
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": log_level,
            "formatter": "simple",
            "stream": "ext://sys.stdout"
        }
    },

    "root": {
        "level": log_level,
        "handlers": ["console"]
    }

}

def init_file_handler(logger,log_file_path,formatter):
    try:
        file_handler = RotatingFileHandler(
            log_file_path,
            maxBytes=2e+7,
            backupCount=20
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except BaseException as e:
        print(f"error - logger - {datetime.datetime.now().isoformat()} cannot add file handler: {str(e)}")
    return logger


def getLogger(logger_name, log_file_name='common.log'):
    logger = logging.getLogger(logger_name)
    formatter = logging.Formatter(log_format)

    logging.config.dictConfig(logger_config)
    log_folder_path = os.path.join(project_folder_path,'logs')
    log_file_path = os.path.join(log_folder_path,log_file_name)
    os.makedirs(log_folder_path,exist_ok=True)

    # Rotating File handler
    logger = init_file_handler(logger,log_file_path,formatter)

    return logger

# --- Tqdm Logger Redirect ---
class TqdmToLogger:
    def __init__(self, logger, level=logging.INFO):
        self.logger = logger
        self.level = level
        self.buf = ''

    def write(self, message):
        message = message.strip()
        if message:
            self.logger.log(self.level, message)

    def flush(self):
        pass

def test_tqdmtologger():
    import tqdm
    logger = getLogger("test")
    tqdm_logger = TqdmToLogger(logger)

    for i in tqdm(range(20), file=tqdm_logger, desc="Processing"):
        time.sleep(0.1)