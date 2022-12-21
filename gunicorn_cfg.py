# import gevent.monkey
import multiprocessing
#
# gevent.monkey.patch_all()

loglevel = 'debug'
bind = "0.0.0.0:9000"
pidfile = "logs/gunicorn.pid"
accesslog = "logs/access.log"
errorlog = "logs/access.log"
daemon = 'true'
# workers = multiprocessing.cpu_count() * 2 + 1
workers = 3

#
# logconfig_dict = {
#     'version':1,
#     'disable_existing_loggers': False,
#     #在最新版本必须添加root配置，否则抛出Error: Unable to configure root logger
#     "root": {
#           "level": "DEBUG",
#           "handlers": ["console"] # 对应handlers字典的键（key）
#     },
#     'loggers':{
#         "gunicorn.error": {
#             "level": "DEBUG",# 打日志的等级；
#             "handlers": ["error_file"], # 对应handlers字典的键（key）；
#             #是否将日志打印到控制台（console），若为True（或1），将打印在supervisor日志监控文件logfile上，对于测试非常好用；
#             "propagate": 0,
#             "qualname": "gunicorn_error"
#         },
#
#         "gunicorn.access": {
#             "level": "DEBUG",
#             "handlers": ["access_file"],
#             "propagate": 0,
#             "qualname": "access"
#         }
#     },
#     'handlers':{
#         "error_file": {
#             "class": "logging.handlers.RotatingFileHandler",
#             "maxBytes": 1024*1024*100,# 打日志的大小（此处限制100mb）
#             "backupCount": 1,# 备份数量（若需限制日志大小，必须存在值，且为最小正整数）
#             "formatter": "generic",# 对应formatters字典的键（key）
#             "filename": "/Users/lexing/Documents/logs/pccp/error.log" #若对配置无特别需求，仅需修改此路径
#         },
#         "access_file": {
#             "class": "logging.handlers.RotatingFileHandler",
#             "maxBytes": 1024*1024*100,
#             "backupCount": 1,
#             "formatter": "generic",
#             "filename": "/Users/lexing/Documents/logs/pccp/access.log", #若对配置无特别需求，仅需修改此路径
#         },
#         'console': {
#             'class': 'logging.StreamHandler',
#             'level': 'DEBUG',
#             'formatter': 'generic',
#         },
#
#     },
#     'formatters':{
#         "generic": {
#             "format": "%(asctime)s [%(process)d]: [%(levelname)s] %(message)s", # 打日志的格式
#             "datefmt": "[%Y-%m-%d %H:%M:%S %z]",# 时间显示格式
#             "class": "logging.Formatter"
#         }
#     }
# }