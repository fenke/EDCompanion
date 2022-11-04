#pylint: disable=missing-module-docstring
#pylint: disable=missing-function-docstring
#pylint: disable=invalid-name

import logging
from logging import handlers
import queue
import os
import datetime


def init_console_logging(name=None):
    '''Setup none-blocking stream handler for sending loggin to the console.'''
    # Only if no handlers defined.
    if True or not logging.getLogger(name).handlers:
        print('Setting up non-blocking console logger')

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        q = queue.SimpleQueue()
        queue_handler = logging.handlers.QueueHandler(q)

        #handler = logging.FileHandler(os.path.join('logs',datetime.datetime.now().isoformat().replace(":", "_").split(".")[0] + '.log'))
        handler = logging.handlers.RotatingFileHandler(os.path.join('logs',f"{__name__}-{datetime.date.today().isoformat()}.log"), maxBytes=32e3, backupCount=5)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)s\t%(name)s\t%(lineno)d\t%(message)s", datefmt='%Y-%m-%dT%H:%M:%S')
        #formatter = logging.Formatter(json.dumps(basic_dict), datefmt='%Y-%m-%dT%H:%M:%S%z')
        handler.setFormatter(formatter)

        listener = logging.handlers.QueueListener(q, handler)
        print(f'Add handler {str(queue_handler)}')
        logger.addHandler(queue_handler)
        listener.start()
        queue_handler.setLevel(logging.INFO)

        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        # set a format which is simpler for console use
        formatter = logging.Formatter("%(asctime)s %(levelname)s\t%(name)s\t%(filename)s\t%(lineno)d\t%(message)s", datefmt='%Y-%m-%dT%H:%M:%S%z')
        # tell the handler to use this format
        console.setFormatter(formatter)
        # add the handler to the root logger
        print(f'Add handler {str(console)}')
        logger.addHandler(console)
        return logger
    else:
        print('There already is a logger installed')
        #listloggers(logging.getLogger(name))

