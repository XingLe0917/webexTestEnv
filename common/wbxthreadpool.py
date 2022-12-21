from concurrent.futures import ThreadPoolExecutor
import logging

class wbxthreadpool(ThreadPoolExecutor):

    def __init__(self, max_workers = None):
        ThreadPoolExecutor.__init__(self, max_workers)


autotaskpool = wbxthreadpool(5)

