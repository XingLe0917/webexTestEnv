class EnvConfig(object):
    def __getitem__(self, key):
        return self.__getattribute__(key)

class ProductionConfig(EnvConfig):
    client_id = "Cc5992db82e1f5600fb221f0ff9e4ef94d5a5038ca54ac73d96ab181a9d66b42c"
    authorization = "Basic Q2M1OTkyZGI4MmUxZjU2MDBmYjIyMWYwZmY5ZTRlZjk0ZDVhNTAzOGNhNTRhYzczZDk2YWIxODFhOWQ2NmI0MmM6OTc2MTEzNzUyNTA3MTQzMjEwYjRiYWZiNGU2YzBhMjU4MTMxNmFhNGIyMjYzMTg1NjlmNTY4NzA2NTdmMjk0Ng=="
    token_info_authorization="Basic UmNlN2MzMzRmODk0NjZhYzA3OTUyYmRmYTU1NjU5ZGU5ZDdiMjAzYmNiYjU0YWE0ZjM0NGNkZWJmZDAxYmJkNjI6OWRkNTQ0MzgzY2JjZjBhOGNkNmRhNzc5MjA4MjAxODVlNzY2NmVlNmM2ZTYwZDFiMGQ5Yjg2NzI5ZmU4NTU5ZA=="
    redirect_uri = "http%3A%2F%2Fsjgrcabt102.webex.com%3A9000%2FloginRedirection"
    PCCP_ERROR_URL = "https://pccp.webex.com/#/401"
    PCCP_INDEX_URL = "https://pccp.webex.com/#/home/index"

class DevelopmentConfig(EnvConfig):
    client_id = "C2940cd3dd7f2b74dc4f5fec11e7734263a38f03c4dae9be060704e09ae19c79c"
    authorization = "Basic QzI5NDBjZDNkZDdmMmI3NGRjNGY1ZmVjMTFlNzczNDI2M2EzOGYwM2M0ZGFlOWJlMDYwNzA0ZTA5YWUxOWM3OWM6YTc1M2U2YTEwMDI4MjMyYTRiM2VjODQ4Yjc4NjQxMmM0MWZhN2I3ODU4ZTFhMDY4Yjc1YWYxMWZmNTRiYjEwMg=="
    redirect_uri = "http%3A%2F%2Ftagrcabt101.webex.com%3A9000%2FloginRedirection"
    PCCP_ERROR_URL = "https://tagrcabt101.webex.com/#/401"
    PCCP_INDEX_URL = "https://tagrcabt101.webex.com/#/home/index"

class DevelopmentQAConfig(EnvConfig):
    client_id = "C3694c73fb2f80c1eb3ef49c4d662a898b000fcb547b5b4b4e66aa3b49dc58b05"
    authorization = "Basic QzM2OTRjNzNmYjJmODBjMWViM2VmNDljNGQ2NjJhODk4YjAwMGZjYjU0N2I1YjRiNGU2NmFhM2I0OWRjNThiMDU6NDkzNDI4ZTBlY2E5ZWViMzUyZTFkN2EwYTQ1ZmVjM2Q1NWFhMDBiNzA3ODAzNzIxMWE0Y2JmYWQzNzk1ODJmYg=="
    redirect_uri = "http%3A%2F%2Fpccp01.webex.com%3A9000%2FloginRedirection"
    PCCP_ERROR_URL = "https://pccp01.webex.com/#/401"
    PCCP_INDEX_URL = "https://pccp01.webex.com/#/home/index"

class DevelopmentChinaConfig(EnvConfig):
    client_id = "Cb20cdbed61b95725a0fef2f7c9e9066cc7efb0a3aaf7a1697e352f056bb93e0d"
    authorization = "Basic Q2IyMGNkYmVkNjFiOTU3MjVhMGZlZjJmN2M5ZTkwNjZjYzdlZmIwYTNhYWY3YTE2OTdlMzUyZjA1NmJiOTNlMGQ6ZTllNWU5ZTE2YWI4ZDFkOGFjZTIxMGFhMzlhYTkxYTdlOWQ3NWYwZDMyYjc4ZDY1NzA3Mzg2Mzc4OWI5NTFjOQ=="
    redirect_uri = "http%3A%2F%2Fpccp.webex.com.cn%3A9000%2FloginRedirection"
    PCCP_ERROR_URL = "https://pccp.webex.com.cn/#/401"
    PCCP_INDEX_URL = "https://pccp.webex.com.cn/#/home/index"

mapping = {
    'dev': DevelopmentConfig,
    'prod': ProductionConfig,
    'qa': DevelopmentQAConfig,
    'china': DevelopmentChinaConfig,
    'default': ProductionConfig
}

