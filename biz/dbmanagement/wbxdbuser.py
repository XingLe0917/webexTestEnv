class wbxdbuser:
    def __init__(self, db_name, username):
        self._db_name = db_name
        self._username = username
        self._appln_support_code = None
        self._password = None
        self._schema_type = None

    def setApplnSupportCode(self, applnsupportcode):
        self._appln_support_code = applnsupportcode

    def getApplnSupportCode(self):
        return self._appln_support_code

    def setPassword(self, password):
        self._password = password

    def getPassword(self):
        return self._password

    def setSchemaType(self, schema_type):
        self._schema_type = schema_type

    def getSchemaType(self):
        return self._schema_type

    def getUserName(self):
        return self._username
