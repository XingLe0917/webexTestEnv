from dao.wbxdaomanager import wbxdao
from dao.vo.configdbvo import WebDomainVO, WebDomainConfigVO

class ConfigDBDao(wbxdao):

    def getWebDomainList(self):
        session = self.getLocalSession()
        domainList = session.query(WebDomainVO).filter(WebDomainVO.active==1).all()
        return domainList

    def assignPasscode(self, domainname):
        session = self.getLocalSession()
        SQL='''
DECLARE
    vCnt                  NUMBER(10) := 0;
    pPoolName             WBXPCNPASSCODERANGE.POOLNAME%TYPE := TRIM('{}');
    pTargetRangeStart     WBXPCNPASSCODERANGE.RANGESTART%TYPE := NULL;
    pTargetRangeEnd       WBXPCNPASSCODERANGE.RANGEEND%TYPE := NULL;
BEGIN
    SELECT count(1) INTO vCnt FROM WBXPCNPASSCODERANGE WHERE poolname=pPoolName;
    IF vCnt = 0 THEN
        AssignPasscodeRange(pPoolName, pTargetRangeStart, pTargetRangeEnd);
        UPDATE WBXPCNPASSCODERANGE SET LASTMODIFIEDTIME=LASTMODIFIEDTIME WHERE STATUS='InUse' AND POOLNAME=pPoolName;
        IF SQL%ROWCOUNT = 1 THEN
            COMMIT;
        ELSE
            ROLLBACK;
            RAISE_APPLICATION_ERROR(-20001, 'UpdateLastModifiedTimeFail');
        END IF;
    END IF;
END;
        '''.format(domainname)
        session.execute(SQL)