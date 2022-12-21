from sqlalchemy import Column,Integer,String, DateTime, func, text, select
from sqlalchemy.orm import column_property
from dao.vo.wbxvo import Base
from common.wbxutil import wbxutil

# class wbxpgdatabase(Base):
#     __tablename__ = "database_info"
#     db_name = Column(String(64), primary_key=True)
#     host_name = Column(String(64))
#     db_vendor = Column(String(64))
#     db_version = Column(String(64))
#     db_type = Column(String(64))
#     application_type = Column(String(64))
#     appln_support_code = Column(String(64))
#     db_home = Column(String(256))
#     listener_port = Column(String(22))
#     monitor = Column(String(1))
#     wbx_cluster = Column(String(25))
#     date_added = Column(DateTime, default=func.now())
#     web_domain = Column(String(13))
#     createdtime = Column(DateTime, default=func.now())
#     lastmodifiedtime = Column(DateTime, default=func.now(), onupdate=func.now())
#
# class wbxpgserver(Base):
#     __tablename__ = "host_info"
#     host_name = Column(String(64), primary_key=True)
#     cname = Column(String(64))
#     domain = Column(String(50))
#     site_code = Column(String(5))
#     region_name = Column(String(5))
#     public_ip = Column(String(30))
#     private_ip = Column(String(30))
#     os_type_code = Column(String(30))
#     processor = Column(String(15))
#     kernel_release = Column(String(30))
#     hardware_platform = Column(String(30))
#     physical_cpu = Column(Integer)
#     cores = Column(Integer)
#     cpu_model = Column(String(50))
#     flag_node_virtual = Column(String(1))
#     install_date = Column(DateTime, default=func.now())
#     ssh_port = Column(String(15))
#     comments = Column(String(100))
#     createdtime = Column(DateTime, default=func.now())
#     lastmodifieddate = Column(DateTime, default=func.now(), onupdate=func.now())
#
class wbxpguser(Base):
    __tablename__ = "appln_pool_info"
    db_name = Column(String(64), primary_key=True)
    schemaname = Column(String(35), primary_key=True)
    appln_support_code = Column(String(25))
    password = Column(String(512))
    password_vault_path = Column(String(512))
    schematype = Column(String(16))
    createtime = Column(DateTime, default=func.now())
    lastmodifiedtime = Column(DateTime, default=func.now(), onupdate=func.now())
#
# class wbxloginuser(Base):
#     __tablename__ = "host_user_info"
#     host_name = Column(String(30), primary_key=True)
#     username = Column(String(30))
#     pwd = Column(String(64))
#     createtime = Column(DateTime, default=func.now())
#     lastmodifieddate = Column(DateTime, default=func.now(), onupdate=func.now())