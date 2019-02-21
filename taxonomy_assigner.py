from sqlalchemy import create_engine, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

engine = create_engine('mysql://root@127.0.0.1/taxonomy', echo=True)
session = sessionmaker(bind=engine)()
Base = declarative_base()

class Job(Base):
    __table__ = Table('jobs', Base.metadata,
                      autoload=True, autoload_with=engine)
    def __init__(self, created_time, finished_time, task_name, primer_seq, match_option, tax_alg, rdp_db, conf_level, tr_len, file_paths, result_path, log_path, job_state):
        self.created_time = created_time
        self.finished_time = finished_time
        self.task_name = task_name
        self.primer_seq = primer_seq
        self.match_option = match_option
        self.tax_alg = tax_alg
        self.rdp_db = rdp_db
        self.conf_level = conf_level
        self.tr_len = tr_len
        self.file_paths = file_paths
        self.result_path = result_path
        self.log_path = log_path
        self.job_state = job_state

# session.add(Job(datetime.now(), datetime.now(), "name", "primer", "fwdrev", "alg", "db", 0.0, 1, "path1", "path2", "path3", "STOPPED"))
data = session.query(Job).all()
