import time
from datetime import datetime
from sqlalchemy import create_engine, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from .session_scope import session_scope
from .taxonomy_assigner import assign_taxonomy

engine = create_engine('mysql://root@127.0.0.1/taxonomy', echo=True)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Job(Base):
    __table__ = Table('jobs', Base.metadata,
                      autoload=True, autoload_with=engine)
    def __init__(self, created_time, finished_time, task_name, primer_seq, match_option, tax_alg, rdp_db, conf_level, tr_len, file_paths, result_path, log_path, job_state):
        self.created_time = created_time # datetime
        self.finished_time = finished_time #datetime
        self.task_name = task_name
        self.primer_seq = primer_seq
        self.match_option = match_option
        self.tax_alg = tax_alg
        self.rdp_db = rdp_db
        self.conf_level = conf_level # float
        self.tr_len = tr_len # integer
        self.file_paths = file_paths
        self.result_path = result_path
        self.log_path = log_path
        self.job_state = job_state # ('ENQUEUED', 'FINISHED', 'PROCESSING')

# session.add(Job(datetime.now(), datetime.now(), "name", "primer", "fwdrev", "alg", "db", 0.0, 1, "path1", "path2", "path3", "STOPPED"))
#data = session.query(Job).order_by(Job.created_time).all()
#data = session.query(Job).all()
#print(len(data))

def is_job_availiable():
    with session_scope(Session) as sess:
        return len(sess.query(Job).order_by(Job.created_time).all()) != 0

def get_and_lock_job():
    with session_scope(Session) as sess:
        job = sess.query(Job).filter_by(Job.job_state == 'ENQUEUED').first()
        if job:
            job.job_state = 'PROCESSING'
            return job
    return None

def process_jobs():
    while is_job_availiable():
        job = get_and_lock_job()
        if job:
            assign_taxonomy(job)

while True:
    if is_job_availiable():
        process_jobs()
    time.sleep(1)
