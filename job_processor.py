import time
from datetime import datetime
from sqlalchemy import create_engine, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from session_scope import session_scope
from taxonomy_assigner import assign_taxonomy

engine = create_engine('mysql://root:1q2w3e4r@127.0.0.1/taxonomy', echo=True)
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
        return sess.query(Job).filter_by(job_state='ENQUEUED').first()

def change_job_state(job, state):
    row_id = job.id
    with session_scope(Session) as sess:
        job = sess.query(Job).filter(Job.id==row_id).first()
        if job:
            job.job_state = state

def get_and_lock_job():
    with session_scope(Session, expunge=True) as sess:
        job = sess.query(Job).filter_by(job_state='ENQUEUED').order_by(Job.created_time).first()
        if job:
            change_job_state(job, 'PROCESSING')
            return job
    return None

def process_jobs():
    while is_job_availiable():
        job = get_and_lock_job()
        print("locked job: ", job)
        if job:
            archive_file = assign_taxonomy(job)
            print("archive_file: ", archive_file)
            job.result_path = archive_file
            change_job_state(job, 'FINISHED')

if __name__ == "__main__":
    while True:
        print("is_job_availiable(): ", is_job_availiable())
        if is_job_availiable():
            process_jobs()
        time.sleep(1)
