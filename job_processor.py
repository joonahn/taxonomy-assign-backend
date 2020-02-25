import os
import time
import traceback
from datetime import datetime
from sqlalchemy import create_engine, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from session_scope import session_scope
from taxonomy_assigner import assign_taxonomy, data_dir

engine = create_engine('postgresql://root:1q2w3e4r@db_server/taxonomy')
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
        self.job_state = job_state # ('ENQUEUED', 'FINISHED', 'PROCESSING','FAILED')

def is_job_availiable():
    with session_scope(Session) as sess:
        return sess.query(Job).filter_by(job_state='ENQUEUED').first()

def change_job_state(job, state):
    with session_scope(Session) as sess:
        job = sess.query(Job).filter(Job.id==job.id).first()
        if job:
            job.job_state = state

def set_job_archive_file(job, archive_file):
    with session_scope(Session) as sess:
        job = sess.query(Job).filter(Job.id==job.id).first()
        if job:
            job.result_path=archive_file
            job.finished_time = datetime.now()

def set_job_log_file(job, log_file):
    with session_scope(Session) as sess:
        job = sess.query(Job).filter(Job.id==job.id).first()
        if job:
            job.log_path=log_file

def get_and_lock_job():
    with session_scope(Session, expunge=True) as sess:
        job = sess.query(Job).filter_by(job_state='ENQUEUED').order_by(Job.created_time).first()
        if job:
            change_job_state(job, 'PROCESSING')
            return job
    return None

def mark_failed_jobs():
    with session_scope(Session) as sess:
        for job in sess.query(Job).filter_by(job_state='PROCESSING').all():
            job.job_state = 'FAILED'

def change_archive_file_name_as_jobname(job, archive_file):
    archive_dir, archive_fname = os.path.split(archive_file)
    new_archive_file = os.path.join(archive_dir, job.task_name + ".zip")
    os.rename(archive_file, new_archive_file)
    return new_archive_file

def process_jobs():
    while is_job_availiable():
        job = get_and_lock_job()
        print("locked job: ", job)
        if job:
            try:
                archive_file, log_file = assign_taxonomy(job, True)
            except KeyboardInterrupt as e:
                raise
            except Exception as e:
                traceback.print_exc()
                archive_file, log_file = None, None
            else:
                if log_file:
                    parsed_log_file=log_file.replace(data_dir, "")
                    set_job_log_file(job, parsed_log_file)

                if archive_file:
                    archive_file = change_archive_file_name_as_jobname(job, archive_file)
                    parsed_archive_file=archive_file.replace(data_dir, "")
                    set_job_archive_file(job, parsed_archive_file)
                    change_job_state(job, 'FINISHED')
                    continue
            
            change_job_state(job, 'FAILED')

if __name__ == "__main__":
    mark_failed_jobs()
    while True:
        print("is_job_availiable(): ", is_job_availiable())
        if is_job_availiable():
            process_jobs()
        time.sleep(5)
