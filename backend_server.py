from datetime import datetime
from flask import Flask, request
from job_processor import Job, Session
app = Flask(__name__)

from session_scope import session_scope



@app.route('/', methods=['POST'])
def main():
    created_time = datetime.now() # datetime
    finished_time = None #datetime
    task_name = request.form['task_name']
    primer_seq = request.form['primer_seq']
    match_option = request.form['match_option']
    tax_alg = request.form['tax_alg']
    rdp_db = request.form['rdp_db']
    conf_level = float(request.form['conf_level']) # float
    tr_len = int(request.form['tr_len']) # integer
    file_paths = request.form['file_paths']
    result_path = None
    log_path = None
    job_state = 'ENQUEUED' # ('ENQUEUED', 'FINISHED', 'PROCESSING')

    job = Job(created_time, finished_time, task_name, primer_seq, match_option, tax_alg, rdp_db, conf_level, tr_len, file_paths, result_path, log_path, job_state)
    with session_scope(Session) as sess:
        sess.add(job)
    return 'OK'


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8787, debug=True)
