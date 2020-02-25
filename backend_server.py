import os
import shutil
from datetime import datetime
from flask import Flask, request, Response, send_from_directory
from flask_cors import CORS
from job_processor import Job, Session
from session_scope import session_scope
from upload_util import handle_delete, handle_upload, make_response, validate
from taxonomy_assigner import data_dir
import json

app = Flask(__name__)
CORS(app)


@app.route('/jobs', methods=['POST'])
def main():
    created_time = datetime.now() # datetime
    finished_time = None #datetime
    task_name = request.form['taskname']
    primer_seq = request.form['primerseq']
    match_option = request.form['matchoption']
    tax_alg = request.form['taxalg']
    rdp_db = request.form['rdpdb']
    conf_level = float(request.form['conflevel']) # float
    tr_len = int(request.form['trlen']) # integer
    file_paths = request.form['filepaths']
    result_path = None
    log_path = None
    job_state = 'ENQUEUED' # ('ENQUEUED', 'FINISHED', 'PROCESSING')

    job = Job(created_time, finished_time, task_name, primer_seq, match_option, tax_alg, rdp_db, conf_level, tr_len, file_paths, result_path, log_path, job_state)
    with session_scope(Session) as sess:
        sess.add(job)
    return 'OK'

@app.route('/list')
def list_jobs():
    jobs_list = []
    with session_scope(Session) as sess:
        for job in sess.query(Job).all():
            job_data = {}
            job_data['created_time'] = str(job.created_time)
            job_data['finished_time'] = str(job.finished_time) \
                    if job.finished_time else None
            job_data['task_name'] = job.task_name
            job_data['primer_seq'] = job.primer_seq
            job_data['match_option'] = job.match_option
            job_data['tax_alg'] = job.tax_alg
            job_data['rdp_db'] = job.rdp_db
            job_data['conf_level'] = job.conf_level
            job_data['tr_len'] = job.tr_len
            job_data['file_paths'] = job.file_paths
            job_data['result_path'] = job.result_path if job.result_path else ''
            job_data['log_path'] = job.log_path if job.log_path else ''
            job_data['job_state'] = job.job_state
            job_data['id'] = job.id
            jobs_list.append(job_data)
    return Response(json.dumps(jobs_list), status=200, mimetype='application/json')

@app.route('/jobs/<job_id>', methods=['DELETE'])
def delete_job(job_id):
    with session_scope(Session) as sess:
        for job in sess.query(Job).filter(Job.id==job_id).all():
            for uploaded_file in job.file_paths.split(","):
                uploaded_dir, _ = os.path.split(uploaded_file)
                uploaded_dir = os.path.join(data_dir, uploaded_dir) 
                shutil.rmtree(uploaded_dir, ignore_errors=True)
        sess.query(Job).filter(Job.id==job_id).delete()
    return 'OK'

@app.route('/upload', methods=['POST'])
def do_upload():
    """A POST request. Validate the form and then handle the upload
    based ont the POSTed data. Does not handle extra parameters yet.
    """
    if validate(request.form):
        handle_upload(request.files['qqfile'], request.form)
        return make_response(200, {"success": True})
    else:
        return make_response(400, {"error", "Invalid request"})


@app.route('/upload/<uuid>', methods=['DELETE'])
def do_delete(uuid):
    """A DELETE request. If found, deletes a file with the corresponding
    UUID from the server's filesystem.
    """
    try:
        handle_delete(uuid)
        return make_response(200, {"success": True})
    except Exception as e:
        return make_response(400, {"success": False, "error": str(e)})

@app.route('/download/<path:filename>', methods=['GET'])
def do_download(filename):
    return send_from_directory(directory=data_dir, filename=filename)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8787, debug=True)
