import os
import sys
import traceback
from logger import Logger
from taxonomy_backend import Archive, CountGenerator, FieldName, FilterSeq, MakeCluster, MarkLabel, MatchPrimer, MergeFiles, RenameCluster, SortCluster, TaxAssn, TaxData, UsearchGlobal

data_dir = "/root/taxonomy-assign-backend/media/upload/"
match_primer_fcn = MatchPrimer()
filter_fcn = FilterSeq()
mark_label_fcn = MarkLabel()
make_cluster_fcn = MakeCluster()
usearch_global_fcn = UsearchGlobal()
rename_cluster_fcn = RenameCluster()
sort_cluster_fcn = SortCluster()
merge_files_fcn = MergeFiles()
tax_assn_fcn = TaxAssn("/root/genedb")
count_gen_fcn = CountGenerator()
archive_fcn = Archive()

def dequote(s):
    """
    If a string has single or double quotes around it, remove them.
    Make sure the pair of quotes match.
    If a matching pair of quotes is not found, return the string unchanged.
    """
    if (s[0] == s[-1]) and s.startswith(("'", '"')):
        return s[1:-1]
    return s

def assign_taxonomy(job, log_enabled=False):
    filename = [os.path.join(data_dir, dequote(f.strip())) for f in job.file_paths.split(",")] # list
    taskname = job.task_name
    primerseq = job.primer_seq
    taxassnalg = job.tax_alg
    rdpdb = job.rdp_db
    conflevel = job.conf_level # float
    trunclen = "nt" if job.tr_len == -1 else job.tr_len # int, str
    logfilename = os.path.join(os.path.split(filename[0])[0], "log.txt")
    sys.stdout = Logger(logfilename)
    sys.stderr = Logger(logfilename)

    # parse job.match_option
    matchfwd = "fwd" in job.match_option # bool
    matchrev = "rev" in job.match_option # bool
    matchfull = "full" in job.match_option # bool
    
    tax_data = TaxData(filename=filename,
                   taskname=taskname,
                   primerseq=primerseq,
                   matchfwd=matchfwd,
                   matchrev=matchrev,
                   matchfull=matchfull,
                   taxassnalg=taxassnalg,
                   rdpdb=rdpdb,
                   conflevel=conflevel,
                   trunclen=trunclen,)
    
    try:
        tax_data.apply(match_primer_fcn)
        tax_data.apply(filter_fcn)
        tax_data.apply(mark_label_fcn)
        tax_data.apply(merge_files_fcn)
        tax_data.apply(make_cluster_fcn)
        tax_data.apply(usearch_global_fcn)
        tax_data.apply(tax_assn_fcn)
        tax_data.apply(count_gen_fcn)
        tax_data.apply(archive_fcn)
    except:
        traceback.print_exc(file=sys.stdout)
        if log_enabled:
            return None, logfilename
        return None
    finally:
        sys.stdout = sys.stdout.get_original_stdout()
        sys.stderr = sys.stderr.get_original_stdout()

    tax_data.delete_temp_file()

    if log_enabled:
        return tax_data.environment[FieldName.ARCHIVEFILE], logfilename
    return tax_data.environment[FieldName.ARCHIVEFILE]

