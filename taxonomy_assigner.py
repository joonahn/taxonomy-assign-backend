from taxonomy_backend import Archive, CountGenerator, FieldName, FilterSeq, MakeCluster, MarkLabel, MatchPrimer, MergeFiles, RenameCluster, SortCluster, TaxAssn, TaxData, UsearchGlobal

match_primer_fcn = MatchPrimer()
filter_fcn = FilterSeq()
mark_label_fcn = MarkLabel()
make_cluster_fcn = MakeCluster()
usearch_global_fcn = UsearchGlobal()
rename_cluster_fcn = RenameCluster()
sort_cluster_fcn = SortCluster()
merge_files_fcn = MergeFiles()
tax_assn_fcn = TaxAssn("/home/qiime/genedb")
count_gen_fcn = CountGenerator()
archive_fcn = Archive()

def assign_taxonomy(job):
    filename = job.filename
    taskname = job.taskname
    primerseq = job.primerseq
    matchfwd = job.matchfwd
    matchrev = job.matchrev
    matchfull = job.matchfull
    taxassnalg = job.taxassnalg
    rdpdb = job.rdpdb
    conflevel = job.conflevel
    trunclen = job.trunclen
    tax_data = TaxData(filename,
                   taskname,
                   primerseq,
                   matchfwd,
                   matchrev,
                   matchfull,
                   taxassnalg,
                   rdpdb,
                   conflevel,
                   trunclen,)
    tax_data.apply(match_primer_fcn)
    tax_data.apply(filter_fcn)
    tax_data.apply(mark_label_fcn)
    tax_data.apply(merge_files_fcn)
    tax_data.apply(make_cluster_fcn)
    tax_data.apply(usearch_global_fcn)
    tax_data.apply(tax_assn_fcn)
    tax_data.apply(count_gen_fcn)
    tax_data.apply(archive_fcn)

    tax_data.delete_temp_file()
    return tax_data.environment[FieldName.ARCHIVEFILE]

