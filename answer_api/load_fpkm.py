from pymongo import MongoClient
from pathlib import Path
import maya
import pandas as pd
import json


class Fpkm(object):
    def __init__(self, row, case):
        # Gene ID Gene Name       Reference       Strand  Start   End     Coverage        FPKM    TPM
        self.case_id = case['caseId']
        self.oncotree_diagnosis = case['oncotreeDiagnosis']
        self.gene_id = row['Gene ID']
        self.gene_name = row['Gene Name']
        self.chrom = row['Reference']
        self.strand = row['Strand']
        self.start = int(row['Start'])
        self.end = int(row['End'])
        self.coverage = float(row['Coverage'])
        self.fpkm = float(row['FPKM'])
        self.tpm = float(row['TPM'])

    def as_mongo_dict(self):
        mongo_dict = {
            'caseId': self.case_id,
            'oncotreeDiagnosis': self.oncotree_diagnosis,
            'geneId': self.gene_id,
            'geneName': self.gene_name,
            'chrom': self.chrom,
            'strand': self.strand,
            'start': self.start,
            'end': self.end,
            'coverage': self.coverage,
            'fpkm': self.fpkm,
            'tpm': self.tpm,
        }
        return mongo_dict


client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = client[DB_NAME]

SEQ_DATA_PATH = '/PHG_Clinical/cases/'


def get_tumor_rna_dir_name(project_name):
    start_path = Path(SEQ_DATA_PATH + '/' + project_name)
    directories = [str(x) for x in start_path.iterdir() if x.is_dir()]
    for dir_name in directories:
        if "T_RNA" in dir_name:
            return dir_name
    print(maya.now().iso8601(), project_name + ":", "Unable to locate Tumor RNA directory")
    return None


def get_fpkm_file(project_name):
    rna_dir_name = get_tumor_rna_dir_name(project_name)
    if rna_dir_name is not None:
        sample_name = rna_dir_name.split('/')[-1]
        fpkm_filename = rna_dir_name + '/' + sample_name + '.fpkm.txt'
        print(fpkm_filename)
        fpkm_path = Path(fpkm_filename)
        if fpkm_path.exists():
            df = pd.read_table(fpkm_path)
            return df
        else:
            print(maya.now().iso8601(), project_name + ":", "FPKM File not found.")
            return None
    else:
        print(maya.now().iso8601(), project_name + ":", "FPKM File not found.")
        return None


def main():
    cases = db['cases'].find()
    missing_header = []
    missing_file = []
    to_mongo = []
    aml_rows = []
    aml_header = '\t'.join(['caseId', 'geneId', 'fpkm'])
    for case in cases:
        case_name = case['caseName']
        print(case_name)
        fpkm_df = get_fpkm_file(case_name)
        if fpkm_df is not None:
            try:
                fpkm_data = []
                for row_index, row in fpkm_df.iterrows():
                    fpkm_row = Fpkm(row, case)
                    fpkm_data.append(fpkm_row)
                    to_mongo.append(fpkm_row.as_mongo_dict())
                    if case['oncotreeDiagnosis'] == 'AML':
                        aml_row = '\t'.join([fpkm_row.case_id, fpkm_row.gene_name, str(fpkm_row.fpkm)])
                        aml_rows.append(aml_row)
            except KeyError:
                missing_header.append(case_name)
                print("Case: ", case_name, " is missing the header")
        else:
            missing_file.append(case_name)

    with open('missing_file.txt', 'w') as missing_file_text:
        for case_name in missing_file:
            missing_file_text.write(case_name + '\n')

    with open('missing_header.txt', 'w') as missing_header_text:
        for case_name in missing_header:
            missing_header_text.write(case_name + '\n')

    print("Got", len(to_mongo), "rows to add.")
    print(to_mongo[0])
    collection = 'fpkm'
    collection = db[collection]
    insert_ids = collection.insert_many(to_mongo)
    with open('aml_fpkm_data.csv', 'w') as aml_fpkm:
        for line in aml_rows:
            aml_fpkm.write(line + '\n')


if __name__ == '__main__':
    main()
