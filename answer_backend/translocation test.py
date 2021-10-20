import pika
import ssl
import json
import maya
from pathlib import Path

from pysam import VariantFile
import variantparser
from variantparser import dbutil as mongo

TEST = True
EXAMPLE_URI = "https://clarity.biohpc.swmed.edu/api/v2/artifacts/2-1057"
HOSTNAME = "clarity.biohpc.smwed.edu"
VERSION = 'v2'
BASE_URI = HOSTNAME + '/api/' + VERSION + '/'
if TEST == False:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'

seq_data_path = '/PHG_BarTender/bioinformatics/seqanalysis/'


def process_translocation_file(file_path, case_id):
    translocations = []
    with open(file_path) as file_in:
        header = file_in.readline()
        header_items = [x for x in header.split('\t')]
        for line in file_in:
            translocation = {}
            for key, value in zip(header_items, line.split('\t')):
                translocation[key] = value.strip()
            translocation['caseId'] = case_id
            translocations.append(translocation)
    return translocations

def get_translocation_file(project_name):
    start_path = Path(seq_data_path) / project_name
    for dir_name in start_path.iterdir():
        if "_T_RNA" in str(dir_name):
            rna_sample_name = str(dir_name).split('/')[-1]
            rna_file_path = dir_name / str(rna_sample_name + '.translocations.txt')
            return str(rna_file_path)


def main():
    project_name = '342338180-73015322'
    case_id = 'ORD606'
    translocation_file_path = get_translocation_file(project_name)

    translocations_output = process_translocation_file(translocation_file_path, case_id)


if __name__ == '__main__':
    main()
