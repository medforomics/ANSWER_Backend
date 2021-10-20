from variantparser import dbutil as mongo
from variantparser import Translocation

DB_NAME = 'answer'


def load_translocation_file(translocation_filename, case_id):
    # Header: FusionName, LeftGene, RightGene, LeftBreakpoint, RightBreakpoint, LeftStrand, RightStrand, RNAReads
    # DNAReads
    translocations = []
    with open(translocation_filename) as translocation_file:
        translocation_file.readline()
        for line in translocation_file:
            translocations.append(Translocation(line, case_id))
    return translocations


def main():
    db = mongo.DbClient(DB_NAME)
    case_id = 'ORD873'
    translocations = load_translocation_file('ORD873-27-5803_T_RNA_panelrnaseq.translocations.txt', case_id)
    for translocation in translocations:
        db.insert('translocations', translocation.as_mongo_dict())


if __name__ == '__main__':
    main()
