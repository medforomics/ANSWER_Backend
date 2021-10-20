from variantparser import dbutil as mongo
from variantparser.copynumbervariation import CopyNumberVariation

DB_NAME = 'answer'


def load_cnv_file(cnv_filename, case_id):
    # Header: FusionName, LeftGene, RightGene, LeftBreakpoint, RightBreakpoint, LeftStrand, RightStrand, RNAReads
    # DNAReads
    cnvs = []
    with open(cnv_filename) as cnv_file:
        header = cnv_file.readline()
        try:
            last_line = cnv_file.readline()
        except IOError:
            last_line = header
        genes = []
        for line in cnv_file:
            last_array = last_line.strip().split('\t')
            array = line.strip().split('\t')
            if (array[1] == last_array[1]) and (array[2] == last_array[2]) and array[3] == last_array[3]:
                genes.append(array[0])
            else:
                cnvs.append(CopyNumberVariation(genes, last_line, case_id))
                genes = []
            last_line = line
        cnvs.append(CopyNumberVariation(genes, last_line, case_id))
    return cnvs

def main():
    db = mongo.DbClient(DB_NAME)
    case_id = 'ORD873'
    cnvs = load_cnv_file('ORD873-27-5802_T_DNA_panel1385.cnvcalls.txt', case_id)
    for cnv in cnvs:
        db.insert('cnvs', cnv.as_mongo_dict())

if __name__ == '__main__':
    main()
