from pymongo import MongoClient
from pysam import VariantFile

vcf_reader = vcf.Reader(filename='m16-3126.utsw.vcf.gz')
record = next(vcf_reader)
print(record.genotype('m16-3126_DNA_Panel1385_1_HFVC2BBXX').data)
print(record.FORMAT)
print(record.INFO)
print(record.ID)
print(record.INFO['ANN'])

for i,record in enumerate(vcf_reader):
    #print(record.ID)
    #print(record)
    #print(record.INFO['ANN'])
    array = record.INFO['ANN'][0].split("|")
    if array[3] == 'FLT3':
        print('Found: ',array[3])
        print(record.INFO)
    #print(record.INFO)
    try:
        if 'Ser1870' in ','.join(record.INFO['ANN']):
            print("Found",array[3])
            print(record.ID)
            print(record.INFO)
            print(array[9])
            print(record.INFO['ANN'])
    except KeyError:
        pass




