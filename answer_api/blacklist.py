from pymongo import MongoClient
from variantparser import ReferenceVariant
import numpy


class SimpleVariant(object):
    def __init__(self, chrom, pos, alt):
        self.chrom = chrom
        self.pos = pos
        self.alt = alt
        self.comp = self.chrom + ":" + str(self.pos) + ":" + self.alt


client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = client[DB_NAME]
cursor = db.cases.find()
num_cases = cursor.count()
cursor = db.reference_variants.find()
print(cursor.count())
histo = {}
for i in range(num_cases + 1):
    histo[i] = 0
most_seen = 0
potential_black_list = []
for ref_variant in cursor:
    cases_seen = len(ref_variant['clinicalCases'])
    histo[cases_seen] += 1
    if cases_seen > most_seen:
        most_seen = cases_seen
    if cases_seen > (0.75 * num_cases):
        potential_black_list.append(ReferenceVariant(ref_variant))

print(len(potential_black_list))

coll = db['variants']

output = []
answer_black_list = []
output.append('Chrom,Pos,Alt,Mean,Standard_deviation,Variance,Times_seen,Times_Outside_Two_Std_Devs')
update = {'$set': {'isArtifact': False, 'isPotentialArtifact': False}}
db.reference_variants.update_many({}, update)
for ref_variant in potential_black_list:
    cursor = coll.find({'chrom': ref_variant.chrom, 'pos': ref_variant.pos, 'alt': ref_variant.alt})
    freqs = []
    for variant in cursor:
        freqs.append(variant['tumorAltFrequency'])
    ref_variant.freqs = numpy.array(freqs)
    ref_variant.mean = numpy.mean(ref_variant.freqs)
    ref_variant.variance = numpy.var(ref_variant.freqs)
    ref_variant.std_dev = numpy.std(ref_variant.freqs)
    two_std = 2 * ref_variant.std_dev
    outside_two = 0
    for value in freqs:
        if (value < (ref_variant.mean - two_std)) or (value > (ref_variant.mean + two_std)):
            outside_two += 1
    # print('Chrom:', ref_variant.chrom, 'Pos:', ref_variant.pos, 'Alt:', ref_variant.alt)
    # print('Mean:', ref_variant.mean, 'Standard Deviation:', ref_variant.std_dev, 'Variance:', ref_variant.variance)
    output.append(','.join([
        ref_variant.chrom, str(ref_variant.pos), ref_variant.alt, str(ref_variant.mean), str(ref_variant.std_dev),
        str(ref_variant.variance), str(len(freqs)), str(outside_two)]))
    answer_black_list.append(SimpleVariant(ref_variant.chrom, ref_variant.pos, ref_variant.alt))
    if ref_variant.mean < 0.4 and ref_variant.std_dev < 0.1:
        db.reference_variants.update({'_id': ref_variant.mongo_id}, {'$set': {'isArtifact': True}})
    else:
        db.reference_variants.update({'_id': ref_variant.mongo_id}, {'$set': {'isPotentialArtifact': True}})
    with open('black_list.txt', 'w') as fpout:
        fpout.write('\n'.join(output))

normal_black_list = []
comparison_out = open("in_both.tsv", 'w')
with open('normal_blacklist.tsv') as normals:
    for line in normals:
        array = line.strip().split("\t")
        normal_black_list.append(SimpleVariant(array[0], int(array[1]), array[3]))
        in_answer_list = "No"
        for variant in answer_black_list:
            if normal_black_list[-1].comp == variant.comp:
                in_answer_list = "Yes"
        comparison_out.write(line.strip() + '\t' + in_answer_list + '\n')
comparison_out.close()
