from pymongo import MongoClient
import numpy
from scipy import stats
import matplotlib
import matplotlib.pyplot as plt

client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = client[DB_NAME]

cursor = db.cases.find()
print("Currently", cursor.count(), "cases in the system.")
results = ["Case name,Diagnosis,Slope,Intercept,R-Value,P-Value,Standard Error,R-Squared"]
normal_liquid_array = []
tumor_liquid_array = []
normal_solid_array = []
tumor_solid_array = []
frequency_header = "Case Name,Chromosome,Position,Tumor_AF,Normal_AF\n"
liquid_af_out = open("liquid_af.csv", 'w')
liquid_af_out.write(frequency_header)
solid_af_out = open("solid_af.csv", 'w')
solid_af_out.write(frequency_header)
for case in cursor:
    case_name = case['caseName']
    case_id = case['caseId']
    diagnosis = case.get('oncotreeDiagnosis', "")
    if diagnosis is None:
        diagnosis = ""
    variants = db.variants.find({'caseId': case_id})
    tumor_af_array = []
    normal_af_array = []
    pos_array = []
    chr_array = []
    for variant in variants:
        if variant['tumorAltFrequency'] < 0.20 and variant['normalTotalDepth'] is not None \
                and variant['filters'][0] == 'PASS':
            pos_array.append(variant['pos'])
            chr_array.append(variant['chrom'])
            tumor_af_array.append(variant['tumorAltFrequency'])
            normal_af_array.append(variant['normalAltFrequency'])
    if case['tumorTissueType'] == 'Blood' or case['tumorTissueType'] == 'Bone Marrow':
        tumor_liquid_array += tumor_af_array
        normal_liquid_array += normal_af_array
        for tumor_af, normal_af, chrom, pos in zip(tumor_af_array, normal_af_array, chr_array, pos_array):
            liquid_af_out.write(",".join([case_name, chrom, str(pos), str(tumor_af), str(normal_af)]) + '\n')
    else:
        tumor_solid_array += tumor_af_array
        normal_solid_array += normal_af_array
        for tumor_af, normal_af, chrom, pos in zip(tumor_af_array, normal_af_array, chr_array, pos_array):
            solid_af_out.write(",".join([case_name, chrom, str(pos), str(tumor_af), str(normal_af)]) + '\n')
    tumor_af_array = numpy.array(tumor_af_array)
    normal_af_array = numpy.array(normal_af_array)
    slope, intercept, r_value, p_value, std_err = stats.linregress(normal_af_array, tumor_af_array)
    array = [case_name, diagnosis, str(slope), str(intercept), str(r_value), str(p_value), str(std_err),
             str(r_value ** 2)]
    print(array)
    line = ",".join(array)
    results.append(line)

fig, ax = plt.subplots()
ax.plot(normal_liquid_array, tumor_liquid_array, 'b,')
ax.set(xlabel='Normal Allele Frequency', ylabel='Tumor Allele Frequency')
ax.set_autoscaley_on(False)
ax.set_ylim([0, 0.2])
ax.set_xlim([0, 0.2])
fig.savefig("liquid_contamination.png")

solid_fig, solid_ax = plt.subplots()
solid_ax.plot(normal_solid_array, tumor_solid_array, 'b,')
solid_ax.set(xlabel='Normal Allele Frequency', ylabel='Tumor Allele Frequency')
solid_ax.set_autoscaley_on(False)
solid_ax.set_ylim([0, 0.2])
solid_ax.set_xlim([0, 0.2])
solid_fig.savefig("solid_contamination.png")

tumor_af_array = numpy.array(tumor_liquid_array)
normal_af_array = numpy.array(normal_liquid_array)
case_name = "Liquid tumors"
diagnosis = ""
slope, intercept, r_value, p_value, std_err = stats.linregress(normal_af_array, tumor_af_array)
array = [case_name, diagnosis, str(slope), str(intercept), str(r_value), str(p_value), str(std_err),
         str(r_value ** 2)]
print(array)
line = ",".join(array)
results.append(line)
tumor_af_array = numpy.array(tumor_solid_array)
normal_af_array = numpy.array(normal_solid_array)
case_name = "Solid tumors"
diagnosis = ""
slope, intercept, r_value, p_value, std_err = stats.linregress(normal_af_array, tumor_af_array)
array = [case_name, diagnosis, str(slope), str(intercept), str(r_value), str(p_value), str(std_err),
         str(r_value ** 2)]
print(array)
line = ",".join(array)
results.append(line)

liquid_af_out.close()
solid_af_out.close()

with open('contamination.csv', 'w') as fpout:
    fpout.write("\n".join(results))
