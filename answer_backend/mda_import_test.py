from variantparser.mdautil import annotate_case
import json

#with open('mda.json') as mda_file:
#    test = mda_file.readlines()
#    print("hello: ",test[68:70])
with open('mda.json', encoding='utf-8') as mda_file:
    test = json.load(mda_file)

#print(json.dumps(test, indent=2, sort_keys=True))

for key in test.keys():
    print(key)
print(test['mrn'])

for key,value in test['annotationRows'].items():
    print(value['alteration'])
    print(value['gene'])

#for key,value in test['annotationRows'].items():
#    for other_key in value:
#        print(other_key)
# annotate_case(test)

