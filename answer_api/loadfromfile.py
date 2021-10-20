fpin = open('cases.txt')
import os

for case in fpin:
    # os.system('pipenv run python sendcase.py -i ' + case.strip() + ' -p')
    os.system('pipenv run python sendcase.py -i ' + case.strip() + ' -t')
