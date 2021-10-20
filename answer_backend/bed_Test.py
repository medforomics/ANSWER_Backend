with open('repeat_regions.bed') as bed_file:
    stuff = set()
    for line in bed_file:
        array = line.strip().split('\t')
        repeats = array[3].split(',')
        for item in repeats:
            stuff.add(item)

for item in stuff:
    print(item)