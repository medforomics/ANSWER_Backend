gene_names = []
with open("gencode.genenames.txt") as gencode_file:
    for line in gencode_file:
        array = line.strip().split("\t")
        gene_names.append(array[4].upper())
with open("genes_in_panel.csv") as genes_file:
    for line in genes_file:
        name = line.strip().upper()
        if name not in gene_names:
            print("Error, couldn't find gene",name)

