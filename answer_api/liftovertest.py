from pyliftover import LiftOver

LIFTOVER_LOCATION = '/home/answerbe/resources/hg38ToHg19.over.chain.gz'

hg38_to_hg19 = LiftOver(LIFTOVER_LOCATION)
test = hg38_to_hg19.convert_coordinate('chr1', 1000000, '+')
blah = test[0]
chrom = blah[0]
print(test)
print(blah)
print(chrom)

