from pyliftover import LiftOver
import maya
"""
Civic Gene Summaries format
 0:gene_id	
 1: gene_civic_url	
 2: name	
 3: entrez_id	
 4: description	
 5: last_review_date
Civic Variant Summaries format
 0: variant_id	
 1: variant_civic_url	
 2: gene	entrez_id
 3: variant	summary	
 4: variant_groups	
 5: chromosome	
 6: start	
 7: stop	
 8: reference_bases	
 9: variant_bases	
 10: representative_transcript	
 11: ensembl_version	
 12: reference_build	
 13: chromosome2	
 14: start2	
 15: stop2	
 16: representative_transcript2variant_types	
 17: hgvs_expressions	
 18: last_review_date	
 19: civic_actionability_score
"""

class CivicGene(object):
    def __init__(self, line):
        array = line.strip().split('\t')
        self.gene_id = array[0]
        self.gene_civic_url = array[1]
        self.name = array[2]
        self.entrez_id = array[3]
        self.description = array[4]
        self.last_review_date = maya.MayaDT.from_rfc2822(array[5])


class CivicVariant(object):
    def __init__(self, line):

        pass
