"""
Score files
============
Tabular file that prints the scores of APPRIS methods. The description of the columns are the following:

0: + __Gene identifier__:
	Ensembl id, RefSeq id, or UniProt entry.

1: + __Gene name__

2: + __Transcript identifier__:
	Ensembl id, RefSeq id, or UniProt entry.

3: + __Protein identifier__:
	Ensembl id, RefSeq id, or UniProt entry.

4: + __Protein coding label__:
	- TRANSLATION, transcript translates to protein.
	- NO_TRANSLATION, transcript does not translate to protein.

5: + __Transcript Class (or Biotype)__:
	- A protein coding transcript is a spliced mRNA that leads to a protein product.
	- A processed transcript is a noncoding transcript that does not contain an open reading frame (ORF). This type of transcript is annotated by the VEGA/Havana manual curation project.
	- Nonsense-mediated decay indicates that the transcript undergoes nonsense mediated decay, a process which detects nonsense mutations and prevents the expression of truncated or erroneous proteins.
	- Transcribed pseudogenes and other non-coding transcripts do not result in a protein product.

6: + __Start/Stop codons do not found__:
	- start means 'Start codon does not found'
	- stop means 'Start codon does not found'

7: + __Consensus CDS identifier (CCDS)__:
 	The Consensus CDS ([CCDS](https://www.ncbi.nlm.nih.gov/projects/CCDS/CcdsBrowse.cgi)) project is a collaborative effort to identify a core set of human and mouse protein coding regions that are consistently annotated and of high quality.
 	The long term goal is to support convergence towards a standard set of gene annotations.

8: + __Transcript Support Level (TSL)__:
	The method relies on the primary data that can support full-length transcript structure: mRNA and EST alignments supplied by UCSC and Ensembl.
	The following categories are assigned to each of the evaluated annotations:
	- _tsl1_,  all splice junctions of the transcript are supported by at least one non-suspect mRNA
    - _tsl2_,  the best supporting mRNA is flagged as suspect or the support is from multiple ESTs
    - _tsl3_,  the only support is from a single EST
    - _tsl4_,  the best supporting EST is flagged as suspect
    - _tsl5_,  no single transcript supports the model structure
    - _tslNA_, the transcript was not analysed for one of the following reasons:
		- pseudogene annotation, including transcribed pseudogenes
        - human leukocyte antigen (HLA) transcript
		- immunoglobin gene transcript
		- T-cell receptor transcript
		- single-exon transcript (will be included in a future version)

	For more information:
	http://www.ensembl.org/Help/Glossary?id=492

9: + The absolute numbers of __functional residues__ detected (_firestar_)

10: + Score related to the number of exon that map to __protein structure__. Whether we have genomic information or not, we use _Matador3D_ or _Matador3Dv2_.

    _Matador3D_, the score is based on the number of exons that can be mapped to structural homologues.

    _Matador3Dv2_, the number represents the sum of bit-scores in PDB alignment.

11: + The __number of vertebrate species__ that have an isoform that aligns to the human isoform over the whole sequence
and without gaps (_CORSAIR_).

    Alignments with the same species scores just 0.5.

    We generate multiple alignments against orthologues from a vertebrate protein database.

	We only align a limited number of vertebrate species, chimp, mouse, rat, dog, cow etc.

12: + The absolute numbers of __pfam domains__ that are detected (_SPADE_):

    SPADE identifies the functional domains present in a transcript and detects those domains that are damaged (not whole). The number represents the sum of bitscores in Pfam alignment.

13: + Unknown

14: + The number of __TMH__ detected (_THUMP_).

    The numbers after the '-' indicate the numbers of partial TMH: 'Whole TMH'-'Partial TMH (damaged)'. By partial we could mean "broken" or not whole. Some TMH will be broken by a splicing event, but many TMH are not whole because the TMH domain boundaries do not always describe the domain well.

15: + Reliability score for __signal peptides and mitochondrial signal__ sequences (_CRASH_).

    We use a score of 3 or above as a reliable signal peptide, and mitochondrial signal sequences (separated by comma).

16: + The number of exons with unusual evolutionary rats (_INERTIA_) *__DEPRECATED!!__*

    INERTIA uses three alignment methods to generate cross-species alignments, from which SLR identifies exons with unusual evolutionary rates.

17: + __APPRIS score__

    Reliability score for the variants based on the scores of methods and a weight for them.

18: + __No. mapping peptides__

    Proteomic evidence *__only for the human genome (GENCODE gene set)__*.

    We have collected peptides from seven separate MS sources. Two came from large-scale proteomics databases, [PeptideAtlas](http://www.peptideatlas.org/) and [NIST](http://peptide.nist.gov/). Another four datasets that were recently published large-scale MS experiments. For all six datasets the starting point was the list of peptides provided by the authors or databases. We generated the final set of peptides (referred to as *__CNIO__* in house from an [X!Tandem](http://www.ncbi.nlm.nih.gov/pubmed/14558131) search against spectra from the [GPM](http://www.ncbi.nlm.nih.gov/pubmed/15595733) and PeptideAtlas databases, following the protocol set out in [Ezkurdia et al](http://www.ncbi.nlm.nih.gov/pubmed/22446687) with a false discovery rate of 0.1%. These seven studies cover a wide range of search engines, tissues and cell types.

    In order to improve reliability the peptides from each of these studies were filtered,
    eliminating non-tryptic and semi-tryptic peptides and peptides containing missed cleavages. For those studies where it was possible we considered only peptides identified by multiple search engines.

19: + __Reliability labels__ of APPRIS

    See [*__Principal Isoforms Flags__*](#principal-isoforms-flags) section, for more information.
"""


def parse_to_mongo(appris_file, appris_columns, db):
    collection = db.appris
    appris_data = []
    for line in appris_file:
        array = line.split('\t')
        appris_data.append({})
        for index, item in enumerate(array):
            appris_data[-1][appris_columns[index]] = item
    for item in appris_data:
        collection.insert_one(item)
