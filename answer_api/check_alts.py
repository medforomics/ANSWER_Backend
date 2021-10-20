from pysam import VariantFile
from pathlib import Path

curdir = Path('.')
for file_name in curdir.iterdir():
    if str(file_name).endswith('.vcf.gz') and str(file_name).startswith('34'):
        print("Analyzing", file_name)
        variants = VariantFile(file_name)

        record = next(variants.fetch())
        for record in variants:
            if (len(record.alts) > 1):
                if ("PASS" in record.filter):
                    print(record)

