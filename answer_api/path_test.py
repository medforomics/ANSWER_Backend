from pathlib import Path

start_path = Path('/PHG_BarTender/bioinformatics/seqanalysis/347497830-72447918')

dir_names = [str(x) for x in start_path.iterdir() if x.is_dir()]
print(dir_names)