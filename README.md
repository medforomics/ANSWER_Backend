# ANSWER: a web-tool for clinical genomics reporting
This is the repository for the backend code for ANSWER.

# Installation
This code is written for Python version 3.6, although later versions of Python should also work. It's typically deployed
using Pipenv and a pipfile is included in the repository.

Requirements:
* Python 3.6
* Pipenv
* MongoDB 3.X

Pipenv should automatically download the rest of the requirements, it has been tested on RHEL 7

# Data Model
Variants are broken up into multiple categories, stored as Translocations, Variants, Virus, and Copy Number Variations. 

Variants are distinguished by Chromosome, Position, Alternate Allele, and Reference Allele. Alt and Reference are 
neccessary to distinguish insertions/deletions.

Viruses are distinguished by name.

CNVs are distinguished by the list of genes they affect.

Translocations are distinguished by the list of genes they affect.

Many other features are included in the MongoDB for each type of data, but they are all optional.

# Accessing the backend
Endpoints are described in the main Answer.py file. The backend is primarily intended for use by the frontend to display
data and not for end users. By default it is only accessible from the server where ANSWER is deployed.
