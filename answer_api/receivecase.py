import pika
import ssl
import json
import maya
import logging
from pathlib import Path
import argparse
from clarityutils import apiutil
from pysam import VariantFile
import apisecret
import variantparser
from variantparser import dbutil as mongo
from variantparser import CaseLoadingError
from systemd.journal import JournaldLogHandler
from tokensecret import token
import requests
import pandas as pd
from pymongo import MongoClient
import traceback
import time
from manifest import Manifest

try:
    import receiverparameters

    SEQ_DATA_PATH = receiverparameters.seq_data_path
except ImportError:
    SEQ_DATA_PATH = '/PHG_Clinical/cases/'

logger = logging.getLogger("AnswerReceiver")


class ServerParameters(object):
    def __init__(self, test, debug):
        if test:
            if debug:
                self.db_name = 'answer-test'
            else:
                self.db_name = 'answer'
            if debug:
                self.bam_path = '/opt/answer/files/test_bams/'
            else:
                self.bam_path = '/opt/answer/files/bams/'
            if debug:
                self.vcf_path = '/opt/answer/files/test_vcfs/'
            else:
                self.vcf_path = '/opt/answer/files/vcfs/'
            self.image_path = '/opt/answer/files/musica/'
            self.credentials = pika.PlainCredentials('test', 'password')
            self.connections_parameters = pika.ConnectionParameters(
                '127.0.0.1',
                5672,
                '/',
                self.credentials,
            )

        else:
            self.credentials = pika.PlainCredentials('test', 'password')
            self.connections_parameters = pika.ConnectionParameters(
                '127.0.0.1',
                5672,
                '/',
                self.credentials,
            )
            self.db_name = 'answer'
            self.bam_path = '/opt/answer/files/bams/'
            self.vcf_path = '/opt/answer/files/vcfs/'
            self.image_path = '/opt/answer/files/musica/'


TEST = False
debug = False
EXAMPLE_URI = "https://utsw.claritylims.com/api/v2/artifacts/101C-101PA1"
HOSTNAME = "utsw.claritylims.com"
VERSION = 'v2'
BASE_URI = HOSTNAME + '/api/' + VERSION + '/'


# if TEST:
#     if debug:
#         DB_NAME = 'answer-test'
#     else:
#         DB_NAME = 'answer'
#     BAM_PATH = '/opt/answer/files/bams/'
#     credentials = pika.PlainCredentials('test', 'password')
#     CONNECTION_PARAMETERS = pika.ConnectionParameters(
#         '127.0.0.1',
#         5672,
#         '/',
#         credentials,
#     )
#
# else:
#     credentials = pika.PlainCredentials('test', 'password')
#     CONNECTION_PARAMETERS = pika.ConnectionParameters(
#         '198.215.54.75',
#         5671,
#         '/',
#         credentials,
#         ssl=True,
#         ssl_options=dict(
#             ca_certs="cacert.pem",
#             keyfile="key.pem",
#             certfile="cert.pem",
#             cert_reqs=ssl.CERT_REQUIRED)
#     )
#     DB_NAME = 'answer'
#     BAM_PATH = '/opt/answer/files/bams/'


def print_error(project_name, message):
    logger.error(project_name + ': ' + message)
    # print(maya.now().iso8601(), project_name, message)
    pass


def setup_globals_from_uri(uri):
    global HOSTNAME
    global VERSION
    global BASE_URI

    tokens = uri.split("/")
    HOSTNAME = "/".join(tokens[0:3])
    VERSION = tokens[4]
    BASE_URI = "/".join(tokens[0:5]) + "/"


def get_tumor_dna_dir_name(project_name):
    start_path = Path(SEQ_DATA_PATH + '/' + project_name)
    if not start_path.exists():
        raise CaseLoadingError('Could not find case directory')
    directories = [str(x) for x in start_path.iterdir() if x.is_dir()]
    for dir_name in directories:
        if "T_DNA" in dir_name:
            return dir_name
    print(maya.now().iso8601(), project_name + ":", "Unable to locate Tumor DNA directory")
    return None


def get_normal_dna_dir_name(project_name):
    start_path = Path(SEQ_DATA_PATH + '/' + project_name)
    directories = [str(x) for x in start_path.iterdir() if x.is_dir()]
    for dir_name in directories:
        if "N_DNA" in dir_name:
            return dir_name
    print(maya.now().iso8601(), project_name + ":", "Unable to locate Normal DNA directory")
    return None


def get_tumor_rna_dir_name(project_name):
    start_path = Path(SEQ_DATA_PATH + '/' + project_name)
    directories = [str(x) for x in start_path.iterdir() if x.is_dir()]
    for dir_name in directories:
        if "T_RNA" in dir_name:
            return dir_name
    print(maya.now().iso8601(), project_name + ":", "Unable to locate Tumor RNA directory")
    return None


def get_cnv_file(project_name):
    tumor_dna_dir_name = get_tumor_dna_dir_name(project_name)
    sample_name = tumor_dna_dir_name.split('/')[-1]
    cnv_filename = tumor_dna_dir_name + '/' + sample_name + '.cnv.answer.txt'
    try:
        cnv_file = open(cnv_filename)
    except FileNotFoundError:
        raise CaseLoadingError("CNV file not found")
    return cnv_file


def get_cnr_file(project_name):
    tumor_dna_dir_name = get_tumor_dna_dir_name(project_name)
    sample_name = tumor_dna_dir_name.split('/')[-1]
    cnr_filename = tumor_dna_dir_name + '/' + sample_name + '.answerplot.cnr'
    try:
        cnr_file = open(cnr_filename)
    except FileNotFoundError:
        raise CaseLoadingError("CNR file not found")
    return cnr_file


def get_cns_file(project_name):
    tumor_dna_dir_name = get_tumor_dna_dir_name(project_name)
    sample_name = tumor_dna_dir_name.split('/')[-1]
    cns_filename = tumor_dna_dir_name + '/' + sample_name + '.answerplot.cns'
    cns_file = open(cns_filename)
    return cns_file


def get_tmb_dataframe(project_name):
    tmb_path = Path(SEQ_DATA_PATH + '/' + project_name + '/' + project_name + '.TMB.csv')
    if tmb_path.exists():
        temp_df = pd.read_table(tmb_path, sep=',')
        df = temp_df.where(temp_df.notnull(), None)
        return df
    else:
        print_error(project_name, "TMB file not found.")
        return pd.DataFrame()


def get_tmb_file(project_name):
    tmb_file_name = SEQ_DATA_PATH + '/' + project_name + '/' + project_name + '.TMB.csv'
    try:
        file_handler = open(tmb_file_name)
    except FileNotFoundError:
        raise CaseLoadingError("TMB file not found")
    return file_handler


def get_virus_dataframe(project_name):
    virus_path = Path(SEQ_DATA_PATH + '/' + project_name + '/' + project_name + 'viral_results.txt')
    if virus_path.exists():
        temp_df = pd.read_table(virus_path)
        df = temp_df.where(temp_df.notnull(), None)
    else:
        print_error(project_name, "Virus file not found.")
        df = pd.DataFrame()

    return df


def get_ballele_freq_dataframe(project_name):
    tumor_dna_dir_name = get_tumor_dna_dir_name(project_name)
    if tumor_dna_dir_name is not None:
        sample_name = tumor_dna_dir_name.split('/')[-1]
        ballele_freq_filename = tumor_dna_dir_name + '/' + sample_name + '.ballelefreq.txt'
        print(ballele_freq_filename)
        ballele_freq_path = Path(ballele_freq_filename)
        if ballele_freq_path.exists():
            temp_df = pd.read_table(ballele_freq_path)
            df = temp_df.where(temp_df.notnull(), None)
            return df
        else:
            print_error(project_name, "Ballele Frequency file not found.")
            return None


def get_fpkm_dataframe(project_name):
    rna_dir_name = get_tumor_rna_dir_name(project_name)
    if rna_dir_name is not None:
        sample_name = rna_dir_name.split('/')[-1]
        fpkm_filename = rna_dir_name + '/' + sample_name + '.fpkm.txt'
        print(fpkm_filename)
        fpkm_path = Path(fpkm_filename)
        if fpkm_path.exists():
            temp_df = pd.read_table(fpkm_path)
            df = temp_df.where(temp_df.notnull(), None)
            return df
        else:
            print(maya.now().iso8601(), project_name + ":", "FPKM File not found.")
            return None
    else:
        print(maya.now().iso8601(), project_name + ":", "FPKM File not found.")
        return None


def get_virus_dataframe(project_name):
    extension = '.viral_results.txt'
    virus_path = Path(SEQ_DATA_PATH + '/' + project_name + '/' + project_name + extension)
    if virus_path.exists():
        temp_df = pd.read_table(virus_path)
        df = temp_df.where(temp_df.notnull(), None)
    else:
        df = pd.DataFrame()
    return df


def get_mutational_signature_df(project_name):
    df = pd.DataFrame()
    extension = '.mutational_signature.txt'
    mutation_signature_path = Path(SEQ_DATA_PATH + '/' + project_name + '/' + project_name + extension)
    if mutation_signature_path.exists():
        temp_df = pd.read_table(mutation_signature_path)
        df = temp_df.where(temp_df.notnull(), None)
    else:
        df = pd.DataFrame()
        print_error(project_name, "Mutation Signature file not found.")
    return df


def get_variant_file(project_name):
    variant_file_name = SEQ_DATA_PATH + '/' + project_name + '/' + project_name + '.vcf.gz'
    try:
        return VariantFile(variant_file_name)
    except FileNotFoundError:
        raise CaseLoadingError("Variant file not found")


def get_translocation_dataframe(project_name):
    rna_dir_name = get_tumor_rna_dir_name(project_name)
    extension = '.translocations.answer.txt'
    translocation_path = Path(SEQ_DATA_PATH + '/' + project_name + '/' + project_name + extension)
    if translocation_path.exists():
        df = pd.read_table(translocation_path)
        df2 = df.where(df.notnull(), None)
        return df2
    if rna_dir_name is not None:
        sample_name = rna_dir_name.split('/')[-1]
        translocation_path = Path(rna_dir_name + '/' + sample_name + extension)
        df = pd.read_table(translocation_path)
        df2 = df.where(df.notnull(), None)
        return df2
    print_error(project_name, "Translocation file not found.")
    return pd.DataFrame()


def get_translocation_file(project_name):
    rna_dir_name = get_tumor_rna_dir_name(project_name)
    if rna_dir_name is not None:
        sample_name = rna_dir_name.split('/')[-1]
        translocation_filename = rna_dir_name + '/' + sample_name + '.translocations.answer.txt'
    else:
        start_path = SEQ_DATA_PATH + '/' + project_name
        translocation_filename = start_path + '/' + project_name + '.translocations.answer.txt'
    try:
        with open(translocation_filename) as fp:
            fp.readline()
            translocation_file = fp.readlines()
    except FileNotFoundError:
        start_path = SEQ_DATA_PATH + '/' + project_name
        translocation_filename = start_path + '/' + project_name + '.translocations.answer.txt'
    try:
        with open(translocation_filename) as fp:
            fp.readline()
            translocation_file = fp.readlines()
    except:
        print_error(project_name, "Translocation file not found.")
        translocation_file = []
    return translocation_file


def check_case_exists(db, case_name):
    try:
        return db.get_case_by_name(case_name)['caseId']
    except TypeError:
        return None


def create_symlink(dir_name):
    if dir_name is None:
        return None
    sample_name = dir_name.split('/')[-1]
    link_path = Path(parameters.bam_path + sample_name + '.bam')
    if link_path.is_symlink():
        link_path.unlink()
    link_path.symlink_to(dir_name + '/' + sample_name + '.bam')
    link_path = Path(parameters.bam_path + sample_name + '.bai')
    if link_path.is_symlink():
        link_path.unlink()
    link_path.symlink_to(dir_name + '/' + sample_name + '.bam.bai')
    return sample_name + '.bam'


def create_musica_image_symlink(project_name):
    extension = '.mutational_signature.png'
    image_path = Path(SEQ_DATA_PATH + '/' + project_name + '/' + project_name + extension)
    if image_path.exists():
        link_path = Path(parameters.image_path + project_name + extension)
        if link_path.is_symlink():
            link_path.unlink()
        link_path.symlink_to(SEQ_DATA_PATH + project_name + '/' + project_name + extension)
        return project_name + extension


def create_musica_symlink(project_name):
    vcf_path = Path(SEQ_DATA_PATH + '/' + project_name + '/' + project_name + '.utswpass.somatic.vcf')
    if vcf_path.exists():
        link_path = Path(parameters.vcf_path + project_name + '.musica.vcf')
        if link_path.is_symlink():
            link_path.unlink()
        link_path.symlink_to(SEQ_DATA_PATH + project_name + '/' + project_name + '.utswpass.somatic.vcf')
        return project_name + '.musica.vcf'
    else:
        print("Musica VCF file not found.")
        return None


def read_manifest_file(manifest_path):
    path = Path(manifest_path)
    if path.exists():
        lines = open(path).readlines()
    else:
        print("ERROR: Unable to locate manifest file.")
        exit()
    return lines


def create_symlinks(case):
    tumor_dna_dir_name = get_tumor_dna_dir_name(case.case_name)
    tumor_rna_dir_name = get_tumor_rna_dir_name(case.case_name)
    normal_dna_dir_name = get_normal_dna_dir_name(case.case_name)
    case.tumor_dna_bam_path = create_symlink(tumor_dna_dir_name)
    case.tumor_rna_bam_path = create_symlink(tumor_rna_dir_name)
    case.normal_dna_bam_path = create_symlink(normal_dna_dir_name)
    case.musica_vcf_path = create_musica_symlink(case.case_name)
    case.mutation_signature_image_path = create_musica_image_symlink(case.case_name)


def load_case(project_name, project_location, manifest_path):
    logger.info(manifest_path)
    if manifest_path is not None:
        manifest = Manifest(read_manifest_file(manifest_path), logger)
        case = variantparser.create_case_from_manifest(manifest, clarity_api, logger, db)
        logger.info("Tried to load from manifest.")
        create_symlinks(case)
        db.insert_case(case)
        db.annotate_case(case)
        return
    if project_location is None:
        project_location = project_name
    tumor_dna_dir_name = get_tumor_dna_dir_name(project_location)
    if tumor_dna_dir_name is not None:
        cnv_file = get_cnv_file(project_location)
        cnr_file = get_cnr_file(project_location)
        cns_file = get_cns_file(project_location)
        variant_file = get_variant_file(project_location)
        tumor_sample_name = get_tumor_dna_dir_name(project_location).split('/')[-1]
    else:
        tumor_sample_name = None
        cnv_file = None
        cnr_file = None
        cns_file = None
        variant_file = []
        print_error(project_name, 'Unable to locate Tumor DNA')
    fpkm_df = get_fpkm_dataframe(project_location)
    ballele_freq_df = get_ballele_freq_dataframe(project_location)
    # translocation_file = get_translocation_file(project_location)
    translocation_df = get_translocation_dataframe(project_location)
    mutation_signature_df = get_mutational_signature_df(project_location)
    # try:
    #     tmb_file = get_tmb_file(project_location)
    # except CaseLoadingError as e:
    #     tmb_file = None
    #     print_error(project_name, e.message)
    tmb_df = get_tmb_dataframe(project_location)
    virus_df = get_virus_dataframe(project_location)
    case = variantparser.create_case(project_name, clarity_api, variant_file, translocation_df, cnv_file, cnr_file,
                                     cns_file, ballele_freq_df, tmb_df, fpkm_df, virus_df, mutation_signature_df,
                                     tumor_sample_name, db)
    create_symlinks(case)
    db.insert_case(case)
    db.annotate_case(case)
    pass


def delete_case(case_name):
    client = MongoClient('localhost', 27017)

    DB_NAME = 'answer'
    db_conn = client[DB_NAME]
    query = {'caseName': case_name}
    try:
        case = db_conn.cases.find_one(query)
        case_id = case['caseId']
    except TypeError:
        return
    if case_id == '':
        return
    elif case_id is None:
        return
    db_conn.variants.delete_many({'caseId': case_id})
    db_conn.translocations.delete_many({'caseId': case_id})
    db_conn.cnvs.delete_many({'caseId': case_id})
    db_conn.cnv_plot.delete_many({'caseId': case_id})
    db_conn.cases.delete_many({'caseId': case_id})


def import_case(channel, method, properties, body):
    logger.info("Waiting for new cases")
    message = json.loads(body)
    # print(maya.now().iso8601(), message['project_name'], "Case received")
    logger.info(message['project_name'] + ' ' + "Case received")
    # Going to have to restructure this to make versioning work.
    case_id = check_case_exists(db, message['project_name'])
    if case_id is not None:
        if message.get('reload', 'false') == 'true':
            db.get_case_version(message['project_name'])
        else:
            logger.info(message['project_name'] + ' ' + "Case already found in system.")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            # print(maya.now().iso8601(), message['project_name'], "Case already found in system.")


    else:
        project_location = message.get('project_location', None)
        try:
            load_case(message['project_name'], project_location, message.get('manifest', None))
            channel.basic_ack(delivery_tag=method.delivery_tag)
            if not args.debug:
                requests.get('https://127.0.0.1/newCaseUploadedEmail', params={'token': token, 'caseId': case_id},
                             verify=False)
        except (CaseLoadingError) as e:
            delete_case(message['project_name'])
            print_error(message['project_name'], e.message)
            print_error(message['project_name'], "Unable to load case")
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except (IndexError, KeyError, TypeError) as e:
            delete_case(message['project_name'])
            print(traceback.format_exc())
            print_error(message['project_name'], "Unable to load case")
            channel.basic_ack(delivery_tag=method.delivery_tag)

        # print(maya.now().iso8601(), message['project_name'], "Finished loading case")
        logger.info(message['project_name'] + " " + "Finished loading case")
    db.create_indices()


def main():
    global clarity_api, db, parameters, args
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--test', help="Set environment to test", dest='test',
                        required=False, default=False, action='store_true')
    parser.add_argument('-g', '--debug', help="Set environment to debug",
                        dest="debug", required=False, default=False, action='store_true')

    args = parser.parse_args()

    global clarity_api, db, parameters
    setup_globals_from_uri(EXAMPLE_URI)
    clarity_api = apiutil.Apiutil()
    clarity_api.set_hostname(HOSTNAME)
    clarity_api.set_version(VERSION)
    clarity_api.setup(apisecret.user, apisecret.password)
    parameters = ServerParameters(args.test, args.debug)
    db = mongo.DbClient(parameters.db_name)
    # 198.215.54.71
    journald_handler = JournaldLogHandler()
    journald_handler.setFormatter(logging.Formatter(
        '[%(levelname)s] %(message)s'
    ))
    if not logger.hasHandlers():
        logger.addHandler(journald_handler)
        logger.setLevel(logging.DEBUG)
    connection = pika.BlockingConnection(parameters.connections_parameters)
    channel = connection.channel()
    channel.queue_declare(queue='answer_import', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume('answer_import', import_case)
    channel.start_consuming()


if __name__ == '__main__':
    while True:
        try:
            main()
        except pika.exceptions.AMQPConnectionError:
            logger.info("RabbitMQ Server error, waiting to try again.")
            time.sleep(2)
