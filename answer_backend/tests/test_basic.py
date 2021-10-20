import pytest
from pysam import VariantFile
import variantparser
from variantparser import Case
from clarity_package import apiutil
import apisecret
from variantparser import dbutil


def test_one():
    pass


def test_two():
    expected = (1, 2, 3)
    actual = (1, 2, 3)
    assert expected == actual


def test_compare_alteration():
    alteration = "K17R"
    variants = VariantFile("GM12878.utsw.vcf.gz")
    record = next(variants.fetch())
    assert variantparser.compare_by_annotation(alteration, record)


def test_convert_alteration():
    mda_alteration = "K17R"
    standard_alteration = "Lys17Arg"
    result = variantparser.mda_aa_to_utsw_aa(mda_alteration)
    assert result == standard_alteration


class TestCase:

    @pytest.fixture
    def clarity_api(self):
        HOSTNAME = "https://clarity.biohpc.swmed.edu"
        VERSION = 'v2'
        clarity_api = apiutil.Apiutil()
        clarity_api.set_hostname(HOSTNAME)
        clarity_api.set_version(VERSION)
        clarity_api.setup(apisecret.user, apisecret.password)
        return clarity_api

    @pytest.fixture
    def test_case(self):
        return Case('348794361-91944182')

    @pytest.fixture
    def db(self):
        return dbutil.DbClient('answer-test')

    def test_case_api(self, clarity_api, test_case):
        test_case.load_metadata(clarity_api)
        assert test_case.epic_order_date == '2018-02-23'

    def test_case_load(self, test_case):
        variant_file_name = '../348794361-91944182.utsw.vcf.gz'
        variant_file = VariantFile(variant_file_name)
        test_case.load_variant_file(variant_file)
        assert len(test_case.variants) == 1334

    def test_case_load_references(self, test_case, db):
        variant_file_name = '../348794361-91944182.utsw.vcf.gz'
        variant_file = VariantFile(variant_file_name)
        test_case.load_variant_file(variant_file)
        test_case.assign_variant_references(db)
        assert len(test_case.variants) == 1334
        assert test_case.variants[0].reference_id is not None
