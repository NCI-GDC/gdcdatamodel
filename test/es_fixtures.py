"""This is the fixture data from sample_biospecemin.xml, just
represented in a way that can be persisted without using xml2psqlgraph.
"""

from gdcdatamodel.models import *

NODES = [
    Clinical(
        node_id='3239e85f-6be7-417b-b8e9-073c4d9c311c',
        project_id='TCGA-BRCA',
        age_at_diagnosis=34,
    ),
    Sample(
        node_id='5fa9998b-deff-493e-8a8e-dc2422192a48',
        project_id='TCGA-BRCA',
        current_weight=None,
        days_to_collection=1416,
        days_to_sample_procurement=None,
        freezing_method=None,
        initial_weight=250.0,
        intermediate_dimension=None,
        is_ffpe=False,
        longest_dimension=None,
        oct_embedded='true',
        pathology_report_uuid='747FB91B-F523-4FA0-91DD-6014EF55643D',
        sample_type='Primary Tumor',
        sample_type_id='01',
        shortest_dimension=None,
        submitter_id='TCGA-AR-A1AR-01A',
        time_between_clamping_and_freezing=None,
        time_between_excision_and_freezing=None,
        tumor_code=None,
        tumor_code_id=None
    ),
    Aliquot(
        node_id='84df0f82-69c4-4cd3-a4bd-f40d2d6ef916',
        project_id='TCGA-BRCA',
        amount=13.0,
        concentration=0.18,
        source_center='23',
        submitter_id='TCGA-AR-A1AR-10A-01D-A133-02'
    ),
    Analyte(
        node_id='344dffb3-2d2b-479d-8be5-9ead2728541b',
        project_id='TCGA-BRCA',
        a260_a280_ratio=None,
        amount=None,
        analyte_type='Repli-G (Qiagen) DNA',
        analyte_type_id='W',
        concentration=None,
        spectrophotometer_method=None,
        submitter_id='TCGA-AR-A1AR-01A-31W',
        well_number=None
    ),
    Analyte(
        node_id='07c974b3-3286-4c4f-8b67-6f8e425936f4',
        project_id='TCGA-BRCA',
        a260_a280_ratio=1.94,
        amount=22.25,
        analyte_type='DNA',
        analyte_type_id='D',
        concentration=0.18,
        spectrophotometer_method='UV Spec',
        submitter_id='TCGA-AR-A1AR-10A-01D',
        well_number=None
    ),
    Analyte(
        node_id='a58e8309-8346-4648-945d-e48efdc1a635',
        project_id='TCGA-BRCA',
        a260_a280_ratio=None,
        amount=None,
        analyte_type='Repli-G (Qiagen) DNA',
        analyte_type_id='W',
        concentration=None,
        spectrophotometer_method=None,
        submitter_id='TCGA-AR-A1AR-10A-01W',
        well_number=None
    ),
    Case(
        node_id='eda6d2d5-4199-4f76-a45b-1d0401b4e54c',
        project_id='TCGA-BRCA',
        days_to_index=0,
        submitter_id='TCGA-AR-A1AR'
    ),
    Portion(
        node_id='5b2a99b7-e1a8-4739-acaf-d5f75cc47021',
        project_id='TCGA-BRCA',
        creation_datetime=1293494400,
        is_ffpe=False,
        portion_number='01',
        submitter_id='TCGA-AR-A1AR-10A-01',
        weight=None
    ),
    Aliquot(
        node_id='2708315c-d58a-42d7-a914-d6299aa74936',
        project_id='TCGA-BRCA',
        amount=6.67,
        concentration=0.18,
        source_center='23',
        submitter_id='TCGA-AR-A1AR-10A-01D-A134-01'
    ),
    Aliquot(
        node_id='c7976361-e689-44f1-9e5a-2a07064f2f95',
        project_id='TCGA-BRCA',
        amount=6.67,
        concentration=0.16,
        source_center='23',
        submitter_id='TCGA-AR-A1AR-01A-31D-A134-01'
    ),
    Aliquot(
        node_id='0ffb3f3d-f20e-43d1-9867-7dc75ac24f3b',
        project_id='TCGA-BRCA',
        amount=20.0,
        concentration=0.16,
        source_center='23',
        submitter_id='TCGA-AR-A1AR-01A-31R-A136-13'
    ),
    Aliquot(
        node_id='05c45162-6c94-4a15-accc-b6239451064c',
        project_id='TCGA-BRCA',
        amount=13.0,
        concentration=0.16,
        source_center='23',
        submitter_id='TCGA-AR-A1AR-01A-31D-A133-02'
    ),
    Analyte(
        node_id='3febc6c8-85ae-4d38-ba55-c959959846db',
        project_id='TCGA-BRCA',
        a260_a280_ratio=1.98,
        amount=48.62,
        analyte_type='DNA',
        analyte_type_id='D',
        concentration=0.16,
        spectrophotometer_method='UV Spec',
        submitter_id='TCGA-AR-A1AR-01A-31D',
        well_number=None
    ),
    Aliquot(
        node_id='281bfaa0-3f3c-412f-a3f8-76f1aa6e53ed',
        project_id='TCGA-BRCA',
        amount=80.0,
        concentration=0.5,
        source_center='23',
        submitter_id='TCGA-AR-A1AR-10A-01W-A14P-09'
    ),
    Aliquot(
        node_id='0395a62f-3f37-4068-bab6-4c1d29cef2d5',
        project_id='TCGA-BRCA',
        amount=40.0,
        concentration=0.09,
        source_center='23',
        submitter_id='TCGA-AR-A1AR-10A-01D-A135-09'
    ),
    Aliquot(
        node_id='6d066a72-f59f-45a8-ab90-216000b36da4',
        project_id='TCGA-BRCA',
        amount=26.7,
        concentration=0.16,
        source_center='23',
        submitter_id='TCGA-AR-A1AR-01A-31R-A137-07'
    ),
    Aliquot(
        node_id='008ba655-a0a3-42c4-8c72-f1341365ef02',
        project_id='TCGA-BRCA',
        amount=40.0,
        concentration=0.08,
        source_center='23',
        submitter_id='TCGA-AR-A1AR-01A-31D-A135-09'
    ),
    Slide(
        node_id='3013e9be-aa3e-4986-990c-559982f00e36',
        project_id='TCGA-BRCA',
        number_proliferating_cells=None,
        percent_eosinophil_infiltration=None,
        percent_granulocyte_infiltration=None,
        percent_inflam_infiltration=None,
        percent_lymphocyte_infiltration=0.0,
        percent_monocyte_infiltration=0.0,
        percent_necrosis=0.0,
        percent_neutrophil_infiltration=0.0,
        percent_normal_cells=0.0,
        percent_stromal_cells=20.0,
        percent_tumor_cells=80.0,
        percent_tumor_nuclei=90.0,
        section_location='TOP',
        submitter_id='TCGA-AR-A1AR-01A-03-TSC'
    ),
    Aliquot(
        node_id='7b017050-97d4-45bb-bf83-c89dab812e44',
        project_id='TCGA-BRCA',
        amount=26.7,
        concentration=0.16,
        source_center='23',
        submitter_id='TCGA-AR-A1AR-01A-31D-A138-05'
    ),
    Aliquot(
        node_id='c1fd82a9-f75f-4297-b2c2-ec91c40a57f4',
        project_id='TCGA-BRCA',
        amount=80.0,
        concentration=0.5,
        source_center='23',
        submitter_id='TCGA-AR-A1AR-01A-31W-A14P-09'
    ),
    Portion(
        node_id='40407260-e805-4c2e-b2a7-13862bc5e494',
        project_id='TCGA-BRCA',
        creation_datetime=1297728000,
        is_ffpe=False,
        portion_number='31',
        submitter_id='TCGA-AR-A1AR-01A-31',
        weight=30.0
    ),
    Analyte(
        node_id='5f5b9bb2-3278-424f-9cf2-e26f0c3b0fd5',
        project_id='TCGA-BRCA',
        a260_a280_ratio=1.82,
        amount=29.44,
        analyte_type='RNA',
        analyte_type_id='R',
        concentration=0.16,
        spectrophotometer_method='UV Spec',
        submitter_id='TCGA-AR-A1AR-01A-31R',
        well_number=None
    ),
    Sample(
        node_id='c1e5beaa-6103-409d-bdd4-a86c0f210014',
        project_id='TCGA-BRCA',
        current_weight=None,
        days_to_collection=1416,
        days_to_sample_procurement=None,
        freezing_method=None,
        initial_weight=None,
        intermediate_dimension=None,
        is_ffpe=False,
        longest_dimension=None,
        oct_embedded='false',
        pathology_report_uuid='91C655D1-C777-41A9-B759-7ED12C72CF30',
        sample_type='Blood Derived Normal',
        sample_type_id='10',
        shortest_dimension=None,
        submitter_id='TCGA-AR-A1AR-10A',
        time_between_clamping_and_freezing=None,
        time_between_excision_and_freezing=None,
        tumor_code=None,
        tumor_code_id=None
    )
]


EDGES = [
    ClinicalDescribesCase(
        src_id='3239e85f-6be7-417b-b8e9-073c4d9c311c',
        dst_id='eda6d2d5-4199-4f76-a45b-1d0401b4e54c',
    ),
    AliquotDerivedFromAnalyte(
        src_id='7b017050-97d4-45bb-bf83-c89dab812e44',
        dst_id='3febc6c8-85ae-4d38-ba55-c959959846db',
        properties={}
    ),
    AliquotShippedToCenter(
        src_id='0395a62f-3f37-4068-bab6-4c1d29cef2d5',
        dst_id='956ca84c-1124-53ff-824f-fa0c84425425',
        properties={'plate_column': '11',
                    'plate_id': 'A135',
                    'plate_row': 'B',
                    'shipment_center_id': '09',
                    'shipment_datetime': 1299542400,
                    'shipment_reason': None}),
    AliquotShippedToCenter(
        src_id='0ffb3f3d-f20e-43d1-9867-7dc75ac24f3b',
        dst_id='6eba705a-0f00-5aa2-b1d0-04dbf62100cc',
        properties={'plate_column': '6',
                    'plate_id': 'A136',
                    'plate_row': 'C',
                    'shipment_center_id': '13',
                    'shipment_datetime': 1299542400,
                    'shipment_reason': None}),
    AliquotShippedToCenter(
        src_id='281bfaa0-3f3c-412f-a3f8-76f1aa6e53ed',
        dst_id='956ca84c-1124-53ff-824f-fa0c84425425',
        properties={'plate_column': '11',
                    'plate_id': 'A14P',
                    'plate_row': 'B',
                    'shipment_center_id': '09',
                    'shipment_datetime': 1304380800,
                    'shipment_reason': None}),
    AnalyteDerivedFromPortion(
        src_id='a58e8309-8346-4648-945d-e48efdc1a635',
        dst_id='5b2a99b7-e1a8-4739-acaf-d5f75cc47021',
        properties={}),
    AliquotDerivedFromAnalyte(
        src_id='281bfaa0-3f3c-412f-a3f8-76f1aa6e53ed',
        dst_id='a58e8309-8346-4648-945d-e48efdc1a635',
        properties={}),
    AnalyteDerivedFromPortion(
        src_id='3febc6c8-85ae-4d38-ba55-c959959846db',
        dst_id='40407260-e805-4c2e-b2a7-13862bc5e494',
        properties={}),
    SampleDerivedFromCase(
        src_id='c1e5beaa-6103-409d-bdd4-a86c0f210014',
        dst_id='eda6d2d5-4199-4f76-a45b-1d0401b4e54c',
        properties={}),
    AliquotShippedToCenter(
        src_id='008ba655-a0a3-42c4-8c72-f1341365ef02',
        dst_id='956ca84c-1124-53ff-824f-fa0c84425425',
        properties={'plate_column': '5',
                    'plate_id': 'A135',
                    'plate_row': 'B',
                    'shipment_center_id': '09',
                    'shipment_datetime': 1299542400,
                    'shipment_reason': None}),
    SlideDerivedFromPortion(
        src_id='3013e9be-aa3e-4986-990c-559982f00e36',
        dst_id='40407260-e805-4c2e-b2a7-13862bc5e494',
        properties={}),
    CaseMemberOfProject(
        src_id='eda6d2d5-4199-4f76-a45b-1d0401b4e54c',
        dst_id='1334612b-3d2e-5941-a476-d455d71b458f',
        properties={}),
    AliquotDerivedFromSample(
        src_id='0395a62f-3f37-4068-bab6-4c1d29cef2d5',
        dst_id='c1e5beaa-6103-409d-bdd4-a86c0f210014',
        properties={}),
    AliquotShippedToCenter(
        src_id='7b017050-97d4-45bb-bf83-c89dab812e44',
        dst_id='7ef3885b-37ce-5e16-8ba3-9d75b6690008',
        properties={'plate_column': '5',
                    'plate_id': 'A138',
                    'plate_row': 'B',
                    'shipment_center_id': '05',
                    'shipment_datetime': 1299542400,
                    'shipment_reason': None}),
    AliquotDerivedFromAnalyte(
        src_id='84df0f82-69c4-4cd3-a4bd-f40d2d6ef916',
        dst_id='07c974b3-3286-4c4f-8b67-6f8e425936f4',
        properties={}),
    AliquotDerivedFromAnalyte(
        src_id='008ba655-a0a3-42c4-8c72-f1341365ef02',
        dst_id='3febc6c8-85ae-4d38-ba55-c959959846db',
        properties={}),
    AnalyteDerivedFromPortion(
        src_id='344dffb3-2d2b-479d-8be5-9ead2728541b',
        dst_id='40407260-e805-4c2e-b2a7-13862bc5e494',
        properties={}),
    AliquotDerivedFromSample(
        src_id='84df0f82-69c4-4cd3-a4bd-f40d2d6ef916',
        dst_id='c1e5beaa-6103-409d-bdd4-a86c0f210014',
        properties={}),
    AliquotDerivedFromSample(
        src_id='008ba655-a0a3-42c4-8c72-f1341365ef02',
        dst_id='5fa9998b-deff-493e-8a8e-dc2422192a48',
        properties={}),
    AliquotShippedToCenter(
        src_id='84df0f82-69c4-4cd3-a4bd-f40d2d6ef916',
        dst_id='c8611490-4cbd-5651-8de2-64484a515eec',
        properties={'plate_column': '11',
                    'plate_id': 'A133',
                    'plate_row': 'B',
                    'shipment_center_id': '02',
                    'shipment_datetime': 1299542400,
                    'shipment_reason': None}),
    AliquotDerivedFromAnalyte(
        src_id='0395a62f-3f37-4068-bab6-4c1d29cef2d5',
        dst_id='07c974b3-3286-4c4f-8b67-6f8e425936f4',
        properties={}),
    AliquotDerivedFromAnalyte(
        src_id='05c45162-6c94-4a15-accc-b6239451064c',
        dst_id='3febc6c8-85ae-4d38-ba55-c959959846db',
        properties={}),
    AliquotDerivedFromAnalyte(
        src_id='2708315c-d58a-42d7-a914-d6299aa74936',
        dst_id='07c974b3-3286-4c4f-8b67-6f8e425936f4',
        properties={}),
    CaseProcessedAtTissueSourceSite(
        src_id='eda6d2d5-4199-4f76-a45b-1d0401b4e54c',
        dst_id='5e793cf6-1554-55db-b2ee-9c772717cea0',
        properties={}),
    AliquotDerivedFromSample(
        src_id='c7976361-e689-44f1-9e5a-2a07064f2f95',
        dst_id='5fa9998b-deff-493e-8a8e-dc2422192a48',
        properties={}),
    AliquotDerivedFromSample(
        src_id='0ffb3f3d-f20e-43d1-9867-7dc75ac24f3b',
        dst_id='5fa9998b-deff-493e-8a8e-dc2422192a48',
        properties={}),
    AliquotDerivedFromAnalyte(
        src_id='c7976361-e689-44f1-9e5a-2a07064f2f95',
        dst_id='3febc6c8-85ae-4d38-ba55-c959959846db',
        properties={}),
    AliquotDerivedFromSample(
        src_id='281bfaa0-3f3c-412f-a3f8-76f1aa6e53ed',
        dst_id='c1e5beaa-6103-409d-bdd4-a86c0f210014',
        properties={}),
    AliquotDerivedFromSample(
        src_id='2708315c-d58a-42d7-a914-d6299aa74936',
        dst_id='c1e5beaa-6103-409d-bdd4-a86c0f210014',
        properties={}),
    AliquotDerivedFromSample(
        src_id='c1fd82a9-f75f-4297-b2c2-ec91c40a57f4',
        dst_id='5fa9998b-deff-493e-8a8e-dc2422192a48',
        properties={}),
    PortionDerivedFromSample(
        src_id='40407260-e805-4c2e-b2a7-13862bc5e494',
        dst_id='5fa9998b-deff-493e-8a8e-dc2422192a48',
        properties={}),
    PortionDerivedFromSample(
        src_id='5b2a99b7-e1a8-4739-acaf-d5f75cc47021',
        dst_id='c1e5beaa-6103-409d-bdd4-a86c0f210014',
        properties={}),
    AliquotDerivedFromSample(
        src_id='05c45162-6c94-4a15-accc-b6239451064c',
        dst_id='5fa9998b-deff-493e-8a8e-dc2422192a48',
        properties={}),
    AliquotDerivedFromAnalyte(
        src_id='0ffb3f3d-f20e-43d1-9867-7dc75ac24f3b',
        dst_id='5f5b9bb2-3278-424f-9cf2-e26f0c3b0fd5',
        properties={}),
    AnalyteDerivedFromPortion(
        src_id='5f5b9bb2-3278-424f-9cf2-e26f0c3b0fd5',
        dst_id='40407260-e805-4c2e-b2a7-13862bc5e494',
        properties={}),
    AliquotShippedToCenter(
        src_id='c7976361-e689-44f1-9e5a-2a07064f2f95',
        dst_id='5069ce55-a23f-57c4-a28c-70a3c3cb0e4c',
        properties={'plate_column': '5',
                    'plate_id': 'A134',
                    'plate_row': 'B',
                    'shipment_center_id': '01',
                    'shipment_datetime': 1299542400,
                    'shipment_reason': None}),
    AliquotShippedToCenter(
        src_id='6d066a72-f59f-45a8-ab90-216000b36da4',
        dst_id='ee7a85b3-8177-5d60-a10c-51180eb9009c',
        properties={'plate_column': '6',
                    'plate_id': 'A137',
                    'plate_row': 'C',
                    'shipment_center_id': '07',
                    'shipment_datetime': 1299542400,
                    'shipment_reason': None}),
    SampleDerivedFromCase(
        src_id='5fa9998b-deff-493e-8a8e-dc2422192a48',
        dst_id='eda6d2d5-4199-4f76-a45b-1d0401b4e54c',
        properties={}),
    AliquotDerivedFromAnalyte(
        src_id='6d066a72-f59f-45a8-ab90-216000b36da4',
        dst_id='5f5b9bb2-3278-424f-9cf2-e26f0c3b0fd5',
        properties={}),
    AliquotShippedToCenter(
        src_id='2708315c-d58a-42d7-a914-d6299aa74936',
        dst_id='5069ce55-a23f-57c4-a28c-70a3c3cb0e4c',
        properties={'plate_column': '11',
                    'plate_id': 'A134',
                    'plate_row': 'B',
                    'shipment_center_id': '01',
                    'shipment_datetime': 1299542400,
                    'shipment_reason': None}),
    AnalyteDerivedFromPortion(
        src_id='07c974b3-3286-4c4f-8b67-6f8e425936f4',
        dst_id='5b2a99b7-e1a8-4739-acaf-d5f75cc47021',
        properties={}),
    AliquotShippedToCenter(
        src_id='05c45162-6c94-4a15-accc-b6239451064c',
        dst_id='c8611490-4cbd-5651-8de2-64484a515eec',
        properties={'plate_column': '5',
                    'plate_id': 'A133',
                    'plate_row': 'B',
                    'shipment_center_id': '02',
                    'shipment_datetime': 1299542400,
                    'shipment_reason': None}),
    AliquotDerivedFromAnalyte(
        src_id='c1fd82a9-f75f-4297-b2c2-ec91c40a57f4',
        dst_id='344dffb3-2d2b-479d-8be5-9ead2728541b',
        properties={}),
    AliquotDerivedFromSample(
        src_id='7b017050-97d4-45bb-bf83-c89dab812e44',
        dst_id='5fa9998b-deff-493e-8a8e-dc2422192a48',
        properties={}),
    AliquotShippedToCenter(
        src_id='c1fd82a9-f75f-4297-b2c2-ec91c40a57f4',
        dst_id='956ca84c-1124-53ff-824f-fa0c84425425',
        properties={'plate_column': '5',
                    'plate_id': 'A14P',
                    'plate_row': 'B',
                    'shipment_center_id': '09',
                    'shipment_datetime': 1304380800,
                    'shipment_reason': None}),
    AliquotDerivedFromSample(
        src_id='6d066a72-f59f-45a8-ab90-216000b36da4',
        dst_id='5fa9998b-deff-493e-8a8e-dc2422192a48',
        properties={})
]


def insert(g):
    with g.session_scope() as session:
        for node in NODES:
            session.merge(node)
        for edge in EDGES:
            session.merge(edge)
