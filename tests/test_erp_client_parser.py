from app.services.erp_client import ErpClient


def test_parse_stored_procedure_xml_extracts_record() -> None:
    xml = (
        '<NewDataSet>'
        '<Table>'
        '<pepUdicID>3bdab300d5f54c09b576c72c67ff13e1</pepUdicID>'
        '<NowDateTime>2026-03-03 16:26:00.994</NowDateTime>'
        '<qcRecords>[{"qcUdicID":"3ba9fbba870542b88d05adc7b5eb2ba5","recordCreatedDate":"2025-09-12T15:22:42.020","projectWbs":"50709.00","submittalName":"Schematic Design","projectName":"Proj","clientName":"Client","projectManager":"jack.turner@greshamsmith.com","market":"Healthcare","location":"Birmingham","reviewerData":[{"disciplineID":"f4540272347d4e438ad2b72806d6835e","reviewerDiscipline":"Structural"}]}]</qcRecords>'
        '</Table>'
        '</NewDataSet>'
    )

    result = ErpClient._parse_stored_procedure_xml(xml, "3ba9fbba870542b88d05adc7b5eb2ba5")

    assert result["pep_udic_id"] == "3bdab300d5f54c09b576c72c67ff13e1"
    assert result["project_wbs"] == "50709.00"
    assert result["pm_email"] == "jack.turner@greshamsmith.com"
    assert result["disciplines"][0]["erp_discipline_code"] == "F4540272347D4E438AD2B72806D6835E"
