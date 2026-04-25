from eu_energy_intelligence.ingestion.overlap import (
    build_dataset_params,
    count_xml_points,
    default_flow_partner,
)


def test_count_xml_points_handles_generation_payloads() -> None:
    xml_text = """
    <GL_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0">
      <TimeSeries>
        <Period>
          <Point><position>1</position></Point>
          <Point><position>2</position></Point>
        </Period>
      </TimeSeries>
    </GL_MarketDocument>
    """
    assert count_xml_points(xml_text) == 2


def test_build_dataset_params_uses_expected_keys_for_flows() -> None:
    params = build_dataset_params(
        dataset="flows",
        zone="RO",
        flow_partner="HU",
        period_start="202401010000",
        period_end="202401020000",
    )
    assert params["documentType"] == "A11"
    assert params["in_Domain"] == "10YRO-TEL------P"
    assert params["out_Domain"] == "10YHU-MAVIR----U"


def test_default_flow_partner_prefers_valid_neighbor() -> None:
    assert default_flow_partner("RO") == "HU"
    assert default_flow_partner("HU") == "RO"
