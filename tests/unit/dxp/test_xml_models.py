import os
import tempfile
import xml.etree.ElementTree as ET

import pytest

from spotfire_community.dxp._xml.data_access_plan import DataAccessPlan
from spotfire_community.dxp._xml.data_connection import DataConnection
from spotfire_community.dxp.errors import DataConnectionNotFoundError


def _make_dap_tree(with_conn: bool = True) -> str:
    root = tempfile.mkdtemp(prefix="dap_root_")
    os.makedirs(os.path.join(root, "DataArchive"), exist_ok=True)
    # Minimal AnalysisDocument to satisfy serializer functions if used later
    with open(os.path.join(root, "AnalysisDocument.xml"), "w", encoding="utf-8") as f:
        f.write("<AnalysisDocument />")

    conn_block = (
        """
        <DataConnection Name="Conn1">
            <DataSourceLink>
                <DataAdapter>
                    <Database>DB1</Database>
                    <DataSource>Host1</DataSource>
                </DataAdapter>
            </DataSourceLink>
        </DataConnection>
        """
        if with_conn
        else ""
    )
    xml_text = f"""
    <Root>
        {conn_block}
    </Root>
    """
    with open(
        os.path.join(root, "DataArchive", "DataAccessPlan.xml"), "w", encoding="utf-8"
    ) as f:
        f.write(xml_text)
    return root


def test_data_access_plan_load_get_and_save(tmp_path: str | None = None):
    root = _make_dap_tree()
    try:
        dap = DataAccessPlan(root)
        # Should find connection
        conn = dap.get_data_connection("Conn1")
        assert isinstance(conn, DataConnection)
        # element tree accessor
        et = dap.get_element_tree()
        assert et.getroot() is not None

        # Get all
        all_conns = dap.get_data_all_connections()
        assert len(all_conns) == 1

        # Update values via properties
        assert conn.database.text == "DB1"
        assert conn.host.text == "Host1"
        conn.database = "DB2"
        conn.host = "Host2"
        # exercise get_element
        assert conn.get_element() is not None

        # Save and verify file changed
        dap.save(root)
        tree = ET.parse(os.path.join(root, "DataArchive", "DataAccessPlan.xml"))
        text_db = tree.findtext(".//Database")
        text_host = tree.findtext(".//DataSource")
        assert text_db == "DB2"
        assert text_host == "Host2"
    finally:
        # cleanup
        for sub in ("DataArchive",):
            p = os.path.join(root, sub)
            if os.path.isdir(p):
                # remove file then dirs
                try:
                    os.remove(os.path.join(p, "DataAccessPlan.xml"))
                except FileNotFoundError:
                    pass
        # remove whole tree
        import shutil

        shutil.rmtree(root)


def test_data_access_plan_connection_not_found():
    root = _make_dap_tree(with_conn=False)
    try:
        dap = DataAccessPlan(root)
        with pytest.raises(DataConnectionNotFoundError):
            dap.get_data_connection("Missing")
    finally:
        import shutil

        shutil.rmtree(root)


def test_data_connection_missing_elements_raise():
    # Make a bogus element missing expected children
    xml = ET.fromstring("<DataConnection />")
    dc = DataConnection(xml)
    with pytest.raises(ValueError):
        _ = dc.database
    with pytest.raises(ValueError):
        _ = dc.host
