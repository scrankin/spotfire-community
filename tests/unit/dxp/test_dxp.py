import os
import tempfile

from spotfire_community.dxp.dxp import Dxp


def _make_basic_dxp_root() -> str:
    root = tempfile.mkdtemp(prefix="dxp_root_")
    # minimal structure used by serializer.is_valid_dxp_folder
    with open(os.path.join(root, "AnalysisDocument.xml"), "w", encoding="utf-8") as f:
        f.write("<AnalysisDocument />")
    # DataAccessPlan with one connection
    os.makedirs(os.path.join(root, "DataArchive"), exist_ok=True)
    with open(
        os.path.join(root, "DataArchive", "DataAccessPlan.xml"), "w", encoding="utf-8"
    ) as f:
        f.write(
            """
            <Root>
              <DataConnection Name="ConnA">
                <DataSourceLink>
                  <DataAdapter>
                    <Database>OldDB</Database>
                    <DataSource>OldHost</DataSource>
                  </DataAdapter>
                </DataSourceLink>
              </DataConnection>
            </Root>
            """
        )
    return root


def test_dxp_update_and_cleanup():
    root = _make_basic_dxp_root()

    # Build a temporary .dxp file pointing at the folder to exercise cleanup behavior
    import zipfile

    dxp_file_fd, dxp_file = tempfile.mkstemp(suffix=".dxp", prefix="dxp_file_")
    os.close(dxp_file_fd)
    with zipfile.ZipFile(dxp_file, "w") as z:
        for r, _, files in os.walk(root):
            for name in files:
                full = os.path.join(r, name)
                arc = os.path.relpath(full, start=root)
                z.write(full, arc)

    # Constructing from file should extract to temp and set cleanup flag
    d = Dxp(dxp_file)
    try:
        d.update_data_connection("ConnA", database="NewDB", host="NewHost")
        # Verify update persisted using public API
        files = d.get_all_files()
        dap = [
            p
            for p in files
            if p.endswith(os.path.join("DataArchive", "DataAccessPlan.xml"))
        ][0]
        with open(dap, "r", encoding="utf-8") as f:
            content = f.read()
        assert "NewDB" in content and "NewHost" in content

        # exercise zip and unzip helpers on the Dxp wrapper
        mem = d.get_zip_folder_in_memory()
        assert hasattr(mem, "read")
        # unzip to a temp directory
        out_dir = tempfile.mkdtemp(prefix="dxp_unzip_")
        try:
            d.unzip_to_path(out_dir)
            assert os.path.isfile(os.path.join(out_dir, "AnalysisDocument.xml"))
        finally:
            import shutil

            shutil.rmtree(out_dir)
    finally:
        # Trigger destructor cleanup
        del d

    os.remove(dxp_file)
    # remove original root
    import shutil

    shutil.rmtree(root)
