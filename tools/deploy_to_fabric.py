"""Creates or updates the project notebooks in a Fabric workspace, straight from this repository.

Run it from any notebook in the target workspace (the lakehouse lh_retail must exist):

    exec(__import__("requests").get("https://raw.githubusercontent.com/Nusrat-cell/fabric-retail-lakehouse/main/tools/deploy_to_fabric.py").text)

Set REF before the exec to deploy a specific commit instead of main.
"""
import requests

REPO = "https://raw.githubusercontent.com/Nusrat-cell/fabric-retail-lakehouse"
REF = globals().get("REF", "main")
LAKEHOUSE = "lh_retail"
NOTEBOOKS = [
    "nb_lib_retail",
    "00_generate_source_data",
    "01_bronze_ingest",
    "02_silver_transform",
    "03_gold_model",
    "04_data_quality_gate",
    "05_semantic_model",
]


def deploy(name):
    content = requests.get(f"{REPO}/{REF}/notebooks/{name}.ipynb", timeout=30)
    content.raise_for_status()
    try:
        notebookutils.notebook.create(name, "", content.text, LAKEHOUSE)  # noqa: F821 (Fabric built-in)
        return "created"
    except Exception as e:  # the notebook is already there (ItemDisplayNameAlreadyInUse)
        if "alreadyinuse" not in str(e).lower().replace(" ", ""):
            raise
        notebookutils.notebook.updateDefinition(name, content.text, LAKEHOUSE)  # noqa: F821
        return "updated"


for nb in NOTEBOOKS:
    print(f"{deploy(nb):8} {nb}")
