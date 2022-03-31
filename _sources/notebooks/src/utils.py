import json, os, shutil, requests, subprocess
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import os.path


def load_metadata(meta_filename):
    """
    Loads the metadata file from json or from the header of the metadata table
    """

    if isinstance(meta_filename, list):
        if len(meta_filename) == 2:
            try:
                with open(meta_filename[0], "r") as f:
                    return json.loads(f.read())
            except IOError:
                with open(meta_filename[1], "r") as f:
                    soup = BeautifulSoup(f.read(), "html.parser")
                    return json.loads(
                        soup.find("script", type="application/ld+json").text
                    )
    else:
        with open(meta_filename, "r") as f:
            return json.loads(f.read())


def download_file(url, filename, method="curl"):
    """
    Download a file to disk given an http url
    """

    if method == "requests":
        # Allows for download in chunks (no checksum verification)
        with requests.get(url, stream=True) as r:
            with open(filename, "wb") as f:
                shutil.copyfileobj(r.raw, f, length=16 * 1024 * 1024)

    if method == "curl":
        subprocess.call(["curl", "–o", filename, url], shell=False)


def makedirs(path: str):
    """
    Make the folders if they do not exist
    Args:
        * path: full path of the new folder to make
    """
    # Check if folder exists
    if not os.path.exists(path):
        os.makedirs(path)


def project_path(i: str = 0):
    """
    Get the project's path of a python file
    """
    abspath = os.path.abspath("")
    if i == 0:
        return abspath
    else:
        return "/".join(abspath.split("/")[slice(0, -i)])


def get_meta(
    meta,
    idx_distribution: int = 0,
    idx_hasPart: int = None,
    parse: bool = False,
):
    """Get the contentUrl field from the metadata json files.

    Args:
        * meta: metadata file in json format
        * idx_distribution: list index of the distribution key
        * idx_hasPart: list index of the hasPart if DataSet has parts
        * parse: parse the url if required (e.g. ftp, db connection)

    Returns:
        * contentUrl field of a selected DataSet distribution option.
        * name of the DataSet relative to the selected distribution.
        * name of the relative distribution method.
    """

    if isinstance(idx_hasPart, int):
        try:
            meta_part = meta["hasPart"][idx_hasPart]
        except KeyError:
            print("Warning: Dataset do not have hasPart key.")
        meta_distr = meta_part["distribution"]
        meta_name = meta_part["name"]
        meta_descr = meta_part["description"]
        if isinstance(meta_distr, list):
            meta_distr = meta_distr[idx_distribution]
    else:
        meta_distr = meta["distribution"]
        meta_name = meta["name"]
        meta_descr = meta["description"]
        if isinstance(meta_distr, list):
            meta_distr = meta_distr[idx_distribution]

    print(
        "Info\n"
        f"dataset name: {meta_name}\n"
        f'distribution name: {meta_distr["name"]}\n'
        f'distribution description: {meta_distr["description"]}\n'
    )

    if parse:
        if isinstance(meta_distr["contentUrl"], list):
            contentUrl = [urlparse(url) for url in meta_distr["contentUrl"]]
        else:
            contentUrl = urlparse(meta_distr["contentUrl"])
    else:
        contentUrl = meta_distr["contentUrl"]
        if isinstance(contentUrl, list):
            if len(contentUrl) == 1:
                contentUrl = contentUrl[0]

    return contentUrl, meta_name, meta_distr["name"]


def extract_element_from_json(obj, path):
    """
    Extracts an element from a nested dictionary or
    a list of nested dictionaries along a specified path.
    If the input is a dictionary, a list is returned.
    If the input is a list of dictionary, a list of lists is returned.

    Args:
        * obj [list or dict] Input dictionary or list of dictionaries.
        * path [list] List of strings that form the path to the desired element.
    """

    def extract(obj, path, ind, arr):
        """
        Extracts an element from a nested dictionary
        along a specified path and returns a list.

        Args:
            * obj [dict] Input dictionary.
            * path [list] List of strings that form the JSON path.
            * ind [int] Starting index.
            * arr [list] Output list.
        """
        key = path[ind]
        if ind + 1 < len(path):
            if isinstance(obj, dict):
                if key in obj.keys():
                    extract(obj.get(key), path, ind + 1, arr)
                else:
                    arr.append(None)
            elif isinstance(obj, list):
                if not obj:
                    arr.append(None)
                else:
                    for item in obj:
                        extract(item, path, ind, arr)
            else:
                arr.append(None)
        if ind + 1 == len(path):
            if isinstance(obj, list):
                if not obj:
                    arr.append(None)
                else:
                    for item in obj:
                        arr.append(item.get(key, None))
            elif isinstance(obj, dict):
                arr.append(obj.get(key, None))
            else:
                arr.append(None)
        return arr

    if isinstance(obj, dict):
        return extract(obj, path, 0, [])
    elif isinstance(obj, list):
        outer_arr = []
        for item in obj:
            outer_arr.append(extract(item, path, 0, []))
        return outer_arr


def info_meta(meta):
    """
    Get general information from the metadata catalog file.

    Args:
        *meta: metadata json file.
    """

    meta_name = extract_element_from_json(meta, ["name"])[0]
    print(f"Metadata name: {meta_name}")

    if "hasPart" in meta.keys():
        part_names = extract_element_from_json(meta, ["hasPart", "name"])

        for i, part_name in enumerate(part_names):
            distr_names = extract_element_from_json(
                meta["hasPart"][i], ["distribution", "name"]
            )
            print(f"\nhasPart {i}: {part_name}\n")
            if isinstance(distr_names, list):
                for j, distr_name in enumerate(distr_names):
                    print(f"\tdistribution {j}: {distr_name}")
            else:
                print(f"\tdistribution 0: {distr_names}")

    else:
        distr_names = extract_element_from_json(meta, ["distribution", "name"])

        if isinstance(distr_names, list):
            for j, distr_name in enumerate(distr_names):
                print(f"\tdistribution {j}: {distr_name}")
        else:
            print(f"\tdistribution 0: {distr_names}")


def set_ipynb_tag(notebooks: str):

    import nbformat as nbf

    """
    Appends ipynb metadata tags from code comments.

    Args:
        * notebooks: filename path of jupyter notebook
    """
    # Text to look for in adding tags
    text_search_dict = {
        "# HIDDEN": "remove-cell",  # Remove the whole cell
        "# NO CODE": "remove-input",  # Remove only the input
        "# HIDE CODE": "hide-input",  # Hide the input w/ a button to show
        "# FULL WIDTH": "full-width",  # Take up all of the horizontal space
        "# PARAMETERS": "parameters",  # Set as parameter for papermill execution
    }

    # Search through each notebook and look for the text, add a tag if necessary
    for ipath in notebooks:
        ntbk = nbf.read(ipath, nbf.NO_CONVERT)

        for cell in ntbk.cells:
            cell_tags = cell.get("metadata", {}).get("tags", [])
            for key, val in text_search_dict.items():
                if key in cell["source"]:
                    if val not in cell_tags:
                        cell_tags.append(val)
            if len(cell_tags) > 0:
                cell["metadata"]["tags"] = cell_tags

        nbf.write(ntbk, ipath)
