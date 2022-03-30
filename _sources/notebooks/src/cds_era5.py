# %%
import os, requests
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import cdsapi
import xarray as xr


def mask_file(mask: xr.DataArray, filepath: str):
    """
    Apply masking on a given netCDF-file.
        
        Args:
        * mask: mask to apply.
        * filepath: filename-path of the netCDF-file on which to apply masking.
    """

    ds = xr.open_dataset(
        filepath,
        mask_and_scale=False,  # Import preserving INT16 dtype
        chunks=None,  # Don't use Dask since mask is small-size
    )

    # Mask preserving INT dtype (if other=NaN then dtype changes to FLOAT64)
    ds_mask = ds.where(mask == True, other=-32767)

    encoding = dict()
    for var in list(ds.keys()):
        encoding[var] = {"zlib": True}

    # Add prefix to filename and save masked netCDF file
    filename = os.path.basename(filepath)
    dirname = os.path.dirname(filepath)
    ds_mask.to_netcdf(f"{dirname}/masked_{filename}", encoding=encoding)

    return True


def download_file(
    api_request: dict,
    name: str,
    url: Optional[str] = None,
    key: Optional[str] = None,
    path: str = ".",
    mask: xr.DataArray = None,
    mask_remove: bool = True,
):
    """
    Download era5 dataset with CDS-API client and apply masking for size
    reduction if a mask is provided

    Args:
        * api_request: key-value specific to the CDS-API format.
        * name: dataset name specific to the CDS-API.
        * url: CDS api url.
        * key: API key given with CDS account.
        * path: filename-path where to store the downloaded netCDF-file.
        * mask: xarray mask where False values represent the masking.
        * mask_remove: remove original downloaded netCDF-files after masking.
    """

    try:
        client = cdsapi.Client(url=url, key=key, verify=1, quiet=True)
        var = api_request["variable"]
        d = api_request["date"]
        filename = f"{var}_t_{d}.nc"
        filepath = f"{path}/{filename}"
        res = client.retrieve(name, api_request, filepath)
        state = res.reply["state"]
        info_download = f"{filename} -> download: {state}"
        print(info_download)

        if mask is not None:
            try:
                res = mask_file(mask, filepath)
                info_mask = f"{filename} -> masked: OK!"
                if res and mask_remove:
                    os.remove(filepath)
                print(info_mask)
            except Exception as e:
                return e

    except requests.exceptions.RequestException as e:
        return e


def request_thread(
    api_request_list: list,
    name: str,
    url: Optional[str] = None,
    key: Optional[str] = None,
    path: str = ".",
    mask: xr.DataArray = None,
    max_workers=None,
):
    """
    Threaded request of the CDS-API.

    Args:
        * api_request_list: list of key-value specific to the CDS-API format.
        * name: dataset name specific to the CDS-API.
        * url_api: CDS api url.
        * key_api: API key given with CDS account.
        * path: filename-path where to store the downloaded netCDF-file.
        * mask: xarray mask where False values represent the masking.
        * max_workers:  number of threads to run. If None it will default to the
                        number of processors on the machine, multiplied by 5.
    """

    threads = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for api_request in api_request_list:
            threads.append(
                executor.submit(download_file, api_request, name, url, key, path, mask)
            )


def days_of_month(year: int, month: int) -> list:
    """
    Makes a list of datetimes for a given month and year.

    Args:
        * year: relative year
        * month: relative month

    Return:
        * List of datetimes in %Y-%m-%d format for a given year and month.
    """
    d0 = datetime(year, month, 1)
    d1 = d0 + relativedelta(months=1)
    out = list()
    while d0 < d1:
        out.append(d0.strftime("%Y-%m-%d"))
        d0 += timedelta(days=1)

    return out
