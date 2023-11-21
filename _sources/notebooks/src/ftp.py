"""Functions for working with remote files using pandas and paramiko (SFTP/SSH)."""
import pandas as pd
import paramiko


def read_csv_sftp(
    hostname: str,
    port: int,
    username: str,
    password: str,
    remotepath: str,
    *args,
    **kwargs
) -> pd.DataFrame:
    """
    Read a file from a remote host using SFTP over SSH.
    Args:
        hostname: the remote host to read the file from
        username: the username to login to the remote host with
        remotepath: the path of the remote file to read
        *args: positional arguments to pass to pd.read_csv
        **kwargs: keyword arguments to pass to pd.read_csv
    Returns:
        a pandas DataFrame with data loaded from the remote host
    """
    # open an SSH connection
    client = paramiko.SSHClient()
    # client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.load_system_host_keys()
    client.connect(hostname=hostname, port=port, username=username, password=password)
    # read the file using SFTP
    sftp = client.open_sftp()
    remote_file = sftp.open(remotepath)
    remote_file.prefetch()  # speed-up the read on the remote server
    dataframe = pd.read_csv(remote_file, *args, **kwargs)
    remote_file.close()
    # close the connections
    sftp.close()
    client.close()

    return dataframe
