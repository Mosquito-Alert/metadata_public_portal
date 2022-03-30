# Notes

Inside _notebooks_ folder are available examples that show how to access to datasets described in metadata. The workflow to build the examples is the following:
  * Create/Change _.py_ files with percentage cell format
  * Transform _.py_ files to jupyter notebook format _.ipynb_ by running:
    ```shell
    $ jupytext --update --to notebook ./notebooks/*.py
    ```
  * Execute the notebooks and build _.html_  for the static web-page docs

## FTP

* This utility is based on [parmiko](http://docs.paramiko.org/en/stable/) Python library.

## _Senscape_

* Code is a copy-paste from the [senscape](https://gitlab.com/ukojz/senscape/-/blob/master/tools.py) GitLab. Thus any change in the source code should be manually copied into _senscape.py_ if relevant.