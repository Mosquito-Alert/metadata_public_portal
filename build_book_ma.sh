#!/bin/bash

set -e
source config.sh

conda activate metadata

# Remove the old version of the website
if [ -d "./_build" ] 
then
    echo "Remove old website version"
    rm -r _build 
fi

# Build the jupyter book
jupyter book build $PATH_DEV/metadata/ --path-output $PATH_DEV/metadata_book/ --toc $PATH_DEV/metadata_book/_toc.yml

# Add schema.org metadata to header
python $PATH_DEV/metadata/json2head.py

# Modify generated metadata tables (collapse, style, etc.)
python $PATH_DEV/metadata/meta_collapse.py

# Add tutorial videos to _static folder
cp -r $PATH_DEV/metadata/docs/videos ./_build/html/_static

# Add google website index verification file 
cp googled194e0c154256c20.html ./_build/html

# Add conda requirementes, notebooks and utility scripts to build and use Binder
cp environment.yml ./_build/html
cp -r $PATH_DEV/metadata/notebooks/src ./_build/html/_sources/notebooks