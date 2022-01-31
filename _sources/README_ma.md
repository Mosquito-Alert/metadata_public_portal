<style>
r { color: Red }
g { color: Green }
</style>
# Table of contents

- [Table of contents](#table-of-contents)
- [1. Introduction](#1-introduction)
- [2. Current state of the art](#2-current-state-of-the-art)
  - [2.1. Findable data](#21-findable-data)
  - [2.2. Accessibility](#22-accessibility)
  - [2.3. Interoperability](#23-interoperability)
  - [2.4. Reusability](#24-reusability)

# 1. Introduction

The scope of this documentation is to provide:

- Metadata description of datasets that are used in the *Big Mosquito Bytes* (BMB) project
- Data access examples by _Python_ scripts

This work aims to implement **FAIR** principles into Mosquito Alerts data management system in order to ease data extraction for partners via API and code-examples.

# 2. Current state of the art

CEAB-CSIC has implemented a data management system based on FAIR principles in order to ease metadata visualization and  data access via API and code-examples. In the following the data management system is presented from a FAIR perspective.

## 2.1. Findable data

Metadata templates are built on [Google's Schema.org metadata standard](https://schema.org/docs/data-and-datasets.html) following the guidelines from [SOSO](https://github.com/ESIPFed/science-on-schema.org) published by the Earth Science Information Partners and the draft document ["DCAT-AP to Schema.org Mapping"](https://ec-jrc.github.io/dcat-ap-to-schema-org/) published by the *European Commission's Joint Research Centre*.

The metadata information relative to a dataset is stored in `.json-ld` files (i.e. JSON for Linking Data) that are validated with Google' [Structured Data Testing Tool](https://search.google.com/structured-data/testing-tool/u/0/) or [Rich Result Test](https://search.google.com/test/rich-results?utm_campaign=sdtt&utm_medium=code&id=zX-8vS_sX8Vir1CdBTMmrg).

Whenever possible, DOI identifiers are used for linking to datasets. Since the management system is uploaded to a GitHub repository, control version of metadata files is inherently performed by tracking changes in any set of `.json-ld` files.

## 2.2. Accessibility

A static website portal was built to ease metadata visualization and dataset access. The static website is hosted free of charge and maintenance costs on GitHub Pages. A [public portal](https://mosquito-alert.github.io/metadata_public_portal/README_ma.html) relative to Mosquito Alert open data is available for general public. For the moment, the full metadata portal is only available on a private repository since it contains not only public but even private datasets.

Every metadata has an associated *Python* script (i.e. jupyter notebook tutorial) that explains how to access the relative dataset distribution. The web-portal is automatically re-generated anytime a new metadata is added.

In the following table are listed all the datasets currently available at the metadata web-portal with the respective licenses and access restrictions. *Private* data is stored in a restricted access disk storage on the CEAB-CSIC computer cluster. Note that if a dataset is labeled as *Private* it does not necessarily imply that it cannot be distributed, since their access may be guaranteed after signing a ad-hoc contract of use.

| Dataset | Project | Description | License  | Example | Format |
| ------- |:-------:| :-----------|:--------:|:-------:|:------:|
| {doc}`reports <../meta_ipynb/reports>` | MosquitoAlert | Yearly reports of bites, breeding sites and adult mosquito encounters. | Public CC0-1.0  | <g>✔</g> | `.json` `.csv` |
| {doc}`tigapics <../meta_ipynb/tigapics>` | MosquitoAlert | Pictures of adult mosquitos and breeding sites. Classification labels are provided for ML training for the pictures of mosquitos visualized on the MosquitoAlert map. | Public CC0-1.0 | <g>✔</g> | `.jpeg` `.png` |
| {doc}`model_tables <../meta_ipynb/model_tables>` | MosquitoAlert | Results from the Bayesian multilevel model that feed the Mosquito Alert raster maps. | Public CC0-1.0 | <g>✔</g> | `.csv` |
| {doc}`sampling_effort <../meta_ipynb/sampling_effort>` | MosquitoAlert| Daily participant counts and sampling effort in 0.025 and 0.05 degree lon/lat sampling cells. | Public CC0-1.0 | <g>✔</g> | `.csv` |
| {doc}`analytic_tables <../meta_ipynb/analytic_tables>` | MosquitoAlert| Tables from Mosquito Alert database selected for analytic purposes. Available on demand for research purposes only since it could contain user sensible information. | Private | <g>✔</g> | `.csv` |

## 2.3. Interoperability

Interoperability of the data is guaranteed through the use of controlled Schema.org vocabularies. In concrete, the requested vocabulary terms used to describe a dataset are: *name*, *description*, *url*, *sameAs*, *identifier*, *license*, *citation*, *creator* (with values: *type*, *id*, *name*, *identifier*, *contactPoint*), variableMeasured (of type Property with values: name, description and unitText, dataType), *variableMeasured* (by type *Property* with values: *name*, *description* and *unitText*, *dataType*), *measurementTechnique*, *temporalCoverage*, *spatialCoverage* (with values: *type*, *name*, *sameAs*) and *distribution* (of type *DataDownload* with values: *name*, *description*, *encodingFormat*, *url*, *contentUrl*, *contentSize*).

## 2.4. Reusability

The degree of reuse permitted when the data are made available to other researchers and the wider public is defined by the relative license or by an *ad-hoc* data use contract usually made for a specific data request.
