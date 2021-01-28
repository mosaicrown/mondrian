# Spark based Mondrian

This repository implements Mondrian, a sanitization algorithm to achieve k-anonimity.
This version of Mondrian is meant to perform sanitization over large datasets, and it is executed by an Apache Spark cluster with a varying number of executors.
The l-diversity refinement is also supported.

The tool relies on Pandas UDFs and Apache Arrow.

## Prerequisites

* [docker](https://docs.docker.com/get-docker/)
* [docker-compose](https://docs.docker.com/compose/install/)

### USA 2018 dataset

We cannot upload the `usa2018.csv` dataset for [licencing reasons](https://ipums.org/about/terms).
However you can get your own copy of the data from [https://usa.ipums.org/usa/](https://usa.ipums.org/usa/).

Our data selection is based on the American Community Servey 2018 sample (2018 ACS) with [STATEFIP](https://usa.ipums.org/usa-action/variables/STATEFIP#description_section), [AGE](https://usa.ipums.org/usa-action/variables/AGE#description_section), [EDUC](https://usa.ipums.org/usa-action/variables/EDUC#description_section),[OCC](https://usa.ipums.org/usa-action/variables/OCC#description_section),[INCTOT](https://usa.ipums.org/usa-action/variables/INCTOT#description_section) variables.

## Run

The Mondrian Docker app can be run with:

```shell
make
```

The default workers number is 4, however it can be set with:

```shell
make WORKERS=16
```

## Web interface
To test the tool with your own datasets you can use our [web interface](./ui/README.md). To run it use:

```shell
make ui
```
The default number of workers available to do your anonymization job is 4, however it can be changed with:
```shell
make ui WORKERS=16
```

## About Mondrian

Mondrian is a sanitization algorithm that ensures a dataset to be compliant with the k-anonimity requirement (i.e., each person in a released dataset cannot be distinguished among k or more individuals).
It is a greedy approximation algorithm, as its partitioning criteria evaluates only the current partition state.
We focus on the strict version of it, or rather the one defyning a set of non-overlapping partitions.

## Workflow

The tool simulates the execution of Mondrian on a cluster of nodes.
The cluster is composed by an Hadoop Namenode and Datanode, plus a Spark driver and a varying number of Spark workers.

The dataset to be anonymized is initially stored on the Datanode.
As execution starts, the dataset is sampled by the Spark Driver, which performs the first Mondrian cut identifying the set of dataset fragments based on a particular score function.
This is crucial to handle datasets too large to fit into the memory of a single worker.
Each fragment is then assigned to a Spark Worker, where Mondrian anonymization is performed.
Before the anonymized fragmets are written to the Datanode, each Worker computes the information loss and send it to the Driver, so that it is possible to determine the global information loss.

Each anonymization job is described with a json configuration file.
It lists the input/output dataset name, classifies the dataset attributes (identifiers, quasi identifiers and sensitive), defines the sampling method, indicates the k-anonimity and l-diversity parameters, the scoring function to be used to determine the cuts, the set of custom generalization methods to be used for quasi identifiers (detailed below), and the information loss metrics to be computed.

Here is an example of configuration file.

```json
{
    "input": "hdfs://namenode:8020/dataset/adults.csv",
    "output": "hdfs://namenode:8020/anonymized/adults.csv",
    "fraction": 1,
    "quasiid_columns": ["age", "education-num", "race", "native-country"],
    "sensitive_columns": ["income"],
    "column_score": "entropy",
    "fragmentation": "quantile",
    "K": 3,
    "L": 3,
    "quasiid_generalizations": [
    {
        "qi_name": "native-country",
        "generalization_type": "categorical",
        "params": {
            "taxonomy_tree": "/mondrian/taxonomy/countries.json"
    }
    },
    {
        "qi_name": "age",
        "generalization_type": "numerical",
        "params": {
            "fanout": 2,
            "accuracy": 2,
            "digits": 3
        }
    },
    {
        "qi_name": "education-num",
        "generalization_type": "common_prefix",
        "params": {
        "hide-mark": "+"
        }
    }
    ],
    "measures": ["discernability_penalty", "global_certainty_penalty"]
}
```

## Functions

The tool supports many functions.

To determine the cut column different functions can be used: span, entropy and negative entropy (to reverse the quasi-identifier order). Partitions cuts are determined using the median.

Multiple generalization methods on quasi-identifiers are available:

* ranges (e.g., [5 - 10]);
* sets (e.g., {Private, Public});
* common prefix (e.g., 100**);
* using numeric partitioning (a balanced tree constructed from the quasi-identifier numerical data based on fanout, accuracy and digits parameters);
* using user-defined taxonomies (a taxonomy tree given by the user that guides the generalization of the quasi-identifier values).

In both numeric partitioning and user-defined taxonomies the lowest common ancestor is used to generalize the set of values.

Three information loss metrics are available:

* Discernability penalty;
* Normalized certainty penalty;
* Global certainty penalty.

## Centralized version of Mondrian

The centralized version of Mondrian is not based on Apache Spark, so it can be run without Docker.
It is contained in the local folder and can be used to anonymize limited size datasets.

The centralized version only relies on pandas and numpy, and it is particularly useful to demonstrate the scalability of Mondrian over large datasets.
A demo of it can be run with:

```shell
make local-adults
```
