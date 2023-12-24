# Scalable Distributed Data Anonymization for Large Datasets

k-anonymity and l-diversity are two well-known privacy metrics that guarantee
protection of the respondents of a dataset by obfuscating information that can
disclose their identities and sensitive information.

This repository provides an extension of the Mondrian algorithm (an efficient
and effective approach designed for achieving k-anonymity) for enforcing both
k-anonimity and l-diversity over large datasets in a distributed manner,
leveraging the Apache Spark framework.

## Prerequisites

* [docker](https://docs.docker.com/get-docker/)
* [docker compose](https://docs.docker.com/compose/install/)

### USA 2018 dataset

> DISCLAIMER: The steps in this subsection are necessary only if you want to
> reproduce the results shown in [1]. Otherwise, by default another version of
> the USA 2018 dataset will be downloaded.

We cannot upload the `usa2018.csv` dataset for [licencing reasons](https://ipums.org/about/terms).
However you can get your own copy of the data from [https://usa.ipums.org/usa/](https://usa.ipums.org/usa/).

Our data selection is based on the American Community Servey 2018 sample (2018 ACS) with [STATEFIP](https://usa.ipums.org/usa-action/variables/STATEFIP#description_section), [AGE](https://usa.ipums.org/usa-action/variables/AGE#description_section), [EDUC](https://usa.ipums.org/usa-action/variables/EDUC#description_section),[OCC](https://usa.ipums.org/usa-action/variables/OCC#description_section),[INCTOT](https://usa.ipums.org/usa-action/variables/INCTOT#description_section) variables.

## Run

To run the distributed anonymization tool over a toy example dataset:

```shell
make
```

This defaults to reading data from one datanode and running the anonymization
algorithm with four worker nodes, however the scale of the cluster can be
customized both in the number of datanodes and worker nodes with:

```shell
make DATANODES=4 WORKERS=16
```

Example runs over more realistic datasets are also there by using the
`transactions`, `usa1990`, `usa2018`, and `usa2019` targets.

## Web interface

To test the tool with your own datasets you can use our [web interface](./ui/README.md). To run it use:

```shell
make ui
```

Again, the default behavior is to deploy a cluster built of one datanode and
four worker nodes, but the number can easily be changed with:

```shell
make ui DATANODES=4 WORKERS=16
```

## About Mondrian

Mondrian is an efficient and effective approach originally proposed for
achieving k-anonymity in a centralized scenario with the goal of making sure
data of each person in a given dataset cannot be distinguished among k or more
other individuals.

Since the identification of the optimal transformation of the dataset (with
respect to information loss) is a complex problem which is not practical to
solve, Mondrian provides a greedy approximation of the solution by making a
series of local partitioning decisions.

There are two versions of the algorithm depending on whether the partitions
are non-overlapping or overlapping. Our solution implements the 'strict'
version of the algorithm where the partitions do not overlap.

## Workflow

The anonymization tool executes Mondrian over a cluster composed of an Hadoop
Namenode and one/more Datanodes, a Spark master and a varying number of Spark
workers.

The dataset target of the anonymization is initially stored on the Hadoop
Distributed File System (HDFS), and thus partitioned and replicated across the
datanodes. Then, the Spark driver reads/samples the dataset and performs the
first Mondrian cuts identifying the set of dataset fragments according to the
scoring function of choise. Sampling the dataset in this early stage of the
anonymization process is crucial to handle datasets too large to fit into the
memory of the Spark driver. Once the dataset fragments have been identified,
each fragment is assigned to a Spark worker, where the Mondrian anonymization
is performed without need to exchange data with others. Finally, just before
the anonymized dataset is written to HDFS, each worker computes the information
loss and send it to the driver, so it is possible to determine a rough extimate
of the quality of the anonymization.

Each anonymization job is described with a json configuration file. It includes
the input and output paths, classifies the dataset attributes in identifiers,
quasi-identifiers and sensitive attrubutes, indicates the k-anonimity and
l-diversity parameters, the scoring function to determine the cuts, the set of
custom generalization methods for quasi-identifiers (if any), and the list of
information loss metrics to compute. But, it also specifies information about
the initial fragmentation of the dataset: the fraction of the dataset read,
the fragmentation strategy (i.e., mondrian, quantile), and whether it is run
in the driver or distributed over the workers.

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

## Configurations

The tool supports many different configurations.

The partitioning of the dataset across the workers is configurable by
specifying the number of fragments (by default equal to the number of workers),
the strategy to identify the fragments (i.e., mondrian, quantile), whether it
is run in the driver or distributed over the workers, and how this partitions
should be translated in Spark partitions.

To determine the cut column different functions can be used: entropy, negative
entropy (to reverse the order), normalized span, and span. On the other hand,
the specific partitioning criteria for a given column depends on whether the
column values have an ordering or not. When the values have a global order, the
partitioning value is decided by the median, otherwise binpacking is used to
determine a balanced partitioning.

Multiple generalization methods on quasi-identifiers are available:

* ranges (e.g., [5 - 10])
* sets (e.g., {Private, Public})
* common prefix (e.g., 100**)
* numeric partitioning (a balanced tree constructed from the quasi-identifier
  numerical data based on fanout, accuracy and digits parameters)
* user-defined taxonomies (a taxonomy tree given by the user that guides the
  generalization of the quasi-identifier values)

In both numeric partitioning and user-defined taxonomies the lowest common
ancestor is used to generalize the set of values.

Three information loss metrics are available:

* discernability penalty
* normalized certainty penalty
* global certainty penalty

## Centralized version of Mondrian

To provide a comparison with the distributed approach, we also implemented a
centralized version of Mondrian. This implementation can be found in the local
folder and can be used to anonymize datasets of limited size.

The centralized version heavily relies on Pandas and NumPy, and we use it as a
baseline to demonstrate the scalability of the distributed implementation of
Mondrian over large datasets.

To run this version over the same toy example dataset above use:

```shell
make local-adults
```

## Publications

* [1] Sabrina De Capitani di Vimercati, Dario Facchinetti, Sara Foresti,
  Gianluca Oldani, Stefano Paraboschi, Matthew Rossi, Pierangela Samarati,
  **Scalable Distributed Anonymization Processing of Sensors Data**,
  in *Proceedings of the 19th IEEE International Conference on Pervasive
  Computing and Communications (PerCom)*, March 22-26, 2021
* [2] Sabrina De Capitani di Vimercati, Dario Facchinetti, Sara Foresti,
  Gianluca Oldani, Stefano Paraboschi, Matthew Rossi, Pierangela Samarati,
  **Artifact: Scalable Distributed Anonymization Processing of Sensors Data**,
  in *Proceedings of the 19th IEEE International Conference on Pervasive
  Computing and Communications (PerCom)*, March 22-26, 2021
* [3] Sabrina De Capitani di Vimercati, Dario Facchinetti, Sara Foresti,
  Giovanni Livraga, Gianluca Oldani, Stefano Paraboschi, Matthew Rossi,
  Pierangela Samarati, **Scalable Distributed Data Anonymization for Large
  Datasets**, in *IEEE Transactions on Big Data (TBD)*, September 19, 2022
