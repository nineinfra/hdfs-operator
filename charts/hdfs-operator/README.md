# Helm Chart for Hdfs Operator for Apache Hdfs

This Helm Chart can be used to install Custom Resource Definitions and the Operator for Apache Hdfs provided by Nineinfra.

## Requirements

- Create a [Kubernetes Cluster](../Readme.md)
- Install [Helm](https://helm.sh/docs/intro/install/)

## Install the Hdfs Operator for Apache Hdfs

```bash
# From the root of the operator repository

helm install hdfs-operator charts/hdfs-operator
```

## Usage of the CRDs

The usage of this operator and its CRDs is described in the [documentation](https://github.com/nineinfra/hdfs-operator/blob/main/README.md).

## Links

https://github.com/nineinfra/hdfs-operator
