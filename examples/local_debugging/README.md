# Local debugging

This example shows how to run the [Titanic example](../titanic/README.md) locally to debug your code.

## Prerequisites

In order to run this example, you'll need to:

* use Python 3
* have [Docker](https://www.docker.com/) installed
* [install `substra`](../../README.md#install)
* checkout this repository

All commands in this example are run from the `substra/examples/local_debugging/` folder.

## Data and script preparation

Follow the [data preparation phase](../titanic/README.md#data-preparation) instructions from the Titanic example.

The objective, data manager and algorithms that we use are defined in the Titanic example.

## Run and debug our pipeline

The [local_debugging.py](./scripts/local_debugging.py) contains the code to add the assets to the platform, 
train the algorithm and make predictions.

In case the script is run on the 'remote' backend, the displayed performance is zero since the script ends 
before the asynchronous execution.

