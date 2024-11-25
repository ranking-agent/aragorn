# ARAGORN

### Autonomous Relay Agent for Generation Of Ranked Networks (ARAGORN)

A tool to query Knowledge Providers (KPs) and synthesize highly ranked answers relevant to user-specified questions.

* Operates in a federated knowledge environment.
* Bridges the precision mismatch between data specificity in KPs and more abstract levels of user queries.
* Generalizes answer ranking.
* Normalizes data to use preferred and equivalent identifiers.  
* Uses mined rules to perform inference and provide predicted answers for certain queries.
* Provides a way to run pathfinder queries, using literature co-occurrence to find multi-hop paths between any two nodes.

The ARAGORN tool relies on a number of external services to perform a standardized ranking of a user-specified question.

 - Strider - Accepts a query and provides knowledge-provider querying, answer generation and ranking.
 - Node normalization - A Translator SRI service that provides the preferred CURIE and equivalent identifiers for data in the query.
 - ARAGORN Ranker - Accepts a query and provides Omnicorp overlays and scores of answers.

## Demonstration

A live version of the API can be found [here](https://aragorn.renci.org/docs).

## Source Code
Below you will find references that detail the standards, web services and supporting tools that are part of ARAGORN. 

* [Strider](https://github.com/ranking-agent/strider)
* [Answer Coalescence](https://github.com/ranking-agent/AnswerCoalesce)
* [ARAGORN ranker](https://github.com/ranking-agent/aragorn-ranker)

## Installation

This version of ARAGORN has all links to subordinate services hard coded. In the future, these links will be defined in the Kubernetes configuration files. 

In the meantime some manual edits will be needed in the src/service_aggregator.py file to support your installation.

### Subordinate services
The ARAGORN subordinate services will have to be deployed prior to the stand-up of ARAGRON. Please reference the following READMEs for more information on standing those up:
* [Strider readme](https://github.com/ranking-agent/strider#readme)
* [Answer Coalescence readme](https://github.com/ranking-agent/AnswerCoalesce#readme)
* [ARAGORN ranker readme](https://github.com/ranking-agent/aragorn-ranker#readme)

### Command line installation

    cd <aragorn codebase root>

    python<version> -m venv venv
    source venv/bin/activate
    
#### Install dependencies

    pip install -r requirements.txt

#### Run Script
  
    cd <aragorn root>

    ./main.sh
    
### DOCKER installation
   Or build an image and run it.

    cd <aragorn root>

    docker build --tag <image_tag> .

   Then start the container

    docker run --name aragorn -p 8080:4868 aragorn-test

### Kubernetes configurations

Kubernetes configurations and helm charts for this project can be found at: 

    https://github.com/helxplatform/translator-devops/helm/aragorn
