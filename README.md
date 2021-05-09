# ARAGORN

### Autonomous Relay Agent for Generation Of Ranked Networks (ARAGORN)

A tool to query Knowledge Providers (KPs) and synthesize highly ranked answers relevant to user-specified questions.

* Operates in a federated knowledge environment.
* Bridges the precision mismatch between data specificity in KPs and more abstract levels of user queries.
* Generalizes answer ranking.
* Normalizes data to use preferred and equivalent identifiers.  

The ARAGORN tool relies on a number of external services to perform a standardized ranking of a user-specified question.

 - Strider - Accepts a query and provides knowledge-provider querying, answer generation and ranking.
 - Answer Coalesce - Accepts a query containing Strider answers and returns answers that have been coalesced by property, graph and/or ontology analysis.
 - Node normalization - Accepts a query containing coalesced answers and provides the preferred CURIE and equivalent identifiers for data in the query.
 - ARAGORN Ranker - Accepts a query and provides Omnicorp overlays, score and weight-correctness rankings of coalesced answers.

## Demonstration

A live version of the API can be found [here](https://aragorn.renci.org/docs).

This version of ARAGORN has all links to subordinate services hard coded. Some adjustment will be needed here to support your installation.

## Source Code
Below you will find references that detail the standards, web services and supporting tools that are part of ARAGORN. 

* [Strider](https://github.com/ranking-agent/strider)
* [Answer Coalescence](https://github.com/ranking-agent/AnswerCoalesce)
* [Node normalization](https://github.com/TranslatorSRI/NodeNormalization)
* [ARAGORN ranker](https://github.com/ranking-agent/aragorn-ranker)

### Aditional resources
* [KP Registry](https://github.com/ranking-agent/kp_registry)
* [Reasoner (TRAPI->cypher transpiler)](https://github.com/ranking-agent/reasoner)
* [ReasonerAPI](https://github.com/NCATSTranslator/ReasonerAPI)  
* [ReasonerStdAPI Message Jupyter Notebook visualizer](https://github.com/ranking-agent/gamma-viewer)

## Installation

### Subordinate services
The ARAGORN subordinate services will have to be deployed prior to the stand-up of ARAGRON. Please reference the following READMEs for more information on standing those up:
* [Strider readme](https://github.com/ranking-agent/strider#readme)
* [Answer Coalescence readme](https://github.com/ranking-agent/AnswerCoalesce#readme)
* [Node normalization readme](https://github.com/TranslatorSRI/NodeNormalization#readme)
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
