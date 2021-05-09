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

## Source Code
Below you will find references that detail the standards, web services and supporting tools that are part of ARAGORN. 

* [Answer Coalescence](https://github.com/ranking-agent/AnswerCoalesce)
* [ARAGORN](https://github.com/ranking-agent/aragorn)
* [KP Registry](https://github.com/ranking-agent/kp_registry)
* [Node normalization](https://github.com/TranslatorSRI/NodeNormalization )
* [ARAGORN ranker](https://github.com/ranking-agent/aragorn-ranker)
* [Reasoner (TRAPI->cypher transpiler)](https://github.com/ranking-agent/reasoner)
* [ReasonerAPI](https://github.com/NCATSTranslator/ReasonerAPI)  
* [ReasonerStdAPI Message Jupyter Notebook visualizer](https://github.com/ranking-agent/gamma-viewer)
* [Strider](https://github.com/ranking-agent/strider)

### Installation

To run the web server directly:

#### Create a virtual Environment and activate.

    cd <aragorn codebase root>

    python<version> -m venv venv
    source venv/bin/activate
    
#### Install dependencies

    pip install -r requirements.txt

#### Run Script
  
    cd <aragorn root>

    ./main.sh
    
 ### DOCKER 
   Or build an image and run it.

    cd <aragorn root>

    docker build --tag <image_tag> .

   Then start the container

    docker run --name aragorn -p 8080:4868 aragorn-test

### Kubernetes configurations

Kubernetes configurations and helm charts for this project can be found at: 

    https://github.com/helxplatform/translator-devops/helm/aragorn
