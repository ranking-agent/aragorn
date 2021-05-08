# ARAGORN

### Autonomous Relay Agent for Generation Of Ranked Networks (ARAGORN)

A tool to query Knowledge Providers (KPs) and synthesize highly ranked answers relevant to user-specified questions.

* Operates in a federated knowledge environment.
* Bridges the precision mismatch between data specificity in KPs and more abstract levels of user queries.
* Generalizes answer ranking.
* Normalizes data to use preferred and equivalent identifiers.  


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
