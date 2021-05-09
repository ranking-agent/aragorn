# Testing ARAGORN

ARAGORN is merely a wrapper around an number of other services.  The most stringent testing is therefore of those services. 
Test plans can be found in the /tests directories of the [strider](https://github.com/ranking-agent/strider), 
[aragorn-ranker](https://github.com/ranking-agent/aragorn-ranker), and [answer coalescence](https://github.com/ranking-agent/AnswerCoalesce).

### Test Files

* [`test_aragorn.py`](test_aragorn.py):

  Performs a simple integration test, making sure that ARAGORN is able to reach and correctly call the services on which it depends.



