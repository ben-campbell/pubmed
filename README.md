# pubmed

This project analyzes the citation network of 30 million+ [Pubmed](https://pubmed.ncbi.nlm.nih.gov/) articles. The XML entries for the articles are [downloaded](ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/), parsed, and the citation information is ingested into a graph database, [Neo4j](http://neo4j.com).

Subsequent network analysis of the graph to follow. 

## Files

| file | description |
|------|-------------|
| pubmed_data.py | downloads, parses, and ingests data into Neo4j |
| logging.conf | the configuration file for the pubmed_data.py logger|
| requirements.txt | lists the Python prerequisite modules, to install via ```pip3 install requirements.txt``` |
