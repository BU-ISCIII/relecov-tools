from owlready2 import *
import re


class SparqlQueries:
    def __init__(self):
        my_world = World()
        my_world.get_ontology(
            "file://ExampleOntolohy.owl"
        ).load()  # path to the owl file is given here
        sync_reasoner(my_world)  # reasoner is started and synchronized here
        self.graph = my_world.as_rdflib_graph()

    def search(self):
        # Search query is given here
        # Base URL of your ontology has to be given here
        query = (
            "base <http://www.semanticweb.org/ExampleOntology> "
            "SELECT ?s ?p ?o "
            "WHERE { "
            "?s ?p ?o . "
            "}"
        )

        # query is being run
        resultsList = self.graph.query(query)

        # creating json object
        response = []
        for item in resultsList:
            s = str(item["s"].toPython())
            s = re.sub(r".*#", "", s)

            p = str(item["p"].toPython())
            p = re.sub(r".*#", "", p)

            o = str(item["o"].toPython())
            o = re.sub(r".*#", "", o)
            response.append({"s": s, "p": p, "o": o})

        print(response)  # just to show the output
        return response

    runQuery = SparqlQueries()
    runQuery.search()
