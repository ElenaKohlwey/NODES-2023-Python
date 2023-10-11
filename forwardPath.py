from neo4j import GraphDatabase
import time

class ForwardPath:
    """
    This class implements a forward Path through an Action graph.
    """

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_forward_path(self):
        """
        This procedure iterates through the graph from Start to End
        calculating the earliest start and earliest finish times.
        Cp. https://www.pmcalculators.com/how-to-calculate-the-critical-path/,
        Example 1, Forward Path.
        """
        with self.driver.session() as session:
            session.execute_write(self._initialize_start_and_starting_nodes)
            count_processed_nodes = 1

            while count_processed_nodes > 0:
                count_processed_nodes = session.execute_write(self._process_current_nodes)

    @staticmethod
    def _initialize_start_and_starting_nodes(tx):
        """
        This procedure initializes the start node by setting its
        earliestStart and earliestFinish properties to 0.
        """
        tx.run("""
               MATCH (a:Action {name: 'Action0'})-[:PRECEDES]->(b:Action)
               SET a.earliestStart = 0, a.earliestFinish = 0
               """)

    @staticmethod
    def _process_current_nodes(tx):
        """
        This procedure processes all current nodes by setting its
        earliestStart and earliestFinish properties.
        The earliestStart is set to the maximum of all predecessor
        earliestFinish properties.
        The earliestFinish is set to the sum of earliestStart and duration.
        @return: returns the number of processed nodes.
        """
        result = tx.run("""
                        MATCH (a:Action)-[:PRECEDES]->(b:Action)
                        WITH COLLECT(a) AS predecessors, b, max(a.earliestFinish) as maxEarliestFinish
                        WHERE b.earliestStart IS NULL
                        AND all(x in predecessors WHERE x.earliestFinish IS NOT NULL)
                        SET b.earliestStart = maxEarliestFinish
                        WITH b
                        SET b.earliestFinish = b.earliestStart + b.duration
                        RETURN COUNT(b)
                        """)
        return result.single()[0]

if __name__ == "__main__":
    algo = ForwardPath("bolt://localhost:7687", "neo4j", "testtest")
    start = time.time()
    algo.execute_forward_path()
    end = time.time()
    print("The execution took %.2f seconds." %(end-start))
    algo.close()