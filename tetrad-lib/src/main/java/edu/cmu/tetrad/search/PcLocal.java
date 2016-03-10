///////////////////////////////////////////////////////////////////////////////
// For information as to what this class does, see the Javadoc, below.       //
// Copyright (C) 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006,       //
// 2007, 2008, 2009, 2010, 2014, 2015 by Peter Spirtes, Richard Scheines, Joseph   //
// Ramsey, and Clark Glymour.                                                //
//                                                                           //
// This program is free software; you can redistribute it and/or modify      //
// it under the terms of the GNU General Public License as published by      //
// the Free Software Foundation; either version 2 of the License, or         //
// (at your option) any later version.                                       //
//                                                                           //
// This program is distributed in the hope that it will be useful,           //
// but WITHOUT ANY WARRANTY; without even the implied warranty of            //
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the             //
// GNU General Public License for more details.                              //
//                                                                           //
// You should have received a copy of the GNU General Public License         //
// along with this program; if not, write to the Free Software               //
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA //
///////////////////////////////////////////////////////////////////////////////

package edu.cmu.tetrad.search;

import edu.cmu.tetrad.data.IKnowledge;
import edu.cmu.tetrad.data.Knowledge2;
import edu.cmu.tetrad.graph.*;
import edu.cmu.tetrad.util.ChoiceGenerator;
import edu.cmu.tetrad.util.DepthChoiceGenerator;
import edu.cmu.tetrad.util.TetradLogger;

import java.util.*;

/**
 * Implements the PC Local algorithm.
 *
 * @author Joseph Ramsey (this version).
 */
public class PcLocal implements GraphSearch {

    /**
     * The independence test used for the PC search.
     */
    private IndependenceTest independenceTest;

    /**
     * Forbidden and required edges for the search.
     */
    private IKnowledge knowledge = new Knowledge2();

    /**
     * True if cycles are to be aggressively prevented. May be expensive for large graphs (but also useful for large
     * graphs).
     */
    private boolean aggressivelyPreventCycles = false;

    /**
     * The logger for this class. The config needs to be set.
     */
    private TetradLogger logger = TetradLogger.getInstance();

    /**
     * Elapsed time of the most recent search.
     */
    private long elapsedTime;

    private Graph graph;
    private MeekRules meekRules;
    private boolean recordSepsets;
    private SepsetMap sepsetMap = new SepsetMap();

    //=============================CONSTRUCTORS==========================//

    /**
     * Constructs a PC Local search with the given independence oracle.
     */
    public PcLocal(IndependenceTest independenceTest, Graph graph) {
        if (independenceTest == null) {
            throw new NullPointerException();
        }

        this.independenceTest = independenceTest;
        this.graph = GraphUtils.undirectedGraph(graph);
    }

    public PcLocal(IndependenceTest independenceTest) {
        if (independenceTest == null) {
            throw new NullPointerException();
        }

        this.independenceTest = independenceTest;
        this.graph = null;
    }

    //==============================PUBLIC METHODS========================//

    public boolean isAggressivelyPreventCycles() {
        return this.aggressivelyPreventCycles;
    }

    public void setAggressivelyPreventCycles(boolean aggressivelyPreventCycles) {
        this.aggressivelyPreventCycles = aggressivelyPreventCycles;
    }


    public IndependenceTest getIndependenceTest() {
        return independenceTest;
    }

    public IKnowledge getKnowledge() {
        return knowledge;
    }

    public void setKnowledge(IKnowledge knowledge) {
        if (knowledge == null) {
            throw new NullPointerException();
        }

        this.knowledge = knowledge;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    /**
     * Runs PC starting with a fully connected graph over all of the variables in the domain of the independence test.
     */
    public Graph search() {
        long time1 = System.currentTimeMillis();

        if (graph == null) {
            graph = new EdgeListGraph(getIndependenceTest().getVariables());
        }
        meekRules = new MeekRules();
        meekRules.setAggressivelyPreventCycles(isAggressivelyPreventCycles());
        meekRules.setKnowledge(knowledge);
        meekRules.setUndirectUnforcedEdges(true);


        // This is the list of all changed nodes from the last iteration
        List<Node> nodes = graph.getNodes();

        Graph outGraph = null;

        int numEdges = nodes.size() * (nodes.size() - 1) / 2;
        int index = 0;

        iteration(nodes, numEdges, index);

        outGraph = graph;

        this.logger.log("graph", "\nReturning this graph: " + graph);

        long time2 = System.currentTimeMillis();
        this.elapsedTime = time2 - time1;

        return outGraph;
    }

    private void iteration(List<Node> nodes, int numEdges, int index) {
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = i + 1; j < nodes.size(); j++) {
                ++index;

                if (index % 100 == 0) {
                    log("info", index + " of " + numEdges);
                }

                Node x = nodes.get(i);
                Node y = nodes.get(j);

                tryAddingEdge(x, y);

            }
        }
    }

    private void log(String info, String message) {
        TetradLogger.getInstance().log(info, message);
        if ("info".equals(info)) {
            System.out.println(message);
        }
    }

    private void tryAddingEdge(Node x, Node y) {
        if (graph.isAdjacentTo(x, y)) {
            return;
        }

        List<Node> sepset = sepset(x, y);

        if (sepset == null) {
            sepset = sepset(y, x);
        }

        if (sepset == null) {
            if (getKnowledge().isForbidden(x.getName(), y.getName()) && getKnowledge().isForbidden(y.getName(), x.getName())) {
                return;
            }

            Edge edge = Edges.undirectedEdge(x, y);
            graph.addEdge(edge);
            orientNewColliders(x, y);

            for (Node w : graph.getAdjacentNodes(x)) {
                tryRemovingEdge(w, x);
            }

            for (Node w : graph.getAdjacentNodes(y)) {
                tryRemovingEdge(w, y);
            }
        }
    }

    private void tryRemovingEdge(Node x, Node y) {
        if (!graph.isAdjacentTo(x, y)) return;

        List<Node> sepsetX, sepsetY;
        boolean existsSepset = false;

        sepsetX = sepset(x, y);

        if (sepsetX != null) {
            existsSepset = true;
        } else {
            sepsetY = sepset(y, x);

            if (sepsetY != null) {
                existsSepset = true;
            }
        }

        if (existsSepset) {
            if (!getKnowledge().noEdgeRequired(x.getName(), y.getName())) {
                return;
            }

            graph.removeEdge(x, y);

            List<Node> start = new ArrayList<>();
            start.add(x);
            start.add(y);

            meekRules.orientImplied(graph, start);
        }
    }

    //================================PRIVATE METHODS=======================//

    private List<Node> sepset(Node x, Node y) {
        List<Node> adj = graph.getAdjacentNodes(x);
        adj.remove(y);

        DepthChoiceGenerator gen = new DepthChoiceGenerator(adj.size(), adj.size());
        int[] choice;

        while ((choice = gen.next()) != null) {
            List<Node> cond = GraphUtils.asList(choice, adj);

            if (getIndependenceTest().isIndependent(x, y, cond)) {
                if (recordSepsets) sepsetMap.set(x, y, cond);
                return cond;
            }
        }

        return null;
    }

    private void orientNewColliders(Node x, Node y) {
        reorient(x, y);
    }

    private void reorient(Node x, Node y) {
        for (Node z : graph.getAdjacentNodes(y)) {
            graph.removeEdge(z, y);
            graph.addUndirectedEdge(z, y);
        }

        for (Node z : graph.getAdjacentNodes(y)) {
            if (z == x) continue;

            List<Node> cond = sepset(z, x);
            if (cond == null) cond = sepset(x, z);

            if (cond != null && !cond.contains(y) && !graph.isParentOf(y, x) && !graph.isParentOf(y, z)) {
                graph.setEndpoint(x, y, Endpoint.ARROW);
                graph.setEndpoint(z, y, Endpoint.ARROW);
            }
        }

        List<Node> start = new ArrayList<>();
        start.add(y);

        MeekRules meekRules = new MeekRules();
        meekRules.setKnowledge(knowledge);
        meekRules.setUndirectUnforcedEdges(true);
        meekRules.orientImplied(graph, start);
    }

    public Set<Triple> getColliderTriples(Graph graph) {
        Set<Triple> triples = new HashSet<>();

        for (Node node : graph.getNodes()) {
            List<Node> nodesInto = graph.getNodesInTo(node, Endpoint.ARROW);

            if (nodesInto.size() < 2) continue;

            ChoiceGenerator gen = new ChoiceGenerator(nodesInto.size(), 2);
            int[] choice;

            while ((choice = gen.next()) != null) {
                triples.add(new Triple(nodesInto.get(choice[0]), node, nodesInto.get(choice[1])));
            }
        }

        return triples;
    }

    /**
     * Checks if an arrowpoint is allowed by background knowledge.
     */
    public static boolean isArrowpointAllowed(Object from, Object to,
                                              IKnowledge knowledge) {
        if (knowledge == null) {
            return true;
        }
        return !knowledge.isRequired(to.toString(), from.toString()) &&
                !knowledge.isForbidden(from.toString(), to.toString());
    }

    public void setRecordSepsets(boolean recordSepsets) {
        this.recordSepsets = recordSepsets;
    }

    public SepsetMap getSepsets() {
        return sepsetMap;
    }
}

