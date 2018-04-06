package simpledb;

import simpledb.logical.LogicalJoinNode;

import java.util.Vector;

/**
 * Class returned by {@link JoinOptimizer#computeCostAndCardOfSubplan} specifying the
 * cost and cardinality of the optimal plan represented by plan.
 */
public class CostCard {

    /**
     * The cost of the optimal subplan
     */
    private double cost;

    /**
     * The cardinality of the optimal subplan
     */
    private int card;

    /**
     * The optimal subplan
     */
    private Vector<LogicalJoinNode> plan;

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public int getCard() {
        return card;
    }

    public void setCard(int card) {
        this.card = card;
    }

    public Vector<LogicalJoinNode> getPlan() {
        return plan;
    }

    public void setPlan(Vector<LogicalJoinNode> plan) {
        this.plan = plan;
    }
}
