package simpledb;

import simpledb.aggregator.Aggregator;
import simpledb.operator.Aggregate;
import simpledb.operator.Filter;
import simpledb.operator.HashEquiJoin;
import simpledb.operator.Join;
import simpledb.operator.JoinPredicate;
import simpledb.operator.OpIterator;
import simpledb.operator.Operator;
import simpledb.operator.OrderBy;
import simpledb.operator.Predicate;
import simpledb.operator.Project;
import simpledb.operator.SeqScan;
import simpledb.tuple.TupleDesc;
import simpledb.tuple.TupleDesc.TDItem;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;

/**
 * 查询可视化
 */
public class QueryPlanVisualizer {

    private static class SubTreeDescriptor {
        SubTreeDescriptor leftChild;
        SubTreeDescriptor rightChild;

        int width;
        int height;
        int upBarPosition;
        int textStartPosition;
        String text;

        SubTreeDescriptor(SubTreeDescriptor leftChild,
                          SubTreeDescriptor rightChild) {
            this.leftChild = leftChild;
            this.rightChild = rightChild;
        }
    }

    private static final String JOIN = "⨝";
    private static final String HASH_JOIN = "⨝(hash)";
    private static final String SELECT = "σ";
    private static final String PROJECT = "π";
    private static final String RENAME = "ρ";
    private static final String SCAN = "scan";
    private static final String ORDERBY = "o";
    private static final String GROUPBY = "g";
    private static final String SPACE = "  ";

    /**
     * 计算查询计划树的深度
     * Join、HashJoin节点才会有左右子树，否则只有一个直系孩子节点
     * 但是我不是很明白为什么普通节点高度是2个单位，Join节点的高度却是3
     */
    private int calculateQueryPlanTreeDepth(OpIterator root) {
        if (root == null) {
            return 0;
        }

        if (!(root instanceof Operator)) {
            return 2;
        }
        Operator o = (Operator) root;
        OpIterator[] children = o.getChildren();

        if (o instanceof Join || o instanceof HashEquiJoin) {
            int d1 = this.calculateQueryPlanTreeDepth(children[0]);
            int d2 = this.calculateQueryPlanTreeDepth(children[1]);
            return Math.max(d1, d2) + 3;
        } else {
            if (children != null && children[0] != null) {
                return this.calculateQueryPlanTreeDepth(children[0]) + 2;
            }
        }
        return 2;
    }

    private SubTreeDescriptor buildTree(int queryPlanDepth, int currentDepth,
                                        OpIterator queryPlan, int currentStartPosition,
                                        int parentUpperBarStartShift) {
        if (queryPlan == null) {
            return null;
        }

        int adjustDepth = currentDepth == 0 ? -1 : 0;
        SubTreeDescriptor thisNode = new SubTreeDescriptor(null, null);

        if (queryPlan instanceof SeqScan) {
            /*
                扫表
             */

            SeqScan s = (SeqScan) queryPlan;
            String tableName = s.getTableName();
            String alias = s.getAlias();
            if (!tableName.equals(alias)) {
                alias = " " + alias;
            } else {
                alias = "";
            }

            thisNode.text = String.format("%1$s(%2$s)", SCAN, tableName + alias);

            if (SCAN.length() / 2 < parentUpperBarStartShift) {
                thisNode.upBarPosition = currentStartPosition + parentUpperBarStartShift;
                thisNode.textStartPosition = thisNode.upBarPosition - SCAN.length() / 2;
            } else {
                thisNode.upBarPosition = currentStartPosition + SCAN.length() / 2;
                thisNode.textStartPosition = currentStartPosition;
            }
            thisNode.width = thisNode.textStartPosition - currentStartPosition
                    + thisNode.text.length();
            int embedHeight = (queryPlanDepth - currentDepth) / 2 - 1;
            thisNode.height = currentDepth + 2 * embedHeight;
            int currentHeight = thisNode.height;
            SubTreeDescriptor parentNode = thisNode;
            for (int i = 0; i < embedHeight; i++) {
                parentNode = new SubTreeDescriptor(parentNode, null);
                parentNode.text = "|";
                parentNode.upBarPosition = thisNode.upBarPosition;
                parentNode.width = thisNode.width;
                parentNode.height = currentHeight - 2;
                parentNode.textStartPosition = thisNode.upBarPosition;
                currentHeight -= 2;
            }
            thisNode = parentNode;
        } else {

            Operator plan = (Operator) queryPlan;
            OpIterator[] children = plan.getChildren();

            if (plan instanceof Join) {
                Join j = (Join) plan;
                TupleDesc td = j.getTupleDesc();
                JoinPredicate jp = j.getJoinPredicate();
                String field1 = td.getFieldName(jp.getField1());
                String field2 = td.getFieldName(jp.getField2()
                        + children[0].getTupleDesc().numFields());
                thisNode.text = String.format("%1$s(%2$s),card:%3$d", JOIN,
                        field1 + jp.getOperator() + field2, j.getEstimatedCardinality());
                int upBarShift = parentUpperBarStartShift;
                if (JOIN.length() / 2 > parentUpperBarStartShift) {
                    upBarShift = JOIN.length() / 2;
                }

                SubTreeDescriptor left = this.buildTree(queryPlanDepth,
                        currentDepth + adjustDepth + 3, children[0],
                        currentStartPosition, upBarShift);

                SubTreeDescriptor right = this.buildTree(queryPlanDepth,
                        currentDepth + adjustDepth + 3, children[1],
                        currentStartPosition + left.width + SPACE.length(), 0);

                thisNode.upBarPosition = (left.upBarPosition + right.upBarPosition) / 2;
                thisNode.textStartPosition = thisNode.upBarPosition - JOIN.length() / 2;
                thisNode.width = Math.max(
                        left.width + right.width + SPACE.length(),
                        thisNode.textStartPosition + thisNode.text.length()
                                - currentStartPosition);
                thisNode.leftChild = left;
                thisNode.rightChild = right;
                thisNode.height = currentDepth;
            } else if (plan instanceof HashEquiJoin) {
                HashEquiJoin j = (HashEquiJoin) plan;
                JoinPredicate jp = j.getJoinPredicate();
                TupleDesc td = j.getTupleDesc();
                String field1 = td.getFieldName(jp.getField1());
                String field2 = td.getFieldName(jp.getField2()
                        + children[0].getTupleDesc().numFields());
                thisNode.text = String.format("%1$s(%2$s),card:%3$d", HASH_JOIN, field1
                        + jp.getOperator() + field2, j.getEstimatedCardinality());
                int upBarShift = parentUpperBarStartShift;
                if (HASH_JOIN.length() / 2 > parentUpperBarStartShift) {
                    upBarShift = HASH_JOIN.length() / 2;
                }
                SubTreeDescriptor left = this.buildTree(queryPlanDepth,
                        currentDepth + 3 + adjustDepth, children[0],
                        currentStartPosition, upBarShift);
                SubTreeDescriptor right = this.buildTree(queryPlanDepth,
                        currentDepth + 3 + adjustDepth, children[1],
                        currentStartPosition + left.width + SPACE.length(), 0);
                thisNode.upBarPosition = (left.upBarPosition + right.upBarPosition) / 2;
                thisNode.textStartPosition = thisNode.upBarPosition - HASH_JOIN.length() / 2;
                thisNode.width = Math.max(
                        left.width + right.width + SPACE.length(),
                        thisNode.textStartPosition + thisNode.text.length()
                                - currentStartPosition);
                thisNode.leftChild = left;
                thisNode.rightChild = right;
                thisNode.height = currentDepth;
            } else if (plan instanceof Aggregate) {
                Aggregate a = (Aggregate) plan;
                int upBarShift = parentUpperBarStartShift;
                String alignTxt;
                TupleDesc td = a.getTupleDesc();
                int gfield = a.groupField();

                if (gfield == Aggregator.NO_GROUPING) {
                    thisNode.text = String.format("%1$s(%2$s),card:%3$d",
                            a.aggregateOp(), a.aggregateFieldName(), a.getEstimatedCardinality());
                    alignTxt = td.getFieldName(00);
                } else {
                    thisNode.text = String.format("%1$s(%2$s), %3$s(%4$s),card:%5$d",
                            GROUPBY, a.groupFieldName(), a.aggregateOp(),
                            a.aggregateFieldName(), a.getEstimatedCardinality());
                    alignTxt = GROUPBY;
                }
                if (alignTxt.length() / 2 > parentUpperBarStartShift) {
                    upBarShift = alignTxt.length() / 2;
                }

                SubTreeDescriptor child = this.buildTree(queryPlanDepth,
                        currentDepth + 2 + adjustDepth, children[0],
                        currentStartPosition, upBarShift);

                thisNode.upBarPosition = child.upBarPosition;
                thisNode.textStartPosition = thisNode.upBarPosition
                        - alignTxt.length() / 2;
                thisNode.width = Math.max(child.width,
                        thisNode.textStartPosition + thisNode.text.length()
                                - currentStartPosition);
                thisNode.leftChild = child;
                thisNode.height = currentDepth;
            } else if (plan instanceof Filter) {
                Filter f = (Filter) plan;
                Predicate p = f.getPredicate();
                thisNode.text = String.format("%1$s(%2$s),card:%3$d", SELECT, children[0]
                        .getTupleDesc().getFieldName(p.getField())
                        + p.getOp()
                        + p.getOperand(), f.getEstimatedCardinality());
                int upBarShift = parentUpperBarStartShift;
                if (SELECT.length() / 2 > parentUpperBarStartShift) {
                    upBarShift = SELECT.length() / 2;
                }
                SubTreeDescriptor child = this.buildTree(queryPlanDepth,
                        currentDepth + 2 + adjustDepth, children[0],
                        currentStartPosition, upBarShift);
                thisNode.upBarPosition = child.upBarPosition;
                thisNode.textStartPosition = thisNode.upBarPosition
                        - SELECT.length() / 2;
                thisNode.width = Math.max(child.width,
                        thisNode.textStartPosition + thisNode.text.length()
                                - currentStartPosition);
                thisNode.leftChild = child;
                thisNode.height = currentDepth;
            } else if (plan instanceof OrderBy) {
                OrderBy o = (OrderBy) plan;
                thisNode.text = String.format(
                        "%1$s(%2$s),card:%3$d",
                        ORDERBY,
                        children[0].getTupleDesc().getFieldName(
                                o.getOrderByField()), o.getEstimatedCardinality());
                int upBarShift = parentUpperBarStartShift;
                if (ORDERBY.length() / 2 > parentUpperBarStartShift) {
                    upBarShift = ORDERBY.length() / 2;
                }
                SubTreeDescriptor child = this.buildTree(queryPlanDepth,
                        currentDepth + 2 + adjustDepth, children[0],
                        currentStartPosition, upBarShift);
                thisNode.upBarPosition = child.upBarPosition;
                thisNode.textStartPosition = thisNode.upBarPosition
                        - ORDERBY.length() / 2;
                thisNode.width = Math.max(child.width,
                        thisNode.textStartPosition + thisNode.text.length()
                                - currentStartPosition);
                thisNode.leftChild = child;
                thisNode.height = currentDepth;
            } else if (plan instanceof Project) {
                Project p = (Project) plan;
                String fields = "";
                Iterator<TDItem> it = p.getTupleDesc().iterator();
                while (it.hasNext()) {
                    fields += it.next().getFieldName() + ",";
                }
                fields = fields.substring(0, fields.length() - 1);
                thisNode.text = String.format("%1$s(%2$s),card:%3$d", PROJECT, fields, p.getEstimatedCardinality());
                int upBarShift = parentUpperBarStartShift;
                if (PROJECT.length() / 2 > parentUpperBarStartShift) {
                    upBarShift = PROJECT.length() / 2;
                }
                SubTreeDescriptor child = this.buildTree(queryPlanDepth,
                        currentDepth + 2 + adjustDepth, children[0],
                        currentStartPosition, upBarShift);

                thisNode.upBarPosition = child.upBarPosition;
                thisNode.textStartPosition = thisNode.upBarPosition
                        - PROJECT.length() / 2;
                thisNode.width = Math.max(child.width,
                        thisNode.textStartPosition + thisNode.text.length()
                                - currentStartPosition);
                thisNode.leftChild = child;
                thisNode.height = currentDepth;
            } else if (plan.getClass().getSuperclass().getSuperclass().getSimpleName().equals("Exchange")) {
                String name = "Exchange";
                int card = 0;
                try {
                    name = (String) plan.getClass().getMethod("getName").invoke(plan);
                    card = (Integer) plan.getClass().getMethod("getEstimatedCardinality").invoke(plan);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                thisNode.text = String.format("%1$s,card:%2$d", name, card);
                int upBarShift = parentUpperBarStartShift;
                if (name.length() / 2 > parentUpperBarStartShift) {
                    upBarShift = name.length() / 2;
                }
                SubTreeDescriptor child = this.buildTree(queryPlanDepth,
                        currentDepth + 2 + adjustDepth, children[0],
                        currentStartPosition, upBarShift);
                if (child == null) {
                    thisNode.upBarPosition = upBarShift;
                    thisNode.textStartPosition = thisNode.upBarPosition
                            - name.length() / 2;
                    thisNode.width = thisNode.textStartPosition + thisNode.text.length()
                            - currentStartPosition;
                } else {
                    thisNode.upBarPosition = child.upBarPosition;
                    thisNode.textStartPosition = thisNode.upBarPosition - name.length() / 2;
                    thisNode.width = Math.max(child.width,
                            thisNode.textStartPosition + thisNode.text.length()
                                    - currentStartPosition);
                    thisNode.leftChild = child;
                }
                thisNode.height = currentDepth;
            } else if (plan.getClass().getName().equals("simpledb.Rename")) {
                String newName = null;
                int fieldIdx = 0;
                try {
                    newName = (String) plan.getClass().getMethod("newName", (Class<?>[]) null).invoke(plan);
                    fieldIdx = (Integer) plan.getClass().getMethod("renamedField", (Class<?>[]) null).invoke(plan);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                String oldName = plan.getChildren()[0].getTupleDesc().getFieldName(fieldIdx);
                thisNode.text = String.format("%1$s,%2$s->%3$s,card:%4$d", RENAME, oldName, newName, plan.getEstimatedCardinality());
                int upBarShift = parentUpperBarStartShift;
                if (RENAME.length() / 2 > parentUpperBarStartShift) {
                    upBarShift = RENAME.length() / 2;
                }
                SubTreeDescriptor child = this.buildTree(queryPlanDepth,
                        currentDepth + 2 + adjustDepth, children[0],
                        currentStartPosition, upBarShift);
                if (child == null) {
                    thisNode.upBarPosition = upBarShift;
                    thisNode.textStartPosition = thisNode.upBarPosition
                            - RENAME.length() / 2;
                    thisNode.width = thisNode.textStartPosition + thisNode.text.length()
                            - currentStartPosition;
                } else {
                    thisNode.upBarPosition = child.upBarPosition;
                    thisNode.textStartPosition = thisNode.upBarPosition - RENAME.length() / 2;
                    thisNode.width = Math.max(child.width,
                            thisNode.textStartPosition + thisNode.text.length() - currentStartPosition);
                    thisNode.leftChild = child;
                }
                thisNode.height = currentDepth;
            }
        }
        return thisNode;
    }

    private void printTree(SubTreeDescriptor root, char[] buffer, int width) {
        if (root == null) {
            return;
        }
        int textHeight = root.height + 1;
        if (root.height != 0) {
            buffer[width * root.height + root.upBarPosition] = '|';
        } else {
            textHeight = root.height;
        }

        int base = width * textHeight + root.textStartPosition;
        char[] text = root.text.toCharArray();
        System.arraycopy(text, 0, buffer, base, text.length);

        if (root.leftChild != null && root.rightChild == null) {
            printTree(root.leftChild, buffer, width);
        } else if (root.leftChild != null && root.rightChild != null) {
            Arrays.fill(buffer, (textHeight + 1) * width
                    + root.leftChild.upBarPosition, (textHeight + 1) * width
                    + root.rightChild.upBarPosition + 1, '_');
            buffer[(textHeight + 1) * width + root.upBarPosition] = '|';
            printTree(root.leftChild, buffer, width);
            printTree(root.rightChild, buffer, width);
        }
    }

    public String getQueryPlanTree(OpIterator physicalPlan) {

        /*
            获取物理查询计划的深度
         */
        int queryPlanDepth = this.calculateQueryPlanTreeDepth(physicalPlan) - 1;

        /*
            根据物理查询计划构建一个“查询计划描述树”
         */
        SubTreeDescriptor root = this.buildTree(queryPlanDepth, 0, physicalPlan, 0, 0);

        /*
            打印这棵树需要 (深度d 乘以 树的最大宽度w+1) 大小的字符空间
            每行多出的一个字节用于记录换行符
         */
        char[] buffer = new char[queryPlanDepth * (root.width + 1)];
        Arrays.fill(buffer, ' ');
        for (int i = 1; i <= queryPlanDepth; i++) {
            buffer[i * (root.width + 1) - 1] = '\n';
        }

        printTree(root, buffer, root.width + 1);
        StringBuilder sb = new StringBuilder();

        boolean ending = false;
        for (int i = buffer.length - 1; i >= 0; i--) {
            if (buffer[i] == '\n') {
                sb.append(buffer[i]);
                ending = true;
            } else if (ending) {
                if (buffer[i] != ' ') {
                    ending = false;
                    sb.append(buffer[i]);
                }
            } else {
                sb.append(buffer[i]);
            }
        }

        return sb.reverse().toString();
    }

    public void printQueryPlanTree(OpIterator physicalPlan, PrintStream out) {
        if (out == null) {
            out = System.out;
        }

        String tree = this.getQueryPlanTree(physicalPlan);

        out.println(tree);
    }
}
