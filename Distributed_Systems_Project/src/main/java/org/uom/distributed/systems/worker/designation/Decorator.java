package org.uom.distributed.systems.worker.designation;

import org.uom.distributed.systems.worker.INode;
import org.uom.distributed.systems.worker.Node;

public abstract class Decorator implements INode {
    private Node node;

    public Decorator (Node node) {
        this.node = node;
    }

    @Override
    public int getCountOfNeighbours() {
        return node.getCountOfNeighbours();
    }

    @Override
    public void deleteNeighbour(Node node) {
        Decorator.this.node.deleteNeighbour(node);
    }

    @Override
    public void addNeighbour(Node node) {
        Decorator.this.node.addNeighbour(node);
    }

    @Override
    public void setX(int X) {
        node.setX(X);
    }

    @Override
    public void setY(int Y) {
        node.setY(Y);
    }

    @Override
    public void setEnergyLevel(int energyLevel) {
        node.setEnergyLevel(energyLevel);
    }

    @Override
    public int getX() {
        return node.getX();
    }

    @Override
    public int getY() {
        return node.getY();
    }

    @Override
    public int getEnergyLevel() {
        return node.getEnergyLevel();
    }

    @Override
    public String getNodeName() {
        return node.getNodeName();
    }

    @Override
    public void setNodeName(String nodeName) {
        node.setNodeName(nodeName);
    }

    @Override
    public long getNodeBullyID() {
        return node.getNodeBullyID();
    }

    @Override
    public void setNodeBullyID(long nodeBullyID) {
        node.setNodeBullyID(nodeBullyID);
    }
}
