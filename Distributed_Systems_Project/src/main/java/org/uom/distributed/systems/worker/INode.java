package org.uom.distributed.systems.worker;

public interface INode {
    int getCountOfNeighbours();

    void deleteNeighbour(Node node);

    void addNeighbour(Node node);

    void setX(int X);

    void setY(int Y);

    void setEnergyLevel(int energyLevel);

    int getX();

    int getY();

    int getEnergyLevel();

    String getNodeName();

    void setNodeName(String nodeName);

    long getNodeBullyID();

    void setNodeBullyID(long nodeBullyID);
}
