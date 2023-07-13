package org.uom.distributed.systems.worker.designation;

import org.uom.distributed.systems.worker.Node;

import java.util.ArrayList;
import java.util.List;

public class FollowerNode extends Decorator {
    private String groupID;
    private LeaderNode leader;
    private final List<FollowerNode> groupMembers;

    public FollowerNode(Node node) {
        super(node);
        this.groupMembers = new ArrayList<>(10);
    }

    public FollowerNode(Node node, List<FollowerNode> groupMembers) {
        super(node);
        this.groupMembers = groupMembers;
    }

    public void setLeader(LeaderNode leader) {
        this.leader = leader;
    }

    public void setGroupID (String groupID) {
        this.groupID = groupID;
    }

    public String getGroupID() {
        return groupID;
    }

    public void addGroupMembers (FollowerNode followerNode) {
        if (groupID.equals(followerNode.getGroupID()))
            groupMembers.add(followerNode);
        else
            throw new RuntimeException("Trying to add group members of other groups.");
    }


}
