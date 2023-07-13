package org.uom.distributed.systems.worker.designation;

import org.uom.distributed.systems.worker.Node;

import java.util.ArrayList;
import java.util.List;

public class LeaderNode extends Decorator {
    private String groupID;
    private final List<FollowerNode> followers;
    private final List<LeaderNode> boardOfExecutives;
    public LeaderNode(Node node) {
        super(node);
        this.followers = new ArrayList<>(10);
        this.boardOfExecutives = new ArrayList<>(10);
    }

    public void setGroupID (String groupID) {
        this.groupID = groupID;
    }

    public String getGroupID() {
        return groupID;
    }

    public LeaderNode(Node node ,List<LeaderNode> boardOfExecutives ) {
        super(node);
        this.followers = new ArrayList<>(10);
        this.boardOfExecutives = boardOfExecutives;
    }

    public void addFellowLeader(LeaderNode leaderNode) {
        boardOfExecutives.add(leaderNode);
    }

    public void addFollower(FollowerNode followerNode) {
        followers.add(followerNode);
    }

    public boolean isFollower(Node node) {
        for (FollowerNode followerNode : followers) {
            if (followerNode.getNodeName().equals(node.getNodeName())) {
                return true;
            }
        }
        return false;
    }

}
