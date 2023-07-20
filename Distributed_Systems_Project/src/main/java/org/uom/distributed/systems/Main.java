package org.uom.distributed.systems;


import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;


public class Main {

    public static void main(String[] args) throws InterruptedException {

        int[][] input = {
                {2,3,345},{12,45,234},{35,45,533},{1,38,234},{4,5,100},{50,3,98},{22,2,144},{34,33,233},{11,39,235}
        };

        NodeManager.initiateSystem(input);
    }
}