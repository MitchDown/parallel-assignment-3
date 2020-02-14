/*
 * Mitchel Downey
 * CPSC 4600 Seattle University
 */


/**
 * @class BitonicPipeline class per cpsc5600 hw3 specification.
 * @versioon 25-Jan-2020
 */

import java.util.Arrays;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 * @class BitonicPipeline class per cpsc4600 hw3 specification.
 * @version 26-Jan-2020
 *
 */
public class BitonicPipeline {
    public static final int N = 1 << 22;  // size of the final sorted array (power of two)
    public static final int TIME_ALLOWED = 10;  // seconds
    public static final int NUM_SYNCH_QUEUE = 11;
    public static final int NUM_STAGE_THREAD = 4;
    public static final int NUM_BITONIC_THREAD = 3;
    /**
     * Main entry for HW3 assignment.
     *
     * @param args not used
     */
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        int work = 0;

        //Initialize the SynchronousQueue, StageOne, and BitonicStage objects
        SynchronousQueue<double[]> inOutQueues[];
        inOutQueues = new SynchronousQueue[NUM_SYNCH_QUEUE];
        Thread[] randomGenerators = new Thread[NUM_STAGE_THREAD];
        Thread[] starters = new Thread[NUM_STAGE_THREAD];
        Thread[] sorters = new Thread[NUM_BITONIC_THREAD];
        for(int i = 0; i < NUM_SYNCH_QUEUE; i++)
        {
            inOutQueues[i] = new SynchronousQueue<double[]>();
        }
        for(int i = 0; i < NUM_STAGE_THREAD; i++)
        {
            starters[i] = new Thread(new StageOne(inOutQueues[2 * i], inOutQueues[(2 * i) + 1]));
            randomGenerators[i] = new Thread(new RandomArrayGenerator(N/4, inOutQueues[2 * i]));
            starters[i].start();
            randomGenerators[i].start();
        }
        for(int i = 0; i < NUM_BITONIC_THREAD - 1; i++)
        {
            sorters[i] = new Thread(new BitonicStage(inOutQueues[(4*i) + 1], inOutQueues[(4 * i) + 3], inOutQueues[8 + i]));
            sorters[i].start();
        }
        sorters[NUM_BITONIC_THREAD - 1] = new Thread(new BitonicStage(inOutQueues[8],inOutQueues[9],inOutQueues[10]));
        sorters[NUM_BITONIC_THREAD - 1].start();

        while (System.currentTimeMillis() < start + TIME_ALLOWED * 1000) {
            double[] finalOutput = new double[1];
            try
            {
                finalOutput = inOutQueues[10].poll(TIME_ALLOWED * 1000, TimeUnit.MILLISECONDS);
            }catch(InterruptedException e) {}
            if (!RandomArrayGenerator.isSorted(finalOutput) || N != finalOutput.length)
                System.out.println("failed");
            work++;
        }
        System.out.println("sorted " + work + " arrays (each: " + N + " doubles) in "
                + TIME_ALLOWED + " seconds");

        for (int i = 0; i < NUM_STAGE_THREAD; i++)
        {
            randomGenerators[i].interrupt();
            starters[i].interrupt();
        }
        for (int i = 0; i < NUM_BITONIC_THREAD; i++)
        {
            sorters[i].interrupt();
        }
    }
}

