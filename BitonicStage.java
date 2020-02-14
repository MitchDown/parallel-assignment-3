

import java.util.Arrays;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * Secondary stage of the bitonic sorting network pipeline. This class takes in two arrays, sorted in ascending order,
 * and outputs a single sorted array created by running a bitonic sort on the two input arrays
 */
public class BitonicStage implements Runnable
{
    private static final int timeout = 10;  // in seconds

    public BitonicStage() {}

    /**
     *  Setup for BitonicStage with two input SynchronousQueues and one output SynchronousQueue
     *
     * @param input1    where to read the first sorted array
     * @param input2    where to read the second sorted array
     * @param output    where to write the final sorted array
     */
    public BitonicStage(SynchronousQueue<double[]> input1, SynchronousQueue<double[]> input2, SynchronousQueue<double[]> output)
    {
        this.input1 = input1;
        this.input2 = input2;
        this.output = output;
        name = "";
    }

    /**
     * Begins the bitonic sort pipeline and returns the final sorted array
     *
     * @param left  an array of doubles sorted in ascending order
     * @param right an array of doubles sorted in ascending order
     * @return  a single array of doubles that results from running a bitonic sort on left and right inputs
     */
    public double[] process(double[] left, double[] right)
    {
        invertArray(right);
        bitonicMerge(left, right);
        bitonicSort(left, 0, left.length );
        bitonicSort(right,0 ,right.length);
        return concat(left,right);
    }

    /**
     *  Performs the bitonic merge operation on two seperate lists of equal size
     *
     * @param left  an array of doubles which will be less than right after the function completes
     * @param right an array of doubles which will be greater than left after the function completes
     */
    public void bitonicMerge(double[] left, double[] right)
    {
        for(int i = 0; i < left.length; i++)
        {
            if(left[i] > right[i])
            {
                swap(left, right,i);
            }
        }
    }

    /**
     * Performs the bitonic merge operation on a single list
     *
     * @param list  the array of doubles to perform the bitonic merge on
     * @param m     the starting index for the merge
     * @param half  half the size of the section of list that is being sorted
     */
    public void bitonicMerge(double[] list, int m, int half)
    {
        for(int i = 0; i < half; i++)
        {
            if(list[m + i] > list[m + half + i])
            {
                swap(list, m + i, m + half + i);
            }

        }
    }

    /**
     * Performs the recursive bitonic sort on a single list
     *
     * @param list  the array of doubles to perform the bitonic sort on
     * @param m     the starting index for the sort
     * @param k     the size of the section of list that is being sorted
     */
    public void bitonicSort(double[] list, int m, int k)
    {
        if (k > 1) {
            int half = k / 2;
            bitonicMerge(list, m, half);
            bitonicSort(list, m, half);
            bitonicSort(list, m + half, half);
        }
    }

    /**
     * The Runnable part of the class. Polls the two input queues and when ready, performs the bitonic sort
     * pipeline on them and write the result to the output queues
     */
    public void run()
    {
        double[] read1 = new double[1];
        double[] read2 = new double[1];
        while (read1 != null && read2 != null) {
            try {
                read1 = input1.poll(timeout * 1000, TimeUnit.MILLISECONDS);
                read2 = input2.poll(timeout * 1000, TimeUnit.MILLISECONDS);
                if (read1 != null && read2 != null) {
                    double[] outArray = process(read1,read2);
                    output.offer(outArray, timeout * 1000, TimeUnit.MILLISECONDS);
                } else {
                    System.out.println(getClass().getName() + " " + name + " got null array");
                }
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private SynchronousQueue<double[]> input1, input2, output;
    private String name;

    /**
     * Static method to swap two items in a list
     *
     * @param list  an array of doubles
     * @param a     the index of the first item to be swapped
     * @param b     the index of the second item to be swapped
     */
    private static void swap(double[]list, int a, int b)
    {
        double temp = list[a];
        list[a] = list[b];
        list[b] = temp;
    }

    /**
     * Static method to swap two items at the same index located in different lists
     *
     * @param left  an array of doubles
     * @param right an array of doubles
     * @param i     the index of the items to be swapped
     */
    private static void swap(double[] left, double[]right, int i)
    {
        double temp = left[i];
        left[i] = right[i];
        right[i] = temp;
    }

    /**
     * Inverts an array
     *
     * @param backwards an array of doubles
     */
    private static void invertArray(double[] backwards)
    {
        for(int i = 0; i < backwards.length/2; i++)
        {
            double temp = backwards[i];
            backwards[i] = backwards[backwards.length - i - 1];
            backwards[backwards.length - i - 1] = temp;
        }
    }

    /**
     * Concatenates two arrays into a single array
     *
     * @param left  an array of doubles
     * @param right an array of doubles
     * @return      a single array of doubles resulting from the concatenation of left and right
     */
    private double[] concat(double[] left, double[] right)
    {
        int length = left.length + right.length;
        double[] result = new double[length];
        int pos = 0;
        for(double element: left)
        {
            result[pos] = element;
            pos++;
        }
        for(double element: right)
        {
            result[pos] = element;
            pos++;
        }
        return result;
    }
}