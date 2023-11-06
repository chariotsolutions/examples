import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 *  Performs a merge sort in which every partition is executed by a thread.
 */
public class ThreadSort
{
    private final static int    ITERATIONS = 10;


    public static void main(String[] argv)
    throws Exception
    {
        int dataSize = Integer.valueOf(argv[0]);

        Map<String,ExecutorService> executors = new HashMap<>();
        executors.put("inline", new InlineExecutorService());
        executors.put("fork-join", Executors.newWorkStealingPool());
        executors.put("virtual", Executors.newVirtualThreadPerTaskExecutor());

        double[] data = new double[dataSize];
        double[] scratch = new double[data.length];

        for (int rep = 0 ; rep < 6 ; rep++)
        {
            for (Map.Entry<String,ExecutorService> execItx : executors.entrySet())
            {
                long elapsed = 0;
                for (int ii = 0 ; ii < ITERATIONS ; ii++)
                {
                    elapsed += run(execItx.getValue(), data, scratch);
                }
                System.out.println(execItx.getKey() + ": time per iteration = " + (elapsed / ITERATIONS / 1000000 ) + " milliseconds");
            }
        }
    }


    /**
     *  Runs a single execution with the specified runner and size, returning elapsed time.
     */
    private static long run(ExecutorService executorService, double[] data, double[] scratch)
    throws Exception
    {
        populateData(data);
        long start = System.nanoTime();
        Future<?> job = executorService.submit(new Sorter(executorService, data, scratch, 0, data.length));
        job.get();
        long elapsed = System.nanoTime() - start;
        assertSorted(data);
        return elapsed;
    }


    private static double[] populateData(double[] data)
    {
        Random rnd = new Random();
        for (int ii = 0 ; ii < data.length ; ii++)
        {
            data[ii] = rnd.nextDouble();
        }
        return data;
    }


    private static void assertSorted(double[] data)
    {
        for (int ii = 1 ; ii < data.length ; ii++)
        {
            if (data[ii-1] > data[ii])
                throw new AssertionError("data[" + (ii - 1) + "] is " + data[ii - 1] + ", data[" + ii + "] is " + data[ii]);
        }
    }


    /**
     *  This performs a merge-sort over a segment of the data array. It expects to be invoked
     *  on a thread.
     */
    private static class Sorter
    implements Runnable
    {
        private ExecutorService executorService;
        private double[] data;
        private double[] scratch;
        private int low;
        private int high;

        public Sorter(ExecutorService executorService, double[] data, double[] scratch, int low, int high)
        {
            this.executorService = executorService;
            this.data = data;
            this.scratch = scratch;
            this.low = low;
            this.high = high;
        }


        @Override
        public void run()
        {
            int size = high - low;
            if (size <= 1)
                return;

            // in the real-world, I would optimize the handling of small partitions here: size <= 8 to be
            // cache-friendly, but at the least size == 2 to avoid N invocations ... but I want lots of
            // threads to spin up for this benchmark

            int split = low + size / 2;

            try
            {
                Future<?> future1 = executorService.submit(new Sorter(executorService, data, scratch, low, split));
                Future<?> future2 = executorService.submit(new Sorter(executorService, data, scratch, split, high));

                future1.get();
                future2.get();
            }
            catch (Exception ignored)
            {
                // should never happen, but if it does the sorter will quietly fail, and that failure will be
                // caught by assertSorted()
                return;
            }

            int src1Idx = low;
            int src2Idx = split;
            for (int destIdx = low ; destIdx < high ; )
            {
                if (src1Idx >= split)
                    scratch[destIdx++] = data[src2Idx++];
                else if (src2Idx >= high)
                    scratch[destIdx++] = data[src1Idx++];
                else if (data[src1Idx] <= data[src2Idx])
                    scratch[destIdx++] = data[src1Idx++];
                else
                    scratch[destIdx++] = data[src2Idx++];
            }

            for (int ii = low ; ii < high ; ii++)
            {
                data[ii] = scratch[ii];
            }
        }
    }


    /**
     *  An ExecutorService that runs tasks inline (this is the default behavior of AbstractExecutorService,
     *  we just need to give dummy implementations of its abstract methods).
     */
    private static class InlineExecutorService
    extends AbstractExecutorService
    {

        @Override
        public void shutdown()
        {
            // nothing to shut down
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown()
        {
            return false;
        }

        @Override
        public boolean isTerminated()
        {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
        {
            return true;
        }

        @Override
        public void execute(Runnable command)
        {
            command.run();
        }
    }
}

