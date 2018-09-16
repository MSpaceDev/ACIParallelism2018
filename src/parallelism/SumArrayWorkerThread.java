package parallelism;

import java.util.concurrent.*;

public class SumArrayWorkerThread extends RecursiveAction {

    private final int SEQ_THRESHOLD;
    private long[] array;
    private int left;
    private int right;
    private int threads;
    public int total;

    public SumArrayWorkerThread(int threads, long[] array, int left, int right) {
        this.threads = threads;
        this.array = array;
        this.left = left;
        this.right = right;
        this.SEQ_THRESHOLD = array.length / this.threads;
    }

    @Override
    protected void compute() {
        if ((right - left) < SEQ_THRESHOLD) {
            for (int i = left; i < right + 1; i++) {
                total += array[i];
            }

            // Simulate "work"
            try {
                double wait = 0.0004 * SEQ_THRESHOLD;
                Thread.sleep((int) wait);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            int mid = (left + right) / 2;
            SumArrayWorkerThread leftTotal = new SumArrayWorkerThread(threads, array, left, mid);
            SumArrayWorkerThread rightTotal = new SumArrayWorkerThread(threads, array, mid + 1, right);
            invokeAll(leftTotal, rightTotal);
            merge(leftTotal, rightTotal);
        }
    }

    private void merge(SumArrayWorkerThread leftTotal, SumArrayWorkerThread rightTotal){
        total = leftTotal.total + rightTotal.total;
    }
}
