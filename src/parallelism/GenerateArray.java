package parallelism;

public class GenerateArray {

    long[] array;


    public long[] generateArray(int size) {
        array = new long[size];

        for (int i = 0; i < size; i++) {
            int rand = (int)(Math.random() * 100 + 1);
            array[i] = rand;
        }

        return array;
    }
}
