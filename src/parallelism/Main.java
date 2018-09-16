package parallelism;

import java.io.*;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.sql.*;

public class Main {

    public static void main(String[] args) {
        if(args[0].equals("all")) {
            int i = 1;
            while (i <= 4) {
                getSumArray(i++);
            }
        } else {
            getSumArray(Integer.parseInt(args[0]));
        }
    }

    private static void getSumArray(int threads) {
        int timeTaken;
        long total;
        Map<Integer, Map.Entry<Integer, Long>> threadSumMap = getThreadTimeSumMap();

        if (threadSumMap != null && threadSumMap.containsKey(threads)) {
            long sum = threadSumMap.get(threads).getValue();
            int time = threadSumMap.get(threads).getKey();

            System.out.println("\n[" + threads + "] Gathered from database:\n-----------------------");
            System.out.println("Time taken:\t\t" + time + " ms");
            System.out.println("Summed value:\t" + sum);
        } else {
            ForkJoinPool pool = new ForkJoinPool();

            //serializeObject("longs.txt", generateArray(1000000));
            long[] array = (long[]) deserializeObject("longs.txt");

            SumArrayWorkerThread sumArray = new SumArrayWorkerThread(threads, array, 0, array.length - 1);
            long startTime = System.currentTimeMillis();
            pool.invoke(sumArray);
            long endTime = System.currentTimeMillis();

            timeTaken = (int) (endTime - startTime);
            total = sumArray.total;

            System.out.println("\n[" + threads + "] Gathered from parallelism program:\n-----------------------");
            System.out.println("Time taken:\t\t" + timeTaken + " ms");
            System.out.println("Summed value:\t" + total);

            // MySQL
            insertData(threads, timeTaken, total);
        }
    }

    private static Object deserializeObject(String objectName){
        Object object;
        try {
            FileInputStream fis = new FileInputStream(new File(objectName));
            ObjectInputStream ois = new ObjectInputStream(fis);
            object = ois.readObject();
        } catch (Exception e) {
            object = null;
            System.out.println(e.toString());
        }

        return object;
    }

    private static void serializeObject(String objectName, Object objectIn){
        try {
            FileOutputStream fos = new FileOutputStream(new File(objectName));
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(objectIn);
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

    private static long[] generateArray(int size) {
        GenerateArray generateArray = new GenerateArray();
        return generateArray.generateArray(size);
    }

    private static void insertData(int threads, int time, long total) {
        try {
            Connection connection = DriverManager.getConnection("jdbc:sqlserver://localhost;databaseName=threads;integratedSecurity=true;");
            CallableStatement statement = connection.prepareCall("{call spInsertValues(?, ?, ?)}");

            statement.setInt(1, threads);
            statement.setInt(2, time);
            statement.setLong(3, total);

            statement.execute();

            System.out.println("Data inserted into the \"" + connection.getCatalog() + "\" database successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Map<Integer, Map.Entry<Integer, Long>> getThreadTimeSumMap() {
        try {
            Connection connection = DriverManager.getConnection("jdbc:sqlserver://localhost;databaseName=threads;integratedSecurity=true");
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from dbo.parallelism");
            Map<Integer, Map.Entry<Integer, Long>> map = new HashMap<>();

            if (resultSet != null) {
                while (resultSet.next()) {
                    map.put(
                        resultSet.getInt("threads"),
                        new AbstractMap.SimpleEntry<>(resultSet.getInt("time"), resultSet.getLong("sum"))
                    );
                }
            }
            return map;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
