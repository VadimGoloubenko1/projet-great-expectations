package imt.ibd.lambda;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.Map;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class Main {
    public static void main(String[] args) throws Exception {
        AccessLogParser parser = new AccessLogParser();
        List<AccessLogEntry> logs = parser.parse("/opt/airflow/log_process/src/big_access.log");

        // Partie 1 Question 1
        /*System.out.println(logs.size());
        System.out.println(logs.get(0));*/

        // Partie 1 Question 2
        // ...

        // Partie 1 Question 3
        /*Consumer<AccessLogEntry> byIp = log -> System.out.println("IP: " + log.getIp());
        Consumer<AccessLogEntry> byMethodAndUrl = log -> System.out.println("Method: " + log.getMethod() + " at: " + log.getUrl());
        logs.forEach(byIp);
        logs.forEach(byMethodAndUrl);*/

        // Partie 2 Question 1
        /*long failureCount = logs.stream().filter(log -> log.getStatus() != 200).count();
        System.out.println("Existing failure: " + (failureCount > 0));

        Predicate<AccessLogEntry> success = log -> log.isSuccess();
        System.out.println("Existing failure2: " + logs.stream().anyMatch(success.negate()));*/

        // Partie 2 Question 2
        /*Predicate<AccessLogEntry> hasSpecialIp = log -> log.getIp().startsWith("81.88");
        System.out.println("Success and special IP count: " + logs.stream().filter(success.and(hasSpecialIp)).count());

        logs.stream().filter(success.and(hasSpecialIp)).forEach(System.out::println);*/

        // Partie 2 Question 3
        /*logs.stream().forEach(log -> System.out.println(log.getIp()));*/

        // Partie 2 Question 4
        /*logs.stream().filter(log -> log.getSize() > 4000).map(log -> log.getIp()).forEach(System.out::println);*/

        // Partie 2 Question 5
        /*Calendar date = Calendar.getInstance();
        date.set(2016, Calendar.MARCH, 1);
        AccessLogEntry closest = logs.stream().min((log1, log2) -> {
            long delta1 = log1.getDate().getTimeInMillis() - date.getTimeInMillis();
            long delta2 = log2.getDate().getTimeInMillis() - date.getTimeInMillis();
            return Long.compare(Math.abs(delta1), Math.abs(delta2));
        }).orElse(null);

        System.out.println("Closest log entry to 1st March 2026: " + closest);*/

        // Partie 2 Question 6  
        // Retourner une chaîne constituée de tous les urls (en utilisant reduce)
        /*String urls = logs.stream().map(log -> log.getUrl()).reduce("", (url1, url2) -> url1 + "," + url2);
        System.out.println("All URLs: " + urls);*/

        // Partie 2 Question 7
        /*long totalSize = logs.stream().mapToLong(log -> log.getSize()).sum();
        System.out.println("Total size of all log entries: " + totalSize);

        Double averageSize = logs.stream().mapToLong(log -> log.getSize()).average().orElse(0.0);
        System.out.println("Average size of log entries: " + averageSize);*/

        LongSummaryStatistics stats = logs.stream().mapToLong(log -> log.getSize()).summaryStatistics();
        /*System.out.println("Summary statistics for log entry sizes:");
        System.out.println(stats);*/
        try (PrintWriter writer = new PrintWriter(new FileWriter("stats.csv"))) {
            writer.println("count,sum,min,average,max");
            writer.printf("%d,%d,%d,%.2f,%d%n",
                stats.getCount(),
                stats.getSum(),
                stats.getMin(),
                stats.getAverage(),
                stats.getMax());
        }

        // Partie 2 Question 8
        List<AccessLogEntry> filteredEntries = logs.stream().filter(log -> log.isSuccess()).collect(Collectors.toList());
        // EXPORT POUR AIRFLOW / GREAT EXPECTATIONS
        exportToCSVListLogs(filteredEntries, "output_logs.csv");

        // Partie 2 Question 9
        /*Map<Boolean, List<AccessLogEntry>> partitioned =
                logs.stream()
                        .filter(log -> log.getSize() > 2500)
                        .collect(Collectors.partitioningBy(log -> log.isSuccess()));
        System.out.println("Successful entries with size > 2500: " + partitioned.get(true).size());
        System.out.println("Failed entries with size > 2500: " + partitioned.get(false).size());

        // Partie 2 Question 10
        Map<String, List<AccessLogEntry>> groupByIp = logs.stream().collect(Collectors.groupingBy(log -> log.getIp()));
        for (Map.Entry<String, List<AccessLogEntry>> entry : groupByIp.entrySet()) {
            System.out.println("IP: " + entry.getKey() + " Count " + entry.getValue().size() + " entries.");
        }*/

        // Partie 3 Question 1
        /*entryWhile(logs, log -> log.isSuccess()).forEach(log -> System.out.println(log));*/
    }

    static List<AccessLogEntry> entryWhile(List<AccessLogEntry> logs, Predicate<AccessLogEntry> predicate) {
        List<AccessLogEntry> result = new ArrayList<>();
        for (AccessLogEntry log : logs) {
            if (predicate.test(log)) {
                result.add(log);
            } else {
                break;
            }
        }
        return result;
    }

    public static void exportToCSVListLogs(List<AccessLogEntry> entries, String fileName) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
            writer.println("ip,method,url,status,size");

            for (AccessLogEntry log : entries) {
                String ip = quote(log.getIp());
                String method = quote(log.getMethod());
                String url = quote(log.getUrl());
                int status = log.getStatus();
                int size = log.getSize();
                writer.printf("%s,%s,%s,%d,%d%n", ip, method, url, status, size);
            }
            System.out.println("Fichier exporté avec succès : " + fileName);
        } catch (IOException e) {
            System.err.println("Erreur lors de l'exportation CSV : " + e.getMessage());
        }
    }

    private static String quote(String s) {
        if (s == null) return "\"\"";
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }
}