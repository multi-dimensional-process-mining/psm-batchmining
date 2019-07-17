/* E.L. Klijn
 * Performance Mining for Batch Processing Using the Performance Spectrum
 */

import com.opencsv.*;
import io.reactivex.Observable;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.*;
import java.util.concurrent.TimeUnit;
import java.text.SimpleDateFormat;

public class BatchMiner {
    public String directory;
    public String segment;

    public BatchMiner(String directory, String segment) {
        this.directory = directory;
        this.segment = segment;
    }

    /**
     * Traverses all directories and its CSV files and filters out traces corresponding to segment name and time window
     *
     * @param directory
     * @return
     * @throws Exception
     */
    public static List<String> listSegments(String directory) throws Exception {
        Path path = Paths.get(directory);

        List<String> segments = new ArrayList<>();

        Files.walkFileTree(path, new FileVisitor<Path>() {

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {

                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String segmentName = file.getFileName().toString();
                segmentName = segmentName.substring(0, segmentName.length() - 4);
                int index = segmentName.indexOf("!");
                segmentName = segmentName.substring(0, index) + ':' + segmentName.substring(index + 1);
                segments.add(segmentName);

                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
//                    System.out.println("Visit Failed File: "+file);
                return FileVisitResult.CONTINUE;
            }
        });
        return segments;
    }

    /**
     * Traverses all directories and its CSV files and filters out traces corresponding to segment name and time window
     *
     * @param directory
     * @param segment
     * @return
     * @throws Exception
     */
    public static List<Trace> filterSegments(String directory, String segment) throws Exception {
        Path path = Paths.get(directory);

        List<Observable<Trace>> filePipes = new ArrayList<>();
        List<Trace> allTraces = new ArrayList<Trace>();

        Files.walkFileTree(path, new FileVisitor<Path>() {

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {

                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

                String segmentName = file.getFileName().toString();
                segmentName = segmentName.substring(0, segmentName.length() - 4);
                int index = segmentName.indexOf("!");
                segmentName = segmentName.substring(0, index) + ':' + segmentName.substring(index + 1);

                if (segment.equals(segmentName)) {

                    String fileName = file.toString();
                    CSVReader reader = new CSVReader(new FileReader(fileName), ',');

                    Observable<Trace> filePipe = Observable
                            .fromIterable(reader)
                            .filter((String[] csvRow) -> segment.equals(csvRow[1]))
                            .map(csvRow -> {
                                long start, duration;
                                String caseID;
                                caseID = csvRow[0];
                                start = Long.parseLong(csvRow[2]);
                                duration = Long.parseLong(csvRow[3]);
                                Trace trace = new Trace(caseID, start, duration);
                                return trace;
                            });
                    filePipes.add(filePipe);
                }
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
//                    System.out.println("Visit Failed File: "+file);
                return FileVisitResult.CONTINUE;
            }
        });

        allTraces = Observable.merge(filePipes)
                .toList()
                .blockingGet();

        return allTraces;
    }

    /**
     * Iterates list of traces and only keeps traces within defined time frame
     *
     * @param traces
     * @param segmentStart
     * @param segmentEnd
     * @return traces
     */
    public static List<Trace> filterTimeFrame(List<Trace> traces, long segmentStart, long segmentEnd) {
        for (Iterator<Trace> iterator = traces.listIterator(); iterator.hasNext(); ) {
            Trace trace = iterator.next();
            if (trace.getStart() < segmentStart || trace.getEnd() > segmentEnd) {
                iterator.remove();
            }
        }
        return traces;
    }

    /**
     * Prints all traces to console
     *
     * @param traces
     */
    public static void printTraces(List<Trace> traces) {
        for (int i = 0; i < traces.size(); i++) {
            System.out.println("[" + traces.get(i).getCaseID() + ", " + traces.get(i).getStart() + ", " + traces.get(i).getDuration() + ", " + traces.get(i).getEnd() + ", " + traces.get(i).getBatched() + "]");
            System.out.println("End time: " + formatDate(traces.get(i).getEnd()) + "Rounded end time: " + formatDate(traces.get(i).getEndRounded()));
        }
    }

    /**
     * Prints a list of strings to the console
     *
     * @param list
     */
    public static void printListOfStrings(List<String> list) {
        System.out.print("[");
        for (int i = 0; i < list.size() - 1; i++) {
            System.out.print(list.get(i) + ", ");
        }
        System.out.println(list.get(list.size() - 1) + "]");
    }

    /**
     * Partitions observations into batches of a minimum batch size following a set of predefined constraints
     *
     * @param allTraces
     * @param minBatchSize
     * @return
     */
    public static List<Batch> listBatches(List<Trace> allTraces, int minBatchSize) {
        List<Batch> batches = new ArrayList<>();
        List<Trace> tracesInBatch = new ArrayList<>();
        Batch batch;

        tracesInBatch.add(allTraces.get(0));

        for (int i = 0; i < allTraces.size() - 1; i++) {
            if ((allTraces.get(i).getEnd() == allTraces.get(i + 1).getEnd()) && (allTraces.get(i + 1).getStart() - allTraces.get(i).getStart() >= 0)) {
                tracesInBatch.add(allTraces.get(i + 1));
                if (i == allTraces.size() - 2 && tracesInBatch.size() >= minBatchSize) {
                    batch = new Batch(tracesInBatch);
                    batches.add(batch);
                    for (int j = 0; j < tracesInBatch.size(); j++) {
                        tracesInBatch.get(j).setBatched(true);
                    }
                }
            } else {
                if (tracesInBatch.size() >= minBatchSize) {
                    batch = new Batch(tracesInBatch);
                    batches.add(batch);
                    for (int j = 0; j < tracesInBatch.size(); j++) {
                        tracesInBatch.get(j).setBatched(true);
                    }
                }
                tracesInBatch.clear();
                tracesInBatch.add(allTraces.get(i + 1));
            }
        }
        return batches;
    }

    /**
     * Partitions observations into batches of a minimum batch size following a set of predefined constraints, using the rounded end time of observations based on the 12h time window
     *
     * @param allTraces
     * @param minBatchSize
     * @return
     */
    public static List<Batch> listBatchesRounded(List<Trace> allTraces, int minBatchSize) {
        List<Batch> batches = new ArrayList<>();
        List<Trace> tracesInBatch = new ArrayList<>();
        Batch batch;

        tracesInBatch.add(allTraces.get(0));

        for (int i = 0; i < allTraces.size() - 1; i++) {
            if ((allTraces.get(i).getEndRounded() == allTraces.get(i + 1).getEndRounded()) && (allTraces.get(i + 1).getStart() - allTraces.get(i).getStart() >= 0)) {
                tracesInBatch.add(allTraces.get(i + 1));
                if (i == allTraces.size() - 2 && tracesInBatch.size() >= minBatchSize) {
                    batch = new Batch(tracesInBatch);
                    batches.add(batch);
                    for (int j = 0; j < tracesInBatch.size(); j++) {
                        tracesInBatch.get(j).setBatched(true);
                    }
                }
            } else {
                if (tracesInBatch.size() >= minBatchSize) {
                    batch = new Batch(tracesInBatch);
                    batches.add(batch);
                    for (int j = 0; j < tracesInBatch.size(); j++) {
                        tracesInBatch.get(j).setBatched(true);
                    }
                }
                tracesInBatch.clear();
                tracesInBatch.add(allTraces.get(i + 1));
            }
        }
        return batches;
    }

    /**
     * Calculates the mean of an array of values
     *
     * @param A
     * @return mean
     */
    public static double calculateMean(double[] A) {
        double mean = 1.0 * DoubleStream.of(A).sum() / A.length;
        return mean;
    }

    /**
     * Calculates the standard deviation of an array of values
     *
     * @param A
     * @return standardDeviation
     */
    public static double calculateStandardDeviation(double[] A) {

        double mean = 1.0 * DoubleStream.of(A).sum() / A.length;
        double[] variances = new double[A.length];

        for (int i = 0; i < A.length; i++) {
            variances[i] = 1.0 * A[i] - mean;
            variances[i] = variances[i] * variances[i];
        }
        double variance = DoubleStream.of(variances).sum() / A.length;
        double standardDeviation = Math.sqrt(variance);
        return standardDeviation;
    }

    /**
     * Converts a matrix to an array
     *
     * @param M
     * @return A
     */
    public static double[] convertToArray(double[][] M) {
        int length = 0;
        for (int i = 0; i < M.length; i++) {
            length += M[i].length;
        }
        double[] A = new double[length];
        int k = 0;
        for (int i = 0; i < M.length; i++) {
            for (int j = 0; j < M[i].length; j++) {
                A[k] = M[i][j];
                k++;
            }
        }
        return A;
    }

    /**
     * Calculates all segment statistics based on observations and batches and prints these to the console
     *
     * @param segments
     * @throws Exception
     */
    public static void calculateAndPrintSegmentStatistics(List<Segment> segments, String outputDirectory, long startTime) throws Exception {
        String pathName = outputDirectory + "\\Statistics\\segment_statistics.csv";

        FileWriter fw = new FileWriter(pathName);
        fw.write("segmentKey,n,BF,m,mu_k,sigma_k,mu_BI,sigma_BI,mu_{IA},sigma_{IA},mu_{IA_b},sigma_{IA_b},mu_{IA_nb},sigma_{IA_nb},mu_{IAIB},sigma_{IAIB},mu_{Wo_b},sigma_{Wo_b},mu_{Wo_nb},sigma_{Wo_nb}");
        fw.write("\n");

        for (int i = 0; i < segments.size(); i++) {
            Segment segment = segments.get(i);
            List<Batch> batches = segment.getBatches();
            List<Trace> traces = segment.getTraces();

            if (batches.size() > 0) {
                segment.setBatchedTraces(batches);
                if (segment.getBatchedTraces().size() < segment.getTraces().size()) {
                    segment.setNonBatchedTraces(segment.getBatchedTraces(), traces);
                }
            } else if (batches.size() == 0) {
//                System.out.println("No batches found in segment " + segment.getName());
                segment.setNonBatchedTracesNoBatches(traces);
            }
            //for calculating intra-batch measures
            double[][] intraBatchInterArrivalTimesM = new double[batches.size()][];
            for (int j = 0; j < batches.size(); j++) {
                Batch batch = batches.get(j); //get batch
                intraBatchInterArrivalTimesM[j] = batch.getInterArrivalTimes();
            }
            double[] intraBatchInterArrivalTimes = convertToArray(intraBatchInterArrivalTimesM);

            fw.write(segment.getName() + "," + segment.getTotalNrTraces() + "," + segment.getBatchPercentage() + "," + segment.getBatches().size() + ",");
            if (batches.size() > 0) {
                fw.write(calculateMean(segment.getBatchSizes()) + "," + calculateStandardDeviation(segment.getBatchSizes()) + ",");
            } else {
                fw.write("-,-,");
            }
            if (segment.getBatchIntervals() == null) {
                fw.write("-,-,");
            } else {
                fw.write(calculateMean(segment.getBatchIntervals()) + "," + calculateStandardDeviation(segment.getBatchIntervals()) + ",");
            }
            fw.write(calculateMean(segment.getAllCaseInterArrivalTimes()) + "," + calculateStandardDeviation(segment.getAllCaseInterArrivalTimes()) + ",");
            if (batches.size() > 0) {
                fw.write(calculateMean(segment.getBatchedCaseInterArrivalTimes()) + "," + calculateStandardDeviation(segment.getBatchedCaseInterArrivalTimes()) + ",");
            } else {
                fw.write("-,-,");
            }
            if (batches.size() == 0 || segment.getBatchedTraces().size() < segment.getTraces().size()) {
                fw.write(calculateMean(segment.getNonBatchedCaseInterArrivalTimes()) + "," + calculateStandardDeviation(segment.getNonBatchedCaseInterArrivalTimes()) + ",");
            } else {
                fw.write("-,-,");
            }
            if (batches.size() > 0) {
                fw.write(calculateMean(intraBatchInterArrivalTimes) + "," + calculateStandardDeviation(intraBatchInterArrivalTimes) + ",");
            } else {
                fw.write("-,-,");
            }
            if (batches.size() > 0) {
                fw.write(calculateMean(segment.getBatchedCaseWaitingTimes()) + "," + calculateStandardDeviation(segment.getBatchedCaseWaitingTimes()) + ",");
            } else {
                fw.write("-,-,");
            }
            if (batches.size() == 0 || segment.getBatchedTraces().size() < segment.getTraces().size()) {
                fw.write(calculateMean(segment.getNonBatchedCaseWaitingTimes()) + "," + calculateStandardDeviation(segment.getNonBatchedCaseWaitingTimes()));
            } else {
                fw.write("-,-");
            }
            fw.write("\n");
        }
        fw.flush();
        fw.close();
    }

    /**
     * Calculates statistics for each batch and prints these to the console
     *
     * @param segments
     * @throws Exception
     */
    public static void calculateAndPrintBatchStatistics(List<Segment> segments, String outputDirectory) throws Exception {
        String pathName = outputDirectory + "\\Statistics\\batch_statistics.csv";
        FileWriter fw = new FileWriter(pathName);
        fw.write("segmentKey,i,k_i,t_{bi_dep},mu_{IBIA_i},sigma{IBIA_i},mu_{Wo_bi},sigma{Wo_bi},W_{i_min},W{i_max}");
        fw.write("\n");
        for (int i = 0; i < segments.size(); i++) {
            Segment segment = segments.get(i);
            List<Batch> batches = segment.getBatches();
            if (batches.size() > 0) {
                for (int j = 0; j < batches.size(); j++) {
                    Batch batch = batches.get(j); //get batch
                    //calculate statistics
                    batch.setMeanInterArrivalTime(calculateMean(batch.getInterArrivalTimes()));
                    batch.setSdInterArrivalTime(calculateStandardDeviation(batch.getInterArrivalTimes()));
                    batch.setMeanWaitingTime(calculateMean(batch.getWaitingTimes()));
                    batch.setSdWaitingTime(calculateStandardDeviation(batch.getWaitingTimes()));
                    //write to line in CSV
                    fw.write(segment.getName() + "," + (j + 1) + "," + batch.getSize() + "," + formatDate(batch.getEndWait()) + "," + batch.getMeanInterArrivalTime() + "," + batch.getSdInterArrivalTime() + "," + batch.getMeanWaitingTime() + "," + batch.getSdWaitingTime() + "," + batch.getMinWaitingTime() + "," + batch.getMaxWaitingTime());
                    fw.write("\n");
                }
            }
        }
        fw.flush();
        fw.close();
    }

    /**
     * Calculates the interarrival times of traces within a segment, prior to the sorting for batching
     *
     * @param allTraces
     * @return
     */
    public static double[] calculateAllCaseInterArrivalTimes(List<Trace> allTraces) {
        double[] allCaseInterArrivalTimes = new double[allTraces.size() - 1];
        for (int i = 0; i < allCaseInterArrivalTimes.length; i++) {
            allCaseInterArrivalTimes[i] = 1.0 * (allTraces.get(i + 1).getStart() - allTraces.get(i).getStart()) / 3600000;
        }
        return allCaseInterArrivalTimes;
    }

    /**
     * Formats the date from UNIX timestamp in milliseconds to MM-dd-yy HH:mm format
     *
     * @param unixMilliSeconds
     * @return formattedDate
     */
    public static String formatDate(long unixMilliSeconds) {
        Date date = new java.util.Date(unixMilliSeconds);
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("MM-dd-yy HH:mm");
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("GMT+1"));
        String formattedDate = sdf.format(date);
        return formattedDate;
    }

    /**
     * Prints all observations to a CSV for each segment separately, additionally prints copies of every trace, annotated with batch/no batch
     *
     * @param traces
     * @param name
     * @throws Exception
     */
    public static void segmentToCSVlog(List<Trace> traces, String name, String outputDirectory) throws Exception {
        // This part changes all "/" to "_" in activity names to make them usable as filenames
        int index = name.indexOf("/");
        while (index >= 0) {
            name = name.substring(0, index) + '_' + name.substring(index + 1);
            index = name.indexOf("/");
        }
        // This part of for BPI19 log, to delete ":" specifically following "SRM", to make sure the colon between activity names is preserved
        index = name.indexOf("SRM:");
        while (index >= 0) {
            name = name.substring(0, index + 3) + name.substring(index + 4);
            index = name.indexOf("SRM:");
        }
        // This part splits segment name of 2 activities in two separate activity names
        index = name.indexOf(":");
        String startEvent = name.substring(0, index);
        String endEvent = name.substring(index + 1);
        // Change to specify filename and path to save batch/non-batch event logs:
        String pathName = outputDirectory + "\\Logs\\segment_" + startEvent + "_" + endEvent + ".csv";

        FileWriter fw = new FileWriter(pathName);
        fw.write("CaseID,eventName,timestamp");
        fw.write("\n");
        for (int i = 0; i < traces.size(); i++) {
            Trace trace = traces.get(i);
            if (trace.getBatched() == true) {
                fw.write(trace.getCaseID() + " (copy)," + startEvent + " (batch)," + formatDate(trace.getStart()));
                fw.write("\n");
                fw.write(trace.getCaseID() + " (copy)," + endEvent + " (batch)," + formatDate(trace.getEnd()));
                fw.write("\n");
            } else {
                fw.write(trace.getCaseID() + " (copy)," + startEvent + " (no batch)," + formatDate(trace.getStart()));
                fw.write("\n");
                fw.write(trace.getCaseID() + " (copy)," + endEvent + " (no batch)," + formatDate(trace.getEnd()));
                fw.write("\n");
            }
            fw.write(trace.getCaseID() + "," + startEvent + "," + formatDate(trace.getStart()));
            fw.write("\n");
            fw.write(trace.getCaseID() + "," + endEvent + "," + formatDate(trace.getEnd()));
            fw.write("\n");
        }
        fw.flush();
        fw.close();
    }

    public static String getCurrentExecutionTimeString(long startTime) {
        final long executionTime = System.currentTimeMillis() - startTime;
        String executionTimeString = String.format("%d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes(executionTime),
                TimeUnit.MILLISECONDS.toSeconds(executionTime) -
                        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(executionTime))
        );
        return executionTimeString;
    }

    /**
     * Main method: uses segment identifier, directory name, time window and minimum batch size as input to detect
     * batches following a set of constraints, after which segment- and batching metrics are computed.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {

        final long startTime = System.currentTimeMillis();

        //Specify path of folder containing PSM data:
        String inputDirectory = "C:\\Users\\s111402\\OneDrive - TU Eindhoven\\perf_mining_batch_processing\\input_test";

        //Specify minimum batch size (integer):
        int minBatchSize = 20;

        //Specify to use 12h non-FIFO time window for batching y/n (recommended for BPI17 log):
        String nonFIFO = "n";

        //Specify directory path to store logs and statistics (directory must contain two folders called "Statistics" and "Logs"):
        String outputDirectory = "C:\\Users\\s111402\\OneDrive - TU Eindhoven\\perf_mining_batch_processing\\output_test";

        List<String> allSegments = listSegments(inputDirectory);
        System.out.println("Listing segments...");

        //Create list for segments
        List<Segment> segments = new ArrayList<>();

        for (int i = 0; i < allSegments.size(); i++) {
            System.out.println(allSegments.get(i).toUpperCase());
            System.out.println("\tListing observations...");
            //Read all CSV files and filter to list
            List<Trace> allTraces = filterSegments(inputDirectory, allSegments.get(i));
//            allTraces = filterTimeFrame(allTraces, segmentStart, segmentEnd);

            if (allTraces.isEmpty()) {
                System.out.println("Segment " + allSegments.get(i) + " is not contained in time frame or cannot be found.");
            } else {
                // Uncomment to filter based on time frame (example below for period 01-01-2003 - 31-12-2005:
//                long segmentStart = 1041379200000L;        long segmentEnd = 1135987200000L;
//                filterTimeFrame(allTraces, segmentStart, segmentEnd);

                //Sort list first on trace start times (for non-batch statistics)
                allTraces.sort(Comparator.comparing(Trace::getStart));
                double[] allCaseInterArrivalTimes = calculateAllCaseInterArrivalTimes(allTraces);

                List<Batch> batches;

                // Different procedure for BPI17, using non-FIFO 12h time-window for batching
//                if (log.equals("BPI2017") || log.equals("BPI2012")) {
                if (nonFIFO.equals("y")) {
                    //Sort list of trace information first by end time, then by start time (for the actual batching)
                    System.out.println("\tSorting observations...");
                    allTraces.sort(Comparator.comparing(Trace::getEndRounded).thenComparing(Trace::getStart));
                    //List all batches based on algorithm
                    System.out.println("\tDetecting batches...");
                    batches = listBatchesRounded(allTraces, minBatchSize);
                } else {
                    System.out.println("\tSorting observations...");
                    allTraces.sort(Comparator.comparing(Trace::getEnd).thenComparing(Trace::getStart));
                    System.out.println("\tDetecting batches...");
                    batches = listBatches(allTraces, minBatchSize);
                }

                //Create segment object based on traces and batches and add to list of segments
                Segment segment = new Segment(allSegments.get(i), allTraces, batches, allCaseInterArrivalTimes);
                segments.add(segment);
                // Uncomment below to print each segment to CSV separately:
                System.out.println("\tPrinting annotated log to CSV...");
                segmentToCSVlog(segment.getTraces(), segment.getName(), outputDirectory);
            }
        }
        System.out.println("Calculating and printing segment statistics...");
        calculateAndPrintSegmentStatistics(segments, outputDirectory, startTime);
        System.out.println("Calculating and printing batch statistics...");
        calculateAndPrintBatchStatistics(segments, outputDirectory);

        final long executionTime = System.currentTimeMillis() - startTime;
        String executionTimeString = String.format("%d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes(executionTime),
                TimeUnit.MILLISECONDS.toSeconds(executionTime) -
                        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(executionTime))
        );
        System.out.println("\nTotal execution time: " + executionTimeString);
    }
}