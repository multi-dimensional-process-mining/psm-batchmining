import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Segment {
    // basic segment statistics
    public String name;
    public int totalNrTraces;
    public long segmentStart;
    public long segmentEnd;
    public int nrTracesInBatch;
    public double batchPercentage;

    // sets of batches and traces
    public List<Trace> traces;
    public List<Trace> nonBatchedTraces;
    public List<Trace> batchedTraces;
    public List<Batch> batches;

    // batch size statistics
    public double[] batchSizes;
    public double meanBatchSize;
    public double sdBatchSize;

    //batch interval statistics
    public double[] batchIntervals;
    public double meanBatchInterval;
    public double sdBatchInterval;

    // all case statistics
    public double[] allCaseWaitingTimes;
    public double[] allCaseInterArrivalTimes;

    //intra-batch statistics
    public double[][] intraBatchInterArrivalTimes;

    //batched case statistics
    public double[] batchedCaseWaitingTimes;
    public double[] batchedCaseInterArrivalTimes;

    //non-batched case statistics
    public double[] nonBatchedCaseWaitingTimes;
    public double[] nonBatchedCaseInterArrivalTimes;


    public Segment(String name, List<Trace> traces, List<Batch> batches, double[] allCaseInterArrivalTimes) {
        this.name = name;
        this.traces = traces;
        this.batches = batches;
        this.totalNrTraces = traces.size();
        this.allCaseInterArrivalTimes = allCaseInterArrivalTimes;

        this.segmentStart = traces.get(0).getStart();
        for (int i = 0; i < traces.size(); i++) {
            if (traces.get(i).getStart() < this.segmentStart) {
                this.segmentStart = traces.get(i).getStart();
            }
        }
        this.segmentEnd = traces.get(0).getEnd();
        for (int i = 0; i < traces.size(); i++) {
            if (traces.get(i).getEnd() > this.segmentEnd) {
                this.segmentEnd = traces.get(i).getEnd();
            }
        }

        for (int i = 0; i < batches.size(); i++) {
            this.nrTracesInBatch += batches.get(i).getSize();
        }
        this.batchPercentage = 100.0 * nrTracesInBatch / totalNrTraces;

        this.batchSizes = new double[batches.size()];
        for (int i = 0; i < batchSizes.length; i++) {
            batchSizes[i] = 1.0 * batches.get(i).getSize();
        }

        if (batches.size() != 0) {
            this.batchIntervals = new double[batches.size() - 1];
            for (int i = 0; i < batchIntervals.length; i++) {
                this.batchIntervals[i] = 1.0 * (batches.get(i + 1).getEndWait() - batches.get(i).getEndWait()) / 3600000;
            }
        }

        this.allCaseWaitingTimes = new double[traces.size()];
        for (int i = 0; i < allCaseWaitingTimes.length; i++) {
            this.allCaseWaitingTimes[i] = 1.0 * traces.get(i).getDuration() / 3600000;
        }
    }

    /**
     * ****************************************************************
     * ******* METHODS FOR GETTING BASIC SEGMENT PARAMETERS ***********
     * ****************************************************************
     */
    public String getName() {
        return name;
    }

    public List<Trace> getTraces() {
        return traces;
    }

    public int getTotalNrTraces() {
        return totalNrTraces;
    }

    public long getSegmentStart() {
        return segmentStart;
    }

    public long getSegmentEnd() {
        return segmentEnd;
    }

    public List<Batch> getBatches() {
        return batches;
    }

    public int getNrTracesInBatch() {
        return nrTracesInBatch;
    }

    public double getBatchPercentage() {
        return batchPercentage;
    }

    /**
     * ****************************************************************
     * ******************** METHODS FOR BATCH SIZES *******************
     * ****************************************************************
     */
    public double[] getBatchSizes() {
        return batchSizes;
    }

    public double getMeanBatchSize() {
        return meanBatchSize;
    }

    public double getSdBatchSize() {
        return sdBatchSize;
    }

    public void setMeanBatchSize(double meanBatchSize) {
        this.meanBatchSize = meanBatchSize;
    }

    public void setSdBatchSize(double sdBatchSize) {
        this.sdBatchSize = sdBatchSize;
    }

    /**
     * ****************************************************************
     * ************** METHODS FOR INTER-BATCH TIMES *******************
     * ****************************************************************
     */
    public double[] getBatchIntervals() {
        return batchIntervals;
    }

    public double getMeanBatchInterval() {
        return meanBatchInterval;
    }

    public double getSdBatchInterval() {
        return sdBatchInterval;
    }

    public void setMeanBatchInterval(double meanBatchInterval) {
        this.meanBatchInterval = meanBatchInterval;
    }

    public void setSdBatchInterval(double sdBatchInterval) {
        this.sdBatchInterval = sdBatchInterval;
    }

    /**
     * ****************************************************************
     * ************** METHODS FOR ALL CASE STATISTICS *****************
     * ****************************************************************
     */
    public double[] getAllCaseWaitingTimes() {
        return allCaseWaitingTimes;
    }

    public double[] getAllCaseInterArrivalTimes() {
        return allCaseInterArrivalTimes;
    }

    /**
     * ****************************************************************
     * ********* METHODS FOR INTRA-BATCH INTERARRIVAL TIMES ***********
     * ****************************************************************
     */
    public void setIntraBatchInterArrivalTimes(double[][] intraBatchInterArrivalTimes) {
        this.intraBatchInterArrivalTimes = intraBatchInterArrivalTimes;
    }

    public double[][] getIntraBatchInterArrivalTimes() {
        return intraBatchInterArrivalTimes;
    }

    /**
     * ****************************************************************
     * *********** METHODS FOR BATCHED TRACES AND STATISTICS***********
     * ****************************************************************
     */
    public void setBatchedTraces(List<Batch> batches) {
        this.batchedTraces = new ArrayList<>();
        for (int i = 0; i < batches.size(); i++) {
            for (int j = 0; j < batches.get(i).getTraces().size(); j++) {
                batches.get(i).getTraces().get(j).setBatched(true);
                this.batchedTraces.add(batches.get(i).getTraces().get(j));
            }
        }
        this.batchedCaseWaitingTimes = new double[this.batchedTraces.size()];
        for (int i = 0; i < this.batchedTraces.size(); i++) {
            this.batchedCaseWaitingTimes[i] = 1.0 * (this.batchedTraces.get(i).getDuration()) / 3600000;
        }
        this.batchedTraces.sort(Comparator.comparing(Trace::getStart));
        this.batchedCaseInterArrivalTimes = new double[this.batchedTraces.size() - 1];
        for (int i = 0; i < batchedCaseInterArrivalTimes.length; i++) {
            batchedCaseInterArrivalTimes[i] = 1.0 * (this.batchedTraces.get(i + 1).getStart() - this.batchedTraces.get(i).getStart()) / 3600000;
        }
    }

    public List<Trace> getBatchedTraces() {
        return batchedTraces;
    }

    public double[] getBatchedCaseWaitingTimes() {
        return batchedCaseWaitingTimes;
    }

    public double[] getBatchedCaseInterArrivalTimes() {
        return batchedCaseInterArrivalTimes;
    }

    /**
     * ****************************************************************
     * ********* METHODS FOR NON-BATCHED TRACES AND STATISTICS*********
     * ****************************************************************
     */
    public void setNonBatchedTraces(List<Trace> batchedTraces, List<Trace> traces) {
        List<Trace> newTraces = new ArrayList<>(traces);
        this.nonBatchedTraces = new ArrayList<>();
        for (int i = 0; i < batchedTraces.size(); i++) {
            for (Iterator<Trace> iterator = newTraces.listIterator(); iterator.hasNext(); ) {
                Trace trace = iterator.next();
                if (trace.getCaseID().equals(batchedTraces.get(i).getCaseID()) && trace.getStart() == batchedTraces.get(i).getStart() && trace.getEnd() == batchedTraces.get(i).getEnd() && trace.getDuration() == batchedTraces.get(i).getDuration()) {
                    iterator.remove();
                }
            }
        }
        for (int i = 0; i < newTraces.size(); i++) {
            this.nonBatchedTraces.add(newTraces.get(i));
        }
        this.nonBatchedCaseWaitingTimes = new double[this.nonBatchedTraces.size()];
        for (int i = 0; i < this.nonBatchedTraces.size(); i++) {
            this.nonBatchedCaseWaitingTimes[i] = 1.0 * (this.nonBatchedTraces.get(i).getDuration()) / 3600000;
        }
        this.nonBatchedTraces.sort(Comparator.comparing(Trace::getStart));
        this.nonBatchedCaseInterArrivalTimes = new double[this.nonBatchedTraces.size() - 1];
        for (int i = 0; i < nonBatchedCaseInterArrivalTimes.length; i++) {
            nonBatchedCaseInterArrivalTimes[i] = 1.0 * (this.nonBatchedTraces.get(i + 1).getStart() - this.nonBatchedTraces.get(i).getStart()) / 3600000;
        }
    }

    public void setNonBatchedTracesNoBatches(List<Trace> traces) {
        List<Trace> newTraces = new ArrayList<>(traces);
        this.nonBatchedTraces = new ArrayList<>();

        for (int i = 0; i < newTraces.size(); i++) {
            this.nonBatchedTraces.add(newTraces.get(i));
        }
        this.nonBatchedCaseWaitingTimes = new double[this.nonBatchedTraces.size()];
        for (int i = 0; i < this.nonBatchedTraces.size(); i++) {
            this.nonBatchedCaseWaitingTimes[i] = 1.0 * (this.nonBatchedTraces.get(i).getDuration()) / 3600000;
        }
        this.nonBatchedTraces.sort(Comparator.comparing(Trace::getStart));
        this.nonBatchedCaseInterArrivalTimes = new double[this.nonBatchedTraces.size() - 1];
        for (int i = 0; i < nonBatchedCaseInterArrivalTimes.length; i++) {
            nonBatchedCaseInterArrivalTimes[i] = 1.0 * (this.nonBatchedTraces.get(i + 1).getStart() - this.nonBatchedTraces.get(i).getStart()) / 3600000;
        }
    }

    public List<Trace> getNonBatchedTraces() {
        return nonBatchedTraces;
    }

    public double[] getNonBatchedCaseWaitingTimes() {
        return nonBatchedCaseWaitingTimes;
    }

    public double[] getNonBatchedCaseInterArrivalTimes() {
        return nonBatchedCaseInterArrivalTimes;
    }
}
