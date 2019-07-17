import java.util.Calendar;

public class Trace implements Comparable<Trace> {

    public String caseID;
    public long start;
    public long duration;
    public long end;
    public boolean batched;
    public int batchID;

    public Trace(String caseID, long start, long duration) {
        this.caseID = caseID;
        this.start = start;
        this.duration = duration;
        this.end = start + duration;
        this.batched = false;
    }

    public String getCaseID() { return caseID; }

    public long getStart() {
        return start;
    }

    public long getDuration() {
        return duration;
    }

    public long getEnd() {
        return end;
    }

    public long getEndRounded() {
        long end = this.end;
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(end);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        if (hour < 12) {
            calendar.set(Calendar.HOUR_OF_DAY, 11);
        } else {
            calendar.set(Calendar.HOUR_OF_DAY, 23);
        }
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        long endRounded = calendar.getTimeInMillis();
        return endRounded;
    }

    public void setBatched(boolean batched) {
        this.batched = batched;
    }

    public boolean getBatched() {
        return batched;
    }

    @Override
    public int compareTo(Trace compareTrace) {
        return Long.compare(this.end, compareTrace.end);
    }
}
