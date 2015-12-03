package crane.task;

public interface CraneWorker extends Runnable {

    public void setTask(Task task);
    
    public void terminate();
}
