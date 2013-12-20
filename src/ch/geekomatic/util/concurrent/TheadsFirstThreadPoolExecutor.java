package ch.geekomatic.util.concurrent;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Stu Thompson
 *
 */
public class TheadsFirstThreadPoolExecutor extends ThreadPoolExecutor
{
    // capacity is Integer.MAX_VALUE.
    private final BlockingQueue<Runnable> secondaryQueue = new LinkedBlockingQueue<Runnable>();
    
    static class RejectionHandler implements RejectedExecutionHandler
    {
        public void rejectedExecution(Runnable runnable, ThreadPoolExecutor executor)
        {
            ((TheadsFirstThreadPoolExecutor) executor).queueRejectedTask(runnable);
        }
    }
    
    public TheadsFirstThreadPoolExecutor(int initialPoolSize, int cruizingPoolSize, long keepAliveTimeSeconds)
    {
        super(initialPoolSize, cruizingPoolSize, keepAliveTimeSeconds, SECONDS, new SynchronousQueue<Runnable>(true),
                new TheadsFirstThreadPoolExecutor.RejectionHandler());
    }
    
    /**
     * @return null Never returns anything
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Future submit(Runnable newTask)
    {
        // drain secondary queue as rejection handler populates it
        Collection<Runnable> tasks = new ArrayList<Runnable>();
        secondaryQueue.drainTo(tasks);
        
        tasks.add(newTask);
        
        for (Runnable task : tasks)
            super.submit(task);
        
        return null; // does not return a future!
    }
    
    private void queueRejectedTask(Runnable task)
    {
        try
        {
            secondaryQueue.put(task);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}
