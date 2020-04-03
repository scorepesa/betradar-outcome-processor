/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package system;

import java.util.TimerTask;
import utils.Logging;

/**
 *
 * @author dennis
 */
public class LogWorking extends TimerTask{    
    private boolean hasStarted = false;
    
    public LogWorking(){
        
    }

    @Override
    public void run() {
        this.hasStarted = true;
        try{
            System.err.println("5 seconds used up");
            Logging.timer("5 seconds used up");   
        }catch(Exception e){
            System.err.println("Error Logging Timer");
            Logging.error("Error Logging Timer",e);             
        }       
    }
    
    public boolean hasRunStarted() {
        return this.hasStarted;
    }
    
}
