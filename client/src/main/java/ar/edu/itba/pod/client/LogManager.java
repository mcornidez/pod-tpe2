package ar.edu.itba.pod.client;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogManager {
    private BufferedWriter buffer;

    public LogManager(String outPath, String file) {
        try {
            buffer = new BufferedWriter(new FileWriter(outPath + file, false));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeLog(String methodName, String className, int lineNumber, String log) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SS");
        try {
            buffer.write(dateFormat.format(new Date()) + " INFO [" + methodName + "] " + className +
                    " (" + className + ".java:" + lineNumber + ") - " + log + "\n");
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }


}
