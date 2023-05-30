package ar.edu.itba.pod.client;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogManager {
    private FileWriter buffer;

    public LogManager(String outPath, String file) {
        try {
            buffer = new FileWriter(Path.of(outPath + file).toFile());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeLog(String methodName, String className, int lineNumber, String log) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            buffer.write(dateFormat.format(new Date()) + " INFO [" + methodName + "] " + className +
                    " (" + className + ".java:" + lineNumber + ") - " + log + "\n");
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }


}
