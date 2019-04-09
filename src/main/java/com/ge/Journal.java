package com.ge;

import com.ge.exception.JournalException;
import com.ge.to.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.Objects.isNull;

/**
 * <code>Journal</code> class ensures that messages will be sent to subscribers. The first record in journal - subscriber
 * received a message, the second record in journal - the subscriber confirmed that the message has processed and can be
 * removed from the queue.
 *
 * @see Journal#write
 * @see Journal#getUnsentMessages
 */
public class Journal implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Journal.class);
    private static final String JOURNAL_NAME_DEFAULT = "broker-temp-default-journal";
    private static final String PROPERTIES = "journal.properties";
    private static final String FILE_EXTENSION = ".journal";

    private ObjectOutputStream stream;
    private Path path;

    public Journal() {
        initProperties();
    }

    private void initProperties() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try (InputStream stream = loader.getResourceAsStream(PROPERTIES)) {
            Properties properties = new Properties();
            properties.load(stream);
            path = prepareFilePathAccordingProperties(properties);
            logger.debug("Prepare file path: " + path);
        } catch (IOException e) {
            throw new JournalException("Properties error: ", e);
        }
    }

    private Path prepareFilePathAccordingProperties(Properties properties) {
        Pair<String, String> pathAndFileName = getAndCheckPathProperties(properties);
        return prepareFilePath(pathAndFileName);
    }

    private Pair<String, String> getAndCheckPathProperties(Properties properties) {
        String path = properties.getProperty("broker.journal.path");
        if (System.getProperty("broker.journal.path") != null) {
            path = System.getProperty("broker.journal.path");
        }

        if (isNull(path)) {
            throw new JournalException("Path to journal file is not specified!");
        }
        String name = properties.getProperty("name", JOURNAL_NAME_DEFAULT);
        return Pair.of(path, name);
    }

    private Path prepareFilePath(Pair<String, String> pathAndFileName) {
        return Paths.get(pathAndFileName.getLeft() + File.separator + pathAndFileName.getRight() + FILE_EXTENSION);
    }

    /**
     * This method writes a message to journal.
     *
     * @param subscriber topic subscriber
     * @param message    message that should be written in journal
     */
    public synchronized void write(Subscriber subscriber, Message message) {
        try {
            if (stream == null && Files.notExists(path)) {
                stream = new ObjectOutputStream(Files.newOutputStream(path, APPEND, CREATE));
            } else if (stream == null) {
                stream = new AppendingObjectOutputStream(Files.newOutputStream(path, APPEND));
            }
            stream.writeUTF(subscriber.getName());
            stream.writeInt(message.getPriority());
            stream.writeUTF(message.getContent());
            stream.flush();
        } catch (IOException e) {
            throw new JournalException("Write to file error: ", e);
        }
    }

    /**
     * This method returns messages which was not acknowledged by subscribers.
     *
     * @return <code>map</code> where <code>key</code> - name of the subscriber, <code>value</code> -
     * <code>list</code>
     * of messages which the subscriber didn't acknowledge.
     */
    public synchronized Map<String, Set<Message>> getUnsentMessages() {
        File file = path.toFile();
        Map<String, Set<Message>> unsentMessagesBySubscribers = new HashMap<>();
        try {
            if (isEmptyFileOrFileNotExist(file)) {
                return Collections.emptyMap();
            }
            try (ObjectInputStream stream = new ObjectInputStream(Files.newInputStream(path))) {
                while (stream.available() > 0) {
                    String subscriber = stream.readUTF();
                    int priority = stream.readInt();
                    String content = stream.readUTF();

                    Message msg = new Message(priority, content);
                    Set<Message> messages = unsentMessagesBySubscribers.getOrDefault(subscriber, new HashSet<>());
                    if (messages.contains(msg)) {
                        messages.remove(msg);
                    } else {
                        messages.add(msg);
                        unsentMessagesBySubscribers.put(subscriber, messages);
                    }
                }
            }
            logger.info("Following messages wasn't consume by subscribers:" + unsentMessagesBySubscribers);
        } catch (IOException e) {
            throw new JournalException("Read from file error: ", e);
        }
        return unsentMessagesBySubscribers;
    }

    private boolean isEmptyFileOrFileNotExist(File file) {
        return !file.exists() || file.exists() && file.length() == 0;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }

    private class AppendingObjectOutputStream extends ObjectOutputStream {
        AppendingObjectOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        @Override
        protected void writeStreamHeader() throws IOException {
            reset();
        }
    }
}