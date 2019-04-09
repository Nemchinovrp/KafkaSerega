package com.ge;

import com.ge.to.Message;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class JournalTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void writeReadWhenAllMessagesWereNotSend() throws IOException {
        //Given
        Path path = folder.newFolder().toPath();
        System.setProperty("broker.journal.path", path.toString());
        Journal journal = new Journal();

        Subscriber subscriberOne = new Subscriber("B1");
        Subscriber subscriberTwo = new Subscriber("B2");

        Message messageOne = new Message(1, "test1");
        Message messageTwo = new Message(1, "test2");

        //When
        journal.write(subscriberOne, messageOne);
        journal.write(subscriberTwo, messageTwo);
        Map<String, Set<Message>> unsentMessages = journal.getUnsentMessages();

        //Then
        assertThat(unsentMessages.size(), is(2));
        assertThat(unsentMessages.get(subscriberOne.getName()).iterator().next().getContent(), is("test1"));
        assertThat(unsentMessages.get(subscriberOne.getName()).iterator().next().getPriority(), is(1));
        assertThat(unsentMessages.get(subscriberTwo.getName()).iterator().next().getContent(), is("test2"));
        assertThat(unsentMessages.get(subscriberTwo.getName()).iterator().next().getPriority(), is(1));
    }

    @Test
    public void writeReadWhenOneMessageForSubscriberWasNotSend() throws IOException {
        //Given
        Path path = folder.newFolder().toPath();
        System.setProperty("broker.journal.path", path.toString());
        Journal journal = new Journal();

        Subscriber subscriberOne = new Subscriber("B1");
        Subscriber subscriberTwo = new Subscriber("B2");

        Message messageOne = new Message(1, "test1");
        Message messageTwo = new Message(1, "test2");

        //When
        journal.write(subscriberOne, messageOne);
        journal.write(subscriberTwo, messageTwo);
        journal.write(subscriberOne, messageOne);
        Map<String, Set<Message>> unsentMessages = journal.getUnsentMessages();

        //Then
        assertThat(unsentMessages.size(), is(2));
        assertThat(unsentMessages.get(subscriberTwo.getName()).iterator().next().getContent(), is("test2"));
        assertThat(unsentMessages.get(subscriberTwo.getName()).iterator().next().getPriority(), is(1));
        assertThat(unsentMessages.get(subscriberOne.getName()).size(), is(0));
    }
}