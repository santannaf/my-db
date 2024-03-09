package rinha.db;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;

class RinhaDBTest {

    @Test
    public void insert_into_page() throws IOException, ClassNotFoundException {
        var page = new RinhaDB.Page();
        page.insert("rinha");
        page.insert("de");
        page.insert(2024);

        Iterator<byte[]> rows = page.rows();

        Assertions.assertEquals("rinha", RinhaDB.Serialized.deserialize(rows.next(), String.class));
        Assertions.assertEquals("de", RinhaDB.Serialized.deserialize(rows.next(), String.class));
        Assertions.assertEquals(2024, RinhaDB.Serialized.deserialize(rows.next(), Integer.class));

        assert !rows.hasNext();
    }

}